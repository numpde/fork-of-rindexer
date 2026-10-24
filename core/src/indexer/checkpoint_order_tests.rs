//! Drive the real callback dispatcher and durable checkpoint writer with two in-flight batches.
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::{Log, ValueOrArray},
};
use futures::FutureExt;
use tempfile::TempDir;
use tokio::{
    sync::{Notify, Semaphore},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{handle_logs_result, CallbackCheckpoints};
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{
            noop_decoder, EventCallbackRegistry, EventCallbackRegistryInformation,
            EventCallbackType,
        },
        config::{ContractEventProcessingConfig, EventProcessingConfig},
        contract_setup::{
            AddressDetails, ContractInformation, IndexingContractSetup, NetworkContract,
        },
    },
    indexer::{
        fetch_logs::FetchLogsResult, progress::IndexingEventsProgressState,
        task_tracker::active_indexing_count,
    },
    provider::mock::MockChainProvider,
};

struct Fixture {
    directory: TempDir,
    config: Arc<EventProcessingConfig>,
    permits: Arc<Semaphore>,
    checkpoints: CallbackCheckpoints,
}

impl Fixture {
    async fn new(callback: EventCallbackType) -> Self {
        let directory = tempfile::tempdir().expect("checkpoint directory");
        let provider = Arc::new(MockChainProvider::new(1).with_block_number(10));
        let network = NetworkContract {
            id: "checkpoint".to_string(),
            network: "ethereum".to_string(),
            indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                address: ValueOrArray::Value(Address::ZERO),
                indexed_filters: None,
            }),
            block_clock: BlockClock::new(None, None, provider.clone()),
            cached_provider: provider,
            decoder: noop_decoder(),
            start_block: Some(U64::from(1)),
            end_block: Some(U64::from(10)),
            disable_logs_bloom_checks: false,
        };
        let mut registry = EventCallbackRegistry::new();
        registry.register_event(EventCallbackRegistryInformation {
            id: "checkpoint".to_string(),
            indexer_name: "checkpoints".to_string(),
            topic_id: B256::ZERO,
            event_name: "Value".to_string(),
            index_event_in_order: false,
            contract: ContractInformation {
                name: "Contract".to_string(),
                details: vec![network.clone()],
                abi: "unused.json".to_string().into(),
                reorg_safe_distance: None,
            },
            callback,
            tables: Arc::new(vec![]),
            reorg_sender: None,
            streams_clients: Arc::new(None),
            providers: Arc::new(HashMap::new()),
            constants: Arc::new(HashMap::new()),
            multicall_addresses: Arc::new(HashMap::new()),
        });
        let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
        let config = Arc::new(EventProcessingConfig::from(ContractEventProcessingConfig {
            id: "checkpoint".to_string(),
            project_path: directory.path().to_path_buf(),
            indexer_name: "checkpoints".to_string(),
            contract_name: "Contract".to_string(),
            topic_id: B256::ZERO,
            event_name: "Value".to_string(),
            config: crate::manifest::config::Config {
                callback_concurrency: Some(2),
                ..Default::default()
            },
            network_contract: Arc::new(network),
            timestamps: false,
            start_block: U64::from(1),
            end_block: U64::from(10),
            registry: registry.complete(),
            progress,
            postgres: None,
            clickhouse: None,
            csv_details: None,
            stream_last_synced_block_file_path: Some("cursor".to_string()),
            index_event_in_order: false,
            live_indexing: false,
            indexing_distance_from_head: U64::ZERO,
            cancel_token: CancellationToken::new(),
            tables: Arc::new(vec![]),
            reorg_sender: None,
            streams_clients: Arc::new(None),
            contract_abi: None,
            providers: Arc::new(HashMap::new()),
            constants: Arc::new(HashMap::new()),
            multicall_addresses: Arc::new(HashMap::new()),
        }));
        let fixture = Self {
            directory,
            config,
            permits: Arc::new(Semaphore::new(2)),
            checkpoints: CallbackCheckpoints::default(),
        };
        let path = fixture.cursor_path();
        std::fs::create_dir_all(path.parent().unwrap()).expect("checkpoint parent");
        std::fs::write(path, "1").expect("seed durable cursor");
        fixture
    }

    fn cursor_path(&self) -> PathBuf {
        self.directory.path().join("cursor/Contract/last-synced-blocks/contract-ethereum-value.txt")
    }

    fn cursor(&self) -> String {
        std::fs::read_to_string(self.cursor_path()).expect("read durable cursor")
    }

    async fn dispatch(&mut self, block: u64, empty: bool) -> JoinHandle<()> {
        let logs = if empty {
            vec![]
        } else {
            vec![Log {
                block_hash: Some(B256::repeat_byte(block as u8)),
                block_number: Some(block),
                transaction_hash: Some(B256::repeat_byte(block as u8)),
                transaction_index: Some(0),
                log_index: Some(0),
                ..Default::default()
            }]
        };
        handle_logs_result(
            self.config.clone(),
            self.permits.clone(),
            &mut self.checkpoints,
            Ok(FetchLogsResult {
                logs,
                from_block: U64::from(block),
                to_block: U64::from(block),
                reorg: None,
            }),
        )
        .await
        .expect("dispatch callback")
    }
}

fn delayed_callback(
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
    later_ran: Arc<Notify>,
) -> EventCallbackType {
    Arc::new(move |events| {
        let (first_started, release_first, later_ran) =
            (first_started.clone(), release_first.clone(), later_ran.clone());
        async move {
            if events[0].tx_information.block_number == 2 {
                first_started.notify_one();
                release_first.notified().await;
            } else {
                later_ran.notify_one();
            }
            Ok(())
        }
        .boxed()
    })
}

async fn assert_later_checkpoint_waits(empty: bool) {
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let later_ran = Arc::new(Notify::new());
    let mut fixture = Fixture::new(delayed_callback(
        first_started.clone(),
        release_first.clone(),
        later_ran.clone(),
    ))
    .await;
    let first = fixture.dispatch(2, false).await;
    timeout(Duration::from_secs(5), first_started.notified()).await.expect("first callback starts");
    let mut later = fixture.dispatch(3, empty).await;
    if !empty {
        timeout(Duration::from_secs(5), later_ran.notified())
            .await
            .expect("callbacks must still execute concurrently");
    }
    // A generous deadline distinguishes a completed later checkpoint from a
    // task deliberately waiting on its predecessor. No sleep orders the callbacks.
    let early_completion = timeout(Duration::from_secs(1), &mut later).await;
    let cursor_while_first_pending = fixture.cursor();
    release_first.notify_one();
    first.await.expect("first callback completes");
    match early_completion {
        Ok(result) => result.expect("later task completes"),
        Err(_) => later.await.expect("later task completes after its predecessor"),
    }
    assert_eq!(
        cursor_while_first_pending, "1",
        "a later callback must not checkpoint past unfinished earlier input"
    );
    assert_eq!(fixture.cursor(), "3", "completed callbacks advance the cursor in source order");
    assert_eq!(active_indexing_count(), 0);
}

#[tokio::test]
async fn later_callback_effects_can_finish_without_advancing_past_a_pending_batch() {
    assert_later_checkpoint_waits(false).await;
}

#[tokio::test]
async fn empty_tail_cannot_advance_past_a_pending_callback() {
    assert_later_checkpoint_waits(true).await;
}

#[tokio::test]
async fn cancelled_predecessor_blocks_later_checkpoint_and_releases_task_tracking() {
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let later_ran = Arc::new(Notify::new());
    let mut fixture =
        Fixture::new(delayed_callback(first_started.clone(), release_first, later_ran.clone()))
            .await;
    let first = fixture.dispatch(2, false).await;
    timeout(Duration::from_secs(5), first_started.notified()).await.expect("first callback starts");
    let later = fixture.dispatch(3, false).await;
    timeout(Duration::from_secs(5), later_ran.notified()).await.expect("later callback ran");
    first.abort();
    assert!(first.await.expect_err("first task was cancelled").is_cancelled());
    timeout(Duration::from_secs(5), later)
        .await
        .expect("later task must not hang on a cancelled predecessor")
        .expect("later task completes");
    assert_eq!(fixture.cursor(), "1", "cancellation must not authorize a later cursor update");
    assert_eq!(active_indexing_count(), 0, "cancelled callbacks must not hang graceful shutdown");
}

#[tokio::test]
async fn unrelated_stream_can_checkpoint_while_another_stream_is_blocked() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut blocked =
        Fixture::new(delayed_callback(started.clone(), release.clone(), Arc::new(Notify::new())))
            .await;
    let first = blocked.dispatch(2, false).await;
    timeout(Duration::from_secs(5), started.notified()).await.expect("blocked callback starts");
    let mut other = Fixture::new(Arc::new(|_| async { Ok(()) }.boxed())).await;
    timeout(Duration::from_secs(5), other.dispatch(3, false).await)
        .await
        .expect("unrelated stream is not blocked")
        .expect("unrelated callback succeeds");
    let other_cursor = other.cursor();
    release.notify_one();
    first.await.expect("release blocked stream");
    assert_eq!(other_cursor, "3");
    assert_eq!(active_indexing_count(), 0);
}
