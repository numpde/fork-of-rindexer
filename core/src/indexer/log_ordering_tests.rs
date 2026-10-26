//! A fetched range can advance only after merged and enriched logs retain event order.
use super::*;
use crate::{
    event::contract_setup::{AddressDetails, IndexingContractSetup},
    manifest::network::AddressFiltering,
    provider::{mock::MockChainProvider, JsonRpcCachedProvider},
};
use alloy::{
    network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
    primitives::{Address, LogData},
    rpc::types::{Block, BlockTransactions, Log, ValueOrArray},
};
use futures::StreamExt;

fn log(block: u64, transaction: u64, index: u64) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(1),
            data: LogData::new_unchecked(vec![B256::ZERO], Default::default()),
        },
        block_number: Some(block),
        block_hash: Some(B256::repeat_byte(block as u8)),
        transaction_index: Some(transaction),
        transaction_hash: Some(B256::repeat_byte(index as u8)),
        log_index: Some(index),
        ..Default::default()
    }
}
fn filter() -> RindexerEventFilter {
    RindexerEventFilter::new_address_filter(
        &B256::ZERO,
        "Value",
        &AddressDetails {
            address: ValueOrArray::Array(vec![Address::repeat_byte(1), Address::repeat_byte(2)]),
            indexed_filters: None,
        },
        U64::from(100),
        U64::from(500),
    )
    .unwrap()
}
fn address_rpc(chunk_size: usize) -> Arc<JsonRpcCachedProvider> {
    let (provider, asserter) = JsonRpcCachedProvider::mock_with_asserter_and_address_filtering(
        1,
        AddressFiltering::MaxAddressPerGetLogsRequest(chunk_size),
    );
    if chunk_size == 1 {
        asserter.push_success(&vec![log(100, 0, 1), log(500, 0, 0)]);
        asserter.push_success(&vec![log(100, 0, 0), log(200, 0, 0)]);
    } else {
        asserter.push_success(&vec![
            log(500, 0, 0),
            log(100, 1, 2),
            log(100, 0, 1),
            log(100, 0, 0),
            log(200, 0, 0),
        ]);
    }
    provider
}

#[tokio::test]
async fn address_query_results_are_ordered_for_one_and_multiple_chunks() {
    for chunk_size in [1, 32] {
        let logs = address_rpc(chunk_size).get_logs(&filter()).await.unwrap();
        let actual = logs
            .iter()
            .map(|log| {
                (log.block_number.unwrap(), log.transaction_index.unwrap(), log.log_index.unwrap())
            })
            .collect::<Vec<_>>();
        let mut expected = vec![(100, 0, 0), (100, 0, 1), (200, 0, 0), (500, 0, 0)];
        if chunk_size > 1 {
            expected.insert(2, (100, 1, 2));
        }
        assert_eq!(actual, expected, "chunk size {chunk_size}");
    }
}

#[tokio::test]
async fn sequential_address_fetch_advances_past_the_highest_log() {
    let (tx, mut rx) = mpsc::channel(4);
    let result = fetch_historic_logs_stream(
        false,
        BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
        address_rpc(1).as_ref(),
        &tx,
        &B256::ZERO,
        filter(),
        None,
        U64::from(500),
        "ordering",
        0,
    )
    .await
    .unwrap();
    assert_eq!(result.next.from_block(), U64::from(501));
    let logs = rx.recv().await.unwrap().unwrap().logs;
    assert_eq!(
        logs.iter().map(|log| log.block_number.unwrap()).collect::<Vec<_>>(),
        vec![100, 100, 200, 500]
    );
}

async fn config(
    provider: Arc<dyn ChainProvider>,
    clock_provider: Arc<dyn ChainProvider>,
    timestamps: bool,
) -> Arc<EventProcessingConfig> {
    let mut fixture = crate::indexer::process::checkpoint_order_tests::Fixture::with_logs(
        Arc::new(|_| Box::pin(async { Ok(()) })),
        vec![],
    )
    .await;
    let EventProcessingConfig::ContractEventProcessing(config) =
        Arc::get_mut(&mut fixture.config).unwrap()
    else {
        panic!("contract fixture")
    };
    config.start_block = U64::from(100);
    config.end_block = U64::from(if timestamps { 100 } else { 500 });
    config.timestamps = timestamps;
    config.live_indexing = true;
    let network = Arc::make_mut(&mut config.network_contract);
    network.cached_provider = provider;
    network.block_clock = BlockClock::new(None, None, clock_provider);
    network.indexing_contract_setup = IndexingContractSetup::Address(AddressDetails {
        address: ValueOrArray::Array(vec![Address::repeat_byte(1), Address::repeat_byte(2)]),
        indexed_filters: None,
    });
    fixture.config
}

async fn run_worker(
    config: Arc<EventProcessingConfig>,
    filter: RindexerEventFilter,
) -> SequencedFetchBatch {
    let (tx, mut rx) = mpsc::channel(4);
    let generation = crate::indexer::reorg::reorg_generation("ethereum");
    tokio::time::timeout(
        Duration::from_secs(2),
        parallel_worker(
            config,
            WorkerState {
                sequence_id: 0,
                filter,
                max_block_range_limitation: None,
                original_max_limit: None,
                reorg_generation: generation,
            },
            tx,
            CancellationToken::new(),
            Arc::new(AdaptiveConcurrency::new(1, 1, 1)),
            Arc::new(AtomicUsize::new(1)),
            Arc::new(tokio::sync::Notify::new()),
        ),
    )
    .await
    .expect("worker must complete its fetched range or report its missing header");
    rx.recv().await.unwrap()
}

#[tokio::test]
async fn parallel_address_fetch_completes_without_refetching_its_delivered_prefix() {
    let provider = address_rpc(1);
    let config = config(provider, Arc::new(MockChainProvider::new(1)), false).await;
    let batch = run_worker(config, filter()).await;
    assert!(batch.is_final);
    assert_eq!(batch.results.len(), 1);
    let logs = &batch.results[0].as_ref().unwrap().logs;
    assert_eq!(
        logs.iter().map(|log| log.block_number.unwrap()).collect::<Vec<_>>(),
        vec![100, 100, 200, 500]
    );
}

async fn missing_timestamp_config() -> Arc<EventProcessingConfig> {
    let header = AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader { number: 101, timestamp: 1010, ..Default::default() }
                    .seal(B256::repeat_byte(101)),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    );
    config(
        Arc::new(
            MockChainProvider::new(1)
                .with_block_number(101)
                .with_blocks(vec![header])
                .with_logs(vec![log(100, 0, 0)]),
        ),
        Arc::new(MockChainProvider::new(1).with_block_batches(vec![vec![]])),
        true,
    )
    .await
}

#[tokio::test]
async fn historical_missing_timestamp_reports_failure_without_live_handoff() {
    let config = missing_timestamp_config().await;
    let mut stream = fetch_logs_stream(
        config.clone(),
        false,
        None,
        None,
        crate::indexer::reorg::subscribe_reorgs("ethereum"),
    );
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("unresolved single-block history must surface an error")
        .unwrap();
    assert!(matches!(first,Err(error) if error.is::<crate::blockclock::BlockClockError>()));
    assert!(tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("failed history must not enter live fetching")
        .is_none());
    config.cancel_token().cancel();
}

#[tokio::test]
async fn parallel_missing_timestamp_reports_the_header_error() {
    let config = missing_timestamp_config().await;
    let batch = run_worker(config, filter().set_to_block(U64::from(100))).await;
    assert!(batch.is_final);
    assert!(
        matches!(&batch.results[0],Err(error) if error.is::<crate::blockclock::BlockClockError>())
    );
}
