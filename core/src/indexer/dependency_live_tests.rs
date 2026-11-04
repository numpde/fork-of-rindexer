//! Exercise dependency-ordered live polling through RPC, callbacks, and durable cursors.
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use alloy::{
    network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
    primitives::{Address, LogData, B256, U64},
    rpc::types::{Block, BlockTransactions, Log, ValueOrArray},
};
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use super::{live_indexing_for_contract_event_dependencies, EventDependenciesIndexingConfig};
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{
            noop_decoder, EventCallbackRegistry, EventCallbackRegistryInformation,
        },
        config::{ContractEventProcessingConfig, EventProcessingConfig},
        contract_setup::{
            AddressDetails, ContractInformation, IndexingContractSetup, NetworkContract,
        },
    },
    indexer::{
        last_synced::{get_last_synced_block_number, SyncConfig},
        progress::IndexingEventsProgressState,
    },
    manifest::{network::BlockPollFrequency, stream::StreamsConfig},
    provider::{create_client, ChainProvider},
};

const NETWORK: &str = "ethereum";
const TOPIC: B256 = B256::repeat_byte(9);

#[derive(Clone)]
struct Detail {
    address: Address,
    indexed: B256,
    start: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Applied {
    cursor: String,
    address: Address,
    indexed: B256,
    block: u64,
}

fn header(number: u64) -> AnyRpcBlock {
    AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader {
                    number,
                    parent_hash: B256::repeat_byte(number.saturating_sub(1) as u8),
                    timestamp: number * 10,
                    ..Default::default()
                }
                .seal(B256::repeat_byte(number as u8)),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    )
}

fn log(detail: &Detail, number: u64, index: usize) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: detail.address,
            data: LogData::new_unchecked(vec![TOPIC, detail.indexed], Default::default()),
        },
        block_number: Some(number),
        block_hash: Some(B256::repeat_byte(number as u8)),
        transaction_index: Some(0),
        transaction_hash: Some(B256::repeat_byte(number as u8)),
        log_index: Some(index as u64),
        ..Default::default()
    }
}

fn hex_number(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

fn selector_matches(selector: &Value, actual: &str) -> bool {
    match selector {
        Value::Null => true,
        Value::String(expected) => expected == actual,
        Value::Array(alternatives) => {
            alternatives.iter().any(|value| selector_matches(value, actual))
        }
        _ => panic!("unexpected RPC filter {selector}"),
    }
}

struct RpcState {
    head: AtomicU64,
    logs: Vec<Log>,
    log_requests: Mutex<Vec<Value>>,
}

impl RpcState {
    fn reply(&self, request: Value) -> Value {
        let result = match request["method"].as_str().unwrap() {
            "eth_chainId" => json!("0x1"),
            "eth_blockNumber" => json!(format!("{:#x}", self.head.load(Ordering::SeqCst))),
            "eth_getBlockByNumber" => {
                let number = if request["params"][0] == "latest" {
                    self.head.load(Ordering::SeqCst)
                } else {
                    hex_number(&request["params"][0])
                };
                json!(header(number))
            }
            "eth_getLogs" => {
                let filter = &request["params"][0];
                self.log_requests.lock().unwrap().push(filter.clone());
                let from = hex_number(&filter["fromBlock"]);
                let to = hex_number(&filter["toBlock"]);
                json!(self
                    .logs
                    .iter()
                    .filter(|log| {
                        log.block_number.is_some_and(|number| number >= from && number <= to)
                            && selector_matches(
                                &filter["address"],
                                &format!("{:#x}", log.address()),
                            )
                            && filter["topics"].as_array().unwrap().iter().zip(log.topics()).all(
                                |(selector, topic)| {
                                    selector_matches(selector, &format!("{topic:#x}"))
                                },
                            )
                    })
                    .collect::<Vec<_>>())
            }
            method => panic!("unexpected dependency-live RPC method {method}"),
        };
        json!({"jsonrpc":"2.0", "id":request["id"], "result":result})
    }
}

struct Rpc {
    state: Arc<RpcState>,
    provider: Arc<dyn ChainProvider>,
    server: JoinHandle<()>,
}

impl Rpc {
    async fn start(logs: Vec<Log>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(RpcState {
            head: AtomicU64::new(101),
            logs,
            log_requests: Mutex::new(vec![]),
        });
        let shared = state.clone();
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let state = shared.clone();
                async move {
                    Json(match request {
                        Value::Array(requests) => Value::Array(
                            requests.into_iter().map(|request| state.reply(request)).collect(),
                        ),
                        request => state.reply(request),
                    })
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = create_client(
            &url,
            1,
            None,
            None,
            Some(BlockPollFrequency::PollRateMs { millis: 0 }),
            Default::default(),
            None,
            None,
        )
        .await
        .unwrap();
        Self { state, provider, server }
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Fixture {
    directory: TempDir,
    rpc: Rpc,
    networks: Vec<NetworkContract>,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<IndexingEventsProgressState>,
    streams: StreamsConfig,
    applied: Arc<Mutex<Vec<Applied>>>,
}

impl Fixture {
    async fn new(details: Vec<Detail>) -> Self {
        let mut unique_details = details.clone();
        unique_details.sort_by_key(|detail| (detail.address, detail.indexed));
        unique_details.dedup_by_key(|detail| (detail.address, detail.indexed));
        let logs = (101..=102)
            .flat_map(|number| {
                unique_details
                    .iter()
                    .enumerate()
                    .map(move |(index, detail)| log(detail, number, index))
            })
            .collect();
        let rpc = Rpc::start(logs).await;
        let networks = details
            .iter()
            .enumerate()
            .map(|(index, detail)| NetworkContract {
                id: format!("detail-{index}"),
                network: NETWORK.into(),
                indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                    address: ValueOrArray::Value(detail.address),
                    indexed_filters: Some(vec![serde_json::from_value(json!({
                        "event_name":"Value", "indexed_1":[format!("{:#x}", detail.indexed)],
                    }))
                    .unwrap()]),
                }),
                cached_provider: rpc.provider.clone(),
                block_clock: BlockClock::new(None, None, rpc.provider.clone()),
                decoder: noop_decoder(),
                start_block: Some(U64::from(detail.start)),
                end_block: None,
                disable_logs_bloom_checks: true,
            })
            .collect::<Vec<_>>();
        let applied = Arc::new(Mutex::new(vec![]));
        let received = applied.clone();
        let mut registry = EventCallbackRegistry::new();
        registry.register_event(EventCallbackRegistryInformation {
            id: "dependency-value".into(),
            indexer_name: "DependencyLive".into(),
            topic_id: TOPIC,
            event_name: "Value".into(),
            index_event_in_order: true,
            contract: ContractInformation {
                name: "Contract".into(),
                details: networks.clone(),
                abi: "unused.json".to_string().into(),
                reorg_safe_distance: None,
            },
            callback: Arc::new(move |events| {
                received.lock().unwrap().extend(events.into_iter().map(|event| Applied {
                    cursor: event.found_in_request.cursor_key,
                    address: event.log.address(),
                    indexed: event.log.topics()[1],
                    block: event.tx_information.block_number,
                }));
                Box::pin(async { Ok(()) })
            }),
            tables: Arc::new(vec![]),
            reorg_sender: None,
            streams_clients: Arc::new(None),
            providers: Arc::new(HashMap::new()),
            constants: Arc::new(HashMap::new()),
            multicall_addresses: Arc::new(HashMap::new()),
        });
        let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
        let fixture = Self {
            directory: tempfile::tempdir().unwrap(),
            rpc,
            networks,
            registry: registry.complete(),
            progress,
            streams: serde_json::from_value(json!({"webhooks":[]})).unwrap(),
            applied,
        };
        for network in &fixture.networks {
            let path = fixture.cursor_path(network);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "100").unwrap();
        }
        fixture
    }

    fn cursor_path(&self, network: &NetworkContract) -> PathBuf {
        self.directory
            .path()
            .join(self.streams.get_streams_last_synced_block_path())
            .join("Contract/last-synced-blocks")
            .join(format!("contract-ethereum-value-{}.txt", network.cursor_key()))
    }

    fn cursors_at(&self, number: u64) -> bool {
        self.networks.iter().all(|network| {
            std::fs::read_to_string(self.cursor_path(network)).unwrap() == number.to_string()
        })
    }

    async fn start(&self, reverse: bool) -> (JoinHandle<()>, CancellationToken) {
        let cancel = CancellationToken::new();
        let mut events = vec![];
        for network in &self.networks {
            let cursor = get_last_synced_block_number(SyncConfig {
                project_path: self.directory.path(),
                postgres: &None,
                clickhouse: &None,
                csv_details: &None,
                stream_details: &Some(&self.streams),
                contract_csv_enabled: false,
                indexer_name: "DependencyLive",
                contract_name: "Contract",
                event_name: "Value",
                network: NETWORK,
                cursor_key: &network.cursor_key(),
            })
            .await
            .unwrap()
            .expect("seeded subscription checkpoint");
            let config = Arc::new(EventProcessingConfig::from(ContractEventProcessingConfig {
                id: "dependency-value".into(),
                project_path: self.directory.path().to_path_buf(),
                indexer_name: "DependencyLive".into(),
                contract_name: "Contract".into(),
                topic_id: TOPIC,
                event_name: "Value".into(),
                config: Default::default(),
                network_contract: Arc::new(network.clone()),
                timestamps: false,
                start_block: network.start_block.unwrap(),
                end_block: cursor,
                registry: self.registry.clone(),
                progress: self.progress.clone(),
                postgres: None,
                clickhouse: None,
                csv_details: None,
                stream_last_synced_block_file_path: Some(
                    self.streams.get_streams_last_synced_block_path(),
                ),
                index_event_in_order: true,
                live_indexing: true,
                indexing_distance_from_head: U64::ZERO,
                cancel_token: cancel.clone(),
                tables: Arc::new(vec![]),
                reorg_sender: None,
                streams_clients: Arc::new(None),
                contract_abi: None,
                providers: Arc::new(HashMap::new()),
                constants: Arc::new(HashMap::new()),
                multicall_addresses: Arc::new(HashMap::new()),
            }));
            let filter = config.to_event_filter().unwrap();
            events.push((config, filter));
        }
        if reverse {
            events.reverse();
        }
        let task = tokio::spawn(live_indexing_for_contract_event_dependencies(
            EventDependenciesIndexingConfig {
                network: NETWORK.into(),
                cached_provider: self.rpc.provider.clone(),
                events,
                reorg_coordinator: None,
                trace_registry: None,
            },
        ));
        (task, cancel)
    }

    fn expected(&self, through: u64) -> Vec<Applied> {
        let mut expected = vec![];
        for network in &self.networks {
            let IndexingContractSetup::Address(details) = &network.indexing_contract_setup else {
                unreachable!()
            };
            let ValueOrArray::Value(address) = details.address else { unreachable!() };
            let indexed = details.indexed_filters.as_ref().unwrap()[0].indexed_1.as_ref().unwrap()
                [0]
            .parse()
            .unwrap();
            for block in 101..=through {
                expected.push(Applied { cursor: network.cursor_key(), address, indexed, block });
            }
        }
        expected.sort();
        expected
    }

    async fn run_through(&self, through: u64, reverse: bool) {
        self.rpc.state.head.store(through, Ordering::SeqCst);
        let expected = self.expected(through);
        let (task, cancel) = self.start(reverse).await;
        let completed = wait_until(|| {
            self.cursors_at(through)
                || self.applied.lock().unwrap().iter().any(|event| !expected.contains(event))
        })
        .await;
        cancel.cancel();
        timeout(Duration::from_secs(5), task).await.expect("dependency loop stops").unwrap();
        let mut actual = self.applied.lock().unwrap().clone();
        actual.sort();
        assert_eq!(actual, expected, "each detail must receive its own logs and cursor key");
        assert!(
            completed && self.cursors_at(through),
            "every subscription must checkpoint its own range"
        );
    }
}

async fn wait_until(predicate: impl Fn() -> bool) -> bool {
    timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

async fn assert_independent_details(details: Vec<Detail>) {
    for reverse in [false, true] {
        let fixture = Fixture::new(details.clone()).await;
        fixture.run_through(101, reverse).await;
        // Restart from the durable per-detail cursors, with both old and new logs still in RPC.
        fixture.run_through(102, reverse).await;
        let requests = fixture.rpc.state.log_requests.lock().unwrap();
        assert_eq!(requests.len(), 4, "two details, one request per live block");
        assert!(requests[..2].iter().all(|request| hex_number(&request["fromBlock"]) == 101));
        assert!(requests[2..].iter().all(|request| hex_number(&request["fromBlock"]) == 102));
    }
}

#[tokio::test]
async fn dependency_live_keeps_address_details_independent_after_restart() {
    assert_independent_details(vec![
        Detail { address: Address::repeat_byte(1), indexed: B256::repeat_byte(3), start: 100 },
        Detail { address: Address::repeat_byte(2), indexed: B256::repeat_byte(3), start: 100 },
    ])
    .await;
}

#[tokio::test]
async fn dependency_live_keeps_indexed_details_independent_after_restart() {
    assert_independent_details(vec![
        Detail { address: Address::repeat_byte(1), indexed: B256::repeat_byte(3), start: 100 },
        Detail { address: Address::repeat_byte(1), indexed: B256::repeat_byte(4), start: 100 },
    ])
    .await;
}

#[tokio::test]
async fn dependency_live_keeps_start_coverage_independent_after_restart() {
    assert_independent_details(vec![
        Detail { address: Address::repeat_byte(1), indexed: B256::repeat_byte(3), start: 99 },
        Detail { address: Address::repeat_byte(1), indexed: B256::repeat_byte(3), start: 100 },
    ])
    .await;
}
