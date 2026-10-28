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
struct AddressRpc {
    provider: Arc<JsonRpcCachedProvider>,
    log_requests: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for AddressRpc {
    fn drop(&mut self) {
        self.server.abort();
    }
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

async fn address_rpc(chunk_size: usize) -> AddressRpc {
    use axum::{routing::post, Json, Router};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let log_requests = Arc::new(AtomicUsize::new(0));
    let observed = log_requests.clone();
    // Header proofs must not consume the log-ordering script. Route by method
    // and height so batching and ancestry boundaries remain production choices.
    let app = Router::new().route(
        "/",
        post(move |Json(request): Json<serde_json::Value>| {
            let observed = observed.clone();
            async move {
                let batch = request.is_array();
                let requests = if let serde_json::Value::Array(requests) = request {
                    requests
                } else {
                    vec![request]
                };
                let replies = requests
                    .into_iter()
                    .map(|request| {
                        let result = match request["method"].as_str().unwrap() {
                            "eth_chainId" => serde_json::json!("0x1"),
                            "eth_getBlockByNumber" => {
                                let number = u64::from_str_radix(
                                    request["params"][0].as_str().unwrap().trim_start_matches("0x"),
                                    16,
                                )
                                .unwrap();
                                serde_json::to_value(header(number)).unwrap()
                            }
                            "eth_getLogs" => {
                                observed.fetch_add(1, Ordering::SeqCst);
                                let mut logs = if chunk_size > 1 {
                                    vec![
                                        log(500, 0, 0),
                                        log(100, 1, 2),
                                        log(100, 0, 1),
                                        log(100, 0, 0),
                                        log(200, 0, 0),
                                    ]
                                } else if request["params"][0]["address"]
                                    .to_string()
                                    .contains(&Address::repeat_byte(1).to_string())
                                {
                                    vec![log(100, 0, 1), log(500, 0, 0)]
                                } else {
                                    vec![log(100, 0, 0), log(200, 0, 0)]
                                };
                                let bound = |name: &str| {
                                    u64::from_str_radix(
                                        request["params"][0][name]
                                            .as_str()
                                            .unwrap()
                                            .trim_start_matches("0x"),
                                        16,
                                    )
                                    .unwrap()
                                };
                                logs.retain(|log| {
                                    log.block_number.is_some_and(|number| {
                                        number >= bound("fromBlock") && number <= bound("toBlock")
                                    })
                                });
                                serde_json::to_value(logs).unwrap()
                            }
                            method => panic!("unexpected ordering-fixture RPC method {method}"),
                        };
                        serde_json::json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    })
                    .collect::<Vec<_>>();
                Json(if batch {
                    serde_json::Value::Array(replies)
                } else {
                    replies.into_iter().next().unwrap()
                })
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let provider = crate::provider::create_client(
        &url,
        1,
        None,
        None,
        None,
        Default::default(),
        Some(AddressFiltering::MaxAddressPerGetLogsRequest(chunk_size)),
        None,
    )
    .await
    .unwrap();
    AddressRpc { provider, log_requests, server }
}

#[tokio::test]
async fn address_query_results_are_ordered_for_one_and_multiple_chunks() {
    for chunk_size in [1, 32] {
        let rpc = address_rpc(chunk_size).await;
        let logs = rpc.provider.get_logs(&filter()).await.unwrap();
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
    let rpc = address_rpc(1).await;
    let result = fetch_historic_logs_stream(
        false,
        BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
        rpc.provider.as_ref(),
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
    assert_eq!(rpc.log_requests.load(Ordering::SeqCst), 2);
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
    let rpc = address_rpc(1).await;
    let config = config(rpc.provider.clone(), Arc::new(MockChainProvider::new(1)), false).await;
    let batch = run_worker(config, filter()).await;
    assert!(batch.is_final);
    assert_eq!(rpc.log_requests.load(Ordering::SeqCst), 2);
    assert_eq!(batch.results.len(), 1);
    let logs = &batch.results[0].as_ref().unwrap().logs;
    assert_eq!(
        logs.iter().map(|log| log.block_number.unwrap()).collect::<Vec<_>>(),
        vec![100, 100, 200, 500]
    );
}

async fn missing_timestamp_config() -> Arc<EventProcessingConfig> {
    config(
        Arc::new(
            MockChainProvider::new(1)
                .with_block_number(101)
                .with_blocks(vec![header(100), header(101)])
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
