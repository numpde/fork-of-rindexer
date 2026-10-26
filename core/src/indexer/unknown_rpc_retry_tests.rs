//! Unknown RPC failures must be able to reduce an inclusive range to one block.
use super::*;
use crate::provider::{create_client_for_network, mock::MockChainProvider, JsonRpcCachedProvider};
use alloy::primitives::{Bytes, LogData};
use serde_json::{json, Value};
use std::sync::Mutex as StdMutex;

fn unknown_error() -> ProviderError {
    ProviderError::RequestFailed(alloy::transports::RpcError::ErrorResp(
        alloy::rpc::json_rpc::ErrorPayload {
            code: -32001,
            message: "Unable to complete request at this time.".into(),
            data: None,
        },
    ))
}

#[tokio::test]
async fn unknown_rpc_retry_shrinks_every_multiblock_range_including_two_blocks() {
    for error in [
        unknown_error(),
        ProviderError::CustomError("Internal server error. Forwarder error: 1000.".into()),
    ] {
        for (from, to) in [(0, 1), (100, 101), (u64::MAX - 1, u64::MAX), (100, 102), (100, 10100)] {
            for cap in [None, Some(U64::ZERO), Some(U64::from(1))] {
                let retry = retry_with_block_range(
                    "unknown-retry",
                    &error,
                    U64::from(from),
                    U64::from(to),
                    cap,
                )
                .await
                .expect("unknown failures need a smaller retry interval");
                assert_eq!(retry.from, U64::from(from));
                assert!(
                    retry.to >= retry.from && retry.to < U64::from(to),
                    "a failed multiblock range must shrink: {from}-{to} became {}-{}",
                    retry.from,
                    retry.to
                );
            }
        }
    }
}

struct SingleBlockRpc {
    _server: mockito::ServerGuard,
    _responses: mockito::Mock,
    provider: Arc<JsonRpcCachedProvider>,
    requests: Arc<StdMutex<Vec<(u64, u64)>>>,
}

impl SingleBlockRpc {
    async fn new() -> Self {
        let mut server = mockito::Server::new_async().await;
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let recorded = requests.clone();
        let responses = server.mock("POST", "/").with_header("content-type", "application/json")
            .with_body_from_request(move |request| {
                let request: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
                let id = request["id"].clone();
                if request["method"] == "eth_chainId" {
                    return json!({"jsonrpc":"2.0","id":id,"result":"0x1"}).to_string().into_bytes();
                }
                assert_eq!(request["method"], "eth_getLogs");
                let block = |key| u64::from_str_radix(request["params"][0][key].as_str().unwrap().trim_start_matches("0x"),16).unwrap();
                let (from,to) = (block("fromBlock"),block("toBlock"));
                recorded.lock().unwrap().push((from,to));
                if from != to {
                    return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32001,"message":"Unable to complete request at this time."}}).to_string().into_bytes();
                }
                let log = Log {
                    inner: alloy::primitives::Log { address: Address::ZERO, data: LogData::new_unchecked(vec![B256::ZERO],Bytes::new()) },
                    block_number: Some(from), block_hash: Some(B256::repeat_byte(from as u8)),
                    transaction_hash: Some(B256::repeat_byte(from as u8)), transaction_index: Some(0), log_index: Some(0),
                    ..Default::default()
                };
                json!({"jsonrpc":"2.0","id":id,"result":[log]}).to_string().into_bytes()
            }).create_async().await;
        let provider = create_client_for_network(
            "unknown-retry",
            &server.url(),
            1,
            None,
            None,
            None,
            alloy::transports::http::reqwest::header::HeaderMap::new(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        Self { _server: server, _responses: responses, provider, requests }
    }
}

async fn complete_single_block_rpc_history(parallel: bool) {
    let rpc = SingleBlockRpc::new().await;
    let clock = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
    let (tx, mut rx) = mpsc::channel(4);
    let mut filter = RindexerEventFilter::empty_for_test()
        .set_from_block(U64::from(100))
        .set_to_block(U64::from(103));
    let mut cap = None;
    let mut batches = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..12 {
            if filter.from_block() > U64::from(103) {
                break;
            }
            let next = if parallel {
                let (result, next, _) = fetch_logs_once(
                    false,
                    &clock,
                    rpc.provider.as_ref(),
                    filter.clone(),
                    cap,
                    U64::from(103),
                    "unknown-retry",
                    0,
                )
                .await;
                if let Some(result) = result {
                    batches.push(result.unwrap());
                }
                next
            } else {
                let next = fetch_historic_logs_stream(
                    false,
                    clock.clone(),
                    rpc.provider.as_ref(),
                    &tx,
                    &B256::ZERO,
                    filter.clone(),
                    cap,
                    U64::from(103),
                    "unknown-retry",
                    0,
                )
                .await;
                while let Ok(result) = rx.try_recv() {
                    batches.push(result.unwrap());
                }
                next
            };
            let Some(next) = next else {
                break;
            };
            filter = next.next;
            cap = next.max_block_range_limitation;
        }
    })
    .await
    .expect("retrying a recovered smaller interval must complete without restarting");
    let completed = batches
        .iter()
        .map(|batch| (batch.from_block.to::<u64>(), batch.to_block.to::<u64>()))
        .collect::<Vec<_>>();
    assert_eq!(
        completed,
        vec![(100, 100), (101, 101), (102, 102), (103, 103)],
        "only successfully fetched blocks may be certified to the consumer"
    );
    let blocks = batches
        .iter()
        .flat_map(|batch| batch.logs.iter().map(|log| log.block_number.unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(blocks, vec![100, 101, 102, 103], "recover each event once without gaps");
    let requests = rpc.requests.lock().unwrap();
    assert!(requests.iter().all(|(from, to)| 100 <= *from && from <= to && *to <= 103));
    for pair in requests.windows(2) {
        let [(from, to), (next_from, next_to)] = pair else { unreachable!() };
        if from == next_from && from < to {
            assert!(next_to < to, "a failed interval must not repeat unchanged: {pair:?}");
        }
    }
}

#[tokio::test]
async fn unknown_rpc_retry_sequential_history_recovers_all_events_at_single_block_size() {
    complete_single_block_rpc_history(false).await;
}

#[tokio::test]
async fn unknown_rpc_retry_parallel_fetch_recovers_all_events_at_single_block_size() {
    complete_single_block_rpc_history(true).await;
}

#[tokio::test(start_paused = true)]
async fn unknown_rpc_retry_single_block_failure_retains_existing_backoff() {
    let start = Instant::now();
    let retry = retry_with_block_range(
        "unknown-retry",
        &unknown_error(),
        U64::from(100),
        U64::from(100),
        None,
    )
    .await;
    assert!(
        retry.is_none(),
        "a one-block failure has no smaller hint; callers retry the same block"
    );
    assert!(start.elapsed() >= Duration::from_secs(1));
}
