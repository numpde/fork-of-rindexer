//! Historical state belongs to a block identity, including replies that arrive after recovery.
use super::*;
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify},
    task::JoinHandle,
};

pub(crate) const ORIGINAL: B256 = B256::repeat_byte(1);
pub(crate) const REPLACEMENT: B256 = B256::repeat_byte(2);
pub(crate) const CALL: &str = "$call($rindexer_contract_address, \"state()\")";

#[derive(Default)]
struct RpcState {
    replacement: AtomicBool,
    hold_original: AtomicBool,
    original_started: Notify,
    release_original: Notify,
    requests: Mutex<Vec<Value>>,
}

pub(crate) struct HistoricalRpc {
    pub(crate) url: String,
    state: Arc<RpcState>,
    server: JoinHandle<()>,
}

impl HistoricalRpc {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(RpcState::default());
        let shared = state.clone();
        let app = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let state = shared.clone();
            async move {
                if request["method"] == "eth_chainId" {
                    return Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x1"}));
                }
                assert_eq!(request["method"], "eth_call");
                state.requests.lock().await.push(request.clone());
                let replaced = state.replacement.load(Ordering::SeqCst);
                let hash = request["params"][1].get("blockHash").and_then(Value::as_str);
                if let Some(hash) = hash {
                    let canonical = if replaced { REPLACEMENT } else { ORIGINAL };
                    if hash != format!("{canonical:#x}") {
                        return Json(json!({"jsonrpc":"2.0", "id":request["id"], "error":{"code":-32000,"message":"block is not canonical"}}));
                    }
                }
                // Snapshot state when the node executes the call, before delaying its reply.
                let value = U256::from(if replaced { 20 } else { 10 });
                if !replaced && state.hold_original.swap(false, Ordering::SeqCst) {
                    state.original_started.notify_one();
                    state.release_original.notified().await;
                }
                let word = value.to_be_bytes::<32>().to_vec();
                let is_multicall = request["params"][0]["data"].as_str().unwrap().starts_with("0x82ad56cb");
                let bytes = if is_multicall {
                    DynSolValue::Array(vec![DynSolValue::Tuple(vec![DynSolValue::Bool(true), DynSolValue::Bytes(word)])]).abi_encode_params()
                } else { word };
                Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":format!("0x{}", hex::encode(bytes))}))
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { url, state, server }
    }

    pub(crate) fn replace(&self) {
        self.state.replacement.store(true, Ordering::SeqCst);
    }
    pub(crate) async fn request_count(&self) -> usize {
        self.state.requests.lock().await.len()
    }
    pub(crate) async fn assert_hash_qualified(&self) {
        for request in self.state.requests.lock().await.iter() {
            assert_eq!(request["params"][1]["requireCanonical"], true);
            assert!(request["params"][1]["blockHash"].is_string());
        }
    }
    pub(crate) async fn stop(self) {
        self.server.abort();
        assert!(self.server.await.unwrap_err().is_cancelled());
    }
}

pub(crate) fn header(hash: B256, timestamp: u64) -> alloy::network::AnyRpcBlock {
    use alloy::{
        network::{AnyHeader, AnyRpcHeader},
        rpc::types::{Block, BlockTransactions},
    };
    alloy::network::AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader {
                    number: 100,
                    timestamp,
                    parent_hash: B256::repeat_byte(99),
                    ..Default::default()
                }
                .seal(hash),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    )
}

fn metadata(hash: B256) -> TxMetadata {
    TxMetadata {
        block_number: 100,
        block_timestamp: None,
        tx_hash: B256::ZERO,
        block_hash: hash,
        contract_address: Address::ZERO,
        log_index: U256::ZERO,
        tx_index: 0,
    }
}

#[tokio::test]
async fn late_original_prefetch_cannot_overwrite_replacement_state() {
    use crate::{
        indexer::reorg::{
            BlockChainWindow, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
        },
        provider::{create_client, mock::MockChainProvider},
    };
    let rpc = HistoricalRpc::start().await;
    rpc.state.hold_original.store(true, Ordering::SeqCst);
    let provider =
        create_client(&rpc.url, 1, None, None, None, Default::default(), None, None).await.unwrap();
    let table = TableRuntime::new(serde_json::from_value(json!({
        "name":"totals", "global":true, "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value", "operations":[{"type":"upsert", "set":[{"column":"amount","action":"set","value":CALL}]}]}]
    })).unwrap(), "Cache", "Contract");
    let original_provider: Arc<dyn ChainProvider> = provider.clone();
    let prefetch = tokio::spawn(async move {
        prefetch_view_calls(
            &[table],
            "Value",
            &[(vec![], "cache_race".to_string(), metadata(ORIGINAL))],
            &[("cache_race".to_string(), original_provider)].into(),
            &Constants::default(),
            &HashMap::new(),
        )
        .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), rpc.state.original_started.notified())
        .await
        .unwrap();
    let mut window = BlockChainWindow::try_new(8).unwrap();
    window.insert(100, ORIGINAL, B256::repeat_byte(99));
    let mut coordinator = ReorgCoordinator::new(
        "cache_race".to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(None, None)),
        Arc::new(MockChainProvider::new(1).with_blocks(vec![header(REPLACEMENT, 20)])),
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: None,
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        )
        .await
        .unwrap();
    rpc.replace();
    let call = parse_view_call(CALL).unwrap();
    let replacement = metadata(REPLACEMENT);
    let constants = Constants::default();
    let read =
        || execute_view_call(&call, &[], &replacement, provider.as_ref(), "cache_race", &constants);
    assert_eq!(read().await, Some(DynSolValue::Uint(U256::from(20), 256)));
    rpc.state.release_original.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(10), prefetch).await.unwrap().unwrap();
    assert_eq!(
        read().await,
        Some(DynSolValue::Uint(U256::from(20), 256)),
        "late A reply cannot poison B"
    );
    assert_eq!(rpc.request_count().await, 2, "identical replacement blocks must hit cache");
    rpc.assert_hash_qualified().await;
    rpc.stop().await;
}

#[tokio::test]
async fn fallback_timestamps_belong_to_the_event_block_hash() {
    use crate::provider::mock::MockChainProvider;
    for (hash, expected) in [(ORIGINAL, 10), (REPLACEMENT, 20)] {
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_blocks(vec![header(hash, expected)]));
        let meta = metadata(hash);
        prefetch_block_timestamps(
            &[(vec![], "timestamp_fork".to_string(), meta.clone())],
            &[("timestamp_fork".to_string(), provider.clone())].into(),
            true,
        )
        .await;
        let actual = extract_value_from_event_async(
            "$rindexer_block_timestamp",
            &[],
            &meta,
            &ColumnType::Uint256,
            Some(provider.as_ref()),
            "timestamp_fork",
            &Constants::default(),
        )
        .await;
        assert!(
            matches!(actual, Some(EthereumSqlTypeWrapper::U256Numeric(value)) if value == U256::from(expected))
        );
    }
}

#[tokio::test]
async fn timestamp_response_for_another_hash_cannot_satisfy_event_metadata() {
    use crate::provider::mock::MockChainProvider;
    let provider: Arc<dyn ChainProvider> =
        Arc::new(MockChainProvider::new(1).with_blocks(vec![header(REPLACEMENT, 20)]));
    let meta = metadata(ORIGINAL);
    prefetch_block_timestamps(
        &[(vec![], "timestamp_mismatch".to_string(), meta.clone())],
        &[("timestamp_mismatch".to_string(), provider.clone())].into(),
        true,
    )
    .await;
    assert!(extract_value_from_event_async(
        "$rindexer_block_timestamp",
        &[],
        &meta,
        &ColumnType::Uint256,
        Some(provider.as_ref()),
        "timestamp_mismatch",
        &Constants::default()
    )
    .await
    .is_none());
}
