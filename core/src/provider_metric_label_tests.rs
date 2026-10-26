//! Configured aliases of one chain must remain separate across RPC and indexing metrics.
use super::*;
use crate::metrics::{definitions::*, indexing};
use alloy::{
    network::{AnyHeader, AnyRpcHeader},
    primitives::Bloom,
    rpc::types::BlockTransactions,
};
use serde_json::{json, Value};

async fn rpc_server() -> (mockito::ServerGuard, mockito::Mock) {
    let mut server = mockito::Server::new_async().await;
    let block = AnyRpcBlock::new(
        alloy::rpc::types::Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader { number: 10, ..Default::default() }.seal(B256::repeat_byte(10)),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    );
    let mock = server.mock("POST", "/").with_header("content-type", "application/json")
        .with_body_from_request(move |request| {
            let request: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            let id = request["id"].clone();
            if request["method"] == "eth_getLogs" && request["params"][0]["fromBlock"] == "0x2" {
                return json!({"jsonrpc":"2.0","id":id,"error":{"code":4444,"message":"Pruned history unavailable"}}).to_string().into_bytes();
            }
            let result = match request["method"].as_str().unwrap() {
                "eth_chainId" => json!("0xa4b1"),
                "eth_getBlockByNumber" => serde_json::to_value(&block).unwrap(),
                "eth_getLogs" => json!([]),
                method => panic!("unexpected RPC method {method}"),
            };
            json!({"jsonrpc":"2.0","id":id,"result":result}).to_string().into_bytes()
        }).create_async().await;
    (server, mock)
}

async fn providers(url: &str, shared: bool) -> Vec<CreateNetworkProvider> {
    let networks = ["42161", "rollup_archive"].map(|name| {
        json!({
            "name":name,"chain_id":42161,"rpc":url,"block_poll_frequency":"10000","disable_logs_bloom_checks":true,
            "shared_tip_logs":{"enabled":shared}
        })
    });
    let manifest: Manifest = serde_json::from_value(json!({
        "name":"labels","project_type":"no-code","networks":networks,"contracts":[]
    }))
    .unwrap();
    CreateNetworkProvider::create(&manifest).await.unwrap()
}

fn filter(block: u64) -> RindexerEventFilter {
    RindexerEventFilter::empty_for_test()
        .set_from_block(U64::from(block))
        .set_to_block(U64::from(block))
}

#[tokio::test]
async fn provider_metric_labels_follow_configured_names_for_success_error_and_cached_heads() {
    let (server, _requests) = rpc_server().await;
    let providers = providers(&server.url(), false).await;
    for provider in &providers {
        let name = &provider.network_name;
        provider.client.get_logs(&filter(1)).await.unwrap();
        assert!(provider.client.get_logs(&filter(2)).await.is_err());
        provider.client.get_latest_block().await.unwrap();
        provider.client.get_latest_block().await.unwrap();
        indexing::record_events_indexed(name, "Account", "Value", 1, 1, Some(10));
    }
    for provider in &providers {
        let name = provider.network_name.as_str();
        for status in ["success", "error"] {
            assert_eq!(
                RPC_REQUESTS_TOTAL.with_label_values(&[name, "eth_getLogs", status]).get(),
                1.0,
                "RPC counts must use the configured alias {name}"
            );
        }
        assert_eq!(
            RPC_REQUEST_DURATION.with_label_values(&[name, "eth_getLogs"]).get_sample_count(),
            2
        );
        assert_eq!(
            RPC_REQUESTS_TOTAL.with_label_values(&[name, "eth_getBlockByNumber", "success"]).get(),
            1.0,
            "a cached head must not add an RPC sample"
        );
        assert_eq!(
            RPC_REQUEST_DURATION
                .with_label_values(&[name, "eth_getBlockByNumber"])
                .get_sample_count(),
            1
        );
        assert_eq!(
            EVENTS_PROCESSED_TOTAL.with_label_values(&[name, "Account", "Value"]).get(),
            1.0
        );
    }
}

#[tokio::test]
async fn provider_metric_labels_include_shared_tip_source_and_cache_for_each_alias() {
    let (server, _requests) = rpc_server().await;
    let providers = providers(&server.url(), true).await;
    for provider in &providers {
        let tip = provider.client.shared_tip_logs().expect("enabled shared-tip cache");
        tip.observe_head(10, B256::repeat_byte(10), B256::ZERO, Bloom::repeat_byte(0xff));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = tip.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if matches!(
                    tip.lookup(10, 10, B256::repeat_byte(10)),
                    crate::indexer::tip_logs::TipLookup::ServeTip(_)
                ) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("the real shared source must complete its RPC request");
    }
    for provider in &providers {
        let name = provider.network_name.as_str();
        let tip = provider.client.shared_tip_logs().unwrap();
        assert_eq!(tip.network(), name, "cache identity must match its configured network");
        assert_eq!(
            RPC_REQUESTS_TOTAL.with_label_values(&[name, "eth_getLogs", "success"]).get(),
            1.0
        );
        assert_eq!(
            RPC_REQUEST_DURATION.with_label_values(&[name, "eth_getLogs"]).get_sample_count(),
            1
        );
        assert_eq!(SHARED_TIP_LOGS_REQUESTS_TOTAL.with_label_values(&[name, "success"]).get(), 1.0);
        assert_eq!(SHARED_TIP_LOGS_CACHE_BLOCKS.with_label_values(&[name]).get(), 1.0);
    }
}

#[tokio::test]
async fn provider_metric_labels_legacy_constructors_keep_chain_derived_defaults() {
    let (server, _requests) = rpc_server().await;
    let old = create_client(&server.url(), 42161, None, None, None, HeaderMap::new(), None, None)
        .await
        .unwrap();
    let shared = create_client_with_shared_tip_logs(
        &server.url(),
        42161,
        None,
        None,
        None,
        HeaderMap::new(),
        None,
        None,
        Some(SharedTipLogsSettings {
            empty_retry_deadline_ms: 1,
            cache_blocks: 2,
            bloom_trusted: false,
        }),
    )
    .await
    .unwrap();
    old.get_logs(&filter(1)).await.unwrap();
    shared.get_logs(&filter(1)).await.unwrap();
    assert_eq!(shared.shared_tip_logs().unwrap().network(), "arbitrum");
    assert_eq!(
        RPC_REQUESTS_TOTAL.with_label_values(&["arbitrum", "eth_getLogs", "success"]).get(),
        2.0
    );
    assert_eq!(
        RPC_REQUEST_DURATION.with_label_values(&["arbitrum", "eth_getLogs"]).get_sample_count(),
        2
    );
}
