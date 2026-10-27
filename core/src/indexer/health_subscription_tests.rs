use super::*;
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{noop_decoder, EventCallbackRegistry},
        contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    },
    manifest::contract::ReorgSafeDistance,
    provider::{mock::MockChainProvider, ChainProvider},
};
use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

fn registry(fixture: &Fixture, provider: Arc<dyn ChainProvider>) -> EventCallbackRegistry {
    let mut registry = super::super::permanent_failure_tests::registry(
        &fixture.params,
        Arc::new(|_| Box::pin(async { Ok(()) })),
    );
    registry.events[0].contract.reorg_safe_distance = Some(ReorgSafeDistance::Custom(2));
    for address in [1, 2] {
        registry.events[0].contract.details.push(NetworkContract {
            id: format!("health-{address}"),
            network: "ethereum".into(),
            indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                address: ValueOrArray::Value(Address::repeat_byte(address)),
                indexed_filters: None,
            }),
            cached_provider: provider.clone(),
            block_clock: BlockClock::new(None, None, provider.clone()),
            decoder: noop_decoder(),
            start_block: Some(U64::from(80)),
            end_block: None,
            disable_logs_bloom_checks: true,
        });
    }
    registry
}
fn cursor_table(fixture: &Fixture) -> String {
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    generate_internal_event_table_name(&schema, &fixture.params.event_info.name)
}
async fn response(
    fixture: &Fixture,
    registry: &EventCallbackRegistry,
) -> (StatusCode, serde_json::Value) {
    let manifest = serde_json::from_value(json!({"name":"Replay", "project_type":"no-code", "networks":[], "contracts":[], "storage":{"postgres":{"enabled":true}}})).unwrap();
    let router =
        crate::health::HealthServer::new(0, Arc::new(manifest), Some(fixture.client.clone()))
            .with_callback_failures(vec![registry.failure_state()])
            .router();
    let result = router
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = result.status();
    let body = to_bytes(result.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn postgres_health_reports_each_subscription_cursor_and_safe_head_lag() {
    let fixture = Fixture::new().await;
    let registry = registry(&fixture, Arc::new(MockChainProvider::new(1).with_block_number(100)));
    let keys = registry.events[0]
        .contract
        .details
        .iter()
        .map(NetworkContract::cursor_key)
        .collect::<Vec<_>>();
    let table = cursor_table(&fixture);
    for (key, position) in keys.iter().zip([95i64, 80]) {
        fixture.client.execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('ethereum',$1,$2::bigint)"), &[key, &position]).await.unwrap();
    }
    let (status, body) = response(&fixture, &registry).await;
    assert_eq!(status, StatusCode::OK, "catch-up is not a terminal process failure");
    let subscriptions = body["indexing"]["subscriptions"]
        .as_array()
        .expect("health must report configured subscription checkpoints, not only table existence");
    assert_eq!(subscriptions.len(), 2);
    for (key, position, lag) in [(keys[0].as_str(), 95, 3), (keys[1].as_str(), 80, 18)] {
        let state = subscriptions.iter().find(|value| value["cursor_key"] == key).unwrap();
        assert_eq!(state["last_synced_block"], position);
        assert_eq!(state["observed_head"], 100);
        assert_eq!(state["target_block"], 98, "reuse the configured safe-distance ceiling");
        assert_eq!(state["lag_blocks"], lag);
        assert_eq!(state["status"], "syncing");
    }
    fixture.client.execute(&format!("UPDATE rindexer_internal.{table} SET last_synced_block=98 WHERE network='ethereum' AND cursor_key=$1"), &[&keys[1]]).await.unwrap();
    let (_, updated) = response(&fixture, &registry).await;
    let subscriptions = updated["indexing"]["subscriptions"].as_array().unwrap();
    let state = subscriptions.iter().find(|value| value["cursor_key"] == keys[1]).unwrap();
    assert_eq!(state["lag_blocks"], 0, "health reads durable progress on each request");
    assert_eq!(state["status"], "live");
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_health_does_not_adopt_table_existence_or_legacy_cursor_as_progress() {
    let fixture = Fixture::new().await;
    let registry = registry(&fixture, Arc::new(MockChainProvider::new(1).with_block_number(100)));
    let (_, body) = response(&fixture, &registry).await;
    assert_eq!(body["services"]["sync"], "nodata", "an existing raw table and legacy network cursor do not prove either selected query completed");
    let subscriptions = body["indexing"]["subscriptions"].as_array().unwrap();
    assert_eq!(subscriptions.len(), 2);
    assert!(subscriptions.iter().all(|state| state["last_synced_block"].is_null()
        && state["lag_blocks"].is_null()
        && state["status"] == "waiting_for_checkpoint"));
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_health_reports_unavailable_rpc_head_instead_of_zero_lag() {
    let fixture = Fixture::new().await;
    let registry = registry(
        &fixture,
        Arc::new(MockChainProvider::new(1).with_block_number_error("scripted unavailable head")),
    );
    let (status, body) = response(&fixture, &registry).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["services"]["sync"], "unhealthy");
    let subscriptions = body["indexing"]["subscriptions"].as_array().unwrap();
    assert!(subscriptions.iter().all(|state| state["observed_head"].is_null()
        && state["lag_blocks"].is_null()
        && state["status"] == "unavailable"));
    fixture.finish().await;
}
