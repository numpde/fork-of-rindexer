//! Startup must keep the schema it admitted before asynchronous external setup.
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::{
    dyn_abi::DynSolValue,
    primitives::{U256, U64},
};
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    blockclock::BlockClock,
    database::postgres::client::TEST_DATABASE_URL_LOCK,
    indexer::{no_code::setup_no_code, reorg::ReorgContext, IndexingEventsProgressState},
    manifest::core::Manifest,
    provider::mock::MockChainProvider,
    GraphqlOverrideSettings, IndexerNoCodeDetails, StartNoCodeDetails,
};

#[tokio::test]
async fn postgres_startup_keeps_admitted_schema_after_abi_file_disappears() {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"].as_array_mut().unwrap().push(json!({
        "name":"key16", "type":"uint256", "indexed":false
    }));
    let fixture = Fixture::with_table(
        definition,
        "Contract",
        json!({"name":"totals", "columns":[
            {"name":"key16","type":"uint256"},
            {"name":"amount32","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "where":{"key16":"$key16"},
                "set":[{"column":"amount32","action":"add","value":"$value"}]}]}]}),
    )
    .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_url = format!("http://{}", listener.local_addr().unwrap());
    let abi_path = fixture._directory.path().join("abi.json");
    let removed = Arc::new(AtomicBool::new(false));
    let removed_by_rpc = removed.clone();
    let app = Router::new().route(
        "/",
        post(move |Json(request): Json<Value>| {
            let abi_path = abi_path.clone();
            let removed = removed_by_rpc.clone();
            async move {
                // Provider connection is an actual asynchronous startup boundary,
                // after admission and SQL creation but before callback registration.
                // Removing the source file here makes a repeated ABI read observable.
                assert_eq!(request["method"], "eth_chainId");
                std::fs::remove_file(&abi_path).unwrap();
                removed.store(true, Ordering::SeqCst);
                Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x1"}))
            }
        }),
    );
    let rpc_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut manifest: Manifest = serde_json::from_value(json!({
        "name":fixture.params.indexer_name, "project_type":"no-code",
        "networks":[{"name":"ethereum","chain_id":1,"rpc":rpc_url,
            "reorg_handling":{"enabled":true,"window_size":16}}],
        "contracts":fixture.indexer.contracts,
        "storage":{"postgres":{"enabled":true,"drop_each_run":true}}
    }))
    .unwrap();
    manifest.contracts[0].details[0].start_block = Some(U64::from(99));
    manifest.contracts[0].details[0].end_block = Some(U64::from(100));
    let manifest_path = fixture._directory.path().join("rindexer.yaml");
    std::fs::write(&manifest_path, serde_yaml::to_string(&manifest).unwrap()).unwrap();

    let setup = {
        let _guard = TEST_DATABASE_URL_LOCK.lock().await;
        let original = std::env::var_os("DATABASE_URL");
        std::env::set_var("DATABASE_URL", fixture.url.as_str());
        let result = setup_no_code(StartNoCodeDetails {
            manifest_path: &manifest_path,
            indexing_details: IndexerNoCodeDetails { enabled: true },
            graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
            watch: false,
        })
        .await;
        match original {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
        result
    };
    rpc_task.abort();
    let _ = rpc_task.await;
    assert!(removed.load(Ordering::SeqCst), "the startup boundary must remove the ABI");
    let mut indexing = match setup {
        Ok(start) => start.indexing_details.unwrap(),
        Err(error) => {
            fixture.finish().await;
            panic!("startup must use its admitted ABI after provider setup: {error}");
        }
    };

    // Only chain transport changes for the rollback exercise. Event columns,
    // table types, and rollback mappings must all come from startup's schema.
    let blocks = (99_u8..=105)
        .map(|number| {
            let hash = if number == 99 { 99 } else { number + 1 };
            let parent = if number == 100 { 99 } else { number };
            let mut header = crate::indexer::tables::view_call_cache_tests::header(
                B256::repeat_byte(hash),
                1000 + u64::from(number),
            );
            header.header.inner.number = u64::from(number);
            header.header.inner.parent_hash = B256::repeat_byte(parent);
            header
        })
        .collect();
    let provider = Arc::new(MockChainProvider::new(1).with_block_number(105).with_blocks(blocks));
    for event in &mut indexing.registry.events {
        for detail in &mut event.contract.details {
            detail.cached_provider = provider.clone();
            detail.block_clock = BlockClock::new(None, None, provider.clone());
        }
    }
    let registry = indexing.registry.complete();
    let trace_registry = indexing.trace_registry.complete();
    let callback = registry.events[0].callback.clone();
    for (block, value) in [(99, 10), (100, 20)] {
        let mut event = super::journal_recovery_tests::at(
            &fixture,
            "ethereum",
            block,
            B256::repeat_byte(block as u8),
            value,
        );
        event.log.inner.data = alloy::primitives::LogData::new_unchecked(
            event.log.topics().to_vec(),
            DynSolValue::Tuple(vec![
                DynSolValue::Uint(U256::from(value), 256),
                DynSolValue::Uint(U256::from(42), 256),
            ])
            .abi_encode_params()
            .into(),
        );
        callback(vec![event]).await.unwrap();
    }
    let table = &registry.events[0].tables[0].full_table_name;
    let before: String = fixture
        .client
        .query_one(&format!("SELECT amount_32::text FROM {table} WHERE key_16=42"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, "30");

    let cancel = CancellationToken::new();
    cancel.cancel();
    let (tasks, _, _, _, coordinators) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &manifest.to_indexer(),
        registry.clone(),
        trace_registry.clone(),
        &[],
        true,
        cancel,
        IndexingEventsProgressState::monitor(&[], &[], None).await,
    )
    .await
    .expect("recovery setup must share the admitted ABI instead of reopening its file");
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let mut coordinator = coordinators["ethereum"].lock().await;
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: Some(&registry),
                trace_registry: Some(&trace_registry),
            },
        )
        .await
        .unwrap();
    let after = fixture
        .client
        .query_one(
            &format!("SELECT t.key_16::text, t.amount_32::text, e.block_number::text FROM {table} t JOIN {} e ON e.key_16=t.key_16", fixture.params.sql_event_table_name),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        (after.get::<_, String>(0), after.get::<_, String>(1), after.get::<_, String>(2)),
        ("42".into(), "10".into(), "99".into())
    );
    drop(coordinator);
    drop(coordinators);
    drop(callback);
    drop(registry);
    drop(trace_registry);
    fixture.finish().await;
}
