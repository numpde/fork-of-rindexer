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
    indexer::{
        no_code::{process_events_prepared, setup_no_code, setup_no_code_prepared},
        reorg::ReorgContext,
        IndexingEventsProgressState,
    },
    manifest::core::Manifest,
    provider::mock::MockChainProvider,
    GraphqlOverrideSettings, IndexerNoCodeDetails, PreparedProject, StartNoCodeDetails,
};

#[tokio::test]
async fn factory_callbacks_keep_prepared_abi_after_files_disappear() {
    let directory = tempfile::tempdir().unwrap();
    let factory_abi = json!([{"type":"event", "name":"Created", "anonymous":false,
        "inputs":[{"name":"child", "type":"address", "indexed":false}]}]);
    let child_abi = json!([{"type":"event", "name":"Value", "anonymous":false,
        "inputs":[{"name":"key16", "type":"uint16", "indexed":false}]}]);
    std::fs::write(directory.path().join("factory.json"), factory_abi.to_string()).unwrap();
    std::fs::write(directory.path().join("child.json"), child_abi.to_string()).unwrap();
    let manifest: Manifest = serde_json::from_value(json!({
        "name":"FrozenFactory", "project_type":"no-code",
        "networks":[{"name":"ethereum", "chain_id":1, "rpc":"http://unused.invalid"}],
        "contracts":[{"name":"Child", "abi":"child.json", "include_events":["Value"],
            "details":[{"network":"ethereum", "factory":{
                "name":"Factory", "abi":"factory.json", "event_name":"Created", "input_name":"child",
                "address":"0x0000000000000000000000000000000000000001"
            }}]}]
    })).unwrap();
    let prepared = PreparedProject::try_new(directory.path(), manifest).unwrap();
    std::fs::remove_file(directory.path().join("factory.json")).unwrap();
    std::fs::remove_file(directory.path().join("child.json")).unwrap();
    let providers = [crate::provider::CreateNetworkProvider {
        network_name: "ethereum".to_string(),
        disable_logs_bloom_checks: true,
        client: Arc::new(MockChainProvider::new(1)),
    }];
    let events = process_events_prepared(
        directory.path(),
        prepared.manifest(),
        prepared.schema(),
        None,
        None,
        &providers,
    )
    .await
    .expect("factory discovery and child callbacks must use the admitted ABIs");
    assert_eq!(events.len(), 2);
    let child = events.iter().find(|event| event.contract.name == "Child").unwrap();
    let factory = child.contract.details[0].indexing_contract_setup.factory_details().unwrap();
    assert_eq!(factory.event.name, "Created");
    assert!(events.iter().any(|event| {
        event.contract.name == factory.contract_name && event.event_name == "Created"
    }));
}

#[tokio::test]
async fn postgres_column_collisions_preserve_existing_data_before_drop_each_run() {
    let fixture = Fixture::new().await;
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let canary = format!("{schema}.admission_canary");
    fixture
        .client
        .batch_execute(&format!(
            "CREATE TABLE {canary} (value TEXT NOT NULL); \
             INSERT INTO {canary} (value) VALUES ('retained before admission');"
        ))
        .await
        .unwrap();
    let manifest_path = fixture._directory.path().join("rindexer.yaml");

    for destination_collision in [false, true] {
        let role = if destination_collision { "destination" } else { "source" };
        let mut definition = serde_json::to_value(&fixture.params.event).unwrap();
        let mut manifest = json!({
            "name":fixture.params.indexer_name,"project_type":"no-code",
            "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}],
            "contracts":fixture.indexer.contracts,
            "storage":{"postgres":{"enabled":true,"drop_each_run":true}}
        });
        for name in ["key16", "key_16"] {
            if destination_collision {
                manifest["contracts"][0]["tables"][0]["columns"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"name":name,"type":"uint256","default":"0"}));
            } else {
                definition["inputs"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"name":name,"type":"uint256","indexed":false}));
            }
        }
        std::fs::write(
            fixture._directory.path().join("abi.json"),
            serde_json::to_vec(&vec![definition]).unwrap(),
        )
        .unwrap();
        std::fs::write(&manifest_path, serde_yaml::to_string(&manifest).unwrap()).unwrap();

        let result = {
            let _guard = TEST_DATABASE_URL_LOCK.lock().await;
            let original = std::env::var_os("DATABASE_URL");
            std::env::set_var("DATABASE_URL", fixture.url.as_str());
            let result = setup_no_code(StartNoCodeDetails {
                manifest_path: &manifest_path,
                // Schema-only startup still performs drop_each_run and DDL;
                // no RPC connection is needed to exercise this public path.
                indexing_details: IndexerNoCodeDetails { enabled: false },
                graphql_details: GraphqlOverrideSettings { enabled: true, override_port: None },
                watch: false,
            })
            .await;
            match original {
                Some(value) => std::env::set_var("DATABASE_URL", value),
                None => std::env::remove_var("DATABASE_URL"),
            }
            result
        };

        // The canary shares the exact schema that drop_each_run would remove.
        // Check persisted data even if startup returned an unexpected error.
        let value: String = fixture
            .client
            .query_one(&format!("SELECT value FROM {canary}"), &[])
            .await
            .unwrap_or_else(|error| {
                panic!("{role} collision must be rejected before dropping existing data: {error}")
            })
            .get(0);
        assert_eq!(value, "retained before admission");
        let error = result
            .err()
            .unwrap_or_else(|| panic!("startup accepted a {role} storage-name collision"))
            .to_string();
        assert!(error.contains("key16"), "missing original ABI/manifest name: {error}");
        assert!(
            error.matches("key_16").count() >= 2,
            "diagnostic must name both the second declaration and the physical collision: {error}"
        );
        let physical = if destination_collision {
            "physical column 'key_16'"
        } else {
            "raw storage column \"key_16\""
        };
        assert!(error.contains(physical), "missing physical collision in diagnostic: {error}");
    }
    fixture.finish().await;
}

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
    let prepared = PreparedProject::read(&manifest_path).unwrap();

    let setup = {
        let _guard = TEST_DATABASE_URL_LOCK.lock().await;
        let original = std::env::var_os("DATABASE_URL");
        std::env::set_var("DATABASE_URL", fixture.url.as_str());
        let result = setup_no_code_prepared(
            StartNoCodeDetails {
                manifest_path: &manifest_path,
                indexing_details: IndexerNoCodeDetails { enabled: true },
                graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
                watch: false,
            },
            &prepared,
        )
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
    let (tasks, _, _, _, coordinators) =
        crate::indexer::start::start_indexing_contract_events_prepared(
            prepared.manifest(),
            fixture._directory.path(),
            Some(fixture.client.clone()),
            None,
            prepared.schema(),
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
            &format!("SELECT t.key_16::text, t.amount_32::text, e.block_number::text FROM {table} t JOIN {} e ON e.key_16::numeric=t.key_16", fixture.params.stored_table()),
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
