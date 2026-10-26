//! One filtered subscription's commit must not certify another's unfinished range.
use super::*;
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{
            noop_decoder, CallbackError, EventCallbackType, TraceCallbackRegistry,
        },
        contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    },
    indexer::{
        tables::view_call_cache_tests::{header, ORIGINAL},
        IndexingEventsProgressState,
    },
    provider::mock::MockChainProvider,
};
use alloy::{
    primitives::{Address, LogData, U64},
    rpc::types::ValueOrArray,
};
use tokio::sync::Notify;

async fn run_subscriptions(fixture: &Fixture, fail_second: bool) -> Vec<String> {
    let apply = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let first_committed = Arc::new(Notify::new());
    let callback: EventCallbackType = Arc::new(move |events| {
        let apply = apply.clone();
        let first_committed = first_committed.clone();
        Box::pin(async move {
            let second = events[0].log.topics()[1].as_slice()[31] == 22;
            if fail_second && second {
                first_committed.notified().await;
                return Err(CallbackError::Permanent(
                    "test stops the second subscription before application".into(),
                ));
            }
            apply(events).await?;
            if !second {
                first_committed.notify_one();
            }
            Ok(())
        })
    });
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    for (account, value) in [(11u8, 10u64), (22, 20)] {
        let mut log = fixture.events(&[value]).remove(0).log;
        let mut topic = [0u8; 32];
        topic[12..].fill(account);
        log.inner.data = LogData::new_unchecked(
            vec![fixture.params.event.selector(), B256::from(topic)],
            log.data().data.clone(),
        );
        log.block_number = Some(100);
        log.block_hash = Some(ORIGINAL);
        let provider = Arc::new(
            MockChainProvider::new(1)
                .with_block_number(100)
                .with_blocks(vec![header(ORIGINAL, 10)])
                .with_logs(vec![log]),
        );
        registry.events[0].contract.details.push(NetworkContract {
            id:format!("subscription-{account}"),network:"ethereum".into(),
            indexing_contract_setup:IndexingContractSetup::Address(AddressDetails {
                address:ValueOrArray::Value(Address::ZERO),
                indexed_filters:Some(vec![serde_json::from_value(json!({"event_name":"Value","indexed_1":[format!("{:#x}",Address::repeat_byte(account))]})).unwrap()]),
            }),
            cached_provider:provider.clone(),block_clock:BlockClock::new(None,None,provider),decoder:noop_decoder(),
            start_block:Some(U64::from(100)),end_block:Some(U64::from(100)),disable_logs_bloom_checks:true,
        });
    }
    let manifest =
        serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}]}))
        .unwrap();
    let keys =
        registry.events[0].contract.details.iter().map(NetworkContract::cursor_key).collect();
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let (handles, _, _, _, _) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &fixture.indexer,
        registry,
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        true,
        Default::default(),
        progress,
    )
    .await
    .unwrap();
    let results = tokio::time::timeout(Duration::from_secs(10), async {
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        results
    })
    .await
    .expect("both finite subscriptions finish or report their callback failure");
    if fail_second {
        assert!(results.iter().any(Result::is_err));
    } else {
        assert!(results.iter().all(Result::is_ok));
    }
    keys
}

#[tokio::test]
async fn postgres_restart_resumes_each_filtered_subscription_without_losing_or_reapplying_effects()
{
    let definition = json!({"name":"Value","type":"event","anonymous":false,"inputs":[
        {"name":"account","type":"address","indexed":true},
        {"name":"value","type":"uint256","indexed":false}
    ]});
    let fixture = Fixture::with_event(definition, "Contract").await;
    let keys = run_subscriptions(&fixture, true).await;
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let table = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    for (key, expected) in keys.iter().zip(["100", "0"]) {
        let actual: String = fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network='ethereum' AND cursor_key=$1"), &[key]).await.unwrap().get(0);
        assert_eq!(actual, expected, "each subscription owns its durable prefix");
    }
    let before = fixture.position().await;
    assert_eq!(before.0, 1, "A committed before B failed");
    assert_eq!(before.1, "10");
    run_subscriptions(&fixture, false).await;
    let after = fixture.position().await;
    assert_eq!(
        (after.0, after.1),
        (2, "30".to_string()),
        "A's checkpoint cannot skip B, and replay cannot apply A twice"
    );
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 2);
    for key in &keys {
        let actual: String = fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network='ethereum' AND cursor_key=$1"), &[key]).await.unwrap().get(0);
        assert_eq!(actual, "100");
    }
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_legacy_progress_is_not_adopted_and_rollback_rewinds_all_query_keys() {
    let fixture = Fixture::new().await;
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let table = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    fixture.client.batch_execute(&format!("DROP TABLE rindexer_internal.{table}; CREATE TABLE rindexer_internal.{table} (network TEXT PRIMARY KEY,last_synced_block NUMERIC); INSERT INTO rindexer_internal.{table} VALUES ('ethereum',900)")).await.unwrap();
    let ddl = generate_tables_for_indexer_sql(fixture._directory.path(), &fixture.indexer, false)
        .unwrap();
    fixture.client.batch_execute(ddl.as_str()).await.unwrap();
    fixture.client.batch_execute(ddl.as_str()).await.unwrap();
    // Model independently completed prefixes and another unaffected network.
    fixture.client.batch_execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('ethereum','v1:a',100),('ethereum','v1:b',80),('other','v1:a',200)")).await.unwrap();
    let legacy: String = fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network='ethereum' AND cursor_key='__event__'"), &[]).await.unwrap().get(0);
    assert_eq!(legacy, "900", "migration retains history without copying it into subscriptions");
    fixture
        .client
        .reorg_rollback_transaction(&[], "ethereum", 98, 100, &[], &[&table])
        .await
        .unwrap();
    let rows = fixture.client.query(&format!("SELECT network,cursor_key,last_synced_block::text FROM rindexer_internal.{table} ORDER BY network,cursor_key"), &[]).await.unwrap();
    let actual: Vec<(String, String, String)> =
        rows.iter().map(|row| (row.get(0), row.get(1), row.get(2))).collect();
    assert_eq!(
        actual,
        vec![
            ("ethereum".into(), "__event__".into(), "97".into()),
            ("ethereum".into(), "v1:a".into(), "97".into()),
            ("ethereum".into(), "v1:b".into(), "80".into()),
            ("other".into(), "v1:a".into(), "200".into())
        ]
    );
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_overlapping_subscriptions_share_receipts_but_advance_separate_cursors() {
    let fixture = Fixture::new().await;
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let table = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    fixture.client.batch_execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('ethereum','v1:a',0),('ethereum','v1:b',0)")).await.unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for key in ["v1:a", "v1:b"] {
        let mut events = fixture.events(&[10]);
        events[0].found_in_request.cursor_key = key.into();
        callback(events).await.unwrap();
    }
    let position = fixture.position().await;
    assert_eq!((position.0, position.1), (1, "10".into()));
    let rows = fixture.client.query(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE cursor_key IN ('v1:a','v1:b')"), &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.get::<_, String>(0) == position.2));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
    fixture.finish().await;
}
