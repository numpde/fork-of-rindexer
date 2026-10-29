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

async fn run_tip_subscription(fixture: &Fixture, head: u64) -> String {
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    registry.events[0].tables = fixture.params.tables.clone();
    let logs = (100..=head)
        .map(|number| {
            let mut log = fixture.events(&[number]).remove(0).log;
            log.block_number = Some(number);
            log.block_hash = Some(B256::repeat_byte(number as u8));
            log
        })
        .collect();
    let blocks = (99..=head)
        .map(|number| {
            let mut block = header(B256::repeat_byte(number as u8), number);
            block.header.inner.number = number;
            block.header.inner.parent_hash = B256::repeat_byte((number - 1) as u8);
            block
        })
        .collect();
    let provider = Arc::new(
        MockChainProvider::new(1).with_block_number(head).with_blocks(blocks).with_logs(logs),
    );
    let detail = NetworkContract {
        id: "tip-subscription".into(),
        network: "ethereum".into(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: noop_decoder(),
        start_block: None,
        end_block: None,
        disable_logs_bloom_checks: true,
    };
    let key = detail.cursor_key();
    registry.events[0].contract.details.push(detail);
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let manifest = serde_json::from_value(json!({"name":"Replay","project_type":"no-code",
        "contracts":[{"name":fixture.params.contract_name,"abi":"abi.json","details":[]}],"networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}]})).unwrap();
    // Complete the production historical phase at the current tip on each run;
    // both range fields remain omitted in the subscription being restarted.
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
    tokio::time::timeout(Duration::from_secs(10), async {
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    })
    .await
    .expect("finite startup snapshot completes");
    key
}

#[tokio::test]
async fn postgres_restart_without_start_block_recovers_downtime_exactly_once() {
    let fixture = Fixture::new().await;
    let first_key = run_tip_subscription(&fixture, 100).await;
    assert_eq!(fixture.position().await, (1, "100".into(), "100".into()));
    let resumed_key = run_tip_subscription(&fixture, 105).await;
    assert_eq!(resumed_key, first_key, "unchanged YAML keeps the same subscription cursor");
    assert_eq!(fixture.position().await, (6, "615".into(), "105".into()));
    let blocks: Vec<String> = fixture
        .client
        .query(
            &format!(
                "SELECT block_number::text FROM {} ORDER BY block_number",
                fixture.params.sql_event_table_name
            ),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(blocks, ["100", "101", "102", "103", "104", "105"]);
    run_tip_subscription(&fixture, 105).await;
    assert_eq!(
        fixture.position().await,
        (6, "615".into(), "105".into()),
        "restart must preserve the completed effects"
    );
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 6, "one application receipt per canonical event");
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_non_reorg_literal_arithmetic_survives_startup_and_restart() {
    let mut fixture = Fixture::new().await;
    Arc::make_mut(&mut fixture.params.tables)[0].table.events[0].operations[0].set[0].value =
        Some("1".into());
    run_tip_subscription(&fixture, 100).await;
    assert_eq!(fixture.position().await, (1, "1".into(), "100".into()));
    run_tip_subscription(&fixture, 102).await;
    assert_eq!(fixture.position().await, (3, "3".into(), "102".into()));
    run_tip_subscription(&fixture, 102).await;
    assert_eq!(
        fixture.position().await,
        (3, "3".into(), "102".into()),
        "non-reorg literal arithmetic remains valid and exactly-once on restart"
    );
    fixture.finish().await;
}

fn handoff_detail(provider: Arc<dyn crate::provider::ChainProvider>) -> NetworkContract {
    NetworkContract {
        id: "handoff-subscription".into(),
        network: "ethereum".into(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: noop_decoder(),
        start_block: Some(U64::from(98)),
        end_block: None,
        disable_logs_bloom_checks: true,
    }
}

#[tokio::test]
async fn postgres_historical_handoff_updates_the_configured_cursor_after_reorg() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    let detail = handoff_detail(Arc::new(MockChainProvider::new(1)));
    let key = detail.cursor_key();
    let detail_id = detail.id.clone();
    let event_id = registry.events[0].id.clone();
    registry.events[0].contract.details.push(detail);
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let table = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    fixture.client.execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('ethereum',$1,0) ON CONFLICT DO NOTHING"),&[&key]).await.unwrap();
    for (block, value) in [(99, 10), (100, 20)] {
        let mut event = super::journal_recovery_tests::at(
            &fixture,
            "ethereum",
            block,
            B256::repeat_byte(block as u8),
            value,
        );
        event.found_in_request.cursor_key = key.clone();
        registry.trigger_event(&event_id, vec![event]).await.unwrap();
    }
    let live =
        registry.reapply_after_historic(vec![crate::indexer::start::ProcessedNetworkContract {
            id: detail_id,
            processed_up_to: U64::from(100),
        }]);
    let live_detail = Arc::new(live.events[0].contract.details[0].clone());
    let live_key = live_detail.cursor_key();
    fixture.client.execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('ethereum',$1,0) ON CONFLICT DO NOTHING"),&[&live_key]).await.unwrap();
    let replay =
        super::journal_recovery_tests::at(&fixture, "ethereum", 100, B256::repeat_byte(100), 20);
    live.trigger_event(
        &event_id,
        vec![EventResult::try_new(live_detail.clone(), replay.log, U64::from(100), U64::from(100))
            .unwrap()],
    )
    .await
    .unwrap();
    super::arithmetic_recovery_tests::rollback(&fixture).await;
    let replacement =
        super::journal_recovery_tests::at(&fixture, "ethereum", 100, B256::repeat_byte(101), 30);
    live.trigger_event(
        &event_id,
        vec![EventResult::try_new(
            live_detail.clone(),
            replacement.log,
            U64::from(100),
            U64::from(100),
        )
        .unwrap()],
    )
    .await
    .unwrap();
    let state = fixture.position().await;
    assert_eq!((state.0, state.1), (2, "40".into()), "canonical effects remain exactly once");
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 2);
    let cursor: String = fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network='ethereum' AND cursor_key=$1"),&[&key]).await.unwrap().get(0);
    assert_eq!(
        cursor, "100",
        "live replacement work must advance the original configured subscription after rollback"
    );
    let keys = fixture
        .client
        .query(
            &format!(
                "SELECT cursor_key FROM rindexer_internal.{table} WHERE cursor_key <> '__event__'"
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(keys.len(), 1, "handoff cannot create a second durable query identity");
    assert_eq!(live_detail.start_block, Some(U64::from(98)));
    assert_eq!(live_key, key);
    drop(live);
    drop(registry);
    fixture.finish().await;
}

async fn handoff_startup_scope(with_postgres: bool) {
    let fixture = Fixture::new().await;
    let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let observed = received.clone();
    let callback: EventCallbackType = Arc::new(move |events| {
        let observed = observed.clone();
        Box::pin(async move {
            for event in events {
                observed.lock().await.push(event.found_in_request);
            }
            Ok(())
        })
    });
    let logs = [99, 101]
        .into_iter()
        .map(|number| {
            super::journal_recovery_tests::at(
                &fixture,
                "ethereum",
                number,
                B256::repeat_byte(number as u8),
                1,
            )
            .log
        })
        .collect();
    let blocks = (99..=101)
        .map(|number| {
            let mut block = header(B256::repeat_byte(number as u8), number);
            block.header.inner.number = number;
            block.header.inner.parent_hash = B256::repeat_byte((number - 1) as u8);
            block
        })
        .collect();
    let provider = Arc::new(
        MockChainProvider::new(1).with_block_number(101).with_blocks(blocks).with_logs(logs),
    );
    let mut detail = handoff_detail(provider);
    detail.start_block = Some(U64::ZERO);
    let key = detail.cursor_key();
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    registry.events[0].contract.details.push(detail);
    let registry =
        registry.reapply_after_historic(vec![crate::indexer::start::ProcessedNetworkContract {
            id: "handoff-subscription".into(),
            processed_up_to: U64::from(100),
        }]);
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let manifest = serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],"networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}]})).unwrap();
    let (handles, _, _, _, _) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        with_postgres.then(|| fixture.client.clone()),
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
    tokio::time::timeout(Duration::from_secs(10), async {
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
    let observed = received.lock().await;
    assert_eq!(
        observed.len(),
        if with_postgres { 2 } else { 1 },
        "a zero durable cursor cannot be replaced by a later in-process hint"
    );
    for event in observed.iter() {
        assert_eq!((event.from_block,event.to_block),
            (U64::from(if with_postgres {0} else {100}),U64::from(101)),
            "durable coverage owns stored subscriptions; an untracked handler uses the historical handoff");
        assert_eq!(
            event.cursor_key, key,
            "runtime resume bounds cannot redefine the configured query"
        );
    }
    drop(observed);
    fixture.finish().await;
}

#[tokio::test]
async fn historical_handoff_without_storage_resumes_without_changing_query_identity() {
    handoff_startup_scope(false).await;
}

#[tokio::test]
async fn postgres_historical_handoff_respects_zero_durable_coverage() {
    handoff_startup_scope(true).await;
}
