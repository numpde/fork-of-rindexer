//! Journal recovery must restore canonical values and their replay ordering.
use super::*;
use crate::{
    indexer::reorg::{
        task::{DerivedColumnJournal, DerivedTableInfo, EventTableInfo},
        BlockChainWindow, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::SetAction,
    provider::mock::MockChainProvider,
};
use alloy::primitives::{U256, U64};

async fn fixture(action: &str, value_type: &str, cross_chain: bool) -> Fixture {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"][0]["type"] = json!(value_type);
    Fixture::with_table_on_networks(
        definition,
        "Contract",
        json!({
            "name":"totals", "cross_chain":cross_chain,
            "columns":[{"name":"id","type":"uint64"},{"name":"amount","type":value_type}],
            "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"1"},
                "set":[{"column":"amount","action":action,"value":"$value"}]}]}]
        }),
        if cross_chain { &["ethereum", "other"] } else { &["ethereum"] },
    )
    .await
}

pub(super) fn at(
    fixture: &Fixture,
    network: &str,
    block: u64,
    hash: B256,
    value: u64,
) -> EventResult {
    let mut event = fixture.events(&[value]).remove(0);
    event.tx_information.network = network.to_string();
    event.log.inner.data = alloy::primitives::LogData::new_unchecked(
        event.log.topics().to_vec(),
        U256::from(value).to_be_bytes::<32>().to_vec().into(),
    );
    event.log.block_number = Some(block);
    event.log.block_hash = Some(hash);
    event.log.transaction_index = Some(3);
    event.log.log_index = Some(4);
    event.tx_information.block_number = block;
    event.tx_information.block_hash = hash;
    event.tx_information.transaction_index = 3;
    event.tx_information.log_index = U256::from(4);
    event.found_in_request.from_block = U64::from(block);
    event.found_in_request.to_block = U64::from(block);
    event
}

async fn rollback(fixture: &Fixture, action: SetAction, cross_chain: bool) {
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let cursor = generate_internal_event_table_name(&schema, "Value");
    let mut coordinator =
        ReorgCoordinator::new(
            "ethereum".into(),
            BlockChainWindow::try_new(8).unwrap(),
            Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
            Arc::new(MockChainProvider::new(1).with_blocks(vec![
                crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(101), 10),
            ])),
            vec![EventTableInfo::try_new(
                schema,
                "value".into(),
                cursor,
                fixture.params.indexer_name.clone(),
                "Contract".into(),
                "Value".into(),
            )
            .unwrap()],
            vec![DerivedTableInfo::try_new(
                fixture.params.tables[0].full_table_name.clone(),
                cross_chain,
                vec![],
                vec![DerivedColumnJournal::try_new("amount".into(), action, vec!["id".into()])
                    .unwrap()],
            )
            .unwrap()],
            vec![],
        )
        .unwrap();
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        )
        .await
        .unwrap();
}

async fn amount(fixture: &Fixture) -> String {
    fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn postgres_journal_rollback_restores_sequence_and_provenance_before_replacement() {
    let fixture = fixture("set", "uint64", false).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), 10)]).await.unwrap();
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 20)]).await.unwrap();
    rollback(&fixture, SetAction::Set, false).await;
    assert_eq!(amount(&fixture).await, "10");
    let metadata = fixture.client.query_one(&format!("SELECT rindexer_sequence_id::text, rindexer_block_number, rindexer_block_hash FROM {}",fixture.params.tables[0].full_table_name),&[]).await.unwrap();
    assert_eq!(
        metadata.get::<_, String>(0),
        crate::manifest::contract::compute_sequence_id(99, 3, 4).to_string()
    );
    assert_eq!(metadata.get::<_, i64>(1), 99);
    assert_eq!(metadata.get::<_, String>(2), format!("{:#x}", B256::repeat_byte(99)));
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(101), 30)]).await.unwrap();
    assert_eq!(amount(&fixture).await, "30");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_uint256_maximum_journal_survives_reorg_without_replacement_event() {
    let fixture = fixture("max", "uint256", false).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), 500)]).await.unwrap();
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 1000)]).await.unwrap();
    assert_eq!(amount(&fixture).await, "1000");
    let journal_count: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.derived_op_log", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal_count, 2, "ordinary uint256 inputs must have durable recovery records");
    rollback(&fixture, SetAction::Max, false).await;
    assert_eq!(amount(&fixture).await, "500");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_journal_failure_aborts_every_application_effect_and_retry_applies_once()
{
    let fixture = fixture("set", "uint64", false).await;
    fixture.client.batch_execute("ALTER TABLE rindexer_internal.derived_op_log ADD CONSTRAINT reject_journal CHECK (block_number < 0)").await.unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let event = at(&fixture, "ethereum", 99, B256::repeat_byte(99), 500);
    let before = fixture.position().await;
    let result = callback(vec![event.clone()]).await;
    assert!(
        matches!(result, Err(crate::event::callback_registry::CallbackError::Retryable(_))),
        "required recovery record failure must fail the callback: {result:?}"
    );
    assert_eq!(fixture.position().await, before);
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 0);
    fixture
        .client
        .batch_execute(
            "ALTER TABLE rindexer_internal.derived_op_log DROP CONSTRAINT reject_journal",
        )
        .await
        .unwrap();
    callback(vec![event.clone()]).await.unwrap();
    callback(vec![event]).await.unwrap();
    assert_eq!(fixture.position().await, (1, "500".to_string(), "99".to_string()));
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_journal_preserves_full_width_signed_and_unsigned_values() {
    use alloy::{dyn_abi::DynSolValue, primitives::I256};
    for signed in [false, true] {
        let fixture = fixture(
            if signed { "min" } else { "max" },
            if signed { "int256" } else { "uint256" },
            false,
        )
        .await;
        let unsigned = (U256::from(1) << 240) + U256::from(123);
        let old = if signed {
            DynSolValue::Int(I256::MIN + I256::from_raw(U256::from(123)), 256)
        } else {
            DynSolValue::Uint(unsigned, 256)
        };
        let orphan = if signed {
            DynSolValue::Int(I256::MIN, 256)
        } else {
            DynSolValue::Uint(U256::MAX, 256)
        };
        let expected = if signed {
            (I256::MIN + I256::from_raw(U256::from(123))).to_string()
        } else {
            unsigned.to_string()
        };
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, value) in [(99, old), (100, orphan)] {
            let mut event = at(&fixture, "ethereum", block, B256::repeat_byte(block as u8), 1);
            event.log.inner.data = alloy::primitives::LogData::new_unchecked(
                event.log.topics().to_vec(),
                value.abi_encode().into(),
            );
            callback(vec![event]).await.unwrap();
        }
        rollback(&fixture, if signed { SetAction::Min } else { SetAction::Max }, false).await;
        assert_eq!(amount(&fixture).await, expected);
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_journal_restores_boolean_address_and_string_set_values() {
    use alloy::{dyn_abi::DynSolValue, primitives::Address};
    for (kind, old, orphan, expected) in [
        ("bool", DynSolValue::Bool(false), DynSolValue::Bool(true), "false".to_string()),
        (
            "address",
            DynSolValue::Address(Address::repeat_byte(1)),
            DynSolValue::Address(Address::repeat_byte(2)),
            format!("{:#x}", Address::repeat_byte(1)),
        ),
        (
            "string",
            DynSolValue::String("O'Brien".into()),
            DynSolValue::String("Orphan".into()),
            "O'Brien".to_string(),
        ),
    ] {
        let fixture = fixture("set", kind, false).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, value) in [(99, old), (100, orphan)] {
            let mut event = at(&fixture, "ethereum", block, B256::repeat_byte(block as u8), 1);
            event.log.inner.data = alloy::primitives::LogData::new_unchecked(
                event.log.topics().to_vec(),
                value.abi_encode().into(),
            );
            callback(vec![event]).await.unwrap();
        }
        rollback(&fixture, SetAction::Set, false).await;
        assert_eq!(amount(&fixture).await, expected);
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_journal_rollback_removes_a_row_created_only_by_the_orphan() {
    let fixture = fixture("max", "uint256", false).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 1000)]).await.unwrap();
    rollback(&fixture, SetAction::Max, false).await;
    let count: i64 = fixture
        .client
        .query_one(
            &format!("SELECT count(*) FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_cross_chain_journal_rollback_keeps_the_unaffected_network_contribution() {
    let fixture = fixture("max", "uint64", true).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (network, block, value) in [("ethereum", 90, 5), ("ethereum", 100, 10), ("other", 200, 20)]
    {
        let mut event = at(&fixture, network, block, B256::repeat_byte(block as u8), value);
        if network == "other" {
            event.tx_information.chain_id = 2;
        }
        callback(vec![event]).await.unwrap();
    }
    assert_eq!(amount(&fixture).await, "20");
    rollback(&fixture, SetAction::Max, true).await;
    let journal: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.derived_op_log WHERE network = 'other'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal, 1, "A's fork height must not invalidate B's independent source history");
    assert_eq!(amount(&fixture).await, "20");
    let raw: i64 = fixture
        .client
        .query_one(
            &format!(
                "SELECT count(*) FROM {} WHERE network = 'other'",
                fixture.params.sql_event_table_name
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let receipts: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.event_applications WHERE network = 'other'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((raw, receipts), (1, 1));
    let remaining_a: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.derived_op_log WHERE network = 'ethereum'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining_a, 1);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_cross_chain_recalculation_waits_for_another_networks_in_flight_application() {
    let fixture = fixture("max", "uint64", true).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, value) in [(90, 5), (100, 10)] {
        callback(vec![at(&fixture, "ethereum", block, B256::repeat_byte(block as u8), value)])
            .await
            .unwrap();
    }
    let controller = client(&fixture.url).await;
    let mut connection = controller.raw_connection().await.unwrap();
    let control = connection.transaction().await.unwrap();
    control.execute("SELECT pg_advisory_xact_lock(8675309)", &[]).await.unwrap();
    fixture.client.batch_execute(&format!(
        "CREATE FUNCTION pause_other_application() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF NEW.network = 'other' THEN PERFORM pg_advisory_xact_lock(8675309); END IF; RETURN NEW; END $$;
         CREATE TRIGGER pause_other BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION pause_other_application()",
        fixture.params.sql_event_table_name)).await.unwrap();
    let other = at(&fixture, "other", 200, B256::repeat_byte(200), 20);
    let other_callback = callback.clone();
    let application = tokio::spawn(async move { other_callback(vec![other]).await });
    let fixture_ref = &fixture;
    let wait_for = |kind: &'static str| async move {
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                let waiting: bool = fixture_ref.admin.query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                     WHERE a.datname = $2 AND NOT l.granted
                       AND (($1 = 'application' AND l.locktype = 'advisory')
                         OR ($1 = 'recovery' AND l.locktype IN ('relation','transactionid'))))",&[&kind, &fixture_ref.database]).await.unwrap().get(0);
                if waiting { break }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await
    };
    wait_for("application")
        .await
        .expect("B pauses after its derived effect and journal, before COMMIT");
    let (_, observed) = tokio::join!(rollback(&fixture, SetAction::Max, true), async {
        let observed = wait_for("recovery").await;
        control.commit().await.unwrap();
        observed
    });
    observed.expect("A waits for B before selecting surviving contributions");
    application.await.unwrap().unwrap();
    assert_eq!(amount(&fixture).await, "20");
    drop(connection);
    drop(callback);
    fixture.finish().await;
}
