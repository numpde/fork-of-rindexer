//! One admitted event must apply all configured writes before its receipt commits.
use alloy::primitives::U64;

use super::journal_recovery_tests::at;
use super::*;

async fn fixture() -> Fixture {
    Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", json!({
        "name":"totals", "global":true,
        "columns":[{"name":"amount", "type":"uint256", "default":"0"}, {"name":"b", "type":"uint256", "default":"0"}],
        "events":[{"event":"Value", "operations":[
            {"type":"upsert", "if":"value == 0", "set":[{"column":"amount", "action":"set", "value":"0"}, {"column":"b", "action":"set", "value":"0"}]},
            {"type":"update", "if":"value > 0", "set":[{"column":"amount", "action":"set", "value":"$value"}]},
            {"type":"update", "if":"value > 0", "set":[{"column":"b", "action":"set", "value":"$value * 2"}]}
        ]}]
    })).await
}

fn event(fixture: &Fixture, block: u64, branch: u8, value: u64) -> EventResult {
    at(fixture, "ethereum", block, B256::repeat_byte(branch), value)
}

async fn values(fixture: &Fixture) -> (String, String) {
    let row = fixture
        .client
        .query_one(
            &format!(
                "SELECT amount::text, b::text FROM {}",
                fixture.params.tables[0].full_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

#[tokio::test]
async fn postgres_same_event_updates_apply_both_columns_and_replay_once() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(
        values(&fixture).await,
        ("1".into(), "2".into()),
        "both required operations must apply before the event is certified"
    );
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(values(&fixture).await, ("1".into(), "2".into()));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 2, "the seed and event each commit exactly one receipt");
    assert_eq!(fixture.position().await.0, 2);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_same_event_continuation_does_not_admit_a_committed_equal_position() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    let before = values(&fixture).await;
    callback(vec![event(&fixture, 100, 101, 9)]).await.unwrap();
    assert_eq!(
        values(&fixture).await,
        before,
        "a different block at an equal position needs rollback, not same-event admission"
    );
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_same_event_operations_follow_event_order_across_a_batch() {
    let mut fixture = fixture().await;
    let table = &mut Arc::make_mut(&mut fixture.params.tables)[0].table;
    table.events[0].operations[1].if_condition = Some("value == 2".into());
    table.events[0].operations[2].if_condition = Some("value == 1".into());
    table.events[0].operations[2].set[0].value = Some("$value".into());
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 99, 99, 1), event(&fixture, 100, 100, 2)]).await.unwrap();
    assert_eq!(
        values(&fixture).await,
        ("2".into(), "1".into()),
        "an earlier event's second operation must run before a later event advances row ordering"
    );
    callback(vec![event(&fixture, 99, 99, 1), event(&fixture, 100, 100, 2)]).await.unwrap();
    assert_eq!(values(&fixture).await, ("2".into(), "1".into()));
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_same_event_upserts_apply_both_columns_and_replay_once() {
    let mut fixture = fixture().await;
    for operation in
        Arc::make_mut(&mut fixture.params.tables)[0].table.events[0].operations.iter_mut().skip(1)
    {
        operation.operation_type = crate::manifest::contract::OperationType::Upsert;
    }
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(values(&fixture).await, ("1".into(), "2".into()));
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(values(&fixture).await, ("1".into(), "2".into()));
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_same_event_arithmetic_upsert_keeps_its_state_setting_fields() {
    let mut fixture = fixture().await;
    let table = &mut Arc::make_mut(&mut fixture.params.tables)[0].table;
    table.events[0].operations[0].set.retain(|column| column.column == "amount");
    table.events[0].operations[2] =
        serde_json::from_value(json!({"type":"upsert", "if":"value > 0", "set":[
            {"column":"amount", "action":"set", "value":"$value * 2"},
            {"column":"b", "action":"add", "value":"$value"}
        ]}))
        .unwrap();
    table.validate_reorg_operations().unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(
        values(&fixture).await,
        ("2".into(), "1".into()),
        "the arithmetic upsert must apply its same-event state fields too"
    );
    callback(vec![event(&fixture, 100, 100, 1)]).await.unwrap();
    assert_eq!(values(&fixture).await, ("2".into(), "1".into()));
    drop(callback);
    fixture.finish().await;
}

async fn extrema_fixture(action: &str) -> Fixture {
    let fixture = Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", json!({
        "name":"totals", "global":true,
        "columns":[{"name":"amount", "type":"uint256", "default":"0"}],
        "events":[{"event":"Value", "operations":[
            {"type":"update", "if":"value != 20", "set":[{"column":"amount", "action":action, "value":"$value"}]},
            {"type":"upsert", "if":"value == 20", "set":[{"column":"amount", "action":action, "value":"$value"}]}
        ]}]
    })).await;
    fixture.params.tables[0].table.validate_reorg_operations().unwrap();
    fixture
}

async fn ignored_update_recovery(action: &str, ignored: u64, orphan: u64) {
    let fixture = extrema_fixture(action).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, ignored)]).await.unwrap();
    let rows: i64 = fixture
        .client
        .query_one(
            &format!("SELECT count(*) FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0, "UPDATE must not create a row");
    let ignored_journals: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.derived_op_log WHERE block_number=98",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    callback(vec![event(&fixture, 99, 99, 20)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, orphan)]).await.unwrap();
    super::arithmetic_recovery_tests::rollback(&fixture).await;
    let amount: String = fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(amount, "20", "a skipped update must never become a canonical extrema contribution");
    assert_eq!(ignored_journals, 0, "only actual effects belong in recovery history");
    assert_eq!(
        fixture.position().await.0,
        2,
        "both surviving canonical raw events remain, including the no-op update"
    );
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 2);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_skipped_update_cannot_fabricate_max_during_recovery() {
    ignored_update_recovery("max", 100, 30).await;
}

#[tokio::test]
async fn postgres_skipped_update_cannot_fabricate_min_during_recovery() {
    ignored_update_recovery("min", 1, 10).await;
}

#[tokio::test]
async fn postgres_skipped_and_applied_updates_in_one_batch_keep_distinct_outcomes() {
    let fixture = extrema_fixture("max").await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 99, 99, 20)]).await.unwrap();
    callback(vec![event(&fixture, 98, 98, 100), event(&fixture, 100, 100, 30)]).await.unwrap();
    let amount: String = fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        amount, "30",
        "an older rejected input must not be aggregated into the admitted update"
    );
    let blocks: Vec<String> = fixture
        .client
        .query(
            "SELECT journal.block_number::text FROM rindexer_internal.derived_op_log journal ORDER BY journal.block_number",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(blocks, ["99", "100"], "the batch's affected-row count cannot certify every input");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_applied_same_event_set_order_survives_recovery() {
    let mut fixture = fixture().await;
    Arc::make_mut(&mut fixture.params.tables)[0].table.events[0].operations[2].set[0].column =
        "amount".into();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 98, 98, 0)]).await.unwrap();
    callback(vec![event(&fixture, 99, 99, 1)]).await.unwrap();
    let canonical = values(&fixture).await;
    assert_eq!(canonical.0, "2");
    callback(vec![event(&fixture, 100, 100, 3)]).await.unwrap();
    super::arithmetic_recovery_tests::rollback(&fixture).await;
    assert_eq!(
        values(&fixture).await,
        canonical,
        "the last applied write at an event position must win during recovery too"
    );
    drop(callback);
    fixture.finish().await;
}

async fn assignment_fixture(condition: Option<&str>) -> Fixture {
    let mut operation =
        json!({"type":"upsert", "set":[{"column":"amount", "action":"set", "value":"$value"}]});
    if let Some(condition) = condition {
        operation["if"] = condition.into();
    }
    Fixture::with_table(
        serde_json::to_value(params().event).unwrap(),
        "Contract",
        json!({
            "name":"totals", "global":true,
            "columns":[{"name":"amount", "type":"uint256", "default":"0"}],
            "events":[{"event":"Value", "operations":[operation]}]
        }),
    )
    .await
}

#[tokio::test]
async fn postgres_superseded_upsert_retains_canonical_value_provenance_and_receipt() {
    let fixture = assignment_fixture(None).await;
    fixture.params.tables[0].table.validate_reorg_operations().unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 100, 100, 30)]).await.unwrap();
    let canonical = event(&fixture, 99, 99, 20);
    let transaction = format!("{:#x}", canonical.tx_information.transaction_hash);
    let address = format!("{:#x}", canonical.tx_information.address);
    callback(vec![canonical]).await.unwrap();
    let value: String = fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, "30", "the superseded upsert must not replace live newer state");
    callback(vec![event(&fixture, 99, 99, 20), event(&fixture, 100, 100, 30)]).await.unwrap();
    super::arithmetic_recovery_tests::rollback(&fixture).await;
    let table = &fixture.params.tables[0].full_table_name;
    let rows = fixture.client.query(&format!("SELECT amount::text, rindexer_sequence_id::text, rindexer_block_number, rindexer_block_hash, rindexer_tx_hash, rindexer_contract_address FROM {table}"), &[]).await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a superseded canonical upsert must keep the row alive after its replacement is orphaned"
    );
    let row = &rows[0];
    assert_eq!(row.get::<_, String>(0), "20");
    assert_eq!(
        row.get::<_, String>(1),
        crate::manifest::contract::compute_sequence_id(99, 3, 4).to_string()
    );
    assert_eq!(row.get::<_, i64>(2), 99);
    assert_eq!(row.get::<_, String>(3), format!("{:#x}", B256::repeat_byte(99)));
    assert_eq!(row.get::<_, String>(4), transaction);
    assert_eq!(row.get::<_, String>(5), address);
    assert_eq!(fixture.position().await.0, 1);
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1, "the canonical receipt survives rollback");
    callback(vec![event(&fixture, 99, 99, 20)]).await.unwrap();
    assert_eq!(fixture.position().await.0, 1);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_superseded_upsert_policy_does_not_journal_a_false_table_guard() {
    let fixture = assignment_fixture(Some("@amount == 0")).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 99, 99, 30)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 20)]).await.unwrap();
    let value: String = fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, "30");
    let journal: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.derived_op_log", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal, 1, "a failed table-state guard is not a superseded canonical assignment");
    drop(callback);
    fixture.finish().await;
}

fn arithmetic_update_table() -> serde_json::Value {
    json!({"name":"totals", "global":true,
        "columns":[{"name":"amount", "type":"uint256", "default":"0"}],
        "events":[{"event":"Value", "operations":[
            {"type":"update", "if":"value != 10", "set":[{"column":"amount", "action":"add", "value":"$value"}]},
            {"type":"upsert", "if":"value == 10", "set":[{"column":"amount", "action":"add", "value":"$value"}]}
        ]}]
    })
}

async fn arithmetic_update_batch_case(batched: bool) {
    let fixture = Fixture::with_table(
        serde_json::to_value(params().event).unwrap(),
        "Contract",
        arithmetic_update_table(),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 100, 100, 10)]).await.unwrap();
    let mut updates = vec![event(&fixture, 99, 99, 100), event(&fixture, 101, 101, 1)];
    for update in &mut updates {
        update.found_in_request.from_block = U64::from(99);
        update.found_in_request.to_block = U64::from(101);
    }
    if batched {
        callback(updates.clone()).await.unwrap();
    } else {
        callback(vec![event(&fixture, 99, 99, 100)]).await.unwrap();
        callback(vec![event(&fixture, 101, 101, 1)]).await.unwrap();
    }
    assert_eq!(
        fixture.position().await,
        (3, "11".into(), "101".into()),
        "batching must not admit an older rejected arithmetic input"
    );
    callback(updates).await.unwrap();
    assert_eq!(fixture.position().await.1, "11");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_update_batch_matches_individual_admission() {
    arithmetic_update_batch_case(true).await;
}

#[tokio::test]
async fn postgres_arithmetic_update_separate_callbacks_are_an_admission_control() {
    arithmetic_update_batch_case(false).await;
}

#[tokio::test]
async fn postgres_arithmetic_update_recovery_requires_support_or_startup_rejection() {
    let fixture = Fixture::with_table(
        serde_json::to_value(params().event).unwrap(),
        "Contract",
        arithmetic_update_table(),
    )
    .await;
    if let Err(reason) = fixture.params.tables[0].table.validate_reorg_operations() {
        assert!(reason.contains("arithmetic update") && reason.contains("reorg"), "{reason}");
        let untouched = fixture.client.query_one(&format!("SELECT (SELECT count(*) FROM {}), (SELECT count(*) FROM {}), (SELECT count(*) FROM rindexer_internal.event_applications)", fixture.params.sql_event_table_name, fixture.params.tables[0].full_table_name), &[]).await.unwrap();
        assert_eq!(
            (untouched.get::<_, i64>(0), untouched.get::<_, i64>(1), untouched.get::<_, i64>(2)),
            (0, 0, 0),
            "unsupported recovery must be rejected before event effects"
        );
        fixture.finish().await;
        return;
    }
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![event(&fixture, 99, 99, 1)]).await.unwrap();
    callback(vec![event(&fixture, 100, 100, 10)]).await.unwrap();
    super::arithmetic_recovery_tests::rollback(&fixture).await;
    let rows: i64 = fixture
        .client
        .query_one(
            &format!("SELECT count(*) FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        rows, 0,
        "a skipped canonical update cannot keep a row that only the orphan created"
    );
    drop(callback);
    fixture.finish().await;
}
