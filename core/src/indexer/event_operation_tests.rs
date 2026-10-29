//! One admitted event must apply all configured writes before its receipt commits.
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
