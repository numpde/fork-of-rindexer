//! Guard input values come from the incoming event, not an inferred destination.
use super::{journal_recovery_tests::at, *};
use alloy::primitives::U64;

async fn guarded_rows(set: serde_json::Value, values: &[u64]) -> (String, String) {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"][0]["name"] = json!("sourceAmount");
    definition["inputs"][0]["type"] = json!("uint64");
    let table = json!({"name":"totals", "global":true,
        "columns":[{"name":"totalAmount","type":"uint256","default":"0"},{"name":"signedView","type":"uint64","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","if":"$sourceAmount > @totalAmount", "set":set}]}]});
    let fixture = Fixture::with_table(definition, "Contract", table).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut events = values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            at(&fixture, "ethereum", 100 + i as u64, B256::repeat_byte((i + 1) as u8), *value)
        })
        .collect::<Vec<_>>();
    for event in &mut events {
        event.found_in_request.from_block = U64::from(100);
        event.found_in_request.to_block = U64::from(99 + values.len() as u64);
    }
    callback(events).await.expect(
        "every input must be admitted using its own incoming ABI value and the current stored row",
    );
    let row = fixture
        .client
        .query_one(
            &format!(
                "SELECT total_amount::text, signed_view::text FROM {}",
                fixture.params.tables[0].full_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    let state = (row.get(0), row.get(1));
    drop(callback);
    fixture.finish().await;
    state
}

#[tokio::test]
async fn postgres_guard_uses_renamed_upsert_input() {
    let state = guarded_rows(
        json!([{"column":"totalAmount","action":"set","value":"$sourceAmount"}]),
        &[5, 11, 7],
    )
    .await;
    assert_eq!(state, ("11".into(), "0".into()), "the final event fails the guard");
}

#[tokio::test]
async fn postgres_guard_uses_unstored_upsert_input_for_each_event() {
    let state =
        guarded_rows(json!([{"column":"totalAmount","action":"increment"}]), &[10, 0, 4, 0]).await;
    assert_eq!(state,("2".into(),"0".into()),"only the initial insert and the event carrying 4 increment; one batch must not combine their guards");
}

#[tokio::test]
async fn postgres_guard_uses_unsigned_upsert_input_without_choosing_a_destination() {
    let state=guarded_rows(json!([{"column":"signedView","action":"set","value":"$sourceAmount"},{"column":"totalAmount","action":"set","value":"$sourceAmount"}]),&[1,u64::MAX,4]).await;
    assert_eq!(state,(u64::MAX.to_string(),"-1".into()),"the same ABI value feeds signed BIGINT and exact NUMERIC destinations; the guard reads the unsigned input");
}
