//! Reversal must use the keys and scalar values that forward execution actually applied.
use super::*;
use alloy::{dyn_abi::DynSolValue, primitives::U256};

async fn fixture(inputs: serde_json::Value, table: serde_json::Value) -> Fixture {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"].as_array_mut().unwrap().extend(inputs.as_array().unwrap().iter().cloned());
    Fixture::with_table(definition, "Contract", table).await
}

fn input(fixture: &Fixture, block: u64, value: U256, extra: Vec<DynSolValue>) -> EventResult {
    let mut event = super::journal_recovery_tests::at(
        fixture,
        "ethereum",
        block,
        B256::repeat_byte(block as u8),
        0,
    );
    let mut values = vec![DynSolValue::Uint(value, 256)];
    values.extend(extra);
    event.log.inner.data = alloy::primitives::LogData::new_unchecked(
        event.log.topics().to_vec(),
        DynSolValue::Tuple(values).abi_encode_params().into(),
    );
    event
}

fn uint(value: u64) -> DynSolValue {
    DynSolValue::Uint(U256::from(value), 256)
}

async fn assert_receipts(fixture: &Fixture, expected: i64) {
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, expected);
}

#[tokio::test]
async fn postgres_arithmetic_required_operands_do_not_use_column_defaults() {
    for missing_key in [false, true] {
        let table = if missing_key {
            json!({"name":"totals", "columns":[
                {"name":"id","type":"uint256","default":"1"},
                {"name":"amount","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert",
                    "where":{"id":"$items[0]"},
                    "set":[{"column":"amount","action":"add","value":"$value"}]}]}]})
        } else {
            json!({"name":"totals", "global":true,
                "columns":[{"name":"amount","type":"uint256","default":"5"}],
                "events":[{"event":"Value","operations":[{"type":"upsert",
                    "set":[{"column":"amount","action":"add","value":"$items[0]"}]}]}]})
        };
        let fixture =
            fixture(json!([{"name":"items","type":"uint256[]","indexed":false}]), table).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let item = if missing_key { 1 } else { 10 };
        callback(vec![input(
            &fixture,
            99,
            U256::from(10),
            vec![DynSolValue::Array(vec![uint(item)])],
        )])
        .await
        .unwrap();
        let canonical = fixture.position().await;
        let valid = input(
            &fixture,
            100,
            U256::from(7),
            vec![DynSolValue::Array(vec![uint(if missing_key { 1 } else { 7 })])],
        );
        let missing = input(&fixture, 101, U256::from(5), vec![DynSolValue::Array(vec![])]);
        let result = callback(vec![valid, missing]).await;
        assert!(result.is_err(), "an explicitly configured missing key/operand must fail before its default can be substituted");
        assert_eq!(
            fixture.position().await,
            canonical,
            "neither sibling raw rows, effects nor checkpoint may commit"
        );
        assert_receipts(&fixture, 1).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_out_of_bounds_fixed_array_operand_is_not_admitted() {
    let fixture = fixture(
        json!([{"name":"items","type":"uint256[2]","indexed":false}]),
        json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"uint256","default":"5"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "set":[{"column":"amount","action":"add","value":"$items[2]"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let result = callback(vec![input(
        &fixture,
        100,
        U256::from(7),
        vec![DynSolValue::FixedArray(vec![uint(1), uint(2)])],
    )])
    .await;
    assert!(
        result.is_err(),
        "a fixed-array reference outside its ABI bounds must fail without writing an application"
    );
    assert_eq!(fixture.position().await, (0, "0".into(), "0".into()));
    assert_receipts(&fixture, 0).await;
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_signed_values_recover_the_applied_delta() {
    for target in ["int128", "int256"] {
        let fixture = fixture(
            json!([]),
            json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":target,"default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(vec![input(&fixture, 99, U256::from(10), vec![])]).await.unwrap();
        callback(vec![input(&fixture, 100, U256::MAX, vec![])]).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (2, "9".into(), "100".into()),
            "the established uint256-to-signed conversion interprets the bits as -1"
        );
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (1, "10".into(), "99".into()),
            "recovery must reverse -1, not the unsigned source magnitude"
        );
        assert_receipts(&fixture, 1).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_keys_use_the_applied_signed_conversion() {
    let fixture = fixture(
        json!([{"name":"key","type":"uint256","indexed":false}]),
        json!({"name":"totals", "columns":[
            {"name":"id","type":"int256"},
            {"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "where":{"id":"$key"},
                "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, amount) in [(99, 10), (100, 5)] {
        callback(vec![input(
            &fixture,
            block,
            U256::from(amount),
            vec![DynSolValue::Uint(U256::MAX, 256)],
        )])
        .await
        .unwrap();
    }
    assert_eq!(fixture.position().await, (2, "15".into(), "100".into()));
    super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
    assert_eq!(
        fixture.position().await,
        (1, "10".into(), "99".into()),
        "both inverse matching and canonical metadata restoration must use key -1"
    );
    assert_receipts(&fixture, 1).await;
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_boolean_operands_recover_the_applied_delta() {
    let fixture = fixture(
        json!([{"name":"flag","type":"bool","indexed":false}]),
        json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "set":[{"column":"amount","action":"add","value":"$flag"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, flag) in [(99, true), (100, true), (101, false)] {
        callback(vec![input(&fixture, block, U256::ZERO, vec![DynSolValue::Bool(flag)])])
            .await
            .unwrap();
    }
    assert_eq!(fixture.position().await, (3, "2".into(), "101".into()));
    super::recovery_boundary_tests::rollback(&fixture, 100, 101).await.unwrap();
    assert_eq!(
        fixture.position().await,
        (1, "1".into(), "99".into()),
        "true/false operands mean 1/0 in both directions"
    );
    assert_receipts(&fixture, 1).await;
    drop(callback);
    fixture.finish().await;
}
