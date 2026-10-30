//! Predicates must admit the same ABI-decoded contributions forward and during recovery.
use super::*;
use alloy::{dyn_abi::DynSolValue, primitives::U256};

async fn arithmetic_fixture(
    inputs: serde_json::Value,
    condition: &str,
    iterate: serde_json::Value,
) -> Fixture {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"].as_array_mut().unwrap().extend(inputs.as_array().unwrap().iter().cloned());
    Fixture::with_table(
        definition,
        "Contract",
        json!({"name":"totals","global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value","iterate":iterate,"operations":[{"type":"upsert","if":condition,
            "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
    )
    .await
}

fn predicate_event(
    fixture: &Fixture,
    block: u64,
    amount: u64,
    extra: Vec<DynSolValue>,
) -> EventResult {
    let mut event = super::journal_recovery_tests::at(
        fixture,
        "ethereum",
        block,
        B256::repeat_byte(block as u8),
        amount,
    );
    let mut values = vec![DynSolValue::Uint(U256::from(amount), 256)];
    values.extend(extra);
    event.log.inner.data = alloy::primitives::LogData::new_unchecked(
        event.log.topics().to_vec(),
        DynSolValue::Tuple(values).abi_encode_params().into(),
    );
    event
}

async fn assert_canonical_amount(fixture: &Fixture, amount: &str) {
    assert_eq!(fixture.position().await, (1, amount.into(), "99".into()));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
}

#[tokio::test]
async fn postgres_named_tuple_predicates_apply_and_revert_the_same_effects() {
    for iterated in [false, true] {
        let input = if iterated {
            json!({"name":"items","type":"tuple[]","indexed":false,"components":[{"name":"amount","type":"uint256"}]})
        } else {
            json!({"name":"data","type":"tuple","indexed":false,"components":[{"name":"info","type":"tuple","components":[{"name":"amount","type":"uint256"}]}]})
        };
        let fixture = arithmetic_fixture(
            json!([input]),
            if iterated { "$item.amount > 0" } else { "$data.info.amount > 0" },
            if iterated { json!(["$items as item"]) } else { json!([]) },
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let extra = |value| {
            let tuple = DynSolValue::Tuple(vec![DynSolValue::Uint(U256::from(value), 256)]);
            vec![if iterated {
                DynSolValue::Array(vec![tuple])
            } else {
                DynSolValue::Tuple(vec![tuple])
            }]
        };
        callback(vec![predicate_event(&fixture, 99, 10, extra(1u64))]).await.unwrap();
        callback(vec![predicate_event(&fixture, 100, 20, extra(1u64))]).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (2, "30".into(), "100".into()),
            "ABI names must survive forward predicate evaluation, including iterate aliases"
        );
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_canonical_amount(&fixture, "10").await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_tuple_array_numeric_predicates_recover_typed_json_paths() {
    for shape in 0..3 {
        let (array_type, components, condition) = match shape {
            0 => ("tuple[]", json!([{"name":"amount","type":"uint256"}]), "$items[0].amount > 7"),
            1 => (
                "tuple[2]",
                json!([{"name":"info","type":"tuple","components":[{"name":"tokenAmount","type":"uint256"}]}]),
                "$items[0].info.tokenAmount > 7",
            ),
            _ => (
                "tuple[]",
                json!([{"name":"amounts","type":"uint256[]"}]),
                "$items[0].amounts[0] > 7",
            ),
        };
        let fixture = arithmetic_fixture(
            json!([{"name":"items","type":array_type,"indexed":false,"components":components}]),
            condition,
            json!([]),
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let extra = |value: u64| {
            let scalar = DynSolValue::Uint(U256::from(value), 256);
            let tuple = DynSolValue::Tuple(vec![match shape {
                0 => scalar,
                1 => DynSolValue::Tuple(vec![scalar]),
                _ => DynSolValue::Array(vec![scalar]),
            }]);
            vec![if shape == 1 {
                DynSolValue::FixedArray(vec![tuple.clone(), tuple])
            } else {
                DynSolValue::Array(vec![tuple])
            }]
        };
        let canonical = predicate_event(&fixture, 99, 10, extra(10));
        callback(vec![canonical.clone()]).await.unwrap();
        callback(vec![predicate_event(&fixture, 100, 20, extra(20))]).await.unwrap();
        callback(vec![predicate_event(&fixture, 101, 999, extra(7))]).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (3, "30".into(), "101".into()),
            "the false predicate must not contribute"
        );
        let result = super::recovery_boundary_tests::rollback(&fixture, 100, 101).await;
        assert!(
            result.is_ok(),
            "{condition} must compare typed numeric leaves, not JSON text: {result:#?}"
        );
        assert_canonical_amount(&fixture, "10").await;
        callback(vec![canonical]).await.unwrap();
        assert_canonical_amount(&fixture, "10").await;
        drop(callback);
        fixture.finish().await;
    }
}
