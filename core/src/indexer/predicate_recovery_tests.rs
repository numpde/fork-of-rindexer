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

#[tokio::test]
async fn postgres_string_predicates_require_equivalent_recovery_or_startup_refusal() {
    for (status, wanted) in [("READY", "ready"), ("İ", "i\u{307}"), ("001", "1")] {
        let fixture = arithmetic_fixture(
            json!([
            {"name":"status","type":"string","indexed":false},
            {"name":"wanted","type":"string","indexed":false}]),
            "$status == $wanted",
            json!([]),
        )
        .await;
        let mut source_types = std::collections::HashMap::new();
        crate::indexer::start::collect_source_column_types(
            &fixture.params.event_info.inputs,
            None,
            &mut source_types,
        );
        let mut derived = std::collections::HashMap::new();
        let mut plans = std::collections::HashMap::new();
        let admitted = crate::indexer::start::build_derived_tables_for_event(
            "Value",
            &fixture.params.indexer_name,
            "Contract",
            "ethereum",
            &source_types,
            &fixture.params.tables,
            &mut derived,
            &mut plans,
        );
        if let Err(error) = admitted {
            let error = format!("{error:#}");
            assert!(error.contains("string") && error.contains("predicate"), "{error}");
            let raw: i64 = fixture
                .client
                .query_one(
                    &format!("SELECT count(*) FROM {}", fixture.params.sql_event_table_name),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(raw, 0, "startup refusal must precede event admission");
            fixture.finish().await;
            continue;
        }
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, value) in [(99, 10), (100, 20)] {
            callback(vec![predicate_event(
                &fixture,
                block,
                value,
                vec![DynSolValue::String(status.into()), DynSolValue::String(wanted.into())],
            )])
            .await
            .unwrap();
        }
        assert_eq!(fixture.position().await, (2, "30".into(), "100".into()));
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_canonical_amount(&fixture, "10").await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_constant_string_predicates_share_forward_recovery_semantics() {
    for (condition, matched) in [
        ("0x00 == 0x0", true),
        ("0x00 != 0x0", false),
        ("'READY' == 'ready'", true),
        ("'İ' == 'i\u{307}'", true),
        ("'001' == '1'", true),
        ("'READY' != 'ready'", false),
        ("'DROP TABLE x;--' == 'drop table x;--'", true),
    ] {
        let fixture = arithmetic_fixture(json!([]), condition, json!([])).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(vec![predicate_event(&fixture, 99, 10, vec![])]).await.unwrap();
        callback(vec![predicate_event(&fixture, 100, 20, vec![])]).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (2, if matched { "30" } else { "0" }.into(), "100".into())
        );
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_canonical_amount(&fixture, if matched { "10" } else { "0" }).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_legacy_string_or_untyped_predicates_fail_without_changing_history() {
    use crate::indexer::reorg::{
        task::{
            DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
            DerivedTableRollbackPlan, ReorgTask,
        },
        BlockChainWindow, EventTableInfo,
    };
    use crate::manifest::contract::{ColumnType, SetAction};
    for mode in ["string", "untyped", "quoted", "hex"] {
        let typed = mode != "untyped";
        let condition = if mode == "quoted" {
            "$value > 0 && 'READY' == 'ready'"
        } else if mode == "hex" {
            "$value > 0 && 0x00 == 0x0"
        } else {
            "$status == $wanted"
        };
        let fixture = arithmetic_fixture(
            json!([
            {"name":"status","type":"string","indexed":false},
            {"name":"wanted","type":"string","indexed":false}]),
            "$status == $wanted",
            json!([]),
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, value) in [(99, 10), (100, 20)] {
            callback(vec![predicate_event(
                &fixture,
                block,
                value,
                vec![DynSolValue::String("READY".into()), DynSolValue::String("ready".into())],
            )])
            .await
            .unwrap();
        }
        let before = fixture.position().await;
        assert_eq!(before, (2, "30".into(), "100".into()));
        let schema =
            generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
        let table = fixture.params.tables[0].full_table_name.clone();
        let task = ReorgTask {
            network: "ethereum".into(),
            fork_point: 100,
            detection_point: 100,
            event_tables: vec![EventTableInfo::try_new(
                schema.clone(),
                "value".into(),
                generate_internal_event_table_name(&schema, "Value"),
                fixture.params.indexer_name.clone(),
                "Contract".into(),
                "Value".into(),
            )
            .unwrap()],
            derived_tables: vec![DerivedTableInfo::try_new(
                table.clone(),
                false,
                vec![DerivedTableRollbackOp::try_new(
                    fixture.params.sql_event_table_name.clone(),
                    vec![],
                    vec![DerivedColumnRollback::try_new(
                        "amount".into(),
                        "value".into(),
                        SetAction::Add,
                    )
                    .unwrap()],
                    Some(condition.into()),
                )
                .unwrap()],
                vec![],
            )
            .unwrap()],
            canonical_blocks: vec![],
        };
        let mut source_types = std::collections::HashMap::new();
        if typed {
            crate::indexer::start::collect_source_column_types(
                &fixture.params.event_info.inputs,
                None,
                &mut source_types,
            );
        }
        let mut plan = DerivedTableRollbackPlan::default();
        plan.try_add_operation(
            table,
            0,
            vec![],
            source_types,
            [("amount".into(), ColumnType::Uint256)].into(),
        )
        .unwrap();
        let result = task
            .execute_with_rollback_plan(
                &mut BlockChainWindow::try_new(8).unwrap(),
                Some(&fixture.client),
                None,
                None,
                &plan,
            )
            .await;
        let error =
            result.err().expect("legacy string or unknown-type predicates must stop recovery");
        let error = format!("{error:#}");
        assert!(
            error.contains("predicate")
                && error.contains(match mode {
                    "string" => "string",
                    "untyped" => "type metadata",
                    "hex" => "string",
                    _ => "quoted",
                }),
            "{error}"
        );
        assert_eq!(
            fixture.position().await,
            before,
            "refusing recovery must preserve raw rows, effects and cursor"
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
}

#[tokio::test]
async fn postgres_journal_only_string_predicate_remains_recoverable() {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"].as_array_mut().unwrap().extend([
        json!({"name":"status","type":"string","indexed":false}),
        json!({"name":"wanted","type":"string","indexed":false}),
    ]);
    let fixture = Fixture::with_table(
        definition,
        "Contract",
        json!({"name":"totals","global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","if":"$status == $wanted",
            "set":[{"column":"amount","action":"set","value":"$value"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, value) in [(99, 10), (100, 20)] {
        callback(vec![predicate_event(
            &fixture,
            block,
            value,
            vec![DynSolValue::String("READY".into()), DynSolValue::String("ready".into())],
        )])
        .await
        .unwrap();
    }
    assert_eq!(fixture.position().await, (2, "20".into(), "100".into()));
    super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
    assert_canonical_amount(&fixture, "10").await;
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn readiness_missing_array_element_must_not_admit_a_reversal() {
    for (condition, expected) in [
        ("$values[0] > 0 || $enabled == true", "10"),
        ("$enabled == true || $values[0] > 0", "15"),
        ("!($values[0] > 0 && $enabled == false)", "10"),
        ("!($enabled == false && $values[0] > 0)", "15"),
    ] {
        let fixture = arithmetic_fixture(
            json!([
                {"name":"values","type":"uint256[]","indexed":false},
                {"name":"enabled","type":"bool","indexed":false}
            ]),
            condition,
            json!([]),
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(vec![predicate_event(
            &fixture,
            99,
            10,
            vec![
                DynSolValue::Array(vec![DynSolValue::Uint(U256::from(1), 256)]),
                DynSolValue::Bool(true),
            ],
        )])
        .await
        .unwrap();
        callback(vec![predicate_event(
            &fixture,
            100,
            5,
            vec![DynSolValue::Array(vec![]), DynSolValue::Bool(true)],
        )])
        .await
        .unwrap();
        assert_eq!(fixture.position().await, (2, expected.into(), "100".into()));
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_canonical_amount(&fixture, "10").await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn readiness_full_range_and_fixed_array_sources_recover_in_postgres() {
    for fixed in [false, true] {
        let mut definition = serde_json::to_value(params().event).unwrap();
        definition["inputs"][0]["type"] = json!(if fixed { "uint256[2]" } else { "uint64" });
        let fixture = Fixture::with_table(
            definition,
            "Contract",
            json!({"name":"totals","global":true,
                "columns":[{"name":"amount","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert",
                    "set":[{"column":"amount","action":"add","value":if fixed { "$value[0]" } else { "$value" }}]}]}]}),
        ).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, amount) in [(99, 10), (100, u64::MAX)] {
            let mut event = super::journal_recovery_tests::at(
                &fixture,
                "ethereum",
                block,
                B256::repeat_byte(block as u8),
                amount,
            );
            let scalar = DynSolValue::Uint(U256::from(amount), if fixed { 256 } else { 64 });
            let value = if fixed {
                DynSolValue::FixedArray(vec![scalar, DynSolValue::Uint(U256::ZERO, 256)])
            } else {
                scalar
            };
            event.log.inner.data = alloy::primitives::LogData::new_unchecked(
                event.log.topics().to_vec(),
                DynSolValue::Tuple(vec![value]).abi_encode_params().into(),
            );
            callback(vec![event]).await.unwrap();
        }
        assert_eq!(
            fixture.position().await,
            (2, (U256::from(u64::MAX) + U256::from(10)).to_string(), "100".into())
        );
        super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
        assert_canonical_amount(&fixture, "10").await;
        drop(callback);
        fixture.finish().await;
    }
}
