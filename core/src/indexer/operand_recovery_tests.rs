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

#[tokio::test]
async fn postgres_arithmetic_guard_can_exclude_missing_optional_inputs() {
    let fixture = fixture(
        json!([{"name":"items","type":"uint256[]","indexed":false}]),
        json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"uint256","default":"5"}],
            "events":[{"event":"Value","operations":[{"type":"upsert","if":"$items[0] > 0",
                "set":[{"column":"amount","action":"add","value":"$items[0]"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, items) in [(99, vec![uint(10)]), (100, vec![]), (101, vec![uint(7)])] {
        callback(vec![input(&fixture, block, U256::ZERO, vec![DynSolValue::Array(items)])])
            .await
            .unwrap();
    }
    assert_eq!(fixture.position().await, (3, "22".into(), "101".into()));
    super::recovery_boundary_tests::rollback(&fixture, 100, 101).await.unwrap();
    assert_eq!(fixture.position().await, (1, "15".into(), "99".into()));
    assert_receipts(&fixture, 1).await;
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_legacy_missing_operands_abort_before_recovery_mutation() {
    let fixture = fixture(
        json!([{"name":"items","type":"uint256[]","indexed":false}]),
        json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"uint256","default":"5"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "set":[{"column":"amount","action":"add","value":"$items[0]"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, value) in [(99, 10), (100, 7), (101, 9)] {
        callback(vec![input(
            &fixture,
            block,
            U256::ZERO,
            vec![DynSolValue::Array(vec![uint(value)])],
        )])
        .await
        .unwrap();
    }
    // Model history committed by an older writer: the missing last delta used
    // the column's default 5. The valid orphan delta keeps SUM from returning NULL.
    fixture
        .client
        .batch_execute(&format!(
            "UPDATE {} SET items = '{{}}' WHERE block_number = 101; UPDATE {} SET amount = 27",
            fixture.params.stored_table(),
            fixture.params.tables[0].full_table_name
        ))
        .await
        .unwrap();
    let before = fixture.position().await;
    let result = super::recovery_boundary_tests::rollback(&fixture, 100, 101).await;
    let error = format!(
        "{:#}",
        result.expect_err("legacy unresolved deltas cannot be guessed during reversal")
    );
    assert!(error.contains("unresolved required arithmetic"), "{error}");
    assert_eq!(
        fixture.position().await,
        before,
        "raw history, derived state and cursor must remain in the same transaction"
    );
    assert_receipts(&fixture, 3).await;
    drop(callback);
    fixture.finish().await;
}

fn prepare_plan(fixture: &Fixture) -> anyhow::Result<()> {
    let source_types = crate::indexer::reorg::task::RollbackEventSource::new(
        "Value",
        &fixture.params.event_info.inputs,
    )
    .unwrap();
    crate::indexer::start::build_derived_tables_for_event(
        "Value",
        &fixture.params.indexer_name,
        "Contract",
        "ethereum",
        &source_types,
        &fixture.params.tables,
        &mut std::collections::HashMap::new(),
        &mut std::collections::HashMap::new(),
    )
}

#[tokio::test]
async fn postgres_arithmetic_ambiguous_byte_keys_fail_before_admission() {
    for (source, target) in [
        ("bytes2", "string"),
        ("bytes", "string"),
        ("bytes2", "bytes32"),
        ("bytes", "bytes32"),
        ("bytes", "bytes"),
        ("bytes32", "bytes"),
    ] {
        let fixture = fixture(
            json!([{"name":"key","type":source,"indexed":false}]),
            json!({"name":"totals", "columns":[
                {"name":"id","type":target},
                {"name":"amount","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"$key"},
                    "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
        )
        .await;
        let error = format!(
            "{:#}",
            prepare_plan(&fixture).expect_err("do not silently reformat legacy byte keys")
        );
        assert!(
            error.contains("legacy debug-byte values") && error.contains("fixed-bytes padding"),
            "{error}"
        );
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let key = match source {
            "bytes" => DynSolValue::Bytes(vec![0xab, 0xcd]),
            "bytes32" => DynSolValue::FixedBytes(B256::repeat_byte(0xab), 32),
            _ => {
                let mut bytes = [0; 32];
                bytes[..2].copy_from_slice(&[0xab, 0xcd]);
                DynSolValue::FixedBytes(B256::from(bytes), 2)
            }
        };
        let result = callback(vec![input(&fixture, 100, U256::from(5), vec![key])]).await;
        assert!(
            result.is_err(),
            "programmatic callbacks require the same type admission: {source} -> {target}"
        );
        assert_eq!(fixture.position().await, (0, "0".into(), "0".into()));
        assert_receipts(&fixture, 0).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_scalar_keys_preserve_supported_conversions() {
    use alloy::primitives::{Address, I256};
    let cases = vec![
        ("uint256", "address", DynSolValue::Uint(U256::MAX, 256), format!("0x{}", "ff".repeat(20))),
        ("uint256", "bool", uint(2), "true".into()),
        ("uint256", "string", DynSolValue::Uint(U256::MAX, 256), U256::MAX.to_string()),
        ("bool", "uint256", DynSolValue::Bool(true), "1".into()),
        ("int256", "int64", DynSolValue::Int(I256::try_from(-1).unwrap(), 256), "-1".into()),
        ("uint256", "int8", DynSolValue::Uint(U256::MAX, 256), "-1".into()),
        ("uint64", "uint64", DynSolValue::Uint(U256::from(u64::MAX), 64), "-1".into()),
        (
            "bytes32",
            "string",
            DynSolValue::FixedBytes(B256::repeat_byte(0xab), 32),
            format!("0x{}", "ab".repeat(32)),
        ),
        (
            "bytes32",
            "bytes32",
            DynSolValue::FixedBytes(B256::repeat_byte(0xab), 32),
            format!("\\x{}", alloy::hex::encode(format!("0x{}", "ab".repeat(32)))),
        ),
        (
            "address",
            "address",
            DynSolValue::Address(Address::repeat_byte(0x11)),
            format!("0x{}", "11".repeat(20)),
        ),
        ("string", "string", DynSolValue::String("Some key".into()), "Some key".into()),
        (
            "address[]",
            "address[]",
            DynSolValue::Array(vec![DynSolValue::Address(Address::repeat_byte(0x11))]),
            format!("{{0x{}}}", "11".repeat(20)),
        ),
        ("uint256", "timestamp", uint(1000), "1000".into()),
        ("uint256", "timestamp", uint(1_100_458_312_577), "1100458312577".into()),
        ("uint256", "timestamp", uint(8_210_266_876_799), "8210266876799".into()),
    ];
    for (source, target, key, expected) in cases {
        let mut fixture = fixture(
            json!([{"name":"key","type":source,"indexed":false}]),
            json!({"name":"totals", "columns":[
                {"name":"id","type":target},
                {"name":"amount","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"$key"},
                    "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
        )
        .await;
        if target == "timestamp" {
            // Give only this fixture database a non-UTC default, then connect
            // afresh so every pooled connection observes the same time zone.
            // Recovery must remain exact without changing the session setting.
            fixture
                .admin
                .batch_execute(&format!(
                    "ALTER DATABASE {} SET TIME ZONE 'America/New_York'",
                    fixture.database
                ))
                .await
                .unwrap();
            let timezone_client = client(&fixture.url).await;
            fixture.params.postgres = Some(timezone_client.clone());
            fixture.client = timezone_client;
            let timezone: String =
                fixture.client.query_one("SHOW TimeZone", &[]).await.unwrap().get(0);
            assert_eq!(timezone, "America/New_York");
        }
        prepare_plan(&fixture).unwrap_or_else(|error| panic!("{source} -> {target}: {error:#}"));
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, amount) in [(99, 10), (100, 5)] {
            callback(vec![input(&fixture, block, U256::from(amount), vec![key.clone()])])
                .await
                .unwrap_or_else(|error| panic!("{source} -> {target}: {error}"));
        }
        if target == "timestamp" {
            // Compare the instant, not session-dependent display text. This
            // also detects a fractional-microsecond conversion of large epochs.
            let exact: bool = fixture
                .client
                .query_one(
                    &format!(
                        "SELECT EXTRACT(EPOCH FROM id) = $1::TEXT::NUMERIC FROM {}",
                        fixture.params.tables[0].full_table_name
                    ),
                    &[&expected],
                )
                .await
                .unwrap()
                .get(0);
            assert!(exact, "{source} -> {target} preserves exact epoch {expected}");
        } else {
            let actual: String = fixture
                .client
                .query_one(
                    &format!("SELECT id::text FROM {}", fixture.params.tables[0].full_table_name),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                actual, expected,
                "{source} -> {target} preserves its stored representation"
            );
        }
        super::recovery_boundary_tests::rollback(&fixture, 100, 100)
            .await
            .unwrap_or_else(|error| panic!("{source} -> {target}: {error:#}"));
        assert_eq!(fixture.position().await, (1, "10".into(), "99".into()), "{source} -> {target}");
        let provenance: i64 = fixture
            .client
            .query_one(
                &format!(
                    "SELECT rindexer_block_number FROM {}",
                    fixture.params.tables[0].full_table_name
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(provenance, 99, "{source} -> {target} canonical metadata uses the same key");
        assert_receipts(&fixture, 1).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_timestamp_operand_uses_epoch_seconds_in_both_directions() {
    let fixture = fixture(
        json!([]),
        json!({"name":"totals", "global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert",
            "set":[{"column":"amount","action":"add","value":"$rindexer_block_timestamp"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, seconds) in [(99, 1000), (100, 1001)] {
        let mut event = input(&fixture, block, U256::ZERO, vec![]);
        event.tx_information.block_timestamp = Some(U256::from(seconds));
        callback(vec![event]).await.unwrap();
    }
    assert_eq!(fixture.position().await, (2, "2001".into(), "100".into()));
    super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
    let (rows, amount, cursor) = fixture.position().await;
    assert_eq!((rows, cursor), (1, "99".into()));
    let exact: bool = fixture
        .client
        .query_one(
            &format!("SELECT amount = 1000 FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(exact, "epoch seconds must remain exact: {amount}");
    assert_receipts(&fixture, 1).await;
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_public_recovery_requires_complete_type_metadata() {
    use crate::{
        indexer::reorg::{
            task::{
                DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
                DerivedTableRollbackPlan, EventTableInfo, ReorgTask,
            },
            BlockChainWindow,
        },
        manifest::contract::{ColumnType, SetAction},
    };
    for source_only in [true, false] {
        let fixture = fixture(
            json!([]),
            json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"int256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
        )
        .await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(vec![input(&fixture, 99, U256::from(10), vec![])]).await.unwrap();
        callback(vec![input(&fixture, 100, U256::MAX, vec![])]).await.unwrap();
        let before = fixture.position().await;
        let schema =
            generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
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
                fixture.params.tables[0].full_table_name.clone(),
                false,
                vec![DerivedTableRollbackOp::try_new(
                    fixture.params.stored_table().to_owned(),
                    vec![],
                    vec![DerivedColumnRollback::try_new(
                        "amount".into(),
                        "value".into(),
                        SetAction::Add,
                    )
                    .unwrap()],
                    None,
                )
                .unwrap()],
                vec![],
            )
            .unwrap()],
            canonical_blocks: vec![(100, B256::repeat_byte(101), B256::repeat_byte(99))],
        };
        let mut window = BlockChainWindow::try_new(16).unwrap();
        let result = if source_only {
            let mut plan = DerivedTableRollbackPlan::default();
            plan.try_add_operation(
                fixture.params.tables[0].full_table_name.clone(),
                0,
                vec![],
                std::collections::HashMap::from([("value".into(), ColumnType::Uint256)]),
                std::collections::HashMap::new(),
            )
            .unwrap();
            task.execute_with_rollback_plan(&mut window, Some(&fixture.client), None, None, &plan)
                .await
        } else {
            task.execute(&mut window, Some(&fixture.client), None, None).await
        };
        let error = format!(
            "{:#}",
            result.err().expect("untyped/partial plans cannot prove the applied signed conversion")
        );
        assert!(
            error.contains("type metadata") && error.contains("execute_with_rollback_plan"),
            "{error}"
        );
        assert_eq!(fixture.position().await, before);
        assert_receipts(&fixture, 2).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_missing_json_array_key_cannot_become_an_empty_key() {
    use alloy::primitives::Address;
    let fixture = fixture(
        json!([{"name":"items","type":"address[][]","indexed":false}]),
        json!({"name":"totals", "columns":[
            {"name":"id","type":"address[]"},
            {"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"$items[0]"},
                "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, amount) in [(99, 10), (100, 5), (101, 5)] {
        let items = DynSolValue::Array(vec![DynSolValue::Array(vec![DynSolValue::Address(
            Address::repeat_byte(0x11),
        )])]);
        callback(vec![input(&fixture, block, U256::from(amount), vec![items])]).await.unwrap();
    }
    let before = fixture.position().await;
    for items in ["[]", "[[]]"] {
        fixture
            .client
            .batch_execute(&format!(
                "UPDATE {} SET items = '{items}'::JSONB WHERE block_number = 101",
                fixture.params.stored_table()
            ))
            .await
            .unwrap();
        let result = super::recovery_boundary_tests::rollback(&fixture, 100, 101).await;
        let error = format!(
            "{:#}",
            result.expect_err("missing/empty JSON keys cannot be reconstructed as admitted arrays")
        );
        assert!(error.contains("unresolved required arithmetic"), "{error}");
        assert_eq!(fixture.position().await, before);
        assert_receipts(&fixture, 3).await;
    }
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_legacy_canonical_missing_keys_abort_before_recovery_mutation() {
    for cross_chain in [false, true] {
        let mut definition = serde_json::to_value(params().event).unwrap();
        definition["inputs"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"items","type":"uint256[]","indexed":false}));
        let fixture = Fixture::with_table_on_networks(
            definition, "Contract",
            json!({"name":"totals", "cross_chain":cross_chain, "columns":[
                {"name":"id","type":"uint256","default":"1"},
                {"name":"amount","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"$items[0]"},
                    "set":[{"column":"amount","action":"add","value":"$value"}]}]}]}),
            if cross_chain { &["ethereum", "other"] } else { &["ethereum"] },
        ).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let canonical_network = if cross_chain { "other" } else { "ethereum" };
        let mut canonical =
            input(&fixture, 99, U256::from(10), vec![DynSolValue::Array(vec![uint(1)])]);
        canonical.tx_information.network = canonical_network.into();
        callback(vec![canonical]).await.unwrap();
        callback(vec![input(
            &fixture,
            100,
            U256::from(5),
            vec![DynSolValue::Array(vec![uint(1)])],
        )])
        .await
        .unwrap();
        // The older canonical application used id's default 1. Reconstructing
        // keys from NULL after reverting a valid orphan would delete its row.
        fixture.client.batch_execute(&format!("UPDATE {} SET items = '{{}}' WHERE network = '{canonical_network}' AND block_number = 99", fixture.params.stored_table())).await.unwrap();
        let before = fixture.position().await;
        let result = super::recovery_boundary_tests::rollback(&fixture, 100, 100).await;
        let error = format!(
            "{:#}",
            result.expect_err("canonical legacy inputs must be checked along with orphan inputs")
        );
        assert!(error.contains("unresolved required arithmetic"), "{error}");
        assert_eq!(fixture.position().await, before, "cross_chain={cross_chain}");
        assert_receipts(&fixture, 2).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_arithmetic_bytes32_array_keys_preserve_the_custom_encoder() {
    for shape in 0..4 {
        let key = DynSolValue::FixedBytes(B256::repeat_byte(0xab), 32);
        let (extra_inputs, reference, iterate, values) = match shape {
            0 => (
                json!([{"name":"keys","type":"bytes32[]","indexed":false}]),
                "$keys[0]",
                json!([]),
                vec![DynSolValue::Array(vec![key])],
            ),
            1 => (
                json!([{"name":"keys","type":"bytes32[1]","indexed":false}]),
                "$keys[0]",
                json!([]),
                vec![DynSolValue::FixedArray(vec![key])],
            ),
            _ => (
                json!([{"name":"items","type":"tuple[]","indexed":false,"components":[{"name":"key","type":"bytes32"}]}]),
                if shape == 2 { "$items[0].key" } else { "$item.key" },
                if shape == 2 { json!([]) } else { json!(["$items as item"]) },
                vec![DynSolValue::Array(vec![DynSolValue::Tuple(vec![key])])],
            ),
        };
        let fixture = fixture(extra_inputs, json!({"name":"totals", "columns":[
            {"name":"id","type":"bytes32"},
            {"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","iterate":iterate,"operations":[{"type":"upsert","where":{"id":reference},
                "set":[{"column":"amount","action":"add","value":"$value"}]}]}]})).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        for (block, amount) in [(99, 10), (100, 5)] {
            callback(vec![input(&fixture, block, U256::from(amount), values.clone())])
                .await
                .unwrap();
        }
        let stored: String = fixture
            .client
            .query_one(
                &format!(
                    "SELECT CONVERT_FROM(id, 'UTF8') FROM {}",
                    fixture.params.tables[0].full_table_name
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(stored, format!("0x{}", "ab".repeat(32)));
        super::recovery_boundary_tests::rollback(&fixture, 100, 100)
            .await
            .unwrap_or_else(|error| panic!("{reference}: {error:#}"));
        assert_eq!(fixture.position().await, (1, "10".into(), "99".into()), "{reference}");
        assert_receipts(&fixture, 1).await;
        drop(callback);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_column_identity_survives_forward_writes_and_rollback() {
    let fixture = fixture(
        json!([{"name":"key16","type":"uint256","indexed":false}]),
        json!({"name":"totals","columns":[{"name":"key16","type":"uint256"},
            {"name":"amount32","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert",
                "where":{"key16":"$key16"},
                "set":[{"column":"amount32","action":"add","value":"$value"}]}]}]}),
    )
    .await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, value) in [(99, 10), (100, 20)] {
        callback(vec![input(&fixture, block, U256::from(value), vec![uint(42)])]).await.unwrap();
    }
    let state = format!(
        "SELECT e.key_16::text, to_jsonb(t)->>'key_16', to_jsonb(t)->>'amount_32', \
         t.rindexer_block_number::bigint FROM {} t JOIN {} e ON e.block_number=t.rindexer_block_number",
        fixture.params.tables[0].full_table_name, fixture.params.stored_table()
    );
    let value = |r: tokio_postgres::Row| {
        (
            r.get::<_, String>(0),
            r.get::<_, Option<String>>(1),
            r.get::<_, Option<String>>(2),
            r.get::<_, i64>(3),
        )
    };
    assert_eq!(
        value(fixture.client.query_one(&state, &[]).await.unwrap()),
        ("42".into(), Some("42".into()), Some("30".into()), 100),
        "one naming rule must reach both stored tables without changing ABI lookup"
    );
    super::recovery_boundary_tests::rollback(&fixture, 100, 100).await.unwrap();
    assert_eq!(
        value(fixture.client.query_one(&state, &[]).await.unwrap()),
        ("42".into(), Some("42".into()), Some("10".into()), 99),
        "rollback must restore the canonical amount, key and block written through those names"
    );
    assert_receipts(&fixture, 1).await;
    drop(callback);
    fixture.finish().await;
}
