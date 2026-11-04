//! One event mapping per table keeps forward operations and recovery plans aligned.
//! Exercise the existing manifest, preparation and planning entry points so callers
//! constructing a TableRuntime directly cannot bypass admission.
use std::{collections::HashMap, sync::Arc};

use alloy::{
    dyn_abi::DynSolValue,
    primitives::{Address, B256, U256},
};
use serde_json::{json, Value};

use super::build_derived_tables_for_event;
use crate::{
    database::sql_type_wrapper::EthereumSqlTypeWrapper,
    indexer::{
        reorg::{DerivedTableInfo, DerivedTableRollbackPlan},
        tables::{prepare_table_operations, PreparedTableOps, TableRuntime, TxMetadata},
    },
    manifest::{
        contract::{ColumnType, Table},
        core::{Constants, Manifest},
        yaml::{read_manifest, ReadManifestError},
    },
    system_state::_test_reset_shutdown_flag,
    types::core::LogParam,
};

fn table_definition(event: &str) -> Value {
    json!({
        "name": "totals", "global": true,
        "columns": [{"name": "amount", "type": "uint256", "default": "0"}],
        "events": [{"event": event, "operations": [
            {"type": "upsert", "set": [{"column": "amount", "action": "add", "value": "$value"}]}
        ]}]
    })
}

fn duplicate_mapping(mut table: Value) -> Value {
    let mapping = table["events"][0].clone();
    table["events"].as_array_mut().unwrap().push(mapping);
    table
}

fn manifest_with_tables(
    tables: &[Value],
    reorg: bool,
    native: bool,
) -> Result<Manifest, Box<ReadManifestError>> {
    let directory = tempfile::tempdir().unwrap();
    let events = ["Value", "Other"].map(|name| {
        json!({
            "type": "event", "name": name, "anonymous": false,
            "inputs": [
                {"name": "value", "type": "uint256", "indexed": false},
                {"name": "values", "type": "uint256[]", "indexed": false},
                {"name": "other_values", "type": "uint256[]", "indexed": false}
            ]
        })
    });
    std::fs::write(directory.path().join("abi.json"), serde_json::to_vec(&events).unwrap())
        .unwrap();
    let mut manifest = json!({
        "name": "mapping_admission", "project_type": "no-code",
        "networks": [{"name": "ethereum", "chain_id": 1, "rpc": "http://unused.invalid",
            "reorg_handling": {"enabled": reorg}}],
        "storage": {"postgres": {"enabled": true}},
        "contracts": []
    });
    if native {
        manifest["native_transfers"] = json!({"enabled": true, "tables": tables});
    } else {
        manifest["contracts"] = json!([{
            "name": "Contract", "abi": "abi.json",
            "details": [{"network": "ethereum", "address": "0x0000000000000000000000000000000000000001"}],
            "tables": tables
        }]);
    }
    let path = directory.path().join("rindexer.yaml");
    std::fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    read_manifest(&path).map_err(Box::new)
}

fn assert_duplicate_error(error: &str, event: &str) {
    assert!(
        error.contains("duplicate")
            && error.contains("totals")
            && error.contains(event)
            && error.contains("one mapping"),
        "admission must identify the table/event and explain consolidation: {error}"
    );
}

fn assert_manifest_rejects_duplicate(reorg: bool) {
    let table = table_definition("Value");
    manifest_with_tables(std::slice::from_ref(&table), reorg, false).expect("one mapping is valid");
    let error = manifest_with_tables(&[duplicate_mapping(table)], reorg, false)
        .expect_err("startup must reject duplicate event mappings, not silently choose the first");
    assert_duplicate_error(&error.to_string(), "Value");
    assert!(error.to_string().contains("Contract"), "{error}");
}

#[test]
fn manifest_rejects_duplicate_event_mappings_without_reorg() {
    assert_manifest_rejects_duplicate(false);
}

#[test]
fn manifest_rejects_duplicate_event_mappings_with_reorg() {
    assert_manifest_rejects_duplicate(true);
}

#[test]
fn manifest_rejects_duplicate_native_transfer_event_mappings() {
    for reorg in [false, true] {
        let table = table_definition("NativeTransfer");
        manifest_with_tables(std::slice::from_ref(&table), reorg, true)
            .expect("one native mapping is valid");
        let error = manifest_with_tables(&[duplicate_mapping(table)], reorg, true)
            .expect_err("native transfer tables need the same event-mapping admission");
        assert_duplicate_error(&error.to_string(), "NativeTransfer");
        assert!(error.to_string().contains("native_transfers"), "{error}");
    }
}

#[test]
fn manifest_rejects_duplicate_event_mappings_with_different_iterate_scopes() {
    let mut table = table_definition("Value");
    table["events"][0]["iterate"] = json!(["$values as item"]);
    table["events"][0]["operations"][0]["set"][0]["value"] = json!("$item");
    manifest_with_tables(std::slice::from_ref(&table), true, false)
        .expect("one iterate mapping is valid");
    let mut duplicate = duplicate_mapping(table);
    duplicate["events"][1]["iterate"] = json!(["$other_values as item"]);
    let error = manifest_with_tables(&[duplicate], true, false)
        .expect_err("different iteration scopes cannot be merged implicitly");
    assert_duplicate_error(&error.to_string(), "Value");
    assert!(error.to_string().contains("iterate"), "{error}");
}

fn runtime(table: Value) -> TableRuntime {
    let table: Table = serde_json::from_value(table).expect("table shape remains deserializable");
    TableRuntime::new(table, "mapping_admission", "Contract")
}

async fn prepare(tables: &[TableRuntime]) -> Result<PreparedTableOps<'_>, String> {
    _test_reset_shutdown_flag();
    let events = vec![(
        vec![LogParam::new("value".into(), DynSolValue::Uint(U256::from(7), 256))],
        "ethereum".into(),
        TxMetadata {
            block_number: 100,
            block_timestamp: Some(U256::from(1_700_000_000)),
            tx_hash: B256::repeat_byte(1),
            block_hash: B256::repeat_byte(2),
            contract_address: Address::repeat_byte(3),
            log_index: U256::ZERO,
            tx_index: 0,
        },
    )];
    prepare_table_operations(
        tables,
        "Value",
        &events,
        Arc::new(HashMap::new()),
        &Constants::new(),
        &HashMap::new(),
    )
    .await
}

fn plan(
    tables: &[TableRuntime],
    derived: &mut HashMap<String, Vec<DerivedTableInfo>>,
    plans: &mut HashMap<String, DerivedTableRollbackPlan>,
) -> anyhow::Result<()> {
    build_derived_tables_for_event(
        "Value",
        "mapping_admission",
        "Contract",
        "ethereum",
        &HashMap::from([("value".into(), ColumnType::Uint256)]),
        tables,
        derived,
        plans,
    )
}

#[tokio::test]
async fn runtime_preparation_rejects_duplicate_event_mappings() {
    let tables = [runtime(duplicate_mapping(table_definition("Value")))];
    let error = match prepare(&tables).await {
        Err(error) => error,
        Ok(prepared) => panic!(
            "runtime accepted duplicate mappings and prepared only {} operations for two mappings",
            prepared.ops.len()
        ),
    };
    assert_duplicate_error(&error, "Value");
}

#[test]
fn recovery_planning_rejects_duplicate_event_mappings_before_mutating_outputs() {
    let mut valid = table_definition("Value");
    valid["name"] = json!("valid_first");
    let tables = [runtime(valid), runtime(duplicate_mapping(table_definition("Value")))];
    let mut derived = HashMap::new();
    let mut plans = HashMap::new();
    let result = plan(&tables, &mut derived, &mut plans);
    assert!(
        result.is_err(),
        "recovery accepted duplicate mappings and planned {} inverse operations for totals",
        derived.get("ethereum").map_or(0, |tables| tables
            .iter()
            .find(|table| table.full_table_name.ends_with(".totals"))
            .map_or(0, |table| table.rollback_ops.len()))
    );
    assert_duplicate_error(&result.unwrap_err().to_string(), "Value");
    assert!(derived.is_empty(), "admission must precede all derived metadata changes");
    assert!(plans.is_empty(), "admission must precede all rollback-plan changes");
}

#[tokio::test]
async fn one_event_mapping_preserves_all_operations_and_allows_other_tables() {
    let mut table = table_definition("Value");
    let operation = table["events"][0]["operations"][0].clone();
    table["events"][0]["operations"].as_array_mut().unwrap().push(operation);
    let mut other = table_definition("Value");
    other["name"] = json!("other_totals");
    let manifest = manifest_with_tables(&[table, other], true, false)
        .expect("multiple operations and the same event in distinct tables are valid");
    let tables = manifest.contracts[0]
        .tables
        .as_ref()
        .unwrap()
        .iter()
        .cloned()
        .map(|table| TableRuntime::new(table, "mapping_admission", "Contract"))
        .collect::<Vec<_>>();
    let prepared = prepare(&tables).await.expect("valid mappings prepare");
    assert_eq!(prepared.ops.len(), 3);
    for operation in &prepared.ops {
        assert_eq!(operation.rows.len(), 1);
        assert!(matches!(operation.rows[0].columns.get("amount"),
            Some(EthereumSqlTypeWrapper::U256Numeric(value)) if *value == U256::from(7)));
        assert_eq!(operation.batch_max_blocks.get("ethereum"), Some(&100));
    }
    let mut derived = HashMap::new();
    let mut plans = HashMap::new();
    plan(&tables, &mut derived, &mut plans).expect("all valid operations have inverse metadata");
    assert_eq!(derived["ethereum"].len(), 2);
    assert_eq!(derived["ethereum"][0].rollback_ops.len(), 2);
    assert_eq!(derived["ethereum"][1].rollback_ops.len(), 1);
}

#[test]
fn distinct_event_names_remain_valid_within_one_table() {
    let mut table = table_definition("Value");
    let mut other_mapping = table["events"][0].clone();
    other_mapping["event"] = json!("Other");
    table["events"].as_array_mut().unwrap().push(other_mapping);
    manifest_with_tables(&[table], true, false).expect("uniqueness is scoped to an event name");
}
