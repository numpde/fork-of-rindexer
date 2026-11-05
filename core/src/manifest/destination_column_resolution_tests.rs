//! Exercise the actual manifest admission and runtime projection boundaries.
use std::fs;

use serde_json::{json, Value};

use crate::{
    indexer::tables::TableRuntime,
    manifest::{
        contract::Table,
        core::Manifest,
        yaml::{read_manifest, ReadManifestError},
    },
};

fn counter(name: &str, columns: &[&str]) -> Value {
    let mut columns = columns
        .iter()
        .map(|name| json!({"name":name,"type":"uint256","default":"0"}))
        .collect::<Vec<_>>();
    columns.push(json!({"name":"amount","type":"uint256","default":"0"}));
    json!({"name":name,"global":true,"columns":columns,"events":[{"event":"Value","operations":[{"type":"upsert","set":[{"column":"amount","action":"increment"}]}]}]})
}

fn admit(tables: &[Value], native: bool) -> Result<Manifest, Box<ReadManifestError>> {
    let dir = tempfile::tempdir().unwrap();
    let abi = json!([{"type":"event","name":"Value","anonymous":false,"inputs":[]}]);
    fs::write(dir.path().join("abi.json"), serde_json::to_vec(&abi).unwrap()).unwrap();
    let mut spec = json!({"name":"NameAdmission","project_type":"no-code","storage":{"postgres":{"enabled":true}},"networks":[{"name":"test","chain_id":1,"rpc":"http://unused.invalid"}],"contracts":[]});
    if native {
        let mut tables = tables.to_vec();
        for table in &mut tables {
            table["events"][0]["event"] = json!("NativeTransfer");
        }
        spec["native_transfers"] = json!({"enabled":true,"tables":tables});
    } else {
        spec["contracts"] = json!([{"name":"Contract","abi":"abi.json","details":[{"network":"test","address":"0x0000000000000000000000000000000000000001"}],"tables":tables}]);
    }
    let path = dir.path().join("rindexer.yaml");
    fs::write(&path, serde_yaml::to_string(&spec).unwrap()).unwrap();
    read_manifest(&path).map_err(Box::new)
}

fn refused(table: Value, native: bool, names: &[&str]) {
    let error = admit(&[table], native)
        .err()
        .expect("startup accepted an ambiguous or unavailable physical destination")
        .to_string();
    for name in names {
        assert!(error.contains(name), "error must identify {name:?}: {error}");
    }
}

#[test]
fn rejects_distinct_logical_columns_with_one_storage_name() {
    refused(
        counter("totals", &["totalAmount", "total_amount"]),
        false,
        &["totals", "totalAmount", "total_amount"],
    );
}

#[test]
fn rejects_colliding_native_transfer_destinations() {
    refused(
        counter("totals", &["totalAmount", "total_amount"]),
        true,
        &["totals", "totalAmount", "total_amount"],
    );
}

#[test]
fn rejects_generated_and_internal_column_names() {
    for logical in
        ["Network", "rindexerBlockNumber", "rindexerSequenceId", "rindexerConditionInput", "ctid"]
    {
        refused(counter("totals", &[logical]), false, &["totals", logical]);
    }
}

#[test]
fn rejects_network_even_when_shared_keys_omit_it() {
    let mut table = counter("totals", &["Network", "id"]);
    table["cross_chain"] = json!(true);
    table["global"] = json!(false);
    table["events"][0]["operations"][0]["where"] = json!({"id":"1"});
    refused(table, false, &["totals", "Network"]);
}

#[test]
fn rejects_postgres_identifier_truncation() {
    let prefix = "x".repeat(63);
    admit(&[counter("totals", &[&prefix])], false).expect("63 bytes fit a PostgreSQL identifier");
    let overlong = format!("{prefix}_a");
    refused(counter("totals", &[&overlong]), false, &["totals", &overlong, "63"]);
}

#[test]
fn column_names_are_scoped_to_their_own_table_and_round_trip_logically() {
    let specs = [counter("first", &["totalAmount"]), counter("second", &["total_amount"])];
    let manifest =
        admit(&specs, false).expect("independent physical tables may reuse a column name");
    let tables = manifest.contracts[0].tables.as_ref().unwrap();
    let saved = serde_json::to_value(tables).unwrap();
    assert_eq!(saved[0]["columns"][0]["name"], "totalAmount");
    assert_eq!(saved[1]["columns"][0]["name"], "total_amount");
}

#[test]
fn runtime_projects_destination_references_once_without_rewriting_inputs_or_literals() {
    let table: Table = serde_json::from_value(json!({
        "name":"Balances","columns":[{"name":"tokenId","type":"uint256"},{"name":"totalAmount","type":"uint256"},{"name":"U256","type":"uint256"}],
        "events":[{"event":"TransferBatch","iterate":["$inputValues as itemAmount"],"operations":[{
            "type":"update","where":{"tokenId":"$transferInfo.tokenId"},
            "if":"@totalAmount < $itemAmount && $memo != '@totalAmount'",
            "set":[{"column":"totalAmount","action":"set","value":"$itemAmount"},{"column":"U256","action":"set","value":"$transferInfo.U256"}]
        }]}],
        "cron":[{"interval":"5m","operations":[{"type":"update","where":{"tokenId":"0"},
            "filter":"@totalAmount < 20", "set":[{"column":"totalAmount","action":"set","value":"$call($contract, \"totalAmount()\")"}]}]}]
    })).unwrap();
    let logical = serde_json::to_value(&table).unwrap();
    let runtime = TableRuntime::new(table.clone(), "NameAdmission", "Contract");
    let physical = &runtime.table;
    assert_eq!(
        physical.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["token_id", "total_amount", "u256"]
    );
    assert_eq!(physical.primary_key_columns(), ["token_id"]);
    let event = &physical.events[0];
    assert_eq!(event.event, "TransferBatch");
    assert_eq!(event.iterate[0].array_field, "inputValues");
    assert_eq!(event.iterate[0].alias, "itemAmount");
    let op = &event.operations[0];
    assert_eq!(op.where_clause["token_id"], "$transferInfo.tokenId");
    assert_eq!(op.condition(), Some("@total_amount < $itemAmount && $memo != '@totalAmount'"));
    assert_eq!(op.set[0].column, "total_amount");
    assert_eq!(op.set[0].effective_value(), "$itemAmount");
    assert_eq!(op.set[1].column, "u256");
    assert_eq!(op.set[1].effective_value(), "$transferInfo.U256");
    let cron = &physical.cron.as_ref().unwrap()[0].operations[0];
    assert_eq!(cron.where_clause["token_id"], "0");
    assert_eq!(cron.set[0].column, "total_amount");
    assert_eq!(cron.set[0].effective_value(), "$call($contract, \"totalAmount()\")");
    assert_eq!(cron.condition(), Some("@total_amount < 20"));
    assert_eq!(
        serde_json::to_value(table).unwrap(),
        logical,
        "the serializable declaration must remain logical"
    );
}
