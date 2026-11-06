//! Exercise startup, including expansion of the implicit native-transfer networks.
use serde_json::{json, Value};

fn table() -> Value {
    json!({
        "name":"totals","database":"accounting",
        "columns":[{"name":"bucket","type":"uint256"},{"name":"amount","type":"uint256","default":"0"}],
        "cron":[{"interval":"1m","operations":[operation()]}]
    })
}

fn operation() -> Value {
    json!({"type":"upsert","where":{"bucket":"1"},"set":[{"column":"amount","action":"increment"}]})
}

fn event_writer(table: &mut Value) {
    table["events"] = json!([{"event":"Value","operations":[operation()]}]);
}

fn manifest(table: Value) -> Value {
    let detail =
        |network| json!({"network":network,"address":"0x0000000000000000000000000000000000000001"});
    json!({
        "name":"Test","project_type":"no-code","storage":{"postgres":{"enabled":true}},
        "networks":[
            {"name":"archive","chain_id":1,"rpc":"http://127.0.0.1:1"},
            {"name":"recover","chain_id":2,"rpc":"http://127.0.0.1:1","reorg_handling":{"enabled":true}}
        ],
        "contracts":[{"name":"Token","abi":"abi.json","details":[detail("archive"),detail("recover")],"tables":[table]}]
    })
}

fn admit(manifest: Value) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("abi.json"),
        r#"[{"type":"event","name":"Value","anonymous":false,"inputs":[]}]"#,
    )
    .unwrap();
    let path = dir.path().join("rindexer.yaml");
    std::fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    crate::PreparedProject::read(&path).map(|_| ()).map_err(|e| e.to_string())
}

fn assert_rejected(manifest: Value, names: &[&str]) {
    let error =
        admit(manifest).expect_err("startup admitted a cron writer without rollback history");
    for name in ["cron", "accounting.totals"].into_iter().chain(names.iter().copied()) {
        assert!(error.contains(name), "missing {name:?}: {error}");
    }
}

#[test]
fn cron_recovery_rejects_cron_only_on_inherited_network() {
    assert_rejected(manifest(table()), &["recover"]);
}

#[test]
fn cron_recovery_rejects_mixed_writers_on_selected_network() {
    let mut table = table();
    event_writer(&mut table);
    table["cron"][0]["network"] = json!("recover");
    assert_rejected(manifest(table), &["recover"]);
}

#[test]
fn cron_recovery_rejects_native_writer_on_default_networks() {
    let mut manifest = manifest(table());
    manifest["contracts"] = json!([]);
    manifest["native_transfers"] = json!({"enabled":true,"tables":[table()]});
    assert_rejected(manifest, &["recover"]);
}

#[test]
fn cron_recovery_rejects_mixed_writers_sharing_cross_chain_rows() {
    let mut table = table();
    event_writer(&mut table);
    table["cross_chain"] = json!(true);
    table["cron"][0]["network"] = json!("archive");
    assert_rejected(manifest(table), &["archive", "recover", "cross_chain"]);
}

#[test]
fn cron_recovery_allows_disjoint_network_rows_and_event_only_recovery() {
    let mut table = table();
    event_writer(&mut table);
    table["cron"][0]["network"] = json!("archive");
    admit(manifest(table.clone()))
        .expect("network-scoped rows separate the cron and recovered writers");
    table.as_object_mut().unwrap().remove("cron");
    admit(manifest(table)).expect("recovered event writers keep their event-backed history");
}

#[test]
fn cron_recovery_allows_cross_chain_cron_only_on_unrecovered_network() {
    let mut table = table();
    table["cross_chain"] = json!(true);
    table["cron"][0]["network"] = json!("archive");
    admit(manifest(table)).expect("no recovered event contributes to the cron-only destination");
}
