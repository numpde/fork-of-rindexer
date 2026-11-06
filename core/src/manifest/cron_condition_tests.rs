//! Cron conditions must be rejected during manifest admission, not ignored at runtime.
use serde_json::{json, Value};

fn manifest() -> Value {
    json!({
        "name":"CronConditions",
        "project_type":"no-code",
        "storage":{"postgres":{"enabled":true}},
        "networks":[{"name":"test","chain_id":1,"rpc":"http://127.0.0.1:1"}],
        "contracts":[{
            "name":"Token",
            "abi":"abi.json",
            "details":[{
                "network":"test",
                "address":"0x0000000000000000000000000000000000000001"
            }],
            "tables":[{
                "name":"totals",
                "global":true,
                "columns":[{"name":"total","type":"uint256","default":"0"}],
                "events":[{
                    "event":"Value",
                    "operations":[{
                        "type":"upsert",
                        "if":"$value > 0",
                        "set":[{"column":"total","action":"increment","value":"$value"}]
                    }]
                }],
                "cron":[{
                    "interval":"1m",
                    "operations":[{
                        "type":"upsert",
                        "set":[{"column":"total","action":"increment","value":"9"}]
                    }]
                }]
            }]
        }]
    })
}

fn admit(value: Value) -> Result<(), String> {
    let project = tempfile::tempdir().unwrap();
    std::fs::write(
        project.path().join("abi.json"),
        r#"[{"type":"event","name":"Value","anonymous":false,"inputs":[{"name":"value","type":"uint256","indexed":false}]}]"#,
    )
    .unwrap();
    let manifest_path = project.path().join("rindexer.yaml");
    std::fs::write(&manifest_path, serde_yaml::to_string(&value).unwrap()).unwrap();
    crate::PreparedProject::read(&manifest_path).map(|_| ()).map_err(|e| e.to_string())
}

fn assert_condition_rejected(value: Value, field: &str, owner: &str) {
    let error = admit(value).expect_err("cron condition was accepted but would not be evaluated");
    for expected in [owner, "totals", "cron[0]", "operations[0]", field] {
        assert!(error.contains(expected), "missing {expected:?}: {error}");
    }
}

#[test]
fn cron_if_condition_is_rejected_before_setup() {
    let mut config = manifest();
    config["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["if"] = json!("@total < 0");
    assert_condition_rejected(config, "if", "Token");
}

#[test]
fn cron_legacy_filter_condition_is_rejected_before_setup() {
    let mut config = manifest();
    config["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["filter"] = json!("@total < 0");
    assert_condition_rejected(config, "filter", "Token");
}

#[test]
fn cron_if_and_filter_are_both_rejected() {
    let mut config = manifest();
    config["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["if"] = json!("1 == 0");
    config["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["filter"] = json!("1 == 0");
    assert_condition_rejected(config, "if", "Token");
}

#[test]
fn native_transfer_cron_conditions_are_rejected() {
    let mut config = manifest();
    let mut table = config["contracts"][0]["tables"][0].clone();
    table.as_object_mut().unwrap().remove("events");
    table["cron"][0]["operations"][0]["filter"] = json!("1 == 0");
    config["contracts"] = json!([]);
    config["native_transfers"] = json!({"enabled":true,"tables":[table]});
    assert_condition_rejected(config, "filter", "EvmTraces");
}

#[test]
fn unconditional_cron_and_conditional_event_operation_remain_supported() {
    admit(manifest()).expect("a cron without a condition and an event with if are supported");
}
