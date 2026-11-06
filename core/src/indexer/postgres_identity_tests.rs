//! Check physical ownership through the same admission entry point as no-code startup.
use crate::{manifest::core::Manifest, PreparedProject};
use serde_json::{json, Value};

fn admit(contracts: &[(&str, &[&str])], customize: impl FnOnce(&mut Value)) -> Result<(), String> {
    let directory = tempfile::tempdir().unwrap();
    let mut definitions = Vec::new();
    for (i, (contract, events)) in contracts.iter().enumerate() {
        let abi = format!("{i}.json");
        let items: Vec<_> = events
            .iter()
            .map(|event| {
                json!({
                    "type": "event", "name": event, "anonymous": false, "inputs": []
                })
            })
            .collect();
        std::fs::write(directory.path().join(&abi), serde_json::to_vec(&items).unwrap()).unwrap();
        definitions.push(json!({
            "name": contract, "abi": abi, "include_events": events,
            "details": [{"network": "test", "address": "0x0000000000000000000000000000000000000001"}]
        }));
    }
    let mut manifest = json!({
        "name": "Demo", "project_type": "no-code", "storage": {"postgres": {"enabled": true}},
        "networks": [{"name":"test","chain_id":1,"rpc":"http://127.0.0.1:1"}],
        "contracts": definitions
    });
    customize(&mut manifest);
    let manifest: Manifest = serde_json::from_value(manifest).unwrap();
    PreparedProject::try_new(directory.path(), manifest).map(|_| ()).map_err(|e| e.to_string())
}

fn collision(result: Result<(), String>, expected: &[&str]) {
    let error = result.expect_err("PostgreSQL startup admitted conflicting storage owners");
    for part in expected {
        assert!(error.contains(part), "missing {part:?} in {error}");
    }
}

#[test]
fn postgres_identity_rejects_distinct_raw_events_with_one_table() {
    collision(
        admit(&[("Token", &["Value16", "Value_16"])], |_| {}),
        &["Value16", "Value_16", "demo_token.value_16"],
    );
}

#[test]
fn postgres_identity_compares_actual_catalog_identifiers() {
    let prefix = "v".repeat(63);
    let first = format!("{prefix}x");
    let second = format!("{prefix}y");
    collision(
        admit(&[("Token", &[&first, &second])], |_| {}),
        &[&first, &second, &format!("demo_token.{prefix}")],
    );
}

#[test]
fn postgres_identity_rejects_distinct_contracts_owning_one_reset_schema() {
    collision(
        admit(&[("Token16", &["First"]), ("Token_16", &["Second"])], |_| {}),
        &["Token16", "Token_16", "demo_token_16"],
    );
}

#[test]
fn postgres_identity_rejects_raw_and_custom_table_overlap() {
    collision(
        admit(&[("Token", &["Value"])], |m| {
            m["contracts"][0]["tables"] = json!([{
                "name":"value", "global":true, "columns":[{"name":"total","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert","set":[{"column":"total","action":"increment"}]}]}]
            }]);
        }),
        &["Value", "custom", "demo_token.value"],
    );
}

#[test]
fn postgres_identity_rejects_shared_checkpoint_even_when_raw_tables_differ() {
    collision(
        admit(&[("a_b", &["c"]), ("a", &["b_c"])], |_| {}),
        &["a_b::c", "a::b_c", "rindexer_internal.demo_a_b_c"],
    );
}

#[test]
fn postgres_identity_allows_multiple_details_of_one_declaration() {
    admit(&[("Token", &["Value"])], |m| {
        m["contracts"][0]["details"].as_array_mut().unwrap().push(json!({
            "network":"test", "address":"0x0000000000000000000000000000000000000002"
        }));
    })
    .unwrap();
}

#[test]
fn postgres_identity_rejects_raw_tables_overlapping_generated_relations() {
    for (event, relation) in [
        ("ValuePkey", "demo_token.value_pkey"),
        ("ValueRindexerIdSeq", "demo_token.value_rindexer_id_seq"),
    ] {
        for names in [["Value", event], [event, "Value"]] {
            collision(admit(&[("Token", &names)], |_| {}), &["Token::Value", event, relation]);
        }
    }
}

#[test]
fn postgres_identity_rejects_custom_table_using_explicit_engine_index() {
    collision(
        admit(&[("Token", &["Value"])], |m| {
            m["contracts"][0]["tables"] = json!([{
                "database":"rindexer_internal", "name":"stream_outbox_pending", "global":true,
                "columns":[{"name":"total","type":"uint256","default":"0"}],
                "events":[{"event":"Value","operations":[{"type":"upsert","set":[{"column":"total","action":"increment"}]}]}]
            }]);
        }),
        &["rindexer_internal.stream_outbox_pending", "index", "custom table Token"],
    );
}

#[test]
fn postgres_identity_allows_generated_suffixes_without_an_owner_in_the_same_schema() {
    admit(&[("First", &["Value"]), ("Second", &["ValuePkey", "ValueRindexerIdSeq"])], |_| {})
        .expect("only ownership in the same physical schema is ambiguous");
}
