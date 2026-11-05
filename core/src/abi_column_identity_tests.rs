//! ABI source paths must identify a single raw storage column on every reader/writer.
use super::{get_abi_item_with_db_map, ABIItem};
use crate::{
    database::{
        clickhouse::generate::generate_tables_for_indexer_clickhouse,
        generate::generate_tables_for_indexer_sql,
    },
    manifest::{core::Manifest, yaml::read_manifest},
};
use serde_json::{json, Value};
use tempfile::TempDir;

fn scalar(name: &str) -> Value {
    json!({"name":name,"type":"uint256","indexed":false})
}
fn tuple(name: &str, kind: &str, mut children: Vec<Value>) -> Value {
    // Tuple members are ABI parameters; only the enclosing event input may
    // carry an indexed flag. Nested tuples have already cleaned their members.
    for child in &mut children {
        child.as_object_mut().unwrap().remove("indexed");
    }
    json!({"name":name,"type":kind,"indexed":false,"components":children})
}
fn event(inputs: Vec<Value>) -> Value {
    json!({"type":"event","name":"Value","anonymous":false,"inputs":inputs})
}
fn fixture(events: Vec<Value>, tables: Vec<Value>) -> (TempDir, Manifest) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("abi.json"), serde_json::to_vec(&events).unwrap()).unwrap();
    let spec = json!({"name":"Names", "project_type":"no-code", "networks":[{
    "name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}],
    "storage":{"postgres":{"enabled":true}}, "contracts":[{
        "name":"Contract","abi":"abi.json","include_events":["Value"],
        "details":[{"network":"ethereum","address":"0x0000000000000000000000000000000000000001"}],
        "tables":tables
    }]});
    std::fs::write(dir.path().join("rindexer.yaml"), serde_yaml::to_string(&spec).unwrap())
        .unwrap();
    (dir, serde_json::from_value(spec).unwrap())
}
fn assert_collision(inputs: Vec<Value>, paths: &[&str]) {
    // Tuple-array JSONB is PostgreSQL storage; the ClickHouse generator does not support that type.
    let check_clickhouse = !inputs.iter().any(|input| input["type"] == "tuple[]");
    let (dir, manifest) = fixture(vec![event(inputs)], vec![]);
    let indexer = manifest.to_indexer();
    // Exercise DDL directly too: callers can construct an Indexer without using read_manifest.
    let mut results = vec![
        ("manifest", read_manifest(&dir.path().join("rindexer.yaml")).err().map(|e| e.to_string())),
        (
            "PostgreSQL DDL",
            generate_tables_for_indexer_sql(dir.path(), &indexer, false)
                .err()
                .map(|e| e.to_string()),
        ),
    ];
    if check_clickhouse {
        results.push((
            "ClickHouse DDL",
            generate_tables_for_indexer_clickhouse(dir.path(), &indexer, false)
                .err()
                .map(|e| e.to_string()),
        ));
    }
    for (entry, error) in &results {
        let error = error.as_ref().unwrap_or_else(|| {
            panic!("{entry} accepted ambiguous ABI paths {paths:?}; outcomes: {results:?}")
        });
        assert!(
            error.contains("Value") && paths.iter().all(|path| error.contains(path)),
            "{entry} must identify the event and ambiguous source paths: {error}"
        );
    }
}

#[test]
fn raw_abi_column_identity_rejects_duplicate_logical_names() {
    assert_collision(vec![scalar("same"), scalar("same")], &["same"]);
}
#[test]
fn raw_abi_column_identity_rejects_normalized_numeric_aliases() {
    assert_collision(vec![scalar("key16"), scalar("key_16")], &["key16", "key_16"]);
}
#[test]
fn raw_abi_column_identity_rejects_flattened_tuple_aliases() {
    assert_collision(
        vec![tuple("position", "tuple", vec![scalar("tokenId")]), scalar("position_token_id")],
        &["position.tokenId", "position_token_id"],
    );
}
#[test]
fn raw_abi_column_identity_rejects_catalog_truncation_aliases() {
    let prefix = "a".repeat(63);
    let a = format!("{prefix}x");
    let b = format!("{prefix}y");
    assert_collision(vec![scalar(&a), scalar(&b)], &[&a, &b]);
}
#[test]
fn raw_abi_column_identity_checks_utf8_catalog_boundary() {
    let prefix = "a".repeat(62);
    let a = format!("{prefix}税x");
    let b = format!("{prefix}ηy");
    assert_collision(vec![scalar(&a), scalar(&b)], &[&a, &b]);
}
#[test]
fn raw_abi_column_identity_rejects_duplicate_json_member_names() {
    assert_collision(
        vec![tuple("entries", "tuple[]", vec![scalar("id"), scalar("id")])],
        &["entries", "id"],
    );
}
#[test]
fn raw_abi_column_identity_preserves_reserved_fields_hashes_and_literal_json_members() {
    let mut hash = tuple("digest", "tuple", vec![scalar("key16"), scalar("key_16")]);
    hash["indexed"] = json!(true);
    let inputs = vec![
        scalar("blockNumber"),
        scalar("rindexer_abi_block_number"),
        scalar("rindexer_id"),
        scalar("ctid"),
        scalar("rindexer_abi_ctid"),
        hash,
        tuple("entries", "tuple[]", vec![scalar("key16"), scalar("key_16")]),
        tuple("position", "tuple", vec![scalar("tokenId")]),
        scalar("entry_id"),
    ];
    let ch_inputs = inputs.iter().filter(|input| input["type"] != "tuple[]").cloned().collect();
    let (dir, manifest) = fixture(vec![event(inputs)], vec![]);
    read_manifest(&dir.path().join("rindexer.yaml")).expect("valid ABI source paths");
    let indexer = manifest.to_indexer();
    let pg = generate_tables_for_indexer_sql(dir.path(), &indexer, false).unwrap();
    assert!(pg.as_str().contains("\"entries\" JSONB"));
    let (ch_dir, ch_manifest) = fixture(vec![event(ch_inputs)], vec![]);
    let ch =
        generate_tables_for_indexer_clickhouse(ch_dir.path(), &ch_manifest.to_indexer(), false)
            .unwrap();
    for ddl in [&pg, &ch] {
        for name in [
            "rindexer_abi_block_number",
            "rindexer_abi_rindexer_abi_block_number",
            "rindexer_abi_rindexer_id",
            "rindexer_abi_ctid",
            "rindexer_abi_rindexer_abi_ctid",
            "digest",
            "position_token_id",
        ] {
            assert!(ddl.as_str().contains(&format!("\"{name}\" ")), "{name} missing from {ddl}");
        }
        assert!(!ddl.as_str().contains("\"ctid\" "), "system columns retain their identities");
        assert!(!ddl.as_str().contains("digest_key"), "indexed tuples store only the hash");
        assert!(
            !ddl.as_str().contains("entries_key"),
            "JSON members remain within one stored column"
        );
    }
}
#[test]
fn raw_abi_column_identity_indexes_and_relationships_use_the_stored_leaf() {
    let items: Vec<ABIItem> = serde_json::from_value(json!([event(vec![
        scalar("blockNumber"),
        tuple("tx", "tuple", vec![scalar("hash")]),
        tuple("nested", "tuple", vec![scalar("nested")]),
        scalar("U256")
    ])]))
    .unwrap();
    for (path, expected) in [
        (&["blockNumber"][..], "rindexer_abi_block_number"),
        (&["tx", "hash"][..], "rindexer_abi_tx_hash"),
        (&["nested", "nested"][..], "nested_nested"),
        (&["U256"][..], "u256"),
    ] {
        let mapped = get_abi_item_with_db_map(&items, "Value", path).unwrap();
        assert_eq!(mapped.db_column_name, expected, "mapping {path:?}");
    }
}
#[test]
fn raw_abi_column_identity_excludes_events_not_stored_by_this_contract() {
    let mut unused = event(vec![scalar("key16"), scalar("key_16")]);
    unused["name"] = json!("Unused");
    let (dir, manifest) = fixture(vec![event(vec![scalar("value")]), unused], vec![]);
    read_manifest(&dir.path().join("rindexer.yaml"))
        .expect("excluded ABI events have no raw layout");
    generate_tables_for_indexer_sql(dir.path(), &manifest.to_indexer(), false).unwrap();
}

#[test]
fn clickhouse_custom_column_identity_ddl_projects_declarations_and_order_by_once() {
    let table = json!({"name":"Totals","database":"shared_state","columns":[
        {"name":"key16","type":"uint256"},{"name":"U256","type":"uint256"},{"name":"amountValue","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","where":{"key16":"$value"},"set":[
            {"column":"U256","action":"set","value":"$value"},{"column":"amountValue","action":"add","value":"$value"}]}]}]});
    let (dir, manifest) = fixture(vec![event(vec![scalar("value")])], vec![table]);
    let ddl =
        generate_tables_for_indexer_clickhouse(dir.path(), &manifest.to_indexer(), false).unwrap();
    for required in [
        "shared_state.totals",
        "`key_16` UInt256",
        "`u256` UInt256",
        "`amount_value` UInt256",
        "ORDER BY (`network`, `key_16`)",
    ] {
        assert!(ddl.as_str().contains(required), "missing physical declaration {required}: {ddl}");
    }
    assert!(!ddl.as_str().contains("`u_256`"), "a resolved name must not be projected twice");
}
#[test]
fn clickhouse_custom_column_identity_ddl_rejects_ambiguous_destinations() {
    let table = json!({"name":"Totals","columns":[{"name":"key16","type":"uint256"},{"name":"key_16","type":"uint256"}],
        "events":[{"event":"Value","operations":[{"type":"insert","set":[{"column":"key16","action":"set","value":"$value"},{"column":"key_16","action":"set","value":"$value"}]}]}]});
    let (dir, manifest) = fixture(vec![event(vec![scalar("value")])], vec![table]);
    let error = generate_tables_for_indexer_clickhouse(dir.path(), &manifest.to_indexer(), false)
        .expect_err("programmatic DDL callers must not emit an ambiguous custom schema")
        .to_string();
    assert!(error.contains("key16") && error.contains("key_16"), "{error}");
}

#[test]
fn raw_abi_column_identity_unicode_acronym_lookahead_uses_characters() {
    use crate::helpers::{camel_to_snake, camel_to_snake_advanced};
    assert_eq!(camel_to_snake("税AB"), "税_ab");
    assert_eq!(camel_to_snake_advanced("税AB", true), "税_ab");
    assert_eq!(camel_to_snake("U256"), "u256");
    assert_eq!(camel_to_snake("key16"), "key_16");
}

#[test]
fn postgres_custom_column_identity_refuses_truncated_legacy_literal() {
    use crate::{
        database::generate::check_legacy_column_names, helpers::postgres_catalog_identifier,
        manifest::contract::Table,
    };
    let logical = format!("{}---b", "a".repeat(61));
    let declaration: Table = serde_json::from_value(json!({"name":"Totals","global":true,
        "columns":[{"name":logical,"type":"uint256"}],"events":[{"event":"Value","operations":[{
            "type":"upsert","set":[{"column":logical,"action":"set","value":"$value"}]}]}]}))
    .unwrap();
    let resolved = declaration.try_resolve_storage_columns().unwrap();
    let legacy = format!("{}--", "a".repeat(61));
    let existing = std::collections::HashSet::from([legacy.clone()]);
    let error =
        check_legacy_column_names(&declaration, &resolved, &existing, postgres_catalog_identifier)
            .expect_err(
            "PostgreSQL stored a truncated literal; never offer to drop it as an unrelated column",
        );
    assert!(
        error.contains(&legacy)
            && error.contains(&logical)
            && error.contains(resolved.storage_column(&logical).unwrap()),
        "{error}"
    );
    // The truncated spelling is not the old literal in a ClickHouse catalogue.
    check_legacy_column_names(&declaration, &resolved, &existing, |name| name).unwrap();
}
