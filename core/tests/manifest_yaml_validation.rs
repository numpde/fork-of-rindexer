use std::{env, fs, path::Path};

use rindexer::manifest::core::{Manifest, ProjectType};
use rindexer::manifest::yaml::{read_manifest, read_manifest_raw, write_manifest};

const ERC20_ABI: &str = r#"[
  {
    "type": "event",
    "name": "Transfer",
    "inputs": [
      { "indexed": true, "name": "from", "type": "address" },
      { "indexed": true, "name": "to", "type": "address" },
      { "name": "value", "type": "uint256" }
    ]
  },
  {
    "type": "event",
    "name": "Approval",
    "inputs": [
      { "indexed": true, "name": "owner", "type": "address" },
      { "indexed": true, "name": "spender", "type": "address" },
      { "name": "value", "type": "uint256" }
    ]
  }
]"#;

fn write_project_file(project: &Path, relative_path: &str, contents: &str) {
    let full_path = project.join(relative_path);
    fs::create_dir_all(full_path.parent().expect("file has a parent")).expect("parent dir");
    fs::write(full_path, contents).expect("file writes");
}

fn write_abi(project: &Path) {
    write_project_file(project, "abis/token.json", ERC20_ABI);
}

fn manifest_path(project: &Path) -> std::path::PathBuf {
    project.join("rindexer.yaml")
}

fn base_manifest(contract_details: &str, extra_contract_fields: &str) -> String {
    format!(
        r#"
name: coverage
project_type: no-code
networks:
  - name: mainnet
    chain_id: 1
    rpc: http://localhost:8545
contracts:
  - name: Token
    details:
{contract_details}
    abi: ./abis/token.json
{extra_contract_fields}
"#
    )
}

#[test]
fn read_manifest_raw_rejects_duplicate_contract_names_before_abi_validation() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let yaml = r#"
name: coverage
project_type: no-code
networks: []
contracts:
  - name: Token
    details: []
    abi: ./missing.json
  - name: Token
    details: []
    abi: ./missing.json
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest_raw(&path).expect_err("duplicate names should fail");

    assert!(err.to_string().contains("Contract names Token must be unique"));
}

#[test]
fn read_manifest_rejects_contract_detail_for_unknown_network() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: optimism
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1""#,
        "    include_events:\n      - Transfer\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("unknown network should fail");

    let err = err.to_string();
    assert!(
        err.contains("Invalid network mapped to contract: network - optimism contract - Token"),
        "{err}"
    );
}

#[test]
fn read_manifest_rejects_missing_include_event() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: mainnet
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1""#,
        "    include_events:\n      - MissingEvent\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("missing include event should fail");

    let err = err.to_string();
    assert!(
        err.contains(
            "Event MissingEvent included in include_events for contract Token but not found in ABI"
        ),
        "{err}"
    );
}

#[test]
fn read_manifest_rejects_indexed_filter_that_defines_too_many_topics() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: mainnet
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1"
        indexed_filters:
          - event_name: Transfer
            indexed_1:
              - 0x0000000000000000000000000000000000000001
            indexed_2:
              - 0x0000000000000000000000000000000000000002
            indexed_3:
              - 0x0000000000000000000000000000000000000003"#,
        "    include_events:\n      - Transfer\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("too many indexed filters should fail");

    let err = err.to_string();
    assert!(
        err.contains(
            "Indexed filter defined more than allowed for event Transfer for contract Token"
        ),
        "{err}"
    );
}

#[test]
fn read_manifest_expands_simple_native_transfers_to_all_root_networks() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let yaml = r#"
name: coverage
project_type: no-code
native_transfers: true
networks:
  - name: mainnet
    chain_id: 1
    rpc: http://localhost:8545
  - name: base
    chain_id: 8453
    rpc: http://localhost:8546
contracts: []
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let manifest = read_manifest(&path).expect("manifest reads");
    let networks = manifest.native_transfers.networks.as_ref().expect("native transfer networks");

    assert_eq!(networks.len(), 2);
    assert_eq!(networks[0].network, "mainnet");
    assert_eq!(networks[1].network, "base");
    assert!(manifest.has_any_live_indexing());
}

#[test]
fn read_manifest_keeps_rust_project_network_rpc_as_env_placeholder() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    env::set_var("COVERAGE_RPC_URL", "http://localhost:8545");
    let yaml = r#"
name: coverage
project_type: rust
networks:
  - name: mainnet
    chain_id: 1
    rpc: ${COVERAGE_RPC_URL}
contracts: []
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let manifest = read_manifest(&path).expect("manifest reads");

    assert_eq!(manifest.project_type, ProjectType::Rust);
    assert_eq!(manifest.networks[0].rpc, "COVERAGE_RPC_URL");
}

#[test]
fn write_manifest_round_trips_manifest_to_yaml_file() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let manifest: Manifest = serde_yaml::from_str(
        r#"
name: coverage
project_type: no-code
networks: []
contracts: []
graphql:
  port: 4001
"#,
    )
    .expect("manifest parses");
    let path = manifest_path(temp_dir.path());

    write_manifest(&manifest, &path).expect("manifest writes");

    let written = fs::read_to_string(path).expect("manifest reads");
    assert!(written.contains("name: coverage"));
    assert!(written.contains("project_type: no-code"));
    assert!(written.contains("port: 4001"));
}

fn table_condition_manifest(condition: &str, table_options: &str) -> String {
    base_manifest(
        r#"      - network: mainnet
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1""#,
        &format!(
            r#"    tables:
      - name: balances
{table_options}
        columns:
          - name: holder
            type: address
          - name: balance
            type: uint256
        events:
          - event: Transfer
            operations:
              - type: upsert
                where:
                  holder: $to
                if: "{condition}"
                set:
                  - column: balance
                    action: set
                    value: $value
"#
        ),
    )
}

#[test]
fn table_conditions_validate_parsed_event_and_stored_references_without_scanning_literals() {
    let dir = tempfile::tempdir().unwrap();
    write_abi(dir.path());
    for (condition, options) in [
        ("@balance < $value && @holder == $to", ""),
        ("!(@balance + 1 > value * 2) || from == to", ""),
        ("@holder != @holder", ""),
        ("@rindexer_sequence_id < $rindexer_block_number", ""),
        ("@network == 'mainnet' && 'Token A' != 'Token B'", ""),
        ("'Token A' != 'Token B' && $value > 0", ""),
        ("@rindexer_block_timestamp > 0", "        timestamp: true"),
    ] {
        fs::write(manifest_path(dir.path()), table_condition_manifest(condition, options)).unwrap();
        read_manifest(&manifest_path(dir.path()))
            .unwrap_or_else(|error| panic!("{condition}: {error}"));
    }
}

#[test]
fn table_conditions_reject_unknown_references_in_their_actual_namespace() {
    let dir = tempfile::tempdir().unwrap();
    write_abi(dir.path());
    for (condition, options, message) in [
        ("@missing == 1", "", "Custom indexing field 'missing'"),
        ("$missing == 1", "", "Custom indexing event field '$missing'"),
        ("@value == 1", "", "Custom indexing field 'value'"),
        ("$balance == 1", "", "Custom indexing event field '$balance'"),
        ("value > 0 && !(1 + @missing < 2)", "", "Custom indexing field 'missing'"),
        ("@balance < 1 + $missing", "", "Custom indexing event field '$missing'"),
        ("@rindexer_tx_index == 1", "", "Custom indexing field 'rindexer_tx_index'"),
        ("@rindexer_block_timestamp > 0", "", "Custom indexing field 'rindexer_block_timestamp'"),
        ("@network == 'mainnet'", "        cross_chain: true", "Custom indexing field 'network'"),
        ("@rindexer_id > 0", "", "Custom indexing field 'rindexer_id'"),
    ] {
        fs::write(manifest_path(dir.path()), table_condition_manifest(condition, options)).unwrap();
        let error = read_manifest(&manifest_path(dir.path())).unwrap_err().to_string();
        assert!(error.contains(message), "{condition}: {error}");
    }
}

fn mixed_insert_manifest(operation: &str, reorg_enabled: bool, include_insert: bool) -> String {
    let mut operations = vec![];
    if include_insert {
        operations.push(serde_json::json!({"type":"insert", "set":[
            {"column":"id", "action":"set", "value":"$from"},
            {"column":"amount", "action":"set", "value":"$value"}
        ]}));
    }
    if !operation.is_empty() {
        operations.push(serde_json::json!({"type":operation, "where":{"id":"$from"},
            "set": if operation == "delete" { vec![] } else { vec![serde_json::json!({"column":"amount", "action":"add", "value":"$value"})] }
        }));
    }
    serde_yaml::to_string(&serde_json::json!({
        "name":"recovery", "project_type":"no-code", "storage":{"postgres":{"enabled":true}},
        "networks":[{"name":"mainnet", "chain_id":1, "rpc":"http://unused.invalid",
            "reorg_handling":{"enabled":reorg_enabled}}],
        "contracts":[{"name":"Token", "abi":"./abis/token.json",
            "details":[{"network":"mainnet", "address":"0x0000000000000000000000000000000000000001", "start_block":"99"}],
            "tables":[{"name":"balances", "columns":[{"name":"id", "type":"address"},
                {"name":"amount", "type":"uint256", "default":"0"}],
                "events":[{"event":"Transfer", "operations":operations}]}]
        }]
    })).unwrap()
}

#[test]
fn read_manifest_rejects_insert_with_mutable_operations_during_reorg_recovery() {
    for operation in ["update", "upsert", "delete"] {
        let directory = tempfile::tempdir().unwrap();
        write_abi(directory.path());
        let path = manifest_path(directory.path());
        fs::write(&path, mixed_insert_manifest(operation, true, true)).unwrap();
        let error = read_manifest(&path).expect_err(
            "recovery must reject mixed inserts before an orphan update can delete canonical creation",
        ).to_string();
        assert!(
            error.contains("balances") && error.contains("insert") && error.contains("reorg"),
            "{operation}: {error}"
        );
    }
}

#[test]
fn read_manifest_keeps_supported_insert_and_mutable_recovery_shapes() {
    for (operation, enabled, insert) in
        [("", true, true), ("upsert", true, false), ("update", false, true)]
    {
        let directory = tempfile::tempdir().unwrap();
        write_abi(directory.path());
        let path = manifest_path(directory.path());
        fs::write(&path, mixed_insert_manifest(operation, enabled, insert)).unwrap();
        read_manifest(&path)
            .expect("insert-only, mutable-only and non-reorg mixed tables remain valid");
    }
}

#[test]
fn read_manifest_rejects_native_insert_with_mutable_reorg_operations() {
    let directory = tempfile::tempdir().unwrap();
    let mut manifest: serde_json::Value =
        serde_yaml::from_str(&mixed_insert_manifest("update", true, true)).unwrap();
    let mut tables = manifest["contracts"][0]["tables"].take();
    tables[0]["events"][0]["event"] = "NativeTransfer".into();
    manifest["contracts"] = serde_json::json!([]);
    manifest["native_transfers"] = serde_json::json!({"enabled":true,
        "networks":[{"network":"mainnet", "start_block":"99"}], "tables":tables});
    let path = manifest_path(directory.path());
    fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    let error = read_manifest(&path)
        .expect_err("native tables need the same recovery admission rule")
        .to_string();
    assert!(
        error.contains("balances") && error.contains("insert") && error.contains("reorg"),
        "{error}"
    );
}

fn recovery_table_manifest(
    table: serde_json::Value,
    enabled: bool,
    native: bool,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    write_abi(directory.path());
    let mut manifest: serde_json::Value =
        serde_yaml::from_str(&mixed_insert_manifest("", enabled, false)).unwrap();
    if native {
        let mut table = table;
        table["events"][0]["event"] = "NativeTransfer".into();
        manifest["contracts"] = serde_json::json!([]);
        manifest["native_transfers"] = serde_json::json!({"enabled":true, "networks":[{"network":"mainnet"}], "tables":[table]});
    } else {
        manifest["contracts"][0]["tables"] = serde_json::json!([table]);
    }
    let path = manifest_path(directory.path());
    fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    (directory, path)
}

fn extrema_recovery_table(action: &str, tracked_creator: bool) -> serde_json::Value {
    let mut creator =
        vec![serde_json::json!({"column":"amount", "action":"add", "value":"$value"})];
    if tracked_creator {
        creator.push(serde_json::json!({"column":"extreme", "action":action, "value":"$value"}));
    }
    serde_json::json!({"name":"balances", "columns":[
        {"name":"id", "type":"address"}, {"name":"amount", "type":"uint256", "default":"0"},
        {"name":"extreme", "type":"uint256", "default":"42"}
    ], "events":[{"event":"Transfer", "operations":[
        {"type":"upsert", "where":{"id":"$from"}, "set":creator},
        {"type":"update", "where":{"id":"$from"}, "set":[{"column":"extreme", "action":action, "value":"$value"}]}
    ]}]})
}

#[test]
fn read_manifest_rejects_extrema_without_a_journaled_creation_baseline() {
    for action in ["max", "min"] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(extrema_recovery_table(action, false), true, native);
            let error = read_manifest(&path)
                .expect_err("surviving operands cannot reconstruct an unjournaled creation default")
                .to_string();
            assert!(error.contains("extreme") && error.contains("baseline"), "{action}: {error}");
        }
    }
}

#[test]
fn read_manifest_keeps_tracked_extrema_and_non_reorg_baselines() {
    for action in ["max", "min"] {
        for (enabled, tracked) in [(true, true), (false, false)] {
            for native in [false, true] {
                let (_directory, path) = recovery_table_manifest(
                    extrema_recovery_table(action, tracked),
                    enabled,
                    native,
                );
                read_manifest(&path)
                    .expect("tracked monotonic extrema and non-reorg defaults remain supported");
            }
        }
    }
}

#[test]
fn read_manifest_rejects_incomplete_extrema_histories() {
    for variant in ["external baseline", "mixed actions", "another untracked creator"] {
        let mut table = extrema_recovery_table("max", true);
        let operations = table["events"][0]["operations"].as_array_mut().unwrap();
        match variant {
            "external baseline" => {
                operations.remove(0);
            }
            "mixed actions" => {
                operations[1]["set"][0]["action"] = "set".into();
            }
            _ => {
                let mut creator = operations[0].clone();
                creator["set"].as_array_mut().unwrap().pop();
                operations.push(creator);
            }
        }
        let (_directory, path) = recovery_table_manifest(table, true, false);
        let error = read_manifest(&path)
            .expect_err("accepted extrema must retain a complete monotonic history")
            .to_string();
        assert!(
            error.contains("extreme")
                && (error.contains("baseline") || error.contains("monotonic")),
            "{variant}: {error}"
        );
    }
}

fn deletion_recovery_table() -> serde_json::Value {
    serde_json::json!({"name":"balances", "columns":[
        {"name":"id", "type":"address"}, {"name":"amount", "type":"uint256", "default":"0"}
    ], "events":[{"event":"Transfer", "operations":[
        {"type":"upsert", "where":{"id":"$from"}, "set":[{"column":"amount", "action":"add", "value":"$value"}]},
        {"type":"delete", "where":{"id":"$from"}}
    ]}]})
}

#[test]
fn read_manifest_rejects_deletes_without_row_reconstruction() {
    for native in [false, true] {
        let (_directory, path) = recovery_table_manifest(deletion_recovery_table(), true, native);
        let error = read_manifest(&path)
            .expect_err("an orphan delete must not permanently erase canonical upserts")
            .to_string();
        assert!(
            error.contains("balances") && error.contains("delete") && error.contains("reorg"),
            "{error}"
        );
    }
}

#[test]
fn read_manifest_keeps_non_reorg_deletes_and_recoverable_state_replacements() {
    for native in [false, true] {
        let (_directory, path) = recovery_table_manifest(deletion_recovery_table(), false, native);
        read_manifest(&path).expect("non-reorg deletes retain their existing behavior");
        let mut table = deletion_recovery_table();
        table["events"][0]["operations"][0]["set"][0]["action"] = "set".into();
        table["events"][0]["operations"][1] = serde_json::json!({"type":"update", "where":{"id":"$from"}, "set":[{"column":"amount", "action":"set", "value":"$value"}]});
        let (_directory, path) = recovery_table_manifest(table, true, native);
        read_manifest(&path).expect("journaled set operations remain valid without deletes");
    }
}

fn arithmetic_operand_table(action: &str, value: Option<&str>) -> serde_json::Value {
    let mut table = deletion_recovery_table();
    table["events"][0]["operations"].as_array_mut().unwrap().pop();
    let column = &mut table["events"][0]["operations"][0]["set"][0];
    column["action"] = action.into();
    if let Some(value) = value {
        column["value"] = value.into();
    } else {
        column.as_object_mut().unwrap().remove("value");
    }
    table
}

#[test]
fn read_manifest_rejects_untracked_literal_arithmetic() {
    for action in ["add", "subtract"] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(arithmetic_operand_table(action, Some("1")), true, native);
            let error = read_manifest(&path)
                .expect_err("a literal arithmetic effect must not disappear from the recovery plan")
                .to_string();
            assert!(
                error.contains("amount") && error.contains("operand") && error.contains("reorg"),
                "{action}: {error}"
            );
        }
    }
}

#[test]
fn read_manifest_keeps_tracked_arithmetic_and_non_reorg_literals() {
    for (action, value, enabled) in [
        ("add", Some("$value"), true),
        ("subtract", Some("$value"), true),
        ("increment", None, true),
        ("decrement", None, true),
        ("increment", Some("1"), true),
        ("decrement", Some("1"), true),
        ("increment", Some("2"), false),
        ("max", Some("42"), true),
        ("min", Some("42"), true),
        ("add", Some("1"), false),
        ("subtract", Some("1"), false),
    ] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(arithmetic_operand_table(action, value), enabled, native);
            read_manifest(&path)
                .expect("tracked arithmetic and non-reorg literal operands remain supported");
        }
    }
}

#[test]
fn read_manifest_rejects_non_unit_counter_operands() {
    for action in ["increment", "decrement"] {
        for value in ["0", "2", "$value"] {
            for native in [false, true] {
                let (_directory, path) = recovery_table_manifest(
                    arithmetic_operand_table(action, Some(value)),
                    true,
                    native,
                );
                let error = read_manifest(&path)
                    .expect_err("count-based reversal requires a unit counter operand")
                    .to_string();
                assert!(error.contains("amount") && error.contains("operand"), "{action}: {error}");
            }
        }
    }
}

fn set_arithmetic_table(action: &str, separate_columns: bool) -> serde_json::Value {
    let mut table = deletion_recovery_table();
    let mut arithmetic = serde_json::json!({"column":"amount", "action":action});
    if action == "add" || action == "subtract" {
        arithmetic["value"] = "$value".into();
    }
    table["events"][0]["operations"][0]["set"] = serde_json::json!([arithmetic]);
    let replacement_column = if separate_columns {
        table["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name":"status", "type":"uint256", "default":"0"}));
        "status"
    } else {
        "amount"
    };
    table["events"][0]["operations"][1] = serde_json::json!({"type":"update", "where":{"id":"$from"}, "set":[
        {"column":replacement_column, "action":"set", "value":"$value"}
    ]});
    table
}

#[test]
fn read_manifest_rejects_set_and_arithmetic_on_one_recovered_column() {
    for action in ["add", "subtract", "increment", "decrement"] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(set_arithmetic_table(action, false), true, native);
            let error = read_manifest(&path)
                .expect_err(
                    "journal baseline replacement cannot reconstruct arithmetic on the same column",
                )
                .to_string();
            assert!(
                error.contains("amount") && error.contains("set") && error.contains("arithmetic"),
                "{action}: {error}"
            );
        }
    }
}

#[test]
fn read_manifest_keeps_separate_column_actions_and_non_reorg_mixtures() {
    for action in ["add", "subtract", "increment", "decrement"] {
        for (enabled, separate) in [(true, true), (false, false)] {
            for native in [false, true] {
                let (_directory, path) = recovery_table_manifest(
                    set_arithmetic_table(action, separate),
                    enabled,
                    native,
                );
                read_manifest(&path)
                    .expect("independent columns and non-reorg ordered mixtures retain support");
            }
        }
    }
}

#[test]
fn read_manifest_keeps_arithmetic_upserts_on_a_recovered_column() {
    for action in ["add", "subtract", "increment", "decrement"] {
        for native in [false, true] {
            let mut table = set_arithmetic_table(action, false);
            table["events"][0]["operations"][1]["set"] =
                table["events"][0]["operations"][0]["set"].clone();
            table["events"][0]["operations"][1]["type"] = "upsert".into();
            let (_directory, path) = recovery_table_manifest(table, true, native);
            read_manifest(&path).expect("multiple arithmetic upserts retain supported recovery");
        }
    }
}

#[test]
fn read_manifest_rejects_arithmetic_updates_without_applied_effect_tracking() {
    for action in ["add", "subtract", "increment", "decrement"] {
        for native in [false, true] {
            let mut table = arithmetic_operand_table(
                action,
                if action == "add" || action == "subtract" { Some("$value") } else { None },
            );
            let mut update = table["events"][0]["operations"][0].clone();
            update["type"] = "update".into();
            table["events"][0]["operations"].as_array_mut().unwrap().push(update);
            let (_directory, path) = recovery_table_manifest(table.clone(), false, native);
            read_manifest(&path)
                .expect("arithmetic updates without reorg handling remain supported");
            let (_directory, path) = recovery_table_manifest(table, true, native);
            let error = read_manifest(&path)
                .expect_err("skipped arithmetic updates cannot be reconstructed from raw inputs")
                .to_string();
            assert!(
                error.contains("arithmetic update") && error.contains("reorg"),
                "{action}: {error}"
            );
        }
    }
}
