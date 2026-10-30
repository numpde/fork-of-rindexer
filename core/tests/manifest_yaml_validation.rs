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
                let mut table = set_arithmetic_table(action, separate);
                if enabled {
                    // Independent actions remain supported when each writer also
                    // supplies the complete assignment version for the row.
                    table["events"][0]["operations"][0]["set"].as_array_mut().unwrap().push(
                        serde_json::json!({"column":"status","action":"set","value":"$value"}),
                    );
                }
                let (_directory, path) = recovery_table_manifest(table, enabled, native);
                read_manifest(&path).expect(
                    "complete mixed-column versions and non-reorg ordered mixtures retain support",
                );
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

fn partial_assignment_table(shape: &str) -> serde_json::Value {
    let mut table = serde_json::json!({"name":"totals","global":true,
    "columns":[{"name":"amount","type":"uint256","default":"0"},{"name":"b","type":"uint256","default":"0"}],
    "events":[{"event":"Transfer","operations":[
        {"type":"upsert","set":[{"column":"amount","action":"set","value":"$value"}]},
        {"type":"upsert","set":[{"column":"b","action":"set","value":"$value"}]}
    ]}]});
    if shape != "creators" {
        table["events"][0]["operations"][0]["set"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"column":"b","action":"set","value":"$value"}));
        if shape == "update" {
            table["events"][0]["operations"][1]["type"] = "update".into();
        } else {
            table["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"name":"total","type":"uint256","default":"0"}));
            table["events"][0]["operations"][1]["set"] = serde_json::json!([
                {"column":"total","action":"add","value":"$value"}]);
        }
    }
    table
}

#[test]
fn read_manifest_rejects_partial_set_recovery_without_row_versions() {
    for shape in ["creators", "update", "arithmetic"] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(partial_assignment_table(shape), true, native);
            let error = read_manifest(&path).expect_err("partial assignment history cannot satisfy whole-row admission during reconstruction").to_string();
            assert!(
                error.contains("partial") && error.contains("set") && error.contains("totals"),
                "{shape}: {error}"
            );
        }
    }
}

#[test]
fn read_manifest_keeps_complete_assignment_versions_and_non_reorg_partials() {
    for native in [false, true] {
        for complete in [false, true] {
            let mut table = partial_assignment_table("creators");
            if complete {
                for operation in table["events"][0]["operations"].as_array_mut().unwrap() {
                    operation["set"] = serde_json::json!([
                        {"column":"amount","action":"set","value":"$value"},
                        {"column":"b","action":"set","value":"$value"}]);
                }
            }
            let (_directory, path) = recovery_table_manifest(table, complete, native);
            read_manifest(&path).expect(
                "complete recovered versions and non-reorg partial assignments remain supported",
            );
        }
    }
}

fn arithmetic_predicate_table(condition: &str) -> serde_json::Value {
    serde_json::json!({"name":"totals","global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Transfer","operations":[{"type":"upsert","if":condition,
            "set":[{"column":"amount","action":"add","value":"$value"}]}]}]})
}

#[test]
fn read_manifest_rejects_arithmetic_predicates_without_integer_recovery_semantics() {
    for condition in [
        "$rindexer_block_timestamp / 1000 == 1",
        "$value + 1 > 1",
        "$value - 1 > 0",
        "$value * 2 > 0",
        "$value ^ 2 > 0",
    ] {
        for native in [false, true] {
            let (_directory, path) =
                recovery_table_manifest(arithmetic_predicate_table(condition), true, native);
            let error = read_manifest(&path)
                .expect_err(
                    "SQL arithmetic cannot certify the same integer predicate as forward admission",
                )
                .to_string();
            assert!(
                error.contains("arithmetic")
                    && error.contains("predicate")
                    && error.contains("totals"),
                "{condition}: {error}"
            );
        }
    }
}

#[test]
fn read_manifest_keeps_comparisons_non_reorg_arithmetic_and_insert_filters() {
    for native in [false, true] {
        for condition in [
            "$rindexer_block_timestamp >= 1000 && $rindexer_block_timestamp < 2000",
            "$value > 0 && $value < 100",
            "'a/b+c' == 'a/b+c'",
        ] {
            let (_directory, path) =
                recovery_table_manifest(arithmetic_predicate_table(condition), true, native);
            read_manifest(&path)
                .expect("comparisons and arithmetic characters inside literals remain supported");
        }
        let table = arithmetic_predicate_table("$rindexer_block_timestamp / 1000 == 1");
        let (_directory, path) = recovery_table_manifest(table.clone(), false, native);
        read_manifest(&path).expect("non-reorg arithmetic filters retain forward evaluation");
        let mut insert = table;
        insert["global"] = false.into();
        insert["events"][0]["operations"][0]["type"] = "insert".into();
        insert["events"][0]["operations"][0]["set"][0]["action"] = "set".into();
        let (_directory, path) = recovery_table_manifest(insert, true, native);
        read_manifest(&path)
            .expect("insert-only cleanup does not reevaluate arithmetic predicates");
    }
}

fn shared_recovery_target_manifest(
    shape: &str,
    enabled: bool,
    native: bool,
) -> (tempfile::TempDir, std::path::PathBuf, String) {
    let mut first = partial_assignment_table("creators");
    first["database"] = "shared_state".into();
    let mut second = first.clone();
    first["events"][0]["operations"] =
        serde_json::json!([first["events"][0]["operations"][0].clone()]);
    second["events"][0]["operations"] =
        serde_json::json!([second["events"][0]["operations"][1].clone()]);
    match shape {
        "set-arithmetic" => {
            second["events"][0]["operations"][0]["set"] = serde_json::json!([
            {"column":"amount","action":"add","value":"$value"}])
        }
        "insert-mutable" => {
            let columns = serde_json::json!([{"name":"id","type":"address"},{"name":"amount","type":"uint256","default":"0"}]);
            first["global"] = false.into();
            second["global"] = false.into();
            first["columns"] = columns.clone();
            second["columns"] = columns;
            first["events"][0]["operations"][0] = serde_json::json!({"type":"insert","set":[
                {"column":"id","action":"set","value":"$from"},{"column":"amount","action":"set","value":"$value"}]});
            second["events"][0]["operations"][0] = serde_json::json!({"type":"upsert","where":{"id":"$from"},"set":[
                {"column":"amount","action":"add","value":"$value"}]});
        }
        "extrema-baseline" => {
            first["events"][0]["operations"][0]["set"][0]["action"] = "max".into();
            second["events"][0]["operations"][0]["set"][0]["action"] = "add".into();
        }
        "name-alias" => second["name"] = "Totals".into(),
        "truncated-alias" => {
            first["name"] = format!("{}one", "t".repeat(63)).into();
            second["name"] = format!("{}two", "t".repeat(63)).into();
        }
        _ => {}
    }
    let (_directory, path) = recovery_table_manifest(first, enabled, false);
    let mut manifest: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let target = if shape == "default-alias" {
        let schema = if native { "recovery_evm_traces" } else { "recovery_second" };
        manifest["contracts"][0]["tables"][0]["database"] = schema.into();
        second.as_object_mut().unwrap().remove("database");
        format!("{schema}.totals")
    } else if shape == "truncated-alias" {
        format!("shared_state.{}", "t".repeat(63))
    } else {
        "shared_state.totals".into()
    };
    if native {
        second["events"][0]["event"] = "NativeTransfer".into();
        manifest["native_transfers"] = serde_json::json!({"enabled":true,"networks":[{"network":"mainnet"}],"tables":[second]});
    } else {
        let mut contract = manifest["contracts"][0].clone();
        contract["name"] = "Second".into();
        contract["tables"] = serde_json::json!([second]);
        manifest["contracts"].as_array_mut().unwrap().push(contract);
    }
    fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    (_directory, path, target)
}

#[test]
fn read_manifest_rejects_shared_recovery_targets_across_all_write_models() {
    for shape in [
        "set",
        "set-arithmetic",
        "insert-mutable",
        "extrema-baseline",
        "name-alias",
        "default-alias",
        "truncated-alias",
    ] {
        for native in [false, true] {
            let (_directory, path, target) = shared_recovery_target_manifest(shape, true, native);
            let error = read_manifest(&path).expect_err("separate declarations cannot hide incompatible operations on one recovered target").to_string();
            assert!(
                error.contains(&target)
                    && error.contains("recovery")
                    && error.contains("Token")
                    && error.contains(if native { "native_transfers" } else { "Second" }),
                "{shape}: {error}"
            );
        }
    }
}

#[test]
fn read_manifest_keeps_non_reorg_sharing_and_distinct_recovered_destinations() {
    for native in [false, true] {
        let (_directory, path, _) = shared_recovery_target_manifest("set", false, native);
        read_manifest(&path).expect("non-reorg table sharing remains supported");
        for distinct_schema in [false, true] {
            let (_directory, path, _) = shared_recovery_target_manifest("set", true, native);
            let mut manifest: serde_json::Value =
                serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            let second = if native {
                &mut manifest["native_transfers"]["tables"][0]
            } else {
                &mut manifest["contracts"][1]["tables"][0]
            };
            if distinct_schema {
                second["database"] = "other_state".into();
            } else {
                second["name"] = "other_totals".into();
            }
            fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
            read_manifest(&path).expect(
                "sharing a schema or table basename alone does not share a physical target",
            );
        }
    }
}

#[test]
fn read_manifest_rejects_recovered_schema_case_mismatch() {
    let (_directory, path) =
        recovery_table_manifest(arithmetic_predicate_table("$value > 0"), true, false);
    let mut manifest: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    manifest["contracts"][0]["tables"][0]["database"] = "Shared_State".into();
    fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    let error = read_manifest(&path)
        .expect_err("quoted forward names and bare recovery SQL must agree on case")
        .to_string();
    assert!(error.contains("lowercase") && error.contains("Shared_State"), "{error}");
}

#[test]
fn read_manifest_rejects_shared_recovery_with_non_reorg_delete_writer() {
    for native in [false, true] {
        let (_directory, path, target) = shared_recovery_target_manifest("set", true, native);
        let mut manifest: serde_json::Value =
            serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        manifest["networks"].as_array_mut().unwrap().push(serde_json::json!({"name":"other","chain_id":2,"rpc":"http://unused.invalid","reorg_handling":{"enabled":false}}));
        // These writers share keys across networks. The second source's own
        // recovery flag cannot exempt its DELETE from the physical write model.
        let mut first = manifest["contracts"][0]["tables"][0].clone();
        first["global"] = false.into();
        first["cross_chain"] = true.into();
        first["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name":"id","type":"address"}));
        first["events"][0]["operations"][0]["where"] = serde_json::json!({"id":"$from"});
        manifest["contracts"][0]["tables"][0] = first.clone();
        let mut second = first;
        second["events"][0]["operations"][0] =
            serde_json::json!({"type":"delete","where":{"id":"$from"}});
        if native {
            second["events"][0]["event"] = "NativeTransfer".into();
            manifest["native_transfers"]["tables"][0] = second;
            manifest["native_transfers"]["networks"][0]["network"] = "other".into();
        } else {
            manifest["contracts"][1]["tables"][0] = second;
            manifest["contracts"][1]["details"][0]["network"] = "other".into();
        }
        fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
        let error = read_manifest(&path)
            .expect_err("all shared writers participate in the recovered physical model")
            .to_string();
        assert!(
            error.contains(&target)
                && error.contains("recovery")
                && error.contains("Token")
                && error.contains(if native { "native_transfers" } else { "Second" }),
            "{error}"
        );
    }
}

#[test]
fn read_manifest_rejects_quoted_alias_from_non_reorg_writer() {
    for native in [false, true] {
        let (_directory, path, target) = shared_recovery_target_manifest("set", true, native);
        let mut manifest: serde_json::Value =
            serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        manifest["storage"]["postgres"]["disable_create_tables"] = true.into();
        manifest["networks"].as_array_mut().unwrap().push(serde_json::json!({"name":"other","chain_id":2,"rpc":"http://unused.invalid","reorg_handling":{"enabled":false}}));
        let second = if native {
            manifest["native_transfers"]["networks"][0]["network"] = "other".into();
            &mut manifest["native_transfers"]["tables"][0]
        } else {
            manifest["contracts"][1]["details"][0]["network"] = "other".into();
            &mut manifest["contracts"][1]["tables"][0]
        };
        second["database"] = "\"shared_state\"".into();
        fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
        let error = read_manifest(&path)
            .expect_err("quoted forward writes resolve to the same recovered destination")
            .to_string();
        assert!(error.contains(&target) && error.contains("recovery"), "{error}");
        let second = if native {
            &mut manifest["native_transfers"]["tables"][0]
        } else {
            &mut manifest["contracts"][1]["tables"][0]
        };
        second["database"] = "\"Shared_State\"".into();
        fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
        read_manifest(&path).expect("quoted uppercase names identify a distinct non-reorg schema");
    }
}

#[test]
fn read_manifest_rejects_qualified_schema_from_non_reorg_writer() {
    let (_directory, path, _) = shared_recovery_target_manifest("set", true, false);
    let mut manifest: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    manifest["storage"]["postgres"]["disable_create_tables"] = true.into();
    manifest["networks"].as_array_mut().unwrap().push(serde_json::json!({"name":"other","chain_id":2,"rpc":"http://unused.invalid","reorg_handling":{"enabled":false}}));
    manifest["contracts"][1]["details"][0]["network"] = "other".into();
    manifest["contracts"][1]["tables"][0]["database"] = "current_database.shared_state".into();
    fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    let error = read_manifest(&path)
        .expect_err("recovery cannot resolve a database-qualified schema alias offline")
        .to_string();
    assert!(error.contains("schema.table") && error.contains("Second"), "{error}");
}

#[test]
fn read_manifest_keeps_one_recovered_owner_with_multiple_sources() {
    for native in [false, true] {
        let (_directory, path) =
            recovery_table_manifest(arithmetic_predicate_table("$value > 0"), true, native);
        let mut manifest: serde_json::Value =
            serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        if native {
            manifest["networks"].as_array_mut().unwrap().push(serde_json::json!({"name":"other","chain_id":2,"rpc":"http://unused.invalid","reorg_handling":{"enabled":true}}));
            manifest["native_transfers"]["networks"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"network":"other"}));
        } else {
            let mut detail = manifest["contracts"][0]["details"][0].clone();
            detail["address"] = "0x0000000000000000000000000000000000000002".into();
            manifest["contracts"][0]["details"].as_array_mut().unwrap().push(detail);
        }
        fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
        read_manifest(&path).expect("source count does not duplicate a logical Table declaration");
    }
}

#[test]
fn read_manifest_rejects_utf8_truncation_alias_from_non_reorg_writer() {
    for native in [false, true] {
        for component in ["name", "database"] {
            for suffix in ["é", "中", "𐐨"] {
                let (_directory, path, _) = shared_recovery_target_manifest("set", true, native);
                let mut manifest: serde_json::Value =
                    serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
                manifest["storage"]["postgres"]["disable_create_tables"] = true.into();
                manifest["networks"].as_array_mut().unwrap().push(serde_json::json!({"name":"other","chain_id":2,"rpc":"http://unused.invalid","reorg_handling":{"enabled":false}}));
                let prefix = "t".repeat(62);
                manifest["contracts"][0]["tables"][0][component] = prefix.clone().into();
                let second = if native {
                    manifest["native_transfers"]["networks"][0]["network"] = "other".into();
                    &mut manifest["native_transfers"]["tables"][0]
                } else {
                    manifest["contracts"][1]["details"][0]["network"] = "other".into();
                    &mut manifest["contracts"][1]["tables"][0]
                };
                second[component] = format!("{prefix}{suffix}").into();
                fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
                let error = read_manifest(&path)
                    .expect_err("PostgreSQL drops the multibyte character crossing byte 63")
                    .to_string();
                assert!(
                    error.contains(&prefix) && error.contains("multiple manifest declarations"),
                    "{component}/{suffix}: {error}"
                );
                // A complete character fitting within the byte limit remains
                // part of the identifier, and therefore names a distinct target.
                let second = if native {
                    &mut manifest["native_transfers"]["tables"][0]
                } else {
                    &mut manifest["contracts"][1]["tables"][0]
                };
                second[component] = format!("{}{suffix}", "t".repeat(63 - suffix.len())).into();
                fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
                read_manifest(&path)
                    .expect("a multibyte character fitting exactly at byte 63 survives");
            }
        }
    }
}
