//! Name-based event selection must reject ambiguous ABIs before emitting code or storage plans.
use super::events_bindings::{generate_event_bindings, generate_event_handlers};
use crate::{
    abi::ABIItem,
    manifest::{contract::Contract, storage::Storage, yaml::read_manifest},
};
use serde_json::{json, Value};
use std::{fs, path::Path};

const EVENT: &str = "Lens_Account_MetadataURISet";

fn definitions() -> [Value; 2] {
    [
        json!({"type":"event","name":EVENT,"anonymous":false,"inputs":[{"name":"metadataURI","type":"string","indexed":false}]}),
        json!({"type":"event","name":EVENT,"anonymous":false,"inputs":[{"name":"metadataURI","type":"string","indexed":false},{"name":"source","type":"address","indexed":true}]}),
    ]
}

fn contract(abi: Value) -> Contract {
    serde_json::from_value(json!({"name":"Account","abi":abi,"details":[{"network":"ethereum","address":"0x0000000000000000000000000000000000000001","start_block":"1"}]})).unwrap()
}

fn write_abis(dir: &Path) {
    let [first, second] = definitions();
    for (file, value) in [
        ("both.json", json!([first, second])),
        ("first.json", json!([first])),
        ("second.json", json!([second])),
    ] {
        fs::write(dir.join(file), serde_json::to_string(&value).unwrap()).unwrap();
    }
}

fn diagnostic(error: &str) {
    for part in [
        "Account",
        EVENT,
        "multiple",
        "separate",
        "Lens_Account_MetadataURISet(string)",
        "Lens_Account_MetadataURISet(string,address)",
    ] {
        assert!(error.contains(part), "missing {part:?} in diagnostic: {error}");
    }
}

#[test]
fn overloaded_event_names_fail_during_manifest_loading_in_both_project_modes() {
    let dir = tempfile::tempdir().unwrap();
    write_abis(dir.path());
    for project_type in ["no-code", "rust"] {
        for abi in [json!("./both.json"), json!(["./first.json", "./second.json"])] {
            let mut item = serde_json::to_value(contract(abi)).unwrap();
            // Loading must reject the ambiguity before selection: the Rust struct
            // generator consumes the complete ABI even when a different event is selected.
            item["include_events"] = json!(["Unrelated"]);
            let manifest = json!({"name":"test","project_type":project_type,"networks":[{"name":"ethereum","chain_id":1,"rpc":"http://localhost:8545"}],"contracts":[item]});
            let path = dir.path().join("rindexer.yaml");
            fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
            let error = read_manifest(&path).expect_err("ambiguous ABI must fail before startup");
            diagnostic(&error.to_string());
        }
    }
}

#[test]
fn overloaded_event_codegen_returns_errors_instead_of_duplicate_items() {
    let dir = tempfile::tempdir().unwrap();
    write_abis(dir.path());
    let contract = contract(json!("./both.json"));
    let storage = Storage::default();
    let bindings = generate_event_bindings(dir.path(), "Test", &contract, false, &storage)
        .err()
        .expect("do not return duplicate Rust types or enum variants");
    diagnostic(&bindings.to_string());
    let handlers = generate_event_handlers(dir.path(), "Test", false, &contract, &storage)
        .err()
        .expect("do not return duplicate handler functions");
    diagnostic(&handlers.to_string());
}

#[test]
fn duplicate_event_declarations_fail_before_codegen_too() {
    let dir = tempfile::tempdir().unwrap();
    let first = definitions()[0].clone();
    fs::write(dir.path().join("both.json"), json!([first, first]).to_string()).unwrap();
    let error = contract(json!("./both.json"))
        .parse_abi(dir.path())
        .expect_err("duplicate declarations also generate duplicate Rust items");
    let message = error.to_string();
    assert!(message.contains(EVENT) && message.contains("multiple"), "{message}");
}

#[test]
fn split_contracts_keep_original_event_names_and_selectors() {
    let dir = tempfile::tempdir().unwrap();
    write_abis(dir.path());
    for (file, name, signature) in [
        ("./first.json", "AccountV1", "Lens_Account_MetadataURISet(string)"),
        ("./second.json", "AccountV2", "Lens_Account_MetadataURISet(string,address)"),
    ] {
        let mut contract = contract(json!(file));
        contract.name = name.into();
        let abi = ABIItem::read_abi_items(dir.path(), &contract).unwrap();
        let events = ABIItem::extract_event_names_and_signatures_from_abi(abi).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, EVENT);
        assert_eq!(events[0].topic_id(), alloy::primitives::keccak256(signature));
        let source =
            generate_event_bindings(dir.path(), "Test", &contract, false, &Storage::default())
                .unwrap()
                .to_string();
        assert_eq!(source.matches(&format!("pub struct {EVENT}Result")).count(), 1);
        assert!(source.contains(&events[0].topic_id_as_hex_string()));
    }
}

#[test]
fn merging_identical_events_and_overloaded_functions_remains_supported() {
    let dir = tempfile::tempdir().unwrap();
    let first = definitions()[0].clone();
    let abi = json!([first,
        {"type":"function","name":EVENT,"inputs":[{"name":"id","type":"uint256"}],"outputs":[]},
        {"type":"function","name":EVENT,"inputs":[{"name":"id","type":"address"}],"outputs":[]}
    ]);
    fs::write(dir.path().join("first.json"), abi.to_string()).unwrap();
    let contract = contract(json!(["./first.json", "./first.json"]));
    let parsed = contract.parse_abi(dir.path()).unwrap();
    let entries: Vec<Value> = serde_json::from_str(&parsed).unwrap();
    assert_eq!(entries.len(), 3, "the existing merge deduplicates identical entries");
    generate_event_bindings(dir.path(), "Test", &contract, false, &Storage::default()).unwrap();
}
