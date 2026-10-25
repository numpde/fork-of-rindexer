//! Flattened tuple wrappers are consumed by leaves, not by tuple containers.
use alloy::{
    dyn_abi::DynSolValue,
    json_abi::Event,
    primitives::{Address, B256, U256},
    rpc::types::Log,
};
use serde_json::{json, Value};

use super::sql_type_wrapper::{map_ethereum_wrapper_to_json, map_log_params_to_ethereum_wrapper};
use crate::{abi::ABIInput, event::callback_registry::TxInformation, helpers::parse_log};

pub(crate) fn event_fixture(siblings: bool) -> (Value, Vec<u8>, Value) {
    let mut components = vec![
        json!({"name": "inner", "type": "tuple", "components": [{"name": "x", "type": "uint256"}]}),
    ];
    let mut tuple = vec![DynSolValue::Tuple(vec![DynSolValue::Uint(U256::from(7), 256)])];
    if siblings {
        components.push(json!({"name": "label", "type": "string"}));
        tuple.push(DynSolValue::String("kept".to_string()));
    }
    let mut inputs =
        vec![json!({"name": "value", "type": "tuple", "indexed": false, "components": components})];
    let mut values = vec![DynSolValue::Tuple(tuple)];
    let mut expected = json!({"value": {"inner": {"x": "7"}}});
    if siblings {
        inputs.push(json!({"name": "after", "type": "uint256", "indexed": false}));
        values.push(DynSolValue::Uint(U256::from(9), 256));
        expected["value"]["label"] = json!("kept");
        expected["after"] = json!("9");
    }
    (
        json!({"type": "event", "name": "Nested", "anonymous": false, "inputs": inputs}),
        DynSolValue::Tuple(values).abi_encode_params(),
        expected,
    )
}

fn assert_json(siblings: bool) {
    let (definition, encoded, expected) = event_fixture(siblings);
    let inputs: Vec<ABIInput> = serde_json::from_value(definition["inputs"].clone()).unwrap();
    let event: Event = serde_json::from_value(definition).unwrap();
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            encoded.into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).unwrap();
    let wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    let transaction = TxInformation {
        chain_id: 1,
        network: "ethereum".to_string(),
        address: Address::ZERO,
        block_hash: B256::ZERO,
        block_number: 1,
        block_timestamp: None,
        transaction_hash: B256::ZERO,
        log_index: U256::ZERO,
        transaction_index: 0,
    };
    let mut actual = map_ethereum_wrapper_to_json(&inputs, &wrappers, &transaction, false);
    actual.as_object_mut().unwrap().remove("transaction_information");
    assert_eq!(actual, expected);
}

#[test]
fn nested_tuple_json_handles_one_leaf_without_panicking() {
    assert_json(false);
}
#[test]
fn nested_tuple_json_preserves_inside_and_following_siblings() {
    assert_json(true);
}
