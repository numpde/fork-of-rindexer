//! Nested tuple-array members retain their complete shape through ABI decoding and storage.
use alloy::{dyn_abi::DynSolValue, json_abi::Event, primitives::Address, rpc::types::Log};
use futures::pin_mut;
use serde_json::{json, Value};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql, NoTls};

use super::{
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::{map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper},
    unsigned_integer_tests::postgres_fixture_url,
};
use crate::{abi::ABIInput, helpers::parse_log};

fn decoded(depth: usize, fixed_outer: bool) -> (Vec<ABIInput>, EthereumSqlTypeWrapper, Value) {
    let array_type = if fixed_outer {
        format!("uint256{}[2]", "[]".repeat(depth - 1))
    } else {
        format!("uint256{}", "[]".repeat(depth))
    };
    let definition = json!({
        "type": "event", "name": "Batch", "anonymous": false,
        "inputs": [{"name": "items", "type": "tuple[]", "indexed": false,
            "components": [{"name": "groups", "type": array_type}]}]
    });
    let inputs: Vec<ABIInput> = serde_json::from_value(definition["inputs"].clone()).unwrap();
    let event: Event = serde_json::from_value(definition).unwrap();
    let mut value = DynSolValue::Array(vec![
        DynSolValue::Uint(alloy::primitives::U256::from(1_u64), 256),
        DynSolValue::Uint(alloy::primitives::U256::from(2_u64), 256),
    ]);
    let mut expected = json!(["1", "2"]);
    for level in 1..depth {
        let values = vec![value, DynSolValue::Array(vec![])];
        value = if fixed_outer && level == depth - 1 {
            DynSolValue::FixedArray(values)
        } else {
            DynSolValue::Array(values)
        };
        expected = json!([expected, []]);
    }
    let encoded =
        DynSolValue::Tuple(vec![DynSolValue::Array(vec![DynSolValue::Tuple(vec![value])])])
            .abi_encode_params();
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            encoded.into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).unwrap();
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1);
    (inputs, wrappers.pop().unwrap(), json!([{"groups": expected}]))
}

#[test]
fn tuple_array_json_preserves_nested_arrays_and_empty_inner_arrays() {
    for (depth, fixed_outer) in [(1, false), (2, false), (3, false), (2, true), (3, true)] {
        let (_, wrapper, expected) = decoded(depth, fixed_outer);
        let EthereumSqlTypeWrapper::JSONB(actual) = wrapper else {
            panic!("tuple arrays require JSONB")
        };
        assert_eq!(actual, expected, "array depth {depth}");
    }
}

async fn roundtrip(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for (depth, fixed_outer) in [(1, false), (2, false), (3, false), (2, true), (3, true)] {
        let (inputs, wrapper, expected) = decoded(depth, fixed_outer);
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE nested_{depth}_{fixed_outer} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        if via_copy {
            let sink = client
                .copy_in(&format!(
                    "COPY nested_{depth}_{fixed_outer} (items) FROM STDIN WITH (FORMAT binary)"
                ))
                .await
                .unwrap();
            let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
            pin_mut!(writer);
            writer.as_mut().write(&[&wrapper as &(dyn ToSql + Sync)]).await.unwrap();
            writer.finish().await.unwrap();
        } else {
            client
                .execute(
                    &format!("INSERT INTO nested_{depth}_{fixed_outer} (items) VALUES ($1)"),
                    &[&wrapper],
                )
                .await
                .unwrap();
        }
        let actual: Value = client
            .query_one(&format!("SELECT items FROM nested_{depth}_{fixed_outer}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(actual, expected, "array depth {depth}, COPY={via_copy}");
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
async fn postgres_nested_tuple_arrays_insert() {
    roundtrip(false).await;
}

#[tokio::test]
async fn postgres_nested_tuple_arrays_copy() {
    roundtrip(true).await;
}
