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

fn decoded(depth: usize) -> (Vec<ABIInput>, EthereumSqlTypeWrapper, Value) {
    let definition = json!({
        "type": "event", "name": "Batch", "anonymous": false,
        "inputs": [{"name": "items", "type": "tuple[]", "indexed": false,
            "components": [{"name": "groups", "type": format!("uint256{}", "[]".repeat(depth))}]}]
    });
    let inputs: Vec<ABIInput> = serde_json::from_value(definition["inputs"].clone()).unwrap();
    let event: Event = serde_json::from_value(definition).unwrap();
    let mut value = DynSolValue::Array(vec![
        DynSolValue::Uint(alloy::primitives::U256::from(1_u64), 256),
        DynSolValue::Uint(alloy::primitives::U256::from(2_u64), 256),
    ]);
    let mut expected = json!(["1", "2"]);
    for _ in 1..depth {
        value = DynSolValue::Array(vec![value, DynSolValue::Array(vec![])]);
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
    for depth in 1..=3 {
        let (_, wrapper, expected) = decoded(depth);
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
    for depth in 1..=3 {
        let (inputs, wrapper, expected) = decoded(depth);
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE nested_{depth} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        if via_copy {
            let sink = client
                .copy_in(&format!("COPY nested_{depth} (items) FROM STDIN WITH (FORMAT binary)"))
                .await
                .unwrap();
            let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
            pin_mut!(writer);
            writer.as_mut().write(&[&wrapper as &(dyn ToSql + Sync)]).await.unwrap();
            writer.finish().await.unwrap();
        } else {
            client
                .execute(&format!("INSERT INTO nested_{depth} (items) VALUES ($1)"), &[&wrapper])
                .await
                .unwrap();
        }
        let actual: Value = client
            .query_one(&format!("SELECT items FROM nested_{depth}"), &[])
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
