//! Array storage must follow the ABI shape, including empty and fixed dimensions.
use alloy::{
    dyn_abi::DynSolValue,
    json_abi::Event,
    primitives::{Address, B256, U256},
    rpc::types::Log,
};
use futures::pin_mut;
use serde_json::{json, Value};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql, NoTls};

use super::{
    clickhouse::generate::solidity_type_to_clickhouse_type,
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::{
        map_log_params_to_ethereum_wrapper, solidity_type_to_ethereum_sql_type_wrapper,
        EthereumSqlTypeWrapper,
    },
    unsigned_integer_tests::postgres_fixture_url,
};
use crate::{abi::ABIInput, helpers::parse_log};

fn decoded(kind: &str, value: DynSolValue) -> (Vec<ABIInput>, EthereumSqlTypeWrapper) {
    let definition = json!({"type":"event", "name":"Values", "anonymous":false,
        "inputs":[{"name":"payload", "type":kind, "indexed":false}]});
    let inputs: Vec<ABIInput> = serde_json::from_value(definition["inputs"].clone()).unwrap();
    let event: Event = serde_json::from_value(definition).unwrap();
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            DynSolValue::Tuple(vec![value]).abi_encode_params().into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).unwrap();
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1);
    (inputs, wrappers.pop().unwrap())
}

fn fixed_uint() -> DynSolValue {
    DynSolValue::FixedArray(vec![
        DynSolValue::Uint(U256::from(0), 16),
        DynSolValue::Uint(U256::from(65535), 16),
    ])
}
fn fixed_bytes() -> DynSolValue {
    DynSolValue::FixedArray(vec![
        DynSolValue::FixedBytes(B256::repeat_byte(0x11), 32),
        DynSolValue::FixedBytes(B256::repeat_byte(0x22), 32),
    ])
}
fn nested_values() -> Vec<(&'static str, DynSolValue, Value)> {
    let inner = DynSolValue::Array(vec![
        DynSolValue::Uint(U256::from(1), 256),
        DynSolValue::Uint(U256::MAX, 256),
    ]);
    let empty = DynSolValue::Array(vec![]);
    let pair = DynSolValue::Array(vec![inner.clone(), empty.clone()]);
    vec![
        ("uint256[][]", pair.clone(), json!([["1", U256::MAX.to_string()], []])),
        (
            "uint256[][2]",
            DynSolValue::FixedArray(vec![inner, empty]),
            json!([["1", U256::MAX.to_string()], []]),
        ),
        (
            "uint256[][][]",
            DynSolValue::Array(vec![pair, DynSolValue::Array(vec![])]),
            json!([[["1", U256::MAX.to_string()], []], []]),
        ),
        ("uint256[][]", DynSolValue::Array(vec![]), json!([])),
    ]
}

#[test]
fn fixed_arrays_use_array_schema_and_wrapper_types() {
    for (kind, value, ddl, ch) in [
        ("uint16[2]", fixed_uint(), "INTEGER[]", "Array(UInt16)"),
        ("bytes32[2]", fixed_bytes(), "BYTEA[]", "Array(String)"),
    ] {
        let (inputs, wrapper) = decoded(kind, value);
        assert_eq!(generate_columns_with_data_types(&inputs), vec![format!("\"payload\" {ddl}")]);
        assert_eq!(solidity_type_to_clickhouse_type(kind), ch);
        assert_eq!(
            solidity_type_to_ethereum_sql_type_wrapper(kind).unwrap().to_type(),
            wrapper.to_type()
        );
    }
}

#[test]
fn direct_nested_arrays_preserve_shape_after_abi_decoding() {
    for (kind, value, expected) in nested_values() {
        let (inputs, wrapper) = decoded(kind, value);
        assert_eq!(generate_columns_with_data_types(&inputs), vec!["\"payload\" JSONB"]);
        let EthereumSqlTypeWrapper::JSONB(actual) = wrapper else {
            panic!("nested arrays require JSONB")
        };
        assert_eq!(actual, expected);
        assert_eq!(
            solidity_type_to_ethereum_sql_type_wrapper(kind).unwrap().to_type(),
            tokio_postgres::types::Type::JSONB
        );
    }
}

#[test]
fn direct_nested_array_clickhouse_schema_and_literal_agree() {
    for (kind, value, expected) in nested_values() {
        assert_eq!(solidity_type_to_clickhouse_type(kind), "String");
        let (_, wrapper) = decoded(kind, value);
        assert_eq!(wrapper.to_clickhouse_value(), format!("'{}'", expected));
    }
}

async fn roundtrip_fixed(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for (index, (kind, value)) in
        [("uint16[2]", fixed_uint()), ("bytes32[2]", fixed_bytes())].into_iter().enumerate()
    {
        let (inputs, wrapper) = decoded(kind, value);
        let table = format!("fixed_{index}");
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE {table} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        write(&client, &table, &wrapper, via_copy).await;
        let row = client.query_one(&format!("SELECT payload FROM {table}"), &[]).await.unwrap();
        if index == 0 {
            assert_eq!(row.get::<_, Vec<i32>>(0), vec![0, 65535]);
        } else {
            assert_eq!(row.get::<_, Vec<Vec<u8>>>(0), vec![vec![0x11; 32], vec![0x22; 32]]);
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

async fn roundtrip_nested(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for (index, (kind, value, expected)) in nested_values().into_iter().enumerate() {
        let (inputs, wrapper) = decoded(kind, value);
        let table = format!("nested_{index}");
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE {table} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        write(&client, &table, &wrapper, via_copy).await;
        let actual: Value =
            client.query_one(&format!("SELECT payload FROM {table}"), &[]).await.unwrap().get(0);
        assert_eq!(actual, expected, "{kind}, COPY={via_copy}");
    }
    drop(client);
    connection.await.unwrap().unwrap();
}
async fn write(
    client: &tokio_postgres::Client,
    table: &str,
    wrapper: &EthereumSqlTypeWrapper,
    via_copy: bool,
) {
    if via_copy {
        let sink = client
            .copy_in(&format!("COPY {table} (payload) FROM STDIN WITH (FORMAT binary)"))
            .await
            .unwrap();
        let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
        pin_mut!(writer);
        writer.as_mut().write(&[wrapper as &(dyn ToSql + Sync)]).await.unwrap();
        writer.finish().await.unwrap();
    } else {
        client
            .execute(&format!("INSERT INTO {table} (payload) VALUES ($1)"), &[wrapper])
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn postgres_fixed_arrays_insert() {
    roundtrip_fixed(false).await;
}
#[tokio::test]
async fn postgres_fixed_arrays_copy() {
    roundtrip_fixed(true).await;
}
#[tokio::test]
async fn postgres_direct_nested_arrays_insert() {
    roundtrip_nested(false).await;
}
#[tokio::test]
async fn postgres_direct_nested_arrays_copy() {
    roundtrip_nested(true).await;
}
