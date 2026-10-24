//! PostgreSQL integer regressions starting at ABI-decoded event values.
//!
//! Pure tests inspect the real binary encoder independently of its declared type.
//! PostgreSQL tests use the managed fixture URL, or Testcontainers for native runs.
use std::time::Duration;

use alloy::{json_abi::Event, primitives::Address, rpc::types::Log};
use bytes::BytesMut;
use futures::pin_mut;
use serde_json::json;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{FromSql, IsNull, ToSql, Type as PgType},
    Client, Config, NoTls,
};

use super::postgres::generate::{generate_columns_with_data_types, solidity_type_to_db_type};
use super::sql_type_wrapper::{
    map_log_params_to_ethereum_wrapper, solidity_type_to_ethereum_sql_type_wrapper,
    EthereumSqlTypeWrapper,
};
use crate::{abi::ABIInput, helpers::parse_log};

const UINT16_VALUES: &[i64] = &[0, 32767, 32768, 65535];
const UINT32_VALUES: &[i64] = &[0, 2147483647, 2147483648, 4294967295];
const CASES: &[(&str, &[i64])] = &[
    ("uint8", &[0, 127, 128, 255]),
    ("uint16", UINT16_VALUES),
    ("uint24", &[0, 8388607, 8388608, 16777215]),
    ("uint32", UINT32_VALUES),
    ("int8", &[-128, -1, 0, 127]),
    ("int16", &[-32768, -1, 0, 32767]),
    ("int24", &[-8388608, -1, 0, 8388607]),
    ("int32", &[-2147483648, -1, 0, 2147483647]),
];

pub(super) fn event(abi_type: &str) -> (Event, Vec<ABIInput>) {
    let definition = json!({
        "type": "event", "name": "Values", "anonymous": false,
        "inputs": [{"name": "payload", "type": abi_type, "indexed": false}]
    });
    let inputs = serde_json::from_value(definition["inputs"].clone()).expect("valid ABI inputs");
    (serde_json::from_value(definition).expect("valid event ABI"), inputs)
}

// Assemble ABI words independently of Alloy's encoder and the production converter.
// All fixtures fit in i64; negative signed integers are sign-extended to 256 bits.
fn word(value: i64) -> [u8; 32] {
    let mut bytes = [if value < 0 { 0xff } else { 0 }; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

pub(super) fn decoded(abi_type: &str, values: &[i64]) -> EthereumSqlTypeWrapper {
    let (event, inputs) = event(abi_type);
    let mut data = Vec::new();
    if abi_type.ends_with("[]") {
        data.extend_from_slice(&word(32));
        data.extend_from_slice(&word(i64::try_from(values.len()).expect("small fixture")));
    } else {
        assert_eq!(values.len(), 1, "scalar fixture needs exactly one value");
    }
    for &value in values {
        data.extend_from_slice(&word(value));
    }
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            data.into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).expect("ABI fixture must decode before conversion");
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1, "one event field must produce one column");
    wrappers.pop().expect("one SQL wrapper")
}

fn encoded(wrapper: &EthereumSqlTypeWrapper) -> (PgType, BytesMut) {
    let ty = wrapper.to_type();
    let mut output = BytesMut::new();
    assert!(
        matches!(wrapper.to_sql(&ty, &mut output).expect("encode integer"), IsNull::No),
        "a nonempty integer value must not become NULL"
    );
    (ty, output)
}

fn scalar_value(wrapper: &EthereumSqlTypeWrapper) -> i64 {
    let (ty, output) = encoded(wrapper);
    if ty == PgType::INT2 {
        i64::from(i16::from_sql(&ty, &output).expect("decode int2"))
    } else if ty == PgType::INT4 {
        i64::from(i32::from_sql(&ty, &output).expect("decode int4"))
    } else if ty == PgType::INT8 {
        i64::from_sql(&ty, &output).expect("decode int8")
    } else {
        panic!("unexpected integer wire type: {ty}");
    }
}

fn array_values(wrapper: &EthereumSqlTypeWrapper) -> Vec<i64> {
    let (ty, output) = encoded(wrapper);
    if ty == PgType::INT2_ARRAY {
        Vec::<i16>::from_sql(&ty, &output)
            .expect("decode int2 array")
            .into_iter()
            .map(i64::from)
            .collect()
    } else if ty == PgType::INT4_ARRAY {
        Vec::<i32>::from_sql(&ty, &output)
            .expect("decode int4 array")
            .into_iter()
            .map(i64::from)
            .collect()
    } else if ty == PgType::INT8_ARRAY {
        Vec::<i64>::from_sql(&ty, &output).expect("decode int8 array")
    } else {
        panic!("unexpected integer array wire type: {ty}");
    }
}

fn assert_scalar_values(abi_type: &str, expected: &[i64]) {
    let actual: Vec<_> = expected.iter().map(|&v| scalar_value(&decoded(abi_type, &[v]))).collect();
    assert_eq!(actual, expected, "{abi_type} scalar encoding must preserve the full value range");
}

fn assert_array_values(abi_type: &str, expected: &[i64]) {
    let actual = array_values(&decoded(&format!("{abi_type}[]"), expected));
    assert_eq!(actual, expected, "{abi_type}[] encoding must preserve the full value range");
}

#[test]
fn uint16_scalar_encoding_preserves_full_range() {
    assert_scalar_values("uint16", UINT16_VALUES);
}

#[test]
fn uint16_array_encoding_preserves_full_range() {
    assert_array_values("uint16", UINT16_VALUES);
}

#[test]
fn uint32_scalar_encoding_preserves_full_range() {
    assert_scalar_values("uint32", UINT32_VALUES);
}

#[test]
fn uint32_array_encoding_preserves_full_range() {
    assert_array_values("uint32", UINT32_VALUES);
}

#[test]
fn generated_schema_and_bind_types_cover_unsigned_ranges() {
    // uint24 shares U32/VecU32 with uint32. DDL and COPY bind types must agree.
    for (abi_type, sql, scalar, array) in [
        ("uint16", "INTEGER", PgType::INT4, PgType::INT4_ARRAY),
        ("uint24", "BIGINT", PgType::INT8, PgType::INT8_ARRAY),
        ("uint32", "BIGINT", PgType::INT8, PgType::INT8_ARRAY),
    ] {
        assert_eq!(
            solidity_type_to_db_type(abi_type),
            sql,
            "generated PostgreSQL columns must hold the complete unsigned range"
        );
        assert_eq!(solidity_type_to_db_type(&format!("{abi_type}[]")), format!("{sql}[]"));
        assert_eq!(solidity_type_to_ethereum_sql_type_wrapper(abi_type).unwrap().to_type(), scalar);
        assert_eq!(
            solidity_type_to_ethereum_sql_type_wrapper(&format!("{abi_type}[]")).unwrap().to_type(),
            array
        );
        assert_eq!(decoded(abi_type, &[0]).to_type(), scalar);
        assert_eq!(decoded(&format!("{abi_type}[]"), &[0]).to_type(), array);
    }
}

#[test]
fn signed_and_uint8_values_keep_existing_storage() {
    for (abi_type, sql, scalar, array) in [
        ("uint8", "SMALLINT", PgType::INT2, PgType::INT2_ARRAY),
        ("int8", "SMALLINT", PgType::INT2, PgType::INT2_ARRAY),
        ("int16", "SMALLINT", PgType::INT2, PgType::INT2_ARRAY),
        ("int24", "INTEGER", PgType::INT4, PgType::INT4_ARRAY),
        ("int32", "INTEGER", PgType::INT4, PgType::INT4_ARRAY),
    ] {
        let values = CASES.iter().find(|(name, _)| *name == abi_type).unwrap().1;
        assert_eq!(solidity_type_to_db_type(abi_type), sql);
        assert_eq!(decoded(abi_type, &[0]).to_type(), scalar);
        assert_eq!(decoded(&format!("{abi_type}[]"), &[0]).to_type(), array);
        assert_scalar_values(abi_type, values);
        assert_array_values(abi_type, values);
    }
    assert_scalar_values("uint24", &[0, 16777215]);
    assert_array_values("uint24", &[0, 16777215]);
}

#[test]
fn empty_unsigned_arrays_keep_null_behavior() {
    for abi_type in ["uint16[]", "uint24[]", "uint32[]"] {
        let wrapper = decoded(abi_type, &[]);
        let mut output = BytesMut::new();
        assert!(matches!(
            wrapper.to_sql(&wrapper.to_type(), &mut output).expect("encode empty array"),
            IsNull::Yes
        ));
        assert!(output.is_empty());
    }
}

#[test]
fn clickhouse_unsigned_values_are_unchanged() {
    for (abi_type, values) in [("uint16", UINT16_VALUES), ("uint32", UINT32_VALUES)] {
        for &value in values {
            assert_eq!(decoded(abi_type, &[value]).to_clickhouse_value(), value.to_string());
        }
        assert_eq!(
            decoded(&format!("{abi_type}[]"), values).to_clickhouse_value(),
            format!("[{}]", values.iter().map(i64::to_string).collect::<Vec<_>>().join(", "))
        );
    }
}

async fn write_value(
    client: &Client,
    table: &str,
    wrapper: &EthereumSqlTypeWrapper,
    via_copy: bool,
) {
    if via_copy {
        let sink = client
            .copy_in(&format!("COPY {table} (payload) FROM STDIN WITH (FORMAT binary)"))
            .await
            .expect("start COPY");
        let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
        pin_mut!(writer);
        let row: [&(dyn ToSql + Sync); 1] = [wrapper];
        writer.as_mut().write(&row).await.expect("write COPY row");
        writer.finish().await.expect("finish COPY");
    } else {
        client
            .execute(&format!("INSERT INTO {table} (payload) VALUES ($1)"), &[wrapper])
            .await
            .expect("INSERT integer value");
    }
}

/// Use the explicit disposable fixture or start a native PostgreSQL container.
/// Keep the returned container alive until all database connections have closed.
pub(super) async fn postgres_fixture_url() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match std::env::var_os("RINDEXER_TEST_POSTGRES_URL") {
        Some(url) => {
            let url = url.into_string().expect("Unicode fixture URL");
            let config: Config = url.parse().expect("valid fixture URL");
            assert_eq!(
                config.get_dbname(),
                Some("rindexer_fixture_admin"),
                "only the disposable fixture database is allowed"
            );
            (url, None)
        }
        None => {
            let container = Postgres::default().start().await.expect("start PostgreSQL fixture");
            let host = container.get_host().await.expect("fixture host").to_string();
            let port = container.get_host_port_ipv4(5432).await.expect("fixture port");
            (format!("postgresql://postgres:postgres@{host}:{port}/postgres"), Some(container))
        }
    }
}

async fn postgres_roundtrip(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let mut config: Config = url.parse().expect("valid fixture URL");
    config.connect_timeout(Duration::from_secs(10));
    config.options("-c statement_timeout=15000 -c lock_timeout=5000");
    let (client, connection) = config.connect(NoTls).await.expect("connect to fixture");
    let connection = tokio::spawn(connection);
    let mut failures = Vec::new();
    for (index, &(abi_type, values)) in CASES.iter().enumerate() {
        for is_array in [false, true] {
            // Session-local temporary tables isolate concurrent cases and disappear
            // even after a failed assertion or cancelled connection. No shared DDL.
            let table = format!("integer_roundtrip_{index}_{}", u8::from(is_array));
            let abi_type = if is_array { format!("{abi_type}[]") } else { abi_type.to_string() };
            let (_, inputs) = event(&abi_type);
            let columns = generate_columns_with_data_types(&inputs).join(", ");
            client
                .batch_execute(&format!("CREATE TEMP TABLE {table} (id SERIAL, {columns})"))
                .await
                .expect("create generated event columns");
            let expected = if is_array {
                write_value(&client, &table, &decoded(&abi_type, values), via_copy).await;
                write_value(&client, &table, &decoded(&abi_type, &[]), via_copy).await;
                vec![
                    Some(format!(
                        "{{{}}}",
                        values.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
                    )),
                    None,
                ]
            } else {
                for &value in values {
                    write_value(&client, &table, &decoded(&abi_type, &[value]), via_copy).await;
                }
                values.iter().map(|value| Some(value.to_string())).collect()
            };
            let rows = client
                .query(&format!("SELECT payload::text FROM {table} ORDER BY id"), &[])
                .await
                .expect("read stored integers");
            let actual: Vec<Option<String>> = rows.iter().map(|row| row.get(0)).collect();
            if actual != expected {
                failures.push(format!("{abi_type}: expected {expected:?}, got {actual:?}"));
            }
        }
    }
    drop(client);
    tokio::time::timeout(Duration::from_secs(10), connection)
        .await
        .expect("connection closes")
        .expect("connection task succeeds")
        .expect("connection succeeds");
    let operation = if via_copy { "COPY" } else { "INSERT" };
    assert!(
        failures.is_empty(),
        "PostgreSQL {operation} must preserve ABI integer values:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn postgres_insert_preserves_unsigned_and_signed_values() {
    postgres_roundtrip(false).await;
}

#[tokio::test]
async fn postgres_copy_preserves_unsigned_and_signed_values() {
    postgres_roundtrip(true).await;
}
