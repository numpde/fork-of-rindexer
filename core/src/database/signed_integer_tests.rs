//! Start with ABI-decoded signed values and read back real PostgreSQL writes.
use bytes::BytesMut;
use futures::pin_mut;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{FromSql, IsNull, ToSql, Type},
    Client, NoTls,
};

use super::{
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::EthereumSqlTypeWrapper,
    unsigned_integer_tests::{decoded, event, postgres_fixture_url},
};

fn values(bits: u32) -> Vec<i64> {
    let (min, max) = if bits == 64 {
        (i64::MIN, i64::MAX)
    } else {
        (-(1_i64 << (bits - 1)), (1_i64 << (bits - 1)) - 1)
    };
    vec![42, -1, 0, min, max]
}

#[test]
fn signed_numeric_schema_bind_types_and_encoding_agree() {
    for bits in [40, 48, 56, 64] {
        let abi_type = format!("int{bits}");
        for value in values(bits) {
            let wrapper = decoded(&abi_type, &[value]);
            assert_eq!(wrapper.to_type(), Type::NUMERIC, "{abi_type} binds to its NUMERIC column");
            let mut bytes = BytesMut::new();
            wrapper.to_sql(&Type::NUMERIC, &mut bytes).unwrap();
            let actual = rust_decimal::Decimal::from_sql(&Type::NUMERIC, &bytes).unwrap();
            assert_eq!(actual.to_string(), value.to_string());
        }
        let wrapper = decoded(&format!("{abi_type}[]"), &values(bits));
        assert_eq!(wrapper.to_type(), Type::NUMERIC_ARRAY);
        let mut bytes = BytesMut::new();
        wrapper.to_sql(&Type::NUMERIC_ARRAY, &mut bytes).unwrap();
        let actual = Vec::<rust_decimal::Decimal>::from_sql(&Type::NUMERIC_ARRAY, &bytes).unwrap();
        assert_eq!(
            actual.iter().map(ToString::to_string).collect::<Vec<_>>(),
            values(bits).iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    }
}

#[test]
fn signed_numeric_rejects_unrelated_bind_types_before_writing() {
    for (wrapper, ty) in
        [(decoded("int64", &[42]), Type::INT4), (decoded("int64[]", &[42]), Type::INT4_ARRAY)]
    {
        let mut bytes = BytesMut::new();
        assert!(wrapper.to_sql(&ty, &mut bytes).is_err());
        assert!(bytes.is_empty());
    }
}

async fn write(
    client: &Client,
    table: &str,
    wrapper: &EthereumSqlTypeWrapper,
    copy_type: Option<Type>,
) {
    if let Some(ty) = copy_type {
        let sink = client
            .copy_in(&format!("COPY {table} (payload) FROM STDIN WITH (FORMAT binary)"))
            .await
            .unwrap();
        let writer = BinaryCopyInWriter::new(sink, &[ty]);
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

async fn numeric_roundtrip(via_copy: bool, array: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    // int64(42) is first: its old BIGINT bytes were accepted as numeric zero.
    for bits in [64, 40, 48, 56] {
        let abi_type = format!("int{bits}{}", if array { "[]" } else { "" });
        let (_, inputs) = event(&abi_type);
        let table = format!("signed_{bits}_{}", u8::from(array));
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE {table} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        let values = values(bits);
        let cases = if array {
            vec![vec![42], values.clone(), vec![]]
        } else {
            values.into_iter().map(|value| vec![value]).collect()
        };
        for values in cases {
            let wrapper = decoded(&abi_type, &values);
            write(&client, &table, &wrapper, via_copy.then(|| wrapper.to_type())).await;
            let actual: Option<String> = client
                .query_one(&format!("SELECT payload::text FROM {table}"), &[])
                .await
                .unwrap()
                .get(0);
            let expected = if values.is_empty() {
                None
            } else if array {
                Some(format!(
                    "{{{}}}",
                    values.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
                ))
            } else {
                Some(values[0].to_string())
            };
            assert_eq!(
                actual,
                expected,
                "{abi_type} via {}",
                if via_copy { "COPY" } else { "INSERT" }
            );
            client.batch_execute(&format!("TRUNCATE {table}")).await.unwrap();
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
async fn postgres_signed_numeric_insert_preserves_scalars() {
    numeric_roundtrip(false, false).await;
}

#[tokio::test]
async fn postgres_signed_numeric_copy_preserves_scalars() {
    numeric_roundtrip(true, false).await;
}

#[tokio::test]
async fn postgres_signed_numeric_insert_preserves_arrays() {
    numeric_roundtrip(false, true).await;
}

#[tokio::test]
async fn postgres_signed_numeric_copy_preserves_arrays() {
    numeric_roundtrip(true, true).await;
}

#[tokio::test]
async fn postgres_signed_bigint_columns_keep_their_schema_encoding() {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute("CREATE TEMP TABLE signed_bigint (payload BIGINT); CREATE TEMP TABLE signed_bigint_array (payload BIGINT[])").await.unwrap();
    for copy in [false, true] {
        for value in values(64) {
            write(
                &client,
                "signed_bigint",
                &EthereumSqlTypeWrapper::I64(value),
                copy.then_some(Type::INT8),
            )
            .await;
            let actual: i64 =
                client.query_one("SELECT payload FROM signed_bigint", &[]).await.unwrap().get(0);
            assert_eq!(actual, value);
            client.batch_execute("TRUNCATE signed_bigint").await.unwrap();
        }
        write(
            &client,
            "signed_bigint_array",
            &EthereumSqlTypeWrapper::VecI64(values(64)),
            copy.then_some(Type::INT8_ARRAY),
        )
        .await;
        let actual: Vec<i64> =
            client.query_one("SELECT payload FROM signed_bigint_array", &[]).await.unwrap().get(0);
        assert_eq!(actual, values(64));
        client.batch_execute("TRUNCATE signed_bigint_array").await.unwrap();
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[test]
fn signed_numeric_empty_arrays_keep_null() {
    for bits in [40, 48, 56, 64] {
        let wrapper = decoded(&format!("int{bits}[]"), &[]);
        let mut output = BytesMut::new();
        assert!(matches!(wrapper.to_sql(&wrapper.to_type(), &mut output).unwrap(), IsNull::Yes));
        assert!(output.is_empty());
    }
}

#[test]
fn signed_numeric_clickhouse_projection_preserves_decoded_values() {
    for bits in [40, 48, 56, 64] {
        for value in values(bits) {
            assert_eq!(
                decoded(&format!("int{bits}"), &[value]).to_clickhouse_value(),
                value.to_string()
            );
        }
    }
}
