//! Wide signed ABI arrays must use PostgreSQL's array framing, not joined text.
use alloy::{
    primitives::{Address, I256, U256},
    rpc::types::Log,
};
use bytes::BytesMut;
use futures::pin_mut;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{FromSql, IsNull, ToSql, Type},
    NoTls,
};

use super::{
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::{map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper},
    unsigned_integer_tests::{event, postgres_fixture_url},
};

fn values(bits: usize) -> Vec<I256> {
    let max = I256::from_raw((U256::from(1) << (bits - 1)) - U256::from(1));
    let min = -max - I256::try_from(1_i64).unwrap();
    vec![I256::try_from(1_i64).unwrap(), I256::try_from(-1_i64).unwrap(), min, max, I256::ZERO]
}

fn decoded(bits: usize, values: &[I256]) -> EthereumSqlTypeWrapper {
    let (event, inputs) = event(&format!("int{bits}[]"));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&U256::from(32).to_be_bytes::<32>());
    bytes.extend_from_slice(&U256::from(values.len()).to_be_bytes::<32>());
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes::<32>());
    }
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            bytes.into(),
        ),
        ..Default::default()
    };
    let parsed = crate::helpers::parse_log(&event, &log).expect("valid signed ABI array");
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1);
    wrappers.pop().unwrap()
}

#[test]
fn signed_wide_arrays_use_postgres_array_framing() {
    for bits in (136..=256).step_by(8) {
        let values = values(bits);
        let wrapper = decoded(bits, &values);
        assert_eq!(wrapper.to_type(), Type::VARCHAR_ARRAY);
        let mut bytes = BytesMut::new();
        assert!(matches!(wrapper.to_sql(&Type::VARCHAR_ARRAY, &mut bytes).unwrap(), IsNull::No));
        let actual =
            Vec::<String>::from_sql(&Type::VARCHAR_ARRAY, &bytes).expect("binary array framing");
        assert_eq!(actual, values.iter().map(ToString::to_string).collect::<Vec<_>>());
    }
}

#[test]
fn signed_wide_arrays_reject_scalar_bind_types_before_writing() {
    let wrapper = decoded(256, &[I256::try_from(1_i64).unwrap(), I256::try_from(-1_i64).unwrap()]);
    let mut bytes = BytesMut::new();
    assert!(wrapper.to_sql(&Type::VARCHAR, &mut bytes).is_err());
    assert!(bytes.is_empty());
}

#[test]
fn signed_wide_empty_arrays_keep_null_and_clickhouse_projection() {
    for bits in (136..=256).step_by(8) {
        let mut bytes = BytesMut::new();
        let empty = decoded(bits, &[]);
        assert!(matches!(empty.to_sql(&empty.to_type(), &mut bytes).unwrap(), IsNull::Yes));
        assert!(bytes.is_empty());
        let values = values(bits);
        assert_eq!(
            decoded(bits, &values).to_clickhouse_value(),
            format!("[{}]", values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
        );
    }
}

async fn roundtrip(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    // Exercise int256 first, then every other ABI width using the same wrapper.
    for bits in std::iter::once(256).chain((136..256).step_by(8)) {
        let (_, inputs) = event(&format!("int{bits}[]"));
        let table = format!("signed_wide_{bits}");
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE {table} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        for values in [
            vec![I256::try_from(1_i64).unwrap(), I256::try_from(-1_i64).unwrap()],
            values(bits),
            vec![],
        ] {
            let wrapper = decoded(bits, &values);
            if via_copy {
                let sink = client
                    .copy_in(&format!("COPY {table} (payload) FROM STDIN WITH (FORMAT binary)"))
                    .await
                    .unwrap();
                let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
                pin_mut!(writer);
                writer.as_mut().write(&[&wrapper as &(dyn ToSql + Sync)]).await.unwrap();
                writer.finish().await.unwrap();
            } else {
                client
                    .execute(&format!("INSERT INTO {table} (payload) VALUES ($1)"), &[&wrapper])
                    .await
                    .unwrap();
            }
            let actual: Option<Vec<String>> = client
                .query_one(&format!("SELECT payload FROM {table}"), &[])
                .await
                .unwrap()
                .get(0);
            let expected = (!values.is_empty())
                .then(|| values.iter().map(ToString::to_string).collect::<Vec<_>>());
            assert_eq!(actual, expected, "int{bits}[]");
            client.batch_execute(&format!("TRUNCATE {table}")).await.unwrap();
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
async fn postgres_signed_wide_array_insert_preserves_order_and_extrema() {
    roundtrip(false).await;
}

#[tokio::test]
async fn postgres_signed_wide_array_copy_preserves_order_and_extrema() {
    roundtrip(true).await;
}
