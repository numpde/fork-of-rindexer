//! ABI transport padding is not part of a bytesN value; logical trailing zeros are.
use alloy::{
    dyn_abi::DynSolValue,
    json_abi::Event,
    primitives::{Address, B256},
    rpc::types::Log,
};
use bytes::BytesMut;
use futures::pin_mut;
use serde_json::{json, Value};
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{FromSql, IsNull, ToSql, Type},
    NoTls,
};

use super::{
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::{map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper},
    unsigned_integer_tests::postgres_fixture_url,
};
use crate::{abi::ABIInput, helpers::parse_log};

#[derive(Clone, Copy, Debug)]
enum Shape {
    Scalar,
    IndexedScalar,
    Array,
    TupleArray,
}

fn logical_values(size: usize) -> Vec<Vec<u8>> {
    let mut first = vec![0x11; size];
    first[size - 1] = 0;
    vec![first, vec![0x22; size]]
}

fn decoded(
    kind: &str,
    shape: Shape,
    values: &[Vec<u8>],
) -> (Vec<ABIInput>, EthereumSqlTypeWrapper) {
    let mut input = json!({"name": "payload", "indexed": matches!(shape, Shape::IndexedScalar)});
    input["type"] = json!(match shape {
        Shape::Scalar | Shape::IndexedScalar => kind.to_string(),
        Shape::Array => format!("{kind}[]"),
        Shape::TupleArray => "tuple[]".to_string(),
    });
    if matches!(shape, Shape::TupleArray) {
        input["components"] = json!([{"name": "value", "type": kind}]);
    }
    let inputs: Vec<ABIInput> = serde_json::from_value(json!([input.clone()])).unwrap();
    let event: Event = serde_json::from_value(
        json!({"type": "event", "name": "BytesValues", "anonymous": false, "inputs": [input]}),
    )
    .unwrap();
    let tokens: Vec<_> = values
        .iter()
        .map(|value| {
            if kind == "bytes" {
                DynSolValue::Bytes(value.clone())
            } else {
                let mut word = [0; 32];
                word[..value.len()].copy_from_slice(value);
                DynSolValue::FixedBytes(B256::from(word), value.len())
            }
        })
        .collect();
    let token = match shape {
        Shape::Scalar | Shape::IndexedScalar => {
            assert_eq!(tokens.len(), 1);
            tokens.into_iter().next().unwrap()
        }
        Shape::Array => DynSolValue::Array(tokens),
        Shape::TupleArray => DynSolValue::Array(
            tokens.into_iter().map(|token| DynSolValue::Tuple(vec![token])).collect(),
        ),
    };
    let encoded = DynSolValue::Tuple(vec![token]).abi_encode_params();
    let (topics, data) = if matches!(shape, Shape::IndexedScalar) {
        (vec![event.selector(), B256::from_slice(&encoded)], vec![])
    } else {
        (vec![event.selector()], encoded)
    };
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(Address::ZERO, topics, data.into()),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).unwrap();
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1);
    (inputs, wrappers.pop().unwrap())
}

fn expected_json(values: &[Vec<u8>]) -> Value {
    json!(values
        .iter()
        .map(|value| json!({"value": format!("0x{}", hex::encode(value))}))
        .collect::<Vec<_>>())
}

fn assert_encoding(shape: Shape) {
    for size in [1, 4, 31, 32] {
        let values = logical_values(size);
        let values = if matches!(shape, Shape::Scalar | Shape::IndexedScalar) {
            &values[..1]
        } else {
            &values[..]
        };
        let (_, wrapper) = decoded(&format!("bytes{size}"), shape, values);
        let mut bytes = BytesMut::new();
        let ty = wrapper.to_type();
        assert!(matches!(wrapper.to_sql(&ty, &mut bytes).unwrap(), IsNull::No));
        match shape {
            Shape::Scalar | Shape::IndexedScalar => assert_eq!(
                Vec::<u8>::from_sql(&Type::BYTEA, &bytes).unwrap(),
                values[0],
                "bytes{size}"
            ),
            Shape::Array => assert_eq!(
                Vec::<Vec<u8>>::from_sql(&Type::BYTEA_ARRAY, &bytes).unwrap(),
                values,
                "bytes{size}[]"
            ),
            Shape::TupleArray => assert_eq!(
                Value::from_sql(&Type::JSONB, &bytes).unwrap(),
                expected_json(values),
                "tuple bytes{size}"
            ),
        }
    }
}

#[test]
fn fixed_bytes_scalars_preserve_declared_length_and_trailing_zeros() {
    assert_encoding(Shape::Scalar);
}
#[test]
fn fixed_bytes_arrays_preserve_declared_lengths_and_trailing_zeros() {
    assert_encoding(Shape::Array);
}
#[test]
fn fixed_bytes_tuple_array_json_preserves_declared_lengths() {
    assert_encoding(Shape::TupleArray);
}

async fn roundtrip(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let mut index = 0;
    for size in [1, 4, 31, 32] {
        for shape in [Shape::Scalar, Shape::IndexedScalar, Shape::Array, Shape::TupleArray] {
            for kind in [format!("bytes{size}"), "bytes".to_string()] {
                // Indexed dynamic bytes are a hash, not the original byte value.
                if kind == "bytes" && matches!(shape, Shape::IndexedScalar) {
                    continue;
                }
                let values = logical_values(size);
                let values = if matches!(shape, Shape::Scalar | Shape::IndexedScalar) {
                    &values[..1]
                } else {
                    &values[..]
                };
                let (inputs, wrapper) = decoded(&kind, shape, values);
                let table = format!("fixed_bytes_{index}");
                index += 1;
                client
                    .batch_execute(&format!(
                        "CREATE TEMP TABLE {table} ({})",
                        generate_columns_with_data_types(&inputs).join(", ")
                    ))
                    .await
                    .unwrap();
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
                let row =
                    client.query_one(&format!("SELECT payload FROM {table}"), &[]).await.unwrap();
                match shape {
                    Shape::Scalar | Shape::IndexedScalar => {
                        assert_eq!(row.get::<_, Vec<u8>>(0), values[0], "{kind}")
                    }
                    Shape::Array => assert_eq!(row.get::<_, Vec<Vec<u8>>>(0), values, "{kind}[]"),
                    Shape::TupleArray => {
                        assert_eq!(row.get::<_, Value>(0), expected_json(values), "tuple {kind}")
                    }
                }
            }
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
async fn postgres_fixed_bytes_insert() {
    roundtrip(false).await;
}
#[tokio::test]
async fn postgres_fixed_bytes_copy() {
    roundtrip(true).await;
}

#[test]
fn empty_fixed_byte_arrays_keep_existing_null_and_json_representations() {
    for size in [1, 4, 31, 32] {
        let kind = format!("bytes{size}");
        let (_, array) = decoded(&kind, Shape::Array, &[]);
        let mut bytes = BytesMut::new();
        assert!(matches!(array.to_sql(&array.to_type(), &mut bytes).unwrap(), IsNull::Yes));
        assert!(bytes.is_empty());
        let (_, tuples) = decoded(&kind, Shape::TupleArray, &[]);
        assert!(matches!(tuples.to_sql(&Type::JSONB, &mut bytes).unwrap(), IsNull::No));
        assert_eq!(Value::from_sql(&Type::JSONB, &bytes).unwrap(), json!([]));
    }
}

#[test]
fn indexed_fixed_byte_scalars_preserve_their_declared_lengths() {
    assert_encoding(Shape::IndexedScalar);
}
