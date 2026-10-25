//! Indexed complex fields contain one topic digest, never recoverable tuple/array values.
use alloy::{
    json_abi::Event,
    primitives::{keccak256, Address, B256, U256},
    rpc::types::Log,
};
use futures::pin_mut;
use serde_json::json;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type},
    NoTls,
};

use super::{
    postgres::generate::generate_columns_with_data_types,
    sql_type_wrapper::{
        map_ethereum_wrapper_to_json, map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper,
    },
    unsigned_integer_tests::postgres_fixture_url,
};
use crate::{abi::ABIInput, event::callback_registry::TxInformation, helpers::parse_log};

fn decoded(kind: &str) -> (Vec<ABIInput>, EthereumSqlTypeWrapper, B256) {
    let mut input = json!({"name": "payload", "type": kind, "indexed": true});
    if kind.starts_with("tuple") {
        input["components"] =
            json!([{"name": "x", "type": "uint256"}, {"name": "y", "type": "uint256"}]);
    }
    // Indexed array/tuple preimages concatenate their uint256 member words;
    // string/bytes preimages are their unpadded contents.
    let preimage = if kind == "string" || kind == "bytes" {
        b"tag".to_vec()
    } else {
        [U256::from(1).to_be_bytes::<32>(), U256::from(2).to_be_bytes::<32>()].concat()
    };
    let digest = keccak256(preimage);
    let inputs: Vec<ABIInput> = serde_json::from_value(json!([input.clone()])).unwrap();
    let event: Event = serde_json::from_value(
        json!({"type": "event", "name": "Tagged", "anonymous": false, "inputs": [input]}),
    )
    .unwrap();
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector(), digest],
            vec![].into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).unwrap();
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1);
    (inputs, wrappers.pop().unwrap(), digest)
}

const COMPLEX: &[&str] = &["uint256[]", "uint256[2]", "tuple", "tuple[]", "tuple[1]"];

#[test]
fn indexed_complex_fields_have_one_bytea_hash_column() {
    for kind in COMPLEX {
        let (inputs, wrapper, _) = decoded(kind);
        assert_eq!(generate_columns_with_data_types(&inputs), vec!["\"payload\" BYTEA"], "{kind}");
        assert_eq!(wrapper.to_type(), Type::BYTEA, "{kind}");
        let clickhouse = ABIInput::generate_abi_name_properties(
            &inputs,
            &crate::abi::GenerateAbiPropertiesType::ClickhouseWithDataTypes,
            None,
        );
        assert_eq!(
            clickhouse.iter().map(|property| property.value.as_str()).collect::<Vec<_>>(),
            vec!["\"payload\" String"],
            "{kind}"
        );
    }
}

#[test]
fn indexed_tuple_json_is_the_digest_without_reconstructed_members() {
    let (inputs, wrapper, digest) = decoded("tuple");
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
    let actual = map_ethereum_wrapper_to_json(&inputs, &[wrapper], &transaction, false);
    assert_eq!(actual["payload"], hex::encode(digest));
}

async fn roundtrip(via_copy: bool) {
    let (url, _container) = postgres_fixture_url().await;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    for (index, kind) in COMPLEX.iter().copied().chain(["bytes", "string"]).enumerate() {
        let (inputs, wrapper, digest) = decoded(kind);
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE hashes_{index} ({})",
                generate_columns_with_data_types(&inputs).join(", ")
            ))
            .await
            .unwrap();
        if via_copy {
            let sink = client
                .copy_in(&format!("COPY hashes_{index} (payload) FROM STDIN WITH (FORMAT binary)"))
                .await
                .unwrap();
            let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
            pin_mut!(writer);
            writer.as_mut().write(&[&wrapper as &(dyn ToSql + Sync)]).await.unwrap();
            writer.finish().await.unwrap();
        } else {
            client
                .execute(&format!("INSERT INTO hashes_{index} (payload) VALUES ($1)"), &[&wrapper])
                .await
                .unwrap();
        }
        let row =
            client.query_one(&format!("SELECT payload FROM hashes_{index}"), &[]).await.unwrap();
        if kind == "string" {
            assert_eq!(row.get::<_, String>(0), format!("{digest:#x}"));
        } else {
            assert_eq!(row.get::<_, Vec<u8>>(0), digest.as_slice(), "{kind}");
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
async fn postgres_indexed_hashes_insert() {
    roundtrip(false).await;
}

#[tokio::test]
async fn postgres_indexed_hashes_copy() {
    roundtrip(true).await;
}

#[test]
fn unindexed_tuple_members_still_flatten_into_their_original_columns() {
    let (mut inputs, _, _) = decoded("tuple");
    inputs[0].indexed = Some(false);
    assert_eq!(
        generate_columns_with_data_types(&inputs),
        vec!["\"payload_x\" VARCHAR(78)", "\"payload_y\" VARCHAR(78)",]
    );
}
