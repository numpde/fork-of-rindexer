//! Native transfer credits require successful execution in the processed block.
use std::{sync::Arc, time::Duration};

use alloy::primitives::U64;
use serde_json::{json, Value};
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

use super::{no_code_callback, Fixture};
use crate::{
    event::{
        callback_registry::{TraceCallbackRegistry, TraceCallbackRegistryInformation},
        config::TraceProcessingConfig,
        contract_setup::TraceInformation,
    },
    indexer::{
        native_transfer::{native_transfer_block_consumer, EVENT_NAME},
        progress::IndexingEventsProgressState,
    },
    manifest::native_transfer::TraceProcessingMethod,
    provider::create_client,
};

async fn anvil_url() -> (String, Option<ContainerAsync<GenericImage>>) {
    if let Some(url) = std::env::var_os("RINDEXER_TEST_ANVIL_URL") {
        assert_eq!(url, "http://anvil:8545", "only the disposable internal fixture is allowed");
        return (url.into_string().unwrap(), None);
    }
    let image = include_str!("fixtures/anvil-image.txt").trim();
    // This Image API joins name/tag with ':', including a digest reference.
    let (name, digest) = image.rsplit_once(':').unwrap();
    let container = GenericImage::new(name, digest)
        .with_exposed_port(8545_u16.into())
        .with_entrypoint("/usr/local/bin/anvil")
        .with_wait_for(WaitFor::message_on_stdout("Listening on"))
        .with_cmd(["--host", "0.0.0.0", "--chain-id", "31337"])
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await
        .unwrap();
    let url = format!(
        "http://{}:{}",
        container.get_host().await.unwrap(),
        container.get_host_port_ipv4(8545).await.unwrap()
    );
    (url, Some(container))
}

async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Value {
    let response: Value = client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(response.get("error").is_none(), "{method} failed: {response}");
    response["result"].clone()
}

async fn receipt(client: &reqwest::Client, url: &str, hash: &Value) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = rpc(client, url, "eth_getTransactionReceipt", json!([hash])).await;
            if !result.is_null() {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn config(fixture: &Fixture, block: u64) -> Arc<TraceProcessingConfig> {
    let mut registry = TraceCallbackRegistry::new();
    registry.register_event(TraceCallbackRegistryInformation {
        id: "native-status".to_string(),
        indexer_name: fixture.params.indexer_name.clone(),
        contract_name: fixture.params.contract_name.clone(),
        event_name: EVENT_NAME.to_string(),
        trace_information: TraceInformation {
            name: EVENT_NAME.to_string(),
            details: vec![],
            reorg_safe_distance: None,
        },
        callback: no_code_callback(Arc::new(fixture.params.clone())).trace_callback,
        tables: fixture.params.tables.clone(),
        streams_clients: fixture.params.streams_clients.clone(),
    });
    Arc::new(TraceProcessingConfig {
        id: "native-status".to_string(),
        chain_id: 31337,
        project_path: fixture._directory.path().to_path_buf(),
        start_block: U64::from(block),
        end_block: U64::from(block),
        indexer_name: fixture.params.indexer_name.clone(),
        contract_name: fixture.params.contract_name.clone(),
        event_name: EVENT_NAME.to_string(),
        network: "ethereum".to_string(),
        progress: IndexingEventsProgressState::monitor(&[], &[], None).await,
        postgres: Some(fixture.client.clone()),
        csv_details: None,
        registry: registry.complete(),
        method: TraceProcessingMethod::TraceBlock,
        stream_last_synced_block_file_path: None,
        cancel_token: CancellationToken::new(),
    })
}

#[tokio::test]
async fn postgres_native_transfer_execution_status_on_anvil() {
    let fixture = Fixture::new_trace().await;
    let (url, _anvil) = anvil_url().await;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    assert_eq!(rpc(&client, &url, "eth_chainId", json!([])).await, "0x7a69");
    let sender = rpc(&client, &url, "eth_accounts", json!([])).await[0].clone();
    let mut addresses = Vec::new();
    // Constructor copies/returns its runtime: STOP accepts value; REVERT rejects it.
    // No Solidity compiler or network dependency acquisition is needed at test time.
    for initcode in ["0x6001600c60003960016000f300", "0x6005600c60003960056000f360006000fd"] {
        let hash = rpc(
            &client,
            &url,
            "eth_sendTransaction",
            json!([{"from": sender, "data": initcode, "gas": "0x1e8480"}]),
        )
        .await;
        let result = receipt(&client, &url, &hash).await;
        assert_eq!(result["status"], "0x1");
        addresses.push(result["contractAddress"].clone());
    }
    rpc(&client, &url, "evm_setAutomine", json!([false])).await;
    let mut hashes = Vec::new();
    for address in &addresses {
        hashes.push(rpc(&client, &url, "eth_sendTransaction", json!([{"from": sender, "to": address, "value": "0x309", "data": "0x", "gas": "0x186a0"}])).await);
    }
    rpc(&client, &url, "evm_mine", json!([])).await;
    let accepted = receipt(&client, &url, &hashes[0]).await;
    let reverted = receipt(&client, &url, &hashes[1]).await;
    assert_eq!(accepted["status"], "0x1");
    assert_eq!(reverted["status"], "0x0");
    assert_eq!(accepted["blockHash"], reverted["blockHash"]);
    assert_eq!(
        rpc(&client, &url, "eth_getBalance", json!([addresses[0], "latest"])).await,
        "0x309"
    );
    assert_eq!(rpc(&client, &url, "eth_getBalance", json!([addresses[1], "latest"])).await, "0x0");
    let block =
        u64::from_str_radix(accepted["blockNumber"].as_str().unwrap().trim_start_matches("0x"), 16)
            .unwrap();
    let provider =
        create_client(&url, 31337, None, None, None, Default::default(), None, None).await.unwrap();
    let config = config(&fixture, block).await;
    native_transfer_block_consumer(provider, &[U64::from(block)], "ethereum", &config)
        .await
        .unwrap();
    assert_eq!(
        fixture.position().await,
        (1, "777".to_string(), block.to_string()),
        "reverted execution cannot create a raw transfer or derived credit"
    );
    let stored_hash: String = fixture
        .client
        .query_one(&format!("SELECT tx_hash FROM {}", fixture.params.sql_event_table_name), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_hash, hashes[0].as_str().unwrap());
    drop(config);
    fixture.finish().await;
}
