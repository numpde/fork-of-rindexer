//! Native transfer credits require successful execution in the processed block.
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use alloy::{
    network::{AnyRpcBlock, AnyTransactionReceipt},
    primitives::{B256, U64},
};
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
    provider::{create_client, mock::MockChainProvider, ChainProvider},
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

async fn processing_config(
    fixture: &Fixture,
    block: u64,
) -> (Arc<TraceProcessingConfig>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).trace_callback;
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
        callback: Arc::new(move |traces| {
            observed.fetch_add(1, Ordering::SeqCst);
            callback(traces)
        }),
        tables: fixture.params.tables.clone(),
        streams_clients: fixture.params.streams_clients.clone(),
    });
    let config = Arc::new(TraceProcessingConfig {
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
    });
    (config, calls)
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
    let (config, calls) = processing_config(&fixture, block).await;
    native_transfer_block_consumer(provider.clone(), &[U64::from(block)], "ethereum", &config)
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
    assert_eq!(calls.load(Ordering::SeqCst), 2, "block callback and successful transfer callback");
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1, "the reverted attempt must not claim an application receipt");

    let blocks = provider.get_block_by_number_batch(&[U64::from(block)], true).await.unwrap();
    assert_receipt_failures_leave_range_unfinished(blocks, accepted, reverted, block).await;
    drop(config);
    fixture.finish().await;
}

async fn assert_receipt_failures_leave_range_unfinished(
    blocks: Vec<AnyRpcBlock>,
    accepted: Value,
    reverted: Value,
    block: u64,
) {
    let fixture = Fixture::new_trace().await;
    let (config, calls) = processing_config(&fixture, block).await;
    let mut cases = vec![
        ("missing receipt".to_string(), vec![accepted.clone()]),
        ("duplicate receipt".to_string(), vec![accepted.clone(), accepted.clone()]),
        ("no receipts".to_string(), vec![]),
    ];
    for index in 0..2 {
        for (field, value) in [
            ("blockHash", json!(B256::repeat_byte(0xaa))),
            ("blockNumber", json!("0xffff")),
            ("transactionIndex", json!("0xff")),
            ("transactionHash", json!(B256::repeat_byte(0xbb))),
            ("from", json!(alloy::primitives::Address::repeat_byte(0xcc))),
            ("to", json!(alloy::primitives::Address::repeat_byte(0xdd))),
            ("blockHash", Value::Null),
            ("blockNumber", Value::Null),
            ("transactionIndex", Value::Null),
        ] {
            let mut receipts = vec![accepted.clone(), reverted.clone()];
            receipts[index][field] = value;
            cases.push((format!("receipt {index}, {field}"), receipts));
        }
    }
    let mut legacy = accepted.clone();
    legacy.as_object_mut().unwrap().remove("status");
    legacy["type"] = json!("0x0");
    legacy["root"] = json!(B256::repeat_byte(0xee));
    cases.push(("state-root-only receipt".to_string(), vec![legacy, reverted.clone()]));
    for (case, receipts) in cases {
        let receipts: Vec<AnyTransactionReceipt> =
            receipts.into_iter().map(|receipt| serde_json::from_value(receipt).unwrap()).collect();
        let provider = Arc::new(
            MockChainProvider::new(31337)
                .with_blocks(blocks.clone())
                .with_receipt_response(receipts),
        );
        let result =
            native_transfer_block_consumer(provider, &[U64::from(block)], "ethereum", &config)
                .await;
        let error = result.expect_err(&format!("{case} cannot authorize range completion"));
        assert!(matches!(&error, crate::indexer::process::ProcessEventError::ProviderCallError(_)));
        let diagnostic = error.to_string();
        assert!(
            diagnostic.starts_with("Could not complete provider operation:"),
            "{case}: {diagnostic}"
        );
        assert!(
            diagnostic.contains("ethereum")
                && diagnostic.contains("transaction")
                && diagnostic.contains("range cannot be checkpointed"),
            "{case}: {diagnostic}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{case} must fail before either callback");
        assert_eq!(fixture.position().await, (0, "0".to_string(), "0".to_string()), "{case}");
    }
    for field in ["block_hash", "block_number", "transaction_index"] {
        let mut wrong = blocks.clone();
        let alloy::rpc::types::BlockTransactions::Full(transactions) = &mut wrong[0].transactions
        else {
            panic!("full Anvil block")
        };
        match field {
            "block_hash" => transactions[0].block_hash = Some(B256::repeat_byte(0xff)),
            "block_number" => transactions[0].block_number = Some(999),
            "transaction_index" => transactions[0].transaction_index = Some(999),
            _ => unreachable!(),
        }
        let receipts = vec![
            serde_json::from_value(accepted.clone()).unwrap(),
            serde_json::from_value(reverted.clone()).unwrap(),
        ];
        let provider = Arc::new(
            MockChainProvider::new(31337).with_blocks(wrong).with_receipt_response(receipts),
        );
        assert!(
            native_transfer_block_consumer(provider, &[U64::from(block)], "ethereum", &config)
                .await
                .is_err(),
            "transaction {field} must agree with its block"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.position().await, (0, "0".to_string(), "0".to_string()));
    }
    // Recovery accepts a complete response in arbitrary receipt order.
    let receipts =
        vec![serde_json::from_value(reverted).unwrap(), serde_json::from_value(accepted).unwrap()];
    let provider =
        Arc::new(MockChainProvider::new(31337).with_blocks(blocks).with_receipt_response(receipts));
    native_transfer_block_consumer(provider, &[U64::from(block)], "ethereum", &config)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.position().await, (1, "777".to_string(), block.to_string()));
    drop(config);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_native_queue_recovery_preserves_replacement_prefix_on_anvil() {
    use crate::indexer::{
        native_transfer::{
            native_block_channel, native_transfer_block_processor, NativeBlockRequest,
        },
        reorg::{
            task::{
                DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
                DerivedTableRollbackPlan, EventTableInfo,
            },
            BlockChainWindow, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
        },
    };
    use crate::{
        database::generate::generate_indexer_contract_schema_name,
        database::postgres::generate::generate_internal_event_table_name,
        manifest::contract::{ColumnType, SetAction},
    };
    let fixture = Fixture::new_trace().await;
    let (url, _anvil) = anvil_url().await;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    rpc(&client, &url, "anvil_reset", json!([])).await;
    rpc(&client, &url, "evm_setAutomine", json!([true])).await;
    let sender = rpc(&client, &url, "eth_accounts", json!([])).await[0].clone();
    let deployment = rpc(
        &client,
        &url,
        "eth_sendTransaction",
        json!([{"from":sender, "data":"0x6001600c60003960016000f300", "gas":"0x1e8480"}]),
    )
    .await;
    let recipient = receipt(&client, &url, &deployment).await["contractAddress"].clone();
    rpc(&client, &url, "anvil_mine", json!(["0x60"])).await;
    assert_eq!(rpc(&client, &url, "eth_blockNumber", json!([])).await, "0x61");
    let snapshot = rpc(&client, &url, "evm_snapshot", json!([])).await;
    rpc(&client, &url, "evm_setAutomine", json!([false])).await;
    for number in 98..=110 {
        let hash = rpc(
            &client,
            &url,
            "eth_sendTransaction",
            json!([{"from":sender,"to":recipient,"value":"0xa","data":"0x","gas":"0x186a0"}]),
        )
        .await;
        rpc(&client, &url, "evm_mine", json!([])).await;
        assert_eq!(receipt(&client, &url, &hash).await["blockNumber"], format!("0x{number:x}"));
    }
    let provider =
        create_client(&url, 31337, None, None, None, Default::default(), None, None).await.unwrap();
    let (mut config, _) = processing_config(&fixture, 110).await;
    Arc::get_mut(&mut config).unwrap().start_block = U64::from(98);
    native_transfer_block_consumer(
        provider.clone(),
        &[U64::from(98), U64::from(99), U64::from(100)],
        "ethereum",
        &config,
    )
    .await
    .unwrap();
    assert_eq!(fixture.position().await, (3, "30".to_string(), "100".to_string()));
    let old_headers = provider
        .get_block_by_number_batch(&(97..=110).map(U64::from).collect::<Vec<_>>(), false)
        .await
        .unwrap();
    // Real FIFO requests already planned before rollback. Delay replacement delivery.
    let (requests, queued) = native_block_channel("ethereum", U64::from(101), 32);
    let planned_generation = crate::indexer::reorg::reorg_generation("ethereum");
    for number in 101..=110 {
        requests
            .sender
            .send(NativeBlockRequest {
                block_number: U64::from(number),
                generation: planned_generation,
                expected_hash: old_headers
                    .iter()
                    .find(|block| block.header.number == number)
                    .unwrap()
                    .header
                    .hash,
            })
            .await
            .unwrap();
    }
    assert!(rpc(&client, &url, "evm_revert", json!([snapshot])).await.as_bool().unwrap());
    rpc(&client, &url, "evm_setAutomine", json!([false])).await;
    for number in 98..=110 {
        let hash = rpc(
            &client,
            &url,
            "eth_sendTransaction",
            json!([{"from":sender,"to":recipient,"value":"0x14","data":"0x","gas":"0x186a0"}]),
        )
        .await;
        rpc(&client, &url, "evm_mine", json!([])).await;
        assert_eq!(receipt(&client, &url, &hash).await["blockNumber"], format!("0x{number:x}"));
    }
    let mut window = BlockChainWindow::try_new(32).unwrap();
    for block in old_headers {
        window.insert(block.header.number, block.header.hash, block.header.parent_hash);
    }
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let checkpoint = generate_internal_event_table_name(&schema, EVENT_NAME);
    let totals = fixture.params.tables[0].full_table_name.clone();
    let mut plan = DerivedTableRollbackPlan::default();
    plan.try_add_operation(
        totals.clone(),
        0,
        vec![],
        [("value".to_string(), ColumnType::Uint256)].into(),
        [("amount".to_string(), ColumnType::Uint256)].into(),
    )
    .unwrap();
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        provider.clone(),
        vec![EventTableInfo::try_new(
            schema,
            "native_transfer".to_string(),
            checkpoint,
            fixture.params.indexer_name.clone(),
            fixture.params.contract_name.clone(),
            EVENT_NAME.to_string(),
        )
        .unwrap()],
        vec![DerivedTableInfo::try_new(
            totals,
            false,
            vec![DerivedTableRollbackOp::try_new(
                fixture.params.sql_event_table_name.clone(),
                vec![],
                vec![DerivedColumnRollback::try_new(
                    "amount".to_string(),
                    "value".to_string(),
                    SetAction::Add,
                )
                .unwrap()],
                None,
            )
            .unwrap()],
            vec![],
        )
        .unwrap()],
        vec![],
    )
    .unwrap()
    .with_rollback_plan(plan);
    let task = coordinator.try_create_reorg_task_for_block_range(98, 110).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(fixture.position().await, (0, "0".to_string(), "97".to_string()));
    let processor = tokio::spawn(native_transfer_block_processor(
        "ethereum".to_string(),
        provider.clone(),
        config.clone(),
        queued,
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while requests.sender.capacity() != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Draining ten requests requires completing the first five-item batch in the old pipeline.
    // No replacement prefix has been scheduled, so no cursor beyond 97 is justified.
    assert_eq!(
        fixture.position().await,
        (0, "0".to_string(), "97".to_string()),
        "queued later blocks must not certify missing replacement history"
    );
    processor.abort();
    assert!(processor.await.unwrap_err().is_cancelled());
    drop(requests);
    drop(config);
    // Restart from the durable checkpoint with fresh processing state and a fresh queue.
    let saved: u64 = fixture.position().await.2.parse().unwrap();
    let (mut restarted, _) = processing_config(&fixture, 110).await;
    Arc::get_mut(&mut restarted).unwrap().start_block = U64::from(saved + 1);
    let (requests, queue) = native_block_channel("ethereum", U64::from(saved + 1), 32);
    let planned_generation = crate::indexer::reorg::reorg_generation("ethereum");
    let required = (saved + 1..=110).map(U64::from).collect::<Vec<_>>();
    for block in provider.get_block_by_number_batch(&required, false).await.unwrap() {
        let number = block.header.number;
        requests
            .sender
            .send(NativeBlockRequest {
                block_number: U64::from(number),
                generation: planned_generation,
                expected_hash: block.header.hash,
            })
            .await
            .unwrap();
    }
    drop(requests);
    native_transfer_block_processor("ethereum".to_string(), provider, restarted.clone(), queue)
        .await
        .unwrap();
    assert_eq!(fixture.position().await, (13, "260".to_string(), "110".to_string()));
    let row = fixture.client.query_one(&format!("SELECT count(DISTINCT block_number), min(block_number)::text, max(block_number)::text FROM {}", fixture.params.sql_event_table_name), &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 13);
    assert_eq!(row.get::<_, String>(1), "98");
    assert_eq!(row.get::<_, String>(2), "110");
    rpc(&client, &url, "evm_setAutomine", json!([true])).await;
    drop(restarted);
    drop(coordinator);
    fixture.finish().await;
}

async fn native_startup_includes_first_required_block(restart: bool) {
    use crate::event::contract_setup::NetworkTrace;
    let fixture = Fixture::new_trace().await;
    let (url, _anvil) = anvil_url().await;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    rpc(&client, &url, "anvil_reset", json!([])).await;
    rpc(&client, &url, "evm_setAutomine", json!([true])).await;
    let sender = rpc(&client, &url, "eth_accounts", json!([])).await[0].clone();
    let deployment = rpc(
        &client,
        &url,
        "eth_sendTransaction",
        json!([{"from":sender,"data":"0x6001600c60003960016000f300","gas":"0x1e8480"}]),
    )
    .await;
    let recipient = receipt(&client, &url, &deployment).await["contractAddress"].clone();
    let before = if restart { 99u64 } else { 97u64 };
    rpc(&client, &url, "anvil_mine", json!([format!("0x{:x}", before - 1)])).await;
    if restart {
        let hash = rpc(
            &client,
            &url,
            "eth_sendTransaction",
            json!([{"from":sender,"to":recipient,"value":"0xa","gas":"0x186a0"}]),
        )
        .await;
        assert_eq!(receipt(&client, &url, &hash).await["blockNumber"], "0x64");
    }
    let first = if restart { 101u64 } else { 98u64 };
    for block in first..=first + 1 {
        let hash = rpc(
            &client,
            &url,
            "eth_sendTransaction",
            json!([{"from":sender,"to":recipient,"value":"0x14","gas":"0x186a0"}]),
        )
        .await;
        assert_eq!(receipt(&client, &url, &hash).await["blockNumber"], format!("0x{block:x}"));
    }
    // Keep the finite range below the tip so publisher completion is independent
    // of waiting for a future head after the configured endpoint.
    rpc(&client, &url, "anvil_mine", json!(["0x1"])).await;
    let provider =
        create_client(&url, 31337, None, None, None, Default::default(), None, None).await.unwrap();
    if restart {
        let (config, _) = processing_config(&fixture, 100).await;
        native_transfer_block_consumer(provider.clone(), &[U64::from(100)], "ethereum", &config)
            .await
            .unwrap();
        assert_eq!(fixture.position().await, (1, "10".to_string(), "100".to_string()));
    }
    let (config, _) = processing_config(&fixture, first + 1).await;
    let mut registration = config.registry.events[0].clone();
    registration.trace_information.details = vec![NetworkTrace {
        id: registration.id.clone(),
        network: "ethereum".to_string(),
        cached_provider: provider,
        start_block: Some(U64::from(98)),
        end_block: Some(U64::from(first + 1)),
        method: TraceProcessingMethod::TraceBlock,
    }];
    let mut registry = TraceCallbackRegistry::new();
    registry.register_event(registration);
    let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({"name":"Replay","project_type":"no-code","networks":[],"contracts":[],
        "native_transfers":{"enabled":true,"networks":[{"network":"ethereum","start_block":98,"end_block":first + 1}]}})).unwrap();
    let handles = crate::indexer::start::start_indexing_traces(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &fixture.indexer,
        registry.complete(),
        CancellationToken::new(),
        config.progress.clone(),
        &Default::default(),
        true,
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    })
    .await
    .expect("finite native startup completes");
    assert_eq!(
        fixture.position().await,
        (
            if restart { 3 } else { 2 },
            if restart { "50" } else { "40" }.to_string(),
            (first + 1).to_string()
        ),
        "startup must include block {first}"
    );
    let receipts:i64 = fixture.client.query_one("SELECT count(*) FROM rindexer_internal.event_applications WHERE block_number >= $1::text::numeric",&[&first.to_string()]).await.unwrap().get(0);
    assert_eq!(receipts, 2, "both required blocks have application receipts");
    drop(config);
    fixture.finish().await;
}
#[tokio::test]
async fn postgres_native_startup_includes_configured_first_block_on_anvil() {
    native_startup_includes_first_required_block(false).await;
}
#[tokio::test]
async fn postgres_native_startup_resumes_at_first_unfinished_block_on_anvil() {
    native_startup_includes_first_required_block(true).await;
}
