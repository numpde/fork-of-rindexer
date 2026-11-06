//! Factory bookkeeping must complete when a successfully stored discovery is replayed.
use std::sync::Arc;

use alloy::{
    dyn_abi::DynSolValue,
    primitives::{Address, LogData, U64},
    rpc::types::ValueOrArray,
};
use serde_json::json;

use super::super::{
    no_code_callback,
    permanent_failure_tests::{registry, ID},
};
use super::Fixture;
use crate::{
    blockclock::BlockClock,
    database::{
        generate::{generate_internal_factory_event_table_name, generate_tables_for_indexer_sql},
        postgres::generate::GenerateInternalFactoryEventTableNameParams,
    },
    event::{
        config::FactoryEventProcessingConfig,
        contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    },
    indexer::IndexingEventsProgressState,
    provider::mock::MockChainProvider,
};

#[tokio::test]
async fn factory_discovery_replay_and_overlap_preserve_unique_addresses() {
    assert_factory_replay(3).await;
}

#[tokio::test]
async fn factory_discovery_large_replay_and_overlap_preserve_unique_addresses() {
    assert_factory_replay(130).await;
}

async fn assert_factory_replay(size: u16) {
    let definition = json!({"name": "Created", "type": "event", "anonymous": false,
        "inputs": [{"name": "child", "type": "address", "indexed": false}]});
    let fixture = Fixture::with_event(definition, "Factory").await;
    let factory = Address::repeat_byte(5);
    let mut indexer = fixture.indexer.clone();
    indexer.contracts[0].details[0].factory = Some(serde_json::from_value(json!({
        "name": "Factory", "address": factory, "event_name": "Created", "input_name": "child", "abi": "abi.json"
    })).unwrap());
    fixture
        .client
        .batch_execute(
            generate_tables_for_indexer_sql(fixture._directory.path(), &indexer, false)
                .unwrap()
                .as_str(),
        )
        .await
        .unwrap();
    let table =
        generate_internal_factory_event_table_name(&GenerateInternalFactoryEventTableNameParams {
            indexer_name: fixture.params.indexer_name.clone(),
            contract_name: "Factory".to_string(),
            event_name: "Created".to_string(),
            input_names: vec!["child".to_string()],
        });
    let mut params = fixture.params.clone();
    params.tables = Arc::new(vec![]);
    params.is_factory_event = true;
    let callback = no_code_callback(Arc::new(params.clone())).event_callback;
    let registry = registry(&params, callback).complete();
    let provider = Arc::new(MockChainProvider::new(1));
    let config = FactoryEventProcessingConfig {
        id: ID.to_string(),
        project_path: fixture._directory.path().to_path_buf(),
        indexer_name: params.indexer_name.clone(),
        contract_name: params.contract_name.clone(),
        address: ValueOrArray::Value(factory),
        input_name: ValueOrArray::Value("child".to_string()),
        event: params.event.clone(),
        config: Default::default(),
        network_contract: Arc::new(NetworkContract {
            id: ID.to_string(),
            network: "ethereum".to_string(),
            indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                address: ValueOrArray::Value(factory),
                indexed_filters: None,
            }),
            cached_provider: provider.clone(),
            block_clock: BlockClock::new(None, None, provider),
            decoder: crate::event::callback_registry::noop_decoder(),
            start_block: Some(U64::from(1)),
            end_block: Some(U64::from(2)),
            disable_logs_bloom_checks: false,
        }),
        timestamps: false,
        start_block: U64::from(1),
        end_block: U64::from(2),
        progress: IndexingEventsProgressState::monitor(&registry.events, &[], None).await,
        registry,
        postgres: Some(fixture.client.clone()),
        clickhouse: None,
        csv_details: None,
        stream_last_synced_block_file_path: None,
        index_event_in_order: true,
        live_indexing: false,
        indexing_distance_from_head: U64::ZERO,
        cancel_token: Default::default(),
        tables: Arc::new(vec![]),
        reorg_sender: None,
        streams_clients: Arc::new(None),
        contract_abi: None,
        providers: params.providers.clone(),
        constants: params.constants.clone(),
        multicall_addresses: params.multicall_addresses.clone(),
    };
    let events = |range: std::ops::RangeInclusive<u16>| {
        range
            .map(|index| {
                let mut event = fixture.events(&[u64::from(index)]).remove(0);
                let mut bytes = [0; 20];
                bytes[18..].copy_from_slice(&index.to_be_bytes());
                event.log.inner.address = factory;
                event.tx_information.address = factory;
                event.log.inner.data = LogData::new_unchecked(
                    vec![params.event.selector()],
                    DynSolValue::Tuple(vec![DynSolValue::Address(Address::from(bytes))])
                        .abi_encode_params()
                        .into(),
                );
                event
            })
            .collect::<Vec<_>>()
    };
    let initial = config.trigger_event(events(1..=size)).await;
    let repeated = config.trigger_event(events(1..=size)).await;
    let overlap = config.trigger_event(events(size..=size * 2)).await;
    let (first, second) = tokio::join!(
        config.trigger_event(events(size * 2..=size * 3)),
        config.trigger_event(events(size * 2..=size * 3))
    );
    let count: i64 = fixture
        .client
        .query_one(
            &format!("SELECT count(*) FROM rindexer_internal.{table} WHERE network = 'ethereum'"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let position = fixture.position().await;
    drop(config);
    fixture.finish().await;

    initial.unwrap();
    repeated.expect("repeated factory discovery must finish its bookkeeping");
    overlap.expect("overlap must retain old addresses and admit new ones");
    first.unwrap();
    second.unwrap();
    assert_eq!(count, i64::from(size * 3));
    assert_eq!(
        position,
        (i64::from(size * 3), "0".to_string(), "0".to_string()),
        "raw PG receipts protect events while factory completion keeps the cursor deferred"
    );
}

#[tokio::test]
async fn postgres_factory_cron_samples_the_child_from_its_birth_block() {
    use crate::{
        database::generate::generate_prepared_tables_sql,
        indexer::{cron_scheduler::CronScheduler, no_code::process_events_prepared},
        provider::{ChainProvider, CreateNetworkProvider},
        PreparedProject,
    };
    use std::collections::HashMap;

    let fixture = Fixture::new().await;
    let directory = fixture._directory.path();
    let emitter = Address::repeat_byte(0xaa);
    let child = Address::repeat_byte(0xbb);
    // contractAddress and contract_address have different owners: the first is
    // the decoded child, the second is metadata naming the emitting factory.
    std::fs::write(
        directory.join("factory.json"),
        serde_json::to_vec(&json!([{
            "name":"Created","type":"event","anonymous":false,
            "inputs":[{"name":"contractAddress","type":"address","indexed":false}]
        }]))
        .unwrap(),
    )
    .unwrap();
    let project = PreparedProject::try_new(
        directory,
        serde_json::from_value(json!({
            "name":"FactoryCron","project_type":"no-code","storage":{"postgres":{"enabled":true}},
            "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}],
            "contracts":[{"name":"Child","abi":"abi.json","include_events":["Value"],
                "details":[{"network":"ethereum","factory":{"name":"Factory","abi":"factory.json",
                    "event_name":"Created","input_name":"contractAddress","address":emitter}}],
                "tables":[{"name":"samples","columns":[{"name":"sampled","type":"address"}],
                    "cron":[{"start_block":"1","end_block":"2","operations":[{"type":"insert",
                        "set":[{"column":"sampled","action":"set","value":"$contract"}]}]}]}]}]
        }))
        .unwrap(),
    )
    .unwrap();
    fixture
        .client
        .batch_execute(generate_prepared_tables_sql(project.schema(), false).unwrap().as_str())
        .await
        .unwrap();
    let provider = Arc::new(MockChainProvider::new(1));
    let callbacks = process_events_prepared(
        directory,
        project.manifest(),
        project.schema(),
        Some(fixture.client.clone()),
        None,
        &[CreateNetworkProvider {
            network_name: "ethereum".into(),
            disable_logs_bloom_checks: true,
            client: provider.clone(),
        }],
    )
    .await
    .unwrap();
    let discovery = callbacks.iter().find(|event| event.event_name == "Created").unwrap();
    let mut log = fixture.events(&[1]).pop().unwrap();
    assert_eq!(log.tx_information.block_number, 2);
    log.log.inner.address = emitter;
    log.tx_information.address = emitter;
    log.log.inner.data = LogData::new_unchecked(
        vec![discovery.topic_id],
        DynSolValue::Tuple(vec![DynSolValue::Address(child)]).abi_encode_params().into(),
    );
    (discovery.callback)(vec![log]).await.unwrap();
    let scheduler = CronScheduler::try_new_prepared(
        project.schema(),
        Some(fixture.client.clone()),
        None,
        Arc::new(HashMap::from([("ethereum".into(), provider as Arc<dyn ChainProvider>)])),
    )
    .unwrap();
    let addresses = scheduler.sample_factory_for_test(&[1, 2]).await;
    let rows = fixture.client.query(
        "SELECT sampled, rindexer_contract_address, rindexer_block_number::text FROM factory_cron_child.samples", &[]
    ).await.unwrap();
    drop(scheduler);
    drop(callbacks);
    fixture.finish().await;
    assert_eq!(
        addresses.expect("look up the admitted discovery source"),
        HashMap::from([(child, 2)])
    );
    assert_eq!(rows.len(), 1, "no pre-birth row or emitter sample");
    assert_eq!(rows[0].get::<_, String>(0), format!("{child:#x}"));
    assert_eq!(rows[0].get::<_, String>(1), format!("{child:#x}"));
    assert_eq!(rows[0].get::<_, String>(2), "2");
}
