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
        config.trigger_event(events(1..=size)),
        config.trigger_event(events(size..=size * 2))
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
    assert_eq!(count, i64::from(size * 2));
    assert_eq!(
        position,
        (i64::from(size * 2), "0".to_string(), "0".to_string()),
        "raw PG receipts protect events while factory completion keeps the cursor deferred"
    );
}
