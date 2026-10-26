//! Journal recovery must restore canonical values and their replay ordering.
use super::*;
use crate::{
    indexer::reorg::{
        task::{DerivedColumnJournal, DerivedTableInfo, EventTableInfo},
        BlockChainWindow, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::SetAction,
    provider::mock::MockChainProvider,
};
use alloy::primitives::{U256, U64};

async fn fixture(action: &str, value_type: &str, cross_chain: bool) -> Fixture {
    let mut definition = serde_json::to_value(params().event).unwrap();
    definition["inputs"][0]["type"] = json!(value_type);
    Fixture::with_table(
        definition,
        "Contract",
        json!({
            "name":"totals", "cross_chain":cross_chain,
            "columns":[{"name":"id","type":"uint64"},{"name":"amount","type":value_type}],
            "events":[{"event":"Value","operations":[{"type":"upsert","where":{"id":"1"},
                "set":[{"column":"amount","action":action,"value":"$value"}]}]}]
        }),
    )
    .await
}

fn at(fixture: &Fixture, network: &str, block: u64, hash: B256, value: u64) -> EventResult {
    let mut event = fixture.events(&[value]).remove(0);
    event.tx_information.network = network.to_string();
    event.log.inner.data = alloy::primitives::LogData::new_unchecked(
        event.log.topics().to_vec(),
        U256::from(value).to_be_bytes::<32>().to_vec().into(),
    );
    event.log.block_number = Some(block);
    event.log.block_hash = Some(hash);
    event.log.transaction_index = Some(3);
    event.log.log_index = Some(4);
    event.tx_information.block_number = block;
    event.tx_information.block_hash = hash;
    event.tx_information.transaction_index = 3;
    event.tx_information.log_index = U256::from(4);
    event.found_in_request.from_block = U64::from(block);
    event.found_in_request.to_block = U64::from(block);
    event
}

async fn rollback(fixture: &Fixture, action: SetAction, cross_chain: bool) {
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let cursor = generate_internal_event_table_name(&schema, "Value");
    let mut coordinator =
        ReorgCoordinator::new(
            "ethereum".into(),
            BlockChainWindow::try_new(8).unwrap(),
            Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
            Arc::new(MockChainProvider::new(1).with_blocks(vec![
                crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(101), 10),
            ])),
            vec![EventTableInfo::try_new(
                schema,
                "value".into(),
                cursor,
                fixture.params.indexer_name.clone(),
                "Contract".into(),
                "Value".into(),
            )
            .unwrap()],
            vec![DerivedTableInfo::try_new(
                fixture.params.tables[0].full_table_name.clone(),
                cross_chain,
                vec![],
                vec![DerivedColumnJournal::try_new("amount".into(), action, vec!["id".into()])
                    .unwrap()],
            )
            .unwrap()],
            vec![],
        )
        .unwrap();
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
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
}

async fn amount(fixture: &Fixture) -> String {
    fixture
        .client
        .query_one(
            &format!("SELECT amount::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn postgres_journal_rollback_restores_sequence_and_provenance_before_replacement() {
    let fixture = fixture("set", "uint64", false).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), 10)]).await.unwrap();
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 20)]).await.unwrap();
    rollback(&fixture, SetAction::Set, false).await;
    assert_eq!(amount(&fixture).await, "10");
    let metadata = fixture.client.query_one(&format!("SELECT rindexer_sequence_id::text, rindexer_block_number, rindexer_block_hash FROM {}",fixture.params.tables[0].full_table_name),&[]).await.unwrap();
    assert_eq!(
        metadata.get::<_, String>(0),
        crate::manifest::contract::compute_sequence_id(99, 3, 4).to_string()
    );
    assert_eq!(metadata.get::<_, i64>(1), 99);
    assert_eq!(metadata.get::<_, String>(2), format!("{:#x}", B256::repeat_byte(99)));
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(101), 30)]).await.unwrap();
    assert_eq!(amount(&fixture).await, "30");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_uint256_maximum_journal_survives_reorg_without_replacement_event() {
    let fixture = fixture("max", "uint256", false).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), 500)]).await.unwrap();
    callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 1000)]).await.unwrap();
    assert_eq!(amount(&fixture).await, "1000");
    let journal_count: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.derived_op_log", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal_count, 2, "ordinary uint256 inputs must have durable recovery records");
    rollback(&fixture, SetAction::Max, false).await;
    assert_eq!(amount(&fixture).await, "500");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_journal_failure_aborts_every_application_effect_and_retry_applies_once()
{
    let fixture = fixture("set", "uint64", false).await;
    fixture.client.batch_execute("ALTER TABLE rindexer_internal.derived_op_log ADD CONSTRAINT reject_journal CHECK (block_number < 0)").await.unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let event = at(&fixture, "ethereum", 99, B256::repeat_byte(99), 500);
    let before = fixture.position().await;
    let result = callback(vec![event.clone()]).await;
    assert!(
        matches!(result, Err(crate::event::callback_registry::CallbackError::Retryable(_))),
        "required recovery record failure must fail the callback: {result:?}"
    );
    assert_eq!(fixture.position().await, before);
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 0);
    fixture
        .client
        .batch_execute(
            "ALTER TABLE rindexer_internal.derived_op_log DROP CONSTRAINT reject_journal",
        )
        .await
        .unwrap();
    callback(vec![event.clone()]).await.unwrap();
    callback(vec![event]).await.unwrap();
    assert_eq!(fixture.position().await, (1, "500".to_string(), "99".to_string()));
    drop(callback);
    fixture.finish().await;
}
