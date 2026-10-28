//! Arithmetic rollback must restore the canonical row, including replay ordering.
use super::journal_recovery_tests::at;
use super::*;
use crate::{
    indexer::reorg::{
        BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::{ColumnType, OperationType},
    provider::mock::MockChainProvider,
};
use alloy::primitives::{Address, U256};

async fn fixture() -> Fixture {
    fixture_with_mixed_operations(false).await
}

async fn fixture_with_mixed_operations(mixed: bool) -> Fixture {
    let mut table = json!({
        "name":"totals","global":true,"timestamp":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","set":[
            {"column":"amount","action":"add","value":"$value"}]}]}]
    });
    if mixed {
        table["columns"].as_array_mut().unwrap().push(json!({"name":"latest","type":"uint256"}));
        table["events"][0]["operations"][0]["set"]
            .as_array_mut()
            .unwrap()
            .push(json!({"column":"latest","action":"set","value":"$value"}));
    }
    Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", table).await
}

fn input(fixture: &Fixture, block: u64, branch: u8, value: u64) -> EventResult {
    let mut event = at(fixture, "ethereum", block, B256::repeat_byte(branch), value);
    event.tx_information.block_timestamp = Some(U256::from(1000 + branch as u64));
    event.log.block_timestamp = Some(1000 + branch as u64);
    event.tx_information.address = Address::repeat_byte(branch);
    event.log.inner.address = Address::repeat_byte(branch);
    event
}

async fn rollback(fixture: &Fixture) {
    rollback_with_ancestor(fixture, 99).await.unwrap();
}

async fn rollback_with_ancestor(fixture: &Fixture, branch: u8) -> anyhow::Result<()> {
    let mut ancestor =
        crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(branch), 1099);
    ancestor.header.inner.number = 99;
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let mut derived = std::collections::HashMap::new();
    let mut plans = std::collections::HashMap::new();
    crate::indexer::start::build_derived_tables_for_event(
        "Value",
        &fixture.params.indexer_name,
        "Contract",
        "ethereum",
        &[("value".into(), ColumnType::Uint256)].into(),
        &fixture.params.tables,
        &mut derived,
        &mut plans,
    )
    .unwrap();
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".into(),
        BlockChainWindow::try_new(8).unwrap(),
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        Arc::new(MockChainProvider::new(1).with_blocks(vec![
            ancestor,
            crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(101), 1101),
        ])),
        vec![EventTableInfo::try_new(
            schema.clone(),
            "value".into(),
            generate_internal_event_table_name(&schema, "Value"),
            fixture.params.indexer_name.clone(),
            "Contract".into(),
            "Value".into(),
        )
        .unwrap()],
        derived.remove("ethereum").unwrap(),
        vec![],
    )
    .unwrap()
    .with_rollback_plan(plans.remove("ethereum").unwrap());
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
        .map(|_| ())
}

async fn state(fixture: &Fixture) -> (String, serde_json::Value) {
    let row = fixture.client.query_one(&format!("SELECT amount::text, (to_jsonb(t)-'amount') || jsonb_build_object('rindexer_sequence_id', rindexer_sequence_id::text) FROM {} t",fixture.params.tables[0].full_table_name),&[]).await.unwrap();
    (row.get(0), row.get(1))
}

async fn replacement_case(update: bool) {
    let mut fixture = fixture().await;
    let seed = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    seed(vec![input(&fixture, 99, 99, 10)]).await.unwrap();
    drop(seed);
    if update {
        Arc::make_mut(&mut fixture.params.tables)[0].table.events[0].operations[0].operation_type =
            OperationType::Update;
    }
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    rollback(&fixture).await;
    callback(vec![input(&fixture, 100, 101, 30)]).await.unwrap();
    let (amount, metadata) = state(&fixture).await;
    assert_eq!(amount, "40", "replacement arithmetic is applied at the identical event position");
    assert_eq!(
        metadata,
        json!({"network":"ethereum",
            "rindexer_sequence_id":crate::manifest::contract::compute_sequence_id(100,3,4).to_string(),
            "rindexer_block_number":100,"rindexer_block_hash":format!("{:#x}",B256::repeat_byte(101)),
            "rindexer_tx_hash":format!("{:#x}",B256::repeat_byte(30)),
            "rindexer_contract_address":format!("{:#x}",Address::repeat_byte(101)),
            "rindexer_block_timestamp":chrono::DateTime::from_timestamp(1101,0).unwrap().to_rfc3339()
        })
    );
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_recovery_restores_all_metadata_without_replacement() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 99, 99, 10)]).await.unwrap();
    let canonical = state(&fixture).await;
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    rollback(&fixture).await;
    assert_eq!(state(&fixture).await, canonical);
    drop(callback);
    fixture.finish().await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_upsert_replacement_has_canonical_provenance() {
    replacement_case(false).await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_update_applies_same_position_replacement() {
    replacement_case(true).await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_removes_orphan_only_row() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    rollback(&fixture).await;
    let count: i64 = fixture
        .client
        .query_one(
            &format!("SELECT count(*) FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "no canonical contribution means no surviving derived row");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_recovery_mixed_table_restores_values_and_metadata_together() {
    let fixture = fixture_with_mixed_operations(true).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 99, 99, 10)]).await.unwrap();
    let canonical = state(&fixture).await;
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    rollback(&fixture).await;
    assert_eq!(state(&fixture).await, canonical);
    callback(vec![input(&fixture, 100, 101, 30)]).await.unwrap();
    let (amount, metadata) = state(&fixture).await;
    assert_eq!(amount, "40");
    assert_eq!(metadata["latest"], json!(30));
    assert_eq!(metadata["rindexer_block_hash"], json!(format!("{:#x}", B256::repeat_byte(101))));
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_recovery_resolves_missing_timestamp_by_matching_hash() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 99, 99, 10)]).await.unwrap();
    let canonical = state(&fixture).await;
    fixture
        .client
        .batch_execute(&format!(
            "UPDATE {} SET block_timestamp = NULL",
            fixture.params.sql_event_table_name
        ))
        .await
        .unwrap();
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    let orphaned = state(&fixture).await;
    assert!(
        rollback_with_ancestor(&fixture, 98).await.is_err(),
        "a different header cannot supply a canonical row's timestamp"
    );
    assert_eq!(
        state(&fixture).await,
        orphaned,
        "failed recovery must roll back its arithmetic and metadata changes"
    );
    assert_eq!(fixture.position().await.0, 2, "failed recovery must retain both raw events");
    rollback(&fixture).await;
    assert_eq!(state(&fixture).await, canonical);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_recovery_orders_sources_for_a_partial_metadata_target() {
    let fixture = fixture().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![
        input(&fixture, 98, 98, 5),
        input(&fixture, 99, 99, 10),
        input(&fixture, 100, 100, 20),
    ])
    .await
    .unwrap();
    // Older/manual tables can expose just the block marker. Source ordering still
    // needs the complete event position even when the target has no sequence column.
    let table = &fixture.params.tables[0].full_table_name;
    fixture.client.batch_execute(&format!("ALTER TABLE {table} DROP COLUMN rindexer_sequence_id, DROP COLUMN rindexer_block_timestamp, DROP COLUMN rindexer_block_hash, DROP COLUMN rindexer_tx_hash, DROP COLUMN rindexer_contract_address")).await.unwrap();
    rollback(&fixture).await;
    let row = fixture
        .client
        .query_one(&format!("SELECT amount::text, rindexer_block_number FROM {table}"), &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "15");
    assert_eq!(row.get::<_, i64>(1), 99);
    drop(callback);
    fixture.finish().await;
}

async fn separate_column_recovery(action: &str, latest: serde_json::Value) {
    let fixture = Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", json!({
        "name":"totals","global":true,"timestamp":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"},latest],
        "events":[{"event":"Value","operations":[
            {"type":"upsert","if":"value == 10","set":[{"column":"amount","action":"add","value":"$value"}]},
            {"type":"update","if":"value == 20","set":[{"column":"latest","action":action,"value":"$value"}]}
        ]}]
    })).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![input(&fixture, 99, 99, 10)]).await.unwrap();
    let canonical = state(&fixture).await;
    callback(vec![input(&fixture, 100, 100, 20)]).await.unwrap();
    assert_ne!(state(&fixture).await, canonical, "the orphan must change the row");
    rollback(&fixture).await;
    assert_eq!(
        state(&fixture).await,
        canonical,
        "every column and all metadata must match the pre-orphan row"
    );
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_arithmetic_recovery_separate_column_set_restores_default() {
    separate_column_recovery("set", json!({"name":"latest","type":"uint256","default":"7"})).await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_separate_column_max_restores_default() {
    separate_column_recovery("max", json!({"name":"latest","type":"uint256","default":"7"})).await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_separate_column_min_restores_default() {
    separate_column_recovery("min", json!({"name":"latest","type":"uint256","default":"42"})).await;
}
#[tokio::test]
async fn postgres_arithmetic_recovery_separate_column_set_restores_null() {
    separate_column_recovery("set", json!({"name":"latest","type":"uint256","nullable":true}))
        .await;
}
