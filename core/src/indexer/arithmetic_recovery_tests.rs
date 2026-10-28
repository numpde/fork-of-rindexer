//! Arithmetic rollback must restore the canonical row, including replay ordering.
use super::journal_recovery_tests::at;
use super::*;
use crate::{
    indexer::reorg::{
        BlockChainWindow, DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
        EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::{ColumnType, OperationType, SetAction},
    provider::mock::MockChainProvider,
};
use alloy::primitives::{Address, U256};

async fn fixture() -> Fixture {
    Fixture::with_table(
        serde_json::to_value(params().event).unwrap(),
        "Contract",
        json!({
            "name":"totals","global":true,"timestamp":true,
            "columns":[{"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert","set":[
                {"column":"amount","action":"add","value":"$value"}]}]}]
        }),
    )
    .await
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
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".into(),
        BlockChainWindow::try_new(8).unwrap(),
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        Arc::new(MockChainProvider::new(1).with_blocks(vec![
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
        vec![DerivedTableInfo::try_new(
            fixture.params.tables[0].full_table_name.clone(),
            false,
            vec![DerivedTableRollbackOp::try_new(
                format!("{schema}.value"),
                vec![],
                vec![DerivedColumnRollback::try_new(
                    "amount".into(),
                    "value".into(),
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
    .unwrap();
    let mut plan = crate::indexer::reorg::task::DerivedTableRollbackPlan::default();
    plan.try_add_operation(
        fixture.params.tables[0].full_table_name.clone(),
        0,
        vec![],
        [("value".into(), ColumnType::Uint256)].into(),
        [("amount".into(), ColumnType::Uint256)].into(),
    )
    .unwrap();
    coordinator = coordinator.with_rollback_plan(plan);
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
