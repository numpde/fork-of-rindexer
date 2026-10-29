//! Recovery must invalidate the same network suffix in every durable store.
use super::journal_recovery_tests::at;
use super::*;
use crate::{
    indexer::reorg::{
        BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::ColumnType,
    provider::mock::MockChainProvider,
};

pub(super) async fn rollback(fixture: &Fixture, fork: u64, detection: u64) -> anyhow::Result<()> {
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
    let blocks = (99..=105)
        .map(|number| {
            let hash = if number == 99 { 99 } else { number + 1 };
            let parent = if number == 100 { 99 } else { number };
            let mut header = crate::indexer::tables::view_call_cache_tests::header(
                B256::repeat_byte(hash),
                1000 + u64::from(number),
            );
            header.header.inner.number = u64::from(number);
            header.header.inner.parent_hash = B256::repeat_byte(parent);
            header
        })
        .collect();
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".into(),
        BlockChainWindow::try_new(16).unwrap(),
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        Arc::new(MockChainProvider::new(1).with_block_number(105).with_blocks(blocks)),
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
    .with_rollback_plan(plans.remove("ethereum").unwrap_or_default());
    let task = coordinator.try_create_reorg_task_for_block_range(fork, detection).unwrap();
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
}

async fn uneven_subscription_rollback(operation: &str, action: &str) {
    let fixture = Fixture::with_table(
        serde_json::to_value(params().event).unwrap(),
        "Contract",
        json!({"name":"totals", "global":true,
            "columns":[{"name":"amount","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":operation,"set":[
                {"column":"amount","action":action,"value":"$value"}]}]}]}),
    )
    .await;
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let cursor_table = generate_internal_event_table_name(&schema, "Value");
    fixture.client.execute(&format!("INSERT INTO rindexer_internal.{cursor_table} (network,cursor_key,last_synced_block) VALUES ('ethereum','slow',0),('ethereum','fast',0) ON CONFLICT DO NOTHING"), &[]).await.unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (block, value, cursor) in [(99, 10, "slow"), (100, 20, "slow"), (105, 30, "fast")] {
        let mut event = at(&fixture, "ethereum", block, B256::repeat_byte(block as u8), value);
        event.found_in_request.cursor_key = cursor.into();
        callback(vec![event]).await.unwrap();
    }
    // The request belongs to a lagging subscription; the exclusive barrier must
    // also account for the faster subscription's committed suffix.
    rollback(&fixture, 100, 100).await.unwrap();
    let raw = fixture
        .client
        .query_one(
            &format!(
                "SELECT count(*), max(block_number)::text FROM {}",
                fixture.params.sql_event_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!((raw.get::<_,i64>(0),raw.get::<_,String>(1)),(1,"99".into()),"raw rows above the detecting stream's range must not survive while their effects are removed");
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1, "every invalidated effect must release its receipt for replay");
    let row = fixture
        .client
        .query_one(
            &format!(
                "SELECT count(*), sum(amount)::text FROM {}",
                fixture.params.tables[0].full_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, String>(1)), (1, "10".into()));
    let cursors = fixture.client.query(&format!("SELECT last_synced_block::text FROM rindexer_internal.{cursor_table} WHERE cursor_key IN ('slow','fast')"),&[]).await.unwrap();
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|row| row.get::<_, String>(0) == "99"));
    let replacement = at(&fixture, "ethereum", 105, B256::repeat_byte(106), 7);
    callback(vec![replacement.clone()]).await.unwrap();
    callback(vec![replacement]).await.unwrap();
    let expected = if action == "add" || operation == "insert" { "17" } else { "7" };
    let amount: String = fixture
        .client
        .query_one(
            &format!("SELECT sum(amount)::text FROM {}", fixture.params.tables[0].full_table_name),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        amount, expected,
        "replacement replay must not be suppressed by a surviving old receipt"
    );
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_recovery_interval_includes_faster_arithmetic_subscription() {
    uneven_subscription_rollback("upsert", "add").await;
}
#[tokio::test]
async fn postgres_recovery_interval_includes_faster_journal_subscription() {
    uneven_subscription_rollback("upsert", "set").await;
}
#[tokio::test]
async fn postgres_recovery_interval_includes_faster_insert_subscription() {
    uneven_subscription_rollback("insert", "set").await;
}
