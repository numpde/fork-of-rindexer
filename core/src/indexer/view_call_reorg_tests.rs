//! Canonical replacement events must use replacement historical contract state.
use super::*;
use crate::{
    indexer::{
        reorg::{
            task::{
                DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
                DerivedTableRollbackPlan, EventTableInfo,
            },
            BlockChainWindow, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
        },
        tables::view_call_cache_tests::{header, HistoricalRpc, CALL, ORIGINAL, REPLACEMENT},
    },
    manifest::contract::SetAction,
    provider::{create_client, mock::MockChainProvider},
};

#[tokio::test]
async fn postgres_reorg_replaces_cached_view_state_without_restart() {
    let mut fixture = Fixture::new().await;
    let rpc = HistoricalRpc::start().await;
    let mut tables = fixture.params.tables.as_ref().clone();
    tables[0].table.events[0].operations[0].set[0].value = Some(CALL.to_string());
    fixture.params.tables = Arc::new(tables);
    fixture.params.providers = Arc::new(
        [(
            "ethereum".to_string(),
            create_client(&rpc.url, 1, None, None, None, Default::default(), None, None)
                .await
                .unwrap() as Arc<dyn crate::provider::ChainProvider>,
        )]
        .into(),
    );
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let events_at = |hash| {
        let mut events = fixture.events(&[1, 2]);
        for event in &mut events {
            let value = alloy::primitives::U256::from(if hash == ORIGINAL { 10 } else { 20 });
            event.log.inner.data = alloy::primitives::LogData::new_unchecked(
                event.log.topics().to_vec(),
                value.to_be_bytes::<32>().to_vec().into(),
            );
            event.log.block_number = Some(100);
            event.log.block_hash = Some(hash);
            event.tx_information.block_number = 100;
            event.tx_information.block_hash = hash;
            event.found_in_request.from_block = alloy::primitives::U64::from(100);
            event.found_in_request.to_block = alloy::primitives::U64::from(100);
        }
        events
    };
    callback(vec![events_at(ORIGINAL).remove(0)]).await.unwrap();
    callback(vec![events_at(ORIGINAL).remove(1)]).await.unwrap();
    assert_eq!(fixture.position().await, (2, "20".to_string(), "100".to_string()));
    assert_eq!(rpc.request_count().await, 1, "same-block cache hit control");
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let cursor = generate_internal_event_table_name(&schema, "Value");
    let mut window = BlockChainWindow::try_new(8).unwrap();
    window.insert(100, ORIGINAL, B256::repeat_byte(99));
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        Arc::new(MockChainProvider::new(1).with_blocks(vec![header(REPLACEMENT, 20)])),
        vec![EventTableInfo::try_new(
            schema,
            "value".to_string(),
            cursor,
            fixture.params.indexer_name.clone(),
            "Contract".to_string(),
            "Value".to_string(),
        )
        .unwrap()],
        vec![DerivedTableInfo::try_new(
            fixture.params.tables[0].full_table_name.clone(),
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
    .unwrap();
    let mut plan = DerivedTableRollbackPlan::default();
    plan.try_add_operation(
        fixture.params.tables[0].full_table_name.clone(),
        0,
        vec![],
        [("value".to_string(), crate::manifest::contract::ColumnType::Uint256)].into(),
        [("amount".to_string(), crate::manifest::contract::ColumnType::Uint256)].into(),
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
    assert_eq!(fixture.position().await, (0, "0".to_string(), "99".to_string()));
    rpc.replace();
    callback(vec![events_at(REPLACEMENT).remove(0)]).await.unwrap();
    assert_eq!(
        fixture.position().await,
        (1, "20".to_string(), "100".to_string()),
        "replacement derived state must come from B"
    );
    assert_eq!(rpc.request_count().await, 2);
    rpc.assert_hash_qualified().await;
    drop(callback);
    drop(coordinator);
    rpc.stop().await;
    fixture.finish().await;
}
