//! Cross-chain sharing must retain each source network's independent history.
use std::collections::HashMap;

use super::journal_recovery_tests::at;
use super::*;
use crate::{
    indexer::reorg::{
        BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::ColumnType,
    provider::mock::MockChainProvider,
};

async fn fixture(operation: &str, action: &str, timestamp: bool) -> Fixture {
    Fixture::with_table_on_networks(
        serde_json::to_value(params().event).unwrap(), "Contract",
        json!({"name":"shared", "cross_chain":true, "timestamp":timestamp,
            "columns":[{"name":"id", "type":"uint64"}, {"name":"amount", "type":"uint256", "default":"0"}],
            "events":[{"event":"Value", "operations":[{"type":operation, "where":{"id":"1"}, "set":[{"column":"amount", "action":action, "value":"$value"}]}]}]
        }), &["ethereum", "other"],
    ).await
}

async fn rollback(fixture: &Fixture) -> anyhow::Result<()> {
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let mut derived = HashMap::new();
    let mut plans = HashMap::new();
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
    let provider = fixture.params.providers.get("ethereum").cloned().unwrap_or_else(|| {
        Arc::new(MockChainProvider::new(1).with_blocks(vec![
            crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(101), 1101),
        ]))
    });
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".into(),
        BlockChainWindow::try_new(8).unwrap(),
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        provider,
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

#[tokio::test]
async fn postgres_cross_chain_insert_rollback_preserves_other_network_rows() {
    let fixture = fixture("insert", "set", false).await;
    fixture.params.tables[0].table.validate_reorg_operations().unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (network, block, value) in [("ethereum", 99, 10), ("ethereum", 100, 20), ("other", 150, 30)]
    {
        callback(vec![at(&fixture, network, block, B256::repeat_byte(block as u8), value)])
            .await
            .unwrap();
    }
    rollback(&fixture).await.unwrap();
    let rows = fixture
        .client
        .query(
            &format!(
                "SELECT amount::text FROM {} ORDER BY amount",
                fixture.params.tables[0].full_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>(),
        ["10", "30"],
        "A's fork must not delete B's append-only row at an unrelated higher height"
    );
    let surviving = fixture.client.query_one(&format!("SELECT (SELECT count(*) FROM {} WHERE network = 'other'), (SELECT count(*) FROM rindexer_internal.event_applications WHERE network = 'other')",fixture.params.sql_event_table_name),&[]).await.unwrap();
    assert_eq!((surviving.get::<_, i64>(0), surviving.get::<_, i64>(1)), (1, 1));
    drop(callback);
    fixture.finish().await;
}

// Deferred migration: these passing assertions pin the manual rebuild requirement.
// Replace this group with migration-success coverage when source identities can be recovered.
mod known_risks {
    use super::*;

    #[tokio::test]
    async fn known_risk_cross_chain_insert_legacy_schema_requires_manual_rebuild() {
        for nullable in [false, true] {
            let fixture = fixture("insert", "set", false).await;
            let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
            callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), 10)]).await.unwrap();
            let table = &fixture.params.tables[0].full_table_name;
            fixture
                .client
                .batch_execute(&format!("ALTER TABLE {table} DROP COLUMN IF EXISTS network"))
                .await
                .unwrap();
            if nullable {
                fixture
                    .client
                    .batch_execute(&format!("ALTER TABLE {table} ADD COLUMN network VARCHAR(50)"))
                    .await
                    .unwrap();
            }
            let ddl =
                generate_tables_for_indexer_sql(fixture._directory.path(), &fixture.indexer, false)
                    .unwrap();
            let error = fixture
                .client
                .batch_execute(ddl.as_str())
                .await
                .expect_err("legacy rows without source identity must stop startup")
                .to_string();
            assert!(
                error.contains("cross-chain")
                    && error.contains("network")
                    && error.contains("rebuild"),
                "{error}"
            );
            assert_eq!(
                fixture.position().await.0,
                1,
                "schema rejection must preserve existing history"
            );
            drop(callback);
            fixture.finish().await;
        }
    }
}
