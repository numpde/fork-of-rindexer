//! Cross-chain sharing must retain each source network's independent history.
use std::collections::HashMap;

use alloy::primitives::U256;

use super::journal_recovery_tests::at;
use super::*;
use crate::{
    indexer::reorg::{
        BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    },
    manifest::contract::ColumnType,
    provider::{mock::MockChainProvider, ChainProvider},
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
    let mut registry = super::super::permanent_failure_tests::registry(
        &fixture.params,
        Arc::new(|_| Box::pin(async { Ok(()) })),
    );
    registry.events[0].providers = fixture.params.providers.clone();
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: Some(&registry),
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
    async fn known_risk_cross_chain_legacy_schema_requires_manual_rebuild() {
        for (operation, nullable) in
            [("insert", false), ("insert", true), ("upsert", false), ("upsert", true)]
        {
            let fixture = fixture(operation, "set", false).await;
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

async fn shared_state(fixture: &Fixture) -> (String, serde_json::Value) {
    let row = fixture
        .client
        .query_one(
            &format!(
                "SELECT amount::text, (to_jsonb(t) - 'amount') || jsonb_build_object('rindexer_sequence_id',rindexer_sequence_id::text) FROM {} t",
                fixture.params.tables[0].full_table_name
            ),
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

async fn equal_position_case(other_first: bool) {
    let fixture = fixture("upsert", "set", false).await;
    fixture.params.tables[0].table.validate_reorg_operations().unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut updates = [("ethereum", 10), ("other", 20)];
    if other_first {
        updates.reverse();
    }
    for (network, value) in updates {
        callback(vec![at(&fixture, network, 99, B256::repeat_byte(value as u8), value)])
            .await
            .unwrap();
    }
    let before = shared_state(&fixture).await;
    rollback(&fixture).await.unwrap();
    assert_eq!(
        shared_state(&fixture).await,
        before,
        "an unrelated later reorg must not change the equal-position winner or its provenance"
    );
    assert_eq!(
        before.0, "20",
        "equal positions use the bytewise greatest configured network name in either arrival order"
    );
    assert_eq!(before.1["network"], "other");
    callback(vec![
        at(&fixture, "ethereum", 99, B256::repeat_byte(10), 10),
        at(&fixture, "other", 99, B256::repeat_byte(20), 20),
    ])
    .await
    .unwrap();
    assert_eq!(
        shared_state(&fixture).await,
        before,
        "receipt replay must preserve the deterministic winner"
    );
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_cross_chain_equal_position_winner_survives_recovery() {
    equal_position_case(false).await;
}

#[tokio::test]
async fn postgres_cross_chain_equal_position_winner_is_independent_of_arrival_order() {
    equal_position_case(true).await;
}

fn source_header(number: u64, branch: u8, timestamp: u64) -> alloy::network::AnyRpcBlock {
    let mut header =
        crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(branch), timestamp);
    header.header.inner.number = number;
    header
}

async fn timestamp_fixture(other_provider: Option<u8>) -> Fixture {
    let mut fixture = Fixture::with_table_on_networks(
        serde_json::to_value(params().event).unwrap(), "Contract",
        json!({"name":"shared", "cross_chain":true, "timestamp":true,
            "columns":[{"name":"id", "type":"address"}, {"name":"amount", "type":"uint256", "default":"0"}],
            "events":[{"event":"Value", "operations":[{"type":"upsert", "where":{"id":"$rindexer_contract_address"}, "set":[{"column":"amount", "action":"add", "value":"$value"}]}]}]
        }), &["ethereum", "other"],
    ).await;
    let own: Arc<dyn ChainProvider> = Arc::new(
        MockChainProvider::new(1)
            .with_blocks(vec![source_header(99, 11, 1099), source_header(100, 101, 1101)]),
    );
    let mut providers = HashMap::from([("ethereum".to_string(), own)]);
    if let Some(branch) = other_provider {
        providers.insert(
            "other".to_string(),
            Arc::new(MockChainProvider::new(2).with_blocks(vec![source_header(99, branch, 2099)])),
        );
    }
    fixture.params.providers = Arc::new(providers);
    fixture
}

fn timestamped_input(
    fixture: &Fixture,
    network: &str,
    block: u64,
    branch: u8,
    value: u64,
    timestamp: u64,
) -> EventResult {
    let mut result = at(fixture, network, block, B256::repeat_byte(branch), value);
    result.tx_information.block_timestamp = Some(U256::from(timestamp));
    result.log.block_timestamp = Some(timestamp);
    if network == "other" {
        result.tx_information.chain_id = 2;
    }
    result
}

async fn remove_other_raw_timestamp(fixture: &Fixture) {
    fixture
        .client
        .batch_execute(&format!(
            "UPDATE {} SET block_timestamp = NULL WHERE network = 'other'",
            fixture.params.sql_event_table_name
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_cross_chain_timestamp_recovery_uses_the_contributors_network() {
    let fixture = timestamp_fixture(Some(22)).await;
    fixture.params.tables[0].table.validate_reorg_operations().unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(vec![timestamped_input(&fixture, "other", 99, 22, 10, 2099)]).await.unwrap();
    let canonical = shared_state(&fixture).await;
    remove_other_raw_timestamp(&fixture).await;
    callback(vec![timestamped_input(&fixture, "ethereum", 100, 100, 20, 1100)]).await.unwrap();
    rollback(&fixture).await.expect(
        "B's canonical contributor requires B's provider, not A's header at the same height",
    );
    assert_eq!(
        shared_state(&fixture).await,
        canonical,
        "value and all source provenance must return to B's canonical row"
    );
    assert_eq!(fixture.position().await.0, 1);
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_cross_chain_timestamp_recovery_rejects_missing_or_wrong_source_provider() {
    for provider in [None, Some(33)] {
        let fixture = timestamp_fixture(provider).await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(vec![timestamped_input(&fixture, "other", 99, 22, 10, 2099)]).await.unwrap();
        remove_other_raw_timestamp(&fixture).await;
        callback(vec![timestamped_input(&fixture, "ethereum", 100, 100, 20, 1100)]).await.unwrap();
        let before = shared_state(&fixture).await;
        let error = rollback(&fixture)
            .await
            .expect_err("missing or inconsistent source identity cannot certify rollback");
        let detail = format!("{error:#}");
        assert!(detail.contains("other") && detail.contains("timestamp"), "{detail}");
        if provider.is_none() {
            assert!(detail.contains("provider"), "{detail}");
        }
        assert_eq!(
            shared_state(&fixture).await,
            before,
            "failed recovery must leave effects and provenance uncommitted"
        );
        assert_eq!(fixture.position().await.0, 2);
        let receipts: i64 = fixture
            .client
            .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(receipts, 2);
        drop(callback);
        fixture.finish().await;
    }
}
