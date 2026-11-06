//! Exercise both subscriptions through historical indexing and PostgreSQL startup recovery.
use super::*;
use crate::{
    event::callback_registry::{EventCallbackRegistry, TraceCallbackRegistry},
    indexer::{
        no_code::process_events_prepared, start::start_indexing_contract_events_prepared,
        tables::view_call_cache_tests::header, IndexingEventsProgressState,
    },
    manifest::core::Manifest,
    provider::{mock::MockChainProvider, CreateNetworkProvider},
    PreparedProject,
};
use tokio_util::sync::CancellationToken;

async fn project() -> (Fixture, PreparedProject) {
    let details = json!([{
        "network":"ethereum", "address":"0x0000000000000000000000000000000000000000",
        "start_block":"99", "end_block":"100"
    }]);
    let manifest: Manifest = serde_json::from_value(json!({
        "name":"Replay","project_type":"no-code","storage":{"postgres":{"enabled":true}},
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid",
            "reorg_handling":{"enabled":true,"window_size":8}}],
        "contracts":[
            {"name":"Stored","abi":"abi.json","include_events":["Value"],"details":details,
             "tables":[Fixture::totals_table("Value")]},
            {"name":"Observed","abi":"abi.json","details":details}
        ]
    }))
    .unwrap();
    let fixture = Fixture::with_manifest(
        serde_json::to_value(params().event).unwrap(),
        "Stored",
        manifest.clone(),
    )
    .await;
    let prepared = PreparedProject::try_new(fixture._directory.path(), manifest).unwrap();
    (fixture, prepared)
}

// Change only the transport between restarts. A cancelled generation performs startup
// recovery but cannot index the replacement until the next phase.
async fn phase(
    fixture: &Fixture,
    project: &PreparedProject,
    last: u8,
    value: u64,
    recover_only: bool,
) -> anyhow::Result<()> {
    let blocks = [(99, 99, 98), (100, last, 99)]
        .into_iter()
        .map(|(height, hash, parent)| {
            let mut block = header(B256::repeat_byte(hash), height);
            block.header.inner.number = height;
            block.header.inner.parent_hash = B256::repeat_byte(parent);
            block
        })
        .collect();
    let logs = [(99, 99, 10), (100, last, value)]
        .into_iter()
        .map(|(height, hash, amount)| {
            super::journal_recovery_tests::at(
                fixture,
                "ethereum",
                height,
                B256::repeat_byte(hash),
                amount,
            )
            .log
        })
        .collect();
    let providers = [CreateNetworkProvider {
        network_name: "ethereum".into(),
        disable_logs_bloom_checks: true,
        client: Arc::new(
            MockChainProvider::new(1).with_block_number(100).with_blocks(blocks).with_logs(logs),
        ),
    }];
    let mut registry = EventCallbackRegistry::new();
    for event in process_events_prepared(
        fixture._directory.path(),
        project.manifest(),
        project.schema(),
        Some(fixture.client.clone()),
        None,
        &providers,
    )
    .await?
    {
        registry.register_event(event);
    }
    assert_eq!(registry.events.len(), 2, "both distinct subscriptions must be registered");
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let cancel = CancellationToken::new();
    if recover_only {
        cancel.cancel();
    }
    let (handles, _, _, _, _) = start_indexing_contract_events_prepared(
        project.manifest(),
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        project.schema(),
        registry,
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        true,
        cancel,
        progress,
    )
    .await?;
    for handle in handles {
        if recover_only {
            handle.abort();
            let _ = handle.await;
        } else {
            tokio::time::timeout(Duration::from_secs(10), handle).await???;
        }
    }
    Ok(())
}

async fn assert_position(fixture: &Fixture, rows: i64, total: i64, block: i64) {
    let row = fixture
        .client
        .query_one(
            "SELECT (SELECT count(*) FROM replay_stored.value),
                (SELECT amount::bigint FROM replay_stored.totals),
                (SELECT count(*) FROM rindexer_internal.event_applications),
                (SELECT last_synced_block::bigint FROM rindexer_internal.replay_stored_value
                 WHERE network='ethereum' AND cursor_key <> '__event__'),
                (SELECT last_synced_block::bigint FROM rindexer_internal.replay_observed_value
                 WHERE network='ethereum' AND cursor_key <> '__event__'),
                to_regclass('replay_observed.value') IS NULL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        (row.get::<_, i64>(0), row.get::<_, i64>(1), row.get::<_, i64>(2)),
        (rows, total, rows)
    );
    assert_eq!((row.get::<_, i64>(3), row.get::<_, i64>(4)), (block, block));
    assert!(row.get::<_, bool>(5), "checkpoint-only indexing must not create a raw table");
}

#[tokio::test]
async fn postgres_rewinds_stored_and_checkpoint_only_subscriptions_together() {
    let (fixture, prepared) = project().await;
    phase(&fixture, &prepared, 100, 20, false).await.unwrap();
    assert_position(&fixture, 2, 30, 100).await;

    phase(&fixture, &prepared, 101, 7, true)
        .await
        .expect("rewind both cursors; delete only stored history");
    assert_position(&fixture, 1, 10, 99).await;

    phase(&fixture, &prepared, 101, 7, false).await.unwrap();
    assert_position(&fixture, 2, 17, 100).await;
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_missing_required_raw_table_stops_rewind_before_effects() {
    let (fixture, prepared) = project().await;
    phase(&fixture, &prepared, 100, 20, false).await.unwrap();
    fixture
        .client
        .batch_execute("ALTER TABLE replay_stored.value RENAME TO held_for_check")
        .await
        .unwrap();
    let outcome = phase(&fixture, &prepared, 101, 7, true).await;
    fixture
        .client
        .batch_execute("ALTER TABLE replay_stored.held_for_check RENAME TO value")
        .await
        .unwrap();

    assert!(format!("{:#}", outcome.unwrap_err()).contains("replay_stored.value"));
    assert_position(&fixture, 2, 30, 100).await;
    fixture.finish().await;
}
