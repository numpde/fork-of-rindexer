//! A phase boundary must not discard the identity of the historical tip it applied.
use super::*;
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{noop_decoder, TraceCallbackRegistry},
        contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    },
    indexer::{
        tables::view_call_cache_tests::{header, ORIGINAL, REPLACEMENT},
        IndexingEventsProgressState,
    },
    provider::mock::MockChainProvider,
};
use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};

async fn phase(fixture: &Fixture, replacement: bool, historical: bool) {
    let mut log = fixture.events(&[if replacement { 20 } else { 10 }]).remove(0).log;
    log.block_number = Some(100);
    log.block_hash = Some(if replacement { REPLACEMENT } else { ORIGINAL });
    // Retain the unchanged parent so recovery can prove the first divergent block.
    let mut ancestor = header(B256::repeat_byte(99), 9);
    ancestor.header.inner.number = 99;
    ancestor.header.inner.parent_hash = B256::repeat_byte(98);
    let mut blocks = vec![ancestor, header(if replacement { REPLACEMENT } else { ORIGINAL }, 10)];
    if replacement {
        let mut next = header(B256::repeat_byte(101), 11);
        next.header.inner.number = 101;
        next.header.inner.parent_hash = REPLACEMENT;
        blocks.push(next);
    }
    let provider = Arc::new(
        MockChainProvider::new(1)
            .with_block_number(if replacement { 101 } else { 100 })
            .with_blocks(blocks)
            .with_logs(vec![log]),
    );
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    let id = registry.events[0].id.clone();
    registry.events[0].contract.details.push(NetworkContract {
        id,
        network: "ethereum".to_string(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: noop_decoder(),
        start_block: Some(U64::from(99)),
        end_block: Some(U64::from(if replacement { 101 } else { 100 })),
        disable_logs_bloom_checks: true,
    });
    let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid","reorg_handling":{"enabled":true,"window_size":8}}]})).unwrap();
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let (handles, _, _, _, coordinators) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &fixture.indexer,
        registry,
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        historical,
        Default::default(),
        progress,
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    })
    .await
    .expect("finite production indexing phase finishes");
    drop(coordinators);
}

#[tokio::test]
async fn postgres_historical_live_handoff_replaces_the_committed_orphan_and_survives_restart() {
    let mut fixture = Fixture::new().await;
    fixture.params.tables = Arc::new(vec![]);
    fixture.indexer.contracts[0].tables = None;
    phase(&fixture, false, true).await;
    let query = format!("SELECT block_hash FROM {}", fixture.params.sql_event_table_name);
    assert_eq!(
        fixture.client.query_one(&query, &[]).await.unwrap().get::<_, String>(0),
        format!("{ORIGINAL:#x}")
    );
    // The historical phase has fully committed before the provider switches chains.
    phase(&fixture, true, false).await;
    let rows = fixture.client.query(&query, &[]).await.unwrap();
    assert_eq!(rows.len(), 1, "orphan and replacement cannot coexist");
    assert_eq!(rows[0].get::<_, String>(0), format!("{REPLACEMENT:#x}"));
    let receipts: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.event_applications WHERE block_hash = $1",
            &[&REPLACEMENT.as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
    let orphan: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.event_applications WHERE block_hash = $1",
            &[&ORIGINAL.as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(orphan, 0);
    phase(&fixture, true, false).await;
    assert_eq!(
        fixture.client.query(&query, &[]).await.unwrap().len(),
        1,
        "restart remains replay-safe"
    );
    fixture.finish().await;
}

async fn historical_identity_case(parallel: bool, stale: bool) {
    let fixture = Fixture::new().await;
    let hash = |number: u64| B256::from(alloy::primitives::U256::from(number).to_be_bytes::<32>());
    let mut log = fixture.events(&[10]).remove(0).log;
    log.block_number = Some(100);
    log.block_hash = Some(if stale { REPLACEMENT } else { hash(100) });
    let blocks = (99..=2099)
        .map(|number| {
            let mut block = header(hash(number), number);
            block.header.inner.number = number;
            block.header.inner.parent_hash = hash(number - 1);
            block
        })
        .collect();
    let provider = Arc::new(
        MockChainProvider::new(1).with_block_number(2099).with_blocks(blocks).with_logs(vec![log]),
    );
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    registry.events[0].contract.details.push(NetworkContract {
        id: "historical-identity".into(),
        network: "ethereum".into(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: noop_decoder(),
        start_block: Some(U64::from(100)),
        end_block: Some(U64::from(2099)),
        disable_logs_bloom_checks: true,
    });
    let manifest =
        serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],
        "config":{"fetch_concurrency":if parallel {2} else {1}},
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}]}))
        .unwrap();
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let (handles, _, _, _, _) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &fixture.indexer,
        registry,
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        true,
        cancel.clone(),
        progress,
    )
    .await
    .unwrap();
    let finished = async move {
        for handle in handles {
            let _ = handle.await.unwrap();
        }
    };
    tokio::pin!(finished);
    if stale {
        tokio::select! {
            _ = &mut finished => {},
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                cancel.cancel();
                tokio::time::timeout(Duration::from_secs(10), &mut finished).await.expect("normal cancellation drains rejected historical work");
            }
        }
    } else {
        tokio::time::timeout(Duration::from_secs(10), finished)
            .await
            .expect("matching historical branch completes");
    }
    let actual = fixture.position().await;
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    fixture.finish().await;
    if stale {
        assert_eq!(
            actual,
            (0, "0".into(), "0".into()),
            "historical fork mismatch cannot certify effects or progress"
        );
        assert_eq!(receipts, 0);
    } else {
        assert_eq!(actual, (1, "10".into(), "2099".into()));
        assert_eq!(receipts, 1);
    }
}

#[tokio::test]
async fn postgres_historical_identity_sequential_rejects_stale_fork() {
    historical_identity_case(false, true).await;
}
#[tokio::test]
async fn postgres_historical_identity_parallel_rejects_stale_fork() {
    historical_identity_case(true, true).await;
}
#[tokio::test]
async fn postgres_historical_identity_matching_branch_is_applied() {
    historical_identity_case(false, false).await;
    historical_identity_case(true, false).await;
}
