use super::*;
use crate::{
    indexer::tip_logs::{BlockLogsSource, FallbackReason, SharedTipLogsSettings},
    provider::mock::MockChainProvider,
};
use alloy::{
    network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
    primitives::{Bloom, Log as PrimitiveLog, LogData},
    rpc::types::{Block, BlockTransactions},
};

fn hash(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}
fn log(number: u64, block_hash: Option<B256>) -> Log {
    Log {
        inner: PrimitiveLog {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![B256::ZERO], Default::default()),
        },
        block_number: Some(number),
        block_hash,
        ..Default::default()
    }
}
fn header(number: u64, block_hash: B256, parent_hash: B256) -> AnyRpcBlock {
    AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader { number, parent_hash, ..Default::default() }.seal(block_hash),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    )
}
fn filter(from: u64) -> RindexerEventFilter {
    RindexerEventFilter::empty_for_test()
        .set_from_block(U64::from(from))
        .set_to_block(U64::from(10))
}
#[derive(Debug)]
struct Answer(Vec<Log>);
#[async_trait::async_trait]
impl BlockLogsSource for Answer {
    async fn block_logs(&self, _: u64) -> Result<Vec<Log>, ProviderError> {
        Ok(self.0.clone())
    }
}
async fn settled_tip(answer: Log) -> Arc<SharedTipLogs> {
    let tip = SharedTipLogs::new(
        "identity-test",
        Arc::new(Answer(vec![answer])),
        SharedTipLogsSettings {
            empty_retry_deadline_ms: 7000,
            cache_blocks: 8,
            bloom_trusted: false,
        },
    );
    tip.observe_head(10, hash(10), hash(9), Bloom::ZERO);
    loop {
        let notified = tip.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !matches!(tip.lookup(10, 10, hash(10)), TipLookup::Wait { .. }) {
            break;
        }
        notified.await;
    }
    tip
}
async fn fallback_result(
    tip: &SharedTipLogs,
    provider: &MockChainProvider,
    from: u64,
) -> Result<Vec<Log>, ProviderError> {
    match fetch_tip_window(tip, provider, &filter(from), &None, hash(10), "identity-test").await {
        TipWindowFetch::Fetched(result) => result,
        TipWindowFetch::Wait => panic!("fixture cache already settled"),
    }
}

#[tokio::test(start_paused = true)]
async fn tip_fallback_rejects_the_orphaned_hash_that_failed_the_cache() {
    let stale = log(10, Some(hash(99)));
    let tip = settled_tip(stale.clone()).await;
    assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::Fallback(FallbackReason::Error));
    let provider = MockChainProvider::new(1).with_logs(vec![stale]).with_blocks(vec![header(
        10,
        hash(10),
        hash(9),
    )]);
    assert!(
        fallback_result(&tip, &provider, 10).await.is_err(),
        "height fallback must not restore a log rejected by the hash-qualified cache"
    );
}

#[tokio::test(start_paused = true)]
async fn tip_fallback_requires_complete_log_block_identity() {
    let tip = settled_tip(log(10, Some(hash(99)))).await;
    for mut missing in [log(10, None), log(10, Some(hash(10)))] {
        if missing.block_hash.is_some() {
            missing.block_number = None;
        }
        let provider = MockChainProvider::new(1)
            .with_logs(vec![missing])
            .with_blocks(vec![header(10, hash(10), hash(9))]);
        assert!(
            fallback_result(&tip, &provider, 10).await.is_err(),
            "fallback cannot certify missing block metadata"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn tip_fallback_accepts_canonical_filtered_logs_after_cache_failure() {
    let tip = settled_tip(log(10, Some(hash(99)))).await;
    let provider = MockChainProvider::new(1)
        .with_logs(vec![log(10, Some(hash(10)))])
        .with_blocks(vec![header(10, hash(10), hash(9))]);
    assert_eq!(fallback_result(&tip, &provider, 10).await.unwrap()[0].block_hash, Some(hash(10)));
}

#[tokio::test(start_paused = true)]
async fn tip_prefix_fallback_rejects_orphaned_prefix_logs() {
    let tip = settled_tip(log(10, Some(hash(10)))).await;
    let provider = MockChainProvider::new(1)
        .with_logs(vec![log(9, Some(hash(99)))])
        .with_blocks(vec![header(9, hash(9), hash(8)), header(10, hash(10), hash(9))]);
    assert!(
        fallback_result(&tip, &provider, 9).await.is_err(),
        "a valid cached tip cannot certify an orphaned RPC prefix"
    );
}

#[tokio::test(start_paused = true)]
async fn tip_prefix_fallback_requires_connected_header_proof_to_the_observed_tip() {
    let tip = settled_tip(log(10, Some(hash(10)))).await;
    let provider = MockChainProvider::new(1)
        .with_logs(vec![log(9, Some(hash(99)))])
        .with_blocks(vec![header(9, hash(99), hash(8)), header(10, hash(10), hash(9))]);
    assert!(fallback_result(&tip, &provider, 9).await.is_err(), "matching a separate prefix header is insufficient when it cannot connect to the observed tip");
}

#[tokio::test(start_paused = true)]
async fn tip_prefix_fallback_accepts_a_connected_canonical_prefix() {
    let tip = settled_tip(log(10, Some(hash(10)))).await;
    let provider = MockChainProvider::new(1)
        .with_logs(vec![log(9, Some(hash(9)))])
        .with_blocks(vec![header(9, hash(9), hash(8)), header(10, hash(10), hash(9))]);
    let result = fallback_result(&tip, &provider, 9).await.unwrap();
    assert_eq!(
        result.iter().map(|log| log.block_number).collect::<Vec<_>>(),
        vec![Some(9), Some(10)]
    );
}

async fn live_result(answer: Log) -> Option<Result<FetchLogsResult, Box<dyn Error + Send>>> {
    let provider = Arc::new(
        MockChainProvider::new(1)
            .with_blocks(vec![header(10, hash(10), hash(9))])
            .with_logs(vec![answer]),
    );
    let (tx, mut rx) = mpsc::channel(4);
    let cancel = CancellationToken::new();
    let registry = EventCallbackRegistry::default();
    let producer = live_indexing_stream(
        crate::indexer::reorg::subscribe_reorgs("direct-identity-test"),
        false,
        BlockClock::new(None, None, provider.clone()),
        provider.clone(),
        &tx,
        U64::from(9),
        &B256::ZERO,
        &U64::ZERO,
        filter(10),
        "direct-identity-test",
        "direct-identity-test",
        true,
        None,
        cancel.clone(),
        None,
        None,
        None,
        &registry,
        None,
    );
    tokio::pin!(producer);
    let received = tokio::select! {
        _ = &mut producer => panic!("live fixture ended before cancellation"),
        result = rx.recv() => result,
        _ = tokio::time::sleep(Duration::from_millis(150)) => None,
    };
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), producer)
        .await
        .expect("cooperative live fixture completion");
    assert!(provider.log_fetches() > 0);
    received
}

#[tokio::test(start_paused = true)]
async fn direct_live_height_fetch_also_rejects_inconsistent_tip_identity() {
    for answer in [log(10, Some(hash(99))), log(10, None)] {
        assert!(
            live_result(answer).await.is_none(),
            "disabling shared tip fetching must not permit orphaned logs or checkpoint batches"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn direct_live_height_fetch_accepts_observed_tip_identity() {
    let result = live_result(log(10, Some(hash(10)))).await.unwrap().unwrap();
    assert_eq!(result.logs[0].block_hash, Some(hash(10)));
}

#[tokio::test]
async fn tip_fallback_old_logs_require_a_continuous_proof_across_header_batches() {
    let number_hash = |number| B256::from(alloy::primitives::U256::from(number));
    for corrupt_ancestor in [false, true] {
        let mut headers = (1..=300)
            .map(|number| header(number, number_hash(number), number_hash(number - 1)))
            .collect::<Vec<_>>();
        if corrupt_ancestor {
            headers[43].header.hash = number_hash(999);
        }
        let provider = MockChainProvider::new(1)
            .with_blocks(headers)
            .with_logs(vec![log(1, Some(number_hash(1)))]);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(1))
            .set_to_block(U64::from(300));
        let result =
            fetch_logs_against_observed_head(&provider, &filter, 300, number_hash(300)).await;
        assert_eq!(
            result.is_err(),
            corrupt_ancestor,
            "older logs need one connected ancestry proof, including across RPC batches"
        );
    }
}
