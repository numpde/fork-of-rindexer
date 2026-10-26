//! Explicit history expiry is terminal; an empty filtered response proves no such failure.
use super::*;
use crate::provider::mock::MockChainProvider;

fn filter(to: u64) -> RindexerEventFilter {
    RindexerEventFilter::empty_for_test().set_from_block(U64::from(100)).set_to_block(U64::from(to))
}

fn clock() -> BlockClock {
    BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)))
}

fn assert_diagnostic(error: &(dyn Error + Send), to: u64) {
    let message = error.to_string();
    for value in ["pruned-test", "100", &to.to_string(), "4444", "retaining", "restart"] {
        assert!(message.contains(value), "missing {value:?}: {message}");
    }
    assert!(error.source().is_some(), "retain the provider's structured cause");
}

#[tokio::test(start_paused = true)]
async fn pruned_history_sequential_fetch_is_terminal_for_large_and_single_block_ranges() {
    for to in [200, 100] {
        let provider =
            MockChainProvider::new(1).with_log_rpc_error(4444, "provider-specific detail");
        let (tx, mut rx) = mpsc::channel(4);
        let next = fetch_historic_logs_stream(
            false,
            clock(),
            &provider,
            &tx,
            &B256::ZERO,
            filter(to),
            None,
            U64::from(to),
            "pruned-test",
            0,
        )
        .await;
        assert!(next.is_none(), "history expiry must not become another range retry");
        let error = rx
            .try_recv()
            .expect("one terminal error must be delivered")
            .err()
            .expect("failed history cannot certify checkpoint work");
        assert_diagnostic(error.as_ref(), to);
        assert!(rx.try_recv().is_err());
        assert_eq!(provider.log_fetches(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn pruned_history_parallel_fetch_is_terminal_even_with_a_retry_hint() {
    for to in [200, 100] {
        let provider = MockChainProvider::new(1).with_log_rpc_error(4444, "block range too large");
        let (batch, next, _) = fetch_logs_once(
            false,
            &clock(),
            &provider,
            filter(to),
            None,
            U64::from(to),
            "pruned-test",
            0,
        )
        .await;
        assert!(next.is_none(), "a range hint cannot override explicit history expiry");
        let error = batch.expect("return a terminal result").err().expect("not a completed range");
        assert_diagnostic(error.as_ref(), to);
        assert_eq!(provider.log_fetches(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn pruned_history_live_catchup_delivers_an_error_and_stops_without_cancellation() {
    let block = alloy::network::AnyRpcBlock::new(
        alloy::rpc::types::Block::new(
            alloy::network::AnyRpcHeader::from_sealed(
                alloy::network::AnyHeader { number: 200, ..Default::default() }.seal(B256::ZERO),
            ),
            alloy::rpc::types::BlockTransactions::Full(vec![]),
        )
        .into(),
    );
    let provider = Arc::new(
        MockChainProvider::new(1)
            .with_blocks(vec![block])
            .with_log_rpc_error(4444, "provider-specific detail"),
    );
    let (tx, mut rx) = mpsc::channel(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        live_indexing_stream(
            crate::indexer::reorg::subscribe_reorgs("pruned-test"),
            false,
            clock(),
            provider.clone(),
            &tx,
            U64::from(99),
            &B256::ZERO,
            &U64::ZERO,
            filter(200),
            "pruned-test",
            "pruned-test",
            true,
            None,
            cancel.clone(),
            None,
            None,
            None,
            &EventCallbackRegistry::default(),
            None,
        ),
    )
    .await
    .expect("explicit pruning must stop live catch-up without a manual abort");
    let error = rx
        .try_recv()
        .expect("terminal error reaches the consumer")
        .err()
        .expect("no checkpoint batch");
    assert_diagnostic(error.as_ref(), 200);
    assert!(!cancel.is_cancelled(), "failure must not masquerade as a requested stop");
    assert!(rx.try_recv().is_err());
    assert_eq!(provider.log_fetches(), 1);
}

#[tokio::test(start_paused = true)]
async fn pruned_history_policy_preserves_empty_ranges_and_other_rpc_error_retries() {
    let provider = MockChainProvider::new(1);
    let (batch, _, _) = fetch_logs_once(
        false,
        &clock(),
        &provider,
        filter(200),
        None,
        U64::from(200),
        "pruned-test",
        0,
    )
    .await;
    assert!(batch.unwrap().unwrap().logs.is_empty(), "empty filtered history is legitimate");
    let provider = MockChainProvider::new(1).with_log_rpc_error(-32000, "header not found");
    let (batch, next, _) = fetch_logs_once(
        false,
        &clock(),
        &provider,
        filter(200),
        None,
        U64::from(200),
        "pruned-test",
        0,
    )
    .await;
    assert!(batch.is_none());
    assert_eq!(next.unwrap().next.from_block(), U64::from(100));
}
