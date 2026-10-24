//! A malformed response must not become a successful batch or move the fetch range forward.
use super::{fetch_historic_logs_stream, fetch_logs_once};
use crate::{
    blockclock::BlockClock, event::RindexerEventFilter, provider::mock::MockChainProvider,
};
use alloy::{
    primitives::{B256, U64},
    rpc::types::Log,
};
use futures::FutureExt;
use std::{panic::AssertUnwindSafe, sync::Arc};
use tokio::sync::mpsc;

#[tokio::test]
async fn parallel_fetch_retries_missing_block_number_without_a_successful_batch() {
    let provider = MockChainProvider::new(1).with_logs(vec![Log::default()]);
    let clock = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
    let filter = RindexerEventFilter::empty_for_test()
        .set_from_block(U64::from(1))
        .set_to_block(U64::from(2));
    let outcome = AssertUnwindSafe(fetch_logs_once(
        false,
        &clock,
        &provider,
        filter,
        None,
        U64::from(2),
        "test",
        0,
    ))
    .catch_unwind()
    .await
    .expect("missing RPC block_number must not panic");
    assert!(outcome.0.is_none(), "malformed logs must not be delivered as a successful batch");
    assert_eq!(outcome.1.expect("retry state").next.from_block(), U64::from(1));
    assert!(outcome.2.is_some(), "report the failed fetch to the retry controller");
}

#[tokio::test]
async fn serial_fetch_retries_missing_block_number_without_a_successful_batch() {
    let provider = MockChainProvider::new(1).with_logs(vec![Log::default()]);
    let clock = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
    let filter = RindexerEventFilter::empty_for_test()
        .set_from_block(U64::from(1))
        .set_to_block(U64::from(2));
    let (sender, mut receiver) = mpsc::channel(4);
    let outcome = AssertUnwindSafe(fetch_historic_logs_stream(
        false,
        clock,
        &provider,
        &sender,
        &B256::ZERO,
        filter,
        None,
        U64::from(2),
        "test",
        0,
    ))
    .catch_unwind()
    .await
    .expect("missing RPC block_number must not panic");
    assert_eq!(outcome.expect("retry state").next.from_block(), U64::from(1));
    assert!(
        receiver.try_recv().is_err(),
        "malformed logs must not be queued for callback dispatch"
    );
}
