//! A changed tip hash is a fork even when its height and parent stay unchanged.
use std::sync::Arc;

use alloy::{
    network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
    primitives::B256,
    rpc::types::{Block, BlockTransactions},
};

use super::{
    detect_and_handle_reorg, subscribe_reorgs, BlockChainWindow, ReorgBlockHashPersistence,
    ReorgContext, ReorgCoordinator,
};
use crate::provider::mock::MockChainProvider;

fn block(number: u64, hash: B256, parent_hash: B256) -> AnyRpcBlock {
    AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader { number, parent_hash, ..Default::default() }.seal(hash),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    )
}

#[tokio::test]
async fn changed_tip_hash_with_same_parent_rolls_back_and_rewinds_every_stream_once() {
    let network = "same-height-fork";
    let ancestor = B256::repeat_byte(99);
    let old_tip = B256::repeat_byte(100);
    let replacement = B256::repeat_byte(200);
    let mut window = BlockChainWindow::try_new(20).unwrap();
    window.insert(99, ancestor, B256::repeat_byte(98));
    window.insert(100, old_tip, ancestor);
    let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![
        block(99, ancestor, B256::repeat_byte(98)),
        block(100, replacement, ancestor),
    ]));
    let mut coordinator = ReorgCoordinator::new(
        network.to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(None, None)),
        provider,
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let stream_a = subscribe_reorgs(network);
    let stream_b = subscribe_reorgs(network);
    let ctx =
        ReorgContext { postgres: None, clickhouse: None, registry: None, trace_registry: None };

    let fork = detect_and_handle_reorg(&mut coordinator, 100, replacement, ancestor, "test", &ctx)
        .await
        .unwrap();
    assert_eq!(
        fork,
        Some(100),
        "same parent does not establish that the block itself is unchanged"
    );
    assert_eq!(stream_a.take_rewind(), Some(100));
    assert_eq!(stream_b.take_rewind(), Some(100));
    let repeated =
        detect_and_handle_reorg(&mut coordinator, 100, replacement, ancestor, "test", &ctx)
            .await
            .unwrap();
    assert_eq!(
        repeated, None,
        "the next stream observes the accepted replacement without repeating rollback"
    );
    assert_eq!(stream_a.take_rewind(), None);
    assert_eq!(stream_b.take_rewind(), None);
}

#[tokio::test]
async fn changed_retained_hash_invalidates_through_the_last_observed_head() {
    let network = "retained-height-fork";
    let hash = |number| B256::repeat_byte(number);
    let mut window = BlockChainWindow::try_new(20).unwrap();
    for number in 98..=100 {
        window.insert(u64::from(number), hash(number), hash(number - 1));
    }
    let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![
        block(98, hash(98), hash(97)),
        block(99, hash(199), hash(98)),
        block(100, hash(200), hash(199)),
    ]));
    let clients = Arc::new(Some(crate::streams::StreamsClients::empty_for_test()));
    clients
        .as_ref()
        .as_ref()
        .unwrap()
        .test_seed_finalized_buffer(network, "Value", &[99, 100], 10)
        .await;
    let mut coordinator = ReorgCoordinator::new(
        network.to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(None, None)),
        provider,
        vec![],
        vec![],
        vec![clients.clone()],
    )
    .unwrap();
    let task = coordinator
        .on_new_block(99, hash(199), hash(98))
        .await
        .unwrap()
        .expect("retained block changed");
    assert_eq!(task.fork_point, 99);
    assert_eq!(
        task.detection_point, 100,
        "a lower observation must invalidate later retained blocks too"
    );
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: None,
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        )
        .await
        .unwrap();
    assert!(
        clients.as_ref().as_ref().unwrap().test_buffered_blocks(network).await.is_empty(),
        "later orphaned blocks must not remain eligible for finalized delivery"
    );
}
