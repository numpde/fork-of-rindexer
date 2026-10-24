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
