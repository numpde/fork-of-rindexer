//! Reject incomplete mined-log metadata before constructing callback inputs.
use std::{panic::AssertUnwindSafe, sync::Arc};

use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::{Log, ValueOrArray},
};

use super::{noop_decoder, EventResult};
use crate::{
    blockclock::BlockClock,
    event::contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    provider::JsonRpcCachedProvider,
};

fn contract() -> Arc<NetworkContract> {
    let provider = JsonRpcCachedProvider::mock(1);
    Arc::new(NetworkContract {
        id: "metadata".to_string(),
        network: "ethereum".to_string(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        block_clock: BlockClock::new(None, None, provider.clone()),
        cached_provider: provider,
        decoder: noop_decoder(),
        start_block: Some(U64::from(1)),
        end_block: Some(U64::from(2)),
        disable_logs_bloom_checks: false,
    })
}

fn complete_log() -> Log {
    Log {
        block_hash: Some(B256::repeat_byte(1)),
        block_number: Some(2),
        transaction_hash: Some(B256::repeat_byte(2)),
        transaction_index: Some(3),
        log_index: Some(4),
        ..Default::default()
    }
}

#[test]
fn missing_rpc_metadata_returns_an_error_without_panicking() {
    for field in
        ["block_hash", "block_number", "transaction_hash", "transaction_index", "log_index"]
    {
        let mut log = complete_log();
        match field {
            "block_hash" => log.block_hash = None,
            "block_number" => log.block_number = None,
            "transaction_hash" => log.transaction_hash = None,
            "transaction_index" => log.transaction_index = None,
            "log_index" => log.log_index = None,
            _ => unreachable!(),
        }
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            EventResult::try_new(contract(), log, U64::from(1), U64::from(2))
        }));
        let result =
            result.unwrap_or_else(|_| panic!("missing {field} must return an error, not panic"));
        let error = result.expect_err("incomplete metadata must not reach callback dispatch");
        assert!(error.contains(field), "identify the missing RPC field: {error}");
        assert!(error.contains("ethereum"), "identify the network: {error}");
    }
}

#[test]
fn complete_metadata_preserves_callback_fields_without_a_timestamp() {
    let log = complete_log();
    let result = EventResult::try_new(contract(), log.clone(), U64::from(1), U64::from(2))
        .expect("block timestamps remain optional");
    assert_eq!(result.tx_information.block_hash, log.block_hash.unwrap());
    assert_eq!(result.tx_information.block_number, 2);
    assert_eq!(result.tx_information.transaction_hash, log.transaction_hash.unwrap());
    assert_eq!(result.tx_information.transaction_index, 3);
    assert_eq!(result.tx_information.log_index, alloy::primitives::U256::from(4));
    assert!(result.tx_information.block_timestamp.is_none());
    assert_eq!(result.found_in_request.from_block, U64::from(1));
    assert_eq!(result.found_in_request.to_block, U64::from(2));
    // The existing public constructor remains source compatible.
    assert_eq!(
        EventResult::new(contract(), log, U64::from(1), U64::from(2)).tx_information.block_number,
        2
    );
}
