//! Fetching a manifest's alternatives must produce their union exactly once.
use crate::{
    event::{contract_setup::IndexingContractSetup, RindexerEventFilter},
    indexer::tip_logs::filter_logs_for_stream,
    manifest::contract::ContractDetails,
    provider::JsonRpcCachedProvider,
};
use alloy::{
    primitives::{Address, Bytes, Log as PrimitiveLog, LogData, B256, U64},
    rpc::types::Log,
};
use serde_json::json;
use std::{collections::HashSet, path::Path};

fn filter(address_scoped: bool) -> RindexerEventFilter {
    let mut detail = json!({"network":"ethereum","indexed_filters":[
        {"event_name":"Transfer","indexed_1":[format!("{:#x}",B256::repeat_byte(11))]},
        {"event_name":"Transfer","indexed_2":[format!("{:#x}",B256::repeat_byte(11))]}
    ]});
    if address_scoped {
        detail["address"] = json!(format!("{:#x}", Address::repeat_byte(7)));
    } else {
        detail["filter"] = json!({"event_name":"Transfer"});
    }
    let detail: ContractDetails = serde_json::from_value(detail).unwrap();
    match detail.indexing_contract_setup(Path::new(".")) {
        IndexingContractSetup::Address(details) => RindexerEventFilter::new_address_filter(
            &B256::repeat_byte(1),
            "Transfer",
            &details,
            U64::from(100),
            U64::from(100),
        ),
        IndexingContractSetup::Filter(details) => RindexerEventFilter::new_filter(
            &B256::repeat_byte(1),
            "Transfer",
            &details,
            U64::from(100),
            U64::from(100),
        ),
        _ => unreachable!("fixture declares address or filter mode"),
    }
    .unwrap()
}

fn log(index: u64, from: u8, to: u8) -> Log {
    Log {
        inner: PrimitiveLog {
            address: Address::repeat_byte(7),
            data: LogData::new_unchecked(
                vec![B256::repeat_byte(1), B256::repeat_byte(from), B256::repeat_byte(to)],
                Bytes::new(),
            ),
        },
        block_number: Some(100),
        block_hash: Some(B256::repeat_byte(100)),
        transaction_hash: Some(B256::repeat_byte(index as u8)),
        transaction_index: Some(index),
        log_index: Some(index),
        ..Default::default()
    }
}

#[tokio::test]
async fn rpc_indexed_alternatives_return_incoming_outgoing_and_self_transfer_once() {
    for address_scoped in [false, true] {
        let (provider, asserter) = JsonRpcCachedProvider::mock_with_asserter(1);
        let outgoing = log(0, 11, 22);
        let incoming = log(1, 22, 11);
        let self_transfer = log(2, 11, 11);
        asserter.push_success(&vec![outgoing, self_transfer.clone()]);
        asserter.push_success(&vec![incoming, self_transfer]);
        let logs = provider.get_logs(&filter(address_scoped)).await.unwrap();
        assert_eq!(logs.iter().map(|log| log.log_index.unwrap()).collect::<Vec<_>>(),vec![0,1,2],
            "both alternatives must be queried, their overlap deduplicated and events ordered; address-scoped={address_scoped}");
    }
}

#[tokio::test]
async fn rpc_indexed_alternatives_do_not_deliver_partial_success() {
    let (provider, asserter) = JsonRpcCachedProvider::mock_with_asserter(1);
    asserter.push_success(&vec![log(0, 11, 22)]);
    // The second RPC has no mock response and fails. The successful first
    // alternative cannot establish completeness of the requested union.
    assert!(provider.get_logs(&filter(true)).await.is_err());
}

#[test]
fn shared_tip_indexed_alternatives_match_the_same_union_without_duplicate_delivery() {
    let logs = vec![log(0, 11, 22), log(1, 22, 11), log(2, 11, 11), log(3, 22, 22)];
    for address_scoped in [false, true] {
        let addresses = address_scoped.then(|| HashSet::from([Address::repeat_byte(7)]));
        let selected = filter_logs_for_stream(&logs, &addresses, &filter(address_scoped));
        assert_eq!(
            selected.iter().map(|log| log.log_index.unwrap()).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }
}
