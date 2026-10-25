//! Response interpretation belongs to the caller, including after prefetch/cache hits.
use super::view_call_cache_tests::{metadata, HistoricalRpc, ORIGINAL};
use super::*;

async fn resolve_fixture(
    bytes: Vec<u8>,
    network: &str,
    is_static: bool,
    prefetch: bool,
    signature: &str,
    accessor: &str,
) -> Option<DynSolValue> {
    let rpc = HistoricalRpc::start().await;
    rpc.set_return_data(bytes).await;
    let provider = crate::provider::create_client(
        &rpc.url,
        1,
        None,
        None,
        None,
        Default::default(),
        None,
        None,
    )
    .await
    .unwrap();
    let expression = format!(
        "${}($rindexer_contract_address, \"{signature}\"){accessor}",
        if is_static { "call_static" } else { "call" }
    );
    let call = parse_view_call(&expression).unwrap();
    let meta = metadata(ORIGINAL);
    let constants = Constants::default();
    if prefetch {
        let pending = resolve_view_call_to_pending(&call, &[], &meta, network, &constants).unwrap();
        if is_static {
            execute_static_multicall3_batch(provider.as_ref(), network, &[pending], Address::ZERO)
                .await
                .unwrap();
        } else {
            execute_multicall3_batch(
                provider.as_ref(),
                network,
                100,
                ORIGINAL,
                &[pending],
                Address::ZERO,
            )
            .await
            .unwrap();
        }
    }
    let value = execute_view_call(&call, &[], &meta, provider.as_ref(), network, &constants).await;
    let cached = execute_view_call(&call, &[], &meta, provider.as_ref(), network, &constants).await;
    assert_eq!(value, cached);
    assert_eq!(rpc.request_count().await, if value.is_some() || prefetch { 1 } else { 2 });
    rpc.stop().await;
    value
}

async fn large_tuple(is_static: bool, prefetch: bool, network: &str) {
    let large = U256::from(1) << 80;
    let bytes = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(32), 256),
        DynSolValue::Uint(large, 256),
    ])
    .abi_encode_params();
    assert_eq!(bytes.len(), 64);
    let result = resolve_fixture(bytes, network, is_static, prefetch, "state()", "[1]").await;
    assert_eq!(result, Some(DynSolValue::Uint(large, 256)));
}

#[tokio::test]
async fn historical_large_tuple_resolves_without_length_narrowing_panic() {
    large_tuple(false, false, "decode_historical").await;
    large_tuple(false, true, "decode_historical_prefetch").await;
}

#[tokio::test]
async fn static_large_tuple_resolves_without_length_narrowing_panic() {
    large_tuple(true, false, "decode_static").await;
    large_tuple(true, true, "decode_static_prefetch").await;
}

#[tokio::test]
async fn explicit_return_type_owns_static_prefetch_and_cache_interpretation() {
    let mut word = [0u8; 32];
    word[..3].copy_from_slice(b"ETH");
    let value = resolve_fixture(
        word.to_vec(),
        "decode_static_typed",
        true,
        true,
        "state() returns (bytes32 tag)",
        "",
    )
    .await;
    assert_eq!(value, Some(DynSolValue::FixedBytes(B256::from(word), 32)));
}

#[tokio::test]
async fn impossible_explicit_dynamic_length_returns_failure() {
    let bytes = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(32), 256),
        DynSolValue::Uint(U256::from(1) << 80, 256),
    ])
    .abi_encode_params();
    for (is_static, prefetch, network) in [
        (false, false, "decode_bad_length"),
        (false, true, "decode_bad_length_prefetch"),
        (true, false, "decode_bad_static_length"),
        (true, true, "decode_bad_static_length_prefetch"),
    ] {
        assert!(resolve_fixture(
            bytes.clone(),
            network,
            is_static,
            prefetch,
            "state() returns (string name)",
            ""
        )
        .await
        .is_none());
    }
}

#[tokio::test]
async fn each_expression_decodes_the_same_cached_response_with_its_own_return_type() {
    for is_static in [false, true] {
        let rpc = HistoricalRpc::start().await;
        let mut word = [0u8; 32];
        word[..3].copy_from_slice(b"ETH");
        rpc.set_return_data(word.to_vec()).await;
        let provider = crate::provider::create_client(
            &rpc.url,
            1,
            None,
            None,
            None,
            Default::default(),
            None,
            None,
        )
        .await
        .unwrap();
        let network = if is_static { "decode_static_shared" } else { "decode_historical_shared" };
        let meta = metadata(ORIGINAL);
        let constants = Constants::default();
        // Alternate declarations to exercise both the initial RPC and subsequent cache hits.
        for (signature, expected) in [
            ("state()", DynSolValue::String("ETH".to_string())),
            ("state() returns (bytes32 tag)", DynSolValue::FixedBytes(B256::from(word), 32)),
            ("state()", DynSolValue::String("ETH".to_string())),
        ] {
            let call = parse_view_call(&format!(
                "${}($rindexer_contract_address, \"{signature}\")",
                if is_static { "call_static" } else { "call" }
            ))
            .unwrap();
            assert_eq!(
                execute_view_call(&call, &[], &meta, provider.as_ref(), network, &constants).await,
                Some(expected)
            );
        }
        assert_eq!(rpc.request_count().await, 1);
        rpc.stop().await;
    }
}
