//! Timestamp-dependent effects must resolve metadata before deciding whether to apply.
use super::*;
use crate::{event::callback_registry::CallbackError, provider::mock::MockChainProvider};
use alloy::{
    network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
    rpc::types::{Block, BlockTransactions},
};

fn timestamp_provider(available: bool) -> Arc<MockChainProvider> {
    let provider = MockChainProvider::new(1);
    Arc::new(if available {
        provider.with_blocks(vec![AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: 2, timestamp: 200, ..Default::default() }
                        .seal(B256::repeat_byte(2)),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )])
    } else {
        provider.with_block_fetch_error("timestamp provider temporarily unavailable")
    })
}

async fn timestamp_fixture(condition: &str, alias: bool) -> Fixture {
    let mut table = json!({"name":"totals","global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","if":condition,
            "set":[{"column":"amount","action":"add","value":"$value"}]}]}]
    });
    if alias {
        table["columns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"observed_at","type":"uint256","default":"0"}));
        table["events"][0]["operations"][0]["set"].as_array_mut().unwrap().push(
            json!({"column":"observed_at","action":"set","value":"$rindexer_block_timestamp + 1"}),
        );
    }
    Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", table).await
}

#[tokio::test]
async fn postgres_timestamp_conditions_and_alias_arithmetic_use_resolved_metadata() {
    for metadata_timestamp in [None, Some(300u64)] {
        let fixture = timestamp_fixture("$rindexer_block_timestamp > 100", true).await;
        let mut params = fixture.params.clone();
        params.providers = Arc::new(std::collections::HashMap::from([(
            "ethereum".into(),
            timestamp_provider(metadata_timestamp.is_none())
                as Arc<dyn crate::provider::ChainProvider>,
        )]));
        let mut events = fixture.events(&[10]);
        events[0].log.block_timestamp = metadata_timestamp;
        events[0].tx_information.block_timestamp =
            metadata_timestamp.map(alloy::primitives::U256::from);
        (no_code_callback(Arc::new(params)).event_callback)(events).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (1, "10".into(), "2".into()),
            "a missing metadata field must not make the condition falsely skip its effect"
        );
        let observed: String = fixture
            .client
            .query_one(
                &format!(
                    "SELECT observed_at::text FROM {}",
                    fixture.params.tables[0].full_table_name
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(observed, (metadata_timestamp.unwrap_or(200) + 1).to_string());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_required_timestamp_failure_rolls_back_and_recovers_exactly_once() {
    let fixture = timestamp_fixture("rindexer_block_timestamp > 100", false).await;
    let mut failing = fixture.params.clone();
    failing.providers = Arc::new(std::collections::HashMap::from([(
        "ethereum".into(),
        timestamp_provider(false) as Arc<dyn crate::provider::ChainProvider>,
    )]));
    let result = (no_code_callback(Arc::new(failing)).event_callback)(fixture.events(&[10])).await;
    assert!(
        matches!(result, Err(CallbackError::Retryable(_))),
        "missing required timestamp must fail retryably, not commit a skipped effect: {result:?}"
    );
    assert_eq!(fixture.position().await, (0, "0".into(), "0".into()));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 0);
    let mut healthy = fixture.params.clone();
    healthy.providers = Arc::new(std::collections::HashMap::from([(
        "ethereum".into(),
        timestamp_provider(true) as Arc<dyn crate::provider::ChainProvider>,
    )]));
    let callback = no_code_callback(Arc::new(healthy)).event_callback;
    callback(fixture.events(&[10])).await.unwrap();
    callback(fixture.events(&[10])).await.unwrap();
    assert_eq!(fixture.position().await, (1, "10".into(), "2".into()));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_timestamp_word_in_a_literal_does_not_require_timestamp_rpc() {
    let fixture =
        timestamp_fixture("'rindexer_block_timestamp' == 'rindexer_block_timestamp'", false).await;
    let mut params = fixture.params.clone();
    params.providers = Arc::new(std::collections::HashMap::from([(
        "ethereum".into(),
        timestamp_provider(false) as Arc<dyn crate::provider::ChainProvider>,
    )]));
    (no_code_callback(Arc::new(params)).event_callback)(fixture.events(&[10])).await.unwrap();
    assert_eq!(fixture.position().await, (1, "10".into(), "2".into()));
    fixture.finish().await;
}
