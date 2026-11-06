//! A failed historical sample cannot be marked complete or enter live cron.
use super::*;
use crate::provider::mock::MockChainProvider;
use serde_json::json;

fn scheduler(
    value: &str,
    end_block: Option<u64>,
    provider: Option<Arc<dyn ChainProvider>>,
) -> CronScheduler {
    let mut cron = json!({
        "interval": "1h",
        "start_block": "101",
        "block_interval": "1",
        "operations": [{
            "type": "upsert",
            "set": [{"column": "total", "action": "set", "value": value}]
        }]
    });
    if let Some(end) = end_block {
        cron["end_block"] = json!(end.to_string());
    }
    let manifest: Manifest = serde_json::from_value(json!({
        "name": "CronFailure",
        "project_type": "no-code",
        "networks": [{"name": "test", "chain_id": 1, "rpc": "http://127.0.0.1:1"}],
        "contracts": [{
            "name": "Token",
            "abi": "unused.json",
            "details": [{
                "network": "test",
                "address": "0x0000000000000000000000000000000000000001",
                "end_block": "102"
            }],
            "tables": [{
                "name": "samples",
                "global": true,
                "columns": [{"name": "total", "type": "uint256", "default": "0"}],
                "cron": [cron]
            }]
        }]
    }))
    .unwrap();

    let providers: HashMap<String, Arc<dyn ChainProvider>> =
        provider.into_iter().map(|p| ("test".to_owned(), p)).collect();
    CronScheduler::try_new(&manifest, None, None, Arc::new(providers)).unwrap()
}

fn mock() -> Arc<dyn ChainProvider> {
    Arc::new(MockChainProvider::new(1).with_block_number(102))
}

#[tokio::test]
async fn missing_required_view_call_is_not_reported_as_completed() {
    // No header or call result is available at 101; block 102 is scheduled too.
    // A completed block 100 must never authorize skipping either sample.
    let view_call = r#"$call($contract, "value() returns (uint256)")"#;
    let cron = scheduler(view_call, Some(102), Some(mock()));
    let error = cron.run().await.expect_err("unavailable required RPC data must fail").to_string();
    for expected in ["samples", "test", "101", "total"] {
        assert!(error.contains(expected), "missing {expected:?}: {error}");
    }
}

#[tokio::test]
async fn unresolved_required_metadata_is_not_a_successful_empty_batch() {
    // The cron reference validator currently admits $rindexer_* names, but
    // this unsupported one resolves to no value in the production extractor.
    let cron = scheduler("$rindexer_unknown", Some(102), Some(mock()));
    let error = cron.run().await.expect_err("missing required value must fail").to_string();
    assert!(error.contains("101") && error.contains("total"), "{error}");
}

#[tokio::test]
async fn missing_network_provider_reaches_the_scheduler_caller() {
    let cron = scheduler("1", Some(102), None);
    let error = cron.run().await.expect_err("missing provider must not be swallowed").to_string();
    assert!(error.contains("test") && error.contains("101"), "{error}");
}

#[tokio::test]
async fn failed_historical_head_lookup_does_not_enter_live_mode() {
    let provider: Arc<dyn ChainProvider> =
        Arc::new(MockChainProvider::new(1).with_block_number_error("head unavailable"));
    let cron = scheduler("1", None, Some(provider));
    let error = tokio::time::timeout(Duration::from_secs(2), cron.run())
        .await
        .expect("historical head failure must terminate the task")
        .expect_err("failed head lookup must not fall back to start_block")
        .to_string();
    assert!(error.contains("head unavailable") && error.contains("test"), "{error}");
}

#[tokio::test]
async fn valid_bounded_historical_work_is_still_successful() {
    let cron = scheduler("1", Some(102), Some(mock()));
    cron.run().await.expect("both complete literal-valued samples are valid");
}
