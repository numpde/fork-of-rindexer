//! Bounded no-code startup must not hide a terminal cron scheduler error.
use super::*;
use serde_json::json;

async fn bounded_start_with_cron(
    outcome: Result<(), String>,
) -> Result<(), Box<StartRindexerError>> {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = dir.path().join("rindexer.yaml");
    let manifest: Manifest = serde_json::from_value(json!({
        "name": "BoundedCron",
        "project_type": "no-code",
        "networks": [{"name": "test", "chain_id": 1, "rpc": "http://127.0.0.1:1"}],
        "contracts": []
    }))
    .unwrap();
    let prepared = PreparedProject::try_new(dir.path(), manifest).unwrap();
    let cron_scheduler_handle = tokio::spawn(async move { outcome });
    start_rindexer_prepared(
        StartDetails {
            manifest_path: &manifest_path,
            indexing_details: None,
            graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
            cron_scheduler_handle: Some(cron_scheduler_handle),
            watch: false,
        },
        prepared,
    )
    .await
    .map_err(Box::new)
}

#[tokio::test]
async fn bounded_cron_failure_is_visible_to_startup() {
    let error = bounded_start_with_cron(Err("historical cron samples 101-102 failed".into()))
        .await
        .expect_err("startup must not report success for a failed cron job")
        .to_string();
    assert!(error.contains("101-102") && error.contains("cron"), "{error}");
}

#[tokio::test]
async fn bounded_cron_success_still_completes_startup() {
    bounded_start_with_cron(Ok(())).await.expect("successful cron remains successful");
}
