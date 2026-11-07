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

/// A fatal cron result must be observed even while another service's
/// future (GraphQL or live indexing) is legitimately pending.
#[tokio::test]
async fn terminal_cron_failure_interrupts_pending_runtime() {
    let cron = tokio::spawn(async { Err::<(), String>("cron cursor failed permanently".into()) });
    let pending_runtime = std::future::pending::<Result<(), StartRindexerError>>();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        await_required_cron_after_runtime(Some(cron), pending_runtime),
    )
    .await
    .expect("cron failure must interrupt a persistent runtime")
    .expect_err("terminal cron failure must fail startup")
    .to_string();
    assert!(result.contains("cron cursor failed permanently"), "{result}");
}

/// A worker panic is an observed task failure, not a successful completion.
#[tokio::test]
async fn cron_join_failure_interrupts_pending_runtime() {
    let cron: tokio::task::JoinHandle<Result<(), String>> =
        tokio::spawn(async { panic!("injected cron worker panic") });
    let pending_runtime = std::future::pending::<Result<(), StartRindexerError>>();
    let error = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        await_required_cron_after_runtime(Some(cron), pending_runtime),
    )
    .await
    .expect("cron join failure must interrupt a persistent runtime")
    .expect_err("a crashed cron worker must fail startup")
    .to_string();
    assert!(error.contains("failed to join"), "{error}");
}

/// Normal bounded cron completion must not stop GraphQL or live indexing.
#[tokio::test]
async fn successful_cron_does_not_complete_a_persistent_runtime() {
    let cron = tokio::spawn(async { Ok::<(), String>(()) });
    let (release, continue_runtime) = tokio::sync::oneshot::channel::<()>();
    let runtime = async move {
        continue_runtime.await.expect("complete the simulated persistent runtime");
        Ok::<(), StartRindexerError>(())
    };
    let supervised = await_required_cron_after_runtime(Some(cron), runtime);
    tokio::pin!(supervised);
    tokio::select! {
        result = &mut supervised => panic!("cron success stopped persistent work: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
    }
    release.send(()).expect("release the persistent runtime");
    tokio::time::timeout(std::time::Duration::from_secs(1), supervised)
        .await
        .expect("runtime completes after release")
        .expect("successful cron and runtime complete");
}

/// A startup failure must not leave an already-owned cron task detached.
#[tokio::test]
async fn runtime_failure_retires_owned_cron_task() {
    let cron = tokio::spawn(std::future::pending::<Result<(), String>>());
    let observer = cron.abort_handle();
    let runtime = async {
        Err::<(), StartRindexerError>(StartRindexerError::PortConflict(
            "injected runtime failure".into(),
        ))
    };
    let error = await_required_cron_after_runtime(Some(cron), runtime)
        .await
        .expect_err("runtime failure must propagate")
        .to_string();
    assert!(error.contains("injected runtime failure"), "{error}");
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while !observer.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("required cron must be aborted after runtime failed");
}
