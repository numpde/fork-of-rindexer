use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn application_failure_is_observed_while_a_peer_stream_is_live() {
    failed_task_stops_its_generation(false).await;
}

#[tokio::test]
async fn task_panic_is_observed_while_a_peer_stream_is_live() {
    failed_task_stops_its_generation(true).await;
}

async fn failed_task_stops_its_generation(panic: bool) {
    let cancel = CancellationToken::new();
    let peer_cancel = cancel.clone();
    let settled = Arc::new(AtomicBool::new(false));
    let peer_settled = settled.clone();
    let peer = tokio::spawn(async move {
        peer_cancel.cancelled().await;
        peer_settled.store(true, Ordering::SeqCst);
        Ok(())
    });
    let failed = tokio::spawn(async move {
        assert!(!panic, "scripted stream task panic");
        Err(CombinedLogEventProcessingError::NonBlockingError(ProcessEventError::ProcessLogs(
            Box::new(ProviderError::CustomError("scripted terminal fetch failure".into())),
        )))
    });
    let supervisor_cancel = cancel.clone();
    let mut supervisor = tokio::spawn(async move {
        await_indexing_tasks(
            vec![peer, failed],
            &supervisor_cancel,
            &EventCallbackRegistry::new(),
            &TraceCallbackRegistry::new(),
        )
        .await
        .map_err(|e| e.to_string())
    });
    let early = timeout(Duration::from_millis(150), &mut supervisor).await;
    let observed_before_cleanup = early.is_ok();
    let requested_stop = cancel.is_cancelled();
    // Retire the synthetic peer even on the red implementation; cleanup cannot make it pass.
    cancel.cancel();
    let result = match early {
        Ok(result) => result,
        Err(_) => timeout(Duration::from_secs(2), supervisor).await.expect("settle supervisor"),
    }
    .expect("supervisor itself does not panic")
    .expect_err("stream failure reaches its caller");
    assert!(result.contains(if panic { "panic" } else { "scripted terminal fetch failure" }));
    assert!(settled.load(Ordering::SeqCst), "peer streams settle before the supervisor returns");
    assert!(observed_before_cleanup && requested_stop, "a failed stream must stop its live generation without waiting for peers to finish naturally");
}

#[tokio::test]
async fn successful_finite_streams_do_not_cancel_the_generation() {
    let cancel = CancellationToken::new();
    await_indexing_tasks(
        vec![tokio::spawn(async { Ok(()) })],
        &cancel,
        &EventCallbackRegistry::new(),
        &TraceCallbackRegistry::new(),
    )
    .await
    .unwrap();
    assert!(!cancel.is_cancelled());
}

#[tokio::test]
async fn task_panic_reaches_health_before_live_peers_finish_cleanup() {
    use crate::indexer::process::checkpoint_order_tests::Fixture;
    use tokio::sync::Notify;
    let fixture = Fixture::with_logs(Arc::new(|_| Box::pin(async { Ok(()) })), vec![]).await;
    let registry = fixture.config.registry();
    let failures = registry.failure_state();
    let cancel = CancellationToken::new();
    let peer_cancel = cancel.clone();
    let release = Arc::new(Notify::new());
    let peer_release = release.clone();
    let peer = tokio::spawn(async move {
        peer_cancel.cancelled().await;
        peer_release.notified().await;
        Ok(())
    });
    let failed = tokio::spawn(async { panic!("scripted task panic before dispatcher reporting") });
    let supervisor_cancel = cancel.clone();
    let supervisor = tokio::spawn(async move {
        await_indexing_tasks(
            vec![peer, failed],
            &supervisor_cancel,
            &registry,
            &TraceCallbackRegistry::new(),
        )
        .await
    });
    timeout(Duration::from_secs(2), cancel.cancelled()).await.expect("generation cancelled");
    let reported = failures.snapshot();
    let still_draining = !supervisor.is_finished();
    release.notify_one();
    timeout(Duration::from_secs(2), supervisor)
        .await
        .unwrap()
        .unwrap()
        .expect_err("panic propagated");
    assert!(still_draining, "live peer cleanup remains owned");
    assert_eq!(reported.len(), 1, "health must fail during task-panic cleanup");
    assert_eq!(reported[0].network, "ethereum");
    assert_eq!(reported[0].contract, "Contract");
}
