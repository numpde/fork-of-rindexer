//! Required delivery must survive a stopped generation without replaying applied effects.
use super::*;
use crate::event::callback_registry::EventCallbackType;
use crate::{
    blockclock::BlockClock,
    event::{
        callback_registry::{noop_decoder, TraceCallbackRegistry},
        contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
    },
    indexer::{reorg::ReorgCoordinator, IndexingEventsProgressState},
    provider::mock::MockChainProvider,
};
use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex as StdMutex, Weak,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct EndpointState {
    failing: AtomicBool,
    attempts: StdMutex<Vec<(String, bool)>>,
    messages: StdMutex<Vec<Value>>,
    hanging: AtomicBool,
    release: tokio::sync::Notify,
}
struct Endpoint {
    url: String,
    state: Arc<EndpointState>,
    server: JoinHandle<()>,
}
impl Endpoint {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(EndpointState::default());
        state.failing.store(true, Ordering::SeqCst);
        let app = Router::new()
            .route(
                "/",
                post(
                    |State(state): State<Arc<EndpointState>>,
                     headers: HeaderMap,
                     Json(message): Json<serde_json::Value>| async move {
                        let failed = state.failing.load(Ordering::SeqCst);
                        let id = headers
                            .get(crate::streams::STREAM_MESSAGE_ID_KEY)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string();
                        state.messages.lock().unwrap().push(message);
                        state.attempts.lock().unwrap().push((id, !failed));
                        if state.hanging.load(Ordering::SeqCst) {
                            state.release.notified().await;
                        }
                        if failed {
                            StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            StatusCode::OK
                        }
                    },
                ),
            )
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { url, state, server }
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Running {
    cancel: CancellationToken,
    handles: Vec<JoinHandle<Result<(), crate::indexer::process::ProcessEventError>>>,
    coordinators: std::collections::HashMap<String, Arc<tokio::sync::Mutex<ReorgCoordinator>>>,
    clients: Weak<Option<crate::streams::StreamsClients>>,
}
impl Running {
    async fn stop_tasks(&mut self) {
        self.cancel.cancel();
        for handle in self.handles.drain(..) {
            tokio::time::timeout(Duration::from_secs(3), handle).await.unwrap().unwrap().unwrap();
        }
    }
    async fn stop(mut self) {
        self.stop_tasks().await;
        drop(self.coordinators);
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.clients.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("stopped generation releases its stream clients and in-memory delivery state");
    }
}

async fn start(fixture: &Fixture, endpoint: &Endpoint) -> Running {
    start_with_delivery(fixture, endpoint, "instant", 100).await
}

async fn start_with_delivery(
    fixture: &Fixture,
    endpoint: &Endpoint,
    delivery: &str,
    head: u64,
) -> Running {
    try_start(
        fixture,
        endpoint,
        delivery,
        Chain { head, hash: B256::repeat_byte(100), value: Some(10) },
    )
    .await
    .unwrap()
}

struct Chain {
    head: u64,
    hash: B256,
    value: Option<u64>,
}
async fn try_start(
    fixture: &Fixture,
    endpoint: &Endpoint,
    delivery: &str,
    chain: Chain,
) -> Result<Running, crate::indexer::start::StartIndexingError> {
    let Chain { head, hash, value } = chain;
    let streams=serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[{
        "endpoint":endpoint.url,"shared_secret":"test","delivery":delivery,"networks":["ethereum"],"events":[{"event_name":"Value"}]
    }]})).unwrap();
    let clients = Arc::new(Some(crate::streams::StreamsClients::new(streams).await));
    let weak = Arc::downgrade(&clients);
    let mut params = fixture.params.clone();
    params.streams_clients = clients.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    let mut registry = super::super::permanent_failure_tests::registry(&fixture.params, callback);
    registry.events[0].streams_clients = clients;
    let logs = value
        .into_iter()
        .map(|value| {
            let mut log = fixture.events(&[value]).remove(0).log;
            log.block_number = Some(100);
            log.block_hash = Some(hash);
            log
        })
        .collect();
    let headers = (99..=head)
        .map(|number| {
            let mut block = crate::indexer::tables::view_call_cache_tests::header(
                B256::repeat_byte(number as u8),
                number,
            );
            block.header.inner.number = number;
            if number == 100 {
                block.header.hash = hash;
            }
            block.header.inner.parent_hash =
                if number == 101 { hash } else { B256::repeat_byte((number - 1) as u8) };
            block
        })
        .collect();
    let provider = Arc::new(
        MockChainProvider::new(1).with_block_number(head).with_blocks(headers).with_logs(logs),
    );
    let id = registry.events[0].id.clone();
    registry.events[0].contract.details.push(NetworkContract {
        id,
        network: "ethereum".into(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: noop_decoder(),
        start_block: Some(U64::from(99)),
        end_block: None,
        disable_logs_bloom_checks: true,
    });
    let registry = registry.complete();
    let progress = IndexingEventsProgressState::monitor(&registry.events, &[], None).await;
    let manifest=serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],"networks":[{
        "name":"ethereum","chain_id":1,"rpc":"http://unused.invalid","reorg_handling":{"enabled":true,"window_size":8}
    }]})).unwrap();
    let cancel = CancellationToken::new();
    let (handles, _, _, _, coordinators) = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &fixture.indexer,
        registry,
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        false,
        cancel.clone(),
        progress,
    )
    .await?;
    Ok(Running { cancel, handles, coordinators, clients: weak })
}

#[tokio::test]
async fn postgres_required_stream_retries_committed_delivery_after_restart_without_new_events() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    let running = start(&fixture, &endpoint).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.position().await == (1, "10".into(), "100".into())
                && !endpoint.state.attempts.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("failed publication must leave the indexed event durably applied");
    running.stop().await;
    let stopped_attempts = endpoint.state.attempts.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        endpoint.state.attempts.lock().unwrap().len(),
        stopped_attempts,
        "publisher tasks cannot outlive their stopped generation"
    );
    let failed_id = endpoint.state.attempts.lock().unwrap()[0].0.clone();
    endpoint.state.failing.store(false, Ordering::SeqCst);
    let restarted = start(&fixture, &endpoint).await;
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if endpoint.state.attempts.lock().unwrap().iter().any(|(_, ok)| *ok) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    restarted.stop().await;
    let position = fixture.position().await;
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    let successful = endpoint
        .state
        .attempts
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    fixture.finish().await;
    assert!(delivered,"a committed required delivery must retry after restart even when the durable cursor leaves no new event to fetch");
    assert_eq!(position, (1, "10".into(), "100".into()));
    assert_eq!(receipts, 1, "outbox retries cannot reclaim the source event");
    assert!(
        successful.iter().all(|id| id == &failed_id),
        "the delivery id must survive retries and restart"
    );
}

#[tokio::test]
async fn postgres_required_stream_finalized_pending_delivery_survives_restart() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    endpoint.state.failing.store(false, Ordering::SeqCst);
    let running = start_with_delivery(&fixture, &endpoint, "finalized", 100).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.position().await.2 != "100" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        endpoint.state.attempts.lock().unwrap().is_empty(),
        "unfinalized source events must remain pending"
    );
    running.stop().await;
    let head = 100 + crate::indexer::reorg::reorg_safe_distance_for_chain(1) + 2;
    let restarted = start_with_delivery(&fixture, &endpoint, "finalized", head).await;
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        while endpoint.state.attempts.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    restarted.stop().await;
    let position = fixture.position().await;
    fixture.finish().await;
    assert!(delivered,"finalization must release a durable pending event after restart, without refetching that applied event");
    assert_eq!((position.0, position.1), (1, "10".into()));
}

#[tokio::test]
async fn postgres_required_stream_healthy_endpoint_delivers_current_event() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    endpoint.state.failing.store(false, Ordering::SeqCst);
    let running = start(&fixture, &endpoint).await;
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let attempted = !endpoint.state.attempts.lock().unwrap().is_empty();
            if attempted && fixture.position().await.2 == "100" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    running.stop().await;
    let position = fixture.position().await;
    fixture.finish().await;
    assert!(delivered);
    assert_eq!(position, (1, "10".into(), "100".into()));
}

async fn callback_with_required_delivery(
    fixture: &Fixture,
    endpoint: &Endpoint,
) -> EventCallbackType {
    let mut params = fixture.params.clone();
    params.streams_clients=Arc::new(Some(crate::streams::StreamsClients::new(serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[{
        "endpoint":endpoint.url,"shared_secret":"test","networks":["ethereum"],"events":[{"event_name":"Value"}]
    }]})).unwrap()).await));
    no_code_callback(Arc::new(params)).event_callback
}

#[tokio::test]
async fn postgres_required_stream_enqueue_failure_aborts_all_source_effects() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    let callback = callback_with_required_delivery(&fixture, &endpoint).await;
    let before = fixture.position().await;
    fixture.client.batch_execute("ALTER TABLE rindexer_internal.stream_outbox ADD CONSTRAINT reject_delivery CHECK(false)").await.unwrap();
    assert!(callback(fixture.events(&[10])).await.is_err());
    assert_eq!(fixture.position().await, before);
    let count: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "failed enqueue must not claim application success");
    fixture
        .client
        .batch_execute(
            "ALTER TABLE rindexer_internal.stream_outbox DROP CONSTRAINT reject_delivery",
        )
        .await
        .unwrap();
    callback(fixture.events(&[10])).await.unwrap();
    callback(fixture.events(&[10])).await.unwrap();
    assert_eq!(fixture.position().await.0, 1);
    let count: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.stream_outbox", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "receipt suppression must not create another obligation");
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_reset_cannot_erase_pending_delivery() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    let callback = callback_with_required_delivery(&fixture, &endpoint).await;
    callback(fixture.events(&[10])).await.unwrap();
    let before = fixture.position().await;
    let sql = crate::database::generate::drop_tables_for_indexer_sql(
        fixture._directory.path(),
        &fixture.indexer,
    );
    assert!(fixture.client.batch_execute(sql.as_str()).await.is_err());
    assert_eq!(fixture.position().await, before);
    drop(callback);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_rejects_retargeting_pending_delivery() {
    let fixture = Fixture::new().await;
    let original = Endpoint::start().await;
    let replacement = Endpoint::start().await;
    let callback = callback_with_required_delivery(&fixture, &original).await;
    callback(fixture.events(&[10])).await.unwrap();
    drop(callback);
    let result = try_start(
        &fixture,
        &replacement,
        "instant",
        Chain { head: 100, hash: B256::repeat_byte(100), value: None },
    )
    .await;
    assert!(result.is_err(), "startup must not send pending work to a different endpoint");
    assert!(replacement.state.attempts.lock().unwrap().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_reorg_cancels_inflight_and_durably_retracts_before_restart() {
    use crate::indexer::reorg::ReorgContext;
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    endpoint.state.hanging.store(true, Ordering::SeqCst);
    let running = start(&fixture, &endpoint).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while endpoint.state.attempts.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let replacement = B256::repeat_byte(0xea);
    let coordinator = running.coordinators.get("ethereum").unwrap().clone();
    let mut owner = coordinator.lock().await;
    let mut task = owner.try_create_reorg_task_for_block_range(100, 100).unwrap();
    task.canonical_blocks = vec![(100, replacement, B256::repeat_byte(99))];
    // Retire the in-flight publisher through recovery's cancellation protocol;
    // do not abort its task manually to make the writer barrier available.
    tokio::time::timeout(
        Duration::from_secs(2),
        owner.handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        ),
    )
    .await
    .expect("recovery must acquire its writer barrier while publication is blocked")
    .unwrap();
    running.cancel.cancel();
    drop(owner);
    drop(coordinator);
    running.stop().await;
    let rows=fixture.client.query("SELECT source_table IS NULL, invalidated FROM rindexer_internal.stream_outbox ORDER BY created_sequence",&[]).await.unwrap();
    assert!(
        rows.iter().any(|row| !row.get::<_, bool>(0) && row.get::<_, bool>(1)),
        "orphaned source obligations must be retired"
    );
    assert!(
        rows.iter().any(|row| row.get::<_, bool>(0) && !row.get::<_, bool>(1)),
        "retraction must commit with rollback"
    );
    endpoint.state.hanging.store(false, Ordering::SeqCst);
    endpoint.state.failing.store(false, Ordering::SeqCst);
    endpoint.state.release.notify_waiters();
    let old = endpoint.state.messages.lock().unwrap().len();
    let restarted = try_start(
        &fixture,
        &endpoint,
        "instant",
        Chain { head: 100, hash: replacement, value: None },
    )
    .await
    .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        while endpoint.state.messages.lock().unwrap().len() == old {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    restarted.stop().await;
    let messages = endpoint.state.messages.lock().unwrap()[old..].to_vec();
    assert!(delivered);
    assert!(
        messages.iter().all(|message| message["event_name"] == "__rindexer_reorg"),
        "old source payloads cannot escape after rollback"
    );
    assert_eq!(fixture.position().await.0, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_ack_failure_retries_the_same_delivery_id() {
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    endpoint.state.failing.store(false, Ordering::SeqCst);
    fixture.client.batch_execute("ALTER TABLE rindexer_internal.stream_outbox ADD CONSTRAINT reject_ack CHECK(acknowledged_at IS NULL)").await.unwrap();
    let running = start(&fixture, &endpoint).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let accepted = !endpoint.state.attempts.lock().unwrap().is_empty();
            if accepted && fixture.position().await.2 == "100" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    running.stop().await;
    let old = endpoint.state.attempts.lock().unwrap().len();
    let id = endpoint.state.attempts.lock().unwrap()[0].0.clone();
    fixture
        .client
        .batch_execute("ALTER TABLE rindexer_internal.stream_outbox DROP CONSTRAINT reject_ack")
        .await
        .unwrap();
    let restarted = start(&fixture, &endpoint).await;
    let retried = tokio::time::timeout(Duration::from_secs(3), async {
        while endpoint.state.attempts.lock().unwrap().len() == old {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    restarted.stop().await;
    assert!(retried, "a published record without a committed acknowledgment remains pending");
    assert!(endpoint.state.attempts.lock().unwrap().iter().all(|(attempt, _)| attempt == &id));
    assert_eq!(fixture.position().await, (1, "10".into(), "100".into()));
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_failed_retraction_enqueue_rolls_back_recovery() {
    use crate::indexer::reorg::ReorgContext;
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    let mut running = start_with_delivery(&fixture, &endpoint, "finalized", 100).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.position().await.2 != "100" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    running.stop_tasks().await;
    let before = fixture.position().await;
    fixture.client.batch_execute("ALTER TABLE rindexer_internal.stream_outbox ADD CONSTRAINT reject_retraction CHECK(source_table IS NOT NULL)").await.unwrap();
    let coordinator = running.coordinators.get("ethereum").unwrap().clone();
    let mut owner = coordinator.lock().await;
    let mut task = owner.try_create_reorg_task_for_block_range(100, 100).unwrap();
    task.canonical_blocks = vec![(100, B256::repeat_byte(0xea), B256::repeat_byte(99))];
    let context = ReorgContext {
        postgres: Some(&fixture.client),
        clickhouse: None,
        registry: None,
        trace_registry: None,
    };
    assert!(owner.handle_reorg(task, &context).await.is_err());
    assert_eq!(
        fixture.position().await,
        before,
        "retraction and raw/derived rollback must commit together"
    );
    let count:i64=fixture.client.query_one("SELECT count(*) FROM rindexer_internal.stream_outbox WHERE NOT invalidated AND source_table IS NOT NULL",&[]).await.unwrap().get(0);
    assert_eq!(count, 1);
    fixture
        .client
        .batch_execute(
            "ALTER TABLE rindexer_internal.stream_outbox DROP CONSTRAINT reject_retraction",
        )
        .await
        .unwrap();
    owner.retry_pending_reorg(&context).await.unwrap();
    assert_eq!(fixture.position().await.0, 0);
    let count: i64 = fixture
        .client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.stream_outbox WHERE source_table IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "retry must retain one durable retraction");
    drop(owner);
    drop(coordinator);
    running.stop().await;
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_rejects_unsupported_source_context_before_startup() {
    let fixture = Fixture::new().await;
    for mode in ["no-postgres", "no-raw-events", "rust-handler"] {
        let clients = Arc::new(Some(
            crate::streams::StreamsClients::new(
                serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[]}))
                    .unwrap(),
            )
            .await,
        ));
        let mut params = fixture.params.clone();
        params.streams_clients = clients.clone();
        if mode == "no-postgres" {
            params.postgres = None;
        }
        if mode == "no-raw-events" {
            params.store_raw_events = false;
        }
        let callback: EventCallbackType = if mode == "rust-handler" {
            Arc::new(|_| Box::pin(async { Ok(()) }))
        } else {
            no_code_callback(Arc::new(params)).event_callback
        };
        let mut registry =
            super::super::permanent_failure_tests::registry(&fixture.params, callback);
        registry.events[0].streams_clients = clients;
        let manifest = serde_json::from_value(
            json!({"name":"Replay","project_type":"no-code","contracts":[],"networks":[]}),
        )
        .unwrap();
        assert!(
            crate::indexer::required_delivery::validate(
                &manifest,
                &registry,
                &TraceCallbackRegistry::new(),
                Some(&fixture.client)
            )
            .await
            .is_err(),
            "{mode} cannot enable required delivery"
        );
    }
    assert_eq!(fixture.position().await.0, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_admission_tracks_only_destination_networks() {
    let fixture = Fixture::new().await;
    for destination in ["ethereum", "missing"] {
        let clients=Arc::new(Some(crate::streams::StreamsClients::new(serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[{
            "endpoint":"http://unused.invalid","shared_secret":"test","networks":[destination],"events":[{"event_name":"Value"}]
        }]})).unwrap()).await));
        let mut params = fixture.params.clone();
        params.streams_clients = clients.clone();
        let callback = no_code_callback(Arc::new(params)).event_callback;
        let mut registry =
            super::super::permanent_failure_tests::registry(&fixture.params, callback);
        registry.events[0].streams_clients = clients;
        for network in ["ethereum", "other"] {
            let provider = Arc::new(MockChainProvider::new(1));
            let id = registry.events[0].id.clone();
            registry.events[0].contract.details.push(NetworkContract {
                id,
                network: network.into(),
                indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                    address: ValueOrArray::Value(Address::ZERO),
                    indexed_filters: None,
                }),
                cached_provider: provider.clone(),
                block_clock: BlockClock::new(None, None, provider),
                decoder: noop_decoder(),
                start_block: None,
                end_block: None,
                disable_logs_bloom_checks: true,
            });
        }
        let manifest=serde_json::from_value(json!({"name":"Replay","project_type":"no-code","contracts":[],"networks":[
            {"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid","reorg_handling":{"enabled":true}},
            {"name":"other","chain_id":2,"rpc":"http://unused.invalid"}
        ]})).unwrap();
        let result = crate::indexer::required_delivery::validate(
            &manifest,
            &registry,
            &TraceCallbackRegistry::new(),
            Some(&fixture.client),
        )
        .await;
        assert_eq!(result.is_ok(),destination=="ethereum","unselected networks need no delivery worker, while an entirely missing destination is a startup error");
    }
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_failed_destination_does_not_starve_healthy_destination() {
    use crate::event::callback_registry::FailedStream;
    let fixture = Fixture::new().await;
    let failing = Endpoint::start().await;
    let healthy = Endpoint::start().await;
    healthy.state.failing.store(false, Ordering::SeqCst);
    let streams=serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[
        {"endpoint":failing.url,"shared_secret":"test","networks":["ethereum"],"events":[{"event_name":"Value"}]},
        {"endpoint":healthy.url,"shared_secret":"test","networks":["ethereum"],"events":[{"event_name":"Value"}]}
    ]})).unwrap();
    let clients = Arc::new(Some(crate::streams::StreamsClients::new(streams).await));
    let weak = Arc::downgrade(&clients);
    let mut params = fixture.params.clone();
    params.streams_clients = clients.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(fixture.events(&[10])).await.unwrap();
    let registry = crate::event::callback_registry::EventCallbackRegistry::new();
    let cancel = CancellationToken::new();
    crate::streams::StreamsClients::start_delivery_worker(
        &clients,
        fixture.client.clone(),
        "ethereum".into(),
        cancel.clone(),
        registry.failure_state(),
        FailedStream {
            id: "delivery-fairness".into(),
            indexer: "Replay".into(),
            contract: "Contract".into(),
            event: "Value".into(),
            network: "ethereum".into(),
            from_block: 2,
            to_block: 2,
        },
    );
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        while healthy.state.attempts.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    cancel.cancel();
    drop(callback);
    drop(clients);
    drop(registry);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(delivered,"one failed destination cannot indefinitely block another destination's committed obligation");
    assert_eq!(fixture.position().await.0, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_finalized_uses_current_distance_for_pending_records() {
    use crate::event::callback_registry::FailedStream;
    let fixture = Fixture::new().await;
    let endpoint = Endpoint::start().await;
    endpoint.state.failing.store(false, Ordering::SeqCst);
    let streams=serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[{
        "endpoint":endpoint.url,"shared_secret":"test","delivery":"finalized","networks":["ethereum"],"events":[{"event_name":"Value"}]
    }]})).unwrap();
    let clients = Arc::new(Some(crate::streams::StreamsClients::new(streams).await));
    let weak = Arc::downgrade(&clients);
    let client = clients.as_ref().as_ref().unwrap();
    client.register_network_reorg_distance("ethereum".into(), 20);
    let mut params = fixture.params.clone();
    params.streams_clients = clients.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(fixture.events(&[10])).await.unwrap();
    fixture.client.batch_execute(&format!("INSERT INTO rindexer_internal.reorg_block_hashes (network,block_number,block_hash,parent_hash) VALUES ('ethereum',100,'{:#x}','{:#x}')",B256::repeat_byte(100),B256::repeat_byte(99))).await.unwrap();
    client.register_network_reorg_distance("ethereum".into(), 200);
    let cancel = CancellationToken::new();
    let registry = crate::event::callback_registry::EventCallbackRegistry::new();
    crate::streams::StreamsClients::start_delivery_worker(
        &clients,
        fixture.client.clone(),
        "ethereum".into(),
        cancel.clone(),
        registry.failure_state(),
        FailedStream {
            id: "finality-policy".into(),
            indexer: "Replay".into(),
            contract: "Contract".into(),
            event: "Value".into(),
            network: "ethereum".into(),
            from_block: 2,
            to_block: 2,
        },
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let released_too_early = !endpoint.state.attempts.lock().unwrap().is_empty();
    client.register_network_reorg_distance("ethereum".into(), 0);
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        while endpoint.state.attempts.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    cancel.cancel();
    drop(callback);
    drop(clients);
    drop(registry);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.finish().await;
    assert!(
        !released_too_early,
        "a stricter current finality distance must protect previously queued records"
    );
    assert!(released, "relaxing the current distance must make the same pending record eligible");
}

#[tokio::test]
async fn postgres_required_stream_hanging_destination_does_not_starve_healthy_destination() {
    use crate::event::callback_registry::FailedStream;
    let fixture = Fixture::new().await;
    let failing = Endpoint::start().await;
    failing.state.hanging.store(true, Ordering::SeqCst);
    let healthy = Endpoint::start().await;
    healthy.state.failing.store(false, Ordering::SeqCst);
    let streams=serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[
        {"endpoint":failing.url,"shared_secret":"test","networks":["ethereum"],"events":[{"event_name":"Value"}]},
        {"endpoint":healthy.url,"shared_secret":"test","networks":["ethereum"],"events":[{"event_name":"Value"}]}
    ]})).unwrap();
    let clients = Arc::new(Some(crate::streams::StreamsClients::new(streams).await));
    let weak = Arc::downgrade(&clients);
    let mut params = fixture.params.clone();
    params.streams_clients = clients.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(fixture.events(&[10])).await.unwrap();
    let registry = crate::event::callback_registry::EventCallbackRegistry::new();
    let cancel = CancellationToken::new();
    crate::streams::StreamsClients::start_delivery_worker(
        &clients,
        fixture.client.clone(),
        "ethereum".into(),
        cancel.clone(),
        registry.failure_state(),
        FailedStream {
            id: "delivery-timeout-fairness".into(),
            indexer: "Replay".into(),
            contract: "Contract".into(),
            event: "Value".into(),
            network: "ethereum".into(),
            from_block: 2,
            to_block: 2,
        },
    );
    let delivered = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let pending: i64 = fixture.client.query_one(
                "SELECT COUNT(*) FROM rindexer_internal.stream_outbox WHERE acknowledged_at IS NULL",
                &[],
            ).await.unwrap().get(0);
            if !healthy.state.attempts.lock().unwrap().is_empty() && pending == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    cancel.cancel();
    drop(callback);
    drop(clients);
    drop(registry);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        delivered,
        "a publisher that never responds must be deferred so a healthy destination progresses"
    );
    let deferred: bool = fixture.client.query_one(
        "SELECT retry_after > created_at AND acknowledged_at IS NULL FROM rindexer_internal.stream_outbox WHERE (destination->>'config_index')::integer=0",
        &[],
    ).await.unwrap().get(0);
    assert!(deferred, "timeout deferral must be committed, preserving the hanging route for retry");
    assert_eq!(fixture.position().await.0, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_required_stream_expired_retry_yields_to_an_older_unattempted_route() {
    use crate::database::postgres::stream_outbox;
    let fixture = Fixture::new().await;
    let streams = serde_json::from_value(json!({"delivery_guarantee":"at_least_once", "webhooks":[
        {"endpoint":"http://first.invalid", "shared_secret":"test", "networks":["ethereum"], "events":[{"event_name":"Value"}]},
        {"endpoint":"http://second.invalid", "shared_secret":"test", "networks":["ethereum"], "events":[{"event_name":"Value"}]}
    ]})).unwrap();
    let clients = Arc::new(Some(crate::streams::StreamsClients::new(streams).await));
    let mut params = fixture.params.clone();
    params.streams_clients = clients.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(fixture.events(&[10])).await.unwrap();
    let source = clients.as_ref().as_ref().unwrap().delivery_source().unwrap();
    let mut connection = fixture.client.raw_connection().await.unwrap();
    let tx = connection.transaction().await.unwrap();
    let first = stream_outbox::next(&tx, source, "ethereum", None).await.unwrap().unwrap();
    let first_id: String = first.get(0);
    stream_outbox::defer(&tx, &first_id).await.unwrap();
    tx.commit().await.unwrap();
    // Another hanging route can consume the retry interval. Expiry must not
    // restore the failed route's precedence over an untouched obligation.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let tx = connection.transaction().await.unwrap();
    let retry_due: bool = tx
        .query_one(
            "SELECT retry_after <= now() FROM rindexer_internal.stream_outbox WHERE id=$1",
            &[&first_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(retry_due, "the fairness check must include an eligible expired retry");
    let next = stream_outbox::next(&tx, source, "ethereum", None).await.unwrap().unwrap();
    let destination: serde_json::Value = next.get(1);
    tx.rollback().await.unwrap();
    drop(connection);
    drop(callback);
    drop(clients);
    fixture.finish().await;
    assert_eq!(
        destination["config_index"], 1,
        "an expired retry must not monopolize the oldest queue position"
    );
}
