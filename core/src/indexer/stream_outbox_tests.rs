//! Required delivery must survive a stopped generation without replaying applied effects.
use super::*;
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
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex as StdMutex, Weak,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct EndpointState {
    failing: AtomicBool,
    attempts: StdMutex<Vec<(String, bool)>>,
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
                     Json(_): Json<serde_json::Value>| async move {
                        let failed = state.failing.load(Ordering::SeqCst);
                        let id = headers
                            .get(crate::streams::STREAM_MESSAGE_ID_KEY)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string();
                        state.attempts.lock().unwrap().push((id, !failed));
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
    async fn stop(self) {
        self.cancel.cancel();
        for handle in self.handles {
            tokio::time::timeout(Duration::from_secs(3), handle).await.unwrap().unwrap().unwrap();
        }
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
    let mut log = fixture.events(&[10]).remove(0).log;
    log.block_number = Some(100);
    log.block_hash = Some(B256::repeat_byte(100));
    let headers = (99..=head)
        .map(|number| {
            let mut block = crate::indexer::tables::view_call_cache_tests::header(
                B256::repeat_byte(number as u8),
                number,
            );
            block.header.inner.number = number;
            block.header.inner.parent_hash = B256::repeat_byte((number - 1) as u8);
            block
        })
        .collect();
    let provider = Arc::new(
        MockChainProvider::new(1).with_block_number(head).with_blocks(headers).with_logs(vec![log]),
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
    .await
    .unwrap();
    Running { cancel, handles, coordinators, clients: weak }
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
