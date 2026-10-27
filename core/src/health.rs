use std::{net::SocketAddr, sync::Arc};

use axum::{extract::State, http::StatusCode, response::Json, routing::get, Extension, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::{
    database::postgres::client::PostgresClient,
    event::callback_registry::{
        CallbackFailures, EventCallbackRegistry, FailedStream, TraceCallbackRegistry,
    },
    indexer::task_tracker::active_indexing_count,
    manifest::core::Manifest,
    metrics::metrics_handler,
    system_state::{get_reload_state, is_running, ReloadState},
};

#[path = "health_subscriptions.rs"]
mod subscriptions;
use subscriptions::SubscriptionSource;
pub use subscriptions::{SubscriptionHealth, SubscriptionIdentity, SubscriptionStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: HealthStatusType,
    pub timestamp: String,
    pub services: HealthServices,
    pub indexing: IndexingStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthServices {
    pub database: HealthStatusType,
    pub indexing: HealthStatusType,
    pub sync: HealthStatusType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingStatus {
    pub active_tasks: usize,
    pub is_running: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_streams: Vec<FailedStream>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<SubscriptionHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatusType {
    Healthy,
    Syncing,
    Unhealthy,
    Unknown,
    NotConfigured,
    Disabled,
    NoData,
    Stopped,
    Reloading,
}

#[derive(Clone)]
pub struct HealthServerState {
    pub manifest: Arc<Manifest>,
    pub postgres_client: Option<Arc<PostgresClient>>,
}

pub struct HealthServer {
    port: u16,
    state: HealthServerState,
    callback_failures: Vec<Arc<CallbackFailures>>,
    subscriptions: Vec<SubscriptionSource>,
}

#[derive(Clone)]
struct CallbackFailureSources(Vec<Arc<CallbackFailures>>);

#[derive(Clone)]
struct SubscriptionSources(Vec<SubscriptionSource>);

impl HealthServer {
    pub fn new(
        port: u16,
        manifest: Arc<Manifest>,
        postgres_client: Option<Arc<PostgresClient>>,
    ) -> Self {
        Self {
            port,
            state: HealthServerState { manifest, postgres_client },
            callback_failures: vec![],
            subscriptions: vec![],
        }
    }

    pub(crate) fn with_callback_failures(mut self, failures: Vec<Arc<CallbackFailures>>) -> Self {
        self.callback_failures = failures;
        self
    }

    pub fn with_event_registry(mut self, registry: &EventCallbackRegistry) -> Self {
        self.subscriptions.extend(subscriptions::event_sources(registry));
        let failures = registry.failure_state();
        if !self.callback_failures.iter().any(|source| Arc::ptr_eq(source, &failures)) {
            self.callback_failures.push(failures);
        }
        self
    }

    pub fn with_trace_registry(mut self, registry: &TraceCallbackRegistry) -> Self {
        self.subscriptions.extend(subscriptions::trace_sources(registry));
        let failures = registry.failure_state();
        if !self.callback_failures.iter().any(|source| Arc::ptr_eq(source, &failures)) {
            self.callback_failures.push(failures);
        }
        self
    }

    pub(crate) fn router(self) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route("/metrics", get(metrics_handler))
            .layer(Extension(CallbackFailureSources(self.callback_failures)))
            .layer(Extension(SubscriptionSources(self.subscriptions)))
            .with_state(self.state)
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let port = self.port;
        let app = self.router();
        let listener = TcpListener::bind(addr).await?;

        info!("🩺 Health server started on http://0.0.0.0:{}/health", port);
        info!("📊 Metrics available at http://0.0.0.0:{}/metrics", port);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_handler(
    State(state): State<HealthServerState>,
    Extension(failures): Extension<CallbackFailureSources>,
    Extension(subscriptions): Extension<SubscriptionSources>,
) -> Result<(StatusCode, Json<HealthStatus>), StatusCode> {
    let database_health = check_database_health(&state).await;
    let failed_streams = failures.0.iter().flat_map(|source| source.snapshot()).collect::<Vec<_>>();
    let indexing_health = check_indexing_health(&failed_streams);
    let (sync_health, subscription_health) =
        check_sync_health(&state, &subscriptions.0, &failed_streams).await;

    let overall_status = determine_overall_status(&database_health, &indexing_health, &sync_health);

    let mut health_status = build_health_status(
        overall_status,
        database_health,
        indexing_health,
        sync_health,
        failed_streams,
    );

    health_status.indexing.subscriptions = subscription_health;

    let status_code = if matches!(health_status.status, HealthStatusType::Healthy) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((status_code, Json(health_status)))
}

fn build_health_status(
    overall_status: HealthStatusType,
    database_health: HealthStatusType,
    indexing_health: HealthStatusType,
    sync_health: HealthStatusType,
    failed_streams: Vec<FailedStream>,
) -> HealthStatus {
    HealthStatus {
        status: overall_status,
        timestamp: chrono::Utc::now().to_rfc3339(),
        services: HealthServices {
            database: database_health,
            indexing: indexing_health,
            sync: sync_health,
        },
        indexing: IndexingStatus {
            active_tasks: active_indexing_count(),
            is_running: is_running(),
            failed_streams,
            subscriptions: vec![],
        },
    }
}

async fn check_database_health(state: &HealthServerState) -> HealthStatusType {
    if !state.manifest.storage.postgres_enabled() {
        return HealthStatusType::Disabled;
    }

    match &state.postgres_client {
        Some(client) => match client.query_one("SELECT 1", &[]).await {
            Ok(_) => HealthStatusType::Healthy,
            Err(e) => {
                error!("Database health check failed: {}", e);
                HealthStatusType::Unhealthy
            }
        },
        None => HealthStatusType::NotConfigured,
    }
}

fn check_indexing_health(failed_streams: &[FailedStream]) -> HealthStatusType {
    if !failed_streams.is_empty() {
        return HealthStatusType::Unhealthy;
    }
    if matches!(get_reload_state(), ReloadState::Reloading) {
        return HealthStatusType::Reloading;
    }
    if is_running() {
        HealthStatusType::Healthy
    } else {
        HealthStatusType::Stopped
    }
}

async fn check_sync_health(
    state: &HealthServerState,
    subscriptions: &[SubscriptionSource],
    failures: &[FailedStream],
) -> (HealthStatusType, Vec<SubscriptionHealth>) {
    if state.manifest.storage.postgres_enabled() {
        if subscriptions.is_empty() {
            let configured = !state.manifest.all_contracts().is_empty()
                || state.manifest.native_transfers.enabled;
            return (
                if configured {
                    HealthStatusType::NotConfigured
                } else {
                    HealthStatusType::Disabled
                },
                vec![],
            );
        }
        subscriptions::snapshot(state.postgres_client.as_deref(), subscriptions, failures).await
    } else if state.manifest.storage.csv_enabled() {
        (check_csv_sync_health(state), vec![])
    } else {
        (HealthStatusType::Disabled, vec![])
    }
}

fn check_csv_sync_health(state: &HealthServerState) -> HealthStatusType {
    match &state.manifest.storage.csv {
        Some(csv_details) => {
            let csv_path = std::path::Path::new(&csv_details.path);
            if !csv_path.exists() {
                return HealthStatusType::NoData;
            }

            match std::fs::read_dir(csv_path) {
                Ok(entries) => {
                    let csv_files: Vec<_> = entries
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "csv"))
                        .collect();

                    if csv_files.is_empty() {
                        HealthStatusType::NoData
                    } else {
                        HealthStatusType::Healthy
                    }
                }
                Err(_) => HealthStatusType::Unhealthy,
            }
        }
        None => HealthStatusType::NotConfigured,
    }
}

fn determine_overall_status(
    database: &HealthStatusType,
    indexing: &HealthStatusType,
    sync: &HealthStatusType,
) -> HealthStatusType {
    if matches!(database, HealthStatusType::Unhealthy | HealthStatusType::NotConfigured)
        || matches!(indexing, HealthStatusType::Stopped | HealthStatusType::Unhealthy)
        || matches!(sync, HealthStatusType::Unhealthy | HealthStatusType::NotConfigured)
    {
        HealthStatusType::Unhealthy
    } else if matches!(sync, HealthStatusType::NoData) {
        // Sync NoData is acceptable when no event tables exist yet
        HealthStatusType::Healthy
    } else {
        HealthStatusType::Healthy
    }
}

pub async fn start_health_server(
    port: u16,
    manifest: Arc<Manifest>,
    postgres_client: Option<Arc<PostgresClient>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let health_server = HealthServer::new(port, manifest, postgres_client);
    health_server.start().await
}
