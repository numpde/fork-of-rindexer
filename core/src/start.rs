use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::database::clickhouse::setup::SetupClickhouseError;
use crate::database::generate::GenerateTablesForIndexerSqlError;
use crate::events::RindexerEventEmitter;
use crate::hot_reload::orchestrator::ReloadOrchestrator;
use crate::hot_reload::watcher::ManifestWatcher;
use crate::indexer::schema::PreparedSchema;
use crate::indexer::start::{start_historical_indexing_prepared, start_live_indexing_prepared};
use crate::indexer::IndexingEventsProgressState;
use crate::{
    api::{
        start_graphql_server, stop_graphql_server, GraphqlOverrideSettings, StartGraphqlServerError,
    },
    database::postgres::{
        client::PostgresConnectionError,
        indexes::{ApplyPostgresIndexesError, PostgresIndexResult},
        relationship::{ApplyAllRelationships, Relationship},
        setup::{setup_postgres_prepared, SetupPostgresError},
    },
    event::callback_registry::{EventCallbackRegistry, TraceCallbackRegistry},
    health::HealthServer,
    indexer::{
        no_code::{setup_no_code_prepared, SetupNoCodeError},
        start::StartIndexingError,
        ContractEventDependencies, ContractEventDependenciesMapFromRelationshipsError,
    },
    initiate_shutdown,
    logger::mark_shutdown_started,
    manifest::{
        core::{Manifest, ProjectType},
        storage::RelationshipsAndIndexersError,
        yaml::{read_manifest_prepared, validate_prepared_manifest, ReadManifestError},
    },
    setup_clickhouse_prepared, setup_info_logger, RindexerEventStream,
};

/// One admitted manifest and its immutable storage declarations for an indexing run.
///
/// Prepare before schema synchronization or any database writes, then retain this
/// snapshot through callback registration, historical indexing and live recovery.
#[derive(Clone)]
pub struct PreparedProject {
    manifest: Arc<Manifest>,
    schema: Arc<PreparedSchema>,
}

impl PreparedProject {
    pub fn try_new(
        project_path: &Path,
        manifest: Manifest,
    ) -> Result<Self, GenerateTablesForIndexerSqlError> {
        let schema = PreparedSchema::try_new(project_path, &manifest.to_indexer())?;
        validate_prepared_manifest(project_path, &manifest, &schema).map_err(|error| {
            GenerateTablesForIndexerSqlError::InvalidPreparedSchema(error.to_string())
        })?;
        Ok(Self::from_parts(manifest, schema))
    }

    pub(crate) fn from_parts(manifest: Manifest, schema: PreparedSchema) -> Self {
        Self { manifest: Arc::new(manifest), schema: Arc::new(schema) }
    }

    #[allow(clippy::result_large_err, reason = "preserve existing error API")]
    pub fn read(manifest_path: &Path) -> Result<Self, StartRindexerError> {
        let (manifest, schema) = read_manifest_prepared(&manifest_path.to_path_buf())?;
        Ok(Self::from_parts(manifest, schema))
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn schema(&self) -> &PreparedSchema {
        &self.schema
    }
}

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
    pub trace_registry: TraceCallbackRegistry,
    pub event_stream: Option<RindexerEventStream>,
}

pub struct StartDetails<'a> {
    pub manifest_path: &'a PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_details: GraphqlOverrideSettings,
    pub cron_scheduler_handle: Option<tokio::task::JoinHandle<Result<(), String>>>,
    pub watch: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerError {
    #[error("Could not work out project path from the parent of the manifest")]
    NoProjectPathFoundUsingParentOfManifestPath,

    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(#[from] ReadManifestError),

    #[error("Could not prepare storage schema: {0}")]
    CouldNotPrepareSchema(#[from] GenerateTablesForIndexerSqlError),

    #[error("Could not start graphql error {0}")]
    CouldNotStartGraphqlServer(#[from] StartGraphqlServerError),

    #[error("Failed to listen to graphql socket")]
    FailedToListenToGraphqlSocket,

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(#[from] SetupPostgresError),

    #[error("Could not setup clickhouse: {0}")]
    SetupClickhouseError(#[from] SetupClickhouseError),

    #[error("Could not start indexing: {0}")]
    CouldNotStartIndexing(#[from] StartIndexingError),

    #[error("{0}")]
    PostgresConnectionError(#[from] PostgresConnectionError),

    #[error("{0}")]
    ApplyRelationshipError(#[from] ApplyAllRelationships),

    #[error("Could not apply indexes: {0}")]
    ApplyPostgresIndexesError(#[from] ApplyPostgresIndexesError),

    #[error("{0}")]
    ContractEventDependenciesMapFromRelationshipsError(
        #[from] ContractEventDependenciesMapFromRelationshipsError,
    ),

    #[error("{0}")]
    RelationshipsAndIndexersError(#[from] RelationshipsAndIndexersError),

    #[error("Shutdown handler failed with error: {0}")]
    ShutdownHandlerFailed(String),

    #[error("Cron scheduler failed: {0}")]
    CronSchedulerFailed(String),

    #[error("Port conflict: {0}")]
    PortConflict(String),

    #[error("Could not start Reth node: {0}")]
    CouldNotStartRethNode(#[from] eyre::Error),

    #[error("Reth CLI error: {0}")]
    RethCliError(#[from] Box<dyn std::error::Error>),
}

async fn handle_shutdown(signal: &str) {
    // Mark shutdown state only once, at the very beginning of the shutdown process
    mark_shutdown_started();
    info!("Received {} signal gracefully shutting down...", signal);
    // Signal GraphQL server to stop its restart loop
    stop_graphql_server();
    initiate_shutdown().await;
    // These info! calls work because they're before/after the shutdown process
    info!("Graceful shutdown completed for {}", signal);
    std::process::exit(0);
}

/// Start a project by preparing its current manifest and ABI declarations.
///
/// This standalone compatibility entrypoint creates a fresh [`PreparedProject`];
/// [`StartDetails`] does not carry the snapshot used by earlier setup or schema
/// synchronization. To retain one schema across those stages, prepare a project
/// once and pass it to [`start_rindexer_prepared`]. For staged no-code startup,
/// first use [`crate::indexer::no_code::setup_no_code_prepared`] with that same
/// project; [`start_rindexer_no_code`] performs both stages in one call.
#[allow(clippy::result_large_err, reason = "preserve existing error API")]
pub async fn start_rindexer(details: StartDetails<'_>) -> Result<(), StartRindexerError> {
    let prepared = PreparedProject::read(details.manifest_path)?;
    start_rindexer_prepared(details, prepared).await
}

/// Preserve the existing bounded-cron wait as a single owner of the join
/// result. The initial implementation waits after other runtime work, just
/// like the previous bounded-only path; the supervision regression below
/// demonstrates why that ordering is insufficient for persistent services.
#[allow(clippy::result_large_err, reason = "preserve existing startup error API")]
async fn await_required_cron_after_runtime<F>(
    cron_handle: Option<tokio::task::JoinHandle<Result<(), String>>>,
    runtime: F,
) -> Result<(), StartRindexerError>
where
    F: std::future::Future<Output = Result<(), StartRindexerError>>,
{
    runtime.await?;
    if let Some(handle) = cron_handle {
        match handle.await {
            Ok(Ok(())) => info!("Cron scheduler completed"),
            Ok(Err(error)) => return Err(StartRindexerError::CronSchedulerFailed(error)),
            Err(error) => {
                return Err(StartRindexerError::CronSchedulerFailed(format!(
                    "Cron scheduler task failed to join: {error}"
                )));
            }
        }
    }
    Ok(())
}

/// Start a project using the snapshot already admitted by schema synchronization.
#[allow(clippy::result_large_err, reason = "preserve existing error API")]
pub async fn start_rindexer_prepared(
    mut details: StartDetails<'_>,
    prepared: PreparedProject,
) -> Result<(), StartRindexerError> {
    let cron_handle = details.cron_scheduler_handle.take();
    await_required_cron_after_runtime(
        cron_handle,
        start_rindexer_prepared_inner(details, prepared),
    )
    .await
}

/// All server and indexer lifecycle work is the runtime future. Cron's
/// JoinHandle is owned by the caller, not by one bounded branch within it.
#[allow(clippy::result_large_err, reason = "preserve existing error API")]
async fn start_rindexer_prepared_inner(
    details: StartDetails<'_>,
    prepared: PreparedProject,
) -> Result<(), StartRindexerError> {
    info!(
        "🚀 start_rindexer called with indexing_details.is_some() = {}",
        details.indexing_details.is_some()
    );
    let project_path = details.manifest_path.parent();

    match project_path {
        Some(project_path) => {
            #[cfg(unix)]
            let shutdown_handle = {
                let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                    .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;
                let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                    .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;
                let mut sigquit = signal::unix::signal(signal::unix::SignalKind::quit())
                    .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;

                tokio::spawn(async move {
                    tokio::select! {
                        _ = sigterm.recv() => handle_shutdown("SIGTERM").await,
                        _ = sigint.recv() => handle_shutdown("SIGINT (Ctrl+C)").await,
                        _ = sigquit.recv() => handle_shutdown("SIGQUIT").await,
                    }
                })
            };

            // On Windows, we just use Ctrl+C to trigger shutdown
            #[cfg(windows)]
            let shutdown_handle = tokio::spawn(async move {
                if let Err(e) = signal::ctrl_c().await {
                    error!("Failed to register Ctrl+C handler: {}", e);
                    panic!("Ctrl+C handler failed: {}", e);
                }
                handle_shutdown("Ctrl+C").await
            });

            let manifest = Arc::clone(&prepared.manifest);
            let schema = prepared.schema();

            if manifest.project_type != ProjectType::NoCode {
                setup_info_logger();
                info!("Starting rindexer rust project");
            }

            // Spawn a separate task for the GraphQL server if specified
            let graphql_server_handle =
                if details.graphql_details.enabled && manifest.storage.postgres_enabled() {
                    let manifest_clone = Arc::clone(&manifest);
                    let indexer = manifest_clone.to_indexer();
                    let mut graphql_settings = manifest.graphql.clone().unwrap_or_default();
                    if let Some(override_port) = &details.graphql_details.override_port {
                        graphql_settings.set_port(*override_port);
                    }
                    Some(tokio::spawn(async move {
                        if let Err(e) = start_graphql_server(&indexer, &graphql_settings).await {
                            error!("Failed to start GraphQL server: {:?}", e);
                            return;
                        }
                        // Keep the task alive - GraphQL server runs in a separate spawned task
                        // We wait here so the select! doesn't complete and exit the process
                        std::future::pending::<()>().await;
                    }))
                } else {
                    None
                };

            // Check for port conflicts between GraphQL and health servers
            let graphql_port = if details.graphql_details.enabled {
                let mut graphql_settings = manifest.graphql.clone().unwrap_or_default();
                if let Some(override_port) = &details.graphql_details.override_port {
                    graphql_settings.set_port(*override_port);
                }
                Some(graphql_settings.port)
            } else {
                None
            };

            let health_port = manifest.global.health_port;

            if let Some(graphql_port) = graphql_port {
                if graphql_port == health_port {
                    return Err(StartRindexerError::PortConflict(format!(
                        "GraphQL and health servers cannot use the same port: {}",
                        graphql_port
                    )));
                }
            }

            // Health server follows the indexer lifecycle - only runs when indexer is running
            let health_server_handle = if let Some(indexing) = details.indexing_details.as_ref() {
                let manifest_clone = Arc::clone(&manifest);
                let health_registry = indexing.registry.clone();
                let health_trace_registry = indexing.trace_registry.clone();
                let callback_failures = vec![
                    indexing.registry.failure_state(),
                    indexing.trace_registry.failure_state(),
                ];

                Some(tokio::spawn(async move {
                    info!("🩺 Starting health server on port {}", health_port);
                    let postgres_client = if manifest_clone.storage.postgres_enabled() {
                        match crate::indexer::start::initialize_database(&manifest_clone).await {
                            Ok(Some(client)) => Some(client),
                            Ok(None) => {
                                error!("PostgreSQL is enabled but no database client was created for health server");
                                None
                            }
                            Err(e) => {
                                error!("Failed to initialize database for health server: {:?}", e);
                                None
                            }
                        }
                    } else {
                        None
                    };

                    if let Err(e) = HealthServer::new(health_port, manifest_clone, postgres_client)
                        .with_callback_failures(callback_failures)
                        .with_event_registry(&health_registry)
                        .with_trace_registry(&health_trace_registry)
                        .start()
                        .await
                    {
                        error!("Failed to start health server: {:?}", e);
                    }
                }))
            } else {
                None
            };

            if graphql_server_handle.is_none() && details.graphql_details.enabled {
                error!("GraphQL can not run without postgres storage enabled, you have tried to run GraphQL which will now be skipped.");
            }

            if let Some(mut indexing_details) = details.indexing_details {
                let postgres_enabled = &manifest.storage.postgres_enabled();
                let clickhouse_enabled = &manifest.storage.clickhouse_enabled();

                // setup postgres is already called in no-code startup
                if manifest.project_type != ProjectType::NoCode && *postgres_enabled {
                    setup_postgres_prepared(project_path, &manifest, schema).await?;
                }

                // setup clickhouse is already called in no-code startup
                if manifest.project_type != ProjectType::NoCode && *clickhouse_enabled {
                    setup_clickhouse_prepared(project_path, &manifest, schema).await?;
                }

                let (relationships, postgres_indexes) =
                    manifest.storage.create_prepared_relationships_and_indexes(schema).await?;

                let mut dependencies: Vec<ContractEventDependencies> =
                    ContractEventDependencies::parse(&manifest);

                // CancellationToken for this indexing generation.
                // When --watch mode is enabled, the orchestrator will cancel this token
                // to trigger a graceful reload. Without --watch, this token is never cancelled.
                let cancel_token = CancellationToken::new();

                let event_emitter =
                    indexing_details.event_stream.map(RindexerEventEmitter::from_stream);

                let historical_registry = indexing_details.registry.complete();
                let trace_registry = indexing_details.trace_registry.complete();

                let progress = IndexingEventsProgressState::monitor(
                    &historical_registry.events,
                    &trace_registry.events,
                    event_emitter.clone(),
                )
                .await;

                let processed_network_contracts = start_historical_indexing_prepared(
                    &manifest,
                    project_path,
                    schema,
                    &dependencies,
                    historical_registry,
                    trace_registry,
                    cancel_token.clone(),
                    progress.clone(),
                )
                .await?;

                if let Some(ref emitter) = event_emitter {
                    emitter.emit(crate::RindexerEvent::HistoricalIndexingCompleted);
                }

                // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we need to handle this
                PostgresIndexResult::apply_indexes(postgres_indexes).await?;

                if !relationships.is_empty() {
                    // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we
                    // need to handle this
                    info!("Applying constraints relationships back to the database as historic resync is complete");
                    Relationship::apply_all(&relationships).await?;
                }

                // Spawn hot-reload watcher and orchestrator BEFORE live indexing
                // (live indexing blocks forever, so anything after it won't execute)
                if details.watch {
                    let manifest_path_owned = details.manifest_path.clone();
                    let (reload_tx, reload_rx) = tokio::sync::mpsc::channel::<PathBuf>(4);

                    // Spawn the file watcher
                    let watcher = ManifestWatcher::new(manifest_path_owned.clone(), reload_tx);
                    tokio::spawn(async move {
                        if let Err(e) = watcher.run().await {
                            error!("Hot-reload: file watcher error: {}", e);
                        }
                    });

                    // Spawn the reload orchestrator
                    let orchestrator_shutdown = CancellationToken::new();
                    let mut orchestrator = ReloadOrchestrator::new(
                        manifest_path_owned,
                        project_path.to_path_buf(),
                        Arc::clone(&manifest),
                        reload_rx,
                        cancel_token.clone(),
                    );
                    let shutdown_token = orchestrator_shutdown.clone();
                    tokio::spawn(async move {
                        orchestrator.run(shutdown_token).await;
                    });

                    info!("Hot-reload: watching rindexer.yaml for changes");
                }

                if manifest.has_any_live_indexing() {
                    if dependencies.is_empty() {
                        dependencies =
                            ContractEventDependencies::map_from_relationships(&relationships)?;
                    } else {
                        info!("Manual dependency_events found, skipping auto-applying the dependency_events with the relationships");
                    }

                    start_live_indexing_prepared(
                        &manifest,
                        project_path,
                        schema,
                        &dependencies,
                        indexing_details
                            .registry
                            .reapply_after_historic(processed_network_contracts),
                        indexing_details.trace_registry.complete(),
                        cancel_token.clone(),
                        progress,
                    )
                    .await
                    .map_err(StartRindexerError::CouldNotStartIndexing)?;
                }
            }

            if graphql_server_handle.is_none() && !manifest.has_any_live_indexing() {
                // The outer owner waits for required cron after bounded runtime
                // completion. Persistent services still expose its blind spot.
                return Ok(());
            }

            match (graphql_server_handle, health_server_handle, shutdown_handle) {
                (Some(graphql_handle), Some(health_handle), shutdown_handle) => {
                    info!("Waiting on GraphQL server, health server, and shutdown signal...");
                    tokio::select! {
                        result = graphql_handle => {
                            if let Err(e) = result {
                                error!("GraphQL server task failed: {:?}", e);
                            }
                        }
                        result = health_handle => {
                            if let Err(e) = result {
                                error!("Health server task failed: {:?}", e);
                            }
                        }
                        result = shutdown_handle => {
                            result.map_err(|e| {
                                error!("Shutdown handler failed: {:?}", e);
                                StartRindexerError::ShutdownHandlerFailed(e.to_string())
                            })?;
                        }
                    }
                }
                (Some(graphql_handle), None, shutdown_handle) => {
                    info!("Waiting on GraphQL server and shutdown signal...");
                    tokio::select! {
                        result = graphql_handle => {
                            if let Err(e) = result {
                                error!("GraphQL server task failed: {:?}", e);
                            }
                        }
                        result = shutdown_handle => {
                            result.map_err(|e| {
                                error!("Shutdown handler failed: {:?}", e);
                                StartRindexerError::ShutdownHandlerFailed(e.to_string())
                            })?;
                        }
                    }
                }
                (None, Some(health_handle), shutdown_handle) => {
                    info!("Waiting on health server and shutdown signal...");
                    tokio::select! {
                        result = health_handle => {
                            if let Err(e) = result {
                                error!("Health server task failed: {:?}", e);
                            }
                        }
                        result = shutdown_handle => {
                            result.map_err(|e| {
                                error!("Shutdown handler failed: {:?}", e);
                                StartRindexerError::ShutdownHandlerFailed(e.to_string())
                            })?;
                        }
                    }
                }
                (None, None, shutdown_handle) => {
                    info!("Waiting for shutdown signal...");
                    shutdown_handle.await.map_err(|e| {
                        error!("Shutdown handler failed: {:?}", e);
                        StartRindexerError::ShutdownHandlerFailed(e.to_string())
                    })?;
                }
            }

            Ok(())
        }
        None => Err(StartRindexerError::NoProjectPathFoundUsingParentOfManifestPath),
    }
}

pub struct IndexerNoCodeDetails {
    pub enabled: bool,
}

pub struct StartNoCodeDetails<'a> {
    pub manifest_path: &'a PathBuf,
    pub indexing_details: IndexerNoCodeDetails,
    pub graphql_details: GraphqlOverrideSettings,
    pub watch: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerNoCode {
    #[error("{0}")]
    StartRindexerError(#[from] StartRindexerError),

    #[error("{0}")]
    SetupNoCodeError(#[from] SetupNoCodeError),
}

#[allow(clippy::result_large_err, reason = "preserve existing error API")]
pub async fn start_rindexer_no_code(
    details: StartNoCodeDetails<'_>,
) -> Result<(), StartRindexerNoCode> {
    let prepared = PreparedProject::read(details.manifest_path)?;
    start_rindexer_no_code_prepared(details, prepared).await
}

/// Share one prepared schema between no-code setup and both indexing phases.
#[allow(clippy::result_large_err, reason = "preserve existing error API")]
pub async fn start_rindexer_no_code_prepared(
    details: StartNoCodeDetails<'_>,
    prepared: PreparedProject,
) -> Result<(), StartRindexerNoCode> {
    let start_details = setup_no_code_prepared(details, &prepared).await?;

    start_rindexer_prepared(start_details, prepared)
        .await
        .map_err(StartRindexerNoCode::StartRindexerError)
}

#[cfg(test)]
#[path = "start_cron_failure_tests.rs"]
mod start_cron_failure_tests;
