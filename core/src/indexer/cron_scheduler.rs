//! Cron scheduler for table operations.
//!
//! This module provides scheduled (cron) triggers for custom tables,
//! allowing operations to run on a time-based schedule instead of
//! (or in addition to) event triggers.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::event::read_factory_addresses_from_event;
use crate::indexer::last_synced::{
    advance_postgres_cron_checkpoint, get_last_synced_cron_block, lock_postgres_cron_checkpoint,
    update_last_synced_clickhouse_cron_block, CronCheckpointLockError, CronSyncConfig,
};
use crate::indexer::schema::PreparedSchema;
use crate::is_running;
use crate::manifest::contract::{
    parse_interval, ColumnType, Contract, FactoryDetailsYaml, OperationType, ResolvedTable,
    SetAction, TableCronMapping,
};
use crate::manifest::core::Manifest;
use crate::provider::ChainProvider;
use alloy::primitives::U64;

use super::tables::{TableRowData, TableRuntime, TxMetadata};

// ============================================================================
// Adaptive Rate Limiter - Scales both concurrency AND batch size
// ============================================================================

// Bounded historical-cron recovery. Retry only pre-effect lock acquisition
// and preparation deadlines; uncertain COMMIT still requires reconciliation.
const HISTORICAL_RPC_PREPARATION_TIMEOUT: Duration = Duration::from_secs(60);
const HISTORICAL_RPC_PREPARATION_RETRIES: usize = 1;
const HISTORICAL_CHECKPOINT_LOCK_RETRIES: usize = 2;
const HISTORICAL_DB_CONNECTION_TIMEOUT: Duration = Duration::from_secs(12);
const HISTORICAL_COMMIT_REPLY_TIMEOUT: Duration = Duration::from_secs(20);
const HISTORICAL_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(15);

/// The existing global shutdown state is the single owner of cancellation.
/// There is no second cron shutdown flag or watch channel.
async fn wait_for_historical_cron_shutdown() {
    while is_running() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Maximum concurrent RPC calls. Paid nodes handle 100+ easily.
const MAX_RPC_CONCURRENCY: usize = 100;

/// Starting concurrent RPC calls. Start tiny so free nodes can complete.
const INITIAL_RPC_CONCURRENCY: usize = 2;

/// Minimum concurrent RPC calls.
const MIN_RPC_CONCURRENCY: usize = 1;

/// Maximum batch size (blocks per batch). Enterprise nodes work great at 1000+.
const MAX_BATCH_SIZE: u64 = 1000;

/// Starting batch size. Start small so free nodes complete quickly.
const INITIAL_BATCH_SIZE: u64 = 50;

/// Minimum batch size.
const MIN_BATCH_SIZE: u64 = 25;

/// Delay between batches when rate limited (milliseconds).
const RATE_LIMIT_DELAY_MS: u64 = 1000;

/// Expected average RPC call latency in milliseconds (for healthy nodes).
/// Used to detect rate limiting by timing.
const EXPECTED_CALL_LATENCY_MS: u64 = 150;

/// If batch takes longer than this multiplier of expected time, it's rate limited.
const RATE_LIMIT_TIME_MULTIPLIER: f64 = 3.0;

/// Number of successful batches required before scaling up after a rate limit event.
/// Prevents oscillation between scaling up and down.
const COOLDOWN_BATCHES_AFTER_RATE_LIMIT: u32 = 5;

/// Adaptive rate limiter that adjusts BOTH concurrency AND batch size.
///
/// Detects rate limiting by TIMING - if a batch takes much longer than expected
/// (based on batch_size / concurrency * expected_latency), we're being throttled.
/// This works even when the provider retries internally and eventually succeeds.
pub struct AdaptiveRateLimiter {
    /// Current concurrency level.
    concurrency: AtomicU32,
    /// Current batch size.
    batch_size: AtomicU64,
    /// Whether we've logged the initial throttle warning.
    logged_throttle_warning: AtomicU32,
    /// Cooldown counter - must be 0 before we can scale up again.
    /// Prevents oscillation after rate limiting.
    cooldown_remaining: AtomicU32,
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter starting small.
    pub fn new() -> Self {
        Self {
            concurrency: AtomicU32::new(INITIAL_RPC_CONCURRENCY as u32),
            batch_size: AtomicU64::new(INITIAL_BATCH_SIZE),
            logged_throttle_warning: AtomicU32::new(0),
            cooldown_remaining: AtomicU32::new(0),
        }
    }

    /// Get current concurrency level.
    pub fn concurrency(&self) -> usize {
        self.concurrency.load(Ordering::Relaxed) as usize
    }

    /// Get current batch size.
    pub fn batch_size(&self) -> u64 {
        self.batch_size.load(Ordering::Relaxed)
    }

    /// Call after each batch completes with the actual duration.
    /// Detects rate limiting by timing - if batch took much longer than expected,
    /// we're being throttled (even if provider retried internally and succeeded).
    /// Returns delay before next batch.
    pub fn after_batch_with_duration(&self, actual_duration: Duration) -> Duration {
        let current_conc = self.concurrency.load(Ordering::Relaxed) as usize;
        let current_batch = self.batch_size.load(Ordering::Relaxed);

        // Calculate expected duration: (batch_size / concurrency) * call_latency
        // This is how many "waves" of concurrent calls times the latency per wave
        let waves = (current_batch as f64 / current_conc as f64).ceil();
        let expected_ms = waves * EXPECTED_CALL_LATENCY_MS as f64;
        let actual_ms = actual_duration.as_millis() as f64;

        // If actual time is much greater than expected, we're rate limited
        let is_rate_limited = actual_ms > expected_ms * RATE_LIMIT_TIME_MULTIPLIER;

        if is_rate_limited {
            // Rate limited - scale DOWN both concurrency and batch size
            let new_conc = (current_conc / 2).max(MIN_RPC_CONCURRENCY);
            let new_batch = (current_batch / 2).max(MIN_BATCH_SIZE);

            self.concurrency.store(new_conc as u32, Ordering::Relaxed);
            self.batch_size.store(new_batch, Ordering::Relaxed);

            // Set cooldown - require N successful batches before scaling up again
            self.cooldown_remaining.store(COOLDOWN_BATCHES_AFTER_RATE_LIMIT, Ordering::Relaxed);

            // Only log the first throttle warning with full context
            if self.logged_throttle_warning.swap(1, Ordering::Relaxed) == 0 {
                warn!(
                    "⚠️  RPC rate limited (batch took {:.1}s, expected {:.1}s) - scaling down to concurrency={}, batch_size={} (free nodes throttle, paid nodes scale up)",
                    actual_ms / 1000.0, expected_ms / 1000.0, new_conc, new_batch
                );
            } else {
                warn!(
                    "⚠️  Rate limited ({:.1}s vs {:.1}s expected) - concurrency={}, batch_size={} (cooldown {} batches)",
                    actual_ms / 1000.0, expected_ms / 1000.0, new_conc, new_batch, COOLDOWN_BATCHES_AFTER_RATE_LIMIT
                );
            }

            Duration::from_millis(RATE_LIMIT_DELAY_MS)
        } else {
            // Successful batch - check cooldown before scaling up
            let cooldown = self.cooldown_remaining.load(Ordering::Relaxed);

            if cooldown > 0 {
                // Still in cooldown period - don't scale up yet
                self.cooldown_remaining.store(cooldown - 1, Ordering::Relaxed);
                return Duration::ZERO;
            }

            // Cooldown complete - can scale up
            let at_max_conc = current_conc >= MAX_RPC_CONCURRENCY;
            let at_max_batch = current_batch >= MAX_BATCH_SIZE;

            if !at_max_conc || !at_max_batch {
                // Double both
                let new_conc = if !at_max_conc {
                    (current_conc * 2).min(MAX_RPC_CONCURRENCY)
                } else {
                    current_conc
                };
                let new_batch = if !at_max_batch {
                    (current_batch * 2).min(MAX_BATCH_SIZE)
                } else {
                    current_batch
                };

                self.concurrency.store(new_conc as u32, Ordering::Relaxed);
                self.batch_size.store(new_batch, Ordering::Relaxed);

                if new_conc == MAX_RPC_CONCURRENCY && new_batch == MAX_BATCH_SIZE {
                    info!(
                        "🚀 Reached maximum speed: concurrency={}, batch_size={}",
                        new_conc, new_batch
                    );
                }
            }

            Duration::ZERO
        }
    }
}

impl Default for AdaptiveRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Cron Scheduler Types
// ============================================================================

/// Enum representing the type of schedule for cron tasks.
#[derive(Debug, Clone)]
pub enum CronSchedule {
    /// Fixed interval (e.g., every 5 minutes)
    Interval(Duration),
    /// Cron expression (e.g., "*/5 * * * *")
    Cron(Box<croner::Cron>),
}

/// Factory configuration for cron tasks that operate on factory-indexed contracts.
#[derive(Debug, Clone)]
pub struct FactoryCronConfig {
    event_table: String,
    address_column: String,
}

impl FactoryCronConfig {
    fn from_prepared(
        schema: &PreparedSchema,
        factory: &FactoryDetailsYaml,
    ) -> Result<Self, String> {
        let contract = schema.contract(&factory.name)?;
        let event =
            contract.events.iter().find(|event| event.info.name == factory.event_name).ok_or_else(
                || format!("Unknown factory event {}::{}", factory.name, factory.event_name),
            )?;
        let event_table = event.raw_table.clone().ok_or_else(|| {
            format!("Factory event {}::{} has no raw storage", factory.name, factory.event_name)
        })?;
        let inputs = factory.input_names();
        let input = inputs.first().ok_or_else(|| {
            format!("Factory event {}::{} has no address input", factory.name, factory.event_name)
        })?;
        let (_, address_column) = event.columns.storage_column(input).ok_or_else(|| {
            format!(
                "No stored address input {input:?} in factory event {}::{}",
                factory.name, factory.event_name
            )
        })?;
        Ok(Self { event_table, address_column: address_column.to_string() })
    }
}

/// A single cron task configuration
#[derive(Debug, Clone)]
pub struct CronTask {
    pub contract_name: String,
    pub table: ResolvedTable,
    pub full_table_name: String,
    pub cron_entry: TableCronMapping,
    pub cron_index: usize,
    pub network: String,
    /// Static contract address (Address::ZERO if using factory)
    pub contract_address: Address,
    pub schedule: CronSchedule,
    pub indexer_name: String,
    /// Factory configuration (if contract uses factory indexing)
    pub factory_config: Option<FactoryCronConfig>,
    #[cfg(test)]
    pub(super) batch_size_for_test: Option<usize>,
    #[cfg(test)]
    pub(super) prepare_timeout_for_test: Option<Duration>,
}

/// The cron scheduler manages and executes scheduled table operations.
pub struct CronScheduler {
    pub tasks: Vec<CronTask>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    pub providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    /// Manifest indexer name.
    pub indexer_name: String,
}

/// Validate the narrow durable PostgreSQL cron profile against every
/// prepared table. Production database setup and scheduler construction call
/// the same validator; SQL generation alone is not admission.
pub(crate) fn validate_postgres_cron_targets<'a>(
    contracts: impl IntoIterator<Item = (&'a Contract, &'a [TableRuntime])>,
) -> Result<(), String> {
    let mut destinations: HashMap<String, (String, bool)> = HashMap::new();
    for (contract, runtimes) in contracts {
        for runtime in runtimes {
            let table = runtime.table.definition();
            let historical = table.cron.iter().flatten().any(|entry| entry.start_block.is_some());
            let destination = runtime
                .full_table_name
                .split('.')
                .map(|part| crate::helpers::postgres_catalog_identifier(part).to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            let owner = format!("{}::{}", contract.name, table.name);
            if let Some((other, other_historical)) = destinations.get(&destination) {
                if historical || *other_historical {
                    return Err(format!(
                        "Historical PostgreSQL cron destination {destination} is shared by {other} and {owner}; separate checkpoints cannot coordinate writers"
                    ));
                }
            } else {
                destinations.insert(destination, (owner.clone(), historical));
            }
            if !historical {
                continue;
            }
            let context = format!("Historical PostgreSQL cron {owner}");
            if table.cross_chain {
                return Err(format!(
                    "{context}: cross_chain keys overlap independent network checkpoints"
                ));
            }
            if !table.events.is_empty()
                || table.cron.as_ref().is_none_or(|entries| entries.len() != 1)
            {
                return Err(format!(
                    "{context}: the destination must have exactly one cron writer and no event writer"
                ));
            }
            if table.timestamp {
                return Err(format!(
                    "{context}: canonical historical timestamps are not yet supported"
                ));
            }
            for entry in table.cron.iter().flatten().filter(|entry| entry.start_block.is_some()) {
                if entry.end_block.is_none() {
                    return Err(format!(
                        "{context}: a fixed end_block is required until live cron is durable"
                    ));
                }
                if entry.operations.is_empty() {
                    return Err(format!(
                        "{context}: at least one operation is required for durable historical cron"
                    ));
                }
                let uses_synthetic_metadata = entry.operations.iter().any(|operation| {
                    operation
                        .where_clause
                        .values()
                        .map(String::as_str)
                        .chain(operation.set.iter().map(|column| column.effective_value()))
                        .any(|value| {
                            [
                                "$rindexer_timestamp",
                                "$rindexer_block_timestamp",
                                "$rindexer_tx_hash",
                                "$rindexer_block_hash",
                            ]
                            .iter()
                            .any(|reference| value.contains(reference))
                        })
                });
                if uses_synthetic_metadata {
                    return Err(format!(
                        "{context}: synthetic historical provenance cannot be used as an input"
                    ));
                }
                let networks: Vec<&str> = entry.network.as_ref().map_or_else(
                    || contract.details.iter().map(|detail| detail.network.as_str()).collect(),
                    |network| vec![network.as_str()],
                );
                for network in networks {
                    let matching = contract
                        .details
                        .iter()
                        .filter(|detail| detail.network == network)
                        .collect::<Vec<_>>();
                    if matching.len() != 1 {
                        return Err(format!(
                            "{context}: network {network:?} requires exactly one contract details entry, found {}; multi-address sampling is not admitted",
                            matching.len()
                        ));
                    }
                    let detail = matching[0];
                    if detail.factory.is_some() {
                        return Err(format!(
                            "{context}: factory discovery completeness is not yet supported"
                        ));
                    }
                    if !matches!(
                        detail.address.as_ref(),
                        Some(alloy::rpc::types::ValueOrArray::Value(address))
                            if *address != Address::ZERO
                    ) {
                        return Err(format!(
                            "{context}: exactly one nonzero static address is required"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

impl CronScheduler {
    /// Create a new cron scheduler from the manifest.
    ///
    /// This scans all contracts and tables for cron configurations and
    /// builds a list of tasks to execute. Factory-backed tasks need their
    /// admitted discovery ABI; use [`Self::try_new_prepared`] for those.
    pub fn try_new(
        manifest: &Manifest,
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
        providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    ) -> Result<Self, String> {
        let contracts = manifest
            .all_contracts()
            .into_iter()
            .map(|contract| {
                let tables = contract
                    .tables
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|table| TableRuntime::try_new(table, &manifest.name, &contract.name))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((contract, tables))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Self::from_resolved_tables(
            &manifest.name,
            None,
            contracts.iter().map(|(contract, tables)| (contract, tables.as_slice())),
            postgres,
            clickhouse,
            providers,
        )
    }

    /// Reuse the tables prepared for event writes, schema creation and recovery.
    pub fn try_new_prepared(
        schema: &PreparedSchema,
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
        providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    ) -> Result<Self, String> {
        Self::from_resolved_tables(
            &schema.indexer().name,
            Some(schema),
            schema
                .contracts()
                .iter()
                .chain(schema.native())
                .map(|contract| (&contract.declaration, contract.tables.as_slice())),
            postgres,
            clickhouse,
            providers,
        )
    }

    fn from_resolved_tables<'a>(
        indexer_name: &str,
        schema: Option<&PreparedSchema>,
        contracts: impl IntoIterator<Item = (&'a Contract, &'a [TableRuntime])>,
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
        providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    ) -> Result<Self, String> {
        let contracts = contracts.into_iter().collect::<Vec<_>>();
        if postgres.is_some() {
            validate_postgres_cron_targets(contracts.iter().copied())?;
        }
        let mut tasks = Vec::new();

        for (contract, tables) in contracts {
            for runtime in tables.iter().filter(|runtime| runtime.table.definition().has_cron()) {
                let table = &runtime.table;
                let cron_entries = table.cron_mappings();

                for (cron_index, cron_entry) in cron_entries.iter().enumerate() {
                    // Determine which networks to run on
                    let networks: Vec<String> = if let Some(network) = &cron_entry.network {
                        vec![network.clone()]
                    } else {
                        contract.details.iter().map(|d| d.network.clone()).collect()
                    };

                    // Parse the schedule (only required for live cron, not historical sync)
                    // Historical sync with start_block doesn't need interval/schedule
                    let schedule = if cron_entry.start_block.is_some()
                        && cron_entry.interval.is_none()
                        && cron_entry.schedule.is_none()
                    {
                        // Historical sync only - use a dummy schedule (won't be used for live)
                        CronSchedule::Interval(Duration::from_secs(60))
                    } else if let Some(interval_str) = &cron_entry.interval {
                        match parse_interval(interval_str) {
                            Ok(duration) => CronSchedule::Interval(duration),
                            Err(e) => {
                                error!(
                                    "Invalid interval '{}' for table '{}': {}",
                                    interval_str,
                                    table.name(),
                                    e
                                );
                                continue;
                            }
                        }
                    } else if let Some(cron_expr) = &cron_entry.schedule {
                        match cron_expr.parse::<croner::Cron>() {
                            Ok(cron) => CronSchedule::Cron(Box::new(cron)),
                            Err(e) => {
                                error!(
                                    "Invalid cron expression '{}' for table '{}': {}",
                                    cron_expr,
                                    table.name(),
                                    e
                                );
                                continue;
                            }
                        }
                    } else {
                        error!(
                            "Cron entry for table '{}' has neither interval nor schedule",
                            table.name()
                        );
                        continue;
                    };

                    for network in networks {
                        // Find the contract details for this network
                        let detail = contract.details.iter().find(|d| d.network == network);

                        if postgres.is_some()
                            && clickhouse.is_some()
                            && cron_entry.start_block.is_some()
                        {
                            return Err(format!(
                                "Historical PostgreSQL cron {}.{} on network {network}: mixed PostgreSQL/ClickHouse sinks are not atomic",
                                contract.name,
                                table.name()
                            ));
                        }

                        // Check if this is a factory-indexed contract
                        let (contract_address, factory_config) = if let Some(factory) =
                            detail.and_then(|d| d.factory.as_ref())
                        {
                            // Factory contract - use Address::ZERO as sentinel
                            let schema = schema.ok_or_else(|| format!(
                                    "Factory cron on {}.{} requires its discovery ABI; use CronScheduler::try_new_prepared with a PreparedProject",
                                    contract.name, table.name()
                                ))?;
                            let config = FactoryCronConfig::from_prepared(schema, factory)?;
                            (Address::ZERO, Some(config))
                        } else {
                            // Static address contract
                            let addr = detail
                                .and_then(|d| d.address.as_ref())
                                .and_then(|addr| match addr {
                                    alloy::rpc::types::ValueOrArray::Value(a) => Some(*a),
                                    alloy::rpc::types::ValueOrArray::Array(arr) => {
                                        arr.first().copied()
                                    }
                                })
                                .unwrap_or_default();
                            (addr, None)
                        };

                        let full_table_name = runtime.full_table_name.clone();

                        if factory_config.is_some() {
                            info!(
                                "Cron task for table '{}' on network '{}' uses factory indexing",
                                table.name(),
                                network
                            );
                        }

                        tasks.push(CronTask {
                            contract_name: runtime.contract_name.clone(),
                            table: table.clone(),
                            full_table_name,
                            cron_entry: cron_entry.clone(),
                            cron_index,
                            network,
                            contract_address,
                            schedule: schedule.clone(),
                            indexer_name: indexer_name.to_string(),
                            factory_config,
                            #[cfg(test)]
                            batch_size_for_test: None,
                            #[cfg(test)]
                            prepare_timeout_for_test: None,
                        });
                    }
                }
            }
        }

        info!("Cron scheduler initialized with {} tasks", tasks.len());

        Ok(Self { tasks, postgres, clickhouse, providers, indexer_name: indexer_name.to_string() })
    }

    /// Check if there are any cron tasks to run.
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Start the cron scheduler - runs until shutdown.
    ///
    /// This spawns a task for each cron configuration and waits for all to complete.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.tasks.is_empty() {
            info!("No cron tasks configured, scheduler exiting");
            return Ok(());
        }

        let postgres = self.postgres;
        let clickhouse = self.clickhouse;
        let providers = self.providers;

        // A detached worker can fail while another keeps running forever.
        // JoinSet aborts the other workers when a terminal error is returned.
        let mut workers = tokio::task::JoinSet::new();

        for task in self.tasks {
            let postgres = postgres.clone();
            let clickhouse = clickhouse.clone();
            let providers = providers.clone();
            workers.spawn(run_cron_task(task, postgres, clickhouse, providers));
        }

        while let Some(joined) = workers.join_next().await {
            let outcome = joined.map_err(|error| {
                std::io::Error::other(format!("Cron worker failed to join: {error}"))
            })?;
            if let Err(error) = outcome {
                workers.abort_all();
                return Err(Box::new(std::io::Error::other(error)));
            }
        }

        info!("Cron scheduler stopped");
        Ok(())
    }
}

/// Maximum number of retry attempts for failed cron operations.
const MAX_RETRIES: u32 = 3;

/// The live cron task must return a terminal error after exhausting its
/// bounded write retries, rather than remaining healthy until its next tick.
fn report_exhausted_live_cron_retries(
    table: &str,
    network: &str,
    retries: u32,
    cause: &str,
) -> Result<(), String> {
    let message = format!(
        "Cron task failed for table '{table}' on network '{network}' after {retries} retries: {cause}"
    );
    error!("{message}");
    Err(message)
}

/// Initial backoff delay for retries (doubles each attempt).
const INITIAL_BACKOFF_SECS: u64 = 2;

/// Get all discovered factory addresses with their birth blocks for a factory-indexed contract.
/// Returns HashMap<Address, u64> where the value is the birth block (0 for static contracts).
async fn get_factory_addresses_with_blocks(
    task: &CronTask,
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
) -> Result<HashMap<Address, u64>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(factory_config) = &task.factory_config else {
        // Not a factory contract - return the static address with birth block 0
        let mut result = HashMap::new();
        result.insert(task.contract_address, 0);
        return Ok(result);
    };

    match read_factory_addresses_from_event(
        &factory_config.event_table,
        &factory_config.address_column,
        &task.network,
        postgres.as_deref(),
        clickhouse.as_deref(),
    )
    .await
    {
        Ok(addresses) => {
            if addresses.is_empty() {
                warn!(
                    "No factory addresses discovered yet for {}.{} - cron will wait for addresses",
                    task.contract_name, task.network
                );
            } else {
                info!(
                    "Found {} factory addresses for {}.{}",
                    addresses.len(),
                    task.contract_name,
                    task.network
                );
            }
            Ok(addresses)
        }
        Err(e) => {
            error!(
                "Failed to get factory addresses for {}.{}: {}",
                task.contract_name, task.network, e
            );
            Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }
    }
}

/// Get factory addresses as a simple Vec (for compatibility with wait_for_factory_sync).
async fn get_factory_addresses(
    task: &CronTask,
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
) -> Result<Vec<Address>, Box<dyn std::error::Error + Send + Sync>> {
    let addresses_with_blocks =
        get_factory_addresses_with_blocks(task, postgres, clickhouse).await?;
    Ok(addresses_with_blocks.keys().copied().collect())
}

/// Wait for factory event indexing to discover addresses.
///
/// For factory contracts, the cron historical sync depends on the factory
/// event indexing to discover addresses first. This waits until at least
/// one factory address is available before proceeding.
async fn wait_for_factory_sync(
    task: &CronTask,
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
    _start_block: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if task.factory_config.is_none() {
        // Not a factory contract - no waiting needed
        return Ok(());
    }

    info!(
        "Waiting for factory event indexing to discover addresses for {}.{}...",
        task.contract_name, task.network
    );

    let mut logged_waiting = false;
    let mut logged_db_error = false;
    loop {
        if !is_running() {
            return Err("Shutdown requested".into());
        }

        // Check if any factory addresses have been discovered
        // DB errors are expected early on (tables may not exist yet), so treat them as "not ready"
        let addresses = match get_factory_addresses(task, postgres, clickhouse).await {
            Ok(addrs) => addrs,
            Err(e) => {
                if !logged_db_error {
                    debug!(
                        "Factory addresses table not ready yet for {}.{}: {} - will retry",
                        task.contract_name, task.network, e
                    );
                    logged_db_error = true;
                }
                // Treat DB errors as "no addresses yet" - the table may not exist
                vec![]
            }
        };

        if !addresses.is_empty() {
            info!(
                "Factory indexing discovered {} addresses for {}.{}, proceeding with cron",
                addresses.len(),
                task.contract_name,
                task.network
            );
            return Ok(());
        }

        if !logged_waiting {
            info!(
                "No factory addresses discovered yet for {}.{}, waiting for factory event indexing...",
                task.contract_name, task.network
            );
            logged_waiting = true;
        }

        // Wait before checking again (short interval for responsiveness)
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run a single cron task until shutdown.
///
/// If the cron has `start_block` configured, historical sync runs first,
/// iterating through blocks from start_block (or last synced) to end_block (or latest).
/// After historical sync completes:
/// - If `end_block` was specified, the task stops (no live cron).
/// - Otherwise, live cron continues with the configured schedule.
async fn run_cron_task(
    task: CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
) -> Result<(), String> {
    let is_factory = task.factory_config.is_some();

    info!(
        "Starting cron task for table '{}' on network '{}' with schedule {:?}{}",
        task.table.name(),
        task.network,
        task.cron_entry.interval.as_ref().or(task.cron_entry.schedule.as_ref()),
        if is_factory { " (factory-indexed)" } else { "" }
    );

    // Check if historical sync is needed
    if let Some(start_block_value) = &task.cron_entry.start_block {
        // For factory contracts, wait for factory event indexing to discover addresses
        if is_factory {
            let start_block = start_block_value.to::<u64>();
            if let Err(e) = wait_for_factory_sync(&task, &postgres, &clickhouse, start_block).await
            {
                if !is_running() {
                    return Ok(());
                }
                return Err(format!(
                    "Factory sync for historical cron {}.{} on network {:?} failed: {e}",
                    task.contract_name,
                    task.table.name(),
                    task.network,
                ));
            }
        }

        let historical_completed =
            run_historical_sync(&task, postgres.clone(), clickhouse.clone(), providers.clone())
                .await?;

        if !historical_completed {
            debug!(
                "Historical sync for table '{}' on network '{}' interrupted by shutdown",
                task.table.name(),
                task.network
            );
            return Ok(());
        }

        // If end_block was specified, stop here (no live cron)
        if task.cron_entry.end_block.is_some() {
            info!(
                "Historical cron sync completed for table '{}' on network '{}' - end_block reached, stopping",
                task.table.name(), task.network
            );
            return Ok(());
        }

        info!(
            "Historical cron sync completed for table '{}' on network '{}' - switching to live mode",
            task.table.name(), task.network
        );
    }

    // Live cron mode
    run_live_cron_loop(&task, postgres, clickhouse, providers).await
}

/// Run historical sync for a cron task using batched operations.
///
/// This batches multiple blocks together for efficient RPC calls via multicall3
/// and batched database writes. Returns Ok(false) on shutdown; a missing
/// required sample is an error, never a successful live-mode handoff.
async fn run_historical_sync(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
) -> Result<bool, String> {
    let start_block = task.cron_entry.start_block.unwrap().to::<u64>();
    let block_interval = task.cron_entry.block_interval.map(|b| b.to::<u64>()).unwrap_or(1); // Default to every block
    if block_interval == 0 {
        return Err(format!(
            "Historical cron {}.{} on {:?} has zero block_interval",
            task.contract_name,
            task.table.name(),
            task.network,
        ));
    }

    // Get contract addresses (1 for static, N for factory)
    // Get addresses with their birth blocks (for factory contracts, only process blocks >= birth)
    let addresses_with_blocks =
        match get_factory_addresses_with_blocks(task, &postgres, &clickhouse).await {
            Ok(addrs) => addrs,
            Err(e) => {
                error!(
                    "Failed to get contract addresses for table '{}' on network '{}': {}",
                    task.table.name(),
                    task.network,
                    e
                );
                return Err(format!(
                    "Historical cron {}.{} on network {:?} cannot discover addresses: {e}",
                    task.contract_name,
                    task.table.name(),
                    task.network,
                ));
            }
        };

    if addresses_with_blocks.is_empty() {
        return Err(format!(
            "Historical cron {}.{} on network {:?} has no discovered addresses",
            task.contract_name,
            task.table.name(),
            task.network,
        ));
    }

    let is_factory = task.factory_config.is_some();
    if is_factory {
        info!("Historical sync for {} discovered factory addresses", addresses_with_blocks.len());
        for (addr, birth_block) in &addresses_with_blocks {
            info!("  Address {:?} birth_block={}", addr, birth_block);
        }
    }

    // Get end block (either specified or latest)
    let provider = providers.get(&task.network);
    let end_block = if let Some(end) = task.cron_entry.end_block {
        end.to::<u64>()
    } else if let Some(p) = provider {
        p.get_block_number()
            .await
            .map_err(|error| {
                format!(
                    "Historical cron {}.{} on network {:?} cannot read head height: {error}",
                    task.contract_name,
                    task.table.name(),
                    task.network,
                )
            })?
            .to::<u64>()
    } else {
        return Err(format!(
            "Historical cron {}.{} on network {:?} has no provider for head height",
            task.contract_name,
            task.table.name(),
            task.network,
        ));
    };

    // The immutable prepared computation is checked both before a read and
    // under the checkpoint's FOR UPDATE lock. Runtime endpoint settings do
    // not enter the certificate; the provider's declared chain identity does.
    let identity = postgres
        .as_ref()
        .map(|_| {
            let source = providers.get(&task.network).ok_or_else(|| {
                format!(
                    "Historical cron {}.{} has no provider for schedule identity",
                    task.contract_name,
                    task.table.name()
                )
            })?;
            crate::indexer::cron_identity::identify(
                crate::indexer::cron_identity::CronIdentityInput {
                    indexer: &task.indexer_name,
                    contract: &task.contract_name,
                    table: &task.table,
                    destination: &task.full_table_name,
                    cron_index: task.cron_index,
                    cron: &task.cron_entry,
                    network: &task.network,
                    address: task.contract_address,
                    chain_id: Some(source.chain().id()),
                },
            )
        })
        .transpose()?;

    // Get last synced block from database
    let sync_config = CronSyncConfig {
        postgres: &postgres,
        clickhouse: &clickhouse,
        indexer_name: &task.indexer_name,
        contract_name: &task.contract_name,
        table_name: task.table.name(),
        cron_index: task.cron_index,
        network: &task.network,
        schedule_identity: identity.as_ref(),
    };

    let last_synced = tokio::time::timeout(
        HISTORICAL_DB_CONNECTION_TIMEOUT,
        get_last_synced_cron_block(sync_config),
    )
    .await
    .map_err(|_| "Historical cron checkpoint read timed out".to_string())??;
    if let Some(last) = last_synced {
        let block = last.to::<u64>();
        if block < start_block || !(block - start_block).is_multiple_of(block_interval) {
            return Err(format!(
                "Historical cron {}.{} checkpoint {block} is outside the sampling grid (start={start_block}, interval={block_interval})",
                task.contract_name,
                task.table.name()
            ));
        }
    }
    let mut expected_previous = last_synced.map(|block| block.to::<u64>());

    // Determine starting point
    let mut current_block = match last_synced {
        Some(last) => {
            // Resume from last synced block + interval
            let Some(next) = last.to::<u64>().checked_add(block_interval) else {
                return Ok(true);
            };
            if next > end_block {
                info!(
                    "Historical cron sync for table '{}' on network '{}' already complete (last synced: {}, end: {})",
                    task.table.name(), task.network, last, end_block
                );
                return Ok(true);
            }
            next
        }
        None => start_block,
    };

    info!(
        "Starting historical cron sync for table '{}' on network '{}' from block {} to {} (interval: {})",
        task.table.name(), task.network, current_block, end_block, block_interval
    );

    let start_time = std::time::Instant::now();

    // Create adaptive rate limiter for this sync
    let rate_limiter = Arc::new(AdaptiveRateLimiter::new());

    // Process blocks in batches
    while current_block <= end_block {
        if !is_running() {
            return Ok(false);
        }

        // Collect blocks for this batch (batch size is dynamic based on rate limiting)
        let current_batch_size = rate_limiter.batch_size() as usize;
        #[cfg(test)]
        let current_batch_size = task.batch_size_for_test.unwrap_or(current_batch_size);
        let mut batch_blocks: Vec<u64> = Vec::with_capacity(current_batch_size);
        let mut block = current_block;
        while block <= end_block && batch_blocks.len() < current_batch_size {
            batch_blocks.push(block);
            let Some(next) = block.checked_add(block_interval) else {
                break;
            };
            block = next;
        }

        if batch_blocks.is_empty() {
            break;
        }

        let batch_start = batch_blocks[0];
        let batch_end = *batch_blocks.last().unwrap();

        // Track batch timing to detect rate limiting
        let batch_start_time = std::time::Instant::now();

        // Resolve every required RPC input outside any database transaction.
        // Only an expired preparation deadline is retried; an ordinary
        // decoding or configuration failure is permanent and propagates.
        let preparation_timeout = HISTORICAL_RPC_PREPARATION_TIMEOUT;
        #[cfg(test)]
        let preparation_timeout = task.prepare_timeout_for_test.unwrap_or(preparation_timeout);
        let mut attempt = 0;
        let result = loop {
            if !is_running() {
                return Ok(false);
            }
            let prepared = tokio::select! {
                _ = wait_for_historical_cron_shutdown() => return Ok(false),
                result = tokio::time::timeout(
                    preparation_timeout,
                    prepare_cron_operations_batch(
                        task,
                        providers.clone(),
                        &batch_blocks,
                        &addresses_with_blocks,
                        rate_limiter.clone(),
                    ),
                ) => result,
            };
            match prepared {
                Ok(result) => break result,
                Err(_) if attempt < HISTORICAL_RPC_PREPARATION_RETRIES => {
                    attempt += 1;
                    warn!(
                        "Historical cron RPC preparation timeout for blocks {}-{}; retry {}",
                        batch_start, batch_end, attempt
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(_) => {
                    return Err(format!(
                        "Historical cron {}.{} on network {:?}: RPC timeout preparing blocks                          {batch_start}-{batch_end} after {} attempts",
                        task.contract_name,
                        task.table.name(),
                        task.network,
                        attempt + 1
                    ));
                }
            }
        };

        let batch_duration = batch_start_time.elapsed();

        match result {
            Ok(Some((completed, rows))) if completed == batch_end => {
                if let Some(postgres) = &postgres {
                    match apply_postgres_historical_batch(
                        task,
                        postgres,
                        sync_config,
                        rows,
                        expected_previous,
                        completed,
                    )
                    .await?
                    {
                        CronBatchOutcome::Committed => {
                            expected_previous = Some(completed);
                        }
                        CronBatchOutcome::Interrupted => return Ok(false),
                        CronBatchOutcome::Replan(progress) => {
                            if let Some(last) = progress {
                                let on_grid = last >= start_block
                                    && (last - start_block).is_multiple_of(block_interval);
                                if !on_grid {
                                    return Err(format!(
                                        "Historical cron checkpoint {last} moved outside the declared sample grid"
                                    ));
                                }
                            }
                            expected_previous = progress;
                            current_block = match progress {
                                Some(last) => match last.checked_add(block_interval) {
                                    Some(next) => next,
                                    None => break,
                                },
                                None => start_block,
                            };
                            continue;
                        }
                    }
                } else {
                    // ClickHouse retains its previous best-effort checkpoint
                    // protocol, outside the PostgreSQL transaction guarantee.
                    if let Some(clickhouse) = &clickhouse {
                        for step in &rows {
                            if step.rows.is_empty() {
                                continue;
                            }
                            let operation = &task.cron_entry.operations[step.operation_index];
                            super::tables::execute_clickhouse_operation_internal(
                                clickhouse,
                                &task.full_table_name,
                                &task.table,
                                operation,
                                &step.rows,
                            )
                            .await
                            .map_err(|error| {
                                format!(
                                    "Historical ClickHouse cron block {} operation {} failed: {error}",
                                    step.block, step.operation_index
                                )
                            })?;
                        }
                    }
                    update_last_synced_clickhouse_cron_block(sync_config, U64::from(completed))
                        .await?;
                }
                info!(
                    "Cron batch committed: table='{}', blocks {}-{}, batch_size={}, {:.1}s, checkpoint={}",
                    task.table.name(),
                    batch_start,
                    batch_end,
                    rate_limiter.batch_size(),
                    batch_duration.as_secs_f64(),
                    completed
                );
            }
            Ok(None) if !is_running() => return Ok(false),
            Ok(last) => {
                let completed = last.map(|(block, _)| block);
                return Err(format!(
                    "Historical cron {}.{} network {:?} blocks {batch_start}-{batch_end} did not finish: last completed {completed:?}",
                    task.contract_name, task.table.name(), task.network,
                ));
            }
            Err(error) => {
                return Err(format!(
                    "Historical cron {}.{} network {:?} blocks {batch_start}-{batch_end} failed: {error}",
                    task.contract_name, task.table.name(), task.network,
                ));
            }
        }

        // Check timing to detect rate limiting and adjust concurrency/batch size
        let delay = rate_limiter.after_batch_with_duration(batch_duration);
        if !delay.is_zero() {
            debug!("Rate limit backoff: sleeping {:?}", delay);
            tokio::time::sleep(delay).await;
        }

        let Some(next_block) = batch_end.checked_add(block_interval) else {
            break;
        };
        current_block = next_block;
    }

    let elapsed = start_time.elapsed();
    info!(
        "Historical cron sync completed for table '{}' on network '{}' in {:?}",
        task.table.name(),
        task.network,
        elapsed
    );

    Ok(true)
}

/// One declared operation at one historical block. RPC preparation may run in
/// parallel, but the database consumes these steps in block and declaration order.
struct PreparedCronOperation {
    block: u64,
    operation_index: usize,
    rows: Vec<TableRowData>,
}

/// Only statements whose aggregation preserves the stepwise result can
/// combine multiple sampled blocks. Keep UPDATE/DELETE, mixed operations, and
/// arithmetic with nullable/array columns on the ordered sequential path.
fn can_bulk_historical_postgres(task: &CronTask) -> bool {
    let [operation] = task.cron_entry.operations.as_slice() else {
        return false;
    };
    match operation.operation_type {
        OperationType::Insert => true,
        OperationType::Upsert if !operation.set.is_empty() => {
            if operation.set.iter().all(|column| column.action == SetAction::Set) {
                // DISTINCT ON by the key, descending sequence: last assignment wins.
                return true;
            }
            // SUM of deltas is equivalent for these scalar counter operations.
            // Arrays and nullable columns use the stepwise path: the shared SQL
            // aggregator picks MAX for arrays and NULL arithmetic is not additive.
            let all_counters = operation.set.iter().all(|column| {
                matches!(
                    column.action,
                    SetAction::Add
                        | SetAction::Subtract
                        | SetAction::Increment
                        | SetAction::Decrement
                )
            });
            all_counters
                && task.table.columns().iter().all(|column| {
                    !column.nullable && !matches!(column.resolved_type(), ColumnType::Array(_))
                })
        }
        _ => false,
    }
}

/// Whether the current attempt committed, or another writer advanced durable
/// progress while its RPC values were being prepared.
enum CronBatchOutcome {
    Committed,
    Replan(Option<u64>),
    Interrupted,
}

/// One transaction owns the checkpoint decision and all table effects.
/// A stale prepared batch never writes; a concurrent runner can replan.
async fn apply_postgres_historical_batch(
    task: &CronTask,
    postgres: &PostgresClient,
    sync: CronSyncConfig<'_>,
    steps: Vec<PreparedCronOperation>,
    expected_previous: Option<u64>,
    completed: u64,
) -> Result<CronBatchOutcome, String> {
    let mut connection =
        tokio::time::timeout(HISTORICAL_DB_CONNECTION_TIMEOUT, postgres.raw_connection())
            .await
            .map_err(|_| "Historical cron connection acquisition timed out".to_string())?
            .map_err(|e| e.to_string())?;

    // Each failed lock attempt is a separate transaction. PostgreSQL aborts
    // a transaction after lock_timeout, so never retry the same transaction.
    // No destination effect can precede this lock.
    let (transaction, durable) = {
        let mut attempt = 0;
        loop {
            if !is_running() {
                return Ok(CronBatchOutcome::Interrupted);
            }
            let transaction =
                tokio::time::timeout(HISTORICAL_DB_CONNECTION_TIMEOUT, connection.transaction())
                    .await
                    .map_err(|_| "Historical cron BEGIN timed out".to_string())?
                    .map_err(|e| format!("Cannot begin cron batch: {e}"))?;
            transaction
                .batch_execute("SET LOCAL lock_timeout = '2s'; SET LOCAL statement_timeout = '30s'")
                .await
                .map_err(|e| format!("Cannot configure cron transaction limits: {e}"))?;
            let locked = tokio::time::timeout(
                HISTORICAL_DB_CONNECTION_TIMEOUT,
                lock_postgres_cron_checkpoint(&transaction, sync),
            )
            .await
            .map_err(|_| "Historical cron checkpoint lock wait timed out".to_string())?;
            match locked {
                Ok(progress) => break (transaction, progress),
                Err(CronCheckpointLockError::LockTimeout(error))
                    if attempt < HISTORICAL_CHECKPOINT_LOCK_RETRIES =>
                {
                    attempt += 1;
                    debug!("Retrying pre-effect cron checkpoint lock ({attempt}): {error}");
                    drop(transaction);
                    tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    };
    if durable != expected_previous {
        if matches!((expected_previous, durable), (Some(_), None))
            || matches!(
                (expected_previous, durable),
                (Some(previous), Some(current)) if current < previous
            )
        {
            return Err(format!(
                "Historical cron checkpoint regressed from {expected_previous:?} to {durable:?}"
            ));
        }
        // The lock is released with the transaction, without any table write.
        return Ok(CronBatchOutcome::Replan(durable));
    }
    if durable.is_some_and(|block| block >= completed) {
        return Err(format!(
            "Historical cron attempted to advance past checkpoint {durable:?} with completed block {completed}"
        ));
    }
    // One table statement per block is wasteful for eligible operations.
    // Keep the same checkpoint transaction, but merge rows across this batch
    // only when the existing PostgreSQL statement builder is equivalent to
    // applying each sampled block in order.
    if can_bulk_historical_postgres(task) {
        let rows = steps.into_iter().flat_map(|step| step.rows).collect::<Vec<_>>();
        if !rows.is_empty() {
            super::tables::execute_postgres_cron_operation_in(
                &transaction,
                &task.full_table_name,
                &task.table,
                &task.cron_entry.operations[0],
                &rows,
            )
            .await
            .map_err(|error| format!("Historical cron bulk operation failed: {error}"))?;
        }
    } else {
        // Mixed operations and non-aggregatable mutation types retain strict
        // block/declaration order. Later failure aborts earlier effects.
        for step in steps {
            if step.rows.is_empty() {
                continue;
            }
            let operation =
                task.cron_entry.operations.get(step.operation_index).ok_or_else(|| {
                    format!("Historical cron operation {} is absent", step.operation_index)
                })?;
            super::tables::execute_postgres_cron_operation_in(
                &transaction,
                &task.full_table_name,
                &task.table,
                operation,
                &step.rows,
            )
            .await
            .map_err(|error| {
                format!(
                    "Historical cron block {} operation {} failed: {error}",
                    step.block, step.operation_index
                )
            })?;
        }
    }
    advance_postgres_cron_checkpoint(&transaction, sync, completed).await?;

    // A commit timeout is an *uncertain* outcome, not a retry signal.
    // Drop the original connection and reconcile the certified checkpoint
    // before claiming success or leaving the job for an operator/restart.
    let commit_error =
        match tokio::time::timeout(HISTORICAL_COMMIT_REPLY_TIMEOUT, transaction.commit()).await {
            Ok(Ok(())) => return Ok(CronBatchOutcome::Committed),
            Ok(Err(error)) => error.to_string(),
            Err(_) => "COMMIT acknowledgement timed out".to_string(),
        };
    drop(connection);
    // Locking the same row waits for the original transaction to
    // settle before declaring whether the progress committed.
    let mut recovery =
        tokio::time::timeout(HISTORICAL_RECONCILIATION_TIMEOUT, postgres.raw_connection())
            .await
            .map_err(|_| {
                format!("Cron COMMIT outcome unknown ({commit_error}); reconnect timed out")
            })?
            .map_err(|error| {
                format!("Cron COMMIT outcome unknown ({commit_error}); cannot reconnect: {error}")
            })?;
    let reconciliation_result =
        tokio::time::timeout(HISTORICAL_RECONCILIATION_TIMEOUT, recovery.transaction()).await;
    let reconciliation = reconciliation_result
        .map_err(|_| {
            format!("Cron COMMIT outcome unknown ({commit_error}); reconciliation BEGIN timed out")
        })?
        .map_err(|error| {
            format!(
            "Cron COMMIT outcome unknown ({commit_error}); cannot start reconciliation: {error}"
        )
        })?;
    let actual = tokio::time::timeout(
        HISTORICAL_RECONCILIATION_TIMEOUT,
        lock_postgres_cron_checkpoint(&reconciliation, sync),
    )
    .await
    .map_err(|_| {
        format!("Cron COMMIT outcome unknown ({commit_error}); reconciliation lock timed out")
    })?
    .map_err(|error| {
        format!("Cron COMMIT outcome unknown ({commit_error}); cannot reconcile: {error}")
    })?;
    drop(reconciliation);
    if matches!(actual, Some(block) if block >= completed) {
        info!("Historical cron COMMIT reply lost, but checkpoint confirms block {completed}");
        Ok(CronBatchOutcome::Committed)
    } else {
        Err(format!(
            "Cron COMMIT outcome unknown ({commit_error}); durable checkpoint {actual:?} \
             does not certify completed block {completed}; aborting for safe restart"
        ))
    }
}

/// Prepare a complete historical batch using bounded parallel RPC calls.
/// The resulting ordered work is returned to the caller; no database write
/// occurs until the checkpoint transaction owns the application decision.
///
/// For factory-indexed contracts, `addresses_with_blocks` maps each address to its birth block.
/// Only (block, address) pairs where block >= birth_block are processed.
/// For static contracts, birth_block is 0.
async fn prepare_cron_operations_batch(
    task: &CronTask,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    blocks: &[u64],
    addresses_with_blocks: &HashMap<Address, u64>,
    rate_limiter: Arc<AdaptiveRateLimiter>,
) -> Result<Option<(u64, Vec<PreparedCronOperation>)>, String> {
    use futures::stream::{FuturesUnordered, StreamExt};

    if task.cron_entry.operations.is_empty() {
        return Err("Historical cron has no declared operation".into());
    }
    if blocks.is_empty() || addresses_with_blocks.is_empty() {
        return Ok(None);
    }

    let provider = match providers.get(&task.network) {
        Some(p) => p.clone(),
        None => return Err(format!("No provider for network '{}'", task.network)),
    };

    // Step 1: Collect all unique $call patterns and their column mappings
    let mut call_patterns: Vec<(String, String, ColumnType)> = Vec::new(); // (value_ref, column_name, column_type)

    for operation in &task.cron_entry.operations {
        for (column_name, value_ref) in &operation.where_clause {
            if value_ref.starts_with("$call(") {
                if let Some(col) = task.table.columns().iter().find(|c| &c.name == column_name) {
                    call_patterns.push((
                        value_ref.clone(),
                        column_name.clone(),
                        col.resolved_type().clone(),
                    ));
                }
            }
        }
        for set_col in &operation.set {
            let val = set_col.effective_value();
            if val.starts_with("$call(") {
                if let Some(col) = task.table.columns().iter().find(|c| c.name == set_col.column) {
                    call_patterns.push((
                        val.to_string(),
                        set_col.column.clone(),
                        col.resolved_type().clone(),
                    ));
                }
            }
        }
    }

    // Deduplicate call patterns (same call might be used for multiple columns)
    let unique_calls: HashSet<String> = call_patterns.iter().map(|(v, _, _)| v.clone()).collect();

    // Count valid (block, address) pairs after birth block filtering
    let mut valid_pairs = 0;
    for &block_number in blocks {
        for (&_addr, &birth_block) in addresses_with_blocks {
            if block_number >= birth_block {
                valid_pairs += 1;
            }
        }
    }

    info!(
        "Cron batch: {} blocks, {} addresses, {} valid pairs (after birth filter), {} call patterns, {} unique calls",
        blocks.len(),
        addresses_with_blocks.len(),
        valid_pairs,
        call_patterns.len(),
        unique_calls.len()
    );

    if unique_calls.is_empty() {
        debug!("No $call patterns found in cron operations - only non-RPC values");
    }

    // Step 2: Execute all RPC calls in parallel for all (block, address) combinations
    // Only process (block, address) pairs where block >= birth_block
    // Key: (block_number, address, call_pattern) -> Result
    #[allow(clippy::type_complexity)]
    let call_cache: Arc<
        tokio::sync::RwLock<HashMap<(u64, Address, String), Option<EthereumSqlTypeWrapper>>>,
    > = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    // Use adaptive concurrency from rate limiter
    let current_concurrency = rate_limiter.concurrency();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(current_concurrency));
    let mut futures = FuturesUnordered::new();

    // Iterate over all (block, address, call_pattern) combinations
    // Skip blocks before the address's birth block (contract didn't exist yet)
    for &block_number in blocks {
        for (&contract_address, &birth_block) in addresses_with_blocks {
            // Skip if this block is before the contract was created
            if block_number < birth_block {
                continue;
            }

            for call_pattern in &unique_calls {
                let provider = provider.clone();
                let network = task.network.clone();
                let call_pattern = call_pattern.clone();
                let cache = call_cache.clone();
                let sem = semaphore.clone();

                // Find the column type for this call pattern
                let column_type = call_patterns
                    .iter()
                    .find(|(v, _, _)| v == &call_pattern)
                    .map(|(_, _, t)| t.clone())
                    .unwrap_or(ColumnType::String);

                futures.push(async move {
                    let _permit = sem.acquire().await.unwrap();

                    let timestamp = U256::from(Utc::now().timestamp() as u64);
                    let tx_metadata = TxMetadata {
                        block_number,
                        block_timestamp: Some(timestamp),
                        tx_hash: B256::ZERO,
                        block_hash: B256::ZERO,
                        contract_address,
                        log_index: U256::ZERO,
                        tx_index: 0,
                    };

                    let result = extract_cron_value(
                        &call_pattern,
                        &tx_metadata,
                        &contract_address,
                        &column_type,
                        Some(provider.as_ref()),
                        &network,
                    )
                    .await;

                    let mut cache_guard = cache.write().await;
                    cache_guard
                        .insert((block_number, contract_address, call_pattern.clone()), result);
                    debug!(
                        "RPC call done: block={}, addr={:?}, pattern={}",
                        block_number,
                        contract_address,
                        call_pattern.chars().take(30).collect::<String>()
                    );
                });
            }
        }
    }

    // Wait for all RPC calls to complete
    let mut completed_calls = 0;
    let total_expected_calls = valid_pairs * unique_calls.len();
    info!("Queued {} RPC call futures, waiting for completion...", total_expected_calls);
    while futures.next().await.is_some() {
        completed_calls += 1;
        if !is_running() {
            info!(
                "RPC calls interrupted by shutdown after {}/{} calls",
                completed_calls, total_expected_calls
            );
            return Ok(None);
        }
    }
    info!("All RPC calls completed: {}", completed_calls);
    if !is_running() {
        return Ok(None);
    }

    // Step 3: Prepare one ordered step per (block, declared operation).
    let cache = call_cache.read().await;

    // Log cache stats for debugging
    let total_calls = cache.len();
    let successful_calls = cache.values().filter(|v| v.is_some()).count();
    debug!(
        "RPC calls completed: {}/{} successful ({}%)",
        successful_calls,
        total_calls,
        (successful_calls * 100).checked_div(total_calls).unwrap_or(0)
    );
    // Every required view call must resolve for every eligible sample before
    // the first database write. Collecting later samples cannot fill a gap.
    for &block_number in blocks {
        for (&address, &birth_block) in addresses_with_blocks {
            if block_number < birth_block {
                continue;
            }
            for call in &unique_calls {
                if cache
                    .get(&(block_number, address, call.clone()))
                    .and_then(Option::as_ref)
                    .is_none()
                {
                    let columns = call_patterns
                        .iter()
                        .filter(|(pattern, _, _)| pattern == call)
                        .map(|(_, column, _)| column.as_str())
                        .collect::<Vec<_>>();
                    return Err(format!(
                        "Required cron view call {call:?} for {}.{} on network {:?} at block {block_number}, address {address:?}, columns {columns:?} returned no value",
                        task.contract_name, task.table.name(), task.network,
                    ));
                }
            }
        }
    }

    let mut steps = Vec::with_capacity(blocks.len() * task.cron_entry.operations.len());
    let mut last_successful_block: Option<u64> = None;

    for &block_number in blocks {
        if !is_running() {
            return Ok(None);
        }
        let mut block_steps = (0..task.cron_entry.operations.len())
            .map(|operation_index| PreparedCronOperation {
                block: block_number,
                operation_index,
                rows: Vec::with_capacity(addresses_with_blocks.len()),
            })
            .collect::<Vec<_>>();

        for (&contract_address, &birth_block) in addresses_with_blocks {
            // Skip if this block is before the contract was created
            if block_number < birth_block {
                continue;
            }
            let timestamp = U256::from(Utc::now().timestamp() as u64);
            let tx_metadata = TxMetadata {
                block_number,
                block_timestamp: Some(timestamp),
                tx_hash: B256::ZERO,
                block_hash: B256::ZERO,
                contract_address,
                log_index: U256::ZERO,
                tx_index: 0,
            };

            for (operation_index, operation) in task.cron_entry.operations.iter().enumerate() {
                let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

                // Add where clause columns
                for (column_name, value_ref) in &operation.where_clause {
                    if value_ref.starts_with("$call(") {
                        if let Some(value) = cache
                            .get(&(block_number, contract_address, value_ref.clone()))
                            .and_then(|v| v.clone())
                        {
                            columns.insert(column_name.clone(), value);
                        }
                    } else {
                        // Handle non-$call values
                        let column_def =
                            task.table.columns().iter().find(|c| &c.name == column_name);
                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_cron_value_sync(
                                value_ref,
                                &tx_metadata,
                                &contract_address,
                                column_def.resolved_type(),
                            ) {
                                columns.insert(column_name.clone(), value);
                            }
                        }
                    }
                }

                // Add set columns
                for set_col in &operation.set {
                    let val = set_col.effective_value();
                    if val.starts_with("$call(") {
                        if let Some(value) = cache
                            .get(&(block_number, contract_address, val.to_string()))
                            .and_then(|v| v.clone())
                        {
                            columns.insert(set_col.column.clone(), value);
                        }
                    } else {
                        // Handle non-$call values
                        let column_def =
                            task.table.columns().iter().find(|c| c.name == set_col.column);
                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_cron_value_sync(
                                val,
                                &tx_metadata,
                                &contract_address,
                                column_def.resolved_type(),
                            ) {
                                columns.insert(set_col.column.clone(), value);
                            }
                        }
                    }
                }

                // Check that all required user columns are present (where clause + set columns)
                let mut required_columns: Vec<String> = operation
                    .where_clause
                    .keys()
                    .cloned()
                    .chain(operation.set.iter().map(|s| s.column.clone()))
                    .collect();
                required_columns.sort();
                required_columns.dedup();

                let missing_columns: Vec<&String> =
                    required_columns.iter().filter(|col| !columns.contains_key(*col)).collect();

                if !missing_columns.is_empty() {
                    return Err(format!(
                        "Historical cron {}.{} network {:?} block {block_number} address {contract_address:?} operation {:?} requires columns {missing_columns:?}, but only {:?} were resolved",
                        task.contract_name,
                        task.table.name(),
                        task.network,
                        operation.operation_type,
                        columns.keys().collect::<Vec<_>>(),
                    ));
                }
                if columns.is_empty() {
                    debug!(
                        "No user columns for cron operation at block {} address {:?} - skipping",
                        block_number, contract_address
                    );
                    continue;
                }

                // Add auto-injected metadata columns
                let sequence_id =
                    crate::manifest::contract::compute_sequence_id(block_number, 0, 0);
                columns.insert(
                    crate::manifest::contract::injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                    EthereumSqlTypeWrapper::U128(sequence_id),
                );
                columns.insert(
                    crate::manifest::contract::injected_columns::BLOCK_NUMBER.to_string(),
                    EthereumSqlTypeWrapper::U64BigInt(block_number),
                );

                if task.table.definition().timestamp {
                    if let Some(ts) = tx_metadata.block_timestamp {
                        if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                            columns.insert(
                                crate::manifest::contract::injected_columns::BLOCK_TIMESTAMP
                                    .to_string(),
                                EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)),
                            );
                        }
                    }
                }

                columns.insert(
                    crate::manifest::contract::injected_columns::TX_HASH.to_string(),
                    EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
                );
                columns.insert(
                    crate::manifest::contract::injected_columns::BLOCK_HASH.to_string(),
                    EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
                );
                columns.insert(
                    crate::manifest::contract::injected_columns::CONTRACT_ADDRESS.to_string(),
                    EthereumSqlTypeWrapper::Address(contract_address),
                );

                block_steps[operation_index]
                    .rows
                    .push(TableRowData { columns, network: task.network.clone() });
            }
        }

        steps.extend(block_steps);
        last_successful_block = Some(block_number);
    }

    drop(cache); // Release the read lock

    Ok(last_successful_block.map(|block| (block, steps)))
}

#[cfg(test)]
async fn execute_cron_operations_batch(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    blocks: &[u64],
    addresses_with_blocks: &HashMap<Address, u64>,
    rate_limiter: Arc<AdaptiveRateLimiter>,
) -> Result<Option<u64>, String> {
    let prepared =
        prepare_cron_operations_batch(task, providers, blocks, addresses_with_blocks, rate_limiter)
            .await?;
    let Some((completed, rows)) = prepared else {
        return Ok(None);
    };
    for step in &rows {
        if step.rows.is_empty() {
            continue;
        }
        let operation = &task.cron_entry.operations[step.operation_index];
        if let Some(postgres) = &postgres {
            super::tables::execute_postgres_operation_internal(
                postgres,
                &task.full_table_name,
                &task.table,
                operation,
                &step.rows,
                None,
            )
            .await?;
        }
        if let Some(clickhouse) = &clickhouse {
            super::tables::execute_clickhouse_operation_internal(
                clickhouse,
                &task.full_table_name,
                &task.table,
                operation,
                &step.rows,
            )
            .await?;
        }
    }
    Ok(Some(completed))
}

// Note: Prefetching is handled by the VIEW_CALL_CACHE in extract_cron_value
// Historical preparation preserves block and operation boundaries for ordered writes.

/// Run live cron loop for a task.
async fn run_live_cron_loop(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
) -> Result<(), String> {
    loop {
        if !is_running() {
            debug!("Cron task for table '{}' stopping due to shutdown", task.table.name());
            break;
        }

        // Wait for next trigger
        let wait_duration = match &task.schedule {
            CronSchedule::Interval(duration) => *duration,
            CronSchedule::Cron(cron) => {
                // Calculate next occurrence from now
                let now = Utc::now();
                match cron.find_next_occurrence(&now, false) {
                    Ok(next) => {
                        let duration = next - now;
                        duration.to_std().unwrap_or(Duration::from_secs(60))
                    }
                    Err(e) => {
                        warn!(
                            "Could not find next cron occurrence for table '{}': {}, using 60s default",
                            task.table.name(), e
                        );
                        Duration::from_secs(60)
                    }
                }
            }
        };

        debug!(
            "Cron task for table '{}' waiting {:?} until next execution",
            task.table.name(),
            wait_duration
        );

        // Sleep in chunks to check for shutdown
        let sleep_chunk = Duration::from_secs(1);
        let mut remaining = wait_duration;

        while remaining > Duration::ZERO {
            if !is_running() {
                debug!("Cron task for table '{}' stopping due to shutdown", task.table.name());
                return Ok(());
            }

            let sleep_time = remaining.min(sleep_chunk);
            tokio::time::sleep(sleep_time).await;
            remaining = remaining.saturating_sub(sleep_time);
        }

        if !is_running() {
            break;
        }

        // Get factory addresses (refreshed on each tick to pick up new contracts)
        // For live cron, birth blocks don't matter (latest block >= all birth blocks)
        let addresses_with_blocks =
            match get_factory_addresses_with_blocks(task, &postgres, &clickhouse).await {
                Ok(addrs) => addrs,
                Err(e) => {
                    error!(
                        "Failed to get contract addresses for table '{}' on network '{}': {}",
                        task.table.name(),
                        task.network,
                        e
                    );
                    continue; // Skip this tick, try again next time
                }
            };
        let addresses: Vec<Address> = addresses_with_blocks.keys().copied().collect();

        if addresses.is_empty() {
            debug!(
                "No contract addresses for table '{}' on network '{}', skipping this tick",
                task.table.name(),
                task.network
            );
            continue;
        }

        // Execute the cron operations with retry and exponential backoff
        let mut retry_count = 0;
        let mut last_error: Option<String> = None;

        while retry_count <= MAX_RETRIES {
            if !is_running() {
                break;
            }

            match execute_cron_operations(
                task,
                postgres.clone(),
                clickhouse.clone(),
                providers.clone(),
                &addresses,
            )
            .await
            {
                Ok(()) => {
                    // Success - clear any previous error state
                    if retry_count > 0 {
                        info!(
                            "Cron task for table '{}' succeeded after {} retry attempt(s)",
                            task.table.name(),
                            retry_count
                        );
                    }
                    break;
                }
                Err(e) => {
                    last_error = Some(e.clone());
                    retry_count += 1;

                    if retry_count <= MAX_RETRIES {
                        // Calculate exponential backoff: 2s, 4s, 8s
                        let backoff_secs = INITIAL_BACKOFF_SECS * (1 << (retry_count - 1));
                        let backoff = Duration::from_secs(backoff_secs);

                        warn!(
                            "Cron task failed for table '{}' on network '{}': {}. Retrying in {:?} (attempt {}/{})",
                            task.table.name(), task.network, e, backoff, retry_count, MAX_RETRIES
                        );

                        // Sleep with shutdown check
                        let mut backoff_remaining = backoff;
                        while backoff_remaining > Duration::ZERO {
                            if !is_running() {
                                return Ok(());
                            }
                            let sleep_time = backoff_remaining.min(Duration::from_secs(1));
                            tokio::time::sleep(sleep_time).await;
                            backoff_remaining = backoff_remaining.saturating_sub(sleep_time);
                        }
                    }
                }
            }
        }

        // A deliberate shutdown is not a failed tick. A genuinely exhausted
        // retry budget is terminal and must reach the owning supervisor.
        if !is_running() {
            return Ok(());
        }
        if retry_count > MAX_RETRIES {
            return report_exhausted_live_cron_retries(
                task.table.name(),
                &task.network,
                MAX_RETRIES,
                last_error.as_deref().unwrap_or("retry budget exhausted without a cause"),
            );
        }
    }
    Ok(())
}

/// Execute the cron operations for a task (for all addresses).
async fn execute_cron_operations(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    addresses: &[Address],
) -> Result<(), String> {
    let provider = providers.get(&task.network);

    // Get latest block number for this network
    let latest_block = if let Some(p) = provider {
        p.get_block_number().await.ok().map(|b| b.to::<u64>()).unwrap_or(0)
    } else {
        warn!(
            "No provider found for network '{}' in cron task for table '{}'",
            task.network,
            task.table.name()
        );
        0
    };

    let now = Utc::now();

    let is_factory = task.factory_config.is_some();
    if is_factory {
        debug!(
            "Executing cron operations for table '{}' on network '{}' at block {} for {} addresses",
            task.table.name(),
            task.network,
            latest_block,
            addresses.len()
        );
    } else {
        debug!(
            "Executing cron operations for table '{}' on network '{}' at block {}",
            task.table.name(),
            task.network,
            latest_block
        );
    }

    // Process each operation
    for operation in &task.cron_entry.operations {
        let mut all_rows: Vec<TableRowData> = Vec::with_capacity(addresses.len());

        // Get provider for view calls
        let provider_ref = provider.map(|p| p.as_ref());

        // Process each address
        for (addr_idx, &contract_address) in addresses.iter().enumerate() {
            // Create synthetic metadata for this address
            let tx_metadata = TxMetadata {
                block_number: latest_block,
                block_timestamp: Some(U256::from(now.timestamp() as u64)),
                tx_hash: B256::ZERO,
                block_hash: B256::ZERO,
                contract_address,
                log_index: U256::ZERO,
                tx_index: 0,
            };

            let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

            // Add where clause columns
            for (column_name, value_ref) in &operation.where_clause {
                let column_def = task.table.columns().iter().find(|c| &c.name == column_name);

                if let Some(column_def) = column_def {
                    if let Some(value) = extract_cron_value(
                        value_ref,
                        &tx_metadata,
                        &contract_address,
                        column_def.resolved_type(),
                        provider_ref,
                        &task.network,
                    )
                    .await
                    {
                        columns.insert(column_name.clone(), value);
                    }
                }
            }

            // Add set columns with their values
            for set_col in &operation.set {
                let column_def = task.table.columns().iter().find(|c| c.name == set_col.column);

                if let Some(column_def) = column_def {
                    if let Some(value) = extract_cron_value(
                        set_col.effective_value(),
                        &tx_metadata,
                        &contract_address,
                        column_def.resolved_type(),
                        provider_ref,
                        &task.network,
                    )
                    .await
                    {
                        columns.insert(set_col.column.clone(), value);
                    }
                }
            }

            if columns.is_empty() {
                debug!(
                    "No columns extracted for cron operation on table '{}' address {}, skipping",
                    task.table.name(),
                    contract_address
                );
                continue;
            }

            // Add auto-injected metadata columns
            // For cron with multiple addresses, include addr_idx to ensure unique sequence_id
            let sequence_id = crate::manifest::contract::compute_sequence_id(
                latest_block,
                u32::try_from(addr_idx).map_err(|_| format!("Cannot order cron operations for table {:?}: address index {addr_idx} exceeds {}", task.table.name(), u32::MAX))?,
                0,
            );

            columns.insert(
                crate::manifest::contract::injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                EthereumSqlTypeWrapper::U128(sequence_id),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::BLOCK_NUMBER.to_string(),
                EthereumSqlTypeWrapper::U64BigInt(latest_block),
            );
            // Only add timestamp if table has timestamp: true
            if task.table.definition().timestamp {
                columns.insert(
                    crate::manifest::contract::injected_columns::BLOCK_TIMESTAMP.to_string(),
                    EthereumSqlTypeWrapper::DateTime(now),
                );
            }
            columns.insert(
                crate::manifest::contract::injected_columns::TX_HASH.to_string(),
                EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::BLOCK_HASH.to_string(),
                EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::CONTRACT_ADDRESS.to_string(),
                EthereumSqlTypeWrapper::Address(contract_address),
            );

            all_rows.push(TableRowData { columns, network: task.network.clone() });
        }

        if all_rows.is_empty() {
            debug!(
                "No rows extracted for cron operation on table '{}', skipping",
                task.table.name()
            );
            continue;
        }

        // Execute the operation for all rows
        if let Some(postgres) = &postgres {
            super::tables::execute_postgres_operation_internal(
                postgres,
                &task.full_table_name,
                &task.table,
                operation,
                &all_rows,
                None, // No SQL condition for cron
            )
            .await?;
        }

        if let Some(clickhouse) = &clickhouse {
            super::tables::execute_clickhouse_operation_internal(
                clickhouse,
                &task.full_table_name,
                &task.table,
                operation,
                &all_rows,
            )
            .await?;
        }
    }

    let addr_info = if task.factory_config.is_some() {
        format!(" ({} addresses)", addresses.len())
    } else {
        String::new()
    };
    info!(
        "Cron operations completed for table '{}' on network '{}' at block {}{}",
        task.table.name(),
        task.network,
        latest_block,
        addr_info
    );

    Ok(())
}

/// Extract value for cron operations (no event data available).
///
/// This is similar to `extract_value_from_event_async` but only supports:
/// - $call(...) - view function calls
/// - $contract - contract address
/// - $rindexer_* - built-in metadata
/// - Literals
async fn extract_cron_value(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
    provider: Option<&dyn ChainProvider>,
    network: &str,
) -> Option<EthereumSqlTypeWrapper> {
    // Handle $call(...) - view function calls
    if value_ref.starts_with("$call(") {
        let provider = provider?;
        // Use existing view call infrastructure from tables module
        return super::tables::execute_view_call_for_cron(
            value_ref,
            tx_metadata,
            contract_address,
            column_type,
            provider,
            network,
        )
        .await;
    }

    // Handle $contract - contract address
    if value_ref == "$contract" {
        return Some(EthereumSqlTypeWrapper::Address(*contract_address));
    }

    // Handle $rindexer_* built-ins
    if let Some(field_name) = value_ref.strip_prefix('$') {
        return match field_name {
            "rindexer_block_number" => {
                Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number))
            }
            "rindexer_block_timestamp" | "rindexer_timestamp" => {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        return Some(EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)));
                    }
                }
                None
            }
            "rindexer_tx_hash" => {
                Some(EthereumSqlTypeWrapper::StringChar(format!("{:?}", tx_metadata.tx_hash)))
            }
            "rindexer_block_hash" => {
                Some(EthereumSqlTypeWrapper::StringChar(format!("{:?}", tx_metadata.block_hash)))
            }
            "rindexer_contract_address" => {
                Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address))
            }
            _ => {
                // Unknown $ reference - this shouldn't happen after validation
                warn!("Unknown cron value reference: {}", value_ref);
                None
            }
        };
    }

    // Handle literal values
    super::tables::parse_literal_value_for_column(value_ref, column_type)
}

/// Extract value for cron operations synchronously (no RPC calls).
///
/// This handles all non-$call values:
/// - $contract - contract address
/// - $rindexer_* - built-in metadata
/// - Literals
fn extract_cron_value_sync(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
) -> Option<EthereumSqlTypeWrapper> {
    // $call requires async - should not be passed here
    if value_ref.starts_with("$call(") {
        return None;
    }

    // Handle $contract - contract address
    if value_ref == "$contract" {
        return Some(EthereumSqlTypeWrapper::Address(*contract_address));
    }

    // Handle $rindexer_* built-ins
    if let Some(field_name) = value_ref.strip_prefix('$') {
        return match field_name {
            "rindexer_block_number" => {
                Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number))
            }
            "rindexer_block_timestamp" | "rindexer_timestamp" => {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        return Some(EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)));
                    }
                }
                None
            }
            "rindexer_tx_hash" => {
                Some(EthereumSqlTypeWrapper::StringChar(format!("{:?}", tx_metadata.tx_hash)))
            }
            "rindexer_block_hash" => {
                Some(EthereumSqlTypeWrapper::StringChar(format!("{:?}", tx_metadata.block_hash)))
            }
            "rindexer_contract_address" => {
                Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address))
            }
            _ => {
                // Unknown $ reference
                None
            }
        };
    }

    // Handle literal values
    super::tables::parse_literal_value_for_column(value_ref, column_type)
}

/// Check if any tables in the manifest have cron triggers.
pub fn manifest_has_cron_tables(manifest: &Manifest) -> bool {
    manifest.contracts.iter().filter_map(|c| c.tables.as_ref()).flatten().any(|t| t.has_cron())
}

#[cfg(test)]
mod cron_parsing_tests {
    //! Locks down the croner contract we depend on, in particular the v2→v3
    //! migration. Our user-facing docs (`documentation/.../tables/index.mdx`)
    //! only advertise standard 5-field cron expressions and reference these
    //! examples verbatim:
    //!
    //!   - `"*/5 * * * *"` — every 5 minutes
    //!   - `"0 * * * *"`   — every hour at minute 0
    //!
    //! These tests pin (a) parsing of the documented surface and (b) the
    //! `find_next_occurrence` semantics the scheduler relies on at runtime.
    //!
    //! v3 default-config note: croner 3.0 widened the parser to also accept
    //! 7-field patterns with an optional year — patterns that errored under
    //! v2. We assert the new behavior explicitly so any future revert is
    //! caught.
    //!
    //! TODO: extend coverage if/when documentation/.../tables/index.mdx
    //! starts advertising additional shorthands (e.g. `@hourly`, `@daily`,
    //! 6-field with seconds, the new v3 7-field-with-year shape). Today the
    //! docs only show the two 5-field examples covered here.
    use chrono::{DateTime, TimeZone, Utc};

    fn parse(expr: &str) -> Result<croner::Cron, croner::errors::CronError> {
        expr.parse::<croner::Cron>()
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    // ---- Parsing: documented patterns ---------------------------------------

    #[test]
    fn docs_example_every_5_minutes_parses() {
        // Verbatim from documentation/.../tables/index.mdx.
        assert!(parse("*/5 * * * *").is_ok());
    }

    #[test]
    fn docs_example_hourly_parses() {
        // Verbatim from documentation/.../tables/index.mdx.
        assert!(parse("0 * * * *").is_ok());
    }

    // ---- Parsing: full 5-field surface --------------------------------------

    #[test]
    fn five_field_basic_patterns_parse() {
        for expr in [
            "* * * * *",          // every minute
            "0 0 * * *",          // midnight daily
            "30 2 * * *",         // 02:30 daily
            "0 12 1 * *",         // noon on day 1 of every month
            "0 9 * * 1-5",        // 09:00 on weekdays (numeric range)
            "0 9 * * MON-FRI",    // 09:00 on weekdays (named range)
            "*/15 * * * *",       // every 15 minutes (step)
            "0,15,30,45 * * * *", // every quarter hour (list)
            "0 0 1,15 * *",       // 1st & 15th at midnight (DOM list)
            "0 0 * 1,7 *",        // Jan & Jul at midnight (month list)
            "0 0 * 1-3 *",        // Jan-Mar at midnight (month range)
            "59 23 31 12 *",      // last minute of the year
        ] {
            assert!(parse(expr).is_ok(), "expected `{expr}` to parse");
        }
    }

    #[test]
    fn invalid_patterns_reject() {
        for expr in [
            "",            // empty
            "not a cron",  // garbage
            "60 * * * *",  // minute out of range (0-59)
            "* 24 * * *",  // hour out of range (0-23)
            "* * 32 * *",  // DOM out of range (1-31)
            "* * * 13 *",  // month out of range (1-12)
            "* * * * 8",   // DOW out of range (0-7 / 0-6 with both Sun)
            "* * *",       // too few fields
            "*/0 * * * *", // zero step is nonsense
        ] {
            assert!(parse(expr).is_err(), "expected `{expr}` to fail to parse");
        }
    }

    // ---- Parsing: 6- and 7-field shapes -------------------------------------

    #[test]
    fn six_field_with_seconds_parses() {
        // 6-field (with seconds) was supported in v2 and remains valid in v3.
        assert!(parse("0 */5 * * * *").is_ok());
        assert!(parse("30 0 12 * * *").is_ok());
    }

    #[test]
    fn seven_field_with_year_parses_in_v3() {
        // Behavior change vs. croner v2: the 7-field pattern (seconds + year)
        // was rejected by v2 and is now accepted by default in v3.
        // We don't advertise this in docs, but we lock in the new parser
        // behavior so a future regression is caught.
        assert!(
            parse("0 30 23 * * FRI 2026-2030").is_ok(),
            "croner v3 default config should accept 7-field patterns with year",
        );
    }

    // ---- Runtime: find_next_occurrence --------------------------------------

    #[test]
    fn next_occurrence_every_5_minutes() {
        // From 12:01:00 the next */5 boundary is 12:05:00.
        let cron = parse("*/5 * * * *").unwrap();
        let now = at(2026, 5, 1, 12, 1, 0);
        let next = cron.find_next_occurrence(&now, false).unwrap();
        assert_eq!(next, at(2026, 5, 1, 12, 5, 0));
    }

    #[test]
    fn next_occurrence_hourly_on_zero() {
        // From 14:30:00 the next "0 * * * *" boundary is 15:00:00.
        let cron = parse("0 * * * *").unwrap();
        let now = at(2026, 5, 1, 14, 30, 0);
        let next = cron.find_next_occurrence(&now, false).unwrap();
        assert_eq!(next, at(2026, 5, 1, 15, 0, 0));
    }

    #[test]
    fn next_occurrence_weekday_morning() {
        // 0 9 * * MON-FRI from a Saturday should land on the following Monday.
        let cron = parse("0 9 * * MON-FRI").unwrap();
        // 2026-05-02 is a Saturday.
        let saturday = at(2026, 5, 2, 12, 0, 0);
        let next = cron.find_next_occurrence(&saturday, false).unwrap();
        // Monday 2026-05-04 09:00:00 UTC.
        assert_eq!(next, at(2026, 5, 4, 9, 0, 0));
    }

    #[test]
    fn next_occurrence_inclusive_flag_returns_now_when_matching() {
        // The `inclusive=true` flag: if `now` itself matches, return `now`.
        // The scheduler currently passes `inclusive=false`, but we lock in
        // both semantics so a flip in croner defaults would surface here.
        let cron = parse("0 * * * *").unwrap();
        let on_the_hour = at(2026, 5, 1, 14, 0, 0);
        assert_eq!(cron.find_next_occurrence(&on_the_hour, true).unwrap(), on_the_hour);
        // With `inclusive=false`, we should advance to the next hour.
        assert_eq!(
            cron.find_next_occurrence(&on_the_hour, false).unwrap(),
            at(2026, 5, 1, 15, 0, 0),
        );
    }

    #[test]
    fn next_occurrence_advances_across_month_boundary() {
        // "0 0 1 * *" from 31 Jan 2026 23:59:59 should jump to 1 Feb 2026 00:00.
        let cron = parse("0 0 1 * *").unwrap();
        let now = at(2026, 1, 31, 23, 59, 59);
        let next = cron.find_next_occurrence(&now, false).unwrap();
        assert_eq!(next, at(2026, 2, 1, 0, 0, 0));
    }

    #[test]
    fn next_occurrence_advances_across_year_boundary() {
        // From 31 Dec 2026 23:55, `*/10 * * * *` next fires at 1 Jan 2027 00:00.
        let cron = parse("*/10 * * * *").unwrap();
        let now = at(2026, 12, 31, 23, 55, 0);
        let next = cron.find_next_occurrence(&now, false).unwrap();
        assert_eq!(next, at(2027, 1, 1, 0, 0, 0));
    }
}

#[cfg(test)]
mod column_name_tests {
    use super::*;

    #[test]
    fn cron_column_identity_binds_tasks_to_the_stored_destination() {
        let manifest: Manifest = serde_json::from_value(serde_json::json!({
            "name":"Example","project_type":"no-code","networks":[],
            "contracts":[{"name":"Token","abi":"unused.json","details":[],
                "tables":[{"name":"stats","global":true,"columns":[{"name":"total32","type":"uint256"}],
                    "cron":[{"interval":"1h","network":"test","operations":[
                        {"type":"upsert","set":[{"column":"total32","action":"set","value":"1"}]}
                    ]}]}]}]
        })).unwrap();
        let scheduler =
            CronScheduler::try_new(&manifest, None, None, Arc::new(HashMap::new())).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);
        let task = &scheduler.tasks[0];
        assert_eq!(task.table.columns()[0].name, "total_32");
        assert_eq!(task.cron_entry.operations[0].set[0].column, "total_32");
    }
}

/// Drive the production address lookup and exactly one historical batch, without
/// the polling loop that would keep retrying a missing discovery table.
#[cfg(test)]
impl CronScheduler {
    pub(super) async fn sample_factory_for_test(
        &self,
        blocks: &[u64],
    ) -> Result<HashMap<Address, u64>, String> {
        let [task] = self.tasks.as_slice() else { return Err("expected one cron task".into()) };
        let addresses = get_factory_addresses_with_blocks(task, &self.postgres, &self.clickhouse)
            .await
            .map_err(|error| error.to_string())?;
        execute_cron_operations_batch(
            task,
            self.postgres.clone(),
            self.clickhouse.clone(),
            self.providers.clone(),
            blocks,
            &addresses,
            Arc::new(AdaptiveRateLimiter::new()),
        )
        .await?;
        Ok(addresses)
    }
}

#[cfg(test)]
#[path = "cron_historical_failure_tests.rs"]
mod cron_historical_failure_tests;

#[cfg(test)]
#[path = "cron_atomic_tests.rs"]
mod cron_atomic_tests;

#[cfg(test)]
#[path = "cron_clickhouse_tests.rs"]
mod cron_clickhouse_tests;

#[cfg(test)]
#[path = "cron_live_supervision_tests.rs"]
mod cron_live_supervision_tests;
