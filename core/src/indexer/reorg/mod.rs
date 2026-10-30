pub mod coordinator;
pub mod persistence;
pub mod task;
pub mod window;

#[cfg(test)]
mod same_height_tests;

use alloy::primitives::{B256, U64};
use anyhow::Context;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex as StdMutex, Weak},
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::event::callback_registry::{EventCallbackRegistry, TraceCallbackRegistry};
use crate::indexer::fetch_logs::ReorgInfo;
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::ChainProvider;

pub use coordinator::ReorgCoordinator;
pub use persistence::ReorgBlockHashPersistence;
pub use task::{
    AffectedTable, DerivedColumnJournal, DerivedColumnRollback, DerivedTableInfo,
    DerivedTableRollbackOp, DerivedTableRollbackPlan, EventTableInfo,
};
pub use window::BlockChainWindow;

static EVENT_WRITER_BARRIERS: LazyLock<StdMutex<HashMap<String, Arc<RwLock<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

#[derive(Default)]
struct StreamRecovery {
    next_unfinished: Option<u64>,
    pending_rewind: Option<u64>,
}
#[derive(Default)]
struct NetworkRecovery {
    generation: u64,
    callbacks: tokio_util::sync::CancellationToken,
    subscribers: Vec<Weak<StdMutex<StreamRecovery>>>,
}
static NETWORK_RECOVERY: LazyLock<StdMutex<HashMap<String, NetworkRecovery>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Each fetcher consumes its own recovery boundary. Include unfinished queued
/// work, and retain the earliest required block across several rollbacks.
#[derive(Clone)]
pub(crate) struct ReorgSubscription {
    network: String,
    position: Arc<StdMutex<StreamRecovery>>,
}

impl ReorgSubscription {
    // Consume the rewind and snapshot its generation under the publication lock.
    // A rollback after this point leaves another pending rewind and invalidates work.
    pub(crate) fn snapshot(&self) -> (u64, Option<u64>) {
        let recovery = NETWORK_RECOVERY.lock().unwrap_or_else(|e| e.into_inner());
        let generation = recovery.get(&self.network).map_or(0, |state| state.generation);
        let rewind = self.position.lock().unwrap_or_else(|e| e.into_inner()).pending_rewind.take();
        (generation, rewind)
    }

    /// Fetching ahead does not finish a range. Only ordered callback completion
    /// advances this boundary, so discarded queued work stays scheduled on rollback.
    pub(crate) fn start_at(&self, first_unfinished: u64) {
        let mut position = self.position.lock().unwrap_or_else(|e| e.into_inner());
        if position.next_unfinished.is_none() {
            let first = position
                .pending_rewind
                .map_or(first_unfinished, |rewind| rewind.min(first_unfinished));
            position.next_unfinished = Some(first);
            if let Some(rewind) = position.pending_rewind.as_mut() {
                *rewind = first;
            }
        }
    }

    /// The caller must hold the current generation's writer read guard so an old
    /// callback cannot raise this boundary after rollback has lowered it.
    pub(crate) fn completed_through(&self, block: u64) {
        let mut position = self.position.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(next) = position.next_unfinished.as_mut() {
            *next = (*next).max(block.saturating_add(1));
        }
    }

    pub(crate) fn first_unfinished(&self) -> Option<u64> {
        self.position.lock().unwrap_or_else(|e| e.into_inner()).next_unfinished
    }

    #[cfg(test)]
    pub(crate) fn take_rewind(&self) -> Option<u64> {
        self.snapshot().1
    }
}

/// Snapshot before fetching a manually constructed `FetchLogsResult`. The
/// dispatcher rejects responses if a network rollback completes after this point.
pub fn reorg_generation(network: &str) -> u64 {
    NETWORK_RECOVERY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(network)
        .map_or(0, |state| state.generation)
}

pub(crate) fn callback_recovery_token(network: &str) -> tokio_util::sync::CancellationToken {
    NETWORK_RECOVERY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(network.to_string())
        .or_default()
        .callbacks
        .clone()
}

pub(crate) fn retire_callbacks(network: &str) {
    callback_recovery_token(network).cancel();
}

pub(crate) fn subscribe_reorgs(network: &str) -> ReorgSubscription {
    let position = Arc::new(StdMutex::new(StreamRecovery::default()));
    let mut recovery = NETWORK_RECOVERY.lock().unwrap_or_else(|e| e.into_inner());
    let state = recovery.entry(network.to_string()).or_default();
    state.subscribers.retain(|entry| entry.strong_count() > 0);
    state.subscribers.push(Arc::downgrade(&position));
    ReorgSubscription { network: network.to_string(), position }
}

fn publish_rewind(network: &str, fork_point: u64) {
    let mut recovery = NETWORK_RECOVERY.lock().unwrap_or_else(|e| e.into_inner());
    let state = recovery.entry(network.to_string()).or_default();
    state.generation = state.generation.checked_add(1).expect("reorg generation exhausted");
    state.callbacks = tokio_util::sync::CancellationToken::new();
    state.subscribers.retain(|entry| {
        let Some(pending) = entry.upgrade() else { return false };
        let mut position = pending.lock().unwrap_or_else(|e| e.into_inner());
        let resume = position.next_unfinished.map_or(fork_point, |next| next.min(fork_point));
        if position.next_unfinished.is_some() {
            position.next_unfinished = Some(resume);
        }
        position.pending_rewind =
            Some(position.pending_rewind.map_or(resume, |old| old.min(resume)));
        true
    });
}

/// One read/write barrier per network: event callbacks hold a read guard while
/// writing, and reorg recovery holds the write guard from snapshot through rollback.
pub(crate) fn event_writer_barrier(network: &str) -> Arc<RwLock<()>> {
    let mut barriers = EVENT_WRITER_BARRIERS.lock().unwrap_or_else(|e| e.into_inner());
    barriers.entry(network.to_string()).or_default().clone()
}

/// Canonical-history decisions need the complete requested connected sequence.
/// Normalize RPC ordering, but never infer missing headers or accept a partial read.
pub(crate) async fn fetch_canonical_headers(
    provider: &dyn ChainProvider,
    requested: &[U64],
) -> anyhow::Result<Vec<alloy::network::AnyRpcBlock>> {
    let mut blocks = provider
        .get_block_by_number_batch(requested, false)
        .await
        .context("Cannot fetch canonical block headers")?;
    anyhow::ensure!(
        blocks.len() == requested.len(),
        "Cannot validate canonical history: requested {} headers but received {}",
        requested.len(),
        blocks.len()
    );
    blocks.sort_by_key(|block| block.header.number);
    for (number, block) in requested.iter().zip(&blocks) {
        anyhow::ensure!(number.to::<u64>() == block.header.number,
            "Cannot validate canonical history at block {}: response contains block {} instead (missing, repeated or unrequested header)", number, block.header.number);
    }
    for pair in blocks.windows(2) {
        anyhow::ensure!(
            pair[0].header.number.checked_add(1) == Some(pair[1].header.number)
                && pair[1].header.parent_hash == pair[0].header.hash,
            "Cannot validate canonical history: block {} does not connect to block {}",
            pair[1].header.number,
            pair[0].header.number
        );
    }
    Ok(blocks)
}

/// Validates that a string is safe to use as a SQL identifier (table name, column name, schema).
/// Rejects anything that doesn't match `[a-zA-Z_][a-zA-Z0-9_]*`.
pub fn validate_sql_identifier(name: &str, kind: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "{kind} must not be empty");
    let first = name.as_bytes()[0];
    anyhow::ensure!(
        first.is_ascii_alphabetic() || first == b'_',
        "{kind} '{name}' must start with a letter or underscore"
    );
    anyhow::ensure!(
        name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "{kind} '{name}' contains invalid characters (only alphanumeric and underscore allowed)"
    );
    Ok(())
}

/// Rejects dangerous tokens in a raw SQL snippet.
/// This does not establish event-filter syntax or evaluation semantics. Recovery
/// predicates use the filter parser and renderer instead of this SQL token check.
pub fn validate_sql_condition(condition: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!condition.is_empty(), "SQL condition must not be empty");
    // Reject injection patterns (comments, statement terminators, string literals)
    for pattern in ["--", "/*", "*/", ";", "'", "\""] {
        anyhow::ensure!(
            !condition.contains(pattern),
            "SQL condition contains forbidden pattern '{pattern}': {condition}"
        );
    }
    // Reject dangerous SQL keywords (case-insensitive)
    let upper = condition.to_ascii_uppercase();
    for keyword in ["UNION", "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "EXEC", "SLEEP"] {
        anyhow::ensure!(
            !upper.contains(keyword),
            "SQL condition contains forbidden keyword '{keyword}': {condition}"
        );
    }
    Ok(())
}

/// Validates that a string is safe to use as a SQL string literal value
/// (e.g. network names in `WHERE network = '...'`).
/// Allows alphanumeric, underscore, and hyphen.
pub fn validate_sql_value(value: &str, kind: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "{kind} must not be empty");
    anyhow::ensure!(
        value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "{kind} '{value}' contains invalid characters (only alphanumeric, underscore, and hyphen allowed)"
    );
    Ok(())
}

/// Bundles the optional DB clients and callback registries that reorg recovery
/// needs, avoiding parameter sprawl. Stream-retraction fan-out lives on the
/// `ReorgCoordinator` itself so a single detection covers every pipeline on
/// the network.
pub struct ReorgContext<'a> {
    pub postgres: Option<&'a PostgresClient>,
    pub clickhouse: Option<&'a Arc<ClickhouseClient>>,
    pub registry: Option<&'a EventCallbackRegistry>,
    pub trace_registry: Option<&'a TraceCallbackRegistry>,
}

impl ReorgContext<'_> {
    // Resolve source networks from the running generation's provider registry.
    // A reorg on one network may restore shared rows contributed by another.
    fn recovery_providers(&self) -> HashMap<String, Arc<dyn ChainProvider>> {
        let mut providers = HashMap::new();
        if let Some(registry) = self.registry {
            for event in &registry.events {
                providers.extend(
                    event
                        .providers
                        .iter()
                        .map(|(network, provider)| (network.clone(), provider.clone())),
                );
                for detail in &event.contract.details {
                    providers
                        .entry(detail.network.clone())
                        .or_insert_with(|| detail.cached_provider.clone());
                }
            }
        }
        if let Some(registry) = self.trace_registry {
            for event in &registry.events {
                for detail in &event.trace_information.details {
                    providers
                        .entry(detail.network.clone())
                        .or_insert_with(|| detail.cached_provider.clone());
                }
            }
        }
        providers
    }
}

async fn quarantine_exhausted_history(
    coordinator: &mut ReorgCoordinator,
    error: &anyhow::Error,
    ctx: &ReorgContext<'_>,
) {
    let Some(history) = error.downcast_ref::<coordinator::RecoveryHistoryExceeded>() else {
        return;
    };
    // Wake blocked admissions before taking the write barrier. Callback retry
    // retirement still lets an active attempt resolve its transaction first.
    if let Some(registry) = ctx.registry {
        for event in &registry.events {
            if event.contract.details.iter().any(|detail| detail.network == history.network) {
                registry.failure_state().record_stream(
                    crate::event::callback_registry::FailedStream {
                        id: event.id.clone(),
                        indexer: event.indexer_name.clone(),
                        contract: event.contract.name.clone(),
                        event: event.event_name.clone(),
                        network: history.network.clone(),
                        from_block: history.oldest,
                        to_block: history.latest,
                    },
                    history.to_string(),
                );
            }
        }
    }
    if let Some(registry) = ctx.trace_registry {
        for event in &registry.events {
            if event
                .trace_information
                .details
                .iter()
                .any(|detail| detail.network == history.network)
            {
                registry.failure_state().record_stream(
                    crate::event::callback_registry::FailedStream {
                        id: event.id.clone(),
                        indexer: event.indexer_name.clone(),
                        contract: event.contract_name.clone(),
                        event: event.event_name.clone(),
                        network: history.network.clone(),
                        from_block: history.oldest,
                        to_block: history.latest,
                    },
                    history.to_string(),
                );
            }
        }
    }
    coordinator.quarantine_history().await;
}

/// Returns `Some(fork_point)` if a reorg was detected and handled (caller should rewind and `continue`),
/// or `None` if no reorg was detected. Returns `Err` if detection or rollback failed.
pub async fn detect_and_handle_reorg(
    coordinator: &mut ReorgCoordinator,
    block_number: u64,
    block_hash: B256,
    parent_hash: B256,
    log_prefix: &str,
    ctx: &ReorgContext<'_>,
) -> anyhow::Result<Option<u64>> {
    let pending = coordinator.retry_pending_reorg(ctx).await;
    if let Err(error) = &pending {
        quarantine_exhausted_history(coordinator, error, ctx).await;
    }
    if let Some(fork_point) = pending.with_context(|| {
        format!("{log_prefix} - Pending reorg recovery failed before block {block_number}")
    })? {
        return Ok(Some(fork_point));
    }
    let detected = coordinator.on_new_block(block_number, block_hash, parent_hash).await;
    if let Err(error) = &detected {
        quarantine_exhausted_history(coordinator, error, ctx).await;
    }
    let Some(reorg_task) = detected.with_context(|| {
        format!("{log_prefix} - Reorg detection failed on block {block_number}")
    })?
    else {
        return Ok(None);
    };

    let fork_point = reorg_task.fork_point;
    let depth = reorg_task.detection_point.saturating_sub(fork_point) + 1;
    warn!(
        "{} - REORG DETECTED by coordinator on block {} (fork_point: {}, depth: {}). Executing rollback.",
        log_prefix, block_number, fork_point, depth,
    );

    coordinator.handle_reorg(reorg_task, ctx).await.with_context(|| {
        format!("{} - Reorg rollback failed for fork_point {}", log_prefix, fork_point)
    })?;

    Ok(Some(fork_point))
}

/// Broadcast event sent when a reorg is detected and recovery is complete.
/// Available in code-gen mode via `EventContext::reorg_receiver()`.
#[derive(Debug, Clone, Serialize)]
pub struct ReorgEvent {
    pub network: String,
    pub fork_block: u64,
    pub depth: u64,
    pub affected_tx_hashes: Vec<B256>,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
}

/// Handles chain state notifications (reorgs, reverts, commits).
/// Used by reth feature-gated providers that emit chain state events.
/// Returns `Some(ReorgInfo)` when a reorg/revert is detected, so the caller
/// can forward it to the main indexing loop for recovery.
pub fn handle_chain_notification(
    notification: ChainStateNotification,
    info_log_name: &str,
    network: &str,
) -> Option<ReorgInfo> {
    match notification {
        ChainStateNotification::Reorged {
            revert_from_block,
            revert_to_block,
            new_from_block,
            new_to_block,
            new_tip_hash,
        } => {
            // reth's `old_range.start()/end()` — `revert_from` is the lowest
            // reverted block, `revert_to` the highest, both inclusive. The
            // reverted span is `[revert_from, revert_to]`.
            let depth = revert_to_block.saturating_sub(revert_from_block) + 1;
            metrics::record_reorg(network, depth);

            warn!(
                "{} - REORG (reth): revert blocks {} to {}, re-index {} to {} (new tip: {})",
                info_log_name,
                revert_from_block,
                revert_to_block,
                new_from_block,
                new_to_block,
                new_tip_hash
            );

            Some(ReorgInfo {
                fork_block: U64::from(revert_from_block),
                depth,
                affected_tx_hashes: vec![],
            })
        }
        ChainStateNotification::Reverted { from_block, to_block } => {
            let depth = to_block.saturating_sub(from_block) + 1;
            metrics::record_reorg(network, depth);

            warn!(
                "{} - CHAIN REVERTED (reth): blocks {} to {} have been reverted",
                info_log_name, from_block, to_block
            );

            Some(ReorgInfo { fork_block: U64::from(from_block), depth, affected_tx_hashes: vec![] })
        }
        ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
            debug!(
                "{} - Chain committed: blocks {} to {} (tip: {})",
                info_log_name, from_block, to_block, tip_hash
            );
            None
        }
    }
}

/// Effective `reorg_safe_distance` (in blocks) for the finalized-delivery
/// buffer on `network`. Equal to the maximum distance demanded by any contract
/// or native-transfer config that touches the network: a contract that sets
/// `reorg_safe_distance: custom(2)` wants a tighter finality window, so we
/// honor it by not holding events longer. Any source that leaves
/// `reorg_safe_distance` unset (or sets `false`, meaning disabled) contributes
/// the chain default to the max, so unset configs never shrink the buffer
/// below the safe chain-level value.
///
/// Returns the chain default when no contract or NT config touches the
/// network (unusual — would mean the network has no indexing targets).
pub fn finalized_buffer_distance_for_network(
    manifest: &crate::manifest::core::Manifest,
    network: &str,
    chain_id: u64,
) -> u64 {
    let chain_default = reorg_safe_distance_for_chain(chain_id);
    let mut observed: Vec<u64> = Vec::new();

    for contract in manifest.all_contracts() {
        if contract.details.iter().any(|d| d.network == network) {
            let d = contract
                .reorg_safe_distance
                .and_then(|rsd| rsd.resolve(chain_id))
                .unwrap_or(chain_default);
            observed.push(d);
        }
    }

    if manifest.native_transfers.enabled {
        let touches = match &manifest.native_transfers.networks {
            Some(nets) => nets.iter().any(|n| n.network == network),
            None => manifest.networks.iter().any(|n| n.name == network),
        };
        if touches {
            let d = manifest
                .native_transfers
                .reorg_safe_distance
                .and_then(|rsd| rsd.resolve(chain_id))
                .unwrap_or(chain_default);
            observed.push(d);
        }
    }

    observed.into_iter().max().unwrap_or(chain_default)
}

/// Returns the default safe reorg distance (in blocks) for a given chain.
/// Used when `reorg_safe_distance: true` in YAML (no custom override).
pub fn reorg_safe_distance_for_chain(chain_id: u64) -> u64 {
    match chain_id {
        // Ethereum mainnet — Casper FFG finality (~13 min, 2 epochs)
        1 => 20,
        // Polygon PoS — historically deep reorgs (157-block in Feb 2023)
        137 => 200,
        // Arbitrum One — sequencer-ordered, no observed reorgs
        42161 => 24,
        // Optimism — sequencer-ordered, no observed reorgs
        10 => 24,
        // Base — sequencer-ordered (Coinbase), no observed reorgs
        8453 => 24,
        // BNB Smart Chain — 3s blocks, DPoS
        56 => 24,
        // Avalanche C-Chain — sub-second finality with Snowman consensus
        43114 => 12,
        // Gnosis Chain (xDai) — POSDAO/AuRa consensus
        100 => 24,
        // All other chains — conservative default
        _ => 64,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn stream_recovery_uses_completed_callbacks_instead_of_fetched_ranges() {
        let stream = super::subscribe_reorgs("completed-prefix-recovery");
        stream.start_at(81);
        let dispatcher = stream.clone();
        super::publish_rewind("completed-prefix-recovery", 98);
        assert_eq!(stream.take_rewind(), Some(81), "buffered work is unfinished");
        dispatcher.completed_through(90);
        super::publish_rewind("completed-prefix-recovery", 98);
        assert_eq!(stream.take_rewind(), Some(91), "completed prefix need not be refetched");
        dispatcher.completed_through(100);
        super::publish_rewind("completed-prefix-recovery", 98);
        assert_eq!(stream.take_rewind(), Some(98), "completed history is rewound to the fork");
    }

    #[test]
    fn fetchers_consume_rewinds_independently_and_keep_the_earliest_fork() {
        let a = super::subscribe_reorgs("coalesced-rewinds");
        let b = super::subscribe_reorgs("coalesced-rewinds");
        super::publish_rewind("coalesced-rewinds", 98);
        assert_eq!(a.take_rewind(), Some(98));
        super::publish_rewind("coalesced-rewinds", 101);
        assert_eq!(a.take_rewind(), Some(101));
        assert_eq!(b.take_rewind(), Some(98));
        assert_eq!(b.take_rewind(), None);
        assert_eq!(a.take_rewind(), None);
    }

    use super::*;

    #[tokio::test]
    async fn initialization_keeps_recovery_and_completion_boundaries_aligned() {
        for (first, expected) in [(101, 98), (81, 81)] {
            let network = format!("bootstrap-{first}");
            let subscription = subscribe_reorgs(&network);
            let writer = event_writer_barrier(&network).write_owned().await;
            publish_rewind(&network, 98);
            drop(writer);
            subscription.start_at(first);
            assert_eq!(subscription.first_unfinished(), Some(expected));
            assert_eq!(subscription.take_rewind(), Some(expected));
        }
    }

    use crate::manifest::contract::ReorgSafeDistance;

    // ======================================================================
    // validate_sql_identifier / validate_sql_value
    // ======================================================================

    #[test]
    fn test_validate_sql_identifier_valid() {
        assert!(validate_sql_identifier("users", "test").is_ok());
        assert!(validate_sql_identifier("_private", "test").is_ok());
        assert!(validate_sql_identifier("table_123", "test").is_ok());
        assert!(validate_sql_identifier("CamelCase", "test").is_ok());
    }

    #[test]
    fn test_validate_sql_identifier_rejects_invalid() {
        assert!(validate_sql_identifier("", "test").is_err());
        assert!(validate_sql_identifier("123start", "test").is_err());
        assert!(validate_sql_identifier("has space", "test").is_err());
        assert!(validate_sql_identifier("has-hyphen", "test").is_err());
        assert!(validate_sql_identifier("drop;--", "test").is_err());
        assert!(validate_sql_identifier("table'name", "test").is_err());
    }

    #[test]
    fn test_validate_sql_value_valid() {
        assert!(validate_sql_value("ethereum", "test").is_ok());
        assert!(validate_sql_value("polygon-mainnet", "test").is_ok());
        assert!(validate_sql_value("bsc_testnet", "test").is_ok());
        assert!(validate_sql_value("chain123", "test").is_ok());
    }

    #[test]
    fn test_validate_sql_value_rejects_invalid() {
        assert!(validate_sql_value("", "test").is_err());
        assert!(validate_sql_value("has space", "test").is_err());
        assert!(validate_sql_value("net'; DROP TABLE --", "test").is_err());
        assert!(validate_sql_value("name'quote", "test").is_err());
    }

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        assert_eq!(reorg_safe_distance_for_chain(1), 20); // Ethereum
        assert_eq!(reorg_safe_distance_for_chain(137), 200); // Polygon
        assert_eq!(reorg_safe_distance_for_chain(42161), 24); // Arbitrum
        assert_eq!(reorg_safe_distance_for_chain(10), 24); // Optimism
        assert_eq!(reorg_safe_distance_for_chain(8453), 24); // Base
        assert_eq!(reorg_safe_distance_for_chain(56), 24); // BNB
        assert_eq!(reorg_safe_distance_for_chain(43114), 12); // Avalanche
        assert_eq!(reorg_safe_distance_for_chain(100), 24); // Gnosis
        assert_eq!(reorg_safe_distance_for_chain(999), 64); // Unknown chain
    }

    // ======================================================================
    // ReorgSafeDistance serde (untagged enum: bool | u64)
    // ======================================================================

    #[test]
    fn test_reorg_safe_distance_serde_true() {
        let yaml = "true";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Enabled(true)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_false() {
        let yaml = "false";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Enabled(false)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_custom_u64() {
        let yaml = "200";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Custom(200)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_roundtrip() {
        let variants = vec![
            ReorgSafeDistance::Enabled(true),
            ReorgSafeDistance::Enabled(false),
            ReorgSafeDistance::Custom(42),
        ];
        for original in variants {
            let yaml = serde_yaml::to_string(&original).unwrap();
            let parsed: ReorgSafeDistance = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(original.resolve(1), parsed.resolve(1));
        }
    }

    // ======================================================================
    // ReorgSafeDistance::resolve()
    // ======================================================================

    #[test]
    fn test_resolve_enabled_true_uses_chain_default() {
        let rsd = ReorgSafeDistance::Enabled(true);
        assert_eq!(rsd.resolve(137), Some(200)); // Polygon default
        assert_eq!(rsd.resolve(1), Some(20)); // Ethereum default
        assert_eq!(rsd.resolve(999), Some(64)); // Unknown chain fallback
    }

    #[test]
    fn test_resolve_enabled_false_returns_none() {
        let rsd = ReorgSafeDistance::Enabled(false);
        assert_eq!(rsd.resolve(137), None);
        assert_eq!(rsd.resolve(1), None);
    }

    #[test]
    fn test_resolve_custom_overrides_chain_default() {
        let rsd = ReorgSafeDistance::Custom(500);
        assert_eq!(rsd.resolve(137), Some(500));
        assert_eq!(rsd.resolve(1), Some(500));
        assert_eq!(rsd.resolve(999), Some(500));
    }

    // ======================================================================
    // handle_chain_notification()
    // ======================================================================

    #[test]
    fn test_handle_chain_notification_reorged() {
        // reth emits `revert_from_block = old_range.start()` (lowest reverted)
        // and `revert_to_block = old_range.end()` (highest reverted), both
        // inclusive. Blocks 101..=110 reverted → 10 blocks, fork_block=101.
        let notification = ChainStateNotification::Reorged {
            revert_from_block: 101,
            revert_to_block: 110,
            new_from_block: 101,
            new_to_block: 112,
            new_tip_hash: B256::from([0xab; 32]),
        };
        let result = handle_chain_notification(notification, "test", "polygon");
        assert!(result.is_some());
        let reorg = result.unwrap();
        assert_eq!(reorg.fork_block, U64::from(101));
        assert_eq!(reorg.depth, 10);
        assert!(reorg.affected_tx_hashes.is_empty());
    }

    #[test]
    fn test_handle_chain_notification_reverted() {
        // Blocks 196..=200 reverted → 5 blocks, fork_block=196.
        let notification = ChainStateNotification::Reverted { from_block: 196, to_block: 200 };
        let result = handle_chain_notification(notification, "test", "ethereum");
        assert!(result.is_some());
        let reorg = result.unwrap();
        assert_eq!(reorg.fork_block, U64::from(196));
        assert_eq!(reorg.depth, 5);
    }

    #[test]
    fn test_handle_chain_notification_committed_returns_none() {
        let notification = ChainStateNotification::Committed {
            from_block: 100,
            to_block: 200,
            tip_hash: B256::ZERO,
        };
        let result = handle_chain_notification(notification, "test", "polygon");
        assert!(result.is_none());
    }

    // ======================================================================
    // ReorgEvent serialization
    // ======================================================================

    #[test]
    fn test_reorg_event_serialization() {
        let tx_hash = B256::from([0xde; 32]);
        let event = ReorgEvent {
            network: "polygon".to_string(),
            fork_block: 1000,
            depth: 3,
            affected_tx_hashes: vec![tx_hash],
            indexer_name: "TestIndexer".to_string(),
            contract_name: "USDC".to_string(),
            event_name: "Transfer".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["network"], "polygon");
        assert_eq!(json["fork_block"], 1000);
        assert_eq!(json["depth"], 3);
        assert_eq!(json["indexer_name"], "TestIndexer");
        assert_eq!(json["contract_name"], "USDC");
        assert_eq!(json["event_name"], "Transfer");

        let hashes = json["affected_tx_hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        let hash_str = hashes[0].as_str().unwrap();
        assert!(hash_str.starts_with("0x"));
        assert_eq!(hash_str.len(), 66); // "0x" + 64 hex chars
    }

    #[test]
    fn test_reorg_event_empty_tx_hashes() {
        let event = ReorgEvent {
            network: "ethereum".to_string(),
            fork_block: 5000,
            depth: 1,
            affected_tx_hashes: vec![],
            indexer_name: "Idx".to_string(),
            contract_name: "DAI".to_string(),
            event_name: "Approval".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert!(json["affected_tx_hashes"].as_array().unwrap().is_empty());
    }
}
