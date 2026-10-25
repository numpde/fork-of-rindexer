use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::consensus::Transaction;
use alloy::network::{ReceiptResponse, TransactionResponse};
use alloy::transports::{RpcError, TransportErrorKind};
use alloy::{
    primitives::{Address, Bytes, U256, U64},
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace},
};
use futures::future::try_join_all;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, info, warn};

use tokio_util::sync::CancellationToken;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::indexer::reorg::{
    detect_and_handle_reorg, event_writer_barrier, ReorgContext, ReorgCoordinator,
    ReorgSubscription,
};
use crate::is_running;
use crate::provider::RECOMMENDED_RPC_CHUNK_SIZE;
use crate::PostgresClient;
use crate::{
    event::{
        callback_registry::{CallbackError, TraceCallbackRegistry, TraceResult, TxInformation},
        config::TraceProcessingConfig,
    },
    indexer::{
        last_synced::evm_trace_update_progress_and_last_synced_task, process::ProcessEventError,
        task_tracker::ActiveCallback,
    },
    manifest::native_transfer::TraceProcessingMethod,
    provider::{ChainProvider, ProviderError},
};

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const NATIVE_TRANSFER_CONTRACT_NAME: &str = "EvmTraces";

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const EVENT_NAME: &str = "NativeTransfer";

/// Invent an ABI to mimic an ERC20 Transfer.
///
/// This will allow indexer consumers, which will typically be configured to consume contract events
/// to simply ingest an ERC20 compatible event for the native tokens.
///
/// In order to reduce name conflicts we will call the Transfer event `NativeTransfer`.
pub const NATIVE_TRANSFER_ABI: &str = r#"[{
    "anonymous": false,
    "inputs": [
        {
            "indexed": true,
            "name": "from",
            "type": "address"
        },
        {
            "indexed": true,
            "name": "to",
            "type": "address"
        },
        {
            "indexed": false,
            "name": "value",
            "type": "uint256"
        }
    ],
    "name": "NativeTransfer",
    "type": "event"
}]"#;

/// Refer to [`NATIVE_TRANSFER_ABI`] as an imaginary associated ABI for this Native Transfer
/// "event" struct.
#[derive(Debug, Clone, Serialize)]
pub struct NativeTransfer {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub transaction_information: TxInformation,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeBlockRequest {
    pub(crate) block_number: U64,
    pub(crate) generation: u64,
}

/// Producer and consumer share completion state; fetching ahead never finishes a prefix.
#[derive(Clone)]
pub struct NativeBlockSender {
    pub(crate) sender: mpsc::Sender<NativeBlockRequest>,
    pub(crate) recovery: ReorgSubscription,
}

pub struct NativeBlockReceiver {
    pub(crate) receiver: mpsc::Receiver<NativeBlockRequest>,
    pub(crate) recovery: ReorgSubscription,
}

/// Initialize the stream from its last completed/start boundary, before planning work.
pub fn native_block_channel(
    network: &str,
    start_after: U64,
    capacity: usize,
) -> (NativeBlockSender, NativeBlockReceiver) {
    let recovery = crate::indexer::reorg::subscribe_reorgs(network);
    recovery.start_at(start_after.to::<u64>().saturating_add(1));
    let (sender, receiver) = mpsc::channel(capacity);
    (
        NativeBlockSender { sender, recovery: recovery.clone() },
        NativeBlockReceiver { receiver, recovery },
    )
}

/// Each request keeps the generation sampled before its range's RPC validation.
async fn push_range(
    block_tx: &NativeBlockSender,
    last: U64,
    latest: U64,
    generation: u64,
    network: &str,
) {
    for block in last.to::<u64>()..=latest.to::<u64>() {
        if generation != crate::indexer::reorg::reorg_generation(network) {
            break;
        }
        let request = NativeBlockRequest { block_number: U64::from(block), generation };
        if let Err(error) = block_tx.sender.send(request).await {
            if is_running() {
                error!("Failed to send block via channel: {}", error);
            }
            break;
        }
    }
}

/// Result of a reorg check over a pending block range.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NativeTransferReorgOutcome {
    /// No coordinator configured; range can be emitted as-is.
    NoCoordinator,
    /// All blocks in the range are canonical. Safe to emit.
    Canonical,
    /// A reorg was handled. `rewind_to` is the first canonical block to re-fetch
    /// (inclusive) — the caller rewinds `last_seen_block` to `rewind_to - 1` so
    /// the next iteration's `from_block` equals `rewind_to`.
    Rewound { rewind_to: u64 },
    /// Detection or rollback failed. Caller should back off and retry.
    Failed,
}

/// Walk the next contiguous block range `from..=to` against the coordinator,
/// feeding each canonical block's `(number, hash, parent_hash)` through
/// `detect_and_handle_reorg`. The first reorg detected rewinds the cursor.
///
/// Extracted from `native_transfer_block_fetch` to keep the hot loop readable
/// and to provide a unit-testable seam for reorg rewind behaviour.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn native_transfer_detect_reorg_in_range(
    provider: &dyn ChainProvider,
    coordinator: Option<&Arc<Mutex<ReorgCoordinator>>>,
    postgres: Option<&PostgresClient>,
    clickhouse: Option<&Arc<ClickhouseClient>>,
    trace_registry: Option<&TraceCallbackRegistry>,
    network: &str,
    from_block: u64,
    to_block: u64,
) -> NativeTransferReorgOutcome {
    let Some(coordinator) = coordinator else {
        return NativeTransferReorgOutcome::NoCoordinator;
    };

    let block_numbers: Vec<U64> = (from_block..=to_block).map(U64::from).collect();
    let blocks = match provider.get_block_by_number_batch(&block_numbers, false).await {
        Ok(b) => b,
        Err(e) => {
            error!(
                network,
                from_block,
                to_block,
                error = %e,
                "Native transfer reorg pre-check: block header fetch failed",
            );
            return NativeTransferReorgOutcome::Failed;
        }
    };

    // Guard against short reads (RPC lag, chain-tip reorg). Advancing the caller's
    // cursor on a partial batch would leave a silent gap: blocks requested but not
    // returned would never be fed to the coordinator, and their native-transfer
    // rows would never be emitted. Treat as a transient failure so the caller
    // backs off and retries the same range.
    if blocks.len() != block_numbers.len() {
        warn!(
            network,
            from_block,
            to_block,
            requested = block_numbers.len(),
            received = blocks.len(),
            "Native transfer reorg pre-check: short read from provider, retrying",
        );
        return NativeTransferReorgOutcome::Failed;
    }

    // Mutex held across reorg handling (DB rollback, stream publishes in
    // parallel, user on_reorg callback firing). On a real reorg this blocks
    // the other indexing path for the duration of handle_reorg, which is
    // acceptable for isolation. If latency becomes a concern, move
    // handle_reorg out of the hot path.
    let mut guard = coordinator.lock().await;
    let ctx = ReorgContext { postgres, clickhouse, registry: None, trace_registry };

    for block in blocks {
        let number = block.header.number;
        let hash = block.header.hash;
        let parent_hash = block.header.parent_hash;
        match detect_and_handle_reorg(
            &mut guard,
            number,
            hash,
            parent_hash,
            "NativeTransfers",
            &ctx,
        )
        .await
        {
            Ok(Some(fork_point)) => {
                warn!(
                    network,
                    fork_point,
                    detection_block = number,
                    "Rewinding native transfer fetch to fork point",
                );
                return NativeTransferReorgOutcome::Rewound { rewind_to: fork_point };
            }
            Ok(None) => {}
            Err(e) => {
                error!(
                    network,
                    block = number,
                    error = ?e,
                    "Native transfer reorg detection failed",
                );
                return NativeTransferReorgOutcome::Failed;
            }
        }
    }

    NativeTransferReorgOutcome::Canonical
}

/// Block publisher.
///
/// This is a long-running process designed to accept a [`NativeBlockSender`] handle and publish blocks
/// in an efficient manner which respects the user defined manifest block ranges.
///
/// This process respects channel backpressure and will only complete once the `end_block` is
/// reached.
#[allow(clippy::too_many_arguments)]
pub async fn native_transfer_block_fetch(
    publisher: Arc<dyn ChainProvider>,
    block_tx: NativeBlockSender,
    start_block: U64,
    end_block: Option<U64>,
    indexing_distance_from_head: U64,
    network: String,
    cancel_token: CancellationToken,
    postgres: Option<Arc<PostgresClient>>,
    _indexer_name: String,
    reorg_coordinator: Option<Arc<Mutex<ReorgCoordinator>>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    trace_registry: Arc<TraceCallbackRegistry>,
) -> Result<(), ProcessEventError> {
    let mut last_seen_block = start_block;
    let reorgs = &block_tx.recovery;

    loop {
        if !is_running() || cancel_token.is_cancelled() || block_tx.sender.is_closed() {
            info!("Exiting native transfer indexing block processor!");
            break Ok(());
        }

        // Pace the poll loop so the per-poll tip reorg check below doesn't spin
        // the CPU when the chain tip is idle. Mirrors the
        // `target_iteration_duration` used in the contract-event fetch loop.
        sleep(Duration::from_millis(200)).await;

        let (plan_generation, rewind) = reorgs.snapshot();
        if let Some(fork_point) = rewind {
            last_seen_block = last_seen_block.min(U64::from(fork_point.saturating_sub(1)));
        }

        let latest_block = publisher.get_latest_block().await;

        match latest_block {
            Ok(Some(latest_block)) => {
                // Per-poll tip reorg check: mirrors the contract-event path in
                // `fetch_logs.rs`. Runs on every iteration — even when no blocks
                // advanced — so we catch reorgs that rewrite the tip without
                // moving the block number (e.g. Anvil `anvil_reorg(depth)` that
                // keeps or lowers the tip height).
                if let Some(coordinator) = reorg_coordinator.as_ref() {
                    let tip_number = latest_block.header.number;
                    let tip_hash = latest_block.header.hash;
                    let tip_parent = latest_block.header.parent_hash;
                    let mut guard = coordinator.lock().await;
                    let ctx = ReorgContext {
                        postgres: postgres.as_deref(),
                        clickhouse: clickhouse.as_ref(),
                        registry: None,
                        trace_registry: Some(trace_registry.as_ref()),
                    };
                    match detect_and_handle_reorg(
                        &mut guard,
                        tip_number,
                        tip_hash,
                        tip_parent,
                        "NativeTransfers",
                        &ctx,
                    )
                    .await
                    {
                        Ok(Some(fork_point)) => {
                            drop(guard);
                            warn!(
                                network = %network,
                                fork_point,
                                "Native transfer tip reorg - rewinding fetch cursor",
                            );
                            last_seen_block =
                                last_seen_block.min(U64::from(fork_point.saturating_sub(1)));
                            continue;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            drop(guard);
                            error!(
                                network = %network,
                                error = ?e,
                                "Native transfer tip reorg detection failed",
                            );
                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }

                let block = U64::from(latest_block.header.number);

                // Always trim back to the safe indexing threshold (which is zero if disabled)
                let block = block - indexing_distance_from_head;

                let to_block = end_block.map(|end| block.min(end)).unwrap_or(block);
                if to_block > last_seen_block {
                    let from_block = last_seen_block + U64::from(1);

                    // Reorg detection runs on the fetch side so every block - even those
                    // with zero native transfers - feeds the coordinator's window, keeping
                    // it contiguous.
                    let outcome = native_transfer_detect_reorg_in_range(
                        publisher.as_ref(),
                        reorg_coordinator.as_ref(),
                        postgres.as_deref(),
                        clickhouse.as_ref(),
                        Some(trace_registry.as_ref()),
                        &network,
                        from_block.to::<u64>(),
                        to_block.to::<u64>(),
                    )
                    .await;

                    match outcome {
                        NativeTransferReorgOutcome::Canonical
                        | NativeTransferReorgOutcome::NoCoordinator => {
                            debug!("Pushing trace blocks {} - {}", from_block, to_block);
                            push_range(&block_tx, from_block, to_block, plan_generation, &network)
                                .await;
                            last_seen_block = to_block;
                        }
                        NativeTransferReorgOutcome::Rewound { rewind_to } => {
                            // `rewind_to` is the first canonical block to re-fetch (inclusive).
                            // Subtract 1 from the cursor so the next iteration computes
                            // `from_block = last_seen_block + 1 = rewind_to`, matching the
                            // coordinator's rollback which deletes rows with
                            // `block_number >= fork_point`.
                            last_seen_block =
                                last_seen_block.min(U64::from(rewind_to.saturating_sub(1)));
                            continue;
                        }
                        NativeTransferReorgOutcome::Failed => {
                            // Mirror contract-event backoff: pause before retrying.
                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }

                if end_block.is_some_and(|end| {
                    block > end
                        && reorgs.first_unfinished().is_some_and(|next| next > end.to::<u64>())
                }) {
                    info!("Finished HISTORICAL INDEXING for {} NativeEvmTraces. No more blocks to push.", network);
                    debug!("Dropping {} 'NativeEvmTraces' block Sender handle", network);
                    drop(block_tx);
                    return Ok(());
                }
            }
            Ok(None) => {}
            Err(e) => {
                error!("Error fetching '{}' blocks: {}", network, e.to_string());
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn native_transfer_block_processor(
    network_name: String,
    provider: Arc<dyn ChainProvider>,
    config: Arc<TraceProcessingConfig>,
    mut block_rx: NativeBlockReceiver,
) -> Result<(), ProcessEventError> {
    // Set the concurrency used to make requests based on the method.
    //
    // Currently, `eth_getBlockByNumber` is a single JSON-RPC batch, and others are individual
    // network calls so can be treated differently.
    let (initial_concurrent_requests, limit_concurrent_requests) = (5, RECOMMENDED_RPC_CHUNK_SIZE);

    let mut concurrent_requests: usize = initial_concurrent_requests;
    let mut buffer: Vec<NativeBlockRequest> = Vec::with_capacity(limit_concurrent_requests);

    loop {
        if let Some(failure) = config.terminal_failure() {
            return Err(failure.into());
        }
        if !is_running() || config.cancel_token.is_cancelled() {
            info!("Exiting native transfer indexing block processor!");
            break Ok(());
        }

        // Fetch more only if buffer was processed ok last time and cleared.
        let recv = if buffer.is_empty() {
            block_rx.receiver.recv_many(&mut buffer, concurrent_requests).await
        } else {
            buffer.len()
        };

        if recv == 0 {
            break Ok(());
        }

        let current_generation = crate::indexer::reorg::reorg_generation(&network_name);
        buffer.retain(|request| request.generation == current_generation);
        if buffer.is_empty() {
            continue;
        }
        let numbers: Vec<U64> = buffer.iter().map(|request| request.block_number).collect();
        let processed_block = native_transfer_block_consumer_at_generation(
            provider.clone(),
            &numbers,
            &network_name,
            &config,
            buffer[0].generation,
            Some(&block_rx.recovery),
        )
        .await;

        // Provider failures can retry the buffered range. A permanent callback
        // failure has already blocked this trace stream and must stop processing.
        if let Err(e) = processed_block {
            if matches!(e, ProcessEventError::Callback(CallbackError::Permanent(_))) {
                return Err(e);
            }
            // On error, drop the block query range. We want a slow increase in concurrency and a
            // relatively aggressive backoff.
            concurrent_requests = cmp::max(1, (concurrent_requests as f64 * 0.8) as usize);

            let is_rate_limit_error = matches!(&e, ProcessEventError::ProviderCallError(
                ProviderError::RequestFailed(
                    RpcError::Transport(
                        TransportErrorKind::HttpError(http_err)
                    )
                )) if http_err.status == 429
            );

            if is_rate_limit_error {
                warn!(
                    "Rate-limited 429 '{}' block requests. Retrying in 2s: {}",
                    network_name,
                    e.to_string(),
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            } else {
                warn!(
                    "Could not process '{}' block requests for {}..{}, Retrying in 500ms: {}",
                    network_name,
                    &buffer
                        .first()
                        .map(|request| request.block_number.as_limbs()[0])
                        .unwrap_or_else(|| 0),
                    &buffer
                        .last()
                        .map(|request| request.block_number.as_limbs()[0])
                        .unwrap_or_else(|| 0),
                    e.to_string(),
                );
            }

            sleep(Duration::from_secs(1)).await;
            continue;
        } else {
            buffer.clear();

            // A random chance of increasing the request count helps us not overload
            // the ratelimit too rapidly across multi-network trace indexing and have a
            // slow ramp-up time (if rpc batching isn't available)
            if rand::random_bool(0.1) {
                concurrent_requests =
                    ((concurrent_requests * 20) / 10).min(limit_concurrent_requests);
            }

            sleep(Duration::from_millis(50)).await;
        };
    }
}

async fn provider_trace_call(
    provider: Arc<dyn ChainProvider>,
    config: &TraceProcessingConfig,
    block: U64,
) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
    match config.method {
        TraceProcessingMethod::TraceBlock => provider.trace_block(block).await,
        TraceProcessingMethod::DebugTraceBlockByNumber => {
            provider.debug_trace_block_by_number(block).await
        }
        _ => unimplemented!("Unsupported trace method"),
    }
}

/// Index native transfers via batched rpc block call method
pub async fn native_transfer_block_consumer(
    provider: Arc<dyn ChainProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &Arc<TraceProcessingConfig>,
) -> Result<(), ProcessEventError> {
    let generation = crate::indexer::reorg::reorg_generation(network_name);
    native_transfer_block_consumer_at_generation(
        provider,
        block_numbers,
        network_name,
        config,
        generation,
        None,
    )
    .await
}

async fn native_transfer_block_consumer_at_generation(
    provider: Arc<dyn ChainProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &Arc<TraceProcessingConfig>,
    generation: u64,
    recovery: Option<&ReorgSubscription>,
) -> Result<(), ProcessEventError> {
    if let Some(failure) = config.terminal_failure() {
        return Err(failure.into());
    }
    if block_numbers.is_empty() {
        return Ok(());
    }
    let blocks = provider.get_block_by_number_batch(block_numbers, true).await?;
    let (from_block, to_block) = block_numbers
        .iter()
        .fold((U64::MAX, U64::ZERO), |(min, max), &num| (cmp::min(min, num), cmp::max(max, num)));
    // Validate the complete range and its candidates before either block or transfer callbacks.
    let mut remaining: HashSet<_> = block_numbers.iter().map(|number| number.to::<u64>()).collect();
    let mut candidates = Vec::new();
    let mut candidate_hashes = HashSet::new();
    for block in &blocks {
        let number = block.header.number;
        if !remaining.remove(&number) {
            return Err(ProviderError::CustomError(format!(
                "Cannot process native-transfer blocks on network {network_name:?}: RPC response contains unrequested or repeated block {number}; the requested range cannot be checkpointed"
            )).into());
        }
        let alloy::rpc::types::BlockTransactions::Full(transactions) = &block.transactions else {
            return Err(ProviderError::CustomError(format!(
                "Cannot process native transfers on network {network_name:?}, block {number}: RPC response omitted full transaction objects despite requesting them; the block cannot be checkpointed"
            )).into());
        };
        for (index, tx) in transactions.iter().enumerate() {
            if tx.to().is_none() || !tx.input().is_empty() || tx.value().is_zero() {
                continue;
            }
            let invalid_field = if tx.block_number != Some(number) {
                Some("block_number")
            } else if tx.block_hash != Some(block.header.hash) {
                Some("block_hash")
            } else if tx.transaction_index != Some(index as u64) {
                Some("transaction_index")
            } else {
                None
            };
            if let Some(field) = invalid_field {
                return Err(ProviderError::CustomError(format!(
                    "Cannot process native transfer on network {network_name:?}, transaction {:#x}: RPC {field} is missing or does not match enclosing block {number}, hash {:#x}, transaction index {index}; the range cannot be checkpointed", tx.tx_hash(), block.header.hash
                )).into());
            }
            if !candidate_hashes.insert(tx.tx_hash()) {
                return Err(ProviderError::CustomError(format!(
                    "Cannot process native transfers on network {network_name:?}: transaction {:#x} appears more than once in the requested range; the range cannot be checkpointed", tx.tx_hash()
                )).into());
            }
            candidates.push((block.header.timestamp, tx));
        }
    }
    if let Some(number) = remaining.iter().min() {
        return Err(ProviderError::CustomError(format!(
            "Cannot process native-transfer blocks on network {network_name:?}: RPC response omitted requested block {number}; the requested range cannot be checkpointed"
        )).into());
    }

    let hashes: Vec<_> = candidates.iter().map(|(_, tx)| tx.tx_hash()).collect();
    let receipts =
        if hashes.is_empty() { vec![] } else { provider.get_tx_receipts_batch(&hashes).await? };
    let mut receipts_by_hash = HashMap::new();
    for receipt in receipts {
        let hash = receipt.transaction_hash();
        if !candidate_hashes.contains(&hash) {
            return Err(ProviderError::CustomError(format!(
                "Cannot process native transfers on network {network_name:?}: RPC returned an unrequested receipt for transaction {hash:#x}; the range cannot be checkpointed"
            )).into());
        }
        if receipts_by_hash.insert(hash, receipt).is_some() {
            return Err(ProviderError::CustomError(format!(
                "Cannot process native transfers on network {network_name:?}: RPC repeated the receipt for transaction {hash:#x}; the range cannot be checkpointed"
            )).into());
        }
    }
    let mut native_transfers = Vec::new();
    for (timestamp, tx) in candidates {
        let invalid_receipt = |reason: &str| {
            ProviderError::CustomError(format!(
            "Cannot process native transfer on network {network_name:?}, transaction {:#x}: {reason}; the range cannot be checkpointed", tx.tx_hash()
        ))
        };
        let receipt = receipts_by_hash
            .remove(&tx.tx_hash())
            .ok_or_else(|| invalid_receipt("RPC omitted its execution receipt"))?;
        let mismatch = if receipt.block_hash() != tx.block_hash {
            Some(format!(
                "RPC receipt block_hash {:?} does not match processed block {:?}",
                receipt.block_hash(),
                tx.block_hash
            ))
        } else if receipt.block_number() != tx.block_number {
            Some(format!(
                "RPC receipt block_number {:?} does not match processed block {:?}",
                receipt.block_number(),
                tx.block_number
            ))
        } else if receipt.transaction_index() != tx.transaction_index {
            Some(format!(
                "RPC receipt transaction_index {:?} does not match processed transaction {:?}",
                receipt.transaction_index(),
                tx.transaction_index
            ))
        } else if receipt.from() != tx.from() {
            Some(format!(
                "RPC receipt sender {:?} does not match processed transaction sender {:?}",
                receipt.from(),
                tx.from()
            ))
        } else if receipt.to() != tx.to() {
            Some(format!(
                "RPC receipt recipient {:?} does not match processed transaction recipient {:?}",
                receipt.to(),
                tx.to()
            ))
        } else {
            None
        };
        if let Some(reason) = mismatch {
            return Err(invalid_receipt(&reason).into());
        }
        if receipt.state_root().is_some() {
            return Err(invalid_receipt(
                "RPC receipt has a pre-EIP-658 state root rather than an execution status",
            )
            .into());
        }
        // A reverted value transaction was included, but transferred no value.
        if !receipt.status() {
            continue;
        }
        native_transfers.push(
            TraceResult::try_new_native_transfer(
                tx.clone(),
                timestamp,
                tx.to().expect("native candidate has a recipient"),
                network_name,
                provider.chain().id(),
                from_block,
                to_block,
            )
            .map_err(ProviderError::CustomError)?,
        );
    }

    let writer_barrier = event_writer_barrier(network_name);
    let _writer_guard = writer_barrier.read().await;
    if generation != crate::indexer::reorg::reorg_generation(network_name) {
        return Err(ProviderError::CustomError(format!(
            "Cannot apply native-transfer batch on network {network_name:?}: a reorg completed while fetching; retry the block range"
        )).into());
    }

    if let Some(recovery) = recovery {
        let required = recovery.first_unfinished();
        let contiguous =
            block_numbers.windows(2).all(|pair| pair[0].checked_add(U64::from(1)) == Some(pair[1]));
        if required != Some(from_block.to::<u64>()) || !contiguous {
            return Err(ProviderError::CustomError(format!(
                "Cannot complete native-transfer range {from_block}..{to_block} on network {network_name:?}: first unfinished block is {required:?}; queued requests must form its contiguous prefix"
            )).into());
        }
    }

    // Empty transfer ranges still checkpoint after any block callbacks succeed.
    let _activity = ActiveCallback::start();

    let blocks = blocks
        .into_iter()
        .map(|b| {
            TraceResult::new_block(b, network_name, provider.chain().id(), from_block, to_block)
        })
        .collect::<Vec<_>>();
    config.trigger_event(blocks).await?;

    if !native_transfers.is_empty() {
        config.trigger_event(native_transfers).await?;
    }

    evm_trace_update_progress_and_last_synced_task(config.clone(), to_block, || {}).await;
    if let Some(recovery) = recovery {
        recovery.completed_through(to_block.to::<u64>());
    }

    Ok(())
}

// Indexing native transfers if debug or trace indexing is enabled.
///
/// NOTE: This is currently unused as we have temporarily migrated to exclusively using
/// `eth_getBlockByNumber` calls instead, this is being retained for posterity should we
/// choose to continue to support `debug` and `trace` based native transfer indexing.
#[allow(unused)]
pub async fn native_transfer_block_consumer_debug(
    provider: Arc<dyn ChainProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: Arc<TraceProcessingConfig>,
) -> Result<(), ProcessEventError> {
    if let Some(failure) = config.terminal_failure() {
        return Err(failure.into());
    }
    let generation = crate::indexer::reorg::reorg_generation(network_name);
    let trace_futures: Vec<_> =
        block_numbers.iter().map(|n| provider_trace_call(provider.clone(), &config, *n)).collect();
    let trace_calls = try_join_all(trace_futures).await?;
    let (from_block, to_block) = block_numbers
        .iter()
        .fold((U64::MAX, U64::ZERO), |(min, max), &num| (cmp::min(min, num), cmp::max(max, num)));

    // We're not ready to support complete "trace" indexing for zksync chains. So we can
    // effectively only get what we need for native transfers by removing calls to "system
    // contracts".
    //
    // As an example, a Zksync ETH transfer will have a complex set of interactions with system
    // contracts. But, there will be only one deeply-nested "transfer" call for the actual two EOAs,
    // so filtering everything else will allow us to grab that without noise.
    //
    // Read more:
    // - https://docs.zksync.io/zksync-protocol/contracts/system-contracts#l2basetoken-msgvaluesimulator
    // - https://github.com/matter-labs/zksync-era/blob/7f36ed98fc6066c1224ff07c95282b647a8114fc/infrastructure/zk/src/verify-upgrade.ts#L24
    let zksync_system_contracts: [Address; 13] = [
        "0x0000000000000000000000000000000000008001".parse().unwrap(), // Native Token
        "0x0000000000000000000000000000000000008002".parse().unwrap(), // AccountCodeStorage
        "0x0000000000000000000000000000000000008003".parse().unwrap(), // NonceHolder
        "0x0000000000000000000000000000000000008004".parse().unwrap(), // KnownCodesStorage
        "0x0000000000000000000000000000000000008005".parse().unwrap(), // ImmutableSimulator
        "0x0000000000000000000000000000000000008006".parse().unwrap(), // ContractDeployer
        "0x0000000000000000000000000000000000008008".parse().unwrap(), // L1Messenger
        "0x0000000000000000000000000000000000008009".parse().unwrap(), // MsgValueSimulator
        "0x000000000000000000000000000000000000800a".parse().unwrap(), // L2BaseToken
        "0x000000000000000000000000000000000000800b".parse().unwrap(), // SystemContext
        "0x000000000000000000000000000000000000800c".parse().unwrap(), // BootloaderUtilities
        "0x000000000000000000000000000000000000800e".parse().unwrap(), // BytecodeCompressor
        "0x000000000000000000000000000000000000800f".parse().unwrap(), // ComplexUpgrader
    ];

    let native_transfers = trace_calls
        .into_iter()
        .flatten()
        .filter_map(|trace| {
            let action = match &trace.trace.action {
                Action::Call(call) => Some(call),
                _ => None,
            }?;

            let no_input = action.input == Bytes::new();
            let has_value = !action.value.is_zero();
            let is_zksync_system_transfer = zksync_system_contracts.contains(&action.from)
                || zksync_system_contracts.contains(&action.to);
            let is_native_transfer = has_value && no_input && !is_zksync_system_transfer;

            if is_native_transfer {
                Some(TraceResult::new_debug_native_transfer(
                    action,
                    &trace,
                    network_name,
                    provider.chain().id(),
                    from_block,
                    to_block,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if native_transfers.is_empty() {
        return Ok(());
    }

    let writer_barrier = event_writer_barrier(network_name);
    let _writer_guard = writer_barrier.read().await;
    if generation != crate::indexer::reorg::reorg_generation(network_name) {
        return Err(ProviderError::CustomError(format!(
            "Cannot apply native-transfer batch on network {network_name:?}: a reorg completed while fetching; retry the block range"
        )).into());
    }

    let _activity = ActiveCallback::start();
    if !native_transfers.is_empty() {
        config.trigger_event(native_transfers).await?;
    }
    evm_trace_update_progress_and_last_synced_task(config, to_block, || {}).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::mock::MockChainProvider;

    #[test]
    fn native_transfer_abi_parses() {
        let abi: Vec<alloy::json_abi::Event> = serde_json::from_str(NATIVE_TRANSFER_ABI).unwrap();
        assert_eq!(abi.len(), 1);
        assert_eq!(abi[0].name, "NativeTransfer");
        assert_eq!(abi[0].inputs.len(), 3);
    }

    #[test]
    fn native_transfer_filter_logic() {
        // The core filtering in native_transfer_block_consumer:
        // has_to_address && is_empty_input && !is_value_zero
        let cases: Vec<(bool, bool, bool, bool)> = vec![
            // (input_empty, value_zero, has_to, expected)
            (true, false, true, true),   // ETH transfer
            (false, false, true, false), // Contract call: has input data
            (true, true, true, false),   // Zero value: not a transfer
            (true, false, false, false), // Contract creation: no to address
        ];

        for (input_empty, value_zero, has_to, expected) in cases {
            let is_native_transfer = has_to && input_empty && !value_zero;
            assert_eq!(
                is_native_transfer, expected,
                "Failed for input_empty={input_empty}, value_zero={value_zero}, has_to={has_to}",
            );
        }
    }

    #[tokio::test]
    async fn mock_provider_returns_empty_blocks() {
        let mock = Arc::new(MockChainProvider::new(1).with_block_number(100));
        let block_numbers = vec![U64::from(10), U64::from(11)];
        let result = mock.get_block_by_number_batch(&block_numbers, true).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mock_provider_trace_block_returns_empty() {
        let mock = Arc::new(MockChainProvider::new(1));
        let traces = mock.trace_block(U64::from(100)).await.unwrap();
        assert!(traces.is_empty());
    }

    #[test]
    fn concurrency_backoff() {
        // Mimics the backoff in native_transfer_block_processor
        let backed_off = |n: usize| cmp::max(1, (n as f64 * 0.8) as usize);
        assert_eq!(backed_off(10), 8);
        assert_eq!(backed_off(5), 4);
        assert_eq!(backed_off(1), 1); // bottoms at 1
    }

    #[test]
    fn concurrency_increase_caps_at_limit() {
        let limit = RECOMMENDED_RPC_CHUNK_SIZE;
        let increase = |n: usize| ((n * 20) / 10).min(limit);
        assert_eq!(increase(5), 10); // doubles
        assert_eq!(increase(25), 50); // doubles
        assert_eq!(increase(40), limit.min(80)); // capped
    }

    #[tokio::test]
    async fn provider_trace_call_dispatches_trace_block() {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use alloy::rpc::types::trace::parity::{LocalizedTransactionTrace, TransactionTrace};
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let trace = LocalizedTransactionTrace {
            trace: TransactionTrace {
                action: Action::default(),
                result: None,
                trace_address: vec![],
                subtraces: 0,
                error: None,
            },
            transaction_hash: None,
            transaction_position: None,
            block_number: Some(100),
            block_hash: None,
        };
        let provider = Arc::new(MockChainProvider::new(1).with_traces(vec![trace]));

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        let config = TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: TraceProcessingMethod::TraceBlock,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        };

        let result = provider_trace_call(provider, &config, U64::from(100u64)).await;
        assert!(result.is_ok());
        let traces = result.unwrap();
        assert_eq!(traces.len(), 1);
    }

    #[tokio::test]
    async fn provider_trace_call_dispatches_debug_trace() {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use alloy::rpc::types::trace::parity::{LocalizedTransactionTrace, TransactionTrace};
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let trace = LocalizedTransactionTrace {
            trace: TransactionTrace {
                action: Action::default(),
                result: None,
                trace_address: vec![],
                subtraces: 0,
                error: None,
            },
            transaction_hash: None,
            transaction_position: None,
            block_number: Some(100),
            block_hash: None,
        };
        let provider = Arc::new(MockChainProvider::new(1).with_traces(vec![trace]));

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        let config = TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: TraceProcessingMethod::DebugTraceBlockByNumber,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        };

        let result = provider_trace_call(provider, &config, U64::from(100u64)).await;
        assert!(result.is_ok());
        let traces = result.unwrap();
        assert_eq!(traces.len(), 1);
    }

    async fn make_trace_config_async() -> Arc<TraceProcessingConfig> {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        Arc::new(TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: TraceProcessingMethod::TraceBlock,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Build a block with a single native-transfer transaction (value > 0, empty input, has to).
    fn make_block_with_native_transfer(
        block_number: u64,
        from: Address,
        to: Address,
        value: U256,
    ) -> alloy::network::AnyRpcBlock {
        use alloy::consensus::transaction::Recovered;
        use alloy::consensus::{Signed, TxEnvelope, TxLegacy};
        use alloy::network::{AnyHeader, AnyRpcHeader, AnyRpcTransaction, AnyTxEnvelope};
        use alloy::primitives::{Signature, TxKind, B256};
        use alloy::rpc::types::{Block, BlockTransactions, Transaction};
        use alloy::serde::WithOtherFields;

        let block_hash = B256::from([block_number as u8; 32]);

        let tx_legacy = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value,
            input: Bytes::new(),
        };

        // Dummy sig and hash — we only need the fields, not cryptographic validity.
        let sig = Signature::new(U256::ONE, U256::ONE, false);
        let tx_hash = B256::from(U256::from(block_number).to_be_bytes::<32>());
        let signed = Signed::new_unchecked(tx_legacy, sig, tx_hash);
        let envelope = AnyTxEnvelope::Ethereum(TxEnvelope::Legacy(signed));
        let recovered = Recovered::new_unchecked(envelope, from);

        let rpc_tx = AnyRpcTransaction::new(WithOtherFields::new(Transaction {
            inner: recovered,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_index: Some(0),
            effective_gas_price: None,
        }));

        alloy::network::AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: block_number, ..Default::default() }.seal(block_hash),
                ),
                BlockTransactions::Full(vec![rpc_tx]),
            )
            .into(),
        )
    }

    fn successful_receipts(
        blocks: &[alloy::network::AnyRpcBlock],
    ) -> Vec<alloy::network::AnyTransactionReceipt> {
        blocks.iter().flat_map(|block| {
            let alloy::rpc::types::BlockTransactions::Full(transactions) = &block.transactions else { panic!("full test block") };
            transactions.iter().map(move |tx| serde_json::from_value(serde_json::json!({
                "transactionHash": tx.tx_hash(), "transactionIndex": tx.transaction_index,
                "blockHash": tx.block_hash, "blockNumber": tx.block_number,
                "from": tx.from(), "to": tx.to(), "contractAddress": null,
                "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "cumulativeGasUsed": "0x5208",
                "status": "0x1", "type": "0x0", "logs": [], "logsBloom": format!("0x{}", "00".repeat(256))
            })).unwrap())
        }).collect()
    }

    async fn callback_case(
        callback: crate::event::callback_registry::TraceCallbackType,
    ) -> (tempfile::TempDir, Arc<TraceProcessingConfig>, Arc<MockChainProvider>, std::path::PathBuf)
    {
        use crate::event::{
            callback_registry::TraceCallbackRegistryInformation, contract_setup::TraceInformation,
        };
        let directory = tempfile::tempdir().unwrap();
        let mut config = make_trace_config_async().await.as_ref().clone();
        let mut registry = TraceCallbackRegistry::new();
        registry.register_event(TraceCallbackRegistryInformation {
            id: config.id.clone(),
            indexer_name: config.indexer_name.clone(),
            contract_name: config.contract_name.clone(),
            event_name: EVENT_NAME.to_string(),
            trace_information: TraceInformation {
                name: EVENT_NAME.to_string(),
                details: vec![],
                reorg_safe_distance: None,
            },
            callback,
            tables: Arc::new(vec![]),
            streams_clients: Arc::new(None),
        });
        config.registry = registry.complete();
        config.project_path = directory.path().to_path_buf();
        config.stream_last_synced_block_file_path = Some("cursor".to_string());
        let path = directory
            .path()
            .join("cursor/EvmTraces/last-synced-blocks/evmtraces-ethereum-nativetransfer.txt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "1").unwrap();
        let block =
            make_block_with_native_transfer(42, Address::ZERO, Address::repeat_byte(1), U256::ONE);
        let receipts = successful_receipts(std::slice::from_ref(&block));
        let provider = Arc::new(
            MockChainProvider::new(1).with_blocks(vec![block]).with_receipt_response(receipts),
        );
        (directory, Arc::new(config), provider, path)
    }

    #[tokio::test]
    async fn terminal_transfer_callback_rejects_checkpoint_and_releases_activity() {
        use futures::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let (_directory, config, provider, path) = callback_case(Arc::new(move |traces| {
            observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if traces.iter().any(|trace| matches!(trace, TraceResult::NativeTransfer { .. })) {
                    Err(CallbackError::Permanent("unrepresentable trace effect".to_string()))
                } else {
                    Ok(())
                }
            }
            .boxed()
        }))
        .await;
        let result =
            native_transfer_block_consumer(provider, &[U64::from(42)], "ethereum", &config).await;
        assert!(matches!(result, Err(ProcessEventError::Callback(CallbackError::Permanent(_)))));
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "block then transfer callback");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "1");
        assert_eq!(config.registry.failure_state().snapshot().len(), 1);
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn terminal_trace_processor_returns_without_retrying_callback_or_checkpointing() {
        use futures::FutureExt;
        let (_directory, config, provider, path) = callback_case(Arc::new(|_| {
            async { Err(CallbackError::Permanent("terminal trace effect".to_string())) }.boxed()
        }))
        .await;
        let (sender, receiver) = native_block_channel("ethereum", U64::from(41), 1);
        sender
            .sender
            .send(NativeBlockRequest {
                block_number: U64::from(42),
                generation: crate::indexer::reorg::reorg_generation("ethereum"),
            })
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            native_transfer_block_processor("ethereum".to_string(), provider, config, receiver),
        )
        .await
        .expect("terminal processor stops instead of retrying its range");
        assert!(matches!(result, Err(ProcessEventError::Callback(CallbackError::Permanent(_)))));
        assert_eq!(std::fs::read_to_string(path).unwrap(), "1");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn cancelled_trace_callback_releases_activity_without_checkpointing() {
        use futures::FutureExt;
        let started = Arc::new(tokio::sync::Notify::new());
        let callback_started = started.clone();
        let (_directory, config, provider, path) = callback_case(Arc::new(move |_| {
            let started = callback_started.clone();
            async move {
                started.notify_one();
                futures::future::pending::<()>().await;
                Ok(())
            }
            .boxed()
        }))
        .await;
        let task = tokio::spawn(async move {
            native_transfer_block_consumer(provider, &[U64::from(42)], "ethereum", &config).await
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified()).await.unwrap();
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "1");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn closed_trace_block_queue_finishes_without_waiting_for_more_input() {
        let (sender, receiver) = native_block_channel("ethereum", U64::from(41), 1);
        drop(sender);
        tokio::time::timeout(
            Duration::from_secs(1),
            native_transfer_block_processor(
                "ethereum".to_string(),
                Arc::new(MockChainProvider::new(1)),
                make_trace_config_async().await,
                receiver,
            ),
        )
        .await
        .expect("closed block source finishes")
        .expect("normal historical completion");
    }

    #[tokio::test]
    async fn native_transfer_block_consumer_empty_blocks() {
        // An actual empty block is evidence that there are no transfers to emit.
        let blocks = [10, 11]
            .into_iter()
            .map(|number| {
                let mut block = make_block_with_native_transfer(
                    number,
                    Address::ZERO,
                    Address::repeat_byte(1),
                    U256::ONE,
                );
                block.transactions = alloy::rpc::types::BlockTransactions::Full(vec![]);
                block
            })
            .collect();
        let provider = Arc::new(MockChainProvider::new(1).with_blocks(blocks));
        let config = make_trace_config_async().await;
        let block_numbers = vec![U64::from(10), U64::from(11)];

        let result =
            native_transfer_block_consumer(provider, &block_numbers, "ethereum", &config).await;

        assert!(result.is_ok(), "Complete empty blocks can be checkpointed");
    }

    #[tokio::test]
    async fn missing_native_blocks_do_not_call_handlers_or_checkpoint() {
        assert_incomplete_native_response("missing").await;
    }

    #[tokio::test]
    async fn partial_native_blocks_do_not_call_handlers_or_checkpoint() {
        assert_incomplete_native_response("partial").await;
    }

    #[tokio::test]
    async fn duplicate_native_blocks_do_not_substitute_for_missing_blocks() {
        assert_incomplete_native_response("duplicate").await;
    }

    #[tokio::test]
    async fn hash_only_native_blocks_do_not_call_handlers_or_checkpoint() {
        assert_incomplete_native_response("hashes").await;
    }

    #[tokio::test]
    async fn empty_native_request_keeps_cursor_unchanged() {
        use futures::FutureExt;
        let (_directory, config, provider, path) = callback_case(Arc::new(|_| {
            async { panic!("empty request cannot invoke a callback") }.boxed()
        }))
        .await;
        native_transfer_block_consumer(provider, &[], "ethereum", &config).await.unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), "1");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn complete_native_response_can_arrive_out_of_order() {
        use futures::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (_directory, config, _, path) = callback_case(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }.boxed()
        }))
        .await;
        let blocks: Vec<_> = [43, 42]
            .into_iter()
            .map(|number| {
                make_block_with_native_transfer(
                    number,
                    Address::ZERO,
                    Address::repeat_byte(1),
                    U256::ONE,
                )
            })
            .collect();
        let receipts = successful_receipts(&blocks);
        let provider =
            Arc::new(MockChainProvider::new(1).with_blocks(blocks).with_receipt_response(receipts));
        native_transfer_block_consumer(
            provider,
            &[U64::from(42), U64::from(43)],
            "ethereum",
            &config,
        )
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "block and transfer batches both complete");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "43");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    async fn assert_incomplete_native_response(case: &str) {
        use futures::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (_directory, config, _, path) = callback_case(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }.boxed()
        }))
        .await;
        let mut block =
            make_block_with_native_transfer(42, Address::ZERO, Address::repeat_byte(1), U256::ONE);
        let (blocks, requested) = match case {
            "missing" => (vec![], vec![U64::from(42)]),
            "partial" => (vec![block], vec![U64::from(42), U64::from(43)]),
            "duplicate" => (vec![block.clone(), block], vec![U64::from(42), U64::from(43)]),
            "hashes" => {
                block.transactions = alloy::rpc::types::BlockTransactions::Hashes(vec![
                    alloy::primitives::B256::repeat_byte(1),
                ]);
                (vec![block], vec![U64::from(42)])
            }
            _ => panic!("unknown RPC fixture"),
        };
        let provider = Arc::new(MockChainProvider::new(1).with_blocks(blocks));
        let result =
            native_transfer_block_consumer(provider, &requested, "ethereum", &config).await;
        let cursor = std::fs::read_to_string(path).unwrap();
        assert!(result.is_err(), "{case} RPC data cannot establish a completed block range");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "validate the entire response before any callback"
        );
        assert_eq!(cursor, "1");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn native_transfer_block_consumer_filters_native_transfers() {
        // Build a block containing three transactions:
        //   1. A valid native ETH transfer (value > 0, empty input, has to) → should match
        //   2. A zero-value tx (value = 0) → should NOT match
        //   3. A tx with non-empty input (contract call) → should NOT match
        //
        // We verify the consumer runs without error; the callback registry is empty so no
        // callbacks fire, but the filtering path is exercised.
        let from: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        let to: Address = "0x2222222222222222222222222222222222222222".parse().unwrap();

        let transfer_block =
            make_block_with_native_transfer(42, from, to, U256::from(1_000_000_000_000_000_000u64));

        let receipts = successful_receipts(std::slice::from_ref(&transfer_block));
        let provider = Arc::new(
            MockChainProvider::new(1)
                .with_blocks(vec![transfer_block])
                .with_receipt_response(receipts),
        );
        let config = make_trace_config_async().await;

        let result =
            native_transfer_block_consumer(provider, &[U64::from(42)], "ethereum", &config).await;

        assert!(result.is_ok(), "Consumer should succeed when a native transfer block is present");
    }

    #[tokio::test]
    async fn reverted_only_native_range_checkpoints_without_transfer_callbacks() {
        use futures::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (_directory, config, _, path) = callback_case(Arc::new(move |traces| {
            assert!(traces.iter().all(|trace| matches!(trace, TraceResult::Block { .. })));
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }.boxed()
        }))
        .await;
        let block =
            make_block_with_native_transfer(42, Address::ZERO, Address::repeat_byte(1), U256::ONE);
        let mut receipt =
            serde_json::to_value(&successful_receipts(std::slice::from_ref(&block))[0]).unwrap();
        receipt["status"] = serde_json::json!("0x0");
        let provider = Arc::new(
            MockChainProvider::new(1)
                .with_blocks(vec![block])
                .with_receipt_response(vec![serde_json::from_value(receipt).unwrap()]),
        );
        native_transfer_block_consumer(provider, &[U64::from(42)], "ethereum", &config)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "only the block callback completes");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "42");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn native_queue_requires_its_contiguous_unfinished_prefix() {
        use futures::FutureExt;
        let (_directory, config, _, path) = callback_case(Arc::new(|_| {
            async { panic!("a gap cannot invoke either callback") }.boxed()
        }))
        .await;
        let (sender, _receiver) = native_block_channel("ethereum", U64::from(41), 8);
        for numbers in [vec![U64::from(43)], vec![U64::from(42), U64::from(44)]] {
            let blocks: Vec<_> = numbers
                .iter()
                .map(|number| {
                    make_block_with_native_transfer(
                        number.to::<u64>(),
                        Address::ZERO,
                        Address::repeat_byte(1),
                        U256::ONE,
                    )
                })
                .collect();
            let receipts = successful_receipts(&blocks);
            let provider = Arc::new(
                MockChainProvider::new(1).with_blocks(blocks).with_receipt_response(receipts),
            );
            let error = native_transfer_block_consumer_at_generation(
                provider,
                &numbers,
                "ethereum",
                &config,
                crate::indexer::reorg::reorg_generation("ethereum"),
                Some(&sender.recovery),
            )
            .await
            .expect_err("gaps must fail before checkpointing");
            assert!(error.to_string().contains("first unfinished block"));
            assert_eq!(sender.recovery.first_unfinished(), Some(42));
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "1");
        }
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn historical_native_publisher_waits_for_queue_completion() {
        let network = "historical-native-completion";
        let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![
            make_block_with_parent(98, b256(98), b256(97)),
            make_block_with_parent(100, b256(100), b256(99)),
        ]));
        let (sender, receiver) = native_block_channel(network, U64::from(97), 1);
        let publisher = tokio::spawn(native_transfer_block_fetch(
            provider.clone(),
            sender,
            U64::from(97),
            Some(U64::from(98)),
            U64::ZERO,
            network.to_string(),
            CancellationToken::new(),
            None,
            "test".to_string(),
            None,
            None,
            Arc::new(TraceCallbackRegistry::new()),
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while receiver.receiver.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!publisher.is_finished(), "queueing end_block alone does not finish indexing");
        tokio::time::timeout(
            Duration::from_secs(2),
            native_transfer_block_processor(
                network.to_string(),
                provider,
                make_trace_config_async().await,
                receiver,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        publisher.await.unwrap().unwrap();
    }

    async fn assert_missing_native_metadata_is_an_error(field: &str) {
        use alloy::rpc::types::BlockTransactions;
        use futures::FutureExt;
        let mut block =
            make_block_with_native_transfer(42, Address::ZERO, Address::repeat_byte(1), U256::ONE);
        let BlockTransactions::Full(transactions) = &mut block.transactions else {
            panic!("full transaction fixture");
        };
        match field {
            "block_number" => transactions[0].block_number = None,
            "block_hash" => transactions[0].block_hash = None,
            "transaction_index" => transactions[0].transaction_index = None,
            _ => panic!("unknown fixture field"),
        }
        let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![block]));
        let config = make_trace_config_async().await;
        let outcome = std::panic::AssertUnwindSafe(native_transfer_block_consumer(
            provider,
            &[U64::from(42)],
            "ethereum",
            &config,
        ))
        .catch_unwind()
        .await;
        let error = outcome
            .expect("incomplete RPC metadata must not panic the indexing task")
            .expect_err("incomplete mined transaction metadata cannot be checkpointed");
        assert!(error.to_string().contains(field), "diagnostic identifies the missing field");
        assert_eq!(crate::indexer::task_tracker::active_indexing_count(), 0);
    }

    #[tokio::test]
    async fn missing_native_block_number_returns_error_without_panic() {
        assert_missing_native_metadata_is_an_error("block_number").await;
    }

    #[tokio::test]
    async fn missing_native_block_hash_returns_error_without_panic() {
        assert_missing_native_metadata_is_an_error("block_hash").await;
    }

    #[tokio::test]
    async fn missing_native_transaction_index_returns_error_without_panic() {
        assert_missing_native_metadata_is_an_error("transaction_index").await;
    }

    #[test]
    fn zksync_system_contracts_list() {
        // Verify the system contract addresses are valid and contain expected count
        let contracts: [Address; 13] = [
            "0x0000000000000000000000000000000000008001".parse().unwrap(),
            "0x0000000000000000000000000000000000008002".parse().unwrap(),
            "0x0000000000000000000000000000000000008003".parse().unwrap(),
            "0x0000000000000000000000000000000000008004".parse().unwrap(),
            "0x0000000000000000000000000000000000008005".parse().unwrap(),
            "0x0000000000000000000000000000000000008006".parse().unwrap(),
            "0x0000000000000000000000000000000000008008".parse().unwrap(),
            "0x0000000000000000000000000000000000008009".parse().unwrap(),
            "0x000000000000000000000000000000000000800a".parse().unwrap(),
            "0x000000000000000000000000000000000000800b".parse().unwrap(),
            "0x000000000000000000000000000000000000800c".parse().unwrap(),
            "0x000000000000000000000000000000000000800e".parse().unwrap(),
            "0x000000000000000000000000000000000000800f".parse().unwrap(),
        ];
        assert_eq!(contracts.len(), 13);
        // 0x8007 is intentionally missing (not a system contract used in transfers)
        assert!(!contracts.contains(&"0x0000000000000000000000000000000000008007".parse().unwrap()));
    }

    // ----------------------------------------------------------------------
    // Reorg rewind behaviour
    // ----------------------------------------------------------------------

    /// Build a block with a fixed parent hash (no transactions). Used to simulate
    /// canonical vs. reorged chains for the coordinator.
    fn make_block_with_parent(
        block_number: u64,
        block_hash: alloy::primitives::B256,
        parent_hash: alloy::primitives::B256,
    ) -> alloy::network::AnyRpcBlock {
        use alloy::network::{AnyHeader, AnyRpcHeader};
        use alloy::rpc::types::{Block, BlockTransactions};

        alloy::network::AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: block_number, parent_hash, ..Default::default() }
                        .seal(block_hash),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    }

    fn b256(n: u8) -> alloy::primitives::B256 {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        alloy::primitives::B256::from(bytes)
    }

    fn make_test_coordinator(
        network: &str,
        window_blocks: &[(u64, u8, u8)],
        provider: Arc<dyn ChainProvider>,
    ) -> Arc<Mutex<ReorgCoordinator>> {
        use crate::indexer::reorg::{BlockChainWindow, ReorgBlockHashPersistence};

        let mut window = BlockChainWindow::try_new(100).unwrap();
        for &(num, h, p) in window_blocks {
            window.insert(num, b256(h), b256(p));
        }

        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
        let coord = ReorgCoordinator::new(
            network.to_string(),
            window,
            persistence,
            provider,
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
        Arc::new(Mutex::new(coord))
    }

    #[tokio::test(start_paused = true)]
    async fn network_reorg_does_not_advance_a_lagging_native_publisher() {
        use futures::poll;
        let network = "lagging-native-reorg";
        let provider = Arc::new(
            MockChainProvider::new(1).with_blocks(
                (81..=100)
                    .map(|number| {
                        make_block_with_parent(number, b256(number as u8), b256((number - 1) as u8))
                    })
                    .collect(),
            ),
        );
        let coordinator = make_test_coordinator(network, &[], provider.clone());
        let (tx, mut rx) = native_block_channel(network, U64::from(80), 1);
        let cancel = CancellationToken::new();
        let mut publisher = Box::pin(native_transfer_block_fetch(
            provider,
            tx,
            U64::from(80),
            None,
            U64::ZERO,
            network.to_string(),
            cancel.clone(),
            None,
            "indexer".to_string(),
            Some(coordinator.clone()),
            None,
            Arc::new(TraceCallbackRegistry::new()),
        ));
        // The publisher has subscribed and is waiting for its first head poll.
        assert!(poll!(publisher.as_mut()).is_pending());
        let mut guard = coordinator.lock().await;
        let task = guard.try_create_reorg_task_for_block_range(98, 100).unwrap();
        guard
            .handle_reorg(
                task,
                &ReorgContext {
                    postgres: None,
                    clickhouse: None,
                    registry: None,
                    trace_registry: None,
                },
            )
            .await
            .unwrap();
        drop(guard);
        let first = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = publisher.as_mut() => panic!("publisher ended: {result:?}"),
                first = rx.receiver.recv() => first.unwrap(),
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        assert_eq!(
            first.block_number,
            U64::from(81),
            "blocks before the fork are still unprocessed"
        );
        assert_eq!(first.generation, crate::indexer::reorg::reorg_generation(network));
    }

    #[tokio::test]
    async fn native_transfer_detects_no_reorg_on_canonical_chain() {
        // Window has blocks 10, 11. Provider returns canonical 12, 13 whose
        // parent_hashes chain back to the window. Expect `Canonical`.
        let canonical_12 = make_block_with_parent(12, b256(12), b256(11));
        let canonical_13 = make_block_with_parent(13, b256(13), b256(12));
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_blocks(vec![canonical_12, canonical_13]));

        let coordinator =
            make_test_coordinator("ethereum", &[(10, 10, 9), (11, 11, 10)], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            12,
            13,
        )
        .await;

        assert_eq!(outcome, NativeTransferReorgOutcome::Canonical);
    }

    #[tokio::test]
    async fn native_transfer_rewinds_to_fork_point_on_reorg() {
        // Window has block 11 with hash=11, parent=10 (pre-reorg state).
        // Canonical blocks 10 and 11 now have hashes 0xFD and 0xFE (reorged).
        // Fetch range 12..=12 with canonical block 12 whose parent_hash=0xFE
        // does NOT match the window's stored hash of block 11 → reorg.
        //
        // The coordinator then fetches missing blocks from the provider to locate
        // the fork point; we expose the complete canonical window so find_fork_point can succeed.
        let canonical_10 = make_block_with_parent(10, b256(0xFD), b256(9));
        let canonical_11 = make_block_with_parent(11, b256(0xFE), b256(0xFD));
        let canonical_12 = make_block_with_parent(12, b256(12), b256(0xFE));
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_blocks(vec![
                canonical_10,
                canonical_11,
                canonical_12,
            ]));

        let coordinator =
            make_test_coordinator("ethereum", &[(10, 10, 9), (11, 11, 10)], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            12,
            12,
        )
        .await;

        // Coordinator's find_fork_point compares the window's entries against
        // canonical hashes. With window blocks [10, 11] and canonical [10@0xFD, 11@0xFE],
        // no canonical hash matches any window entry → fork_point falls back to the
        // oldest window block (10).
        //
        // Helper contract: `rewind_to` IS the first canonical block to re-fetch
        // (inclusive) — equal to `fork_point`.
        let rewind_to = match outcome {
            NativeTransferReorgOutcome::Rewound { rewind_to } => rewind_to,
            other => panic!("expected Rewound, got {other:?}"),
        };
        assert_eq!(
            rewind_to, 10,
            "helper must return rewind_to == fork_point (the first canonical block to re-fetch)"
        );

        // Caller contract: applying `last_seen_block = rewind_to - 1` makes the
        // next iteration's `from_block = last_seen_block + 1 = rewind_to = fork_point`,
        // so block `fork_point` is NOT skipped. This mirrors the contract-event
        // path at fetch_logs.rs where `last_seen_block_number = fork_point - 1`.
        let last_seen_block_after_rewind = U64::from(rewind_to.saturating_sub(1));
        let next_from_block = last_seen_block_after_rewind + U64::from(1);
        assert_eq!(
            next_from_block,
            U64::from(rewind_to),
            "caller rewind must set next from_block to fork_point (inclusive)"
        );
    }

    #[tokio::test]
    async fn native_transfer_short_read_returns_failed() {
        // Provider registered with blocks [10, 11] but caller requests [10, 11, 12].
        // The helper must detect the short read and return Failed, so the caller
        // backs off and retries the same range instead of silently advancing
        // `last_seen_block` past block 12.
        let canonical_10 = make_block_with_parent(10, b256(10), b256(9));
        let canonical_11 = make_block_with_parent(11, b256(11), b256(10));
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_blocks(vec![canonical_10, canonical_11]));

        // Coordinator with an empty window so validation would otherwise succeed
        // — we want to isolate the short-read check.
        let coordinator = make_test_coordinator("ethereum", &[], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            10,
            12,
        )
        .await;

        assert_eq!(
            outcome,
            NativeTransferReorgOutcome::Failed,
            "short read (2 blocks returned when 3 requested) must yield Failed",
        );
    }

    #[tokio::test]
    async fn native_transfer_no_coordinator_short_circuits() {
        // Without a coordinator configured, the helper must return NoCoordinator
        // so the caller emits the range as-is.
        let provider: Arc<dyn ChainProvider> = Arc::new(MockChainProvider::new(1));

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            None,
            None,
            None,
            None,
            "ethereum",
            12,
            13,
        )
        .await;

        assert_eq!(outcome, NativeTransferReorgOutcome::NoCoordinator);
    }
}
