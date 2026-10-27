//! Read-only subscription health: runtime query identities select durable PostgreSQL cursors.
use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy::primitives::U64;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::HealthStatusType;
use crate::{
    database::{
        generate::generate_indexer_contract_schema_name,
        postgres::generate::generate_internal_event_table_name,
    },
    event::callback_registry::{EventCallbackRegistry, FailedStream, TraceCallbackRegistry},
    indexer::{reorg::validate_sql_identifier, start::calculate_safe_block_number},
    manifest::contract::ReorgSafeDistance,
    provider::ChainProvider,
    PostgresClient,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionIdentity {
    pub id: String,
    pub indexer: String,
    pub contract: String,
    pub event: String,
    pub network: String,
    pub cursor_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    WaitingForCheckpoint,
    Syncing,
    Live,
    Completed,
    Failed,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionHealth {
    #[serde(flatten)]
    pub subscription: SubscriptionIdentity,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub last_synced_block: Option<u64>,
    pub observed_head: Option<u64>,
    pub target_block: Option<u64>,
    pub lag_blocks: Option<u64>,
    pub status: SubscriptionStatus,
}

#[derive(Clone)]
pub(super) struct SubscriptionSource {
    identity: SubscriptionIdentity,
    start: Option<u64>,
    end: Option<u64>,
    safe_distance: Option<ReorgSafeDistance>,
    provider: Arc<dyn ChainProvider>,
}

pub(super) fn event_sources(registry: &EventCallbackRegistry) -> Vec<SubscriptionSource> {
    let mut sources = Vec::new();
    for event in &registry.events {
        for detail in &event.contract.details {
            sources.push(SubscriptionSource {
                identity: SubscriptionIdentity {
                    id: event.id.clone(),
                    indexer: event.indexer_name.clone(),
                    contract: event.contract.name.clone(),
                    event: event.event_name.clone(),
                    network: detail.network.clone(),
                    cursor_key: detail.cursor_key(),
                },
                start: detail.start_block.map(|block| block.to()),
                end: detail.end_block.map(|block| block.to()),
                safe_distance: event.contract.reorg_safe_distance,
                provider: detail.cached_provider.clone(),
            });
        }
    }
    sources
}

pub(super) fn trace_sources(registry: &TraceCallbackRegistry) -> Vec<SubscriptionSource> {
    let mut sources = Vec::new();
    for event in &registry.events {
        for detail in &event.trace_information.details {
            sources.push(SubscriptionSource {
                identity: SubscriptionIdentity {
                    id: event.id.clone(),
                    indexer: event.indexer_name.clone(),
                    contract: event.contract_name.clone(),
                    event: event.event_name.clone(),
                    network: detail.network.clone(),
                    cursor_key: crate::event::cursor::NETWORK_CURSOR_KEY.into(),
                },
                start: detail.start_block.map(|block| block.to()),
                end: detail.end_block.map(|block| block.to()),
                safe_distance: event.trace_information.reorg_safe_distance,
                provider: detail.cached_provider.clone(),
            });
        }
    }
    sources
}

async fn checkpoint(
    client: &PostgresClient,
    source: &SubscriptionSource,
) -> anyhow::Result<Option<u64>> {
    let identity = &source.identity;
    let schema = generate_indexer_contract_schema_name(&identity.indexer, &identity.contract);
    let table = generate_internal_event_table_name(&schema, &identity.event);
    validate_sql_identifier(&table, "subscription checkpoint table")?;
    let qualified = format!("rindexer_internal.\"{table}\"");
    let exists: bool =
        client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&qualified]).await?.get(0);
    if !exists {
        return Ok(None);
    }
    let row = client.query_one_or_none(&format!("SELECT last_synced_block::text FROM {qualified} WHERE network=$1 AND cursor_key=$2"), &[&identity.network, &identity.cursor_key]).await?;
    row.map(|row| -> anyhow::Result<u64> {
        row.try_get::<_, String>(0)?
            .parse::<u64>()
            .context("subscription checkpoint is not an unsigned block number")
    })
    .transpose()
}

pub(super) async fn snapshot(
    client: Option<&PostgresClient>,
    sources: &[SubscriptionSource],
    failures: &[FailedStream],
) -> (HealthStatusType, Vec<SubscriptionHealth>) {
    // Probe each configured network once per request. Readiness must not hang
    // indefinitely on an RPC outage or present an unavailable head as zero lag.
    let mut heads = HashMap::new();
    if client.is_some() {
        for source in sources {
            let network = &source.identity.network;
            if heads.contains_key(network) {
                continue;
            }
            let head = match tokio::time::timeout(
                Duration::from_secs(2),
                source.provider.get_block_number(),
            )
            .await
            {
                Ok(Ok(head)) => Some(head.to::<u64>()),
                Ok(Err(error)) => {
                    warn!(%network, %error, "Cannot read RPC head for subscription health");
                    None
                }
                Err(_) => {
                    warn!(%network, "RPC head probe for subscription health exceeded two seconds");
                    None
                }
            };
            heads.insert(network.clone(), head);
        }
    }
    let mut states = Vec::new();
    for source in sources {
        let identity = &source.identity;
        let failed = failures
            .iter()
            .any(|failure| failure.id == identity.id && failure.network == identity.network);
        let head = heads.get(&identity.network).copied().flatten();
        let position = match client {
            Some(client) => checkpoint(client, source).await,
            None => Err(anyhow::anyhow!("PostgreSQL client is unavailable")),
        };
        let unavailable = position.is_err() || head.is_none();
        if let Err(error) = &position {
            warn!(network=%identity.network, contract=%identity.contract, event=%identity.event, cursor_key=%identity.cursor_key, %error, "Cannot read durable subscription progress for health");
        }
        let last = position.ok().flatten();
        let target = head.map(|head| {
            calculate_safe_block_number(
                source.safe_distance,
                source.provider.chain().id(),
                U64::from(head),
                U64::from(source.end.unwrap_or(head).min(head)),
            )
            .0
            .to::<u64>()
        });
        // A configured start is outside the query's required prefix. Do not count
        // pre-start blocks as lag when the newly initialized cursor is still zero.
        let lag = last.zip(target).map(|(last, target)| {
            target.saturating_sub(last.max(source.start.unwrap_or(0).saturating_sub(1)))
        });
        let status = if failed {
            SubscriptionStatus::Failed
        } else if unavailable {
            SubscriptionStatus::Unavailable
        } else if last.is_none() {
            SubscriptionStatus::WaitingForCheckpoint
        } else if source.end.zip(last).is_some_and(|(end, last)| last >= end) {
            SubscriptionStatus::Completed
        } else if lag == Some(0) {
            SubscriptionStatus::Live
        } else {
            SubscriptionStatus::Syncing
        };
        states.push(SubscriptionHealth {
            subscription: identity.clone(),
            start_block: source.start,
            end_block: source.end,
            last_synced_block: last,
            observed_head: head,
            target_block: target,
            lag_blocks: lag,
            status,
        });
    }
    states.sort_by(|a, b| {
        (
            &a.subscription.network,
            &a.subscription.indexer,
            &a.subscription.contract,
            &a.subscription.event,
            &a.subscription.cursor_key,
        )
            .cmp(&(
                &b.subscription.network,
                &b.subscription.indexer,
                &b.subscription.contract,
                &b.subscription.event,
                &b.subscription.cursor_key,
            ))
    });
    let status = if client.is_none() {
        HealthStatusType::NotConfigured
    } else if states.iter().any(|state| {
        matches!(state.status, SubscriptionStatus::Failed | SubscriptionStatus::Unavailable)
    }) {
        HealthStatusType::Unhealthy
    } else if states
        .iter()
        .any(|state| matches!(state.status, SubscriptionStatus::WaitingForCheckpoint))
    {
        HealthStatusType::NoData
    } else if states.iter().any(|state| matches!(state.status, SubscriptionStatus::Syncing)) {
        HealthStatusType::Syncing
    } else {
        HealthStatusType::Healthy
    };
    (status, states)
}
