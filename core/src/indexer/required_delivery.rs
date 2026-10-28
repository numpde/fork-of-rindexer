//! Validate durable destination ownership before recovery, then start drainers only
//! after every network's persisted canonical history has passed startup validation.
use crate::{
    database::postgres::{
        client::PostgresClient,
        stream_outbox::{DeliveryDestination, DeliverySource},
    },
    event::callback_registry::{EventCallbackRegistry, FailedStream, TraceCallbackRegistry},
    manifest::core::Manifest,
    streams::StreamsClients,
};
use anyhow::Context;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

struct RequiredStream {
    clients: Arc<Option<StreamsClients>>,
    source: DeliverySource,
    network: String,
    identity: FailedStream,
}

fn configured(registry: &EventCallbackRegistry) -> anyhow::Result<Vec<RequiredStream>> {
    let mut streams = Vec::new();
    let mut seen = HashSet::new();
    for event in &registry.events {
        let Some(clients) = event
            .streams_clients
            .as_ref()
            .as_ref()
            .filter(|clients| clients.requires_durable_delivery())
        else {
            continue;
        };
        let source = clients
            .delivery_source()
            .with_context(|| {
                format!(
                    "Cannot configure required delivery for {}::{}::{}",
                    event.indexer_name, event.contract.name, event.event_name
                )
            })?
            .clone();
        anyhow::ensure!(
            source.indexer == event.indexer_name && source.contract == event.contract.name,
            "Required delivery client is bound to another contract"
        );
        let mut matched = false;
        for detail in &event.contract.details {
            if clients.network_destinations(&detail.network).is_empty() {
                continue;
            }
            matched = true;
            if !seen.insert((source.key(), detail.network.clone())) {
                continue;
            }
            streams.push(RequiredStream {
                clients: event.streams_clients.clone(),
                source: source.clone(),
                network: detail.network.clone(),
                identity: FailedStream {
                    id: event.id.clone(),
                    indexer: event.indexer_name.clone(),
                    contract: event.contract.name.clone(),
                    event: event.event_name.clone(),
                    network: detail.network.clone(),
                    from_block: detail.start_block.unwrap_or_default().to(),
                    to_block: detail.end_block.unwrap_or_default().to(),
                },
            });
        }
        anyhow::ensure!(matched,"Required delivery for {}::{} has no configured destination on any subscribed network; check publisher feature support and destination networks",source.indexer,source.contract);
    }
    Ok(streams)
}

pub(super) async fn validate(
    manifest: &Manifest,
    registry: &EventCallbackRegistry,
    traces: &TraceCallbackRegistry,
    postgres: Option<&PostgresClient>,
) -> anyhow::Result<()> {
    anyhow::ensure!(!traces.events.iter().any(|event|event.streams_clients.as_ref().as_ref().is_some_and(StreamsClients::requires_durable_delivery)),"Required stream delivery supports PostgreSQL no-code contract events, not native-transfer callbacks");
    let streams = configured(registry)?;
    for stream in &streams {
        anyhow::ensure!(
            manifest.networks.iter().any(|network| network.name == stream.network
                && network.reorg_handling.as_ref().is_some_and(|config| config.enabled)),
            "Required stream delivery on network {} needs reorg_handling.enabled: true",
            stream.network
        );
    }
    let Some(postgres) = postgres else {
        anyhow::ensure!(streams.is_empty(), "Required stream delivery needs PostgreSQL");
        return Ok(());
    };
    let exists: bool = postgres
        .query_one("SELECT to_regclass('rindexer_internal.stream_outbox') IS NOT NULL", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        exists || streams.is_empty(),
        "Required stream delivery needs the current generated PostgreSQL internal schema"
    );
    if !exists {
        return Ok(());
    }
    let available = streams
        .iter()
        .map(|stream| {
            (
                (stream.source.key(), stream.network.clone()),
                stream
                    .clients
                    .as_ref()
                    .as_ref()
                    .expect("configured required client")
                    .network_destinations(&stream.network),
            )
        })
        .collect::<HashMap<_, _>>();
    // Include acknowledged records still inside recovery coverage: their old
    // destinations may need a retraction. Credentials are excluded from routing identity.
    for row in postgres.query("SELECT owner_key,network,destination FROM rindexer_internal.stream_outbox WHERE source_indexer=$1 AND NOT invalidated", &[&manifest.name]).await? {
        let owner:String=row.try_get(0)?;let network:String=row.try_get(1)?;
        let destination:DeliveryDestination=serde_json::from_value(row.try_get::<_,serde_json::Value>(2)?)?;
        anyhow::ensure!(available.get(&(owner,network.clone())).is_some_and(|destinations|destinations.contains(&destination)),
            "Cannot change or remove required delivery routing on network {network}: durable obligations or recoverable delivered records still reference its original configuration. Restore that configuration and drain/review the retained outbox before changing destinations");
    }
    Ok(())
}

pub(super) fn start(
    registry: &EventCallbackRegistry,
    postgres: Option<&Arc<PostgresClient>>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let streams = configured(registry)?;
    if streams.is_empty() {
        return Ok(());
    }
    let postgres =
        postgres.ok_or_else(|| anyhow::anyhow!("Required stream delivery needs PostgreSQL"))?;
    for stream in streams {
        StreamsClients::start_delivery_worker(
            &stream.clients,
            postgres.clone(),
            stream.network,
            cancel.clone(),
            registry.failure_state(),
            stream.identity,
        );
    }
    Ok(())
}
