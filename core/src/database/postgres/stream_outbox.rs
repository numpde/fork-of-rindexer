//! Required delivery records share the transaction that applies their source event.
use alloy::primitives::keccak256;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Row, Transaction};

use crate::event::EventMessage;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DeliverySource {
    pub indexer: String,
    pub contract: String,
}
impl DeliverySource {
    pub(crate) fn key(&self) -> String {
        format!(
            "{:#x}",
            keccak256(
                serde_json::to_vec(&(&self.indexer, &self.contract))
                    .expect("source names serialize")
            )
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DeliveryDestination {
    pub stream_type: String,
    pub config_index: usize,
    pub fingerprint: String,
    pub finalized: bool,
}

pub(crate) const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS rindexer_internal.stream_outbox (
    id TEXT PRIMARY KEY,
    created_sequence BIGSERIAL UNIQUE NOT NULL,
    source_indexer TEXT NOT NULL,
    source_contract TEXT NOT NULL,
    owner_key VARCHAR(66) NOT NULL,
    network VARCHAR(50) NOT NULL,
    source_table TEXT,
    block_number NUMERIC NOT NULL,
    destination JSONB NOT NULL,
    message JSONB,
    retry_after TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    invalidated BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS stream_outbox_pending ON rindexer_internal.stream_outbox
    (owner_key, network, created_sequence) WHERE acknowledged_at IS NULL AND NOT invalidated;
CREATE INDEX IF NOT EXISTS stream_outbox_recovery ON rindexer_internal.stream_outbox
    (network, source_table, block_number);
";

/// One immutable obligation. A new canonical application cycle gets a new ID;
/// retries of a committed record keep its ID, including after an ambiguous ACK.
pub(crate) struct PendingDelivery<'a> {
    pub id: String,
    pub source: &'a DeliverySource,
    pub source_table: Option<&'a str>,
    pub block_number: u64,
    pub destination: &'a DeliveryDestination,
    pub message: &'a EventMessage,
}

pub(crate) async fn enqueue(
    tx: &Transaction<'_>,
    delivery: PendingDelivery<'_>,
) -> anyhow::Result<()> {
    let destination = serde_json::to_value(delivery.destination)?;
    let message = serde_json::to_value(delivery.message)?;
    let conflict =
        if delivery.source_table.is_none() { " ON CONFLICT (id) DO NOTHING" } else { "" };
    tx.execute(
        &format!("INSERT INTO rindexer_internal.stream_outbox
        (id,source_indexer,source_contract,owner_key,network,source_table,block_number,destination,message)
        VALUES ($1,$2,$3,$4,$5,$6,$7::text::numeric,$8,$9){conflict}"),
        &[&delivery.id,&delivery.source.indexer,&delivery.source.contract,&delivery.source.key(),
        &delivery.message.network,&delivery.source_table,&delivery.block_number.to_string(),
        &destination,&message],
    ).await?;
    Ok(())
}

/// Keep the selected row locked through its bounded publish attempt. Another
/// worker must not skip an in-flight delivery and overtake it for the same route.
pub(crate) async fn next(
    tx: &Transaction<'_>,
    source: &DeliverySource,
    network: &str,
    distance: Option<u64>,
) -> anyhow::Result<Option<Row>> {
    // A slow publish elsewhere can outlast the retry delay. Requeue deferred
    // records at their retry time so creation order cannot monopolize other
    // routes; the predecessor check still preserves order within each route.
    Ok(tx.query_opt(
        "SELECT id, destination, message FROM rindexer_internal.stream_outbox job
        WHERE owner_key=$1 AND network=$2 AND acknowledged_at IS NULL AND NOT invalidated
        AND retry_after <= now()
        AND (source_table IS NULL OR NOT (destination->>'finalized')::boolean
            OR block_number + $3::text::numeric <= COALESCE((SELECT MAX(block_number)
            FROM rindexer_internal.reorg_block_hashes WHERE network=$2),0))
        AND (source_table IS NULL OR NOT EXISTS (SELECT 1 FROM rindexer_internal.stream_outbox earlier
            WHERE earlier.owner_key=job.owner_key AND earlier.network=job.network
            AND earlier.destination=job.destination AND earlier.created_sequence<job.created_sequence
            AND earlier.acknowledged_at IS NULL AND NOT earlier.invalidated))
        ORDER BY (source_table IS NULL) DESC, GREATEST(created_at, retry_after), created_sequence LIMIT 1 FOR UPDATE",
        &[&source.key(),&network,&distance.map(|value|value.to_string())],
    ).await?)
}

pub(crate) async fn acknowledge(tx: &Transaction<'_>, id: &str) -> anyhow::Result<()> {
    tx.execute("UPDATE rindexer_internal.stream_outbox SET acknowledged_at=now(), message=NULL WHERE id=$1", &[&id]).await?;
    Ok(())
}

/// Completed records are needed only within supported recovery history. Pending
/// canonical obligations are never removed by maintenance, even after finality.
pub(crate) async fn prune(tx: &Transaction<'_>, network: &str) -> anyhow::Result<()> {
    tx.execute("DELETE FROM rindexer_internal.stream_outbox WHERE network=$1
        AND (acknowledged_at IS NOT NULL OR invalidated)
        AND block_number < (SELECT MIN(block_number) FROM rindexer_internal.reorg_block_hashes WHERE network=$1)", &[&network]).await?;
    Ok(())
}

pub(crate) async fn invalidate(
    tx: &Transaction<'_>,
    network: &str,
    fork: u64,
    tables: &[String],
) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE rindexer_internal.stream_outbox SET invalidated=true, message=NULL
        WHERE network=$1 AND source_table=ANY($2) AND block_number >= $3::text::numeric",
        &[&network, &tables, &fork.to_string()],
    )
    .await?;
    Ok(())
}

/// Source inserts cannot advance while the coordinator holds its writer barrier.
/// This watermark and canonical replacement hashes therefore identify a recovery
/// retry consistently, including a successful COMMIT with a lost response.
pub(crate) async fn recovery_id(
    tx: &Transaction<'_>,
    network: &str,
    canonical: &[(u64, alloy::primitives::B256, alloy::primitives::B256)],
    fork: u64,
) -> anyhow::Result<String> {
    let watermark:i64=tx.query_one("SELECT COALESCE(MAX(created_sequence),0) FROM rindexer_internal.stream_outbox WHERE network=$1 AND source_table IS NOT NULL", &[&network]).await?.get(0);
    Ok(format!("{:#x}", keccak256(serde_json::to_vec(&(network, fork, canonical, watermark))?)))
}

/// Retire orphaned pending/acknowledged source deliveries and persist their
/// retractions in the same transaction as raw/derived rollback.
pub(crate) async fn rollback(
    tx: &Transaction<'_>,
    network: &str,
    fork: u64,
    canonical: &[(u64, alloy::primitives::B256, alloy::primitives::B256)],
    tables: &[String],
    mut configured: Vec<(DeliverySource, DeliveryDestination)>,
    message: &EventMessage,
) -> anyhow::Result<()> {
    let exists: bool = tx
        .query_one("SELECT to_regclass('rindexer_internal.stream_outbox') IS NOT NULL", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        exists || configured.is_empty(),
        "Cannot roll back required delivery without its PostgreSQL outbox schema"
    );
    if !exists {
        return Ok(());
    }
    for row in tx.query("SELECT DISTINCT source_indexer,source_contract,destination FROM rindexer_internal.stream_outbox WHERE network=$1 AND source_table=ANY($2) AND block_number >= $3::text::numeric AND NOT invalidated",&[&network,&tables,&fork.to_string()]).await? {
        let route=(DeliverySource{indexer:row.try_get(0)?,contract:row.try_get(1)?},serde_json::from_value(row.try_get::<_,serde_json::Value>(2)?)?);
        if !configured.contains(&route) { configured.push(route); }
    }
    let recovery = recovery_id(tx, network, canonical, fork).await?;
    invalidate(tx, network, fork, tables).await?;
    for (source, destination) in configured {
        let id = format!(
            "{:#x}",
            keccak256(serde_json::to_vec(&(&recovery, source.key(), &destination))?)
        );
        enqueue(
            tx,
            PendingDelivery {
                id,
                source: &source,
                source_table: None,
                block_number: fork,
                destination: &destination,
                message,
            },
        )
        .await?;
    }
    Ok(())
}

/// A failing destination must not monopolize the owner's single publish slot.
/// The predecessor check still keeps later records for that destination behind it.
pub(crate) async fn defer(tx: &Transaction<'_>, id: &str) -> anyhow::Result<()> {
    tx.execute("UPDATE rindexer_internal.stream_outbox SET retry_after=clock_timestamp()+interval '2 seconds' WHERE id=$1", &[&id]).await?;
    Ok(())
}
