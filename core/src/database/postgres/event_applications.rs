//! Event receipts are committed with raw rows and derived effects. A cursor is
//! deliberately not evidence of application: unseen history can arrive behind it.
use std::collections::HashSet;

use tokio_postgres::Transaction;

use super::client::pg_error_to_string;
use crate::{
    event::callback_registry::{CallbackError, CallbackExecutionResult, TxInformation},
    indexer::reorg::validate_sql_identifier,
};

/// One canonical, decoded callback row and its stable source position.
pub(crate) struct EventApplication {
    pub tx_information: TxInformation,
    pub position: String,
}

pub(crate) fn schema_sql() -> &'static str {
    "CREATE TABLE IF NOT EXISTS rindexer_internal.event_application_streams (
        namespace TEXT PRIMARY KEY,
        raw_table TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS rindexer_internal.event_applications (
        namespace TEXT NOT NULL REFERENCES rindexer_internal.event_application_streams(namespace) ON DELETE CASCADE,
        network TEXT NOT NULL,
        block_number NUMERIC NOT NULL,
        block_hash BYTEA NOT NULL,
        tx_hash BYTEA NOT NULL,
        event_position TEXT NOT NULL,
        PRIMARY KEY (namespace, network, block_hash, tx_hash, event_position)
    );
    DO $receipt_position$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = 'rindexer_internal.event_applications'::regclass
                   AND attname = 'log_index' AND NOT attisdropped) THEN
            ALTER TABLE rindexer_internal.event_applications
                RENAME COLUMN log_index TO event_position;
        END IF;
    END;
    $receipt_position$;
    CREATE INDEX IF NOT EXISTS event_applications_block_range
        ON rindexer_internal.event_applications(namespace, network, block_number);"
}

/// Return one selection bit per input, admitting each event at most once. The
/// caller must apply only selected events and commit every effect in this tx.
pub(crate) async fn claim(
    tx: &Transaction<'_>,
    namespace: &str,
    raw_table: &str,
    events: &[EventApplication],
) -> CallbackExecutionResult<Vec<bool>> {
    let database_error = |error| {
        CallbackError::Retryable(format!(
            "Cannot prepare PostgreSQL event receipts for {namespace:?}: {}",
            pg_error_to_string(&error)
        ))
    };
    let invalid = |error: anyhow::Error| CallbackError::Permanent(error.to_string());
    validate_sql_identifier(namespace, "PostgreSQL application namespace").map_err(invalid)?;
    let (schema, table) = raw_table.split_once('.').ok_or_else(|| {
        CallbackError::Permanent(format!(
            "Cannot claim PostgreSQL events: raw table {raw_table:?} must name its schema"
        ))
    })?;
    validate_sql_identifier(schema, "raw event schema").map_err(invalid)?;
    validate_sql_identifier(table, "raw event table").map_err(invalid)?;

    let initialized = tx
        .execute(
            "INSERT INTO rindexer_internal.event_application_streams(namespace, raw_table) VALUES ($1, $2)
         ON CONFLICT DO NOTHING",
            &[&namespace, &raw_table],
        )
        .await
        .map_err(database_error)?;
    if initialized == 1 {
        let populated: bool = tx
            .query_one(&format!("SELECT EXISTS(SELECT 1 FROM {raw_table})"), &[])
            .await
            .map_err(database_error)?
            .get(0);
        if populated {
            return Err(CallbackError::Permanent(format!(
                "Cannot initialize PostgreSQL event receipts for {namespace:?}: raw table {raw_table:?} already contains history without application receipts; stop writers and review migration of raw events and derived tables together"
            )));
        }
    }

    let networks: Vec<_> =
        events.iter().map(|event| event.tx_information.network.clone()).collect();
    let blocks: Vec<_> =
        events.iter().map(|event| event.tx_information.block_number.to_string()).collect();
    let hashes: Vec<_> =
        events.iter().map(|event| event.tx_information.block_hash.as_slice().to_vec()).collect();
    let transactions: Vec<_> = events
        .iter()
        .map(|event| event.tx_information.transaction_hash.as_slice().to_vec())
        .collect();
    let indexes: Vec<_> = events.iter().map(|event| event.position.clone()).collect();

    // Consistent lock ordering prevents overlapping receipt batches from taking
    // the same identity locks in opposite orders. RETURNING admits only new rows,
    // including one representative of duplicates inside this very batch.
    let rows = tx
        .query(
            "INSERT INTO rindexer_internal.event_applications
            (namespace, network, block_number, block_hash, tx_hash, event_position)
         SELECT $1, network, block_number::numeric, block_hash, tx_hash, event_position
         FROM unnest($2::text[], $3::text[], $4::bytea[], $5::bytea[], $6::text[])
            AS input(network, block_number, block_hash, tx_hash, event_position)
         ORDER BY network, block_hash, tx_hash, event_position
         ON CONFLICT DO NOTHING
         RETURNING network, block_hash, tx_hash, event_position",
            &[&namespace, &networks, &blocks, &hashes, &transactions, &indexes],
        )
        .await
        .map_err(database_error)?;
    let mut admitted: HashSet<(String, Vec<u8>, Vec<u8>, String)> =
        rows.iter().map(|row| (row.get(0), row.get(1), row.get(2), row.get(3))).collect();
    Ok(networks
        .into_iter()
        .zip(hashes)
        .zip(transactions)
        .zip(indexes)
        .map(|(((network, block), transaction), index)| {
            admitted.remove(&(network, block, transaction, index))
        })
        .collect())
}
