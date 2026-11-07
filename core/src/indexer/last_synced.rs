use alloy::primitives::U64;
use clickhouse::Row;
use serde::Deserialize;
use std::time::Duration;
use std::{path::Path, str::FromStr, sync::Arc};
use tokio::{
    fs,
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};
use tracing::{debug, error};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::pg_error_to_string;
use crate::database::postgres::generate::{
    generate_internal_cron_table_name, generate_internal_cron_table_name_no_shorten,
    generate_internal_event_table_name_no_shorten,
};
use crate::indexer::cron_identity::{self, CronIdentity};
use crate::{
    database::{
        generate::generate_indexer_contract_schema_name,
        postgres::generate::generate_internal_event_table_name,
    },
    event::config::{EventProcessingConfig, TraceProcessingConfig},
    helpers::get_full_path,
    manifest::{storage::CsvDetails, stream::StreamsConfig},
    metrics::indexing as metrics,
    EthereumSqlTypeWrapper, PostgresClient,
};

async fn get_last_synced_block_number_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
    cursor_key: &str,
) -> Result<Option<U64>, UpdateLastSyncedBlockNumberFile> {
    let file_path = build_last_synced_block_number_file(
        full_path,
        contract_name,
        network,
        event_name,
        cursor_key,
    );

    let path = Path::new(&file_path);

    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    if reader.read_line(&mut line).await? > 0 {
        let value = line.trim();
        let parse = U64::from_str(value);
        return match parse {
            Ok(value) => Ok(Some(value)),
            Err(e) => {
                Err(UpdateLastSyncedBlockNumberFile::ParseError(value.to_string(), e.to_string()))
            }
        };
    }

    Ok(None)
}

fn build_last_synced_block_number_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
    cursor_key: &str,
) -> String {
    let filename = format!(
        "{}-{}-{}",
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase()
    );
    // Preserve the native pipeline's historical filename. Contract subscriptions
    // never read that shared file because it cannot prove their own completeness.
    let filename = if cursor_key == crate::event::cursor::NETWORK_CURSOR_KEY {
        format!("{filename}.txt")
    } else {
        format!("{filename}-{cursor_key}.txt")
    };
    let path = full_path.join(contract_name).join("last-synced-blocks").join(filename);

    path.to_string_lossy().into_owned()
}

pub struct SyncConfig<'a> {
    pub project_path: &'a Path,
    pub postgres: &'a Option<Arc<PostgresClient>>,
    pub clickhouse: &'a Option<Arc<ClickhouseClient>>,
    pub csv_details: &'a Option<CsvDetails>,
    pub stream_details: &'a Option<&'a StreamsConfig>,
    pub contract_csv_enabled: bool,
    pub indexer_name: &'a str,
    pub contract_name: &'a str,
    pub event_name: &'a str,
    pub network: &'a str,
    pub cursor_key: &'a str,
}

/// Seed only this query's cursor. A legacy network maximum cannot establish
/// completeness for any one independently scheduled manifest detail.
pub(crate) async fn initialize_subscription_cursor(config: &SyncConfig<'_>) -> Result<(), String> {
    let schema = generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
    if let Some(postgres) = config.postgres {
        let table = generate_internal_event_table_name(&schema, config.event_name);
        postgres.execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ($1,$2,0) ON CONFLICT (network,cursor_key) DO NOTHING"), &[&config.network,&config.cursor_key]).await.map_err(|error| format!("Cannot initialize subscription cursor {table}/{}: {error}. Apply the reviewed cursor schema migration before indexing; no subscription progress was inferred from a legacy maximum",config.cursor_key))?;
    } else if let Some(clickhouse) = config.clickhouse {
        #[derive(Row, Deserialize)]
        struct Cursor {
            count: u64,
        }
        let table = generate_internal_event_table_name_no_shorten(&schema, config.event_name);
        #[derive(Row, Deserialize)]
        struct SortingKey {
            sorting_key: String,
        }
        let sorting = clickhouse.query_one::<SortingKey>(&format!("SELECT sorting_key FROM system.tables WHERE database = 'rindexer_internal' AND name = '{table}'")).await.map_err(|error| format!("Cannot inspect ClickHouse cursor table {table}: {error}"))?;
        if sorting
            .sorting_key
            .replace(' ', "")
            .trim_start_matches("tuple")
            .trim_start_matches('(')
            .trim_end_matches(')')
            != "network,cursor_key"
        {
            return Err(format!("Cannot resume ClickHouse cursor table {table}: sorting key is {}. Rebuild the cursor table with ORDER BY (network,cursor_key) before indexing; a network-only key merges independent subscriptions", sorting.sorting_key));
        }
        let row = clickhouse.query_one::<Cursor>(&format!("SELECT count() AS count FROM rindexer_internal.{table} FINAL WHERE network = '{}' AND cursor_key = '{}'",config.network,config.cursor_key)).await.map_err(|error| format!("Cannot initialize subscription cursor {table}/{}: {error}. Existing ClickHouse cursor tables require a reviewed rebuild with ORDER BY (network,cursor_key) before indexing",config.cursor_key))?;
        if row.count == 0 {
            clickhouse.execute(&format!("INSERT INTO rindexer_internal.{table} (network,cursor_key,last_synced_block) VALUES ('{}','{}',0)",config.network,config.cursor_key)).await.map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

pub async fn get_last_synced_block_number(config: SyncConfig<'_>) -> Result<Option<U64>, String> {
    if let Some(postgres) = config.postgres {
        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table = generate_internal_event_table_name(&schema, config.event_name);
        let row = postgres.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network = $1 AND cursor_key = $2"), &[&config.network, &config.cursor_key]).await.map_err(|error| format!("Cannot read subscription cursor {table}/{}: {error}",config.cursor_key))?;
        let value: String = row.get(0);
        let value = U64::from_str(&value).map_err(|error| {
            format!("Cannot parse subscription cursor {table}/{}: {error}", config.cursor_key)
        })?;
        return Ok((!value.is_zero()).then_some(value));
    }
    if let Some(clickhouse) = config.clickhouse {
        #[derive(Row, Deserialize)]
        struct LastBlock {
            last_synced_block: u64,
        }
        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table = generate_internal_event_table_name_no_shorten(&schema, config.event_name);
        let row = clickhouse.query_one::<LastBlock>(&format!("SELECT last_synced_block FROM rindexer_internal.{table} FINAL WHERE network = '{}' AND cursor_key = '{}'",config.network,config.cursor_key)).await.map_err(|error| format!("Cannot read subscription cursor {table}/{}: {error}",config.cursor_key))?;
        return Ok((row.last_synced_block != 0).then(|| U64::from(row.last_synced_block)));
    }
    // 2. File-based fallbacks (only when no database storage is configured)

    // Check CSV file for last seen block
    if config.contract_csv_enabled {
        if let Some(csv_details) = config.csv_details {
            return if let Ok(result) = get_last_synced_block_number_file(
                &get_full_path(config.project_path, &csv_details.path).unwrap_or_else(|_| {
                    panic!("failed to get full path {}", config.project_path.display())
                }),
                config.contract_name,
                config.network,
                config.event_name,
                config.cursor_key,
            )
            .await
            {
                if let Some(value) = result {
                    if value.is_zero() {
                        return Ok(None);
                    }
                }

                Ok(result)
            } else {
                error!("Error fetching last synced block from CSV");
                Err("Cannot read CSV subscription cursor".to_string())
            };
        }
    }

    // Check stream files for last seen block
    if config.stream_details.is_some() {
        let stream_details = config.stream_details.as_ref().unwrap();

        // create the path if it does not exist
        stream_details
            .create_full_streams_last_synced_block_path(config.project_path, config.contract_name)
            .await;

        return if let Ok(result) = get_last_synced_block_number_file(
            &config.project_path.join(stream_details.get_streams_last_synced_block_path()),
            config.contract_name,
            config.network,
            config.event_name,
            config.cursor_key,
        )
        .await
        {
            if let Some(value) = result {
                if value.is_zero() {
                    return Ok(None);
                }
            }

            Ok(result)
        } else {
            error!("Error fetching last synced block from stream");
            Err("Cannot read stream subscription cursor".to_string())
        };
    }

    Ok(None)
}
#[derive(thiserror::Error, Debug)]
pub enum UpdateLastSyncedBlockNumberFile {
    #[error("File IO error: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("Failed to parse block number: {0} err: {0}")]
    ParseError(String, String),
}

async fn update_last_synced_block_number_for_file(
    contract_name: &str,
    network: &str,
    event_name: &str,
    full_path: &Path,
    to_block: U64,
    cursor_key: &str,
) -> Result<(), UpdateLastSyncedBlockNumberFile> {
    let file_path = build_last_synced_block_number_file(
        full_path,
        contract_name,
        network,
        event_name,
        cursor_key,
    );

    let last_block = get_last_synced_block_number_file(
        full_path,
        contract_name,
        network,
        event_name,
        cursor_key,
    )
    .await?;

    let to_block_higher_then_last_block =
        if let Some(last_block_value) = last_block { to_block > last_block_value } else { true };

    if last_block.is_none() || to_block_higher_then_last_block {
        let temp_file_path = format!("{file_path}.tmp");
        if let Some(parent) = Path::new(&file_path).parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = File::create(&temp_file_path).await?;
        file.write_all(to_block.to_string().as_bytes()).await?;
        file.sync_all().await?;

        fs::rename(temp_file_path, file_path).await?;
    }

    Ok(())
}

/// Update the last indexed block.
///
/// Note: this is an async task and should be awaited rather than spawned in the background
/// to prevent overloading concurrent "update" statements on the database which will increase
/// lock contention and slow down the system.
pub async fn update_progress_and_last_synced_task(
    config: Arc<EventProcessingConfig>,
    to_block: U64,
    on_complete: impl FnOnce() + Send + 'static,
) {
    let update_last_synced_block_result = tokio::time::timeout(
        Duration::from_millis(100),
        config.progress().update_last_synced_block(
            config.network_contract().cached_provider.chain().id(),
            config.id(),
            to_block,
        ),
    )
    .await;

    // We don't want the in-memory progress reporter to hold up processing. Under high-ingest
    // workloads, contention can be high enough to hang here.
    match update_last_synced_block_result {
        Ok(Err(e)) => error!("Error updating in-mem last synced block result: {:?}", e),
        Err(_) => debug!("Timeout in update_last_synced_block_result, completing early"),
        _ => {}
    };

    let latest = config
        .network_contract()
        .cached_provider
        .get_latest_block()
        .await
        .ok()
        .flatten()
        .map(|b| b.header.number)
        .unwrap_or(0);

    // Record block progress metrics
    let network = &config.network_contract().network;
    let contract = &config.contract_name();
    let event = &config.event_name();
    let to_block_u64 = to_block.to::<u64>();

    metrics::set_last_synced_block(network, contract, event, to_block_u64);
    if latest > 0 {
        metrics::set_latest_chain_block(network, latest);
        metrics::set_blocks_behind(network, contract, event, to_block_u64, latest);
    }

    if let Some(postgres) = &config.postgres() {
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
        let table_name = generate_internal_event_table_name(&schema, &config.event_name());
        let network = &config.network_contract().network;
        let cursor_key = config.network_contract().cursor_key();
        let query = format!(
            "UPDATE rindexer_internal.{table_name} SET last_synced_block = {to_block} WHERE network = '{network}' AND cursor_key = '{cursor_key}' AND {to_block} > last_synced_block;
             UPDATE rindexer_internal.latest_block SET block = {latest} WHERE network = '{network}' AND {latest} > block;"
        );

        let result = postgres.batch_execute(&query).await;

        if let Err(e) = result {
            error!("Error updating db last synced block: {:?}", e);
        }
    } else if let Some(clickhouse) = &config.clickhouse() {
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
        let table_name =
            generate_internal_event_table_name_no_shorten(&schema, &config.event_name());
        let network = &config.network_contract().network;
        let cursor_key = config.network_contract().cursor_key();
        let query = format!(
            r#"
            INSERT INTO rindexer_internal.{table_name} (network, cursor_key, last_synced_block) VALUES ('{network}', '{cursor_key}', {to_block});
            INSERT INTO rindexer_internal.latest_block (network, block) VALUES ('{network}', {latest});
            "#
        );

        let result = clickhouse.execute_batch(&query).await;

        if let Err(e) = result {
            error!("Error updating clickhouse last synced block: {:?}", e);
        }
    } else if let Some(csv_details) = &config.csv_details() {
        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name(),
            &config.network_contract().network,
            &config.event_name(),
            &get_full_path(&config.project_path(), &csv_details.path).unwrap_or_else(|_| {
                panic!("failed to get full path {}", config.project_path().display())
            }),
            to_block,
            &config.network_contract().cursor_key(),
        )
        .await
        {
            error!(
                "Error updating last synced block to CSV - path - {} error - {:?}",
                csv_details.path, e
            );
        }
    } else if let Some(stream_last_synced_block_file_path) =
        &config.stream_last_synced_block_file_path()
    {
        let full_path = config.project_path().join(stream_last_synced_block_file_path);

        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name(),
            &config.network_contract().network,
            &config.event_name(),
            &full_path,
            to_block,
            &config.network_contract().cursor_key(),
        )
        .await
        {
            error!(
                "Error updating last synced block to stream - path - {} error - {:?}",
                stream_last_synced_block_file_path, e
            );
        }
    }

    on_complete();
}

pub async fn evm_trace_update_progress_and_last_synced_task(
    config: Arc<TraceProcessingConfig>,
    to_block: U64,
    on_complete: impl FnOnce() + Send + 'static,
) {
    let update_last_synced_block_result = tokio::time::timeout(
        Duration::from_millis(100),
        config.progress.update_last_synced_block(config.chain_id, &config.id, to_block),
    )
    .await;

    // We don't want the in-memory progress reporter to hold up processing. Under high-ingest
    // workloads, contention can be high enough to hang here.
    match update_last_synced_block_result {
        Ok(Err(e)) => error!("Error updating in-mem last synced trace result: {:?}", e),
        Err(_) => debug!("Timeout in update_last_synced_block_result, completing early"),
        _ => {}
    }

    if let Some(postgres) = &config.postgres {
        // Use the native_transfer table for all trace events since they share the same pipeline
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name, &config.contract_name);
        let table_name = generate_internal_event_table_name(&schema, "native_transfer");
        let query = format!(
                "UPDATE rindexer_internal.{table_name} SET last_synced_block = $1 WHERE network = $2 AND cursor_key = $3 AND $1 > last_synced_block"
            );
        let result = postgres
            .execute(
                &query,
                &[
                    &EthereumSqlTypeWrapper::U64(to_block.to()),
                    &config.network,
                    &crate::event::cursor::NETWORK_CURSOR_KEY,
                ],
            )
            .await;

        if let Err(e) = result {
            error!("Error updating last synced trace block db: {:?}", e);
        }
    }

    if let Some(csv_details) = &config.csv_details {
        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name,
            &config.network,
            &config.event_name,
            &get_full_path(&config.project_path, &csv_details.path).unwrap_or_else(|_| {
                panic!("failed to get full path {}", config.project_path.display())
            }),
            to_block,
            crate::event::cursor::NETWORK_CURSOR_KEY,
        )
        .await
        {
            error!(
                "Error updating last synced block to CSV - path - {} error - {:?}",
                csv_details.path, e
            );
        }
    } else if let Some(stream_last_synced_block_file_path) =
        &config.stream_last_synced_block_file_path
    {
        let full_path = config.project_path.join(stream_last_synced_block_file_path);

        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name,
            &config.network,
            &config.event_name,
            &full_path,
            to_block,
            crate::event::cursor::NETWORK_CURSOR_KEY,
        )
        .await
        {
            error!(
                "Error updating last synced block to stream - path - {} error - {:?}",
                stream_last_synced_block_file_path, e
            );
        }
    }

    on_complete();
}

// ============================================================================
// Cron Sync State Functions
// ============================================================================

/// One PostgreSQL cron checkpoint, scoped by the existing internal table and network.
#[derive(Clone, Copy)]
pub struct CronSyncConfig<'a> {
    pub postgres: &'a Option<Arc<PostgresClient>>,
    pub clickhouse: &'a Option<Arc<ClickhouseClient>>,
    pub indexer_name: &'a str,
    pub contract_name: &'a str,
    pub table_name: &'a str,
    pub cron_index: usize,
    pub network: &'a str,
    /// Required for PostgreSQL; ClickHouse retains its separate protocol.
    pub schedule_identity: Option<&'a CronIdentity>,
}

fn postgres_cron_table(config: CronSyncConfig<'_>) -> String {
    let schema = generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
    generate_internal_cron_table_name(&schema, config.table_name, config.cron_index)
}

fn read_versioned_cron_row(
    row: tokio_postgres::Row,
    table: &str,
    network: &str,
    identity: &CronIdentity,
) -> Result<Option<u64>, String> {
    let version: Option<i16> = row
        .try_get("protocol_version")
        .map_err(|e| format!("Cannot decode cron protocol for {table}/{network}: {e}"))?;
    if version != Some(2) {
        return Err(format!(
            "Cron checkpoint {table}/{network} has uncertified legacy protocol {version:?}; \
             the recorded computation has no verified schedule identity. Stop all writers and \
             reconcile or rebuild destination and progress together"
        ));
    }
    let fingerprint: Option<String> = row.try_get("schedule_fingerprint").map_err(|e| {
        format!("Cannot decode cron schedule certificate for {table}/{network}: {e}")
    })?;
    let descriptor: Option<String> = row.try_get("schedule_descriptor").map_err(|e| {
        format!("Cannot decode cron schedule descriptor for {table}/{network}: {e}")
    })?;
    if fingerprint.as_deref() != Some(identity.fingerprint.as_str())
        || descriptor.as_deref() != Some(identity.descriptor.as_str())
    {
        let reason = descriptor
            .as_deref()
            .map(|stored| cron_identity::mismatch(stored, identity))
            .unwrap_or_else(|| "missing schedule descriptor".into());
        return Err(format!(
            "Historical cron schedule identity mismatch at {table}/{network}: {reason}; \
             stop writers and reconcile or rebuild destination and checkpoint together"
        ));
    }
    let value: Option<String> = row
        .try_get("last_synced_block")
        .map_err(|e| format!("Cannot decode cron progress for {table}/{network}: {e}"))?;
    value
        .map(|text| {
            text.parse::<u64>()
                .map_err(|e| format!("Invalid cron progress {text:?} for {table}/{network}: {e}"))
        })
        .transpose()
}

/// Read authoritative progress. NULL means never started, including when start_block is zero.
/// A missing row, unsupported protocol, or any database error must never mean a fresh schedule.
pub async fn get_last_synced_cron_block(config: CronSyncConfig<'_>) -> Result<Option<U64>, String> {
    if let Some(postgres) = config.postgres {
        let table = postgres_cron_table(config);
        let query = format!(
            "SELECT protocol_version, schedule_fingerprint, schedule_descriptor, \
             last_synced_block::TEXT AS last_synced_block \
             FROM rindexer_internal.{table} WHERE network = $1"
        );
        let row = postgres
            .query_one_or_none(&query, &[&config.network])
            .await
            .map_err(|e| format!("Cannot read cron checkpoint {table}/{}: {e}", config.network))?
            .ok_or_else(|| {
                format!("Cron checkpoint row missing from {table} for network {}", config.network)
            })?;
        let identity = config.schedule_identity.ok_or_else(|| {
            format!("Cannot read historical cron checkpoint {table}: missing schedule identity")
        })?;
        return read_versioned_cron_row(row, &table, config.network, identity)
            .map(|block| block.map(U64::from));
    }

    if let Some(clickhouse) = config.clickhouse {
        #[derive(Row, Deserialize)]
        struct LastBlock {
            last_synced_block: u64,
        }

        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table = generate_internal_cron_table_name_no_shorten(
            &schema,
            config.table_name,
            config.cron_index,
        );
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{table} FINAL WHERE network = '{}'",
            config.network
        );
        let row = clickhouse
            .query_one::<LastBlock>(&query)
            .await
            .map_err(|e| format!("Cannot read ClickHouse cron checkpoint {table}: {e}"))?;
        return Ok((row.last_synced_block != 0).then_some(U64::from(row.last_synced_block)));
    }

    // The no-database path is used by isolated preparation tests.
    Ok(None)
}

/// The only retryable checkpoint-lock failure is PostgreSQL SQLSTATE 55P03,
/// reported by the server for a lock_timeout before any table effects.
/// Semantic identity, protocol and missing-row errors are never retryable.
#[derive(Debug, thiserror::Error)]
pub enum CronCheckpointLockError {
    #[error("{0}")]
    LockTimeout(String),
    #[error("{0}")]
    Permanent(String),
}

/// Lock the checkpoint BEFORE staging any table effects. Waiting on this row
/// serializes both the decision to apply a prepared batch and its eventual commit.
pub async fn lock_postgres_cron_checkpoint(
    transaction: &tokio_postgres::Transaction<'_>,
    config: CronSyncConfig<'_>,
) -> Result<Option<u64>, CronCheckpointLockError> {
    let table = postgres_cron_table(config);
    let query = format!(
        "SELECT protocol_version, schedule_fingerprint, schedule_descriptor, \
         last_synced_block::TEXT AS last_synced_block \
         FROM rindexer_internal.{table} WHERE network = $1 FOR UPDATE"
    );
    let row = transaction
        .query_opt(&query, &[&config.network])
        .await
        .map_err(|e| {
            if e.as_db_error().is_some_and(|db| db.code().code() == "55P03") {
                CronCheckpointLockError::LockTimeout(format!(
                    "Cannot lock cron checkpoint {table}/{}: PostgreSQL lock timeout",
                    config.network
                ))
            } else {
                CronCheckpointLockError::Permanent(format!(
                    "Cannot lock cron checkpoint {table}/{}: {e}",
                    config.network
                ))
            }
        })?
        .ok_or_else(|| {
            CronCheckpointLockError::Permanent(format!(
                "Cron checkpoint row missing from {table} for network {}",
                config.network
            ))
        })?;
    let identity = config.schedule_identity.ok_or_else(|| {
        CronCheckpointLockError::Permanent(format!(
            "Cannot lock historical cron checkpoint {table}: missing schedule identity"
        ))
    })?;
    read_versioned_cron_row(row, &table, config.network, identity)
        .map_err(CronCheckpointLockError::Permanent)
}

/// Advance inside the caller-owned PostgreSQL transaction after every
/// scheduled table operation has completed. An absent row cannot be ignored.
pub async fn advance_postgres_cron_checkpoint(
    transaction: &tokio_postgres::Transaction<'_>,
    config: CronSyncConfig<'_>,
    to_block: u64,
) -> Result<(), String> {
    let table = postgres_cron_table(config);
    let query = format!(
        "UPDATE rindexer_internal.{table} SET last_synced_block = $1 \
         WHERE network = $2 AND protocol_version = 2 \
           AND schedule_fingerprint = $3 AND schedule_descriptor = $4"
    );
    let identity = config.schedule_identity.ok_or_else(|| {
        format!("Cannot advance historical cron checkpoint {table}: missing schedule identity")
    })?;
    let value = EthereumSqlTypeWrapper::U64(to_block);
    let affected = transaction
        .execute(&query, &[&value, &config.network, &identity.fingerprint, &identity.descriptor])
        .await
        .map_err(|e| {
            format!(
                "Cannot advance cron checkpoint {table}/{}: {}",
                config.network,
                pg_error_to_string(&e)
            )
        })?;
    if affected != 1 {
        return Err(format!(
            "Cron checkpoint {table}/{} updated {affected} rows, expected exactly one",
            config.network
        ));
    }
    Ok(())
}

/// ClickHouse continues to use its pre-existing non-transactional protocol.
/// It is not part of the PostgreSQL durability guarantee.
pub async fn update_last_synced_clickhouse_cron_block(
    config: CronSyncConfig<'_>,
    to_block: U64,
) -> Result<(), String> {
    if let Some(clickhouse) = config.clickhouse {
        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table = generate_internal_cron_table_name_no_shorten(
            &schema,
            config.table_name,
            config.cron_index,
        );
        let query = format!(
            "INSERT INTO rindexer_internal.{table} (network, last_synced_block) VALUES ('{}', {to_block})",
            config.network
        );
        clickhouse
            .execute(&query)
            .await
            .map_err(|e| format!("Cannot advance ClickHouse cron checkpoint {table}: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_checkpoint_update_creates_missing_parent_directories() {
        let temp_dir = tempfile::tempdir().unwrap();
        let full_path = temp_dir.path().join(".rindexer/rabbitmq");

        update_last_synced_block_number_for_file(
            "EntryPoint",
            "13337",
            "UserOperationEvent",
            &full_path,
            U64::from(2235535),
            crate::event::cursor::NETWORK_CURSOR_KEY,
        )
        .await
        .unwrap();

        let checkpoint_path = full_path
            .join("EntryPoint")
            .join("last-synced-blocks")
            .join("entrypoint-13337-useroperationevent.txt");

        let contents = fs::read_to_string(checkpoint_path).await.unwrap();
        assert_eq!(contents, "2235535");
    }

    #[tokio::test]
    async fn file_checkpoint_update_keeps_highest_block() {
        let temp_dir = tempfile::tempdir().unwrap();
        let full_path = temp_dir.path().join(".rindexer/rabbitmq");

        update_last_synced_block_number_for_file(
            "EntryPoint",
            "13337",
            "UserOperationEvent",
            &full_path,
            U64::from(20),
            crate::event::cursor::NETWORK_CURSOR_KEY,
        )
        .await
        .unwrap();
        update_last_synced_block_number_for_file(
            "EntryPoint",
            "13337",
            "UserOperationEvent",
            &full_path,
            U64::from(10),
            crate::event::cursor::NETWORK_CURSOR_KEY,
        )
        .await
        .unwrap();

        let checkpoint_path = full_path
            .join("EntryPoint")
            .join("last-synced-blocks")
            .join("entrypoint-13337-useroperationevent.txt");

        let contents = fs::read_to_string(checkpoint_path).await.unwrap();
        assert_eq!(contents, "20");
    }
}
