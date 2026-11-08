use crate::indexer::schema::PreparedSchema;
use std::path::Path;

use tracing::{debug, info};

use crate::database::generate::{
    drop_prepared_tables_sql, generate_prepared_tables_sql_with_network_ids,
    GenerateTablesForIndexerSqlError,
};
use crate::database::postgres::migrations::execute_prepared_migrations;
use crate::{
    database::postgres::client::{PostgresClient, PostgresConnectionError, PostgresError},
    manifest::core::Manifest,
};

#[derive(thiserror::Error, Debug)]
pub enum SetupPostgresError {
    #[error("{0}")]
    PostgresConnection(#[from] PostgresConnectionError),

    #[error("{0}")]
    PostgresError(#[from] PostgresError),

    #[error("Error creating tables for indexer: {0}")]
    GeneratingTables(#[from] GenerateTablesForIndexerSqlError),
}

/// Check persisted schedule identity before CREATE/ALTER, migration, or drop_each_run.
/// A legacy checkpoint without a descriptor is not automatically promoted.
async fn verify_existing_cron_certificates(
    client: &PostgresClient,
    manifest: &Manifest,
    schema: &PreparedSchema,
) -> Result<(), SetupPostgresError> {
    use crate::database::postgres::generate::generate_internal_cron_table_name;
    use crate::indexer::cron_identity::{identify, CronIdentityInput};
    use crate::indexer::verify_versioned_cron_certificate;
    let invalid = |error: String| {
        SetupPostgresError::GeneratingTables(
            GenerateTablesForIndexerSqlError::InvalidPreparedSchema(error),
        )
    };
    for contract in schema.contracts().iter().chain(schema.native()) {
        for runtime in contract.tables.iter() {
            for (cron_index, cron) in runtime.table.cron_mappings().iter().enumerate() {
                if cron.start_block.is_none() {
                    continue;
                }
                let checkpoint = generate_internal_cron_table_name(
                    &contract.schema_name,
                    runtime.table.name(),
                    cron_index,
                );
                let table = format!("rindexer_internal.{checkpoint}");
                let exists = client
                    .query_one("SELECT to_regclass($1)::TEXT AS relation", &[&table])
                    .await?
                    .get::<_, Option<String>>("relation")
                    .is_some();
                if !exists {
                    // Only first-time initialization may create this table;
                    // the DDL checks that no committed destination rows remain.
                    continue;
                }
                let networks = cron.network.as_ref().map_or_else(
                    || contract.declaration.details.iter().map(|d| d.network.as_str()).collect(),
                    |network| vec![network.as_str()],
                );
                for network in networks {
                    let chain_id = manifest
                        .networks
                        .iter()
                        .find(|entry| entry.name == network)
                        .ok_or_else(|| {
                            invalid(format!("No chain identity for cron network {network}"))
                        })?
                        .chain_id;
                    let detail = contract
                        .declaration
                        .details
                        .iter()
                        .find(|entry| entry.network == network)
                        .ok_or_else(|| {
                            invalid(format!("No contract source for cron network {network}"))
                        })?;
                    let address = detail
                        .address
                        .as_ref()
                        .and_then(|value| match value {
                            alloy::rpc::types::ValueOrArray::Value(address) => Some(*address),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            invalid(format!("No static address for cron network {network}"))
                        })?;
                    let requested = identify(CronIdentityInput {
                        indexer: &manifest.name,
                        contract: &contract.declaration.name,
                        table: &runtime.table,
                        destination: &runtime.full_table_name,
                        cron_index,
                        cron,
                        network,
                        address,
                        chain_id: Some(chain_id),
                    })
                    .map_err(invalid)?;
                    let query = format!(
                        "SELECT protocol_version, schedule_fingerprint, schedule_descriptor \
                         FROM {table} WHERE network = $1"
                    );
                    let row = client.query_one_or_none(&query, &[&network]).await.map_err(|error| {
                        invalid(format!(
                            "Cannot verify historical cron schedule identity for {table}/{network}: \
                             {error}. An unsigned legacy checkpoint requires explicit \
                             reconciliation or rebuilding with its destination"
                        ))
                    })?.ok_or_else(|| invalid(format!(
                        "Cannot resume historical cron: missing checkpoint row {table}/{network}"
                    )))?;
                    verify_versioned_cron_certificate(&row, &requested).map_err(|reason| {
                        invalid(format!(
                            "Cannot resume historical cron checkpoint {table}/{network}: \
                             {reason}. No database changes have begun; stop writers \
                             and reconcile or rebuild before resuming"
                        ))
                    })?;
                }
            }
        }
    }
    Ok(())
}

pub async fn setup_postgres(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<PostgresClient, SetupPostgresError> {
    let schema = PreparedSchema::try_new(project_path, &manifest.to_indexer())?;
    setup_postgres_prepared(project_path, manifest, &schema).await
}

/// Create storage from a checked startup schema before starting any writer.
pub async fn setup_postgres_prepared(
    _project_path: &Path,
    manifest: &Manifest,
    schema: &PreparedSchema,
) -> Result<PostgresClient, SetupPostgresError> {
    schema.validate_postgres_identities()?;
    // Admission precedes even drop_each_run; DDL generation alone remains a
    // pure SQL builder for the existing test-only factory sampling fixture.
    crate::indexer::cron_scheduler::validate_postgres_cron_targets(
        schema
            .contracts()
            .iter()
            .chain(schema.native())
            .map(|contract| (&contract.declaration, contract.tables.as_slice())),
    )
    .map_err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema)?;
    info!("Setting up postgres");

    let disable_event_tables = manifest.storage.postgres_disable_create_tables();
    // Admit the complete prepared schema before even an explicitly configured
    // drop_each_run can delete existing storage.
    let chain_ids = manifest
        .networks
        .iter()
        .map(|network| (network.name.clone(), network.chain_id))
        .collect::<std::collections::HashMap<_, _>>();
    let sql =
        generate_prepared_tables_sql_with_network_ids(schema, disable_event_tables, &chain_ids)?;
    if manifest.storage.clickhouse_enabled()
        && schema.contracts().iter().chain(schema.native()).any(|contract| {
            contract.tables.iter().any(|runtime| {
                runtime
                    .table
                    .definition()
                    .cron
                    .iter()
                    .flatten()
                    .any(|entry| entry.start_block.is_some())
            })
        })
    {
        return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(
            "Historical PostgreSQL cron cannot write to mixed PostgreSQL/ClickHouse sinks"
                .to_string(),
        )
        .into());
    }

    let client = PostgresClient::new().await?;
    verify_existing_cron_certificates(&client, manifest, schema).await?;

    if manifest.storage.postgres_drop_each_run() {
        info!(
            "`drop_each_run` enabled so dropping all data for {} before starting",
            &manifest.name
        );
        let sql = drop_prepared_tables_sql(schema);
        client.batch_execute(sql.as_str()).await?;
        info!("Dropped all data for {}", manifest.name);
    }

    if disable_event_tables {
        info!("Creating internal rindexer tables for {}", manifest.name);
    } else {
        info!("Creating tables for {}", manifest.name);
    }

    debug!("{}", sql);
    if let Err(e) = client.batch_execute(sql.as_str()).await {
        tracing::error!("Failed to execute SQL:\n{}", sql);
        tracing::error!("Error: {:?}", e);
        return Err(e.into());
    }

    if disable_event_tables {
        info!("Created tables for {}", manifest.name);
    } else {
        info!("Created internal rindexer tables for {}", manifest.name);
    }

    let version_applied =
        execute_prepared_migrations(&client, schema, disable_event_tables).await?;

    for version in version_applied {
        info!("Applied migration V{} for {}", version.as_u32(), manifest.name);
    }

    Ok(client)
}
