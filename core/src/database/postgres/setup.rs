use crate::indexer::schema::PreparedSchema;
use std::path::Path;

use tracing::{debug, info};

use crate::database::generate::{
    drop_prepared_tables_sql, generate_prepared_tables_sql, GenerateTablesForIndexerSqlError,
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
    info!("Setting up postgres");

    let client = PostgresClient::new().await?;
    let disable_event_tables = manifest.storage.postgres_disable_create_tables();

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

    let sql = generate_prepared_tables_sql(schema, disable_event_tables)?;

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
