//! Exercise custom-table DDL, writes and rollback with the shared unsigned encoder.
use std::{collections::HashMap, time::Duration};

use anyhow::{ensure, Result};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls};

use super::generate_tables_sql;
use crate::database::{
    postgres::{
        client::{PostgresClient, TEST_DATABASE_URL_LOCK},
        generate::{
            generate_column_names_only_with_base_properties, generate_reorg_block_hashes_table_sql,
        },
        schema_sync::{detect_schema_changes, SchemaChange},
    },
    unsigned_integer_tests::{decoded, event, postgres_fixture_url},
};
use crate::indexer::{
    reorg::{
        task::{
            DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
            DerivedTableRollbackPlan, EventTableInfo, ReorgTask,
        },
        window::BlockChainWindow,
    },
    tables::{execute_postgres_operation_internal, parse_literal_value_for_column, TableRowData},
};
use crate::manifest::{
    contract::{injected_columns, ColumnType, OperationType, SetAction, Table},
    core::Manifest,
};
use crate::EthereumSqlTypeWrapper;

const SCHEMA: &str = "integers_values";
const VALUES: &[(&str, &str, ColumnType, &[i64])] = &[
    ("value16", "uint16", ColumnType::Uint16, &[0, 32767, 32768, 65535]),
    ("value24", "uint24", ColumnType::Uint32, &[0, 8388607, 8388608, 16777215]),
    ("value32", "uint32", ColumnType::Uint32, &[0, 2147483647, 2147483648, 4294967295]),
    ("signed16", "int16", ColumnType::Int16, &[-32768, -1, 0, 32767]),
    ("signed32", "int32", ColumnType::Int32, &[-2147483648, -1, 0, 2147483647]),
];

/// Each test owns a database; nextest isolates the DATABASE_URL change by process.
struct TestDatabase {
    postgres: PostgresClient,
    admin: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
    name: String,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        let (url, container) = postgres_fixture_url().await;
        let mut url = url::Url::parse(&url).expect("fixture connection URI");
        let mut config: Config = url.as_str().parse().expect("valid fixture URL");
        config.connect_timeout(Duration::from_secs(10));
        config.options("-c statement_timeout=15000 -c lock_timeout=5000");
        let (admin, connection) = config.connect(NoTls).await.expect("connect to fixture");
        let connection = tokio::spawn(connection);
        let name = format!("unsigned_custom_{:032x}", rand::random::<u128>());
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("create test database");
        url.set_path(&name);
        url.query_pairs_mut()
            .append_pair("sslmode", "disable")
            .append_pair("connect_timeout", "10");
        // PostgreSQL URI options need %20 spaces; form encoding's '+' is literal
        // to tokio-postgres and would become part of the configuration names.
        let query = format!(
            "{}&options=-c%20statement_timeout%3D15000%20-c%20lock_timeout%3D5000",
            url.query().expect("fixture connection parameters")
        );
        url.set_query(Some(&query));
        let postgres = {
            let _guard = TEST_DATABASE_URL_LOCK.lock().await;
            std::env::set_var("DATABASE_URL", url.as_str());
            PostgresClient::new().await.expect("create custom-table client")
        };
        Self { postgres, admin, connection, name, _container: container }
    }

    /// Close pooled connections and remove the database before releasing the native fixture.
    async fn finish(self) {
        drop(self.postgres);
        // Pool connections can outlive the last handle briefly. Terminate only
        // this test's connections; native Testcontainers still uses PostgreSQL 11,
        // which predates DROP DATABASE ... WITH (FORCE).
        self.admin
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                &[&self.name],
            )
            .await
            .expect("close isolated test database connections");
        self.admin
            .batch_execute(&format!("DROP DATABASE {}", self.name))
            .await
            .expect("drop isolated test database");
        drop(self.admin);
        tokio::time::timeout(Duration::from_secs(10), self.connection)
            .await
            .expect("admin connection closes")
            .expect("admin task succeeds")
            .expect("admin connection succeeds");
    }
}

fn table(name: &str, operation: &str) -> Table {
    let mut table: Table = serde_json::from_value(json!({
        "name": name,
        "columns": [{"name": "holder", "type": "string"}],
        "events": [{"event": "Values", "operations": [{
            "type": operation, "where": {"holder": "$holder"}, "set": []
        }]}]
    }))
    .expect("custom table definition");
    for &(column, _, ref column_type, _) in VALUES {
        table.columns.push(
            serde_json::from_value(json!({
                "name": column, "type": column_type.to_type_string()
            }))
            .expect("typed column"),
        );
        table.events[0].operations[0].set.push(
            serde_json::from_value(json!({
                "column": column, "action": "add", "value": format!("${column}")
            }))
            .expect("add operation"),
        );
    }
    table
}

fn metadata(block: u64) -> HashMap<String, EthereumSqlTypeWrapper> {
    HashMap::from([
        (injected_columns::BLOCK_NUMBER.to_string(), EthereumSqlTypeWrapper::U64BigInt(block)),
        (
            injected_columns::TX_HASH.to_string(),
            EthereumSqlTypeWrapper::String(format!("0x{block:064x}")),
        ),
        (
            injected_columns::BLOCK_HASH.to_string(),
            EthereumSqlTypeWrapper::String(format!("0x{block:064x}")),
        ),
        (
            injected_columns::CONTRACT_ADDRESS.to_string(),
            EthereumSqlTypeWrapper::String(format!("0x{:040x}", 1)),
        ),
        (
            injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
            EthereumSqlTypeWrapper::U128(crate::manifest::contract::compute_sequence_id(
                block, 0, 0,
            )),
        ),
    ])
}

fn boundary_row(index: usize, block: u64) -> TableRowData {
    let mut columns = metadata(block);
    columns.insert("holder".to_string(), EthereumSqlTypeWrapper::String(format!("player{index}")));
    for &(column, abi_type, _, values) in VALUES {
        columns.insert(column.to_string(), decoded(abi_type, &[values[index]]));
    }
    TableRowData { columns, network: "ethereum".to_string() }
}

fn increment_row(index: usize) -> TableRowData {
    let mut row = boundary_row(index, 2);
    for &(column, abi_type, ref column_type, values) in VALUES {
        let increment = i64::from(abi_type.starts_with("uint") && values[index] > 0);
        // Cron literals and decoded events must use compatible parameter types.
        let wrapper = parse_literal_value_for_column(&increment.to_string(), column_type)
            .expect("typed increment");
        row.columns.insert(column.to_string(), wrapper);
    }
    row
}

async fn create_table(postgres: &PostgresClient, table: &Table) -> Result<()> {
    postgres.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}")).await?;
    postgres
        .batch_execute(
            &generate_tables_sql(std::slice::from_ref(table), "Values", SCHEMA, &[]).unwrap(),
        )
        .await?;
    Ok(())
}

#[test]
fn custom_table_unsigned_types_preserve_ranges_and_signed_types() {
    for (column_type, expected) in [
        (ColumnType::Uint16, "INTEGER"),
        (ColumnType::Uint32, "BIGINT"),
        (ColumnType::Uint8, "SMALLINT"),
        (ColumnType::Int16, "SMALLINT"),
        (ColumnType::Int32, "INTEGER"),
    ] {
        assert_eq!(column_type.to_postgres_type(), expected);
        assert_eq!(
            ColumnType::Array(Box::new(column_type)).to_postgres_type(),
            format!("{expected}[]")
        );
    }
    assert_eq!(ColumnType::from_solidity_type("uint24"), Some(ColumnType::Uint32));
}

async fn insert_and_upsert(postgres: &PostgresClient) -> Result<()> {
    let tables = [table("inserts", "insert"), table("balances", "upsert")];
    for table in &tables {
        create_table(postgres, table).await?;
        let name = format!("{SCHEMA}.{}", table.name);
        let operation = &table.events[0].operations[0];
        let mut rows: Vec<_> = (0..4).map(|i| boundary_row(i, 1)).collect();
        if operation.operation_type == OperationType::Upsert {
            for (i, row) in rows.iter_mut().enumerate() {
                for &(column, abi_type, _, values) in VALUES {
                    if abi_type.starts_with("uint") && values[i] > 0 {
                        row.columns.insert(column.to_string(), decoded(abi_type, &[values[i] - 1]));
                    }
                }
            }
        }
        execute_postgres_operation_internal(postgres, &name, table, operation, &rows, None)
            .await
            .map_err(anyhow::Error::msg)?;
        if operation.operation_type == OperationType::Upsert {
            let deltas: Vec<_> = (0..4).map(increment_row).collect();
            execute_postgres_operation_internal(postgres, &name, table, operation, &deltas, None)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        let stored = postgres.query(&format!(
            "SELECT value16::text, value24::text, value32::text, signed16::text, signed32::text FROM {name} ORDER BY holder"
        ), &[]).await?;
        ensure!(stored.len() == 4, "{} must store all four boundary cases", table.name);
        for (i, stored) in stored.iter().enumerate() {
            for (j, &(column, _, _, values)) in VALUES.iter().enumerate() {
                ensure!(
                    stored.get::<_, String>(j) == values[i].to_string(),
                    "{}.{column} case {i} changed value",
                    table.name
                );
            }
        }
    }
    let manifest: Manifest = serde_json::from_value(json!({
        "name": "integers", "project_type": "no-code", "networks": [],
        "contracts": [{"name": "Values", "details": [], "abi": "unused.json", "tables": tables}]
    }))?;
    let changes = detect_schema_changes(postgres, &manifest).await.map_err(anyhow::Error::msg)?;
    ensure!(changes.is_empty(), "generated schemas disagree with schema checks: {changes:?}");
    // Existing narrow columns must require manual migration, not pass unnoticed.
    postgres.batch_execute(&format!("TRUNCATE {SCHEMA}.inserts; ALTER TABLE {SCHEMA}.inserts ALTER COLUMN value16 TYPE SMALLINT, ALTER COLUMN value32 TYPE INTEGER")).await?;
    let changes = detect_schema_changes(postgres, &manifest).await.map_err(anyhow::Error::msg)?;
    for (column, expected) in [("value16", "integer"), ("value32", "bigint")] {
        ensure!(changes.iter().any(|change| matches!(change, SchemaChange::ColumnTypeChanged { column_name, new_type, .. } if column_name == column && new_type == expected)), "old {column} schema must require widening: {changes:?}");
    }
    Ok(())
}

#[tokio::test]
async fn postgres_custom_table_insert_and_upsert_preserve_unsigned_values() {
    let fixture = TestDatabase::new().await;
    let result = insert_and_upsert(&fixture.postgres).await;
    fixture.finish().await;
    result.expect("custom-table writes and schemas must preserve unsigned values");
}

async fn rollback(postgres: &PostgresClient) -> Result<()> {
    let table: Table = serde_yaml::from_str(
        r#"
name: totals
columns:
  - { name: key16, type: uint16 }
  - { name: key32, type: uint32 }
  - { name: amount16, type: uint16 }
  - { name: amount32, type: uint32 }
events:
  - event: Values
    operations:
      - type: upsert
        where: { key16: $key16, key32: $key32 }
        set:
          - { column: amount16, action: add, value: $amount16 }
          - { column: amount32, action: add, value: $amount32 }
"#,
    )?;
    create_table(postgres, &table).await?;
    postgres.batch_execute("CREATE SCHEMA rindexer_internal").await?;
    postgres.batch_execute(&generate_reorg_block_hashes_table_sql()).await?;
    let inputs = ["key16", "key32", "amount16", "amount32"]
        .into_iter()
        .zip(["uint16", "uint32", "uint16", "uint32"])
        .map(|(name, abi_type)| {
            let (_, mut inputs) = event(abi_type);
            inputs[0].name = name.to_string();
            inputs.remove(0)
        })
        .collect::<Vec<_>>();
    let source_columns = generate_column_names_only_with_base_properties(&inputs).join(", ");
    let event = crate::abi::EventInfo::new(serde_json::from_value(json!({
        "type":"event", "name":"events", "anonymous":false, "inputs":inputs,
    }))?)?;
    postgres
        .batch_execute(&super::generate_event_table_sql_with_comments(
            &[event],
            "Values",
            SCHEMA,
            vec![],
        ))
        .await?;
    let full_name = format!("{SCHEMA}.totals");
    for (block, amount16, amount32) in [(1, 1, 1), (2, 65534, 4294967294i64)] {
        let mut columns = metadata(block);
        for (column, abi_type, value) in [
            ("key16", "uint16", 65535),
            ("key32", "uint32", 4294967295),
            ("amount16", "uint16", amount16),
            ("amount32", "uint32", amount32),
        ] {
            columns.insert(column.to_string(), decoded(abi_type, &[value]));
        }
        let row = TableRowData { columns, network: "ethereum".to_string() };
        execute_postgres_operation_internal(
            postgres,
            &full_name,
            &table,
            &table.events[0].operations[0],
            &[row],
            None,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        postgres.batch_execute(&format!("INSERT INTO {SCHEMA}.events ({source_columns}) VALUES ('0x{:040x}',65535,4294967295,{amount16},{amount32},'0x{block:064x}',{block},NULL,'0x{block:064x}','ethereum',0,'0')",1)).await?;
    }
    let types = HashMap::from([
        ("key16".to_string(), ColumnType::Uint16),
        ("key32".to_string(), ColumnType::Uint32),
        ("amount16".to_string(), ColumnType::Uint16),
        ("amount32".to_string(), ColumnType::Uint32),
    ]);
    let mut plan = DerivedTableRollbackPlan::default();
    plan.try_add_operation(full_name.clone(), 0, vec![], types.clone(), types)?;
    let task = ReorgTask {
        network: "ethereum".to_string(),
        fork_point: 2,
        detection_point: 2,
        // The event-table entry supplies raw deletion and checkpoint maintenance.
        event_tables: vec![EventTableInfo::try_new(
            SCHEMA.to_string(),
            "events".to_string(),
            "events".to_string(),
            "integers".to_string(),
            "Values".to_string(),
            "Values".to_string(),
        )?],
        derived_tables: vec![DerivedTableInfo::try_new(
            full_name.clone(),
            false,
            vec![DerivedTableRollbackOp::try_new(
                format!("{SCHEMA}.events"),
                vec![
                    ("key16".to_string(), "key16".to_string()),
                    ("key32".to_string(), "key32".to_string()),
                ],
                vec![
                    DerivedColumnRollback::try_new(
                        "amount16".to_string(),
                        "amount16".to_string(),
                        SetAction::Add,
                    )?,
                    DerivedColumnRollback::try_new(
                        "amount32".to_string(),
                        "amount32".to_string(),
                        SetAction::Add,
                    )?,
                ],
                None,
            )?],
            vec![],
        )?],
        canonical_blocks: vec![],
    };
    postgres.batch_execute("CREATE TABLE rindexer_internal.events (network TEXT, last_synced_block NUMERIC); INSERT INTO rindexer_internal.events VALUES ('ethereum', 2)").await?;
    let result = task
        .execute_with_rollback_plan(
            &mut BlockChainWindow::try_new(16)?,
            Some(postgres),
            None,
            None,
            &plan,
        )
        .await?;
    ensure!(result.events_deleted == 1, "only the invalidated event should be deleted");
    let stored = postgres
        .query_one(&format!("SELECT key16, key32, amount16, amount32 FROM {full_name}"), &[])
        .await?;
    ensure!(
        stored.get::<_, i32>(0) == 65535 && stored.get::<_, i64>(1) == 4294967295,
        "rollback changed unsigned row keys"
    );
    ensure!(
        stored.get::<_, i32>(2) == 1 && stored.get::<_, i64>(3) == 1,
        "rollback must retain the first block's contributions"
    );
    let remaining =
        postgres.query_one(&format!("SELECT block_number::text FROM {SCHEMA}.events"), &[]).await?;
    ensure!(remaining.get::<_, String>(0) == "1", "the canonical event must remain");
    let checkpoint = postgres
        .query_one("SELECT last_synced_block::text FROM rindexer_internal.events", &[])
        .await?;
    ensure!(checkpoint.get::<_, String>(0) == "1", "rollback must rewind the checkpoint");
    Ok(())
}

#[tokio::test]
async fn postgres_custom_table_reorg_preserves_unsigned_keys_and_remaining_amounts() {
    let fixture = TestDatabase::new().await;
    let result = rollback(&fixture.postgres).await;
    fixture.finish().await;
    result.expect("custom-table rollback must preserve unsigned keys and amounts");
}
