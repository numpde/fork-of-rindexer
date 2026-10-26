//! Exercise the real no-code callback before CSV writes and atomic PostgreSQL progress.
use std::{panic::AssertUnwindSafe, sync::Arc};

use alloy::{
    primitives::{Address, Bytes, Log as PrimitiveLog, LogData, B256, U256, U64},
    rpc::types::Log,
};
use anyhow::{ensure, Result};
use futures::FutureExt;
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

use super::{no_code_callback, NoCodeCallbackParams};
use crate::{
    abi::{ABIItem, EventInfo},
    database::{
        generate::generate_indexer_contract_schema_name,
        postgres::{
            client::{PostgresClient, TEST_DATABASE_URL_LOCK},
            generate::{
                generate_column_names_only_with_base_properties, generate_columns_with_data_types,
                generate_internal_event_table_name,
            },
        },
    },
    event::callback_registry::{EventResult, LogFoundInRequest, TxInformation},
    AsyncCsvAppender,
};

pub(super) fn params() -> NoCodeCallbackParams {
    let definition = json!({"name": "Value", "type": "event", "anonymous": false, "inputs": [{"name": "value", "type": "uint256", "indexed": false}]});
    let event_info =
        EventInfo::new(serde_json::from_value::<ABIItem>(definition.clone()).unwrap()).unwrap();
    let sql_column_names = generate_column_names_only_with_base_properties(&event_info.inputs);
    NoCodeCallbackParams {
        event_info,
        event: serde_json::from_value(definition).unwrap(),
        indexer_name: "input_tests".to_string(),
        contract_name: "Contract".to_string(),
        index_event_in_order: true,
        csv: None,
        postgres: None,
        clickhouse: None,
        sql_event_table_name: String::new(),
        sql_column_names,
        streams_clients: Arc::new(None),
        chat_clients: Arc::new(None),
        tables: Arc::new(vec![]),
        store_raw_events: true,
        providers: Arc::new(Default::default()),
        constants: Arc::new(Default::default()),
        multicall_addresses: Arc::new(Default::default()),
        is_factory_event: false,
        atomic_safe_concurrency: true,
    }
}

pub(super) fn event(params: &NoCodeCallbackParams, index: u64) -> EventResult {
    let hash = B256::repeat_byte(index as u8);
    let log = Log {
        inner: PrimitiveLog {
            address: Address::ZERO,
            data: LogData::new_unchecked(
                vec![params.event.selector()],
                Bytes::copy_from_slice(&U256::from(index).to_be_bytes::<32>()),
            ),
        },
        block_hash: Some(hash),
        block_number: Some(2),
        block_timestamp: None,
        transaction_hash: Some(hash),
        transaction_index: Some(index),
        log_index: Some(index),
        removed: false,
    };
    EventResult {
        log,
        decoded_data: Arc::new(()),
        tx_information: TxInformation {
            chain_id: 1,
            network: "ethereum".to_string(),
            address: Address::ZERO,
            block_hash: hash,
            block_number: 2,
            block_timestamp: None,
            transaction_hash: hash,
            log_index: U256::from(index),
            transaction_index: index,
        },
        found_in_request: LogFoundInRequest {
            cursor_key: crate::event::cursor::NETWORK_CURSOR_KEY.to_string(),
            from_block: U64::from(2),
            to_block: U64::from(2),
        },
    }
}

fn truncate_data(event: &mut EventResult) {
    event.log.inner.data = LogData::new_unchecked(event.log.topics().to_vec(), Bytes::new());
}

async fn csv_rejects_batch(mixed: bool, no_topics: bool) {
    for invalid_first in [true, false] {
        let file = tempfile::NamedTempFile::new().expect("CSV output");
        let mut params = params();
        params.csv = Some(Arc::new(AsyncCsvAppender::new(file.path().to_str().unwrap())));
        let mut invalid = event(&params, 2);
        if no_topics {
            invalid.log.inner.data = LogData::new_unchecked(vec![], Bytes::new());
        } else {
            truncate_data(&mut invalid);
        }
        let transaction_hash = format!("{:#x}", invalid.tx_information.transaction_hash);
        let mut batch = vec![invalid];
        if mixed {
            batch.insert(if invalid_first { 1 } else { 0 }, event(&params, 1));
        }
        let callback = no_code_callback(Arc::new(params)).event_callback;
        let result = AssertUnwindSafe(callback(batch))
            .catch_unwind()
            .await
            .expect("malformed ABI input must return an error rather than panic");
        let error = result
            .expect_err("a rejected event must not disappear behind a successful callback")
            .to_string();
        assert!(
            std::fs::read(file.path()).unwrap().is_empty(),
            "reject the entire batch before writing valid neighbors"
        );
        for context in ["Contract", "Value", "ethereum", transaction_hash.as_str()] {
            assert!(error.contains(context), "identify the failed log using {context}: {error}");
        }
    }
}

#[tokio::test]
async fn mixed_decode_failure_rejects_the_whole_csv_batch() {
    csv_rejects_batch(true, false).await;
}

#[tokio::test]
async fn entirely_undecodable_batch_is_not_successful() {
    csv_rejects_batch(false, false).await;
}

#[tokio::test]
async fn missing_topics_returns_an_error_without_writing_or_panicking() {
    csv_rejects_batch(true, true).await;
}

#[tokio::test]
async fn valid_and_empty_batches_keep_existing_csv_behavior() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut params = params();
    params.csv = Some(Arc::new(AsyncCsvAppender::new(file.path().to_str().unwrap())));
    let batch = vec![event(&params, 1), event(&params, 2)];
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(vec![]).await.expect("empty ranges remain successful");
    assert!(std::fs::read(file.path()).unwrap().is_empty());
    callback(batch).await.expect("valid ABI values remain indexable");
    let mut csv = csv::ReaderBuilder::new().has_headers(false).from_path(file.path()).unwrap();
    let records = csv.records().collect::<std::result::Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(&records[0][1], "1");
    assert_eq!(&records[1][1], "2");
}

async fn postgres_batch_checks(
    postgres: Arc<PostgresClient>,
    params: NoCodeCallbackParams,
    cursor: &str,
) -> Result<()> {
    let valid = event(&params, 1);
    let corrected = event(&params, 2);
    let mut invalid = corrected.clone();
    truncate_data(&mut invalid);
    let table = params.sql_event_table_name.clone();
    let callback = no_code_callback(Arc::new(params)).event_callback;
    let result = callback(vec![valid.clone(), invalid]).await;
    let rows: i64 = postgres.query_one(&format!("SELECT count(*) FROM {table}"), &[]).await?.get(0);
    let position: String = postgres.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{cursor} WHERE network = 'ethereum'"), &[]).await?.get(0);
    ensure!(result.is_err() && rows == 0 && position == "1", "decode failure must reject before writes and cursor advance: result={result:?}, rows={rows}, cursor={position}");
    callback(vec![valid, corrected]).await.map_err(anyhow::Error::msg)?;
    let rows = postgres.query(&format!("SELECT value FROM {table} ORDER BY value"), &[]).await?;
    ensure!(
        rows.len() == 2 && rows[0].get::<_, String>(0) == "1" && rows[1].get::<_, String>(0) == "2",
        "corrected input must index both original events exactly once"
    );
    let position: String = postgres.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{cursor} WHERE network = 'ethereum'"), &[]).await?.get(0);
    ensure!(position == "2", "corrected batch must advance the cursor");
    Ok(())
}

/// Keep the returned native container alive until the pooled client is dropped.
pub(super) async fn postgres_client(
) -> (Arc<PostgresClient>, Option<testcontainers::ContainerAsync<Postgres>>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (url, container) = match std::env::var_os("RINDEXER_TEST_POSTGRES_URL") {
        Some(url) => {
            let url = url.into_string().expect("Unicode fixture URL");
            let config: tokio_postgres::Config = url.parse().expect("valid fixture URL");
            assert_eq!(config.get_dbname(), Some("rindexer_fixture_admin"));
            (url, None)
        }
        None => {
            let container = Postgres::default().start().await.expect("start fixture");
            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            (format!("postgresql://postgres:postgres@{host}:{port}/postgres"), Some(container))
        }
    };
    let mut url = url::Url::parse(&url).expect("fixture URI");
    url.query_pairs_mut().append_pair("connect_timeout", "10");
    let query = format!(
        "{}&options=-c%20statement_timeout%3D15000%20-c%20lock_timeout%3D5000",
        url.query().unwrap()
    );
    url.set_query(Some(&query));
    let postgres = {
        let _guard = TEST_DATABASE_URL_LOCK.lock().await;
        let original = std::env::var_os("DATABASE_URL");
        std::env::set_var("DATABASE_URL", url.as_str());
        let result = PostgresClient::new().await;
        match original {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
        Arc::new(result.expect("fixture client"))
    };
    postgres
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS rindexer_internal; {}",
            crate::database::postgres::event_applications::schema_sql()
        ))
        .await
        .expect("prepare receipt metadata");
    (postgres, container)
}

#[tokio::test]
async fn postgres_decode_failure_keeps_rows_and_cursor_unchanged_until_corrected() {
    let (postgres, _container) = postgres_client().await;
    let mut params = params();
    params.indexer_name = format!("input_{:032x}", rand::random::<u128>());
    let schema = generate_indexer_contract_schema_name(&params.indexer_name, &params.contract_name);
    let cursor = generate_internal_event_table_name(&schema, &params.event_info.name);
    params.sql_event_table_name = format!("{schema}.value");
    params.postgres = Some(postgres.clone());
    let columns = generate_columns_with_data_types(&params.event_info.inputs).join(", ");
    // Only the ABI columns are under test; the fixed metadata shape matches event-table DDL.
    postgres.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE SCHEMA IF NOT EXISTS rindexer_internal;
        CREATE TABLE {schema}.value (contract_address CHAR(42), {columns}, tx_hash CHAR(66), block_number NUMERIC, block_timestamp TIMESTAMPTZ, block_hash CHAR(66), network VARCHAR(50), tx_index NUMERIC, log_index VARCHAR(78));
        CREATE TABLE rindexer_internal.{cursor} (network TEXT PRIMARY KEY, last_synced_block NUMERIC);
        INSERT INTO rindexer_internal.{cursor} VALUES ('ethereum', 1);")).await.expect("create isolated event and cursor tables");
    let result = postgres_batch_checks(postgres.clone(), params, &cursor).await;
    postgres
        .batch_execute(&format!(
            "DROP SCHEMA {schema} CASCADE; DROP TABLE rindexer_internal.{cursor}"
        ))
        .await
        .expect("remove owned fixture tables");
    drop(postgres);
    drop(_container);
    result.expect("decode rejection and corrected replay preserve rows and cursor");
}
