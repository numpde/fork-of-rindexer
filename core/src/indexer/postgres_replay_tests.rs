//! Replay the real no-code PostgreSQL callback, including an acknowledged-by-server
//! COMMIT whose response is deliberately withheld from the client.
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use alloy::primitives::B256;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

use super::{
    input_validation_tests::{event, params, postgres_client},
    no_code_callback, NoCodeCallbackParams,
};
use crate::{
    database::{
        generate::{generate_indexer_contract_schema_name, generate_tables_for_indexer_sql},
        postgres::generate::generate_internal_event_table_name,
    },
    event::callback_registry::EventResult,
    indexer::tables::TableRuntime,
    PostgresClient,
};

struct Fixture {
    admin: Arc<PostgresClient>,
    client: Arc<PostgresClient>,
    params: NoCodeCallbackParams,
    database: String,
    url: url::Url,
    indexer: crate::indexer::Indexer,
    _directory: tempfile::TempDir,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

async fn client(url: &url::Url) -> Arc<PostgresClient> {
    let mut config: tokio_postgres::Config = url.as_str().parse().unwrap();
    config.ssl_mode(tokio_postgres::config::SslMode::Disable);
    config.connect_timeout(Duration::from_secs(5));
    config.options("-c statement_timeout=15000 -c lock_timeout=5000");
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    let manager = bb8_postgres::PostgresConnectionManager::new(
        config,
        tokio_postgres_rustls::MakeRustlsConnect::new(tls),
    );
    let pool = bb8::Pool::builder()
        .max_size(2)
        .connection_timeout(Duration::from_secs(5))
        .build(manager)
        .await
        .unwrap();
    Arc::new(PostgresClient::from_connection(pool).await.unwrap())
}

impl Fixture {
    async fn new() -> Self {
        let (admin, container) = postgres_client().await;
        let mut url: url::Url = match container.as_ref() {
            Some(container) => format!(
                "postgresql://postgres:postgres@{}:{}/postgres",
                container.get_host().await.unwrap(),
                container.get_host_port_ipv4(5432).await.unwrap()
            )
            .parse()
            .unwrap(),
            None => std::env::var("RINDEXER_TEST_POSTGRES_URL").unwrap().parse().unwrap(),
        };
        let database = format!("rindexer_replay_{:032x}", rand::random::<u128>());
        admin.batch_execute(&format!("CREATE DATABASE {database}")).await.unwrap();
        url.set_path(&database);
        let client = client(&url).await;
        let directory = tempfile::tempdir().unwrap();
        let mut params = params();
        let abi = serde_json::to_value(&params.event).unwrap();
        std::fs::write(directory.path().join("abi.json"), serde_json::to_vec(&vec![abi]).unwrap())
            .unwrap();
        let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({
            "name": "Replay", "project_type": "no-code", "networks": [],
            "contracts": [{"name": "Contract", "abi": "abi.json", "include_events": ["Value"],
                "details": [{"network": "ethereum", "address": "0x0000000000000000000000000000000000000000"}],
                "tables": [{"name": "totals", "global": true,
                    "columns": [{"name": "amount", "type": "uint256", "default": "0"}],
                    "events": [{"event": "Value", "operations": [{"type": "upsert", "set": [{"column": "amount", "action": "add", "value": "$value"}]}]}]
                }]
            }]
        })).unwrap();
        let indexer = manifest.to_indexer();
        let ddl = generate_tables_for_indexer_sql(directory.path(), &indexer, false).unwrap();
        client.batch_execute(ddl.as_str()).await.unwrap();
        params.indexer_name = manifest.name.clone();
        let schema =
            generate_indexer_contract_schema_name(&params.indexer_name, &params.contract_name);
        params.sql_event_table_name = format!("{schema}.value");
        params.tables = Arc::new(
            manifest.contracts[0]
                .tables
                .as_ref()
                .unwrap()
                .iter()
                .cloned()
                .map(|table| TableRuntime::new(table, &params.indexer_name, &params.contract_name))
                .collect(),
        );
        params.postgres = Some(client.clone());
        Self {
            admin,
            client,
            params,
            database,
            url,
            indexer,
            _directory: directory,
            _container: container,
        }
    }

    fn events(&self, values: &[u64]) -> Vec<EventResult> {
        values
            .iter()
            .map(|index| {
                let mut event = event(&self.params, *index);
                event.log.block_hash = Some(B256::repeat_byte(2));
                event.tx_information.block_hash = B256::repeat_byte(2);
                event
            })
            .collect()
    }

    async fn position(&self) -> (i64, String, String) {
        let schema = generate_indexer_contract_schema_name(
            &self.params.indexer_name,
            &self.params.contract_name,
        );
        let cursor = generate_internal_event_table_name(&schema, &self.params.event_info.name);
        let count = self
            .client
            .query_one(&format!("SELECT count(*) FROM {}", self.params.sql_event_table_name), &[])
            .await
            .unwrap()
            .get(0);
        let amount = self
            .client
            .query_one(
                &format!(
                    "SELECT coalesce(sum(amount::numeric), 0)::text FROM {}",
                    self.params.tables[0].full_table_name
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let position = self.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{cursor} WHERE network = 'ethereum'"), &[]).await.unwrap().get(0);
        (count, amount, position)
    }

    async fn finish(self) {
        let Self { admin, client, params, database, _container, .. } = self;
        drop(params);
        drop(client);
        admin
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                &[&database],
            )
            .await
            .unwrap();
        admin.batch_execute(&format!("DROP DATABASE {database}")).await.unwrap();
        drop(admin);
        drop(_container);
    }
}

struct LostCommitResponse {
    url: url::Url,
    dropped: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl LostCommitResponse {
    async fn start(upstream: &url::Url) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let host = upstream.host_str().unwrap().to_string();
        let port = upstream.port().unwrap_or(5432);
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (incoming, _) = accepted.unwrap();
                        let host = host.clone();
                        let dropped = observed.clone();
                        sessions.spawn(async move {
                            let upstream = TcpStream::connect((host.as_str(), port)).await.unwrap();
                            // A dropped session is the injected condition; protocol errors
                            // still fail the test through this JoinSet's result.
                            relay(incoming, upstream, dropped).await
                        });
                    }
                    Some(joined) = sessions.join_next(), if !sessions.is_empty() => {
                        joined.unwrap().unwrap();
                    }
                }
            }
        });
        let mut url = upstream.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(address.port())).unwrap();
        Self { url, dropped, task }
    }

    async fn stop(mut self) {
        self.task.abort();
        let error = (&mut self.task).await.expect_err("proxy runs until explicitly stopped");
        assert!(error.is_cancelled(), "proxy session failed: {error}");
    }
}

impl Drop for LostCommitResponse {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn relay(
    mut incoming: TcpStream,
    mut upstream: TcpStream,
    dropped: Arc<AtomicBool>,
) -> io::Result<()> {
    // The fixture client explicitly disables TLS. Only the startup packet lacks
    // the usual one-byte message tag; all following messages are framed.
    let size = incoming.read_u32().await?;
    if !(8..=1_048_576).contains(&size) {
        return Err(io::Error::other("invalid startup frame"));
    }
    let mut startup = vec![0; size as usize - 4];
    incoming.read_exact(&mut startup).await?;
    upstream.write_u32(size).await?;
    upstream.write_all(&startup).await?;
    let (mut client_read, mut client_write) = incoming.into_split();
    let (mut server_read, mut server_write) = upstream.into_split();
    let forward_requests = tokio::io::copy(&mut client_read, &mut server_write);
    let forward_responses = async {
        loop {
            let tag = match server_read.read_u8().await {
                Ok(tag) => tag,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            };
            let size = server_read.read_u32().await?;
            if !(4..=1_048_576).contains(&size) {
                return Err(io::Error::other("invalid backend frame"));
            }
            let mut body = vec![0; size as usize - 4];
            server_read.read_exact(&mut body).await?;
            if tag == b'C' && body == b"COMMIT\0" && !dropped.swap(true, Ordering::SeqCst) {
                return Ok(()); // PostgreSQL committed; the client receives neither C nor Z.
            }
            client_write.write_u8(tag).await?;
            client_write.write_u32(size).await?;
            client_write.write_all(&body).await?;
        }
    };
    tokio::select! {
        result = forward_requests => result.map(|_| ()),
        result = forward_responses => result,
    }
}

#[tokio::test]
async fn postgres_lost_commit_response_replay_preserves_rows_and_additive_effects() {
    let fixture = Fixture::new().await;
    let proxy = LostCommitResponse::start(&fixture.url).await;
    let mut params = fixture.params.clone();
    params.postgres = Some(client(&proxy.url).await);
    let callback = no_code_callback(Arc::new(params)).event_callback;
    let first = tokio::time::timeout(Duration::from_secs(20), callback(fixture.events(&[1, 2])))
        .await
        .unwrap();
    let after_server_commit = fixture.position().await;
    let dropped = proxy.dropped.load(Ordering::SeqCst);
    let replay = callback(fixture.events(&[1, 2])).await;
    let after_replay = fixture.position().await;
    drop(callback);
    proxy.stop().await;
    fixture.finish().await;
    assert!(dropped, "drop the real PostgreSQL COMMIT response");
    assert!(first.is_err(), "client must see an uncertain commit outcome");
    assert_eq!(
        after_server_commit,
        (2, "3".to_string(), "2".to_string()),
        "server committed every effect before disconnecting"
    );
    replay.expect("retry the uncertain outcome");
    assert_eq!(
        after_replay, after_server_commit,
        "replay must not duplicate raw rows or reapply addition"
    );
}

#[tokio::test]
async fn postgres_regrouped_replay_applies_only_new_events() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    callback(fixture.events(&[2, 3])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (3, "6".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_concurrent_replays_apply_effects_once() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let (first, second) =
        tokio::join!(callback(fixture.events(&[1, 2])), callback(fixture.events(&[1, 2])));
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    first.unwrap();
    second.unwrap();
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_copy_replay_can_filter_down_to_the_insert_path() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&(1..=150).collect::<Vec<_>>())).await.unwrap();
    callback(fixture.events(&(75..=175).collect::<Vec<_>>())).await.unwrap();
    callback(fixture.events(&(1..=175).collect::<Vec<_>>())).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (175, "15400".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_explicit_reset_clears_receipts_before_rebuilding_history() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    // Reset must use stored ownership, not assume the old event is still in a
    // readable ABI when its schema and application history are removed.
    let abi = fixture._directory.path().join("abi.json");
    let saved = fixture._directory.path().join("saved-abi.json");
    std::fs::rename(&abi, &saved).unwrap();
    let drop_sql = crate::database::generate::drop_tables_for_indexer_sql(
        fixture._directory.path(),
        &fixture.indexer,
    );
    fixture.client.batch_execute(drop_sql.as_str()).await.unwrap();
    std::fs::rename(saved, abi).unwrap();
    let create_sql =
        generate_tables_for_indexer_sql(fixture._directory.path(), &fixture.indexer, false)
            .unwrap();
    fixture.client.batch_execute(create_sql.as_str()).await.unwrap();
    callback(fixture.events(&[1, 2])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_configuration_cannot_switch_receipted_stream_to_untracked_writes() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    drop(callback);
    let mut untracked = fixture.params.clone();
    untracked.atomic_safe_concurrency = false;
    let callback = no_code_callback(Arc::new(untracked)).event_callback;
    let result = callback(fixture.events(&[1, 2])).await;
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert!(matches!(result, Err(crate::event::callback_registry::CallbackError::Permanent(_))));
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_distinct_log_indexes_in_one_transaction_remain_distinct() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let mut events = fixture.events(&[1, 2]);
    events[1].tx_information.transaction_hash = events[0].tx_information.transaction_hash;
    events[1].log.transaction_hash = events[0].log.transaction_hash;
    events[1].log.transaction_index = events[0].log.transaction_index;
    events[1].tx_information.transaction_index = events[0].tx_information.transaction_index;
    events[1].tx_information.block_hash = events[0].tx_information.block_hash;
    events[1].log.block_hash = events[0].log.block_hash;
    callback(events.clone()).await.unwrap();
    callback(events).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_unseen_history_behind_cursor_is_not_discarded() {
    let fixture = Fixture::new().await;
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let cursor = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    fixture
        .client
        .batch_execute(&format!("UPDATE rindexer_internal.{cursor} SET last_synced_block = 1000"))
        .await
        .unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (2, "3".to_string(), "1000".to_string()));
}

#[tokio::test]
async fn postgres_failed_write_rolls_back_receipts_with_all_effects() {
    let fixture = Fixture::new().await;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER TABLE {} ADD CONSTRAINT reject_second CHECK (value <> '2')",
            fixture.params.sql_event_table_name
        ))
        .await
        .unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let failed = callback(fixture.events(&[1, 2])).await;
    let after_failure = fixture.position().await;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER TABLE {} DROP CONSTRAINT reject_second",
            fixture.params.sql_event_table_name
        ))
        .await
        .unwrap();
    let corrected = callback(fixture.events(&[1, 2])).await;
    let after_retry = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert!(failed.is_err());
    assert_eq!(after_failure, (0, "0".to_string(), "0".to_string()));
    corrected.unwrap();
    assert_eq!(after_retry, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_duplicates_within_one_batch_apply_once() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 1])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (1, "1".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_existing_untracked_history_requires_reviewed_migration() {
    let fixture = Fixture::new().await;
    fixture.client.batch_execute(&format!("INSERT INTO {} (contract_address, value, tx_hash, block_number, block_hash, network, tx_index, log_index) VALUES ('0x{:040x}', '99', '0x{:064x}', 2, '0x{:064x}', 'ethereum', 0, '0')", fixture.params.sql_event_table_name, 0, 99, 99)).await.unwrap();
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let outcome = callback(fixture.events(&[1, 2])).await;
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert!(
        matches!(outcome, Err(crate::event::callback_registry::CallbackError::Permanent(_))),
        "old rows are not evidence that their derived effects were applied correctly"
    );
    assert_eq!(position, (1, "0".to_string(), "0".to_string()));
}

#[tokio::test]
async fn postgres_reorg_reversal_releases_event_identity_for_canonical_replay() {
    use crate::{
        indexer::reorg::{
            task::{
                DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
                DerivedTableRollbackPlan, EventTableInfo, ReorgTask,
            },
            BlockChainWindow,
        },
        manifest::contract::{ColumnType, SetAction},
    };
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let cursor = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    let totals = fixture.params.tables[0].full_table_name.clone();
    let mut plan = DerivedTableRollbackPlan::default();
    plan.try_add_operation(
        totals.clone(),
        0,
        vec![],
        [("value".to_string(), ColumnType::Uint256)].into(),
        [("amount".to_string(), ColumnType::Uint256)].into(),
    )
    .unwrap();
    let task = ReorgTask {
        network: "ethereum".to_string(),
        fork_point: 2,
        detection_point: 2,
        event_tables: vec![EventTableInfo::try_new(
            schema,
            "value".to_string(),
            cursor,
            "Replay".to_string(),
            "Contract".to_string(),
            "Value".to_string(),
        )
        .unwrap()],
        derived_tables: vec![DerivedTableInfo::try_new(
            totals,
            false,
            vec![DerivedTableRollbackOp::try_new(
                fixture.params.sql_event_table_name.clone(),
                vec![],
                vec![DerivedColumnRollback::try_new(
                    "amount".to_string(),
                    "value".to_string(),
                    SetAction::Add,
                )
                .unwrap()],
                None,
            )
            .unwrap()],
            vec![],
        )
        .unwrap()],
        canonical_blocks: vec![],
    };
    task.execute_with_rollback_plan(
        &mut BlockChainWindow::try_new(16).unwrap(),
        Some(&fixture.client),
        None,
        None,
        &plan,
    )
    .await
    .unwrap();
    let reversed = fixture.position().await;
    callback(fixture.events(&[1, 2])).await.unwrap();
    let replayed = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(reversed, (0, "0".to_string(), "1".to_string()));
    assert_eq!(replayed, (2, "3".to_string(), "2".to_string()));
}
