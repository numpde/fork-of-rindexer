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

#[path = "factory_replay_tests.rs"]
mod factory_replay_tests;

#[path = "native_transfer_status_tests.rs"]
mod native_transfer_status_tests;

#[path = "view_call_reorg_tests.rs"]
mod view_call_reorg_tests;

#[tokio::test]
async fn postgres_nested_tuple_callback_stores_rows_and_delivers_webhook_json() {
    use crate::{database::nested_tuple_tests::event_fixture, streams::StreamsClients};
    use axum::{routing::post, Json, Router};

    for siblings in [false, true] {
        let (definition, encoded, expected) = event_fixture(siblings);
        let mut fixture = Fixture::with_event(definition, "Contract").await;
        fixture.params.tables = Arc::new(vec![]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut received) = tokio::sync::mpsc::channel(1);
        let app = Router::new().route(
            "/",
            post(move |Json(payload): Json<serde_json::Value>| {
                let sender = sender.clone();
                async move {
                    sender.send(payload).await.unwrap();
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        fixture.params.streams_clients = Arc::new(Some(
            StreamsClients::new(
                serde_json::from_value(
                    json!({"webhooks": [{"endpoint": format!("http://{address}/"),
            "shared_secret": "disposable-test-fixture", "networks": ["ethereum"],
            "events": [{"event_name": "Nested"}]}]}),
                )
                .unwrap(),
            )
            .await,
        ));
        let mut events = fixture.events(&[1]);
        events[0].log.inner.data = alloy::primitives::LogData::new_unchecked(
            vec![fixture.params.event.selector()],
            encoded.into(),
        );
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        tokio::time::timeout(Duration::from_secs(10), callback(events)).await.unwrap().unwrap();
        let delivered =
            tokio::time::timeout(Duration::from_secs(10), received.recv()).await.unwrap().unwrap();
        assert_eq!(delivered["event_data"][0]["value"], expected["value"]);
        if siblings {
            assert_eq!(delivered["event_data"][0]["after"], expected["after"]);
        } else {
            assert!(delivered["event_data"][0].get("after").is_none());
        }
        let row = fixture
            .client
            .query_one(
                &format!("SELECT value_inner_x FROM {}", fixture.params.sql_event_table_name),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "7");
        if siblings {
            let row = fixture
                .client
                .query_one(
                    &format!(
                        "SELECT value_label, \"after\" FROM {}",
                        fixture.params.sql_event_table_name
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(row.get::<_, String>(0), "kept");
            assert_eq!(row.get::<_, String>(1), "9");
        }
        let receipts: i64 = fixture
            .client
            .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(receipts, 1);
        let schema =
            generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
        let checkpoint = generate_internal_event_table_name(&schema, "Nested");
        let cursor: String = fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{checkpoint} WHERE network = 'ethereum'"), &[]).await.unwrap().get(0);
        // Streams complete after PG writes, so the callback must defer its cursor.
        assert_eq!(cursor, "0");
        drop(callback);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn postgres_reorg_never_advances_a_lagging_stream_checkpoint() {
    let fixture = Fixture::new().await;
    let mut lagging = fixture.indexer.clone();
    lagging.contracts[0].name = "Lagging".to_string();
    fixture
        .client
        .batch_execute(
            generate_tables_for_indexer_sql(fixture._directory.path(), &lagging, false)
                .unwrap()
                .as_str(),
        )
        .await
        .unwrap();
    let checkpoints: Vec<_> = ["Contract", "Lagging"]
        .iter()
        .map(|contract| {
            let schema =
                generate_indexer_contract_schema_name(&fixture.params.indexer_name, contract);
            generate_internal_event_table_name(&schema, &fixture.params.event_info.name)
        })
        .collect();
    for (table, position) in checkpoints.iter().zip([100, 80]) {
        fixture.client.batch_execute(&format!("UPDATE rindexer_internal.{table} SET last_synced_block = {position} WHERE network = 'ethereum'")).await.unwrap();
    }
    fixture
        .client
        .reorg_rollback_transaction(
            &[],
            "ethereum",
            98,
            100,
            &[],
            &checkpoints.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let mut actual = Vec::new();
    for table in &checkpoints {
        actual.push(fixture.client.query_one(&format!("SELECT last_synced_block::text FROM rindexer_internal.{table} WHERE network = 'ethereum'"), &[]).await.unwrap().get::<_, String>(0));
    }
    fixture.finish().await;
    assert_eq!(
        actual,
        ["97", "80"],
        "network rollback must never mark unprocessed blocks as complete"
    );
}

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
        Self::with_event(serde_json::to_value(params().event).unwrap(), "Contract").await
    }

    async fn new_trace() -> Self {
        let mut definitions: Vec<serde_json::Value> =
            serde_json::from_str(crate::indexer::native_transfer::NATIVE_TRANSFER_ABI).unwrap();
        let mut fixture = Self::with_event(definitions.remove(0), "EvmTraces").await;
        fixture.params.atomic_safe_concurrency = false;
        fixture
    }

    async fn with_event(definition: serde_json::Value, contract: &str) -> Self {
        Self::with_table(definition.clone(), contract, json!({"name": "totals", "global": true,
            "columns": [{"name": "amount", "type": "uint256", "default": "0"}],
            "events": [{"event": definition["name"], "operations": [{"type": "upsert", "set": [{"column": "amount", "action": "add", "value": "$value"}]}]}]
        })).await
    }

    async fn with_table(
        definition: serde_json::Value,
        contract: &str,
        table: serde_json::Value,
    ) -> Self {
        Self::with_table_on_networks(definition, contract, table, &["ethereum"]).await
    }

    async fn with_table_on_networks(
        definition: serde_json::Value,
        contract: &str,
        table: serde_json::Value,
        networks: &[&str],
    ) -> Self {
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
        params.event = serde_json::from_value(definition.clone()).unwrap();
        params.event_info = crate::abi::EventInfo::new(
            serde_json::from_value::<crate::abi::ABIItem>(definition.clone()).unwrap(),
        )
        .unwrap();
        params.sql_column_names =
            crate::database::postgres::generate::generate_column_names_only_with_base_properties(
                &params.event_info.inputs,
            );
        params.contract_name = contract.to_string();
        let abi = definition;
        std::fs::write(directory.path().join("abi.json"), serde_json::to_vec(&vec![abi]).unwrap())
            .unwrap();
        let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({
            "name": "Replay", "project_type": "no-code", "networks": [],
            "contracts": [{"name": contract, "abi": "abi.json", "include_events": [params.event_info.name],
                "details": networks.iter().map(|network| json!({"network":network,"address":"0x0000000000000000000000000000000000000000"})).collect::<Vec<_>>(),
                "tables": [table]
            }]
        })).unwrap();
        let indexer = manifest.to_indexer();
        let ddl = generate_tables_for_indexer_sql(directory.path(), &indexer, false).unwrap();
        client.batch_execute(ddl.as_str()).await.unwrap();
        params.indexer_name = manifest.name.clone();
        let schema =
            generate_indexer_contract_schema_name(&params.indexer_name, &params.contract_name);
        params.sql_event_table_name =
            format!("{schema}.{}", crate::helpers::camel_to_snake(&params.event_info.name));
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
        let position = self.client.query_one(&format!("SELECT max(last_synced_block)::text FROM rindexer_internal.{cursor} WHERE network = 'ethereum'"), &[]).await.unwrap().get(0);
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
    assert_lost_commit_replay(ReplayKind::AtomicEvent).await;
}

#[tokio::test]
async fn postgres_lost_commit_response_with_deferred_cursor_preserves_effects() {
    assert_lost_commit_replay(ReplayKind::DeferredEvent).await;
}

#[tokio::test]
async fn postgres_native_transfer_lost_commit_response_preserves_effects() {
    assert_lost_commit_replay(ReplayKind::NativeTransfer).await;
}

enum ReplayKind {
    AtomicEvent,
    DeferredEvent,
    NativeTransfer,
}

async fn assert_lost_commit_replay(kind: ReplayKind) {
    let fixture = match kind {
        ReplayKind::NativeTransfer => Fixture::new_trace().await,
        _ => Fixture::new().await,
    };
    let proxy = LostCommitResponse::start(&fixture.url).await;
    let mut params = fixture.params.clone();
    params.atomic_safe_concurrency = matches!(kind, ReplayKind::AtomicEvent);
    params.postgres = Some(client(&proxy.url).await);
    let callbacks = no_code_callback(Arc::new(params));
    let apply = || match kind {
        ReplayKind::NativeTransfer => {
            (callbacks.trace_callback)(vec![native_trace(1, 1, &[]), native_trace(2, 2, &[])])
        }
        _ => (callbacks.event_callback)(fixture.events(&[1, 2])),
    };
    let commit_errors = crate::metrics::definitions::ATOMIC_BATCHES_TOTAL
        .with_label_values(&["commit_error", "error"]);
    let rollbacks = crate::metrics::definitions::ATOMIC_BATCHES_TOTAL
        .with_label_values(&["rolled_back", "error"]);
    let before = (commit_errors.get(), rollbacks.get());
    let first = tokio::time::timeout(Duration::from_secs(20), apply()).await.unwrap();
    let after_server_commit = fixture.position().await;
    let after_failure = (commit_errors.get(), rollbacks.get());
    let dropped = proxy.dropped.load(Ordering::SeqCst);
    let replay = apply().await;
    let after_replay = fixture.position().await;
    drop(callbacks);
    proxy.stop().await;
    fixture.finish().await;
    assert!(dropped, "drop the real PostgreSQL COMMIT response");
    assert!(first.is_err(), "client must see an uncertain commit outcome");
    assert_eq!(
        after_failure,
        (before.0 + 1.0, before.1),
        "a lost COMMIT response must not be reported as a proven rollback"
    );
    assert_eq!(
        after_server_commit,
        (
            2,
            "3".to_string(),
            if matches!(kind, ReplayKind::AtomicEvent) { "2" } else { "0" }.to_string()
        ),
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
async fn postgres_derived_constant_nul_stops_once_without_effects_and_allows_corrected_replay() {
    use super::permanent_failure_tests::{health, registry, ID};
    use crate::manifest::core::ConstantValue;
    use std::sync::atomic::AtomicUsize;

    let fixture = Fixture::new().await;
    let mut params = fixture.params.clone();
    let mut tables = params.tables.as_ref().clone();
    tables[0].table.columns.push(
        serde_json::from_value(json!({
            "name": "note", "type": "string", "nullable": true
        }))
        .unwrap(),
    );
    tables[0].table.events[0].operations[0].set.push(
        serde_json::from_value(json!({
            "column": "note", "action": "set", "value": "$constant(private_note)"
        }))
        .unwrap(),
    );
    fixture
        .client
        .batch_execute(&format!("ALTER TABLE {} ADD COLUMN note TEXT", tables[0].full_table_name))
        .await
        .unwrap();
    params.tables = Arc::new(tables);
    params.constants = Arc::new(
        [("private_note".to_string(), ConstantValue::Simple("private\0payload".to_string()))]
            .into(),
    );
    let callback = no_code_callback(Arc::new(params.clone())).event_callback;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let registry = registry(
        &params,
        Arc::new(move |events| {
            observed.fetch_add(1, Ordering::SeqCst);
            callback(events)
        }),
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        registry.trigger_event(&ID.to_string(), fixture.events(&[1, 2])),
    )
    .await;
    let rejected_position = fixture.position().await;
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    let failures = registry.failure_state().snapshot();
    let (status, body) = health(vec![registry.failure_state()]).await;
    drop(registry);

    // A fresh callback models restarting after correcting the configured constant.
    params.constants = Arc::new(
        [("private_note".to_string(), ConstantValue::Simple("corrected".to_string()))].into(),
    );
    let corrected = no_code_callback(Arc::new(params)).event_callback;
    let replay = corrected(fixture.events(&[1, 2])).await;
    let corrected_position = fixture.position().await;
    drop(corrected);
    fixture.finish().await;

    assert!(outcome.is_ok(), "a derived text NUL must stop instead of retrying forever");
    let error = outcome.unwrap().expect_err("reject the unrepresentable custom-table value");
    assert!(matches!(error, crate::event::callback_registry::CallbackError::Permanent(_)));
    let error = error.to_string();
    assert!(
        error.contains("note") && error.contains("U+0000"),
        "identify the blocked column and reason: {error}"
    );
    assert!(!error.contains("private\0payload"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(rejected_position, (0, "0".to_string(), "0".to_string()));
    assert_eq!(receipts, 0, "rejected preparation must not leave application claims");
    assert_eq!(failures.len(), 1);
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.to_string().contains("private"));
    replay.unwrap();
    assert_eq!(corrected_position, (2, "3".to_string(), "2".to_string()));
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
async fn postgres_receipted_stream_can_enable_concurrency_without_reapplying_effects() {
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
    result.expect("enabling concurrency must retain PostgreSQL receipt protection");
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

async fn assert_replay_with_deferred_cursor(configure: impl FnOnce(&mut NoCodeCallbackParams)) {
    let fixture = Fixture::new().await;
    let mut params = fixture.params.clone();
    configure(&mut params);
    let callback = no_code_callback(Arc::new(params)).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    callback(fixture.events(&[1, 2])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(
        position,
        (2, "3".to_string(), "0".to_string()),
        "PG effects apply once, but the outer dispatcher still owns this cursor"
    );
}

#[tokio::test]
async fn postgres_concurrent_configuration_keeps_effects_replay_safe_without_early_cursor() {
    assert_replay_with_deferred_cursor(|params| params.atomic_safe_concurrency = false).await;
}

#[tokio::test]
async fn postgres_factory_configuration_keeps_effects_replay_safe_without_early_cursor() {
    assert_replay_with_deferred_cursor(|params| params.is_factory_event = true).await;
}

#[tokio::test]
async fn postgres_csv_replay_keeps_pg_effects_once_and_replays_csv_independently() {
    let output = tempfile::NamedTempFile::new().unwrap();
    assert_replay_with_deferred_cursor(|params| {
        params.csv = Some(Arc::new(crate::AsyncCsvAppender::new(output.path().to_str().unwrap())));
    })
    .await;
    let rows = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(output.path())
        .unwrap()
        .records()
        .count();
    assert_eq!(rows, 4, "PG receipts must not suppress independent CSV re-delivery");
}

#[tokio::test]
async fn postgres_committed_effects_survive_csv_failure_without_reapplication_on_retry() {
    let fixture = Fixture::new().await;
    let mut params = fixture.params.clone();
    params.csv =
        Some(Arc::new(crate::AsyncCsvAppender::new(fixture._directory.path().to_str().unwrap())));
    let callback = no_code_callback(Arc::new(params.clone())).event_callback;
    let rejected_csv = callback(fixture.events(&[1, 2])).await;
    let pg_committed = fixture.position().await;
    drop(callback);
    let output = tempfile::NamedTempFile::new().unwrap();
    params.csv = Some(Arc::new(crate::AsyncCsvAppender::new(output.path().to_str().unwrap())));
    let callback = no_code_callback(Arc::new(params)).event_callback;
    let retry = callback(fixture.events(&[1, 2])).await;
    let after_retry = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert!(rejected_csv.is_err(), "directory is not a writable CSV file");
    assert_eq!(
        pg_committed,
        (2, "3".to_string(), "0".to_string()),
        "PG effects commit as one group before independent outputs"
    );
    retry.unwrap();
    assert_eq!(after_retry, pg_committed, "retrying another sink must not reapply PG effects");
    assert_eq!(
        csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(output.path())
            .unwrap()
            .records()
            .count(),
        2
    );
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

fn native_trace(
    value: u64,
    transaction: u8,
    path: &[usize],
) -> crate::event::callback_registry::TraceResult {
    use alloy::{
        primitives::{Address, Bytes, U256, U64},
        rpc::types::trace::parity::{
            Action, CallAction, CallType, LocalizedTransactionTrace, TransactionTrace,
        },
    };
    let action = CallAction {
        from: Address::repeat_byte(3),
        to: Address::repeat_byte(4),
        value: U256::from(value),
        gas: 21_000,
        input: Bytes::new(),
        call_type: CallType::Call,
    };
    let trace = LocalizedTransactionTrace {
        trace: TransactionTrace {
            action: Action::Call(action.clone()),
            result: None,
            trace_address: path.to_vec(),
            subtraces: 0,
            error: None,
        },
        transaction_hash: Some(B256::repeat_byte(transaction)),
        transaction_position: Some(u64::from(transaction)),
        block_number: Some(2),
        block_hash: Some(B256::repeat_byte(2)),
    };
    crate::event::callback_registry::TraceResult::new_debug_native_transfer(
        &action,
        &trace,
        "ethereum",
        1,
        U64::from(2),
        U64::from(2),
    )
}

#[tokio::test]
async fn postgres_native_transfer_replay_keeps_pg_effects_once_and_defers_cursor() {
    let fixture = Fixture::new_trace().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).trace_callback;
    let traces = vec![native_trace(1, 1, &[]), native_trace(2, 2, &[])];
    callback(traces.clone()).await.unwrap();
    callback(traces).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (2, "3".to_string(), "0".to_string()));
}

#[tokio::test]
async fn postgres_internal_transfers_in_one_transaction_keep_distinct_trace_paths() {
    let fixture = Fixture::new_trace().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).trace_callback;
    callback(vec![native_trace(1, 1, &[0]), native_trace(2, 1, &[1])]).await.unwrap();
    callback(vec![native_trace(2, 1, &[1]), native_trace(3, 1, &[2])]).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(
        position,
        (3, "6".to_string(), "0".to_string()),
        "synthetic log_index=0 cannot identify different internal calls"
    );
}

#[tokio::test]
async fn postgres_deep_trace_paths_fit_receipts_and_remain_replay_safe() {
    let fixture = Fixture::new_trace().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).trace_callback;
    let first = vec![99; 1023];
    let mut second = first.clone();
    second[1022] = 100;
    callback(vec![native_trace(1, 1, &first), native_trace(2, 1, &second)]).await.unwrap();
    callback(vec![native_trace(2, 1, &second)]).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(position, (2, "3".to_string(), "0".to_string()));
}

#[tokio::test]
async fn postgres_v1_receipt_migration_preserves_log_keys_and_replay_protection() {
    let fixture = Fixture::new().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    fixture
        .client
        .batch_execute(include_str!("../database/postgres/testdata/event_applications_v1.sql"))
        .await
        .unwrap();
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let namespace = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    for event in fixture.events(&[1, 2]) {
        fixture.client.execute("INSERT INTO rindexer_internal.event_applications VALUES ($1, 'ethereum', 2, $2, $3, $4)",
            &[&namespace, &event.tx_information.block_hash.as_slice(), &event.tx_information.transaction_hash.as_slice(), &event.tx_information.log_index.to_string()]).await.unwrap();
    }
    fixture
        .client
        .batch_execute(crate::database::postgres::event_applications::schema_sql())
        .await
        .unwrap();
    let positions: Vec<String> = fixture.client.query("SELECT event_position FROM rindexer_internal.event_applications WHERE namespace = $1 ORDER BY event_position", &[&namespace]).await.unwrap().iter().map(|row| row.get(0)).collect();
    callback(fixture.events(&[1, 2])).await.unwrap();
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert_eq!(positions, ["1", "2"]);
    assert_eq!(position, (2, "3".to_string(), "2".to_string()));
}

#[tokio::test]
async fn postgres_trace_with_zero_hash_rejects_the_whole_batch_before_effects() {
    let fixture = Fixture::new_trace().await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).trace_callback;
    let mut invalid = native_trace(2, 1, &[1]);
    if let crate::event::callback_registry::TraceResult::NativeTransfer { tx_information, .. } =
        &mut invalid
    {
        tx_information.block_hash = B256::ZERO;
    }
    let result = callback(vec![native_trace(1, 1, &[0]), invalid]).await;
    let position = fixture.position().await;
    drop(callback);
    fixture.finish().await;
    assert!(
        matches!(result, Err(crate::event::callback_registry::CallbackError::Permanent(_))),
        "a zero-hash placeholder is not an admissible mined trace identity"
    );
    assert_eq!(position, (0, "0".to_string(), "0".to_string()));
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
    let rollbacks = crate::metrics::definitions::ATOMIC_BATCHES_TOTAL
        .with_label_values(&["rolled_back", "error"]);
    let before_rollback = rollbacks.get();
    let failed = callback(fixture.events(&[1, 2])).await;
    let after_rollback = rollbacks.get();
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
    assert_eq!(
        after_rollback,
        before_rollback + 1.0,
        "a rejected pre-COMMIT write remains a rollback"
    );
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
    assert_reorg_replay(ReplayKind::AtomicEvent).await;
}

#[tokio::test]
async fn postgres_native_reorg_reversal_releases_trace_identity_for_canonical_replay() {
    assert_reorg_replay(ReplayKind::NativeTransfer).await;
}

async fn assert_reorg_replay(kind: ReplayKind) {
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
    let fixture = match kind {
        ReplayKind::NativeTransfer => Fixture::new_trace().await,
        _ => Fixture::new().await,
    };
    let callbacks = no_code_callback(Arc::new(fixture.params.clone()));
    let apply = || match kind {
        ReplayKind::NativeTransfer => {
            (callbacks.trace_callback)(vec![native_trace(1, 1, &[0]), native_trace(2, 1, &[1])])
        }
        _ => (callbacks.event_callback)(fixture.events(&[1, 2])),
    };
    apply().await.unwrap();
    let schema = generate_indexer_contract_schema_name(
        &fixture.params.indexer_name,
        &fixture.params.contract_name,
    );
    let cursor = generate_internal_event_table_name(&schema, &fixture.params.event_info.name);
    // Model dispatcher completion for a callback whose transaction defers progress.
    fixture.client.batch_execute(&format!("UPDATE rindexer_internal.{cursor} SET last_synced_block = 2 WHERE network = 'ethereum'")).await.unwrap();
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
            crate::helpers::camel_to_snake(&fixture.params.event_info.name),
            cursor,
            fixture.params.indexer_name.clone(),
            fixture.params.contract_name.clone(),
            fixture.params.event_info.name.clone(),
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
    apply().await.unwrap();
    let replayed = fixture.position().await;
    drop(callbacks);
    fixture.finish().await;
    assert_eq!(reversed, (0, "0".to_string(), "1".to_string()));
    assert_eq!(
        replayed,
        (
            2,
            "3".to_string(),
            if matches!(kind, ReplayKind::AtomicEvent) { "2" } else { "1" }.to_string()
        )
    );
}

#[tokio::test]
async fn postgres_buffered_reorg_batch_leaves_raw_derived_receipts_and_cursor_unchanged() {
    let fixture = Fixture::new().await;
    let callbacks = no_code_callback(Arc::new(fixture.params.clone()));
    let events = fixture.events(&[2]);
    let before = fixture.position().await;
    crate::indexer::process::checkpoint_order_tests::reject_buffered_callback_after_rollback(
        callbacks.event_callback.clone(),
        events.iter().map(|event| event.log.clone()).collect(),
    )
    .await;
    assert_eq!(fixture.position().await, before);
    assert_eq!(
        fixture
            .client
            .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // The input is representable: a fresh replay must apply the callback normally.
    (callbacks.event_callback)(events).await.unwrap();
    assert_eq!(fixture.position().await.0, 1);
    assert_eq!(fixture.position().await.1, "2");
    assert_eq!(
        fixture
            .client
            .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(callbacks);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_startup_requires_readable_reorg_window() {
    let fixture = Fixture::new().await;
    fixture.client.batch_execute("DROP TABLE rindexer_internal.reorg_block_hashes").await.unwrap();
    let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({
        "name": "Replay", "project_type": "no-code", "contracts": [],
        "networks": [{"name": "ethereum", "chain_id": 1, "rpc": "http://unused.invalid", "reorg_handling": {"enabled": true}}]
    })).unwrap();
    let registry = Arc::new(crate::event::callback_registry::EventCallbackRegistry::new());
    let trace_registry = Arc::new(crate::event::callback_registry::TraceCallbackRegistry::new());
    let progress = crate::indexer::IndexingEventsProgressState::monitor(&[], &[], None).await;
    let result = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &manifest.to_indexer(),
        registry,
        trace_registry,
        &[],
        true,
        Default::default(),
        progress,
    )
    .await;
    assert!(
        result.is_err(),
        "startup must not treat an unreadable persisted window as an empty one"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_startup_stops_after_canonical_validation_rpc_error() {
    use crate::{
        blockclock::BlockClock,
        event::callback_registry::TraceCallbackRegistry,
        event::contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract},
        indexer::IndexingEventsProgressState,
        provider::mock::MockChainProvider,
    };
    use alloy::{
        primitives::{Address, U64},
        rpc::types::ValueOrArray,
    };
    let fixture = Fixture::new().await;
    fixture.client.batch_execute(&format!(
        "INSERT INTO rindexer_internal.reorg_block_hashes (network, block_number, block_hash, parent_hash) VALUES ('ethereum', 100, '{:#x}', '{:#x}')",
        B256::repeat_byte(100), B256::repeat_byte(99),
    )).await.unwrap();
    let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({
        "name": "Replay", "project_type": "no-code", "contracts": [],
        "networks": [{"name": "ethereum", "chain_id": 1, "rpc": "http://unused.invalid", "reorg_handling": {"enabled": true}}]
    })).unwrap();
    let provider = Arc::new(
        MockChainProvider::new(1)
            .with_block_number(2)
            .with_block_fetch_error("canonical history unavailable"),
    );
    let mut registry = super::permanent_failure_tests::registry(
        &fixture.params,
        Arc::new(|_| Box::pin(async { Ok(()) })),
    );
    let id = registry.events[0].id.clone();
    registry.events[0].contract.details.push(NetworkContract {
        id,
        network: "ethereum".to_string(),
        indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        }),
        cached_provider: provider.clone(),
        block_clock: BlockClock::new(None, None, provider),
        decoder: crate::event::callback_registry::noop_decoder(),
        start_block: Some(U64::from(1)),
        end_block: Some(U64::from(2)),
        disable_logs_bloom_checks: true,
    });
    let result = crate::indexer::start::start_indexing_contract_events(
        &manifest,
        fixture._directory.path(),
        Some(fixture.client.clone()),
        None,
        &manifest.to_indexer(),
        registry.complete(),
        Arc::new(TraceCallbackRegistry::new()),
        &[],
        true,
        Default::default(),
        IndexingEventsProgressState::monitor(&[], &[], None).await,
    )
    .await;
    let rejected = result.is_err();
    if let Ok((tasks, _, _, _, _)) = result {
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
    assert!(rejected, "startup must not launch indexing after canonical validation failed");
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_reorg_refetches_discarded_unprocessed_history_before_the_fork() {
    let fixture = Fixture::new().await;
    let mut params = fixture.params.clone();
    params.atomic_safe_concurrency = false;
    let callbacks = no_code_callback(Arc::new(params));
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let checkpoint = generate_internal_event_table_name(&schema, "Value");
    fixture.client.batch_execute(&format!("UPDATE rindexer_internal.{checkpoint} SET last_synced_block = 80 WHERE network = 'ethereum'")).await.unwrap();
    let mut events = fixture.events(&(81..=100).collect::<Vec<_>>());
    for (number, event) in (81..=100).zip(&mut events) {
        event.log.block_number = Some(number);
        event.log.block_hash = Some(B256::repeat_byte(number as u8));
    }
    crate::indexer::process::checkpoint_order_tests::replay_lagging_buffer_after_rollback(
        callbacks.event_callback.clone(),
        events.into_iter().map(|event| event.log).collect(),
        fixture.client.clone(),
        &fixture.params.indexer_name,
    )
    .await;
    assert_eq!(fixture.position().await, (20, "1810".to_string(), "100".to_string()));
    let receipts: i64 = fixture
        .client
        .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 20);
    drop(callbacks);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_same_height_recovery_preserves_the_original_header_until_commit() {
    use crate::indexer::reorg::{
        subscribe_reorgs, BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence,
        ReorgContext, ReorgCoordinator,
    };
    use crate::provider::mock::MockChainProvider;
    use alloy::{
        network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
        rpc::types::{Block, BlockTransactions},
    };
    let fixture = Fixture::new().await;
    let parent = B256::repeat_byte(99);
    let original = B256::repeat_byte(100);
    let replacement = B256::repeat_byte(200);
    let header = |number, hash, parent_hash| {
        AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number, parent_hash, ..Default::default() }.seal(hash),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    };
    let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![
        header(99, parent, B256::repeat_byte(98)),
        header(100, replacement, parent),
    ]));
    let mut window = BlockChainWindow::try_new(20).unwrap();
    window.insert(99, parent, B256::repeat_byte(98));
    window.insert(100, original, parent);
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let checkpoint = generate_internal_event_table_name(&schema, "Value");
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        provider,
        vec![EventTableInfo::try_new(
            schema.clone(),
            "value".to_string(),
            checkpoint,
            fixture.params.indexer_name.clone(),
            "Contract".to_string(),
            "Value".to_string(),
        )
        .unwrap()],
        vec![],
        vec![],
    )
    .unwrap();
    let notification = subscribe_reorgs("ethereum");
    // An identical observation is accepted and persists A as the original header.
    assert!(coordinator.on_new_block(100, original, parent).await.unwrap().is_none());
    let original_text = format!("{original:#x}");
    let query = "SELECT block_hash FROM rindexer_internal.reorg_block_hashes WHERE network = 'ethereum' AND block_number = 100";
    let task = coordinator
        .on_new_block(100, replacement, parent)
        .await
        .unwrap()
        .expect("replacement must trigger recovery");
    assert_eq!(task.fork_point, 100);
    assert_eq!(
        fixture.client.query_one(query, &[]).await.unwrap().get::<_, String>(0),
        original_text
    );
    assert_eq!(notification.take_rewind(), None, "detection alone cannot authorize recovery");
    // Make the actual rollback DELETE fail, without altering its prepared headers.
    fixture
        .client
        .batch_execute(&format!("ALTER TABLE {schema}.value RENAME TO value_saved"))
        .await
        .unwrap();
    let ctx = ReorgContext {
        postgres: Some(&fixture.client),
        clickhouse: None,
        registry: None,
        trace_registry: None,
    };
    assert!(coordinator.handle_reorg(task, &ctx).await.is_err());
    assert_eq!(
        fixture.client.query_one(query, &[]).await.unwrap().get::<_, String>(0),
        original_text
    );
    assert_eq!(notification.take_rewind(), None);
    assert!(
        coordinator.on_new_block(100, replacement, parent).await.is_err(),
        "ordinary headers must not overwrite unrecovered history"
    );
    fixture
        .client
        .batch_execute(&format!("ALTER TABLE {schema}.value_saved RENAME TO value"))
        .await
        .unwrap();
    coordinator.retry_pending_reorg(&ctx).await.unwrap();
    assert_eq!(
        fixture.client.query_one(query, &[]).await.unwrap().get::<_, String>(0),
        format!("{replacement:#x}")
    );
    assert_eq!(notification.take_rewind(), Some(100));
    drop(coordinator);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_removed_log_recovery_retries_on_matching_headers() {
    use crate::indexer::reorg::{
        subscribe_reorgs, BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence,
        ReorgContext, ReorgCoordinator,
    };
    use crate::provider::mock::MockChainProvider;
    use alloy::{
        network::{AnyHeader, AnyRpcBlock, AnyRpcHeader},
        rpc::types::{Block, BlockTransactions},
    };
    let fixture = Fixture::new().await;
    let parent = B256::repeat_byte(99);
    let original = B256::repeat_byte(100);
    let replacement = B256::repeat_byte(200);
    let header = |number, hash, parent_hash| {
        AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number, parent_hash, ..Default::default() }.seal(hash),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    };
    let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![
        header(99, parent, B256::repeat_byte(98)),
        header(100, replacement, parent),
    ]));
    let mut window = BlockChainWindow::try_new(20).unwrap();
    window.insert(99, parent, B256::repeat_byte(98));
    window.insert(100, original, parent);
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let checkpoint = generate_internal_event_table_name(&schema, "Value");
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".to_string(),
        window,
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        provider,
        vec![EventTableInfo::try_new(
            schema.clone(),
            "value".to_string(),
            checkpoint,
            fixture.params.indexer_name.clone(),
            "Contract".to_string(),
            "Value".to_string(),
        )
        .unwrap()],
        vec![],
        vec![],
    )
    .unwrap();
    let notification = subscribe_reorgs("ethereum");
    // An identical observation is accepted and persists A as the original header.
    assert!(coordinator.on_new_block(100, original, parent).await.unwrap().is_none());
    let original_text = format!("{original:#x}");
    let query = "SELECT block_hash FROM rindexer_internal.reorg_block_hashes WHERE network = 'ethereum' AND block_number = 100";
    let task = coordinator.try_create_reorg_task_for_block_range(100, 100).unwrap();
    // Make the actual rollback DELETE fail, without altering its prepared headers.
    fixture
        .client
        .batch_execute(&format!("ALTER TABLE {schema}.value RENAME TO value_saved"))
        .await
        .unwrap();
    let ctx = ReorgContext {
        postgres: Some(&fixture.client),
        clickhouse: None,
        registry: None,
        trace_registry: None,
    };
    assert!(coordinator.handle_reorg(task, &ctx).await.is_err());
    assert_eq!(
        fixture.client.query_one(query, &[]).await.unwrap().get::<_, String>(0),
        original_text
    );
    assert_eq!(notification.take_rewind(), None);
    // The one-shot signal has been consumed. No changed header will recreate it.
    fixture
        .client
        .batch_execute(&format!("ALTER TABLE {schema}.value_saved RENAME TO value"))
        .await
        .unwrap();
    // Cancel an actual retry while its DELETE waits on a PostgreSQL table lock.
    // The consumed request and its already-acquired writer guard must survive.
    let (lock_client, connection) =
        tokio_postgres::connect(fixture.url.as_str(), tokio_postgres::NoTls).await.unwrap();
    let connection_task = tokio::spawn(connection);
    lock_client
        .batch_execute(&format!("BEGIN; LOCK TABLE {schema}.value IN ACCESS EXCLUSIVE MODE"))
        .await
        .unwrap();
    let mut retry = Box::pin(crate::indexer::reorg::detect_and_handle_reorg(
        &mut coordinator,
        100,
        original,
        parent,
        "cancelled recovery test",
        &ctx,
    ));
    let waiting = async {
        loop {
            let blocked: i64 = fixture.client.query_one(
                "SELECT count(*) FROM pg_locks WHERE relation = $1::text::regclass AND NOT granted",
                &[&format!("{schema}.value")],
            ).await.unwrap().get(0);
            if blocked > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        result = &mut retry => panic!("rollback unexpectedly completed while the table was locked: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(10), waiting) => result.expect("rollback must reach the locked table"),
    }
    drop(retry);
    let barrier = crate::indexer::reorg::event_writer_barrier("ethereum");
    assert!(barrier.try_read().is_err(), "cancellation must keep unrecovered writers blocked");
    lock_client.batch_execute("COMMIT").await.unwrap();
    drop(lock_client);
    connection_task.await.unwrap().unwrap();
    let recovered = crate::indexer::reorg::detect_and_handle_reorg(
        &mut coordinator,
        100,
        original,
        parent,
        "pending recovery test",
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(recovered, Some(100), "a matching header must retry the consumed signal");
    let barrier = crate::indexer::reorg::event_writer_barrier("ethereum");
    let _writer = barrier.try_read().expect("recovery must release callback writers");
    assert_eq!(
        fixture.client.query_one(query, &[]).await.unwrap().get::<_, String>(0),
        format!("{replacement:#x}")
    );
    assert_eq!(notification.take_rewind(), Some(100));
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    callback(fixture.events(&[1, 2])).await.unwrap();
    assert_eq!(fixture.position().await, (2, "3".to_string(), "2".to_string()));
    drop(callback);
    drop(_writer);
    drop(coordinator);
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_sequence_positions_choose_the_later_block_in_combined_and_separate_batches() {
    for tx_index in [1000, 1001] {
        for separate in [false, true] {
            let mut fixture = Fixture::new().await;
            let mut tables = fixture.params.tables.as_ref().clone();
            tables[0].table.events[0].operations[0].set[0].action =
                crate::manifest::contract::SetAction::Set;
            fixture.params.tables = Arc::new(tables);
            let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
            let mut events = fixture.events(&[11, 22]);
            for (event, (block, tx)) in events.iter_mut().zip([(100, tx_index), (101, 0)]) {
                event.log.block_number = Some(block);
                event.log.transaction_index = Some(tx);
                event.log.log_index = Some(0);
                event.tx_information.block_number = block;
                event.tx_information.transaction_index = tx;
                event.tx_information.log_index = alloy::primitives::U256::ZERO;
                event.found_in_request.from_block =
                    alloy::primitives::U64::from(if separate { block } else { 100 });
                event.found_in_request.to_block =
                    alloy::primitives::U64::from(if separate { block } else { 101 });
            }
            if separate {
                callback(vec![events[0].clone()]).await.unwrap();
                callback(vec![events[1].clone()]).await.unwrap();
            } else {
                callback(events).await.unwrap();
            }
            assert_eq!(
                fixture.position().await,
                (2, "22".to_string(), "101".to_string()),
                "tx_index={tx_index}, separate={separate}"
            );
            let receipts: i64 = fixture
                .client
                .query_one("SELECT count(*) FROM rindexer_internal.event_applications", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(receipts, 2);
            drop(callback);
            fixture.finish().await;
        }
    }
}

#[tokio::test]
async fn postgres_sequence_positions_reject_indices_outside_the_packed_bounds_before_writes() {
    for transaction in [true, false] {
        let fixture = Fixture::new().await;
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        let mut events = fixture.events(&[1]);
        let large = u64::from(u32::MAX) + 1;
        if transaction {
            events[0].log.transaction_index = Some(large);
            events[0].tx_information.transaction_index = large;
        } else {
            events[0].log.log_index = Some(large);
            events[0].tx_information.log_index = alloy::primitives::U256::from(large);
        }
        let before = fixture.position().await;
        assert!(matches!(
            callback(events).await,
            Err(crate::event::callback_registry::CallbackError::Permanent(_))
        ));
        assert_eq!(fixture.position().await, before);
        drop(callback);
        fixture.finish().await;
    }
}

#[path = "historical_handoff_tests.rs"]
mod historical_handoff_tests;

#[path = "journal_recovery_tests.rs"]
mod journal_recovery_tests;

#[path = "table_condition_tests.rs"]
mod table_condition_tests;

#[path = "subscription_cursor_tests.rs"]
mod subscription_cursor_tests;

#[path = "schema_override_tests.rs"]
mod schema_override_tests;

#[path = "table_timestamp_tests.rs"]
mod table_timestamp_tests;

#[path = "raw_column_collision_tests.rs"]
mod raw_column_collision_tests;

#[path = "health_subscription_tests.rs"]
mod health_subscription_tests;

#[path = "arithmetic_recovery_tests.rs"]
mod arithmetic_recovery_tests;

#[path = "stream_outbox_tests.rs"]
mod stream_outbox_tests;
