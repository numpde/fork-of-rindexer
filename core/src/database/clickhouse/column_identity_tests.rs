//! A local catalogue exposes the same physical columns and keys as ClickHouse DDL.
use std::sync::{Arc, Mutex};

use axum::{body::Bytes, http::Uri, routing::any, Router};
use serde_json::json;

use super::ClickhouseClient;
use crate::{database::clickhouse::schema_sync::detect_schema_changes, manifest::core::Manifest};

fn manifest() -> Manifest {
    serde_json::from_value(json!({"name":"Names","project_type":"no-code",
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}],
        "contracts":[{"name":"Contract","abi":"unused.json","details":[],"tables":[{
            "name":"Totals","database":"shared_state","columns":[{"name":"key16","type":"uint256"},
                {"name":"U256","type":"uint256"},{"name":"amountValue","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{"type":"upsert","where":{"key16":"$value"},"set":[
                {"column":"U256","action":"set","value":"$value"},{"column":"amountValue","action":"add","value":"$value"}]}]}]
        }]}]})).unwrap()
}

// All fixture strings are under 128 bytes, so their RowBinary length is one byte.
fn string(out: &mut Vec<u8>, s: &str) {
    assert!(s.len() < 128);
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

async fn catalogue(
    legacy: bool,
) -> (ClickhouseClient, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let shared = seen.clone();
    let app = Router::new().route(
        "/",
        any(move |uri: Uri, body: Bytes| {
            let shared = shared.clone();
            async move {
                let sql = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                    .find(|(key, _)| key == "query")
                    .map(|(_, sql)| sql.into_owned())
                    .unwrap_or_else(|| String::from_utf8(body.to_vec()).unwrap());
                shared.lock().unwrap().push(sql.clone());
                let mut out = vec![];
                if sql.contains("count() > 0") {
                    out.push(1);
                } else if sql.contains("FROM system.columns") {
                    for (name, kind) in [
                        ("network", "String"),
                        (if legacy { "key16" } else { "key_16" }, "UInt256"),
                        ("u256", "UInt256"),
                        ("amount_value", "UInt256"),
                        ("rindexer_block_number", "UInt64"),
                        ("rindexer_tx_hash", "FixedString(66)"),
                        ("rindexer_block_hash", "FixedString(66)"),
                        ("rindexer_contract_address", "FixedString(42)"),
                        ("rindexer_sequence_id", "UInt128"),
                    ] {
                        string(&mut out, name);
                        string(&mut out, kind);
                    }
                } else if sql.contains("SELECT sorting_key") {
                    string(&mut out, "network, key_16");
                } else {
                    panic!("unexpected catalogue query: {sql}");
                }
                out
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = ClickhouseClient {
        conn: clickhouse::Client::default()
            .with_url(url)
            .with_compression(clickhouse::Compression::None)
            .with_validation(false),
        batch_size: 100,
    };
    (client, seen, server)
}

#[tokio::test]
async fn clickhouse_custom_column_identity_catalog_matches_ddl_without_reprojecting() {
    let (client, seen, server) = catalogue(false).await;
    let changes = detect_schema_changes(&client, &manifest()).await.unwrap();
    server.abort();
    let queries = seen.lock().unwrap();
    assert!(
        !queries.is_empty() && queries.iter().all(|q| q.contains("database = 'shared_state'")),
        "schema sync must inspect the same override as DDL, queries: {queries:?}"
    );
    assert!(changes.is_empty(), "a current physical schema must not be altered: {changes:?}");
}

#[tokio::test]
async fn clickhouse_custom_column_identity_catalog_refuses_legacy_rename() {
    let (client, _, server) = catalogue(true).await;
    let result = detect_schema_changes(&client, &manifest()).await;
    server.abort();
    let error = result
        .expect_err("a legacy physical key needs reviewed migration; never propose dropping it");
    assert!(error.contains("key16") && error.contains("key_16"), "{error}");
}
