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
    reconciled_overlap: bool,
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
                    if reconciled_overlap {
                        string(&mut out, "u_256");
                        string(&mut out, "UInt256");
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
    let (client, seen, server) = catalogue(false, false).await;
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
    let (client, _, server) = catalogue(true, false).await;
    let result = detect_schema_changes(&client, &manifest()).await;
    server.abort();
    let error = result
        .expect_err("a legacy physical key needs reviewed migration; never propose dropping it");
    assert!(error.contains("key16") && error.contains("key_16"), "{error}");
}

#[tokio::test]
async fn clickhouse_custom_column_identity_refuses_partial_legacy_overlap() {
    // A previous `u256` literal can be mistaken for the newly declared `U256`'s
    // destination. Adding the missing `u_256` would reassign the existing data.
    let mut manifest = manifest();
    manifest.contracts[0].tables.as_mut().unwrap()[0]
        .columns
        .push(serde_json::from_value(json!({"name":"u256","type":"uint256"})).unwrap());
    for reconciled in [false, true] {
        let (client, _, server) = catalogue(false, reconciled).await;
        let result = detect_schema_changes(&client, &manifest).await;
        server.abort();
        if reconciled {
            assert!(result.expect("both reconciled physical columns are present").is_empty());
        } else {
            let error = result
                .expect_err("do not repurpose an existing literal as a different logical column");
            assert!(error.contains("u256") && error.contains("u_256"), "{error}");
        }
    }
}

#[tokio::test]
async fn clickhouse_custom_column_identity_native_sync_checks_override_only_when_enabled() {
    let mut manifest = manifest();
    let mut tables = manifest.contracts.pop().unwrap().tables.unwrap();
    tables[0].events[0].event = "NativeTransfer".to_owned();
    manifest.native_transfers.tables = Some(tables);
    manifest.native_transfers.enabled = false;
    let (client, seen, server) = catalogue(true, false).await;
    assert!(detect_schema_changes(&client, &manifest).await.unwrap().is_empty());
    assert!(seen.lock().unwrap().is_empty(), "disabled native tables must not be probed");
    manifest.native_transfers.enabled = true;
    let result = detect_schema_changes(&client, &manifest).await;
    server.abort();
    let error =
        result.expect_err("native-only legacy ClickHouse table must refuse automatic migration");
    assert!(error.contains("key16") && error.contains("key_16"), "{error}");
    let queries = seen.lock().unwrap();
    assert!(
        !queries.is_empty() && queries.iter().all(|q| q.contains("database = 'shared_state'")),
        "native override must identify the real catalog table: {queries:?}"
    );
}
