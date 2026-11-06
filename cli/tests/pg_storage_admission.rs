//! Exercise the CLI boundary: it can synchronize a custom table before core setup resets schemas.
use std::{env, fs, time::Duration};

use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio::{process::Command, time::timeout};
use tokio_postgres::{Client, NoTls};
use url::Url;

async fn connect(
    url: &Url,
) -> (Client, tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
    let (client, connection) = tokio_postgres::connect(url.as_str(), NoTls).await.unwrap();
    (client, tokio::spawn(connection))
}

#[tokio::test]
async fn cli_rejects_raw_table_collision_before_schema_sync_or_reset() {
    let (mut url, _container) = match env::var_os("RINDEXER_TEST_POSTGRES_URL") {
        Some(external) => {
            let external = external.into_string().unwrap();
            let config: tokio_postgres::Config = external.parse().unwrap();
            assert_eq!(config.get_dbname(), Some("rindexer_fixture_admin"));
            (Url::parse(&external).unwrap(), None)
        }
        None => {
            let pg = Postgres::default().start().await.expect("start disposable PostgreSQL");
            let host = pg.get_host().await.unwrap();
            let port = pg.get_host_port_ipv4(5432).await.unwrap();
            (
                Url::parse(&format!("postgresql://postgres:postgres@{host}:{port}/postgres"))
                    .unwrap(),
                Some(pg),
            )
        }
    };
    let project = tempfile::Builder::new().prefix("rindexer_cli_").tempdir().unwrap();
    let database = project.path().file_name().unwrap().to_str().unwrap();
    assert!(database.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    let (admin, admin_connection) = connect(&url).await;
    admin.batch_execute(&format!(r#"CREATE DATABASE "{database}""#)).await.unwrap();
    url.set_path(database);
    let (client, connection) = connect(&url).await;
    client
        .batch_execute(
            "CREATE SCHEMA external_metrics;
         CREATE TABLE external_metrics.totals (network VARCHAR(50) PRIMARY KEY);
         CREATE SCHEMA names_token;
         CREATE TABLE names_token.canary (value TEXT);
         INSERT INTO names_token.canary VALUES ('original');",
        )
        .await
        .unwrap();
    fs::write(
        project.path().join("abi.json"),
        r#"[{"type":"event","name":"Value16","anonymous":false,"inputs":[]},
            {"type":"event","name":"Value_16","anonymous":false,"inputs":[]}]"#,
    )
    .unwrap();
    fs::write(project.path().join("rindexer.yaml"), r#"name: Names
project_type: no-code
storage:
  postgres: { enabled: true, drop_each_run: true }
networks:
  - { name: local, chain_id: 1, rpc: 'http://127.0.0.1:0' }
contracts:
  - name: Metrics
    abi: ./abi.json
    details: [{network: local, address: '0x0000000000000000000000000000000000000001'}]
    include_events: [Value16]
    tables:
      - name: totals
        database: external_metrics
        global: true
        columns: [{name: pending, type: uint256, default: '0'}]
        events: [{event: Value16, operations: [{type: upsert, set: [{column: pending, action: increment}]}]}]
  - name: Token
    abi: ./abi.json
    details: [{network: local, address: '0x0000000000000000000000000000000000000002'}]
    include_events: [Value16, Value_16]
"#).unwrap();
    let output = timeout(
        Duration::from_secs(30),
        Command::new(env!("CARGO_BIN_EXE_rindexer_cli"))
            .args(["start", "--path"])
            .arg(project.path())
            .args(["--yes", "indexer"])
            .env("DATABASE_URL", url.as_str())
            .kill_on_drop(true)
            .output(),
    )
    .await;

    // This override is outside the Names contract schemas. Reset therefore cannot
    // conceal an earlier ALTER; the unrelated canary independently detects reset.
    let added: bool = client.query_one(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'external_metrics'
         AND table_name = 'totals' AND column_name = 'pending')", &[],
    ).await.unwrap().get(0);
    let present: bool = client
        .query_one("SELECT to_regclass('names_token.canary') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    let retained: bool = present
        && client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM names_token.canary WHERE value = 'original')",
                &[],
            )
            .await
            .unwrap()
            .get(0);
    drop(client);
    connection.await.unwrap().unwrap();
    admin.batch_execute(&format!(r#"DROP DATABASE "{database}""#)).await.unwrap();
    drop(admin);
    admin_connection.await.unwrap().unwrap();

    assert_eq!((added, retained), (false, true), "CLI changed PostgreSQL before rejecting the collision (pending column added, canary retained)");
    let output = output.expect("CLI must reject the manifest before it contacts an RPC").unwrap();
    assert!(!output.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for identity in ["Value16", "Value_16", "names_token.value_16"] {
        assert!(message.contains(identity), "missing collision identity {identity}: {message}");
    }
}
