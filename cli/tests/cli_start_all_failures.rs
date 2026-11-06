//! A no-code startup error must be visible to the calling process.
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
async fn cli_start_all_propagates_postgres_setup_failure() {
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
    let project = tempfile::Builder::new().prefix("rindexer_start_failure_").tempdir().unwrap();
    let database = project.path().file_name().unwrap().to_str().unwrap();
    assert!(database.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    let (admin, admin_connection) = connect(&url).await;
    admin.batch_execute(&format!(r#"CREATE DATABASE "{database}""#)).await.unwrap();
    admin
        .batch_execute(&format!(
            r#"ALTER DATABASE "{database}" SET default_transaction_read_only = on"#
        ))
        .await
        .unwrap();
    url.set_path(database);

    fs::write(
        project.path().join("abi.json"),
        r#"[{"type":"event","name":"Value","anonymous":false,"inputs":[]}]"#,
    )
    .unwrap();
    fs::write(
        project.path().join("rindexer.yaml"),
        r#"name: ReadOnly
project_type: no-code
storage:
  postgres: { enabled: true }
networks:
  - { name: local, chain_id: 1, rpc: 'http://127.0.0.1:1' }
contracts:
  - name: Token
    abi: ./abi.json
    details: [{network: local, address: '0x0000000000000000000000000000000000000001'}]
    include_events: [Value]
"#,
    )
    .unwrap();

    for command in ["indexer", "all"] {
        let output = timeout(
            Duration::from_secs(30),
            Command::new(env!("CARGO_BIN_EXE_rindexer_cli"))
                .args(["start", "--path"])
                .arg(project.path())
                .args(["--yes", command])
                .env("DATABASE_URL", url.as_str())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("startup must fail during PostgreSQL setup")
        .unwrap();
        let message = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            message.to_lowercase().contains("read-only"),
            "{command} failed before reaching read-only PostgreSQL setup: {message}"
        );
        assert!(
            !output.status.success(),
            "{command} reported PostgreSQL setup failure but exited with status 0: {message}"
        );
    }

    admin.batch_execute(&format!(r#"DROP DATABASE "{database}""#)).await.unwrap();
    drop(admin);
    admin_connection.await.unwrap().unwrap();
}
