//! PostgreSQL round-trip tests for byte array wrappers.
//!
//! `make tests/postgres` supplies an isolated external fixture without Docker access.
//! Native callers retain Testcontainers when RINDEXER_TEST_POSTGRES_URL is absent.

use std::time::Duration;

use alloy::primitives::{Address, Bytes, B256, I256, U256};
use futures::pin_mut;
use rindexer::{EthereumSqlTypeWrapper, PgType};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;
use tokio::task::JoinHandle;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql, Client, Config, NoTls};

struct ExternalDatabase {
    admin: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
    name: String,
}

struct TestDatabase {
    client: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
    external: Option<ExternalDatabase>,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (mut config, external, container) = match std::env::var_os("RINDEXER_TEST_POSTGRES_URL")
        {
            Some(url) => {
                let url = url.into_string().expect("fixture URL is not valid Unicode");
                let mut config: Config = url.parse().expect("invalid fixture configuration");
                assert_eq!(
                    config.get_dbname(),
                    Some("rindexer_fixture_admin"),
                    "external tests require the explicitly named disposable fixture database"
                );
                config.connect_timeout(Duration::from_secs(10));
                config.options("-c statement_timeout=15000 -c lock_timeout=5000");
                let (admin, connection) =
                    config.connect(NoTls).await.expect("external fixture is unavailable");
                let connection = tokio::spawn(connection);
                let name = format!("rindexer_test_{:032x}", rand::random::<u128>());
                admin
                    .batch_execute(&format!("CREATE DATABASE {name}"))
                    .await
                    .expect("failed to create isolated test database");
                config.dbname(&name);
                (config, Some(ExternalDatabase { admin, connection, name }), None)
            }
            None => {
                // Preserve the native upstream runner. The container runner requires
                // the explicit URL before nextest starts; invalid URLs never fall back.
                let container = Postgres::default().start().await.expect("start postgres");
                let host = container.get_host().await.expect("fixture host").to_string();
                let port = container.get_host_port_ipv4(5432).await.expect("fixture port");
                let mut config = Config::new();
                config.host(&host).port(port).user("postgres").password("postgres");
                config.dbname("postgres");
                (config, None, Some(container))
            }
        };
        config.connect_timeout(Duration::from_secs(10));
        config.options("-c statement_timeout=15000 -c lock_timeout=5000");
        let (client, connection) = config.connect(NoTls).await.expect("test database unavailable");
        Self { client, connection: tokio::spawn(connection), external, _container: container }
    }

    async fn close(self) {
        let Self { client, connection, external, _container } = self;
        drop(client);
        tokio::time::timeout(Duration::from_secs(10), connection)
            .await
            .expect("test connection did not close")
            .expect("test connection task panicked")
            .expect("test connection failed");
        if let Some(ExternalDatabase { admin, connection, name }) = external {
            admin
                .batch_execute(&format!("DROP DATABASE {name}"))
                .await
                .expect("failed to drop isolated test database");
            drop(admin);
            tokio::time::timeout(Duration::from_secs(10), connection)
                .await
                .expect("admin connection did not close")
                .expect("admin connection task panicked")
                .expect("admin connection failed");
        }
    }
}

#[tokio::test]
async fn vec_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecBytes",
        EthereumSqlTypeWrapper::VecBytes(vec![
            Bytes::copy_from_slice(&[1u8; 32]),
            Bytes::copy_from_slice(&[2u8; 32]),
        ]),
        vec![vec![1u8; 32], vec![2u8; 32]],
    )
    .await;
}

#[tokio::test]
async fn vec_address_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecAddressBytes",
        EthereumSqlTypeWrapper::VecAddressBytes(vec![
            Address::from([3u8; 20]),
            Address::from([4u8; 20]),
        ]),
        vec![vec![3u8; 20], vec![4u8; 20]],
    )
    .await;
}

#[tokio::test]
async fn vec_b256_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecB256Bytes",
        EthereumSqlTypeWrapper::VecB256Bytes(vec![B256::from([5u8; 32]), B256::from([6u8; 32])]),
        vec![vec![5u8; 32], vec![6u8; 32]],
    )
    .await;
}

#[tokio::test]
async fn vec_u256_bytes_insert_and_copy_into_postgres_bytea_array() {
    let values = vec![U256::from(42u64), U256::from(99u64)];
    let expected_ids = values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect();

    assert_bytea_array_roundtrip(
        "VecU256Bytes",
        EthereumSqlTypeWrapper::VecU256Bytes(values),
        expected_ids,
    )
    .await;
}

#[tokio::test]
async fn vec_i256_bytes_insert_and_copy_into_postgres_bytea_array() {
    let values = vec![I256::try_from(42i64).unwrap(), I256::try_from(-99i64).unwrap()];
    let expected_ids = values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect();

    assert_bytea_array_roundtrip(
        "VecI256Bytes",
        EthereumSqlTypeWrapper::VecI256Bytes(values),
        expected_ids,
    )
    .await;
}

async fn assert_bytea_array_roundtrip(
    name: &str,
    wrapper: EthereumSqlTypeWrapper,
    expected_ids: Vec<Vec<u8>>,
) {
    let database = TestDatabase::new().await;
    let client = &database.client;
    assert_eq!(wrapper.to_type(), PgType::BYTEA_ARRAY, "{name}");

    client
        .batch_execute(
            "CREATE TABLE bytea_array_insert_roundtrip (ids BYTEA[] NOT NULL);
             CREATE TABLE bytea_array_copy_roundtrip (ids BYTEA[] NOT NULL);",
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to create test tables: {error}"));

    client
        .execute("INSERT INTO bytea_array_insert_roundtrip (ids) VALUES ($1)", &[&wrapper])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to insert bytea array: {error}"));

    let row = client
        .query_one("SELECT ids FROM bytea_array_insert_roundtrip", &[])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to query bytea array: {error}"));
    let stored_ids: Vec<Vec<u8>> = row.get("ids");
    assert_eq!(stored_ids, expected_ids, "{name}: insert roundtrip");

    let sink = client
        .copy_in("COPY bytea_array_copy_roundtrip (ids) FROM STDIN WITH (FORMAT binary)")
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to start bytea array copy: {error}"));
    let writer = BinaryCopyInWriter::new(sink, &[PgType::BYTEA_ARRAY]);
    pin_mut!(writer);

    let row: [&(dyn ToSql + Sync); 1] = [&wrapper];
    writer
        .as_mut()
        .write(&row)
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to copy bytea array: {error}"));
    writer
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to finish bytea array copy: {error}"));

    let row = client
        .query_one("SELECT ids FROM bytea_array_copy_roundtrip", &[])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to query copied bytea array: {error}"));
    let stored_ids: Vec<Vec<u8>> = row.get("ids");
    assert_eq!(stored_ids, expected_ids, "{name}: copy roundtrip");
    database.close().await;
}

#[tokio::test]
async fn postgres_fixture_state_and_lifecycle_are_isolated() {
    let left = TestDatabase::new().await;
    let right = TestDatabase::new().await;
    left.client
        .batch_execute(
            "CREATE TABLE isolation_probe (value INTEGER);
             INSERT INTO isolation_probe VALUES (42)",
        )
        .await
        .unwrap();
    right.client.batch_execute("CREATE TABLE isolation_probe (value INTEGER)").await.unwrap();
    let row = right.client.query_one("SELECT COUNT(*) FROM isolation_probe", &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 0, "fixture data leaked between tests");
    left.close().await;
    right.client.execute("INSERT INTO isolation_probe VALUES (7)", &[]).await.unwrap();
    let row = right.client.query_one("SELECT value FROM isolation_probe", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 7, "closing one fixture affected another");
    right.close().await;
}
