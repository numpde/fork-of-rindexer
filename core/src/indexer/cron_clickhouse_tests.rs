//! Real ClickHouse historical cron regressions. Native invocation owns a
//! Testcontainers server; the rootless runner uses an external disposable fixture.
use super::*;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::generate_internal_cron_table_name_no_shorten;
use crate::provider::mock::MockChainProvider;
use serde::Deserialize;
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;

#[derive(clickhouse::Row, Deserialize)]
struct AmountRow {
    value: String,
}

#[derive(clickhouse::Row, Deserialize)]
struct CountRow {
    value: u64,
}

struct Fixture {
    _container: Option<testcontainers::ContainerAsync<ClickHouseImage>>,
    client: Arc<ClickhouseClient>,
    name: String,
    schema: String,
    checkpoint: String,
}

impl Fixture {
    async fn new(reject_second: bool) -> Self {
        // An explicit endpoint selects the rootless external fixture. Never
        // silently fall back to Testcontainers after an invalid/unreachable URL.
        let (url, container) = match std::env::var("RINDEXER_TEST_CLICKHOUSE_URL") {
            Ok(url) => {
                let parsed = url::Url::parse(&url).expect("valid external ClickHouse URL");
                assert_eq!(parsed.scheme(), "http", "ClickHouse fixture must use HTTP");
                assert!(parsed.host_str().is_some(), "ClickHouse fixture needs a host");
                (url, None)
            }
            Err(std::env::VarError::NotPresent) => {
                let container = ClickHouseImage::default()
                    .start()
                    .await
                    .expect("start native ClickHouse fixture");
                let host = container.get_host().await.expect("ClickHouse fixture host");
                let port =
                    container.get_host_port_ipv4(8123).await.expect("ClickHouse fixture port");
                (format!("http://{host}:{port}"), Some(container))
            }
            Err(error) => panic!("Invalid RINDEXER_TEST_CLICKHOUSE_URL: {error}"),
        };
        let settings = [
            ("CLICKHOUSE_URL", url.as_str()),
            ("CLICKHOUSE_USER", "default"),
            ("CLICKHOUSE_PASSWORD", ""),
            ("CLICKHOUSE_DB", "default"),
        ];
        let original = settings.iter().map(|(name, _)| std::env::var_os(name)).collect::<Vec<_>>();
        for (name, value) in &settings {
            std::env::set_var(name, value);
        }
        let result = tokio::time::timeout(Duration::from_secs(15), ClickhouseClient::new())
            .await
            .expect("ClickHouse fixture did not respond");
        for ((name, _), previous) in settings.iter().zip(original) {
            match previous {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        let client = Arc::new(result.expect("connect to ClickHouse fixture"));
        let name = format!("CronClickhouse{:016x}", rand::random::<u64>());
        let schema = generate_indexer_contract_schema_name(&name, "Token");
        let checkpoint = generate_internal_cron_table_name_no_shorten(&schema, "samples", 0);
        client.execute("CREATE DATABASE IF NOT EXISTS rindexer_internal").await.unwrap();
        client.execute(&format!("CREATE DATABASE {schema}")).await.unwrap();
        let constraint =
            if reject_second { "CONSTRAINT reject_later_operation CHECK amount != 7," } else { "" };
        client
            .execute(&format!(
                "CREATE TABLE {schema}.samples (
                    network String,
                    amount UInt256,
                    rindexer_block_number UInt64,
                    rindexer_tx_hash FixedString(66),
                    rindexer_block_hash FixedString(66),
                    rindexer_contract_address FixedString(42),
                    rindexer_sequence_id UInt128,
                    {constraint}
                    _rindexer_test_marker UInt8 DEFAULT 0
                 ) ENGINE = ReplacingMergeTree(rindexer_sequence_id) ORDER BY network"
            ))
            .await
            .expect("create ClickHouse cron destination");
        client
            .execute(&format!(
                "CREATE TABLE rindexer_internal.{checkpoint} (
                    network String, last_synced_block UInt64
                 ) ENGINE = ReplacingMergeTree(last_synced_block) ORDER BY network"
            ))
            .await
            .expect("create ClickHouse progress");
        client
            .execute(&format!("INSERT INTO rindexer_internal.{checkpoint} VALUES ('test', 0)"))
            .await
            .expect("initialize historical progress");
        Self { _container: container, client, name, schema, checkpoint }
    }

    fn scheduler(&self) -> CronScheduler {
        let manifest: Manifest = serde_json::from_value(json!({
            "name": self.name,
            "project_type": "no-code",
            "networks": [{"name": "test", "chain_id": 1, "rpc": "http://127.0.0.1:1"}],
            "contracts": [{
                "name": "Token",
                "abi": "unused.json",
                "details": [{
                    "network": "test",
                    "address": "0x0000000000000000000000000000000000000001",
                    "end_block": "101"
                }],
                "tables": [{
                    "name": "samples",
                    "global": true,
                    "columns": [{"name": "amount", "type": "uint256", "default": "0"}],
                    "cron": [{
                        "start_block": "101", "end_block": "101", "block_interval": "1",
                        "operations": [
                            {"type": "upsert", "set": [
                                {"column": "amount", "action": "set", "value": "3"}
                            ]},
                            {"type": "upsert", "set": [
                                {"column": "amount", "action": "set", "value": "7"}
                            ]}
                        ]
                    }]
                }]
            }]
        }))
        .expect("ClickHouse cron manifest");
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_block_number(101));
        let providers = Arc::new(HashMap::from([("test".to_owned(), provider)]));
        CronScheduler::try_new(&manifest, None, Some(self.client.clone()), providers)
            .expect("ClickHouse cron scheduler admission")
    }

    async fn amount(&self) -> String {
        self.client
            .query_one::<AmountRow>(&format!(
                "SELECT toString(amount) AS value FROM {}.samples FINAL WHERE network = 'test'",
                self.schema
            ))
            .await
            .expect("read ClickHouse final logical row")
            .value
    }

    async fn persisted_rows(&self) -> u64 {
        self.client
            .query_one::<CountRow>(&format!("SELECT count() AS value FROM {}.samples", self.schema))
            .await
            .expect("read ClickHouse physical rows")
            .value
    }

    async fn checkpoint(&self) -> u64 {
        self.client
            .query_one::<CountRow>(&format!(
                "SELECT max(last_synced_block) AS value FROM rindexer_internal.{}",
                self.checkpoint
            ))
            .await
            .expect("read ClickHouse checkpoint")
            .value
    }

    async fn finish(self) {
        self.client
            .execute(&format!("DROP TABLE rindexer_internal.{}", self.checkpoint))
            .await
            .expect("drop test checkpoint");
        self.client
            .execute(&format!("DROP DATABASE {}", self.schema))
            .await
            .expect("drop isolated ClickHouse test database");
    }
}

#[tokio::test]
async fn clickhouse_applies_declared_operations_in_order() {
    let fixture = Fixture::new(false).await;
    fixture.scheduler().run().await.expect("ordered ClickHouse historical cron");
    assert_eq!(fixture.amount().await, "7", "last declared assignment must win");
    assert_eq!(fixture.checkpoint().await, 101);
    fixture.finish().await;
}

#[tokio::test]
async fn clickhouse_later_failure_keeps_first_write_without_checkpoint() {
    let fixture = Fixture::new(true).await;
    let error = fixture
        .scheduler()
        .run()
        .await
        .expect_err("server must reject the second operation")
        .to_string();
    assert!(error.contains("operation 1"), "wrong error: {error}");
    assert_eq!(fixture.persisted_rows().await, 1, "earlier INSERT survives");
    assert_eq!(fixture.amount().await, "3", "only the earlier assignment remains");
    assert_eq!(fixture.checkpoint().await, 0, "progress has not advanced");
    fixture.finish().await;
}
