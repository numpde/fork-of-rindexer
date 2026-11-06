//! Real PostgreSQL regressions for historical cron effects and checkpoint durability.
//! Run with cargo nextest: tests use RINDEXER_TEST_POSTGRES_URL's disposable
//! admin database when provided, or a native Testcontainers PostgreSQL.
//! Each test creates a fresh database and exercises the actual scheduler.
use super::*;
use crate::database::generate::{
    generate_indexer_contract_schema_name, generate_tables_for_indexer_sql,
};
use crate::database::postgres::client::TEST_DATABASE_URL_LOCK;
use crate::database::postgres::generate::generate_internal_cron_table_name;
use crate::indexer::postgres_test_wire::LostCommitResponse;
use crate::provider::mock::MockChainProvider;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

struct Fixture {
    _container: Option<testcontainers::ContainerAsync<Postgres>>,
    _directory: tempfile::TempDir,
    admin: Arc<PostgresClient>,
    database: String,
    url: url::Url,
    postgres: Arc<PostgresClient>,
    table: String,
    checkpoint: String,
}

impl Fixture {
    async fn connect(url: &str) -> Arc<PostgresClient> {
        let _guard = TEST_DATABASE_URL_LOCK.lock().await;
        let previous = std::env::var_os("DATABASE_URL");
        std::env::set_var("DATABASE_URL", url);
        let result = PostgresClient::new().await;
        match previous {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
        Arc::new(result.expect("connect to isolated PostgreSQL fixture"))
    }

    async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (admin_url, container) = match std::env::var("RINDEXER_TEST_POSTGRES_URL") {
            Ok(url) => {
                let config: tokio_postgres::Config =
                    url.parse().expect("valid external PostgreSQL fixture URL");
                assert_eq!(
                    config.get_dbname(),
                    Some("rindexer_fixture_admin"),
                    "the external fixture must use the disposable admin database"
                );
                (url, None)
            }
            Err(std::env::VarError::NotPresent) => {
                let container = Postgres::default().start().await.expect("start PostgreSQL");
                let host = container.get_host().await.expect("fixture host");
                let port = container.get_host_port_ipv4(5432).await.expect("fixture port");
                (
                    format!(
                        "postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable"
                    ),
                    Some(container),
                )
            }
            Err(error) => panic!("Invalid RINDEXER_TEST_POSTGRES_URL: {error}"),
        };
        let admin = Self::connect(&admin_url).await;
        let database = format!("rindexer_cron_{:032x}", rand::random::<u128>());
        admin
            .batch_execute(&format!("CREATE DATABASE {database}"))
            .await
            .expect("create isolated cron test database");
        let mut database_url = url::Url::parse(&admin_url).expect("fixture URI");
        database_url.set_path(&database);
        let postgres = Self::connect(database_url.as_str()).await;
        let directory = tempfile::tempdir().expect("temporary manifest directory");
        std::fs::write(directory.path().join("abi.json"), "[]").expect("empty ABI fixture");
        let schema = generate_indexer_contract_schema_name("CronDurability", "Token");
        let table = format!("{schema}.samples");
        let checkpoint = generate_internal_cron_table_name(&schema, "samples", 0);
        postgres
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 CREATE SCHEMA IF NOT EXISTS rindexer_internal;
                 CREATE TABLE {table} (
                     network VARCHAR(50) NOT NULL,
                     amount NUMERIC NOT NULL,
                     rindexer_block_number BIGINT NOT NULL,
                     rindexer_tx_hash CHAR(66) NOT NULL,
                     rindexer_block_hash CHAR(66) NOT NULL,
                     rindexer_contract_address CHAR(42) NOT NULL,
                     rindexer_sequence_id NUMERIC NOT NULL,
                     rindexer_id BIGSERIAL,
                     PRIMARY KEY (network, rindexer_id)
                 );
                 CREATE TABLE rindexer_internal.{checkpoint} (
                     network TEXT PRIMARY KEY,
                     last_synced_block NUMERIC,
                     protocol_version SMALLINT
                 );
                 INSERT INTO rindexer_internal.{checkpoint}
                     (network, last_synced_block, protocol_version)
                 VALUES ('test', NULL, 1);"
            ))
            .await
            .expect("fixture schema");
        Self {
            _container: container,
            _directory: directory,
            admin,
            database,
            url: database_url,
            postgres,
            table,
            checkpoint,
        }
    }

    fn manifest(start: u64, end: u64) -> Manifest {
        let cron = json!({
            "start_block": start.to_string(),
            "end_block": end.to_string(),
            "block_interval": "1",
            "operations": [{
                "type": "insert",
                "set": [{"column": "amount", "action": "set", "value": "7"}]
            }]
        });
        serde_json::from_value(json!({
            "name": "CronDurability",
            "project_type": "no-code",
            "networks": [{"name": "test", "chain_id": 1, "rpc": "http://127.0.0.1:1"}],
            "contracts": [{
                "name": "Token",
                "abi": "abi.json",
                "details": [{
                    "network": "test",
                    "address": "0x0000000000000000000000000000000000000001",
                    "end_block": end.to_string()
                }],
                "tables": [{
                    "name": "samples",
                    "columns": [{"name": "amount", "type": "uint256"}],
                    "cron": [cron]
                }]
            }]
        }))
        .expect("cron manifest")
    }

    fn scheduler_with_postgres(
        &self,
        manifest: &Manifest,
        end: u64,
        postgres: Arc<PostgresClient>,
    ) -> Result<CronScheduler, String> {
        let provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(1).with_block_number(end));
        let providers = HashMap::from([("test".to_owned(), provider)]);
        CronScheduler::try_new(manifest, Some(postgres), None, Arc::new(providers))
    }

    fn scheduler(&self, manifest: &Manifest, end: u64) -> Result<CronScheduler, String> {
        self.scheduler_with_postgres(manifest, end, self.postgres.clone())
    }

    fn worker(&self, start: u64, end: u64) -> CronScheduler {
        self.scheduler(&Self::manifest(start, end), end).expect("cron admission")
    }

    fn duplicate_network_details() -> Manifest {
        let mut manifest = serde_json::to_value(Self::manifest(101, 101)).unwrap();
        let mut second = manifest["contracts"][0]["details"][0].clone();
        second["address"] = json!("0x0000000000000000000000000000000000000002");
        manifest["contracts"][0]["details"].as_array_mut().unwrap().push(second);
        serde_json::from_value(manifest).unwrap()
    }

    fn cross_chain_cron() -> Manifest {
        let mut manifest = serde_json::to_value(Self::manifest(101, 101)).unwrap();
        manifest["contracts"][0]["tables"][0]["cross_chain"] = json!(true);
        serde_json::from_value(manifest).unwrap()
    }

    fn shared_destination_crons() -> Manifest {
        let mut manifest = serde_json::to_value(Self::manifest(101, 101)).unwrap();
        manifest["contracts"][0]["tables"][0]["database"] = json!("shared_cron");
        let mut second = manifest["contracts"][0].clone();
        second["name"] = json!("Other");
        second["details"][0]["address"] = json!("0x0000000000000000000000000000000000000002");
        manifest["contracts"].as_array_mut().unwrap().push(second);
        serde_json::from_value(manifest).unwrap()
    }

    fn additive_manifest() -> Manifest {
        let mut manifest = serde_json::to_value(Self::manifest(101, 101)).unwrap();
        let table = &mut manifest["contracts"][0]["tables"][0];
        table["global"] = json!(true);
        table["columns"][0]["default"] = json!("0");
        let operation = &mut table["cron"][0]["operations"][0];
        operation["type"] = json!("upsert");
        operation["set"][0]["action"] = json!("add");
        serde_json::from_value(manifest).unwrap()
    }

    async fn use_additive_table(&self) {
        self.postgres
            .batch_execute(&format!(
                "DROP TABLE {};
                 CREATE TABLE {} (
                     network VARCHAR(50) PRIMARY KEY,
                     amount NUMERIC NOT NULL DEFAULT 0,
                     rindexer_block_number BIGINT NOT NULL,
                     rindexer_tx_hash CHAR(66) NOT NULL,
                     rindexer_block_hash CHAR(66) NOT NULL,
                     rindexer_contract_address CHAR(42) NOT NULL,
                     rindexer_sequence_id NUMERIC NOT NULL
                 );",
                self.table, self.table
            ))
            .await
            .expect("replace append-only fixture with global additive destination");
    }

    async fn amount(&self) -> Option<String> {
        self.postgres
            .query_one_or_none(&format!("SELECT amount::TEXT FROM {}", self.table), &[])
            .await
            .expect("read aggregate")
            .map(|row| row.get(0))
    }

    async fn production_admission_error(&self, manifest: &Manifest) -> Result<(), String> {
        let schema = crate::indexer::schema::PreparedSchema::try_new(
            self._directory.path(),
            &manifest.to_indexer(),
        )
        .map_err(|error| error.to_string())?;
        let _guard = TEST_DATABASE_URL_LOCK.lock().await;
        let previous = std::env::var_os("DATABASE_URL");
        std::env::set_var("DATABASE_URL", self.url.as_str());
        let result = crate::database::postgres::setup::setup_postgres_prepared(
            self._directory.path(),
            manifest,
            &schema,
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());
        match previous {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
        result
    }

    /// Drive the actual startup DDL generator and execute it again on restart.
    async fn production_setup(&self, start: u64, end: u64) -> Result<(), String> {
        let indexer = Self::manifest(start, end).to_indexer();
        let ddl = generate_tables_for_indexer_sql(self._directory.path(), &indexer, false)
            .map_err(|error| error.to_string())?;
        self.postgres.batch_execute(ddl.as_str()).await.map_err(|error| error.to_string())
    }

    async fn finish(self) {
        let Self { postgres, admin, database, .. } = self;
        drop(postgres);
        // Do not require PostgreSQL 13's DROP DATABASE WITH (FORCE);
        // the shared test fixture also exercises older PostgreSQL versions.
        admin
            .batch_execute(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database}'"
            ))
            .await
            .expect("close isolated fixture connections");
        admin.batch_execute(&format!("DROP DATABASE {database}")).await.expect("drop fixture");
    }

    async fn seed(&self, block: Option<u64>) {
        let value = block.map_or_else(|| "NULL".to_owned(), |number| number.to_string());
        self.postgres
            .batch_execute(&format!(
                "UPDATE rindexer_internal.{} SET last_synced_block = {value}",
                self.checkpoint
            ))
            .await
            .expect("seed progress");
    }

    async fn checkpoint(&self) -> Option<i64> {
        self.postgres
            .query_one(
                &format!(
                    "SELECT last_synced_block::BIGINT FROM rindexer_internal.{} WHERE network = 'test'",
                    self.checkpoint
                ),
                &[],
            )
            .await
            .expect("read progress")
            .get(0)
    }

    async fn count(&self) -> i64 {
        self.postgres
            .query_one(&format!("SELECT COUNT(*) FROM {}", self.table), &[])
            .await
            .expect("read rows")
            .get(0)
    }
}

#[tokio::test]
async fn fresh_single_operation_is_applied() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("fresh batch");
    assert_eq!(fixture.count().await, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn checkpoint_failure_rolls_back_every_staged_insert() {
    let fixture = Fixture::new().await;
    fixture.seed(Some(100)).await;
    fixture
        .postgres
        .batch_execute(&format!(
            "CREATE FUNCTION rindexer_internal.reject_cron_progress() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END $$;
             CREATE TRIGGER fail_progress BEFORE UPDATE ON rindexer_internal.{}
             FOR EACH ROW EXECUTE FUNCTION rindexer_internal.reject_cron_progress();",
            fixture.checkpoint
        ))
        .await
        .expect("install fault after effects");
    let failure = fixture.worker(100, 101).run().await;
    assert!(failure.is_err(), "checkpoint failure must fail the scheduler");
    assert_eq!(fixture.count().await, 0, "staged rows must roll back");
    assert_eq!(fixture.checkpoint().await, Some(100));
    fixture.finish().await;
}

#[tokio::test]
async fn completed_sample_is_not_replayed_on_resume() {
    let fixture = Fixture::new().await;
    fixture.seed(Some(101)).await;
    fixture.worker(101, 102).run().await.expect("resume beyond block 101");
    assert_eq!(fixture.count().await, 1, "only block 102 is new");
    assert_eq!(fixture.checkpoint().await, Some(102));
    fixture.finish().await;
}

#[tokio::test]
async fn completed_block_zero_is_not_a_fresh_schedule() {
    let fixture = Fixture::new().await;
    fixture.seed(Some(0)).await;
    fixture.worker(0, 0).run().await.expect("zero was already completed");
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.checkpoint().await, Some(0));
    fixture.finish().await;
}

#[tokio::test]
async fn missing_checkpoint_row_cannot_commit_table_effects() {
    let fixture = Fixture::new().await;
    fixture
        .postgres
        .batch_execute(&format!("DELETE FROM rindexer_internal.{}", fixture.checkpoint))
        .await
        .expect("remove checkpoint");
    let outcome = fixture.worker(101, 101).run().await;
    assert!(outcome.is_err(), "a missing checkpoint must be an error");
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn concurrent_writers_apply_each_sample_once() {
    let fixture = Fixture::new().await;
    let (left, right) =
        tokio::join!(fixture.worker(101, 101).run(), fixture.worker(101, 101).run());
    assert!(left.is_ok(), "first writer: {left:?}");
    assert!(right.is_ok(), "second writer: {right:?}");
    assert_eq!(fixture.count().await, 1, "only one application is committed");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn database_read_failure_is_not_interpreted_as_fresh() {
    let fixture = Fixture::new().await;
    fixture
        .postgres
        .batch_execute(&format!("DROP TABLE rindexer_internal.{}", fixture.checkpoint))
        .await
        .expect("drop checkpoint");
    let failure = fixture.worker(101, 101).run().await;
    assert!(failure.is_err(), "unavailable progress must abort");
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn legacy_checkpoint_cannot_certify_existing_progress() {
    let fixture = Fixture::new().await;
    fixture.seed(Some(100)).await;
    fixture
        .postgres
        .batch_execute(&format!(
            "UPDATE rindexer_internal.{} SET protocol_version = NULL",
            fixture.checkpoint
        ))
        .await
        .expect("mark legacy progress");
    let failure = fixture.worker(100, 101).run().await;
    assert!(failure.is_err(), "uncertified legacy progress must fail closed");
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.checkpoint().await, Some(100));
    fixture.finish().await;
}

#[tokio::test]
async fn fractional_checkpoint_is_not_an_unstarted_schedule() {
    let fixture = Fixture::new().await;
    fixture
        .postgres
        .batch_execute(&format!(
            "UPDATE rindexer_internal.{} SET last_synced_block = 100.5",
            fixture.checkpoint
        ))
        .await
        .expect("install malformed progress");
    let failure = fixture.worker(100, 101).run().await;
    assert!(failure.is_err(), "fractional progress must fail closed");
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn production_startup_refuses_to_reseed_deleted_checkpoint_after_committed_effects() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit first historical sample");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));

    fixture
        .postgres
        .batch_execute(&format!(
            "DELETE FROM rindexer_internal.{} WHERE network = 'test'",
            fixture.checkpoint
        ))
        .await
        .expect("delete checkpoint after acknowledged commit");
    let error = fixture
        .production_setup(101, 101)
        .await
        .expect_err("production schema sync must refuse a missing checkpoint row");
    assert!(
        error.contains("missing checkpoint row") && error.contains(&fixture.checkpoint),
        "wrong startup failure: {error}"
    );
    assert_eq!(fixture.count().await, 1, "previously committed effects remain intact");
    assert!(
        fixture.worker(101, 101).run().await.is_err(),
        "restart must not replay effects without durable progress"
    );
    assert_eq!(fixture.count().await, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn production_startup_refuses_to_recreate_lost_checkpoint_table_with_surviving_rows() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit first historical sample");
    fixture
        .postgres
        .batch_execute(&format!("DROP TABLE rindexer_internal.{}", fixture.checkpoint))
        .await
        .expect("simulate lost checkpoint table");
    let error = fixture
        .production_setup(101, 101)
        .await
        .expect_err("surviving rows must prevent checkpoint-table initialization");
    assert!(
        error.contains("destination") && error.contains("durable checkpoint"),
        "wrong startup failure: {error}"
    );
    assert_eq!(fixture.count().await, 1, "committed effects remain");
    assert!(fixture.worker(101, 101).run().await.is_err(), "restart cannot replay the batch");
    assert_eq!(fixture.count().await, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn new_empty_destination_can_initialize_its_checkpoint_once() {
    let fixture = Fixture::new().await;
    fixture
        .postgres
        .batch_execute(&format!("DROP TABLE rindexer_internal.{}", fixture.checkpoint))
        .await
        .expect("start before first initialization");
    fixture.production_setup(101, 101).await.expect("initial setup is permitted");
    assert_eq!(fixture.checkpoint().await, None);
    fixture.production_setup(101, 101).await.expect("repeat setup remains idempotent");
    fixture.worker(101, 101).run().await.expect("apply first sample");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.production_setup(101, 101).await.expect("resume setup after commit");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn existing_checkpoint_table_is_not_reseeded_even_if_destination_is_empty() {
    let fixture = Fixture::new().await;
    fixture
        .postgres
        .batch_execute(&format!(
            "DELETE FROM rindexer_internal.{} WHERE network = 'test'",
            fixture.checkpoint
        ))
        .await
        .expect("delete unstarted progress");
    let error = fixture
        .production_setup(101, 101)
        .await
        .expect_err("existing checkpoint storage cannot acquire a fresh network row");
    assert!(
        error.contains("missing checkpoint row") && error.contains(&fixture.checkpoint),
        "wrong startup failure: {error}"
    );
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn production_startup_refuses_missing_destination_with_surviving_checkpoint() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit first sample");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture
        .postgres
        .batch_execute(&format!("DROP TABLE {}", fixture.table))
        .await
        .expect("simulate lost destination");
    let error = fixture
        .production_setup(101, 101)
        .await
        .expect_err("startup must not recreate a lost destination with completed progress");
    assert!(
        error.contains("checkpoint") && error.contains("destination"),
        "wrong startup failure: {error}"
    );
    let missing: Option<String> = fixture
        .postgres
        .query_one("SELECT to_regclass($1)::TEXT", &[&fixture.table])
        .await
        .expect("inspect destination catalog")
        .get(0);
    assert!(missing.is_none(), "startup must not recreate the lost table");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn repeated_network_details_must_not_omit_an_address() {
    let fixture = Fixture::new().await;
    let manifest = Fixture::duplicate_network_details();
    let error = fixture
        .scheduler(&manifest, 101)
        .err()
        .expect("two static details for the same network must be rejected");
    assert!(
        error.contains("network") && error.contains("details"),
        "unexpected admission failure: {error}"
    );
    let prepared = fixture
        .production_admission_error(&manifest)
        .await
        .expect_err("schema setup must reject unsupported duplicate details before DDL");
    assert!(prepared.contains("network") && prepared.contains("details"), "{prepared}");
    fixture.finish().await;
}

#[tokio::test]
async fn shared_physical_destination_must_not_have_separate_cron_checkpoints() {
    let fixture = Fixture::new().await;
    let manifest = Fixture::shared_destination_crons();
    let error = fixture
        .scheduler(&manifest, 101)
        .err()
        .expect("two cron owners must not share one destination");
    assert!(
        error.contains("shared_cron.samples") && error.contains("cron"),
        "unexpected admission failure: {error}"
    );
    let prepared = fixture
        .production_admission_error(&manifest)
        .await
        .expect_err("schema setup must reject the shared destination before DDL");
    assert!(prepared.contains("shared_cron.samples"), "{prepared}");
    fixture.finish().await;
}

#[tokio::test]
async fn cross_chain_historical_cron_requires_a_separate_design() {
    let fixture = Fixture::new().await;
    let manifest = Fixture::cross_chain_cron();
    let error = fixture
        .scheduler(&manifest, 101)
        .err()
        .expect("cross-chain destination must not share independent progress");
    assert!(error.contains("cross_chain"), "unexpected admission failure: {error}");
    let prepared = fixture
        .production_admission_error(&manifest)
        .await
        .expect_err("schema setup must reject cross-chain historical cron");
    assert!(prepared.contains("cross_chain"), "{prepared}");
    fixture.finish().await;
}

#[tokio::test]
async fn additive_upsert_rollback_and_restart_preserve_the_single_delta() {
    let fixture = Fixture::new().await;
    fixture.use_additive_table().await;
    fixture
        .postgres
        .batch_execute(&format!(
            "CREATE FUNCTION rindexer_internal.fail_arithmetic_cron_checkpoint() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected arithmetic checkpoint failure'; END $$;
             CREATE TRIGGER fail_arithmetic_checkpoint BEFORE UPDATE ON rindexer_internal.{}
             FOR EACH ROW EXECUTE FUNCTION rindexer_internal.fail_arithmetic_cron_checkpoint();",
            fixture.checkpoint
        ))
        .await
        .expect("install failure after arithmetic effects are staged");
    let manifest = Fixture::additive_manifest();
    let error = fixture
        .scheduler(&manifest, 101)
        .expect("single-operation additive cron is supported")
        .run()
        .await
        .expect_err("checkpoint failure must abort the entire arithmetic batch")
        .to_string();
    assert!(
        error.contains("injected arithmetic checkpoint failure"),
        "wrong failure boundary: {error}"
    );
    assert_eq!(fixture.amount().await, None, "additive effect must roll back");
    assert_eq!(fixture.checkpoint().await, None, "checkpoint must not advance");
    fixture
        .postgres
        .batch_execute(&format!(
            "DROP TRIGGER fail_arithmetic_checkpoint ON rindexer_internal.{}",
            fixture.checkpoint
        ))
        .await
        .expect("remove checkpoint fault");
    fixture.scheduler(&manifest, 101).unwrap().run().await.expect("retry");
    assert_eq!(fixture.amount().await.as_deref(), Some("7"));
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.scheduler(&manifest, 101).unwrap().run().await.expect("replay");
    assert_eq!(fixture.amount().await.as_deref(), Some("7"), "replay must not add twice");
    fixture.finish().await;
}

#[tokio::test]
async fn multiple_operations_follow_declared_order_for_each_sample() {
    let fixture = Fixture::new().await;
    fixture.use_additive_table().await;
    let mut manifest = serde_json::to_value(Fixture::additive_manifest()).unwrap();
    let cron = &mut manifest["contracts"][0]["tables"][0]["cron"][0];
    cron["end_block"] = json!("102");
    cron["operations"] = json!([
        {"type":"upsert", "set":[{"column":"amount", "action":"set", "value":"3"}]},
        {"type":"upsert", "set":[{"column":"amount", "action":"add", "value":"7"}]}
    ]);
    let manifest: Manifest = serde_json::from_value(manifest).unwrap();
    fixture
        .scheduler(&manifest, 102)
        .expect("multiple operations must be admitted")
        .run()
        .await
        .unwrap();
    assert_eq!(fixture.amount().await.as_deref(), Some("10"));
    assert_eq!(fixture.checkpoint().await, Some(102));
    fixture.scheduler(&manifest, 102).unwrap().run().await.unwrap();
    assert_eq!(fixture.amount().await.as_deref(), Some("10"));
    fixture.finish().await;
}

#[tokio::test]
async fn failure_in_later_cron_operation_rolls_back_the_whole_sample() {
    let fixture = Fixture::new().await;
    fixture.use_additive_table().await;
    let mut manifest = serde_json::to_value(Fixture::additive_manifest()).unwrap();
    manifest["contracts"][0]["tables"][0]["cron"][0]["operations"] = json!([
        {"type":"upsert", "set":[{"column":"amount", "action":"set", "value":"3"}]},
        {"type":"upsert", "set":[{"column":"amount", "action":"add", "value":"7"}]}
    ]);
    let manifest: Manifest = serde_json::from_value(manifest).unwrap();
    fixture
        .postgres
        .batch_execute(&format!(
            "CREATE FUNCTION rindexer_internal.reject_second_cron_operation() RETURNS trigger
             LANGUAGE plpgsql AS $cron_fault$ BEGIN RAISE EXCEPTION 'injected second operation failure'; END $cron_fault$;
             CREATE TRIGGER reject_second_cron_operation BEFORE UPDATE ON {}
             FOR EACH ROW EXECUTE FUNCTION rindexer_internal.reject_second_cron_operation();",
            fixture.table
        ))
        .await
        .expect("install fault on the second operation");
    let error = fixture
        .scheduler(&manifest, 101)
        .unwrap()
        .run()
        .await
        .expect_err("failure in a later operation must abort the entire sample")
        .to_string();
    assert!(
        error.contains("injected second operation failure") && error.contains("operation 1"),
        "wrong failure boundary: {error}"
    );
    assert_eq!(fixture.amount().await, None, "the first operation must roll back");
    assert_eq!(fixture.checkpoint().await, None, "the sample cannot be certified");
    fixture
        .postgres
        .batch_execute(&format!("DROP TRIGGER reject_second_cron_operation ON {}", fixture.table))
        .await
        .expect("remove second-operation fault");
    fixture.scheduler(&manifest, 101).unwrap().run().await.expect("retry entire sample");
    assert_eq!(fixture.amount().await.as_deref(), Some("10"));
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.scheduler(&manifest, 101).unwrap().run().await.expect("replay");
    assert_eq!(fixture.amount().await.as_deref(), Some("10"), "replay cannot double-apply");
    fixture.finish().await;
}

#[tokio::test]
async fn lost_commit_reply_is_reconciled_without_replaying_the_sample() {
    let fixture = Fixture::new().await;
    let proxy = LostCommitResponse::start(&fixture.url).await;
    let mut proxy_url = proxy.url.clone();
    proxy_url.set_query(Some("sslmode=disable"));
    let injected = Fixture::connect(proxy_url.as_str()).await;
    let manifest = Fixture::manifest(101, 101);
    let scheduler = fixture
        .scheduler_with_postgres(&manifest, 101, injected.clone())
        .expect("single-operation schedule admitted");
    let first = tokio::time::timeout(Duration::from_secs(20), scheduler.run())
        .await
        .expect("lost COMMIT response recovery must terminate");
    assert!(
        proxy.dropped.load(Ordering::SeqCst),
        "wire proxy must intercept a real PostgreSQL COMMIT acknowledgement"
    );
    first.expect("checkpoint reconciliation must recognize the committed batch");
    assert_eq!(fixture.checkpoint().await, Some(101));
    assert_eq!(fixture.count().await, 1);
    fixture.worker(101, 101).run().await.expect("repeat from durable checkpoint");
    assert_eq!(fixture.count().await, 1, "replay must not duplicate committed effects");
    drop(injected);
    proxy.stop().await;
    fixture.finish().await;
}
