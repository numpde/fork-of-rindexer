//! Real PostgreSQL regressions for historical cron effects and checkpoint durability.
//! Run with cargo nextest: tests use RINDEXER_TEST_POSTGRES_URL's disposable
//! admin database when provided, or a native Testcontainers PostgreSQL.
//! Each test creates a fresh database and exercises the actual scheduler.
use super::*;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::client::TEST_DATABASE_URL_LOCK;
use crate::database::postgres::generate::generate_internal_cron_table_name;
use crate::indexer::postgres_test_wire::LostCommitResponse;
use crate::provider::mock::MockChainProvider;
use alloy::network::{AnyHeader, AnyRpcBlock, AnyRpcHeader};
use alloy::rpc::types::{Block, BlockTransactions};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

fn cron_test_header(number: u64) -> AnyRpcBlock {
    AnyRpcBlock::new(
        Block::new(
            AnyRpcHeader::from_sealed(
                AnyHeader { number, ..Default::default() }.seal(B256::repeat_byte(101)),
            ),
            BlockTransactions::Full(vec![]),
        )
        .into(),
    )
}

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
        let identity = Self::identity_for(&Self::manifest(101, 101));
        let fingerprint_sql = identity.fingerprint.replace('\'', "''");
        let descriptor_sql = identity.descriptor.replace('\'', "''");
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
                     protocol_version SMALLINT,
                     schedule_fingerprint TEXT,
                     schedule_descriptor TEXT
                 );
                 INSERT INTO rindexer_internal.{checkpoint}
                     (network, last_synced_block, protocol_version,
                      schedule_fingerprint, schedule_descriptor)
                 VALUES ('test', NULL, 2, '{fingerprint_sql}', '{descriptor_sql}');"
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

    fn identity_for(manifest: &Manifest) -> crate::indexer::cron_identity::CronIdentity {
        let indexer = manifest.to_indexer();
        let contract = &indexer.contracts[0];
        let table = crate::indexer::tables::TableRuntime::try_new(
            &contract.tables.as_ref().unwrap()[0],
            &indexer.name,
            &contract.name,
        )
        .expect("resolve test table");
        let cron = &table.table.cron_mappings()[0];
        let address = contract.details[0]
            .address
            .as_ref()
            .and_then(|source| match source {
                alloy::rpc::types::ValueOrArray::Value(value) => Some(*value),
                _ => None,
            })
            .unwrap();
        crate::indexer::cron_identity::identify(crate::indexer::cron_identity::CronIdentityInput {
            indexer: &indexer.name,
            contract: &contract.name,
            table: &table.table,
            destination: &table.full_table_name,
            cron_index: 0,
            cron,
            network: "test",
            address,
            chain_id: Some(1),
        })
        .expect("test schedule descriptor")
    }

    async fn bind(&self, manifest: &Manifest) {
        let identity = Self::identity_for(manifest);
        self.postgres.execute(
            &format!(
                "UPDATE rindexer_internal.{} SET protocol_version = 2,                  schedule_fingerprint = $1, schedule_descriptor = $2 WHERE network = 'test'",
                self.checkpoint
            ),
            &[&identity.fingerprint, &identity.descriptor],
        ).await.expect("bind explicit test schedule");
    }

    fn one_block_scheduler(&self, manifest: &Manifest, end: u64) -> CronScheduler {
        let mut scheduler = self.scheduler(manifest, end).expect("one-block reference");
        scheduler.tasks[0].batch_size_for_test = Some(1);
        scheduler
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
    async fn apply_generated_ddl(&self, start: u64, end: u64) -> Result<(), String> {
        let indexer = Self::manifest(start, end).to_indexer();
        let chains = std::collections::HashMap::from([("test".to_owned(), 1)]);
        let schema =
            crate::indexer::schema::PreparedSchema::try_new(self._directory.path(), &indexer)
                .map_err(|error| error.to_string())?;
        let ddl = crate::database::generate::generate_prepared_tables_sql_with_network_ids(
            &schema, false, &chains,
        )
        .map_err(|error| error.to_string())?;
        self.postgres.batch_execute(ddl.as_str()).await.map_err(|error| error.to_string())
    }

    async fn install_statement_counter(&self) {
        self.postgres
            .batch_execute(&format!(
                "CREATE TABLE rindexer_internal.cron_write_count (id BIGSERIAL PRIMARY KEY);
                 CREATE FUNCTION rindexer_internal.count_cron_writes() RETURNS trigger
                 LANGUAGE plpgsql AS $cron_count$ BEGIN
                     INSERT INTO rindexer_internal.cron_write_count DEFAULT VALUES;
                     RETURN NULL;
                 END $cron_count$;
                 CREATE TRIGGER count_cron_writes AFTER INSERT ON {}
                 FOR EACH STATEMENT EXECUTE FUNCTION rindexer_internal.count_cron_writes();",
                self.table
            ))
            .await
            .expect("install statement-level write counter");
    }

    async fn write_statements(&self) -> i64 {
        self.postgres
            .query_one("SELECT COUNT(*) FROM rindexer_internal.cron_write_count", &[])
            .await
            .expect("count destination statements")
            .get(0)
    }

    async fn stored_rows(&self) -> Vec<(String, String, String, String)> {
        self.postgres
            .query(
                &format!(
                    "SELECT network::TEXT, amount::TEXT, rindexer_block_number::TEXT,
                     rindexer_sequence_id::TEXT FROM {} ORDER BY network, rindexer_block_number,
                     rindexer_sequence_id, amount",
                    self.table
                ),
                &[],
            )
            .await
            .expect("read complete destination state")
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect()
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
    fixture.bind(&Fixture::manifest(100, 101)).await;
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
    fixture.bind(&Fixture::manifest(101, 102)).await;
    fixture.seed(Some(101)).await;
    fixture.worker(101, 102).run().await.expect("resume beyond block 101");
    assert_eq!(fixture.count().await, 1, "only block 102 is new");
    assert_eq!(fixture.checkpoint().await, Some(102));
    fixture.finish().await;
}

#[tokio::test]
async fn completed_block_zero_is_not_a_fresh_schedule() {
    let fixture = Fixture::new().await;
    fixture.bind(&Fixture::manifest(0, 0)).await;
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
    fixture.bind(&Fixture::manifest(100, 101)).await;
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
    fixture.bind(&Fixture::manifest(100, 101)).await;
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
        .apply_generated_ddl(101, 101)
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
        .apply_generated_ddl(101, 101)
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
    fixture.apply_generated_ddl(101, 101).await.expect("initial setup is permitted");
    assert_eq!(fixture.checkpoint().await, None);
    fixture.apply_generated_ddl(101, 101).await.expect("repeat setup remains idempotent");
    fixture.worker(101, 101).run().await.expect("apply first sample");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.apply_generated_ddl(101, 101).await.expect("resume setup after commit");
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
        .apply_generated_ddl(101, 101)
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
        .apply_generated_ddl(101, 101)
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
    let manifest = Fixture::additive_manifest();
    // Certificate setup must precede the intentionally failing UPDATE trigger.
    fixture.bind(&manifest).await;
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
    fixture.bind(&manifest).await;
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
    fixture.bind(&manifest).await;
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

#[tokio::test]
async fn insert_backfill_matches_one_block_at_a_time_with_fewer_statements() {
    let bulk = Fixture::new().await;
    let reference = Fixture::new().await;
    let manifest = Fixture::manifest(101, 108);
    bulk.bind(&manifest).await;
    reference.bind(&manifest).await;
    bulk.install_statement_counter().await;
    reference.install_statement_counter().await;

    bulk.scheduler(&manifest, 108).unwrap().run().await.expect("bounded insertion");
    reference.one_block_scheduler(&manifest, 108).run().await.expect("one-block reference");

    assert_eq!(bulk.stored_rows().await, reference.stored_rows().await);
    assert_eq!(bulk.checkpoint().await, reference.checkpoint().await);
    assert_eq!(bulk.checkpoint().await, Some(108));
    assert_eq!(bulk.count().await, 8);
    assert_eq!(bulk.write_statements().await, 1, "multi-block COPY must use one statement");
    assert_eq!(reference.write_statements().await, 8);
    bulk.finish().await;
    reference.finish().await;
}

#[tokio::test]
async fn repeated_key_arithmetic_upsert_matches_one_block_reference() {
    let bulk = Fixture::new().await;
    let reference = Fixture::new().await;
    bulk.use_additive_table().await;
    reference.use_additive_table().await;
    bulk.install_statement_counter().await;
    reference.install_statement_counter().await;
    let mut manifest = serde_json::to_value(Fixture::additive_manifest()).unwrap();
    manifest["contracts"][0]["tables"][0]["columns"][0]["default"] = json!("5");
    manifest["contracts"][0]["tables"][0]["cron"][0]["end_block"] = json!("108");
    let bulk_manifest: Manifest = serde_json::from_value(manifest.clone()).unwrap();
    bulk.bind(&bulk_manifest).await;
    reference.bind(&bulk_manifest).await;

    bulk.scheduler(&bulk_manifest, 108).unwrap().run().await.expect("bulk arithmetic");
    reference.one_block_scheduler(&bulk_manifest, 108).run().await.expect("one-block arithmetic");

    assert_eq!(bulk.stored_rows().await, reference.stored_rows().await);
    assert_eq!(bulk.checkpoint().await, Some(108));
    assert_eq!(reference.checkpoint().await, Some(108));
    assert_eq!(bulk.amount().await.as_deref(), Some("61"), "default + eight deltas");
    assert_eq!(bulk.write_statements().await, 1, "one keyed SQL upsert for eight deltas");
    assert_eq!(reference.write_statements().await, 8);
    bulk.finish().await;
    reference.finish().await;
}

#[tokio::test]
async fn latest_set_upsert_matches_one_block_reference() {
    let bulk = Fixture::new().await;
    let reference = Fixture::new().await;
    bulk.use_additive_table().await;
    reference.use_additive_table().await;
    bulk.install_statement_counter().await;
    let mut value = serde_json::to_value(Fixture::additive_manifest()).unwrap();
    let cron = &mut value["contracts"][0]["tables"][0]["cron"][0];
    cron["end_block"] = json!("108");
    cron["operations"][0]["set"][0]["action"] = json!("set");
    cron["operations"][0]["set"][0]["value"] = json!("3");
    let manifest: Manifest = serde_json::from_value(value.clone()).unwrap();
    bulk.bind(&manifest).await;
    reference.bind(&manifest).await;

    bulk.scheduler(&manifest, 108).unwrap().run().await.expect("bulk last-by-sequence");
    reference.one_block_scheduler(&manifest, 108).run().await.expect("one-block set");

    assert_eq!(bulk.stored_rows().await, reference.stored_rows().await);
    assert_eq!(bulk.amount().await.as_deref(), Some("3"));
    assert_eq!(bulk.checkpoint().await, Some(108));
    assert_eq!(bulk.write_statements().await, 1, "latest-value upsert is one SQL statement");
    bulk.finish().await;
    reference.finish().await;
}

#[tokio::test]
async fn batched_insert_checkpoint_failure_rolls_back_all_blocks() {
    let fixture = Fixture::new().await;
    fixture.bind(&Fixture::manifest(101, 108)).await;
    fixture.install_statement_counter().await;
    fixture
        .postgres
        .batch_execute(&format!(
            "CREATE FUNCTION rindexer_internal.reject_bulk_cron_checkpoint() RETURNS trigger
             LANGUAGE plpgsql AS $reject_bulk$ BEGIN RAISE EXCEPTION 'reject bulk checkpoint'; END $reject_bulk$;
             CREATE TRIGGER reject_bulk_checkpoint BEFORE UPDATE ON rindexer_internal.{}
             FOR EACH ROW EXECUTE FUNCTION rindexer_internal.reject_bulk_cron_checkpoint();",
            fixture.checkpoint
        ))
        .await
        .unwrap();
    assert!(fixture.worker(101, 108).run().await.is_err());
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.write_statements().await, 0, "statement audit also rolls back");
    assert_eq!(fixture.checkpoint().await, None);
    fixture.finish().await;
}

#[tokio::test]
async fn changed_operation_must_not_reuse_committed_cron_progress() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit original schedule");
    let mut changed = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    changed["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["set"][0]["value"] =
        json!("9");
    let manifest: Manifest = serde_json::from_value(changed).unwrap();
    let error = fixture
        .scheduler(&manifest, 101)
        .unwrap()
        .run()
        .await
        .expect_err("changed operation must not inherit the old checkpoint")
        .to_string();
    assert!(error.contains("schedule") || error.contains("fingerprint"), "{error}");
    assert_eq!(fixture.count().await, 1, "old effects remain untouched");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn changed_source_and_sample_grid_must_not_replay_under_old_progress() {
    for change in ["address", "interval", "end_block"] {
        let fixture = Fixture::new().await;
        fixture.worker(101, 101).run().await.expect("commit old schedule");
        let mut changed = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
        match change {
            "address" => {
                changed["contracts"][0]["details"][0]["address"] =
                    json!("0x0000000000000000000000000000000000000002");
            }
            "interval" => {
                changed["contracts"][0]["tables"][0]["cron"][0]["block_interval"] = json!("2");
            }
            "end_block" => {
                changed["contracts"][0]["tables"][0]["cron"][0]["end_block"] = json!("102");
            }
            _ => unreachable!(),
        }
        let manifest: Manifest = serde_json::from_value(changed).unwrap();
        let error = fixture
            .scheduler(&manifest, 102)
            .unwrap()
            .run()
            .await
            .expect_err("changed source or sampling schedule cannot reuse a checkpoint")
            .to_string();
        assert!(error.contains("schedule") || error.contains("fingerprint"), "{change}: {error}");
        assert_eq!(fixture.count().await, 1, "{change}: no replay");
        assert_eq!(fixture.checkpoint().await, Some(101), "{change}: no progress change");
        fixture.finish().await;
    }
}

#[tokio::test]
async fn changed_operation_order_and_column_defaults_are_incompatible() {
    for change in ["operation_order", "column_default"] {
        let fixture = Fixture::new().await;
        fixture.use_additive_table().await;
        let mut original = serde_json::to_value(Fixture::additive_manifest()).unwrap();
        original["contracts"][0]["tables"][0]["cron"][0]["operations"] = json!([
            {"type":"upsert","set":[{"column":"amount","action":"set","value":"3"}]},
            {"type":"upsert","set":[{"column":"amount","action":"add","value":"7"}]}
        ]);
        let manifest: Manifest = serde_json::from_value(original.clone()).unwrap();
        fixture.bind(&manifest).await;
        fixture.scheduler(&manifest, 101).unwrap().run().await.unwrap();
        assert_eq!(fixture.amount().await.as_deref(), Some("10"));
        match change {
            "operation_order" => {
                original["contracts"][0]["tables"][0]["cron"][0]["operations"] = json!([
                    {"type":"upsert","set":[{"column":"amount","action":"add","value":"7"}]},
                    {"type":"upsert","set":[{"column":"amount","action":"set","value":"3"}]}
                ]);
            }
            "column_default" => {
                original["contracts"][0]["tables"][0]["columns"][0]["default"] = json!("5");
            }
            _ => unreachable!(),
        }
        let modified: Manifest = serde_json::from_value(original).unwrap();
        let error = fixture
            .scheduler(&modified, 101)
            .unwrap()
            .run()
            .await
            .expect_err("changed operations/schema cannot reuse certified progress")
            .to_string();
        assert!(error.contains("schedule") || error.contains("fingerprint"), "{change}: {error}");
        assert_eq!(fixture.amount().await.as_deref(), Some("10"), "{change}: old effects");
        assert_eq!(fixture.checkpoint().await, Some(101));
        fixture.finish().await;
    }
}

#[tokio::test]
async fn production_setup_rejects_incompatible_schedule_before_drop_each_run() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit old schedule");
    let mut changed = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    changed["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["set"][0]["value"] =
        json!("9");
    changed["storage"] = json!({"postgres": {"enabled": true, "drop_each_run": true}});
    let manifest: Manifest = serde_json::from_value(changed).unwrap();
    let error = fixture
        .production_admission_error(&manifest)
        .await
        .expect_err("schedule drift must stop before any destructive setup");
    assert!(error.contains("schedule") || error.contains("fingerprint"), "{error}");
    assert_eq!(fixture.count().await, 1, "setup must not destroy old data");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn unsigned_v1_checkpoint_cannot_be_implicitly_adopted() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit old checkpoint");
    fixture
        .postgres
        .batch_execute(&format!(
            "UPDATE rindexer_internal.{} SET protocol_version = 1",
            fixture.checkpoint
        ))
        .await
        .unwrap();
    let error = fixture
        .apply_generated_ddl(101, 101)
        .await
        .expect_err("unsigned version-1 progress must not be auto-certified");
    assert!(
        error.contains("legacy") || error.contains("schedule") || error.contains("version"),
        "{error}"
    );
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn nonsemantic_rpc_and_runtime_settings_preserve_schedule_identity() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit original schedule");
    let mut value = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    value["networks"][0]["rpc"] = json!("http://127.0.0.1:9999");
    value["config"]["max_concurrent_view_calls"] = json!(3);
    let manifest: Manifest = serde_json::from_value(value).unwrap();
    fixture.scheduler(&manifest, 101).unwrap().run().await.expect("same effective schedule");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn shutdown_interrupts_a_stalled_historical_rpc_before_any_write() {
    let fixture = Fixture::new().await;
    let mut value = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    value["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["set"][0]["value"] =
        json!(r#"$call($contract, "value() returns (uint256)")"#);
    let manifest: Manifest = serde_json::from_value(value).unwrap();
    fixture.bind(&manifest).await;
    let provider: Arc<dyn ChainProvider> = Arc::new(
        MockChainProvider::new(1)
            .with_block_number(101)
            .with_blocks(vec![cron_test_header(101)])
            .with_eth_call_waiter(Arc::new(tokio::sync::Notify::new())),
    );
    let providers = Arc::new(HashMap::from([("test".to_string(), provider)]));
    let scheduler =
        CronScheduler::try_new(&manifest, Some(fixture.postgres.clone()), None, providers)
            .expect("admitted schedule");

    let running = tokio::spawn(async move { scheduler.run().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    crate::initiate_shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("shutdown must interrupt a stalled RPC preparation")
        .expect("scheduler task joins")
        .expect("deliberate shutdown is not a terminal cron error");
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.checkpoint().await, None);
    fixture.finish().await;
}

#[tokio::test]
async fn historical_cron_aborts_a_permanently_blocked_checkpoint_lock() {
    let fixture = Fixture::new().await;
    let mut blocker = fixture.postgres.raw_connection().await.expect("blocker connection");
    let lock = blocker.transaction().await.expect("blocker transaction");
    lock.query_one(
        &format!(
            "SELECT network FROM rindexer_internal.{} WHERE network = 'test' FOR UPDATE",
            fixture.checkpoint
        ),
        &[],
    )
    .await
    .expect("hold the real checkpoint lock");

    let blocked = fixture.worker(101, 101);
    let result = tokio::time::timeout(Duration::from_secs(9), blocked.run())
        .await
        .expect("a blocked checkpoint must not hang indefinitely")
        .expect_err("permanently held checkpoint must exhaust its finite retry budget")
        .to_string();
    assert!(result.contains("lock") && result.contains("timeout"), "{result}");
    assert_eq!(fixture.count().await, 0, "no effects while checkpoint is locked");
    assert_eq!(fixture.checkpoint().await, None);
    drop(lock);
    drop(blocker);
    fixture.worker(101, 101).run().await.expect("restart after lock release");
    assert_eq!(fixture.count().await, 1);
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn historical_cron_recovers_when_a_checkpoint_lock_is_released() {
    let fixture = Fixture::new().await;
    let mut blocker = fixture.postgres.raw_connection().await.expect("blocker connection");
    let lock = blocker.transaction().await.expect("blocker transaction");
    lock.query_one(
        &format!(
            "SELECT network FROM rindexer_internal.{} WHERE network = 'test' FOR UPDATE",
            fixture.checkpoint
        ),
        &[],
    )
    .await
    .expect("hold the checkpoint lock");

    let worker = fixture.worker(101, 101);
    let (result, _) = tokio::join!(
        async { tokio::time::timeout(Duration::from_secs(9), worker.run()).await },
        async {
            tokio::time::sleep(Duration::from_millis(2600)).await;
            drop(lock);
        },
    );
    drop(blocker);
    result.expect("bounded retry must settle").expect("lock release permits retry");
    assert_eq!(fixture.count().await, 1, "effect is committed once");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}

#[tokio::test]
async fn historical_cron_bounds_a_stalled_required_rpc_call_before_writing() {
    let fixture = Fixture::new().await;
    let mut value = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    value["contracts"][0]["tables"][0]["cron"][0]["operations"][0]["set"][0]["value"] =
        json!(r#"$call($contract, "value() returns (uint256)")"#);
    let manifest: Manifest = serde_json::from_value(value).unwrap();
    fixture.bind(&manifest).await;
    let provider: Arc<dyn ChainProvider> = Arc::new(
        MockChainProvider::new(1)
            .with_block_number(101)
            .with_blocks(vec![cron_test_header(101)])
            .with_eth_call_waiter(Arc::new(tokio::sync::Notify::new())),
    );
    let providers = Arc::new(HashMap::from([("test".to_string(), provider)]));
    let mut scheduler =
        CronScheduler::try_new(&manifest, Some(fixture.postgres.clone()), None, providers)
            .expect("admitted schedule");
    scheduler.tasks[0].prepare_timeout_for_test = Some(Duration::from_millis(100));
    let result = tokio::time::timeout(Duration::from_secs(2), scheduler.run())
        .await
        .expect("a stalled RPC must terminate within its test deadline")
        .expect_err("a required RPC timeout cannot certify a sampled block")
        .to_string();
    assert!(result.contains("RPC") && result.contains("timeout"), "{result}");
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.checkpoint().await, None);
    fixture.finish().await;
}

#[tokio::test]
async fn corrupted_certificates_refuse_startup_and_replay_without_mutating_rows() {
    for (case, change) in [
        ("unsupported protocol", "SET protocol_version = 3"),
        ("different fingerprint", "SET schedule_fingerprint = '0xdeadbeef'"),
        ("different descriptor", "SET schedule_descriptor = '{}'"),
        ("missing descriptor", "SET schedule_descriptor = NULL"),
    ] {
        let fixture = Fixture::new().await;
        fixture.worker(101, 101).run().await.expect("commit the certified sample");
        fixture
            .postgres
            .batch_execute(&format!(
                "UPDATE rindexer_internal.{} {change} WHERE network = 'test'",
                fixture.checkpoint
            ))
            .await
            .expect("corrupt the stored certificate");
        let mut input = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
        input["storage"] = json!({"postgres": {"enabled": true, "drop_each_run": true}});
        let manifest: Manifest = serde_json::from_value(input).unwrap();

        let startup = fixture.production_admission_error(&manifest).await;
        assert!(startup.is_err(), "{case}: startup must fail before drop_each_run");
        let replay = fixture.worker(101, 101).run().await;
        assert!(replay.is_err(), "{case}: runtime must reject the same certificate");
        assert_eq!(fixture.count().await, 1, "{case}: preserve committed effects");
        assert_eq!(fixture.checkpoint().await, Some(101), "{case}: preserve progress");
        fixture.finish().await;
    }
}

#[tokio::test]
async fn missing_certificate_column_refuses_destructive_startup_without_replaying() {
    let fixture = Fixture::new().await;
    fixture.worker(101, 101).run().await.expect("commit the certified sample");
    fixture
        .postgres
        .batch_execute(&format!(
            "ALTER TABLE rindexer_internal.{} DROP COLUMN schedule_descriptor",
            fixture.checkpoint
        ))
        .await
        .expect("simulate an incomplete legacy checkpoint schema");
    let mut input = serde_json::to_value(Fixture::manifest(101, 101)).unwrap();
    input["storage"] = json!({"postgres": {"enabled": true, "drop_each_run": true}});
    let manifest: Manifest = serde_json::from_value(input).unwrap();

    let startup = fixture.production_admission_error(&manifest).await;
    assert!(startup.is_err(), "missing certificate column must block startup");
    assert!(fixture.worker(101, 101).run().await.is_err(), "runtime must not replay");
    assert_eq!(fixture.count().await, 1, "existing effects must survive startup rejection");
    assert_eq!(fixture.checkpoint().await, Some(101));
    fixture.finish().await;
}
