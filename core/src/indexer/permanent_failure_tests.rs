//! Terminal failures must stop one stream, preserve retryable errors, and reach health reporting.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::time::timeout;
use tower::ServiceExt;

use super::{
    input_validation_tests::{event, params},
    no_code_callback, NoCodeCallbackParams,
};
use crate::{
    event::{
        callback_registry::{
            CallbackFailures, EventCallbackRegistry, EventCallbackRegistryInformation,
            EventCallbackType, FailedStream,
        },
        contract_setup::ContractInformation,
    },
    health::HealthServer,
};

struct PostgresCase {
    params: NoCodeCallbackParams,
    client: Arc<crate::PostgresClient>,
    schema: String,
    cursor: String,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

impl PostgresCase {
    async fn new(abi_type: &str, components: Option<Value>) -> Self {
        use crate::{
            abi::{ABIItem, EventInfo},
            database::{
                generate::generate_indexer_contract_schema_name,
                postgres::generate::{
                    generate_column_names_only_with_base_properties,
                    generate_columns_with_data_types, generate_internal_event_table_name,
                },
            },
        };
        let (client, container) = super::input_validation_tests::postgres_client().await;
        let mut params = params();
        let mut input = json!({"name": "value", "type": abi_type, "indexed": false});
        if let Some(components) = components {
            input["components"] = components;
        }
        let definition =
            json!({"name": "Value", "type": "event", "anonymous": false, "inputs": [input]});
        params.event = serde_json::from_value(definition.clone()).unwrap();
        params.event_info =
            EventInfo::new(serde_json::from_value::<ABIItem>(definition).unwrap()).unwrap();
        params.sql_column_names =
            generate_column_names_only_with_base_properties(&params.event_info.inputs);
        params.indexer_name = format!("permanent_{:032x}", rand::random::<u128>());
        let schema =
            generate_indexer_contract_schema_name(&params.indexer_name, &params.contract_name);
        let cursor = generate_internal_event_table_name(&schema, &params.event_info.name);
        params.postgres = Some(client.clone());
        params.sql_event_table_name = format!("{schema}.value");
        let columns = generate_columns_with_data_types(&params.event_info.inputs).join(", ");
        client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE SCHEMA IF NOT EXISTS rindexer_internal;
            CREATE TABLE {schema}.value (contract_address CHAR(42), {columns}, tx_hash CHAR(66), block_number NUMERIC, block_timestamp TIMESTAMPTZ, block_hash CHAR(66), network VARCHAR(50), tx_index NUMERIC, log_index VARCHAR(78));
            CREATE TABLE rindexer_internal.{cursor} (network TEXT PRIMARY KEY, last_synced_block NUMERIC);
            INSERT INTO rindexer_internal.{cursor} VALUES ('ethereum', 1);")).await.expect("create owned event/cursor fixture");
        Self { params, client, schema, cursor, _container: container }
    }

    fn event(
        &self,
        value: alloy::dyn_abi::DynSolValue,
    ) -> crate::event::callback_registry::EventResult {
        let mut event = event(&self.params, 2);
        let encoded = alloy::dyn_abi::DynSolValue::Tuple(vec![value]).abi_encode_params();
        event.log.inner.data = alloy::primitives::LogData::new_unchecked(
            vec![self.params.event.selector()],
            encoded.into(),
        );
        assert!(
            crate::helpers::parse_log(&self.params.event, &event.log).is_some(),
            "fixture payload must be valid ABI input"
        );
        event
    }

    async fn position(&self) -> (i64, String) {
        let rows = self
            .client
            .query_one(&format!("SELECT count(*) FROM {}", self.params.sql_event_table_name), &[])
            .await
            .unwrap()
            .get(0);
        let cursor = self
            .client
            .query_one(
                &format!("SELECT last_synced_block::text FROM rindexer_internal.{}", self.cursor),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        (rows, cursor)
    }

    async fn finish(self) {
        self.client
            .batch_execute(&format!(
                "DROP SCHEMA {} CASCADE; DROP TABLE rindexer_internal.{}",
                self.schema, self.cursor
            ))
            .await
            .expect("remove owned fixture tables");
    }
}

async fn reject_postgres_nul(
    abi_type: &str,
    value: alloy::dyn_abi::DynSolValue,
    components: Option<Value>,
) {
    let fixture = PostgresCase::new(abi_type, components).await;
    let event = fixture.event(value);
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let registry = registry(
        &fixture.params,
        Arc::new(move |events| {
            observed.fetch_add(1, Ordering::SeqCst);
            callback(events)
        }),
    );
    let outcome =
        timeout(Duration::from_secs(5), registry.trigger_event(&ID.to_string(), vec![event])).await;
    let position = fixture.position().await;
    let failure_count = registry.failure_state().snapshot().len();
    drop(registry);
    fixture.finish().await;
    assert!(
        outcome.is_ok(),
        "unrepresentable PostgreSQL {abi_type} must fail once, not retry indefinitely; attempts={}",
        attempts.load(Ordering::SeqCst)
    );
    outcome.unwrap().expect_err("PostgreSQL text NUL must not be silently rewritten or skipped");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(position, (0, "1".to_string()), "terminal failure preserves rows and cursor");
    assert_eq!(failure_count, 1, "the registry must retain the blocked stream");
}

#[tokio::test]
async fn postgres_string_nul_is_terminal_without_advancing_the_cursor() {
    reject_postgres_nul(
        "string",
        alloy::dyn_abi::DynSolValue::String("not\0representable".to_string()),
        None,
    )
    .await;
}

#[tokio::test]
async fn postgres_string_array_nul_is_terminal_without_advancing_the_cursor() {
    reject_postgres_nul(
        "string[]",
        alloy::dyn_abi::DynSolValue::Array(vec![alloy::dyn_abi::DynSolValue::String(
            "not\0representable".to_string(),
        )]),
        None,
    )
    .await;
}

#[tokio::test]
async fn postgres_tuple_array_jsonb_nul_is_terminal_without_advancing_the_cursor() {
    reject_postgres_nul(
        "tuple[]",
        alloy::dyn_abi::DynSolValue::Array(vec![alloy::dyn_abi::DynSolValue::Tuple(vec![
            alloy::dyn_abi::DynSolValue::String("not\0representable".to_string()),
        ])]),
        Some(json!([{"name": "text", "type": "string"}])),
    )
    .await;
}

#[tokio::test]
async fn postgres_binary_nul_remains_storable() {
    let fixture = PostgresCase::new("bytes", None).await;
    let event = fixture.event(alloy::dyn_abi::DynSolValue::Bytes(vec![1, 0, 2]));
    let registry = registry(
        &fixture.params,
        no_code_callback(Arc::new(fixture.params.clone())).event_callback,
    );
    let result =
        timeout(Duration::from_secs(5), registry.trigger_event(&ID.to_string(), vec![event])).await;
    let position = fixture.position().await;
    let stored: Vec<u8> = fixture
        .client
        .query_one(&format!("SELECT value FROM {}", fixture.params.sql_event_table_name), &[])
        .await
        .expect("binary event stored")
        .get(0);
    drop(registry);
    fixture.finish().await;
    result.expect("binary write completes").expect("binary NUL is representable");
    assert_eq!(stored, vec![1, 0, 2]);
    assert_eq!(position, (1, "2".to_string()));
}

const ID: &str = "permanent-failure";

fn registry(params: &NoCodeCallbackParams, callback: EventCallbackType) -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    registry.register_event(EventCallbackRegistryInformation {
        id: ID.to_string(),
        indexer_name: params.indexer_name.clone(),
        topic_id: params.event.selector(),
        event_name: params.event_info.name.clone(),
        index_event_in_order: false,
        contract: ContractInformation {
            name: params.contract_name.clone(),
            details: vec![],
            abi: "unused.json".to_string().into(),
            reorg_safe_distance: None,
        },
        callback,
        tables: Arc::new(vec![]),
        reorg_sender: None,
        streams_clients: Arc::new(None),
        providers: Arc::new(Default::default()),
        constants: Arc::new(Default::default()),
        multicall_addresses: Arc::new(Default::default()),
    });
    registry
}

fn failure() -> FailedStream {
    FailedStream {
        id: ID.to_string(),
        indexer: "input_tests".to_string(),
        contract: "Contract".to_string(),
        event: "Value".to_string(),
        network: "ethereum".to_string(),
        from_block: 2,
        to_block: 2,
    }
}

async fn health(sources: Vec<Arc<CallbackFailures>>) -> (StatusCode, Value) {
    let manifest = serde_json::from_value(
        json!({"name": "health-test", "project_type": "no-code", "networks": [], "contracts": []}),
    )
    .unwrap();
    let router =
        HealthServer::new(0, Arc::new(manifest), None).with_callback_failures(sources).router();
    let response = router
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test(start_paused = true)]
async fn decode_failure_stops_retries_and_blocks_only_its_network_stream() {
    let params = params();
    let valid = event(&params, 1);
    let mut invalid = event(&params, 2);
    invalid.log.inner.data = alloy::primitives::LogData::new_unchecked(
        invalid.log.topics().to_vec(),
        alloy::primitives::Bytes::new(),
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
    let source = registry.failure_state();
    timeout(
        Duration::from_secs(10),
        registry.trigger_event(&ID.to_string(), vec![invalid.clone()]),
    )
    .await
    .expect("a permanent decode failure must return instead of retrying indefinitely")
    .expect_err("the stream must fail without skipping the event");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "never retry an immutable undecodable batch");
    registry
        .trigger_event(&ID.to_string(), vec![invalid])
        .await
        .expect_err("a blocked stream stays blocked");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "blocked streams do not invoke callbacks again");
    let (status, body) = health(vec![source]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["services"]["indexing"], "unhealthy");
    assert_eq!(body["indexing"]["failed_streams"][0]["network"], "ethereum");
    let mut other_network = valid;
    other_network.tx_information.network = "other-network".to_string();
    registry
        .trigger_event(&ID.to_string(), vec![other_network])
        .await
        .expect("an unrelated network remains indexable");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn transient_callback_errors_still_retry_until_success() {
    let params = params();
    let valid = event(&params, 1);
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let registry = registry(
        &params,
        Arc::new(move |_| {
            let attempt = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 2 {
                    Err("temporary callback failure".into())
                } else {
                    Ok(())
                }
            }
            .boxed()
        }),
    );
    timeout(Duration::from_secs(10), registry.trigger_event(&ID.to_string(), vec![valid]))
        .await
        .expect("bounded transient retries")
        .expect("third attempt succeeds");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(registry.failure_state().snapshot().is_empty());
}

#[tokio::test]
async fn health_reports_failed_stream_identity_without_private_error_text() {
    let source = Arc::new(CallbackFailures::default());
    source.record(failure(), "private diagnostic must stay in server logs".to_string());
    let (status, body) = health(vec![source]).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a running process with a failed stream is unhealthy"
    );
    assert_eq!(body["indexing"]["failed_streams"][0]["contract"], "Contract");
    assert!(!body.to_string().contains("private diagnostic"));
}

#[tokio::test]
async fn healthy_status_keeps_its_existing_shape_without_failures() {
    let (status, body) = health(vec![Arc::new(CallbackFailures::default())]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["services"]["indexing"], "healthy");
    assert!(body["indexing"].get("failed_streams").is_none(), "omit an empty failure list");
}

#[tokio::test]
async fn registry_clones_and_health_share_one_terminal_failure_record() {
    let params = params();
    let registry = registry(&params, Arc::new(|_| async { Ok(()) }.boxed()));
    let health_source = registry.failure_state();
    health_source.record(failure(), "terminal diagnostic".to_string());
    assert_eq!(
        registry.complete().failure_state().reason(ID, "ethereum"),
        Some("terminal diagnostic".to_string()),
        "completion clones must not lose the runtime failure state"
    );
}
