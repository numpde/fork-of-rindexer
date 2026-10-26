//! Startup DDL, schema sync and production writes must target the same table.
use super::*;
use crate::database::postgres::schema_sync::{
    apply_schema_change, detect_schema_changes, SchemaChange,
};

#[tokio::test]
async fn postgres_custom_schema_override_is_shared_by_startup_writes_and_schema_sync() {
    let fixture = Fixture::new().await;
    fixture
        .client
        .batch_execute(&format!("DROP TABLE {}", fixture.params.tables[0].full_table_name))
        .await
        .unwrap();
    let mut indexer = fixture.indexer.clone();
    indexer.contracts[0].tables.as_mut().unwrap()[0].database = Some("shared_state".into());
    let mut second = indexer.contracts[0].clone();
    second.name = "Second".into();
    let mut local = second.clone();
    local.name = "Local".into();
    local.tables.as_mut().unwrap()[0].database = None;
    indexer.contracts.extend([second, local]);
    let ddl = generate_tables_for_indexer_sql(fixture._directory.path(), &indexer, false).unwrap();
    fixture.client.batch_execute(ddl.as_str()).await.unwrap();
    fixture.client.batch_execute(ddl.as_str()).await.unwrap();
    let destination: Option<String> = fixture
        .client
        .query_one("SELECT to_regclass('shared_state.totals')::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        destination.as_deref(),
        Some("shared_state.totals"),
        "startup must create the same override destination used by writes"
    );
    for (contract, value) in indexer.contracts.iter().zip([10, 20, 40]) {
        let mut params = fixture.params.clone();
        params.contract_name = contract.name.clone();
        let schema = generate_indexer_contract_schema_name(&indexer.name, &contract.name);
        params.sql_event_table_name = format!("{schema}.value");
        params.tables = Arc::new(
            contract
                .tables
                .as_ref()
                .unwrap()
                .iter()
                .cloned()
                .map(|table| TableRuntime::new(table, &indexer.name, &contract.name))
                .collect(),
        );
        let callback = no_code_callback(Arc::new(params)).event_callback;
        callback(fixture.events(&[value])).await.unwrap();
    }
    let shared: String = fixture
        .client
        .query_one("SELECT amount::text FROM shared_state.totals", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(shared, "30", "both override declarations contribute to the shared row");
    let local_schema = generate_indexer_contract_schema_name(&indexer.name, "Local");
    let local: String = fixture
        .client
        .query_one(&format!("SELECT amount::text FROM {local_schema}.totals"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(local, "40", "the default destination remains isolated");
    let mut invalid = indexer.clone();
    invalid.contracts[0].tables.as_mut().unwrap()[0].database =
        Some("shared; DROP SCHEMA public".into());
    assert!(
        generate_tables_for_indexer_sql(fixture._directory.path(), &invalid, false).is_err(),
        "schema identifiers must be rejected before interpolating startup SQL"
    );
    let manifest: crate::manifest::core::Manifest = serde_json::from_value(json!({"name":indexer.name,"project_type":"no-code","contracts":indexer.contracts,"networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid"}]})).unwrap();
    assert!(detect_schema_changes(&fixture.client, &manifest).await.unwrap().is_empty());
    fixture
        .client
        .batch_execute("ALTER TABLE shared_state.totals DROP COLUMN amount")
        .await
        .unwrap();
    let changes = detect_schema_changes(&fixture.client, &manifest).await.unwrap();
    assert_eq!(changes.len(), 1, "one physical shared table requires one migration: {changes:?}");
    assert!(
        matches!(&changes[0],SchemaChange::AddColumn {table_full_name,column_name,..} if table_full_name=="shared_state.totals" && column_name=="amount")
    );
    for change in &changes {
        apply_schema_change(&fixture.client, change).await.unwrap();
    }
    assert!(detect_schema_changes(&fixture.client, &manifest).await.unwrap().is_empty());
    fixture.finish().await;
}
