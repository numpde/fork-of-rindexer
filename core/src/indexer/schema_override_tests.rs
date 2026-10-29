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

#[tokio::test]
async fn postgres_shared_target_recovery_requires_one_manifest_owner() {
    use crate::indexer::reorg::{
        BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence, ReorgContext, ReorgCoordinator,
    };
    use crate::manifest::contract::ColumnType;
    use crate::provider::mock::MockChainProvider;
    let table = json!({"name":"totals","database":"shared_state","global":true,
        "columns":[{"name":"amount","type":"uint256","default":"0"},{"name":"b","type":"uint256","default":"0"}],
        "events":[{"event":"Value","operations":[{"type":"upsert","set":[
            {"column":"amount","action":"set","value":"$value"}]}]}]});
    let fixture =
        Fixture::with_table(serde_json::to_value(params().event).unwrap(), "Contract", table).await;
    let mut indexer = fixture.indexer.clone();
    let mut second = indexer.contracts[0].clone();
    second.name = "Second".into();
    second.tables.as_mut().unwrap()[0].events[0].operations[0].set[0].column = "b".into();
    indexer.contracts.push(second);
    let path = fixture._directory.path().join("rindexer.yaml");
    let manifest = json!({"name":indexer.name,"project_type":"no-code","storage":{"postgres":{"enabled":true}},"contracts":indexer.contracts,
        "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://unused.invalid","reorg_handling":{"enabled":true}}]});
    std::fs::write(&path, serde_yaml::to_string(&manifest).unwrap()).unwrap();
    // Refusal is safe until shared declarations have one complete source/recovery
    // model. If admission is relaxed, this scenario must preserve surviving state.
    if let Err(error) = crate::manifest::yaml::read_manifest(&path) {
        let error = error.to_string();
        assert!(
            error.contains("shared_state.totals")
                && error.contains("recovery")
                && error.contains("Contract")
                && error.contains("Second"),
            "{error}"
        );
        assert_eq!(fixture.position().await, (0, "0".into(), "0".into()));
        fixture.finish().await;
        return;
    }
    let ddl = generate_tables_for_indexer_sql(fixture._directory.path(), &indexer, false).unwrap();
    fixture.client.batch_execute(ddl.as_str()).await.unwrap();
    let mut derived = std::collections::HashMap::new();
    let mut plans = std::collections::HashMap::new();
    let mut sources = Vec::new();
    for (contract, block, value) in
        [(&indexer.contracts[1], 100, 2), (&indexer.contracts[0], 99, 1)]
    {
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
        crate::indexer::start::build_derived_tables_for_event(
            "Value",
            &indexer.name,
            &contract.name,
            "ethereum",
            &[("value".into(), ColumnType::Uint256)].into(),
            &params.tables,
            &mut derived,
            &mut plans,
        )
        .unwrap();
        sources.push(
            EventTableInfo::try_new(
                schema.clone(),
                "value".into(),
                generate_internal_event_table_name(&schema, "Value"),
                indexer.name.clone(),
                contract.name.clone(),
                "Value".into(),
            )
            .unwrap(),
        );
        let callback = no_code_callback(Arc::new(params)).event_callback;
        callback(vec![super::journal_recovery_tests::at(
            &fixture,
            "ethereum",
            block,
            B256::repeat_byte(block as u8),
            value,
        )])
        .await
        .unwrap();
    }
    let before: (String, String) = {
        let row = fixture
            .client
            .query_one("SELECT amount::text,b::text FROM shared_state.totals", &[])
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(before, ("0".into(), "2".into()));
    let tables = derived.remove("ethereum").unwrap();
    assert_eq!(tables.len(), 1, "production planning merges physical destinations");
    assert_eq!(
        tables[0].journal_columns.len(),
        2,
        "both independent definitions reach one recovery plan"
    );
    let mut header =
        crate::indexer::tables::view_call_cache_tests::header(B256::repeat_byte(104), 1103);
    header.header.inner.number = 103;
    header.header.inner.parent_hash = B256::repeat_byte(103);
    let mut coordinator = ReorgCoordinator::new(
        "ethereum".into(),
        BlockChainWindow::try_new(16).unwrap(),
        Arc::new(ReorgBlockHashPersistence::new(Some(fixture.client.clone()), None)),
        Arc::new(MockChainProvider::new(1).with_blocks(vec![header])),
        sources,
        tables,
        vec![],
    )
    .unwrap()
    .with_rollback_plan(plans.remove("ethereum").unwrap());
    let task = coordinator.try_create_reorg_task_for_block_range(103, 103).unwrap();
    coordinator
        .handle_reorg(
            task,
            &ReorgContext {
                postgres: Some(&fixture.client),
                clickhouse: None,
                registry: None,
                trace_registry: None,
            },
        )
        .await
        .unwrap();
    let row = fixture
        .client
        .query_one("SELECT amount::text,b::text FROM shared_state.totals", &[])
        .await
        .unwrap();
    let after: (String, String) = (row.get(0), row.get(1));
    assert_eq!(
        after, before,
        "unrelated recovery cannot install a superseded assignment from another declaration"
    );
    fixture.finish().await;
}
