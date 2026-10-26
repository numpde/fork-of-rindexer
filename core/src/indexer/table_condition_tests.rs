//! Table-state guards must survive manifest validation and reach mutation SQL.
use super::journal_recovery_tests::at;
use super::*;
use crate::manifest::contract::OperationType;

async fn guarded_operation(kind: &str) {
    for table_reference in [true, false] {
        for guard_true in [false, true] {
            let condition = if table_reference {
                "@balance == 0 && balance == 50".to_string()
            } else {
                format!("balance == {}", if guard_true { 50 } else { 51 })
            };
            let mut definition = serde_json::to_value(params().event).unwrap();
            definition["inputs"][0]["name"] = json!("balance");
            definition["inputs"][0]["type"] = json!("uint64");
            let table = json!({"name":"totals","columns":[{"name":"id","type":"uint64"},{"name":"balance","type":"uint64"}],
                "events":[{"event":"Value","operations":[{"type":kind,"where":{"id":"1"},"if":condition,
                    "set": if kind == "delete" {json!([])} else {json!([{"column":"balance","action":"set","value":"$balance"}])}}]}]});
            let mut fixture = Fixture::with_table(definition, "Contract", table.clone()).await;
            let manifest_path = fixture._directory.path().join("rindexer.yaml");
            std::fs::write(&manifest_path,serde_yaml::to_string(&json!({"name":"Replay","project_type":"no-code",
                "networks":[{"name":"ethereum","chain_id":1,"rpc":"http://127.0.0.1:8545"}],
                "contracts":[{"name":"Contract","abi":"abi.json","details":[{"network":"ethereum","address":"0x0000000000000000000000000000000000000000"}],"tables":[table]}]})).unwrap()).unwrap();
            let validated = crate::manifest::yaml::read_manifest(&manifest_path)
                .expect("guarded mutation is accepted at startup");
            fixture.params.tables = Arc::new(
                validated.contracts[0]
                    .tables
                    .as_ref()
                    .unwrap()
                    .iter()
                    .cloned()
                    .map(|table| TableRuntime::new(table, "Replay", "Contract"))
                    .collect(),
            );
            let mut seed = fixture.params.clone();
            let mut tables = seed.tables.as_ref().clone();
            let operation = &mut tables[0].table.events[0].operations[0];
            operation.operation_type = OperationType::Upsert;
            operation.if_condition = None;
            operation.set = serde_json::from_value(
                json!([{"column":"balance","action":"set","value":"$balance"}]),
            )
            .unwrap();
            seed.tables = Arc::new(tables);
            let seed_callback = no_code_callback(Arc::new(seed)).event_callback;
            let original = if guard_true && table_reference { 0 } else { 100 };
            seed_callback(vec![at(&fixture, "ethereum", 99, B256::repeat_byte(99), original)])
                .await
                .unwrap();
            let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
            callback(vec![at(&fixture, "ethereum", 100, B256::repeat_byte(100), 50)])
                .await
                .unwrap();
            let rows = fixture
                .client
                .query(
                    &format!(
                        "SELECT balance::text FROM {}",
                        fixture.params.tables[0].full_table_name
                    ),
                    &[],
                )
                .await
                .unwrap();
            if guard_true && kind == "delete" {
                assert!(rows.is_empty(), "true delete guard must delete the row");
            } else {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].get::<_, String>(0),
                    if guard_true { "50".to_string() } else { original.to_string() },
                    "{kind}, table reference={table_reference}, condition={condition}"
                );
            }
            drop(callback);
            drop(seed_callback);
            fixture.finish().await;
        }
    }
}

#[tokio::test]
async fn postgres_update_enforces_table_and_event_conditions() {
    guarded_operation("update").await;
}

#[tokio::test]
async fn postgres_delete_enforces_table_and_event_conditions() {
    guarded_operation("delete").await;
}
