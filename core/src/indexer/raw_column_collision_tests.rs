//! ABI values and indexing metadata must retain separate storage and recovery identities.
use super::*;
use crate::{
    abi::{ABIInput, GenerateAbiPropertiesType},
    database::{
        clickhouse::generate::generate_columns_with_data_types as clickhouse_columns,
        postgres::generate::generate_column_names_only_with_base_properties,
    },
    indexer::reorg::{
        task::{
            DerivedColumnRollback, DerivedTableInfo, DerivedTableRollbackOp,
            DerivedTableRollbackPlan, EventTableInfo, ReorgTask,
        },
        BlockChainWindow,
    },
    manifest::contract::{ColumnType, SetAction},
};
use alloy::{
    dyn_abi::DynSolValue,
    primitives::{LogData, U256, U64},
};

fn names() -> Vec<String> {
    let mut names = [
        "value",
        "contract_address",
        "block_number",
        "block_timestamp",
        "block_hash",
        "tx_hash",
        "network",
        "tx_index",
        "log_index",
        "rindexer_id",
        "rindexer_abi_block_number",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    names.extend(['x', 'y'].map(|suffix| format!("rindexer_abi_{}{suffix}", "a".repeat(49))));
    names
}
fn definition() -> serde_json::Value {
    json!({"name":"Value","type":"event","anonymous":false,"inputs":names().iter().map(|name|json!({"name":name,"type":"uint256","indexed":false})).collect::<Vec<_>>()})
}
fn values(id: u64) -> Vec<u64> {
    (0..names().len())
        .map(|position| if position == 0 { id } else { (position as u64) * 1000 + id })
        .collect()
}
fn encoded_event(fixture: &Fixture, id: u64, values: &[u64]) -> EventResult {
    let mut event = fixture.events(&[id]).remove(0);
    let encoded = DynSolValue::Tuple(
        values.iter().map(|value| DynSolValue::Uint(U256::from(*value), 256)).collect(),
    )
    .abi_encode_params();
    event.log.inner.data =
        LogData::new_unchecked(vec![fixture.params.event.selector()], encoded.into());
    event
}

#[test]
fn raw_storage_names_separate_metadata_and_preserve_original_event_paths() {
    let inputs: Vec<ABIInput> = serde_json::from_value(definition()["inputs"].clone()).unwrap();
    let sql = generate_column_names_only_with_base_properties(&inputs);
    let expected = [
        "value",
        "rindexer_abi_contract_address",
        "rindexer_abi_block_number",
        "rindexer_abi_block_timestamp",
        "rindexer_abi_block_hash",
        "rindexer_abi_tx_hash",
        "rindexer_abi_network",
        "rindexer_abi_tx_index",
        "rindexer_abi_log_index",
        "rindexer_abi_rindexer_id",
        "rindexer_abi_rindexer_abi_block_number",
    ];
    assert_eq!(&sql[1..12], &expected.map(str::to_string));
    assert_eq!(sql.iter().collect::<std::collections::HashSet<_>>().len(), sql.len());
    assert!(
        sql.iter().all(|name| name.len() <= 63),
        "escaped names must not collide through PostgreSQL truncation"
    );
    let csv = ABIInput::generate_abi_name_properties(
        &inputs,
        &GenerateAbiPropertiesType::CsvHeaderNames,
        None,
    );
    assert_eq!(
        csv.iter().map(|property| &property.value).collect::<Vec<_>>(),
        sql[1..1 + inputs.len()].iter().collect::<Vec<_>>()
    );
    let event = crate::abi::EventInfo::new(serde_json::from_value(definition()).unwrap()).unwrap();
    assert_eq!(
        event.csv_headers_for_event(),
        sql.iter().filter(|name| name.as_str() != "block_timestamp").cloned().collect::<Vec<_>>()
    );
    let objects =
        ABIInput::generate_abi_name_properties(&inputs, &GenerateAbiPropertiesType::Object, None);
    assert_eq!(
        objects.iter().map(|property| property.value.clone()).collect::<Vec<_>>(),
        names(),
        "storage escapes must not rename ABI access paths"
    );
    let columns = clickhouse_columns(&inputs);
    for (column, name) in columns.iter().zip(&sql[1..1 + inputs.len()]) {
        assert!(
            column.starts_with(&format!("\"{name}\" ")),
            "ClickHouse DDL and runtime column names must agree"
        );
    }
}

#[tokio::test]
async fn postgres_abi_metadata_collisions_round_trip_through_insert_and_copy() {
    for count in [2, 101] {
        let mut event_definition = definition();
        if count > 100 {
            event_definition["inputs"][2]["name"] = json!("blockNumber");
        }
        let fixture = Fixture::with_event(event_definition, "Contract").await;
        let events = (1..=count).map(|id| encoded_event(&fixture, id, &values(id))).collect();
        let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
        callback(events).await.unwrap();
        assert_eq!(
            fixture.position().await,
            (count as i64, (count * (count + 1) / 2).to_string(), "2".into())
        );
        let columns = &fixture.params.sql_column_names[1..1 + names().len()];
        let selection =
            columns.iter().map(|name| format!("\"{name}\"::text")).collect::<Vec<_>>().join(",");
        let rows=fixture.client.query(&format!("SELECT {selection},block_number::text,network,contract_address,rindexer_id,tx_hash,block_hash,tx_index::text,log_index::text,(block_timestamp IS NULL) FROM {} AS event_row ORDER BY event_row.value::numeric",fixture.params.sql_event_table_name),&[]).await.unwrap();
        for (offset, row) in rows.iter().enumerate() {
            let id = offset as u64 + 1;
            for (index, value) in values(id).iter().enumerate() {
                assert_eq!(row.get::<_, String>(index), value.to_string());
            }
            assert_eq!(row.get::<_, String>(names().len()), "2");
            assert_eq!(row.get::<_, String>(names().len() + 1), "ethereum");
            assert_eq!(
                row.get::<_, String>(names().len() + 2),
                format!("{:#x}", alloy::primitives::Address::ZERO)
            );
            assert_eq!(row.get::<_, i32>(names().len() + 3), id as i32);
            assert_eq!(
                row.get::<_, String>(names().len() + 4),
                format!("{:#x}", B256::repeat_byte(id as u8))
            );
            assert_eq!(
                row.get::<_, String>(names().len() + 5),
                format!("{:#x}", B256::repeat_byte(2))
            );
            assert_eq!(row.get::<_, String>(names().len() + 6), id.to_string());
            assert_eq!(row.get::<_, String>(names().len() + 7), id.to_string());
            assert!(row.get::<_, bool>(names().len() + 8));
        }
        let declared:Vec<String>=fixture.client.query("SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name='value'",&[&generate_indexer_contract_schema_name(&fixture.params.indexer_name,"Contract")]).await.unwrap().iter().map(|row|row.get(0)).collect();
        assert_eq!(
            declared.len(),
            fixture.params.sql_column_names.len() + 1,
            "all generated metadata must participate in the naming contract"
        );
        fixture.finish().await;
    }
}

async fn rollback(
    fixture: &Fixture,
    source_types: std::collections::HashMap<String, ColumnType>,
    bindings: Vec<crate::manifest::contract::IterateBinding>,
    operand: &str,
) {
    let schema = generate_indexer_contract_schema_name(&fixture.params.indexer_name, "Contract");
    let cursor = generate_internal_event_table_name(&schema, "Value");
    let totals = fixture.params.tables[0].full_table_name.clone();
    let mut plan = DerivedTableRollbackPlan::default();
    plan.try_add_operation(
        totals.clone(),
        0,
        bindings,
        source_types,
        [("amount".into(), ColumnType::Uint256)].into(),
    )
    .unwrap();
    let task = ReorgTask {
        network: "ethereum".into(),
        fork_point: 100,
        detection_point: 100,
        event_tables: vec![EventTableInfo::try_new(
            schema,
            "value".into(),
            cursor,
            fixture.params.indexer_name.clone(),
            "Contract".into(),
            "Value".into(),
        )
        .unwrap()],
        derived_tables: vec![DerivedTableInfo::try_new(
            totals,
            false,
            vec![DerivedTableRollbackOp::try_new(
                fixture.params.sql_event_table_name.clone(),
                vec![],
                vec![DerivedColumnRollback::try_new(
                    "amount".into(),
                    operand.into(),
                    SetAction::Add,
                )
                .unwrap()],
                None,
            )
            .unwrap()],
            vec![],
        )
        .unwrap()],
        canonical_blocks: vec![],
    };
    task.execute_with_rollback_plan(
        &mut BlockChainWindow::try_new(8).unwrap(),
        Some(&fixture.client),
        None,
        None,
        &plan,
    )
    .await
    .unwrap();
}
fn table(iterate: serde_json::Value, operand: &str) -> serde_json::Value {
    json!({"name":"totals","global":true,"columns":[{"name":"amount","type":"uint256","default":"0"}],"events":[{"event":"Value","iterate":iterate,"operations":[{"type":"upsert","set":[{"column":"amount","action":"add","value":format!("${operand}")}]}]}]})
}

#[tokio::test]
async fn postgres_collision_rollback_reverses_the_abi_value_instead_of_chain_height() {
    let fixture =
        Fixture::with_table(definition(), "Contract", table(json!([]), "block_number")).await;
    let callback = no_code_callback(Arc::new(fixture.params.clone())).event_callback;
    for (height, amount) in [(99, 500), (100, 700)] {
        let mut data = values(height);
        data[2] = amount;
        let mut event = encoded_event(&fixture, height, &data);
        event.log.block_number = Some(height);
        event.log.block_hash = Some(B256::repeat_byte(height as u8));
        event.tx_information.block_number = height;
        event.tx_information.block_hash = B256::repeat_byte(height as u8);
        event.found_in_request.from_block = U64::from(height);
        event.found_in_request.to_block = U64::from(height);
        callback(vec![event]).await.unwrap();
    }
    assert_eq!(fixture.position().await.1, "1200");
    rollback(
        &fixture,
        [
            ("rindexer_abi_block_number".into(), ColumnType::Uint256),
            ("block_number".into(), ColumnType::Uint64),
        ]
        .into(),
        vec![],
        "block_number",
    )
    .await;
    assert_eq!(fixture.position().await.1, "500");
    fixture.finish().await;
}

#[tokio::test]
async fn postgres_iterate_alias_matching_metadata_still_uses_its_virtual_value() {
    for (array_field, alias, expected) in
        [("ids", "block_number", "1200"), ("network", "block_number", "1200")]
    {
        let event = json!({"name":"Value","type":"event","anonymous":false,"inputs":[{"name":array_field,"type":"uint256[]","indexed":false}]});
        let fixture = Fixture::with_table(
            event,
            "Contract",
            table(json!([format!("${array_field} as {alias}")]), alias),
        )
        .await;
        let mut event = fixture.events(&[1]).remove(0);
        event.log.inner.data = LogData::new_unchecked(
            vec![fixture.params.event.selector()],
            DynSolValue::Tuple(vec![DynSolValue::Array(vec![
                DynSolValue::Uint(U256::from(500), 256),
                DynSolValue::Uint(U256::from(700), 256),
            ])])
            .abi_encode_params()
            .into(),
        );
        event.log.block_number = Some(100);
        event.tx_information.block_number = 100;
        event.found_in_request.from_block = U64::from(100);
        event.found_in_request.to_block = U64::from(100);
        (no_code_callback(Arc::new(fixture.params.clone())).event_callback)(vec![event])
            .await
            .unwrap();
        assert_eq!(fixture.position().await.1, expected);
        rollback(
            &fixture,
            [(
                fixture.params.sql_column_names[1].clone(),
                ColumnType::Array(Box::new(ColumnType::Uint256)),
            )]
            .into(),
            vec![crate::manifest::contract::IterateBinding {
                array_field: array_field.into(),
                alias: alias.into(),
            }],
            alias,
        )
        .await;
        assert_eq!(fixture.position().await.1, "0");
        fixture.finish().await;
    }
}
