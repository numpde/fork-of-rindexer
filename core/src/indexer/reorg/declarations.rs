//! Compile event and table declarations into the same recovery plan used by managed indexing.

use super::*;
use crate::{
    abi::{validated_raw_event_columns, ABIInput, ABIItem},
    database::generate::generate_event_table_full_name,
    indexer::tables::TableRuntime,
    manifest::contract::Table,
};

/// Physical raw column names keyed by their exact declared ABI paths, together
/// with scalar storage types. JSON tuple members retain their original names.
#[derive(Clone, Default, Debug)]
pub(crate) struct RollbackEventSource {
    pub(crate) types: Arc<HashMap<String, ColumnType>>,
    names: Arc<HashMap<String, String>>,
}

impl RollbackEventSource {
    pub(crate) fn new(name: &str, inputs: &[ABIInput]) -> anyhow::Result<Self> {
        let mut types = HashMap::new();
        let mut names = HashMap::new();
        for leaf in validated_raw_event_columns(name, inputs).map_err(anyhow::Error::msg)? {
            let storage = leaf.input.storage_type();
            names.insert(leaf.logical_path, leaf.storage_name.clone());
            if storage.starts_with("tuple[") {
                if let Some(components) = &leaf.input.components {
                    collect_json_types(components, &leaf.storage_name, &mut types);
                }
            } else if let Some(kind) = ColumnType::from_solidity_type(storage) {
                types.insert(leaf.storage_name, kind);
            }
        }
        Ok(Self { types: Arc::new(types), names: Arc::new(names) })
    }
}

fn collect_json_types(inputs: &[ABIInput], prefix: &str, out: &mut HashMap<String, ColumnType>) {
    for input in inputs {
        let path = format!("{prefix}.{}", input.name);
        if let Some(components) = &input.components {
            collect_json_types(components, &path, out);
        } else if let Some(kind) = ColumnType::from_solidity_type(&input.type_) {
            out.insert(path, kind);
        }
    }
}

impl DerivedTableRollbackPlan {
    /// Register a table driven by one event using its original, logical declarations.
    /// For multi-event tables, pass all contributing events to [`Self::try_add_table_events`].
    ///
    /// # Errors
    ///
    /// Returns the admission errors described by [`Self::try_add_table_events`].
    pub fn try_add_table_event(
        &mut self,
        derived_tables: &mut Vec<DerivedTableInfo>,
        indexer_name: &str,
        contract_name: &str,
        event: &ABIItem,
        table: &Table,
    ) -> anyhow::Result<()> {
        self.try_add_table_events(
            derived_tables,
            indexer_name,
            contract_name,
            std::slice::from_ref(event),
            table,
        )
    }

    /// Register all the events contributing to **one complete table declaration**.
    /// Provide its original manifest table and the relevant ABI events in any order.
    /// This resolves storage names, infers missing destination types using all events,
    /// and registers the operations and journals together. Separate declarations
    /// cannot share the same recovered table. Use the resulting `derived_tables`
    /// in the [`ReorgTask`] and pass this plan to [`ReorgTask::execute_with_rollback_plan`].
    ///
    /// # Errors
    ///
    /// Missing or ambiguous ABI events, unsafe recovery operations, invalid storage
    /// names, and repeat registrations are rejected. Both outputs remain unchanged.
    pub fn try_add_table_events(
        &mut self,
        derived_tables: &mut Vec<DerivedTableInfo>,
        indexer_name: &str,
        contract_name: &str,
        events: &[ABIItem],
        table: &Table,
    ) -> anyhow::Result<()> {
        table.validate_event_mappings().map_err(anyhow::Error::msg)?;
        table.validate_reorg_operations().map_err(anyhow::Error::msg)?;
        table.validate_where_columns().map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            !table.events.is_empty(),
            "table '{}' has no event mapping to register",
            table.name
        );

        let sources = table.events.iter().map(|mapping| {
            let mut matches = events.iter().filter(|item| item.type_ == "event" && item.name == mapping.event);
            let event = matches.next().with_context(|| format!(
                "no ABI event '{}' for table '{}'; supply every mapped event to try_add_table_events", mapping.event, table.name
            ))?;
            anyhow::ensure!(matches.next().is_none(), "ambiguous ABI event '{}' for table '{}'", mapping.event, table.name);
            Ok((event, RollbackEventSource::new(&event.name, &event.inputs)?))
        }).collect::<anyhow::Result<Vec<_>>>()?;
        let mut table = table.clone();
        if table.columns.iter().any(|column| column.column_type.is_none()) {
            let types = crate::indexer::no_code::build_event_abi_types(
                sources.iter().map(|(event, _)| (*event).clone()).collect(),
            )?;
            table.resolve_column_types(&types).map_err(anyhow::Error::msg)?;
        }
        let runtime = TableRuntime::try_new(&table, indexer_name, contract_name)
            .map_err(anyhow::Error::msg)?;
        let name = &runtime.full_table_name;
        crate::indexer::reorg::validate_recovered_table_name(name)?;
        let identity = crate::indexer::reorg::recovery_table_identity(name);
        let registered = self
            .operations
            .keys()
            .chain(derived_tables.iter().map(|table| &table.full_table_name))
            .any(|registered| {
                crate::indexer::reorg::recovery_table_identity(registered) == identity
            });
        anyhow::ensure!(!registered,
            "recovery already registered for {identity}: use one complete table declaration with every contributing event");
        let mut staged_tables = Vec::new();
        let mut staged = Self::default();
        for (event, source) in sources {
            let raw_table =
                generate_event_table_full_name(indexer_name, contract_name, &event.name);
            staged.add_resolved_table_event(
                &mut staged_tables,
                &event.name,
                &raw_table,
                &source,
                &runtime,
            )?;
        }
        self.operations.extend(staged.operations);
        derived_tables.extend(staged_tables);
        Ok(())
    }

    pub(crate) fn add_resolved_table_event(
        &mut self,
        derived: &mut Vec<DerivedTableInfo>,
        event_name: &str,
        event_table_name: &str,
        source: &RollbackEventSource,
        runtime: &TableRuntime,
    ) -> anyhow::Result<()> {
        runtime.table.definition().validate_event_mappings().map_err(anyhow::Error::msg)?;
        let derived_column_types = Arc::new(
            runtime
                .table
                .columns()
                .iter()
                .filter_map(|column| {
                    column.column_type.clone().map(|column_type| (column.name.clone(), column_type))
                })
                .collect::<HashMap<_, _>>(),
        );

        let mut rollback_ops: Vec<DerivedTableRollbackOp> = Vec::new();
        let mut rollback_metadata = Vec::new();
        let mut journal_columns: Vec<DerivedColumnJournal> = Vec::new();

        for table_event in runtime.table.event_mappings() {
            if table_event.event != event_name {
                continue;
            }
            for operation in &table_event.operations {
                use crate::manifest::contract::OperationType;
                if !matches!(
                    operation.operation_type,
                    OperationType::Upsert | OperationType::Update
                ) {
                    continue;
                }

                let mut where_columns: Vec<(String, String)> = operation
                    .where_clause
                    .iter()
                    .filter_map(|(col, val)| {
                        val.strip_prefix('$').map(|field| (col.clone(), field.to_string()))
                    })
                    .collect();
                where_columns.sort_by(|a, b| a.0.cmp(&b.0));

                let mut where_col_names =
                    operation.where_clause.keys().cloned().collect::<Vec<_>>();
                where_col_names.sort();

                let columns: Vec<DerivedColumnRollback> = operation
                    .set
                    .iter()
                    .filter(|set_col| set_col.action.reverse().is_some())
                    .map(|set_col| {
                        let event_field = set_col
                            .reorg_event_field()
                            .map_err(anyhow::Error::msg)?
                            .unwrap_or("rindexer_block_number")
                            .to_string();
                        DerivedColumnRollback::try_new(
                            set_col.column.clone(),
                            event_field,
                            set_col.action.clone(),
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                // Collect non-reversible columns for journal-based recalculation
                for set_col in &operation.set {
                    if set_col.action.reverse().is_some() {
                        continue; // handled by rollback_ops
                    }
                    if !journal_columns.iter().any(|jc| jc.derived_column == set_col.column) {
                        journal_columns.push(DerivedColumnJournal::try_new(
                            set_col.column.clone(),
                            set_col.action.clone(),
                            where_col_names.clone(),
                        )?);
                    }
                }

                if !columns.is_empty() {
                    anyhow::ensure!(
                        where_columns.len() == operation.where_clause.len(),
                        "reorg rollback for {} requires every WHERE value to be a single event-field reference",
                        runtime.full_table_name,
                    );
                    rollback_ops.push(DerivedTableRollbackOp::try_new(
                        event_table_name.to_string(),
                        where_columns,
                        columns,
                        operation.condition().map(String::from),
                    )?);
                    rollback_metadata.push(table_event.iterate.clone());
                }
            }
        }

        let table_name = &runtime.full_table_name;
        let mut addition = DerivedTableInfo::try_new(
            table_name.clone(),
            runtime.table.definition().cross_chain,
            rollback_ops,
            journal_columns,
        )?;
        // Validate a detached delta before publishing any table or sidecar entries.
        let mut delta = Self::default();
        for (i, iterate) in rollback_metadata.into_iter().enumerate() {
            delta.add_operation(
                table_name.clone(),
                i,
                iterate,
                Arc::clone(&source.types),
                Arc::clone(&derived_column_types),
                Some(Arc::clone(&source.names)),
            )?;
        }
        delta.validate_operations(&addition)?;
        let existing = derived.iter().position(|table| &table.full_table_name == table_name);
        let offset = existing.map_or(0, |i| derived[i].rollback_ops.len());
        if let Some(i) = existing {
            anyhow::ensure!(
                addition.rollback_ops.is_empty()
                    || !derived[i].rollback_ops.iter().any(|op| op.event_table == event_table_name),
                "arithmetic recovery already registered for {table_name} from {event_table_name}"
            );
            anyhow::ensure!(
                derived[i].cross_chain == addition.cross_chain,
                "conflicting cross_chain declarations for table {table_name}"
            );
        }
        for i in 0..addition.rollback_ops.len() {
            anyhow::ensure!(
                self.operation(table_name, offset + i).is_none(),
                "duplicate rollback metadata for table {table_name} operation {}",
                offset + i
            );
        }
        if let Some(operations) = delta.operations.remove(table_name) {
            self.operations
                .entry(table_name.clone())
                .or_default()
                .extend(operations.into_iter().map(|(i, metadata)| (offset + i, metadata)));
        }
        if let Some(i) = existing {
            let table = &mut derived[i];
            table.rollback_ops.append(&mut addition.rollback_ops);
            for column in addition.journal_columns {
                if !table
                    .journal_columns
                    .iter()
                    .any(|entry| entry.derived_column == column.derived_column)
                {
                    table.journal_columns.push(column);
                }
            }
        } else {
            derived.push(addition);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn declared_plan_refuses_unsafe_recovery_models_before_registration() {
        let event: ABIItem = serde_json::from_value(json!({
            "type":"event","name":"Value","inputs":[{"type":"uint256","name":"value"}]
        }))
        .unwrap();
        let valid = json!({
            "name":"balances","columns":[{"name":"key","type":"uint256"},{"name":"total","type":"uint256"}],
            "events":[{"event":"Value","operations":[{"type":"upsert","where":{"key":"$value"},
                "set":[{"column":"total","action":"add","value":"$value"}]}]}]
        });
        for (change, reason) in
            [("delete", "delete"), ("update", "arithmetic update"), ("schema", "lowercase")]
        {
            let mut input = valid.clone();
            if change == "schema" {
                input["database"] = json!("MixedCase");
            } else {
                input["events"][0]["operations"][0]["type"] = json!(change);
            }
            let table = serde_json::from_value(input).unwrap();
            let mut plan = DerivedTableRollbackPlan::default();
            let mut tables = Vec::new();
            let error = plan
                .try_add_table_event(&mut tables, "chain", "Token", &event, &table)
                .expect_err("unsafe declarations cannot produce a recovery plan");
            assert!(format!("{error:#}").contains(reason), "{error:#}");
            assert!(tables.is_empty());
            assert!(plan.operations.is_empty());
        }
    }

    #[test]
    fn declared_plan_refuses_multiple_owners_of_one_catalog_table() {
        let event: ABIItem = serde_json::from_value(json!({
            "type":"event","name":"Move","inputs":[{"type":"uint256","name":"value"}]
        }))
        .unwrap();
        let mut table: Table = serde_json::from_value(json!({
            "database":"shared","name":"balances","global":true,
            "columns":[{"name":"total","type":"uint256"}],
            "events":[{"event":"Move","operations":[{"type":"upsert",
                "set":[{"column":"total","action":"add","value":"$value"}]}]}]
        }))
        .unwrap();
        let prefix = "x".repeat(63);
        table.name = format!("{prefix}a");
        let mut tables = Vec::new();
        let mut plan = DerivedTableRollbackPlan::default();
        plan.try_add_table_event(&mut tables, "chain", "First", &event, &table).unwrap();
        table.name = format!("{prefix}b");
        table.events[0].operations[0].set[0].action = SetAction::Set;
        let error =
            plan.try_add_table_event(&mut tables, "chain", "Second", &event, &table).expect_err(
                "individually valid declarations still cannot split a recovered write history",
            );
        assert!(error.to_string().contains("already registered"), "{error:#}");
        assert_eq!(tables.len(), 1);
        assert!(tables[0].journal_columns.is_empty());
        assert_eq!(tables[0].rollback_ops.len(), 1);
    }

    #[test]
    fn declared_plan_infers_types_and_preserves_prior_events_after_rejection() {
        let event: ABIItem = serde_json::from_value(json!({
            "type":"event","name":"Values","inputs":[{"type":"uint16","name":"key16"},
            {"type":"uint32","name":"value32"}]
        }))
        .unwrap();
        let mut table: Table = serde_json::from_value(json!({
            "name":"balances","columns":[{"name":"myKey16"},{"name":"sum32"}],
            "events":[{"event":"Values","operations":[{"type":"upsert",
                "where":{"myKey16":"$key16"},"set":[{"column":"sum32","action":"add","value":"$value32"}]}]}]
        })).unwrap();
        let mut tables = Vec::new();
        let mut plan = DerivedTableRollbackPlan::default();
        plan.try_add_table_event(&mut tables, "indexer", "Token", &event, &table).unwrap();
        let full = tables[0].full_table_name.clone();
        assert!(plan
            .try_add_table_event(&mut tables, "indexer", "Token", &event, &table)
            .unwrap_err()
            .to_string()
            .contains("already registered"));
        assert_eq!(tables[0].rollback_ops[0].where_columns[0].0, "my_key_16");
        assert_eq!(
            plan.operation(&full, 0).unwrap().derived_column_types["sum_32"],
            ColumnType::Uint32
        );

        // The decoder sees key16, not its physical spelling key_16. A later
        // malformed operation cannot publish an earlier one from the same event.
        table.columns[0].column_type = Some(ColumnType::Uint16);
        table.columns[1].column_type = Some(ColumnType::Uint32);
        table.name = "other_balances".into();
        table.events.push(table.events[0].clone());
        table.events[1].event = "Other".to_owned();
        let mut bad = table.events[0].operations[0].clone();
        bad.where_clause.insert("myKey16".into(), "$key_16".into());
        table.events[1].operations.push(bad);
        let other = ABIItem { name: "Other".into(), ..event.clone() };
        let events = [event, other];
        let err = plan
            .try_add_table_events(&mut tables, "indexer", "Token", &events, &table)
            .unwrap_err();
        assert!(format!("{err:#}").contains("key_16"), "{err:#}");
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].rollback_ops.len(), 1);
        assert!(plan.operation(&full, 1).is_none());

        table.events[1].operations.pop();
        plan.try_add_table_events(&mut tables, "indexer", "Token", &events, &table).unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[1].rollback_ops.len(), 2);
        assert!(plan.operation(&tables[1].full_table_name, 1).is_some());
    }
}
