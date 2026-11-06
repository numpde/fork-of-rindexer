//! Immutable storage declarations compiled before startup performs any database work.
//!
//! Keep this snapshot for the lifetime of one startup. Database setup, callbacks,
//! cron and recovery consume these same columns and types instead of re-reading
//! ABI files or projecting logical names independently.

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, OnceLock},
};

use alloy::json_abi::{Event, JsonAbi};

use crate::{
    abi::{
        ABIItem, EventInfo, GetAbiItemWithDbMap, GetAbiItemWithDbMapError, RawEventColumns,
        ReadAbiError,
    },
    database::{
        generate::{generate_indexer_contract_schema_name, GenerateTablesForIndexerSqlError},
        postgres::generate::generate_internal_event_table_name,
    },
    helpers::camel_to_snake,
    indexer::{
        native_transfer::{NATIVE_TRANSFER_ABI, NATIVE_TRANSFER_CONTRACT_NAME},
        reorg::task::RollbackEventSource,
        tables::{warn_on_unusable_arithmetic_defaults, TableRuntime},
        Indexer,
    },
    manifest::contract::{Contract, ContractDetails, ContractEvent},
    types::single_or_array::StringOrArray,
};

/// A prepared project schema. The input indexer contains the manifest's expanded
/// contracts (including factory discovery contracts); it is never modified.
#[derive(Clone, Debug)]
pub struct PreparedSchema {
    indexer: Indexer,
    contracts: Vec<PreparedContract>,
    native: Option<PreparedContract>,
    postgres_admission: OnceLock<Result<(), String>>,
}

/// One logical contract declaration and its canonical runtime identity.
#[derive(Clone, Debug)]
pub struct PreparedContract {
    pub(crate) declaration: Contract,
    pub(crate) name: String,
    pub(crate) schema_name: String,
    pub(crate) events: Vec<PreparedEvent>,
    pub(crate) tables: Arc<Vec<TableRuntime>>,
    pub(crate) abi: JsonAbi,
    pub(crate) items: Vec<ABIItem>,
}

/// One selected ABI event with the raw layout used by every storage consumer.
#[derive(Clone, Debug)]
pub struct PreparedEvent {
    pub(crate) info: EventInfo,
    pub(crate) event: Event,
    pub(crate) columns: Arc<RawEventColumns>,
    pub(crate) source: RollbackEventSource,
    /// A subscription can own progress without storing raw rows.
    pub(crate) raw_table: Option<String>,
    pub(crate) checkpoint_table: String,
}

#[path = "postgres_identities.rs"]
mod postgres_identities;

impl PreparedSchema {
    /// Resolve ABI inputs and custom-table declarations before generating SQL.
    ///
    /// # Errors
    ///
    /// Missing ABI files, invalid event declarations, unresolved table types and
    /// ambiguous storage names are rejected without mutating the input indexer.
    pub fn try_new(
        project_path: &Path,
        indexer: &Indexer,
    ) -> Result<Self, GenerateTablesForIndexerSqlError> {
        if let Some(contract) = indexer
            .contracts
            .iter()
            .find(|contract| contract.name.eq_ignore_ascii_case(NATIVE_TRANSFER_CONTRACT_NAME))
        {
            return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                "contract name {:?} conflicts with the reserved native-transfer name {NATIVE_TRANSFER_CONTRACT_NAME:?}",
                contract.name
            )));
        }
        let mut prepared_indexer = indexer.clone();
        let mut contracts = Vec::with_capacity(indexer.contracts.len());
        for declaration in &indexer.contracts {
            let abi_text = declaration.parse_abi(project_path).map_err(ReadAbiError::from)?;
            contracts.push(PreparedContract::try_new(&indexer.name, declaration, &abi_text)?);
        }
        // Retain resolved types in the snapshot's logical declarations as well.
        // Names stay logical here: only PreparedContract::name is a runtime name.
        prepared_indexer.contracts =
            contracts.iter().map(|contract| contract.declaration.clone()).collect();
        let native = if indexer.native_transfers.enabled {
            let declaration = native_declaration(indexer);
            let prepared =
                PreparedContract::try_new(&indexer.name, &declaration, NATIVE_TRANSFER_ABI)?;
            prepared_indexer.native_transfers.tables = prepared.declaration.tables.clone();
            Some(prepared)
        } else {
            None
        };
        let schema = Self {
            indexer: prepared_indexer,
            contracts,
            native,
            postgres_admission: OnceLock::new(),
        };
        schema.validate_contract_identities()?;
        schema.validate_factory_references()?;
        Ok(schema)
    }

    /// The immutable logical indexer, including inferred custom-table types.
    pub fn indexer(&self) -> &Indexer {
        &self.indexer
    }

    pub fn contracts(&self) -> &[PreparedContract] {
        &self.contracts
    }

    pub fn native(&self) -> Option<&PreparedContract> {
        self.native.as_ref()
    }

    /// Find an exact logical or runtime name. No suffix stripping or filesystem
    /// lookup is performed, including for names containing the word `Filter`.
    pub fn contract(&self, name: &str) -> Result<&PreparedContract, String> {
        let mut matches = self
            .contracts
            .iter()
            .chain(self.native.iter())
            .filter(|contract| contract.name == name || contract.declaration.name == name);
        let found = matches.next().ok_or_else(|| {
            format!("contract {name:?} was not present when the storage schema was prepared")
        })?;
        if matches.next().is_some() {
            return Err(format!("contract name {name:?} is ambiguous in the prepared schema"));
        }
        Ok(found)
    }

    fn validate_factory_references(&self) -> Result<(), GenerateTablesForIndexerSqlError> {
        for contract in &self.contracts {
            for factory in
                contract.declaration.details.iter().filter_map(|detail| detail.factory.as_ref())
            {
                let source = self.contract(&factory.name).map_err(|reason| {
                    GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                        "factory {:?} for contract {:?} cannot be resolved: {reason}",
                        factory.name, contract.declaration.name
                    ))
                })?;
                if !source.events.iter().any(|event| event.info.name == factory.event_name) {
                    return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                        "factory event {}::{} for contract {:?} is absent from the prepared schema",
                        factory.name, factory.event_name, contract.declaration.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_contract_identities(&self) -> Result<(), GenerateTablesForIndexerSqlError> {
        let mut names = HashMap::new();
        for contract in self.contracts.iter().chain(self.native.iter()) {
            for name in [&contract.declaration.name, &contract.name] {
                if let Some(previous) = names.insert(name.as_str(), contract) {
                    if !std::ptr::eq(previous, contract) {
                        return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(
                            format!(
                                "contracts {:?} and {:?} share the logical or runtime name {name:?}",
                                previous.declaration.name, contract.declaration.name
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl PreparedContract {
    /// The complete parsed ABI retained from this startup's input snapshot.
    pub fn abi(&self) -> &JsonAbi {
        &self.abi
    }

    pub(crate) fn storage_column(
        &self,
        event_name: &str,
        logical_path: &str,
    ) -> Result<GetAbiItemWithDbMap, GetAbiItemWithDbMapError> {
        if !self.declaration.stores_raw_events(event_name) {
            return Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
                "Event {event_name} has no raw storage table for {}",
                self.declaration.name
            )));
        }
        let event =
            self.events.iter().find(|event| event.info.name == event_name).ok_or_else(|| {
                GetAbiItemWithDbMapError::ParameterNotFound(format!(
                    "Event {event_name} not found in the prepared ABI for {}",
                    self.declaration.name
                ))
            })?;
        let (input, storage_name) =
            event.columns.storage_column(logical_path).ok_or_else(|| {
                GetAbiItemWithDbMapError::ParameterNotFound(format!(
                    "Parameter {logical_path} is not a stored column in event {event_name}"
                ))
            })?;
        Ok(GetAbiItemWithDbMap {
            abi_item: input.clone(),
            db_column_name: storage_name.to_string(),
        })
    }

    fn try_new(
        indexer_name: &str,
        declaration: &Contract,
        abi_text: &str,
    ) -> Result<Self, GenerateTablesForIndexerSqlError> {
        let filter_count =
            declaration.details.iter().filter(|detail| detail.filter.is_some()).count();
        if filter_count > 0 && filter_count != declaration.details.len() {
            return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                "contract {:?} mixes address and filter details",
                declaration.name
            )));
        }
        let is_filter = filter_count > 0;
        let items: Vec<ABIItem> = serde_json::from_str(abi_text).map_err(ReadAbiError::from)?;
        // Standalone DDL/setup callers use PreparedSchema directly. Reject
        // missing sources here rather than silently preparing a partial schema.
        let referenced_events =
            declaration.include_events.iter().flatten().map(|event| &event.name).chain(
                declaration
                    .tables
                    .iter()
                    .flatten()
                    .flat_map(|table| table.events.iter().map(|mapping| &mapping.event)),
            );
        for event_name in referenced_events {
            if !items.iter().any(|item| item.type_ == "event" && &item.name == event_name) {
                return Err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                    "contract {:?} references event {event_name:?}, which is absent from its ABI",
                    declaration.name
                )));
            }
        }
        // Manifest admission needs the include/table selection before the
        // narrower runtime filter, including non-event ABI declarations.
        let items = ABIItem::select_contract_items(items, declaration, false);
        let runtime_items = ABIItem::select_contract_items(items.clone(), declaration, is_filter);
        let infos = ABIItem::extract_event_names_and_signatures_from_abi(runtime_items)?;
        let name = declaration.before_modify_name_if_filter_readonly().into_owned();
        let schema_name = generate_indexer_contract_schema_name(indexer_name, &name);
        let mut layouts = Vec::with_capacity(infos.len());
        for info in infos {
            let stores_raw = declaration.stores_raw_events(&info.name);
            let columns = Arc::new(if stores_raw {
                RawEventColumns::try_new(&info.name, &info.inputs).map_err(|reason| {
                    GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                        "{}: {reason}",
                        declaration.name
                    ))
                })?
            } else {
                RawEventColumns::for_unstored_event(&info.inputs)
            });
            let source = if stores_raw {
                RollbackEventSource::from_columns(&columns)
            } else {
                RollbackEventSource::default()
            };
            let raw_table =
                stores_raw.then(|| format!("{schema_name}.{}", camel_to_snake(&info.name)));
            let checkpoint_table = generate_internal_event_table_name(&schema_name, &info.name);
            layouts.push((info, columns, source, raw_table, checkpoint_table));
        }
        // Keep the original names and their storage collision in diagnostics,
        // even when Alloy would reject the same ABI's syntax first. Each layout
        // is frozen above and only its decoder is attached below.
        let abi: JsonAbi = serde_json::from_str(abi_text).map_err(ReadAbiError::from)?;
        let mut events = Vec::with_capacity(layouts.len());
        for (info, columns, source, raw_table, checkpoint_table) in layouts {
            let event = abi
                .events
                .get(&info.name)
                .and_then(|events| events.iter().find(|event| event.selector() == info.topic_id()))
                .ok_or_else(|| {
                    GenerateTablesForIndexerSqlError::InvalidPreparedSchema(format!(
                        "event {:?} was not found in the ABI for contract {:?}",
                        info.name, declaration.name
                    ))
                })?
                .clone();
            events.push(PreparedEvent {
                info,
                event,
                columns,
                source,
                raw_table,
                checkpoint_table,
            });
        }
        let event_types = events
            .iter()
            .map(|event| {
                (
                    event.info.name.clone(),
                    event
                        .info
                        .inputs
                        .iter()
                        .map(|input| (input.name.clone(), input.type_.clone()))
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut declaration = declaration.clone();
        let mut tables = Vec::new();
        if let Some(declarations) = declaration.tables.as_mut() {
            for table in declarations {
                table
                    .resolve_column_types(&event_types)
                    .map_err(GenerateTablesForIndexerSqlError::InvalidCustomTableSchema)?;
                warn_on_unusable_arithmetic_defaults(table, &format!("{name}.{}", table.name));
                tables.push(
                    TableRuntime::try_new(table, indexer_name, &name)
                        .map_err(GenerateTablesForIndexerSqlError::InvalidCustomTableSchema)?,
                );
            }
        }
        Ok(Self { declaration, name, schema_name, events, tables: Arc::new(tables), abi, items })
    }
}

fn native_declaration(indexer: &Indexer) -> Contract {
    let native = &indexer.native_transfers;
    Contract {
        name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
        details: native
            .networks
            .iter()
            .flatten()
            .map(|detail| ContractDetails {
                network: detail.network.clone(),
                address: None,
                filter: None,
                indexed_filters: None,
                factory: None,
                start_block: detail.start_block,
                end_block: detail.end_block,
            })
            .collect(),
        // Native transfers use the in-memory ABI above, never this path.
        abi: StringOrArray::Single(String::new()),
        include_events: Some(vec![ContractEvent {
            name: "NativeTransfer".to_string(),
            timestamps: None,
        }]),
        index_event_in_order: None,
        dependency_events: None,
        reorg_safe_distance: native.reorg_safe_distance,
        generate_csv: native.generate_csv,
        streams: native.streams.clone(),
        chat: native.chat.clone(),
        tables: native.tables.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        database::{
            clickhouse::generate::generate_prepared_tables_clickhouse,
            generate::generate_prepared_tables_sql,
        },
        manifest::native_transfer::NativeTransfers,
    };
    use serde_json::{json, Value};

    fn fixture(input: Value, filter: bool) -> (tempfile::TempDir, Indexer) {
        let dir = tempfile::tempdir().unwrap();
        let abi = json!([{
            "type":"event", "name":"Value", "anonymous":false,
            "inputs":[input]
        }]);
        std::fs::write(dir.path().join("abi.json"), serde_json::to_vec(&abi).unwrap()).unwrap();
        let detail = if filter {
            json!({"network":"ethereum","filter":{"event_name":"Value"}})
        } else {
            json!({"network":"ethereum","address":"0x0000000000000000000000000000000000000001"})
        };
        let contract = serde_json::from_value(json!({
            "name":"NameFilter", "abi":"abi.json", "include_events":["Value"],
            "details":[detail]
        }))
        .unwrap();
        (
            dir,
            Indexer {
                name: "Names".to_string(),
                contracts: vec![contract],
                native_transfers: NativeTransfers::default(),
            },
        )
    }

    #[test]
    fn prepared_schema_survives_abi_file_changes_for_both_databases() {
        let (dir, indexer) =
            fixture(json!({"name":"key16","type":"uint256","indexed":false}), false);
        let schema = PreparedSchema::try_new(dir.path(), &indexer).unwrap();
        // A startup must use the compiled declaration, even if the input file is
        // removed between schema synchronization and callback registration.
        std::fs::remove_file(dir.path().join("abi.json")).unwrap();
        let contract = schema.contract("NameFilter").unwrap();
        let mapped = contract.storage_column("Value", "key16").unwrap();
        assert_eq!(mapped.db_column_name, "key_16");
        assert_eq!(mapped.abi_item.name, "key16");
        assert!(contract.events[0].columns.column_names().contains(&"key_16".to_string()));
        let pg = generate_prepared_tables_sql(&schema, false).unwrap();
        let ch = generate_prepared_tables_clickhouse(&schema, false).unwrap();
        assert!(pg.as_str().contains("\"key_16\""));
        assert!(ch.as_str().contains("\"key_16\""));
    }

    #[test]
    fn prepared_schema_resolves_filter_identity_without_mutating_logical_name() {
        let (dir, indexer) =
            fixture(json!({"name":"key16","type":"uint256","indexed":false}), true);
        let schema = PreparedSchema::try_new(dir.path(), &indexer).unwrap();
        let logical = schema.contract("NameFilter").unwrap();
        let runtime = schema.contract("NameFilterFilter").unwrap();
        assert!(std::ptr::eq(logical, runtime));
        assert_eq!(logical.declaration.name, "NameFilter");
        assert_eq!(logical.name, "NameFilterFilter");
        assert_eq!(indexer.contracts[0].name, "NameFilter");
        assert_eq!(schema.indexer().contracts[0].name, "NameFilter");
        assert!(schema.contract("Name").is_err());
        assert!(schema.contract("NameFilterFilterFilter").is_err());
    }

    #[test]
    fn prepared_schema_checks_column_collisions_only_for_stored_events() {
        let (dir, mut indexer) =
            fixture(json!({"name":"key16","type":"uint256","indexed":false}), false);
        std::fs::write(
            dir.path().join("abi.json"),
            serde_json::to_vec(&json!([{
                "type":"event", "name":"Value", "anonymous":false,
                "inputs":[
                    {"name":"key16","type":"uint256","indexed":false},
                    {"name":"key_16","type":"uint256","indexed":false}
                ]
            }]))
            .unwrap(),
        )
        .unwrap();
        indexer.contracts[0].include_events = None;
        let schema = PreparedSchema::try_new(dir.path(), &indexer)
            .expect("nonstored events retain their logical stream fields");
        let prepared = schema.contract("NameFilter").unwrap();
        assert_eq!(prepared.events.len(), 1);
        assert!(prepared.events[0].source.types.is_empty());
        assert!(prepared.events[0].columns.postgres_definitions().is_empty());
        assert!(prepared.events[0].columns.clickhouse_definitions().is_empty());
        assert!(prepared.storage_column("Value", "key16").is_err());
        for ddl in [
            generate_prepared_tables_sql(&schema, false).unwrap(),
            generate_prepared_tables_clickhouse(&schema, false).unwrap(),
        ] {
            assert!(!ddl.as_str().contains("names_name_filter.value"));
        }

        for raw_include in [true, false] {
            let mut stored = indexer.clone();
            if raw_include {
                stored.contracts[0].include_events =
                    Some(vec![ContractEvent { name: "Value".to_string(), timestamps: None }]);
            } else {
                stored.contracts[0].tables = Some(vec![serde_json::from_value(json!({
                    "name":"totals", "global":true,
                    "columns":[{"name":"total","type":"uint256","default":"0"}],
                    "events":[{"event":"Value","operations":[{
                        "type":"upsert","set":[{"column":"total","action":"add","value":"$key16"}]
                    }]}]
                }))
                .unwrap()]);
            }
            let error = PreparedSchema::try_new(dir.path(), &stored)
                .expect_err("stored and table-driving events need unambiguous columns")
                .to_string();
            for name in ["key16", "key_16"] {
                assert!(error.contains(name), "{error}");
            }
        }
    }

    #[test]
    fn prepared_schema_rejects_reserved_contract_names_before_abi_io() {
        let (dir, mut indexer) =
            fixture(json!({"name":"key16","type":"uint256","indexed":false}), false);
        std::fs::remove_file(dir.path().join("abi.json")).unwrap();
        assert!(!indexer.native_transfers.enabled);
        // A different contract appears first; admission must scan all names
        // before trying that contract's now-missing ABI file.
        let reserved = indexer.contracts[0].clone();
        indexer.contracts.push(reserved);
        for name in [
            NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            NATIVE_TRANSFER_CONTRACT_NAME.to_ascii_lowercase(),
        ] {
            indexer.contracts[1].name = name.clone();
            let error = PreparedSchema::try_new(dir.path(), &indexer)
                .expect_err("native-transfer names are reserved even when traces are disabled")
                .to_string();
            assert!(error.contains(&name) && error.contains("reserved"), "{error}");
        }
    }

    #[test]
    fn prepared_schema_rejects_missing_stored_event_references() {
        let (dir, indexer) =
            fixture(json!({"name":"key16","type":"uint256","indexed":false}), false);
        for table_driven in [false, true] {
            let mut missing = indexer.clone();
            if table_driven {
                missing.contracts[0].include_events = None;
                missing.contracts[0].tables = Some(vec![serde_json::from_value(json!({
                    "name":"totals", "global":true,
                    "columns":[{"name":"total","type":"uint256","default":"0"}],
                    "events":[{"event":"Missing","operations":[{
                        "type":"upsert","set":[{"column":"total","action":"increment"}]
                    }]}]
                }))
                .unwrap()]);
            } else {
                missing.contracts[0].include_events =
                    Some(vec![ContractEvent { name: "Missing".to_string(), timestamps: None }]);
            }
            let error = PreparedSchema::try_new(dir.path(), &missing)
                .expect_err("an absent stored event cannot produce a complete schema")
                .to_string();
            assert!(error.contains("NameFilter") && error.contains("Missing"), "{error}");
        }
    }

    #[test]
    fn prepared_schema_rejects_missing_factory_sources() {
        let (dir, mut indexer) =
            fixture(json!({"name":"child","type":"address","indexed":false}), false);
        let mut factory = indexer.contracts[0].clone();
        factory.name = "Factory".to_string();
        indexer.contracts[0].details[0].address = None;
        indexer.contracts[0].details[0].factory = Some(
            serde_json::from_value(json!({
                "name":"Factory", "abi":"abi.json", "event_name":"Value", "input_name":"child",
                "address":"0x0000000000000000000000000000000000000001"
            }))
            .unwrap(),
        );
        let error = PreparedSchema::try_new(dir.path(), &indexer)
            .expect_err("factory registration requires its prepared discovery contract")
            .to_string();
        assert!(error.contains("Factory") && error.contains("NameFilter"), "{error}");

        indexer.contracts.push(factory);
        PreparedSchema::try_new(dir.path(), &indexer).expect("expanded factory sources are valid");
        indexer.contracts[0].details[0].factory.as_mut().unwrap().event_name =
            "Missing".to_string();
        let error = PreparedSchema::try_new(dir.path(), &indexer)
            .expect_err("factory registration requires an admitted discovery event")
            .to_string();
        assert!(error.contains("Factory::Missing") && error.contains("NameFilter"), "{error}");
    }

    #[test]
    fn prepared_schema_keeps_postgres_tuple_arrays_without_rendering_clickhouse() {
        let (dir, indexer) = fixture(
            json!({
                "name":"entries","type":"tuple[]","indexed":false,
                "components":[{"name":"key16","type":"uint256"}]
            }),
            false,
        );
        let schema = PreparedSchema::try_new(dir.path(), &indexer).unwrap();
        let pg = generate_prepared_tables_sql(&schema, false).unwrap();
        assert!(pg.as_str().contains("\"entries\" JSONB"));
    }
}

#[cfg(test)]
#[path = "postgres_identity_tests.rs"]
mod postgres_identity_tests;
