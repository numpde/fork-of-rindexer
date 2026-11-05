use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use alloy::{
    primitives::{keccak256, B256},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};

use crate::database::clickhouse::generate::solidity_type_to_clickhouse_type;
use crate::database::sql_type_wrapper::{
    solidity_type_to_ethereum_sql_type_wrapper, EthereumSqlTypeWrapper,
};
use crate::{
    database::postgres::generate::solidity_type_to_db_type,
    helpers::camel_to_snake,
    manifest::contract::{Contract, ParseAbiError},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed: Option<bool>,

    pub name: String,

    #[serde(rename = "type")]
    pub type_: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub components: Option<Vec<ABIInput>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ParamTypeError {
    #[error("tuple type specified but no components found")]
    MissingComponents,
}

#[derive(PartialEq)]
pub enum GenerateAbiPropertiesType {
    PostgresWithDataTypes,
    PostgresColumnsNamesOnly,
    CsvHeaderNames,
    Object,
    ClickhouseWithDataTypes,
}

#[derive(Debug, Clone)]
pub struct AbiNamePropertiesPath {
    pub abi_type: String,
    pub abi_name: String,
}

impl AbiNamePropertiesPath {
    pub fn new(abi_name: &str, abi_type: &str) -> Self {
        Self { abi_type: abi_type.to_string(), abi_name: abi_name.to_string() }
    }
}

#[derive(Debug)]
pub struct AbiProperty {
    pub value: String,
    pub abi_type: String,
    pub abi_name: String,
    /// The path to the property, none if the property is at the root level.
    pub path: Option<Vec<AbiNamePropertiesPath>>,
    pub ethereum_sql_type_wrapper: Option<EthereumSqlTypeWrapper>,
}

impl AbiProperty {
    pub fn new(
        value: String,
        name: &str,
        abi_type: &str,
        path: Option<Vec<AbiNamePropertiesPath>>,
    ) -> Self {
        let has_array_in_path =
            path.as_ref().is_some_and(|p| p.iter().any(|pp| pp.abi_type.ends_with("[]")));
        let adjusted_abi_type =
            if has_array_in_path { format!("{abi_type}[]") } else { abi_type.to_string() };

        Self {
            value,
            ethereum_sql_type_wrapper: solidity_type_to_ethereum_sql_type_wrapper(
                &adjusted_abi_type,
            ),
            abi_type: abi_type.to_string(),
            abi_name: name.to_string(),
            path,
        }
    }
}

/// Project a normalized ABI leaf name into raw storage. ABI access paths and
/// virtual iterate aliases are separate identities and must not be passed here
/// after they have already been projected into a stored column name.
pub(crate) fn raw_event_abi_column_name(name: &str) -> String {
    use crate::database::generate::{RAW_EVENT_ID_COLUMN, RAW_EVENT_METADATA_COLUMNS};
    const PREFIX: &str = "rindexer_abi_";
    if name != RAW_EVENT_ID_COLUMN
        && !RAW_EVENT_METADATA_COLUMNS.contains(&name)
        && !crate::helpers::POSTGRES_SYSTEM_COLUMNS.contains(&name)
        && !name.starts_with(PREFIX)
    {
        return name.to_string();
    }
    let escaped = format!("{PREFIX}{name}");
    if escaped.len() <= 63 {
        return escaped;
    }
    // PostgreSQL truncates identifiers at 63 bytes. A fixed ASCII digest keeps
    // distinct long escaped names from losing their distinguishing suffixes.
    let mut bounded = format!("{PREFIX}{:x}", keccak256(name.as_bytes()));
    bounded.truncate(63);
    bounded
}

/// One physical raw column and its original ABI source. Non-indexed scalar tuples
/// flatten; tuple arrays and indexed complex values each remain a single leaf.
#[derive(Debug)]
pub(crate) struct RawAbiColumn<'a> {
    pub(crate) input: &'a ABIInput,
    pub(crate) storage_name: String,
    pub(crate) logical_path: String,
    object_name: String,
    parent_path: Option<Vec<AbiNamePropertiesPath>>,
}

fn raw_columns<'a>(
    inputs: &'a [ABIInput],
    path: Option<Vec<AbiNamePropertiesPath>>,
) -> Vec<RawAbiColumn<'a>> {
    fn visit<'a>(
        inputs: &'a [ABIInput],
        path: Option<Vec<AbiNamePropertiesPath>>,
        out: &mut Vec<RawAbiColumn<'a>>,
    ) {
        for input in inputs {
            if input.storage_type() == "tuple" {
                if let Some(components) = &input.components {
                    let mut next = path.clone().unwrap_or_default();
                    next.push(AbiNamePropertiesPath::new(&input.name, &input.type_));
                    visit(components, Some(next), out);
                    continue;
                }
            }
            let name = camel_to_snake(&input.name);
            let (normalized, object_name) = match &path {
                Some(parents) => {
                    let prefix = parents
                        .iter()
                        .map(|p| camel_to_snake(&p.abi_name))
                        .collect::<Vec<_>>()
                        .join("_");
                    (format!("{prefix}_{name}"), format!("{prefix}.{name}"))
                }
                None => (name.clone(), name),
            };
            let logical_path = path
                .iter()
                .flatten()
                .map(|p| p.abi_name.as_str())
                .chain(std::iter::once(input.name.as_str()))
                .collect::<Vec<_>>()
                .join(".");
            out.push(RawAbiColumn {
                input,
                storage_name: raw_event_abi_column_name(&normalized),
                logical_path,
                object_name,
                parent_path: path.clone(),
            });
        }
    }
    let mut out = Vec::new();
    visit(inputs, path, &mut out);
    out
}

/// Keep source/object identities separate from physical column identities. JSON
/// object keys retain their exact ABI spelling; hash leaves expose no components.
fn validate_abi_siblings(
    event_name: &str,
    inputs: &[ABIInput],
    parent: &str,
) -> Result<(), String> {
    let mut names = HashMap::new();
    for (index, input) in inputs.iter().enumerate() {
        let path =
            if parent.is_empty() { input.name.clone() } else { format!("{parent}.{}", input.name) };
        if let Some(previous) = names.insert(&input.name, index) {
            return Err(format!("Event {event_name:?} has duplicate ABI source path {path:?} at positions {previous} and {index}; give each input in this scope a unique name"));
        }
        if input.storage_type().split('[').next() == Some("tuple") {
            if let Some(components) = &input.components {
                validate_abi_siblings(event_name, components, &path)?;
            }
        }
    }
    Ok(())
}

/// Validate before collecting source types into a map: insertion there would
/// already have lost one side of a flattened or catalog-truncated collision.
pub(crate) fn validated_raw_event_columns<'a>(
    event_name: &str,
    inputs: &'a [ABIInput],
) -> Result<Vec<RawAbiColumn<'a>>, String> {
    validate_abi_siblings(event_name, inputs, "")?;
    let columns = raw_columns(inputs, None);
    let mut sources = HashMap::new();
    for column in &columns {
        let physical = crate::helpers::postgres_catalog_identifier(&column.storage_name);
        if let Some(previous) = sources.insert(physical, column.logical_path.as_str()) {
            return Err(format!("Event {event_name:?} ABI source paths {previous:?} and {:?} map to the same raw storage column {physical:?}; give the inputs distinct storage names",column.logical_path));
        }
    }
    Ok(columns)
}

pub(crate) fn validate_raw_event_columns(
    event_name: &str,
    inputs: &[ABIInput],
) -> Result<(), String> {
    validated_raw_event_columns(event_name, inputs).map(drop)
}

impl ABIInput {
    /// Indexed arrays and tuples expose only their topic digest. Their original
    /// Solidity type still defines the event signature, never the stored value.
    pub(crate) fn storage_type(&self) -> &str {
        if self.indexed == Some(true) && (self.type_ == "tuple" || self.type_.contains('[')) {
            "bytes32"
        } else {
            &self.type_
        }
    }

    pub fn format_param_type(&self) -> Result<String, ParamTypeError> {
        match self.type_.as_str() {
            "tuple" => {
                let components =
                    self.components.as_ref().ok_or(ParamTypeError::MissingComponents)?;
                let formatted_components = components
                    .iter()
                    .map(|component| component.format_param_type())
                    .collect::<Result<Vec<_>, ParamTypeError>>()?
                    .join(",");
                Ok(format!("({formatted_components})"))
            }
            _ => Ok(self.type_.to_string()),
        }
    }

    pub fn generate_abi_name_properties(
        inputs: &[ABIInput],
        properties_type: &GenerateAbiPropertiesType,
        path: Option<Vec<AbiNamePropertiesPath>>,
    ) -> Vec<AbiProperty> {
        raw_columns(inputs, path)
            .into_iter()
            .map(|column| {
                let storage_type = column.input.storage_type();
                let value = match properties_type {
                    GenerateAbiPropertiesType::PostgresWithDataTypes => format!(
                        "\"{}\" {}",
                        column.storage_name,
                        solidity_type_to_db_type(storage_type)
                    ),
                    GenerateAbiPropertiesType::PostgresColumnsNamesOnly
                    | GenerateAbiPropertiesType::CsvHeaderNames => column.storage_name,
                    GenerateAbiPropertiesType::Object => column.object_name,
                    GenerateAbiPropertiesType::ClickhouseWithDataTypes => format!(
                        "\"{}\" {}",
                        column.storage_name,
                        solidity_type_to_clickhouse_type(storage_type)
                    ),
                };
                AbiProperty::new(value, &column.input.name, storage_type, column.parent_path)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIItem {
    #[serde(default)]
    pub inputs: Vec<ABIInput>,

    #[serde(default)]
    pub name: String,

    #[serde(rename = "type", default)]
    pub type_: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ReadAbiError {
    #[error("Could not find ABI path: {0}")]
    AbiPathDoesNotExist(String),

    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("{0}")]
    ParseAbiError(#[from] ParseAbiError),
}

impl ABIItem {
    pub fn format_event_signature(&self) -> Result<String, ParamTypeError> {
        let name = &self.name;
        let params = self
            .inputs
            .iter()
            .map(Self::format_param_type)
            .collect::<Result<Vec<_>, _>>()?
            .join(",");

        Ok(format!("{name}({params})"))
    }

    fn format_param_type(input: &ABIInput) -> Result<String, ParamTypeError> {
        let base_type = input.type_.split('[').next().unwrap_or(&input.type_);
        let array_suffix = input.type_.strip_prefix(base_type).unwrap_or("");

        let type_str = match base_type {
            "tuple" => {
                let inner = input
                    .components
                    .as_ref()
                    .ok_or(ParamTypeError::MissingComponents)?
                    .iter()
                    .map(Self::format_param_type)
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",");
                format!("({inner})")
            }
            _ => base_type.to_string(),
        };

        Ok(format!("{type_str}{array_suffix}"))
    }

    pub fn extract_event_names_and_signatures_from_abi(
        abi_json: Vec<ABIItem>,
    ) -> Result<Vec<EventInfo>, ParamTypeError> {
        let mut events = Vec::new();
        for item in abi_json.into_iter() {
            if item.type_ == "event" {
                events.push(EventInfo::new(item)?);
            }
        }
        Ok(events)
    }

    pub fn read_abi_items(
        project_path: &Path,
        contract: &Contract,
    ) -> Result<Vec<ABIItem>, ReadAbiError> {
        let abi_str = contract.parse_abi(project_path)?;
        let abi_items: Vec<ABIItem> = serde_json::from_str(&abi_str)?;

        // Only stored or table-driving events need a raw layout. Unused ABI
        // declarations and non-event functions must not block a valid contract.
        for item in abi_items
            .iter()
            .filter(|item| item.type_ == "event" && contract.stores_raw_events(&item.name))
        {
            validate_raw_event_columns(&item.name, &item.inputs).map_err(|reason| {
                ParseAbiError::InvalidAbiFormat(format!("{}: {reason}", contract.name))
            })?;
        }

        // Get events from include_events
        let include_event_names: Vec<String> = contract
            .include_events
            .as_ref()
            .map(|events| events.iter().map(|a| a.name.clone()).collect())
            .unwrap_or_default();

        // Get events from tables
        let table_event_names = contract.get_table_event_names();

        // If both are empty, return all events (backward compatible behavior)
        if include_event_names.is_empty() && table_event_names.is_empty() {
            return Ok(abi_items);
        }

        // Combine both sets of event names
        let all_needed_events: HashSet<String> =
            include_event_names.into_iter().chain(table_event_names).collect();

        // Filter to only include needed events (keep all non-event items)
        let filtered_abi_items = abi_items
            .into_iter()
            .filter(|item| item.type_ != "event" || all_needed_events.contains(&item.name))
            .collect();

        Ok(filtered_abi_items)
    }

    pub fn get_abi_items(
        project_path: &Path,
        contract: &Contract,
        is_filter: bool,
    ) -> Result<Vec<ABIItem>, ReadAbiError> {
        let mut abi_items = ABIItem::read_abi_items(project_path, contract)?;

        if is_filter {
            let filter_event_names: HashSet<String> = contract
                .details
                .iter()
                .filter_map(|detail| detail.filter.clone())
                .flat_map(|events| match events {
                    ValueOrArray::Value(event) => vec![event.event_name],
                    ValueOrArray::Array(event_array) => {
                        event_array.into_iter().map(|e| e.event_name).collect()
                    }
                })
                .collect();

            abi_items = abi_items
                .iter()
                .filter(|item| item.type_ == "event" && filter_event_names.contains(&item.name))
                .cloned()
                .collect();
        }

        Ok(abi_items)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventInfo {
    pub name: String,
    pub inputs: Vec<ABIInput>,
    signature: String,
    struct_result: String,
    struct_data: String,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateCsvFileForEvent {
    #[error("Could not create the dir {0}")]
    CreateDirFailed(std::io::Error),
}

impl EventInfo {
    pub fn new(item: ABIItem) -> Result<Self, ParamTypeError> {
        let struct_result = format!("{}Result", item.name);
        let struct_data = format!("{}Data", item.name);
        let signature = item.format_event_signature()?;
        Ok(EventInfo {
            name: item.name,
            inputs: item.inputs,
            signature,
            struct_result,
            struct_data,
        })
    }

    pub fn topic_id(&self) -> B256 {
        let event_signature = self.signature.clone();

        keccak256(event_signature)
    }

    pub fn topic_id_as_hex_string(&self) -> String {
        format!("{:x}", self.topic_id())
    }

    pub fn struct_result(&self) -> &str {
        &self.struct_result
    }

    pub fn struct_data(&self) -> &str {
        &self.struct_data
    }

    pub fn csv_headers_for_event(&self) -> Vec<String> {
        let mut headers: Vec<String> = ABIInput::generate_abi_name_properties(
            &self.inputs,
            &GenerateAbiPropertiesType::CsvHeaderNames,
            None,
        )
        .into_iter()
        .map(|m| m.value)
        .collect();

        use crate::database::generate::RAW_EVENT_METADATA_COLUMNS;
        headers.insert(0, RAW_EVENT_METADATA_COLUMNS[0].to_string());
        // Preserve CSV's existing metadata layout, which omits block_timestamp.
        headers.extend(
            RAW_EVENT_METADATA_COLUMNS[1..]
                .iter()
                .filter(|name| **name != "block_timestamp")
                .map(|name| name.to_string()),
        );

        headers
    }

    pub fn create_csv_file_for_event(
        &self,
        project_path: &Path,
        contract_name: &str,
        csv_path: &str,
    ) -> Result<String, CreateCsvFileForEvent> {
        let csv_file_name = format!("{}-{}.csv", contract_name, self.name).to_lowercase();
        let csv_folder = project_path.join(csv_path).join(contract_name);

        // Create directory if it does not exist.
        if let Err(e) = fs::create_dir_all(&csv_folder) {
            return Err(CreateCsvFileForEvent::CreateDirFailed(e));
        }

        // Create last-synced-blocks if it does not exist.
        if let Err(e) = fs::create_dir_all(csv_folder.join("last-synced-blocks")) {
            return Err(CreateCsvFileForEvent::CreateDirFailed(e));
        }

        Ok(csv_folder.join(csv_file_name).display().to_string())
    }
}

pub struct GetAbiItemWithDbMap {
    pub abi_item: ABIInput,
    pub db_column_name: String,
}

#[derive(thiserror::Error, Debug)]
pub enum GetAbiItemWithDbMapError {
    #[error("Parameter not found: {0}")]
    ParameterNotFound(String),

    #[error("{0}")]
    InvalidRawColumns(String),
}

pub fn get_abi_item_with_db_map(
    abi_items: &[ABIItem],
    event_name: &str,
    parameter_mapping: &[&str],
) -> Result<GetAbiItemWithDbMap, GetAbiItemWithDbMapError> {
    let event = abi_items
        .iter()
        .find(|item| item.type_ == "event" && item.name == event_name)
        .ok_or_else(|| {
            GetAbiItemWithDbMapError::ParameterNotFound(format!(
                "Event {event_name} not found in contract ABI"
            ))
        })?;
    let columns = validated_raw_event_columns(event_name, &event.inputs)
        .map_err(GetAbiItemWithDbMapError::InvalidRawColumns)?;
    columns
        .into_iter()
        .find(|column| {
            column
                .parent_path
                .iter()
                .flatten()
                .map(|p| p.abi_name.as_str())
                .chain(std::iter::once(column.input.name.as_str()))
                .eq(parameter_mapping.iter().copied())
        })
        .map(|column| GetAbiItemWithDbMap {
            abi_item: column.input.clone(),
            db_column_name: column.storage_name,
        })
        .ok_or_else(|| {
            GetAbiItemWithDbMapError::ParameterNotFound(format!(
                "Parameter {} is not a stored column in event {event_name}",
                parameter_mapping.join(".")
            ))
        })
}

#[cfg(test)]
#[path = "abi_column_identity_tests.rs"]
mod storage_column_identity_tests;
