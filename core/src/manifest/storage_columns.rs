//! Resolve a logical table declaration to immutable physical storage identifiers.
use std::collections::HashMap;

use super::{
    injected_columns as injected, Table, TableColumn, TableCronMapping, TableEventMapping,
};
use crate::{
    event::{parse_filter_expression, VariableSource},
    helpers::camel_to_snake,
};

/// A table ready for SQL generation. Event paths, iterator names and literals
/// retain their ABI/manifest spelling; only destination identifiers are projected.
///
/// A physical table must not be submitted as another logical declaration: a
/// second projection can turn the stored column `u256` into `u_256`.
///
/// ```compile_fail,E0308
/// use rindexer::{indexer::tables::TableRuntime, manifest::contract::ResolvedTable};
///
/// fn register_again(resolved: &ResolvedTable) {
///     let _ = TableRuntime::try_new(resolved, "indexer", "Contract");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    table: Table,
    storage_columns: HashMap<String, String>,
}

const METADATA: &[&str] = &[
    "network",
    injected::RINDEXER_ID,
    injected::RINDEXER_SEQUENCE_ID,
    injected::BLOCK_NUMBER,
    injected::BLOCK_TIMESTAMP,
    injected::TX_HASH,
    injected::BLOCK_HASH,
    injected::CONTRACT_ADDRESS,
];

impl Table {
    /// Project a logical declaration once, before passing it to storage users.
    pub fn try_resolve_storage_columns(&self) -> Result<ResolvedTable, String> {
        let mut table = self.clone();
        let mut names = HashMap::with_capacity(self.columns.len());
        let mut owners = HashMap::with_capacity(self.columns.len());
        for (column, stored) in self.columns.iter().zip(&mut table.columns) {
            let logical = &column.name;
            let invalid =
                |reason: &str| format!("Table '{}' column '{logical}': {reason}", self.name);
            if !logical.is_ascii() {
                return Err(invalid("use an ASCII storage identifier"));
            }
            let physical = camel_to_snake(logical);
            if physical.len() > 63 {
                return Err(invalid(&format!(
                    "physical column '{physical}' exceeds the 63-byte PostgreSQL identifier limit"
                )));
            }
            if !physical.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                return Err(invalid(&format!(
                    "physical column '{physical}' must begin with a letter or underscore"
                )));
            }
            if METADATA.contains(&physical.as_str())
                || physical.starts_with("rindexer_condition_")
                || crate::helpers::POSTGRES_SYSTEM_COLUMNS.contains(&physical.as_str())
            {
                return Err(invalid(&format!(
                    "physical column '{physical}' is reserved; choose a distinct destination"
                )));
            }
            if let Some(previous) = owners.insert(physical.clone(), logical) {
                return Err(invalid(&format!("'{previous}' and '{logical}' both map to physical column '{physical}'; choose distinct destinations")));
            }
            names.insert(logical.clone(), physical.clone());
            stored.name = physical;
        }
        let destination = |logical: &str| {
            names.get(logical).cloned().ok_or_else(|| format!("Table '{}' references undeclared destination column '{logical}'; use its manifest name", self.name))
        };
        for op in table
            .events
            .iter_mut()
            .flat_map(|event| &mut event.operations)
            .chain(table.cron.iter_mut().flatten().flat_map(|cron| &mut cron.operations))
        {
            op.where_clause = std::mem::take(&mut op.where_clause)
                .into_iter()
                .map(|(key, value)| Ok((destination(&key)?, value)))
                .collect::<Result<_, String>>()?;
            for assignment in &mut op.set {
                assignment.column = destination(&assignment.column)?;
            }
            // `if` wins over the legacy `filter`; an ignored legacy expression
            // is not an executable reference and must not gain new validation.
            if let Some(condition) = op.if_condition.as_mut().or(op.filter.as_mut()) {
                if condition.contains('@') {
                    *condition = project_condition(condition, &names, &self.name)?;
                }
            }
        }
        Ok(ResolvedTable { table, storage_columns: names })
    }
}

// The filter parser borrows identifier tokens directly from `input`. Replace
// those exact ranges in reverse order: quoted literals, $event references and
// path accessors can contain the same spelling and must not be rewritten.
fn project_condition(
    input: &str,
    names: &HashMap<String, String>,
    table: &str,
) -> Result<String, String> {
    let expression = parse_filter_expression(input)
        .map_err(|e| format!("Invalid condition on table '{table}': {e}"))?;
    let mut ranges = Vec::new();
    let mut error = None;
    expression.visit_variables(&mut |variable| {
        if variable.source() != VariableSource::Table {
            return;
        }
        let logical = variable.base_name();
        if let Some(physical) = names.get(logical) {
            let start = logical.as_ptr() as usize - input.as_ptr() as usize;
            ranges.push((start..start + logical.len(), physical));
        } else if !METADATA.contains(&logical) {
            error = Some(format!(
                "Table '{table}' guard references undeclared destination column '@{logical}'"
            ));
        }
    });
    if let Some(e) = error {
        return Err(e);
    }
    let mut projected = input.to_string();
    ranges.sort_unstable_by_key(|(range, _)| std::cmp::Reverse(range.start));
    for (range, physical) in ranges {
        projected.replace_range(range, physical);
    }
    Ok(projected)
}

impl ResolvedTable {
    /// The logical table name (schema qualification is assigned by the runtime).
    pub fn name(&self) -> &str {
        &self.table.name
    }

    /// Columns with their physical storage names.
    pub fn columns(&self) -> &[TableColumn] {
        &self.table.columns
    }

    /// Event mappings with resolved destination columns, ready for SQL writers.
    pub fn event_mappings(&self) -> &[TableEventMapping] {
        &self.table.events
    }

    /// Cron mappings with resolved destination columns, ready for SQL writers.
    pub fn cron_mappings(&self) -> &[TableCronMapping] {
        self.table.cron.as_deref().unwrap_or_default()
    }

    /// The physical definition is exclusively for internal consumers of stored
    /// names. Never pass it to a declaration resolver.
    pub(crate) fn definition(&self) -> &Table {
        &self.table
    }

    /// Return the physical name for an exact user-declared logical column.
    pub fn storage_column(&self, logical: &str) -> Option<&str> {
        self.storage_columns.get(logical).map(String::as_str)
    }
}
