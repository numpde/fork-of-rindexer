//! Deterministic identity of one effective historical PostgreSQL cron computation.
//! This is a data certificate, not an RPC endpoint or a batching configuration.
use alloy::primitives::Address;
use serde_json::{json, Map, Value};

use crate::manifest::contract::{ResolvedTable, TableCronMapping};

#[derive(Clone, Debug)]
pub(crate) struct CronIdentity {
    pub fingerprint: String,
    pub descriptor: String,
}

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(entries) => {
            let mut sorted = Map::new();
            let entries = entries.into_iter().collect::<std::collections::BTreeMap<_, _>>();
            for (key, value) in entries {
                sorted.insert(key, canonical(value));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

/// Bind source, storage, ordered operations and sampling grid. Deliberately
/// exclude RPC URL, timing, logging, concurrency, and adaptive batch size.
pub(crate) struct CronIdentityInput<'a> {
    pub indexer: &'a str,
    pub contract: &'a str,
    pub table: &'a ResolvedTable,
    pub destination: &'a str,
    pub cron_index: usize,
    pub cron: &'a TableCronMapping,
    pub network: &'a str,
    pub address: Address,
    pub chain_id: Option<u64>,
}

/// Accept the complete resolved computation as one typed input, so every
/// consumer constructs the same descriptor without positional arguments.
pub(crate) fn identify(input: CronIdentityInput<'_>) -> Result<CronIdentity, String> {
    let CronIdentityInput {
        indexer,
        contract,
        table,
        destination,
        cron_index,
        cron,
        network,
        address,
        chain_id,
    } = input;
    let definition = table.definition();
    let start = cron.start_block.ok_or("Historical schedule has no start_block")?.to::<u64>();
    let end = cron.end_block.map(|value| value.to::<u64>());
    let block_interval = cron.block_interval.map(|value| value.to::<u64>()).unwrap_or(1);
    let operations = cron
        .operations
        .iter()
        .map(|operation| {
            let assignments = operation
                .set
                .iter()
                .map(|column| {
                    json!({
                        "column": column.column,
                        "action": column.action,
                        "value": column.effective_value()
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "type": operation.operation_type,
                "where": operation.where_clause,
                "set": assignments,
            })
        })
        .collect::<Vec<_>>();
    let document = canonical(json!({
        "identity_version": 1,
        "execution_semantics": "postgres-historical-cron-atomic-ordered-v2",
        "indexer": indexer,
        "contract": contract,
        "cron_index": cron_index,
        "network": network,
        "chain_id": chain_id,
        "address": format!("{address:#x}"),
        "destination": destination,
        "table": {
            "name": definition.name,
            "global": definition.global,
            "cross_chain": definition.cross_chain,
            "timestamp": definition.timestamp,
            "columns": definition.columns,
            "primary_key": definition.primary_key_columns(),
        },
        "sampling": {
            "start": start,
            "end": end,
            "interval": block_interval,
        },
        "operations": operations,
    }));
    let descriptor = serde_json::to_string(&document)
        .map_err(|error| format!("Cannot serialize historical cron schedule: {error}"))?;
    let fingerprint = format!("{:#x}", alloy::primitives::keccak256(descriptor.as_bytes()));
    Ok(CronIdentity { fingerprint, descriptor })
}

fn first_change(path: &str, former: &Value, current: &Value) -> Option<String> {
    if former == current {
        return None;
    }
    match (former, current) {
        (Value::Object(old), Value::Object(new)) => {
            for key in old.keys().chain(new.keys()).collect::<std::collections::BTreeSet<_>>() {
                let next = if path.is_empty() { key.to_string() } else { format!("{path}.{key}") };
                if old.get(key) != new.get(key) {
                    let a = old.get(key).unwrap_or(&Value::Null);
                    let b = new.get(key).unwrap_or(&Value::Null);
                    return first_change(&next, a, b);
                }
            }
            None
        }
        (Value::Array(old), Value::Array(new)) => {
            for index in 0..old.len().max(new.len()) {
                if old.get(index) != new.get(index) {
                    let a = old.get(index).unwrap_or(&Value::Null);
                    let b = new.get(index).unwrap_or(&Value::Null);
                    return first_change(&format!("{path}[{index}]"), a, b);
                }
            }
            None
        }
        _ => Some(format!("{path}: recorded {former}, requested {current}")),
    }
}

/// Return a compact and actionable mismatch rather than only two opaque hashes.
pub(crate) fn mismatch(stored: &str, expected: &CronIdentity) -> String {
    match (
        serde_json::from_str::<Value>(stored),
        serde_json::from_str::<Value>(&expected.descriptor),
    ) {
        (Ok(old), Ok(new)) => {
            first_change("", &old, &new).unwrap_or_else(|| "fingerprint or encoding differs".into())
        }
        _ => "recorded schedule descriptor is unreadable".into(),
    }
}
