//! A recovered physical table needs one complete manifest write model.
//!
//! Resolve destinations before applying recovery rules, so separate contracts or
//! native tables cannot hide incompatible operations behind a database override.
use std::collections::{BTreeMap, HashSet};

use super::ValidateManifestError;
use crate::{
    database::generate::generate_table_full_name,
    indexer::{
        native_transfer::NATIVE_TRANSFER_CONTRACT_NAME,
        schema::{PreparedContract, PreparedSchema},
    },
    manifest::{
        contract::{Contract, Table},
        core::Manifest,
    },
};

struct Declaration<'a> {
    owner: &'a str,
    table: &'a Table,
    recovery_enabled: bool,
}

/// Match the forward writer's quoted names and PostgreSQL catalog truncation.
/// Recovered names additionally must agree with the bare identifiers in recovery
/// SQL; other writers still participate when quoting hides a shared destination.
fn register<'a>(
    targets: &mut BTreeMap<String, Vec<Declaration<'a>>>,
    target: String,
    declaration: Declaration<'a>,
) -> Result<(), ValidateManifestError> {
    let invalid = |reason: String| {
        ValidateManifestError::CustomIndexingValidationError(
            format!("Cannot resolve table {target:?} for recovery validation: {reason}"),
            declaration.owner.to_string(),
        )
    };
    let parts = target.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(invalid("expected one schema.table destination; database must name a single schema, without database qualification".into()));
    }
    if declaration.recovery_enabled {
        crate::indexer::reorg::validate_recovered_table_name(&target)
            .map_err(|error| invalid(error.to_string()))?;
    }
    let identity = crate::indexer::reorg::recovery_table_identity(&target);
    targets.entry(identity).or_default().push(declaration);
    Ok(())
}

fn prepared_tables(prepared: &PreparedContract) -> impl Iterator<Item = (&Table, String)> {
    // Keep the logical columns for validation/diagnostics and the writer's resolved destination.
    prepared
        .declaration
        .tables
        .iter()
        .flatten()
        .zip(prepared.tables.iter())
        .map(|(table, runtime)| (table, runtime.full_table_name.clone()))
}

fn declared_tables<'a>(
    indexer: &'a str,
    contract: &'a str,
    tables: &'a Option<Vec<Table>>,
) -> impl Iterator<Item = (&'a Table, String)> {
    tables.iter().flatten().map(move |table| {
        (table, generate_table_full_name(indexer, contract, &table.name, table.database.as_deref()))
    })
}

struct RecoveryScope<'a> {
    postgres: bool,
    recovered: HashSet<&'a str>,
}

impl RecoveryScope<'_> {
    fn register_tables<'a>(
        &self,
        targets: &mut BTreeMap<String, Vec<Declaration<'a>>>,
        owner: &'a str,
        networks: &[&str],
        tables: impl Iterator<Item = (&'a Table, String)>,
    ) -> Result<(), ValidateManifestError> {
        let recovered = networks.iter().copied().find(|network| self.recovered.contains(network));
        for (table, target) in tables {
            if self.postgres {
                for (index, cron) in table.cron.iter().flatten().enumerate() {
                    if cron.operations.is_empty() {
                        continue;
                    }
                    let cron_networks = match cron.network.as_deref() {
                        Some(network) => vec![network],
                        None => networks.to_vec(),
                    };
                    for network in cron_networks {
                        let reason = if self.recovered.contains(network) {
                            format!("its network {network:?} has reorg recovery enabled")
                        } else if let Some(other) =
                            recovered.filter(|_| table.cross_chain && !table.events.is_empty())
                        {
                            format!("its network {network:?} shares cross_chain rows with recovered event writes on {other:?}")
                        } else {
                            continue;
                        };
                        return Err(ValidateManifestError::CustomIndexingValidationError(
                            format!(
                                "Table {owner}.{} cron[{index}] writes to PostgreSQL table {target:?}, but {reason}. Recovery cannot undo scheduled writes. Remove this cron writer or keep its rows on a network without reorg recovery.",
                                table.name
                            ),
                            owner.to_owned(),
                        ));
                    }
                }
            }
            register(
                targets,
                target,
                Declaration { owner, table, recovery_enabled: recovered.is_some() },
            )?;
        }
        Ok(())
    }
}

/// Validate all recovery restrictions once the runtime-expanded destinations are known.
/// One declaration may watch many addresses/networks; multiple declarations of a
/// recovered target remain unsupported until their complete histories can be combined.
pub(super) fn validate(
    manifest: &Manifest,
    contracts: &[Contract],
    schema: Option<&PreparedSchema>,
) -> Result<(), ValidateManifestError> {
    let scope = RecoveryScope {
        postgres: manifest.storage.postgres_enabled(),
        recovered: manifest
            .networks
            .iter()
            .filter(|network| network.reorg_handling.as_ref().is_some_and(|config| config.enabled))
            .map(|network| network.name.as_str())
            .collect(),
    };
    if scope.recovered.is_empty() {
        return Ok(());
    }

    let native_networks = || {
        manifest.native_transfers.networks.as_ref().map_or_else(
            || manifest.networks.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
            |details| details.iter().map(|d| d.network.as_str()).collect(),
        )
    };
    let mut targets = BTreeMap::new();
    if let Some(schema) = schema {
        for prepared in schema.contracts() {
            let contract = &prepared.declaration;
            let networks: Vec<_> = contract.details.iter().map(|d| d.network.as_str()).collect();
            scope.register_tables(
                &mut targets,
                &contract.name,
                &networks,
                prepared_tables(prepared),
            )?;
        }
        if let Some(native) = schema.native() {
            scope.register_tables(
                &mut targets,
                "native_transfers",
                &native_networks(),
                prepared_tables(native),
            )?;
        }
    } else {
        for contract in contracts {
            let networks: Vec<_> = contract.details.iter().map(|d| d.network.as_str()).collect();
            scope.register_tables(
                &mut targets,
                &contract.name,
                &networks,
                declared_tables(&manifest.name, &contract.name, &contract.tables),
            )?;
        }
        if manifest.native_transfers.enabled {
            scope.register_tables(
                &mut targets,
                "native_transfers",
                &native_networks(),
                declared_tables(
                    &manifest.name,
                    NATIVE_TRANSFER_CONTRACT_NAME,
                    &manifest.native_transfers.tables,
                ),
            )?;
        }
    }
    for (target, declarations) in targets {
        if !declarations.iter().any(|declaration| declaration.recovery_enabled) {
            continue;
        }
        let first = &declarations[0];
        if declarations.len() != 1 {
            let owners = declarations
                .iter()
                .map(|declaration| format!("{}.{}", declaration.owner, declaration.table.name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ValidateManifestError::CustomIndexingValidationError(
                format!("Shared table {target:?} has multiple manifest declarations ({owners}), but recovery requires one complete table owner. Consolidate the write model into one Table declaration, or use distinct physical destinations. Separate declarations cannot establish compatible assignment, arithmetic, insert or journal histories."),
                first.owner.to_string(),
            ));
        }
        first.table.validate_reorg_operations().map_err(|error| {
            ValidateManifestError::CustomIndexingValidationError(
                format!("Recovery rules for physical table {target:?}: {error}"),
                first.owner.to_string(),
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prepared_recovery_checks_filter_runtime_destinations() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("abi.json"),
            r#"[{"type":"event","name":"Value","anonymous":false,"inputs":[]}]"#,
        )
        .unwrap();
        let table = json!({
            "name":"totals", "global":true,
            "columns":[{"name":"total","type":"uint256","default":"0"}],
            "events":[{"event":"Value","operations":[{
                "type":"upsert","set":[{"column":"total","action":"increment"}]
            }]}]
        });
        for (database, shared) in [("names_token_filter", true), ("names_token", false)] {
            let mut second_table = table.clone();
            second_table["database"] = json!(database);
            let manifest: Manifest = serde_json::from_value(json!({
                "name":"Names", "project_type":"no-code",
                "storage":{"postgres":{"enabled":true}},
                "networks":[{
                    "name":"test","chain_id":1,"rpc":"http://unused.invalid",
                    "reorg_handling":{"enabled":true}
                }],
                "contracts":[
                    {
                        "name":"Token","abi":"abi.json",
                        "details":[{"network":"test","filter":{"event_name":"Value"}}],
                        "tables":[table.clone()]
                    },
                    {
                        "name":"Second","abi":"abi.json",
                        "details":[{"network":"test","address":"0x0000000000000000000000000000000000000001"}],
                        "tables":[second_table]
                    }
                ]
            }))
            .unwrap();
            let indexer = manifest.to_indexer();
            let schema = PreparedSchema::try_new(dir.path(), &indexer).unwrap();
            let first_target = &schema.contract("Token").unwrap().tables[0].full_table_name;
            let second_target = &schema.contract("Second").unwrap().tables[0].full_table_name;
            assert_eq!(first_target, "names_token_filter.totals");
            assert_eq!(first_target == second_target, shared);
            let result = validate(&manifest, &indexer.contracts, Some(&schema));
            if shared {
                let error =
                    result.expect_err("two writers share the filter's physical table").to_string();
                for expected in [
                    "names_token_filter.totals",
                    "Token.totals",
                    "Second.totals",
                    "multiple manifest declarations",
                ] {
                    assert!(error.contains(expected), "missing {expected:?}: {error}");
                }
            } else {
                result.expect("the logical unsuffixed schema is a separate destination");
            }
        }
    }
}

#[cfg(test)]
#[path = "cron_recovery_tests.rs"]
mod cron_recovery_tests;
