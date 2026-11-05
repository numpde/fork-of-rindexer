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

fn register_prepared_contract<'a>(
    targets: &mut BTreeMap<String, Vec<Declaration<'a>>>,
    prepared: &'a PreparedContract,
    owner: &'a str,
    recovery_enabled: bool,
) -> Result<(), ValidateManifestError> {
    // Preparation retains these in declaration order. Keep the logical table for
    // diagnostics and operation validation, and the writer's exact destination.
    for (table, runtime) in prepared.declaration.tables.iter().flatten().zip(prepared.tables.iter())
    {
        register(
            targets,
            runtime.full_table_name.clone(),
            Declaration { owner, table, recovery_enabled },
        )?;
    }
    Ok(())
}

/// Validate all recovery restrictions once the runtime-expanded destinations are known.
/// One declaration may watch many addresses/networks; multiple declarations of a
/// recovered target remain unsupported until their complete histories can be combined.
pub(super) fn validate(
    manifest: &Manifest,
    contracts: &[Contract],
    schema: Option<&PreparedSchema>,
) -> Result<(), ValidateManifestError> {
    let recovered_networks = manifest
        .networks
        .iter()
        .filter(|network| network.reorg_handling.as_ref().is_some_and(|config| config.enabled))
        .map(|network| network.name.as_str())
        .collect::<HashSet<_>>();
    if recovered_networks.is_empty() {
        return Ok(());
    }
    let mut targets = BTreeMap::new();
    if let Some(schema) = schema {
        for prepared in schema.contracts() {
            let contract = &prepared.declaration;
            let recovery_enabled = contract
                .details
                .iter()
                .any(|detail| recovered_networks.contains(detail.network.as_str()));
            register_prepared_contract(&mut targets, prepared, &contract.name, recovery_enabled)?;
        }
        if let Some(native) = schema.native() {
            let recovery_enabled = manifest.native_transfers.networks.as_ref().map_or(
                !recovered_networks.is_empty(),
                |networks| {
                    networks
                        .iter()
                        .any(|network| recovered_networks.contains(network.network.as_str()))
                },
            );
            register_prepared_contract(&mut targets, native, "native_transfers", recovery_enabled)?;
        }
    } else {
        for contract in contracts {
            let recovery_enabled = contract
                .details
                .iter()
                .any(|detail| recovered_networks.contains(detail.network.as_str()));
            for table in contract.tables.iter().flatten() {
                let target = generate_table_full_name(
                    &manifest.name,
                    &contract.name,
                    &table.name,
                    table.database.as_deref(),
                );
                register(
                    &mut targets,
                    target,
                    Declaration { owner: &contract.name, table, recovery_enabled },
                )?;
            }
        }
        if manifest.native_transfers.enabled {
            let recovery_enabled = manifest.native_transfers.networks.as_ref().map_or(
                !recovered_networks.is_empty(),
                |networks| {
                    networks
                        .iter()
                        .any(|network| recovered_networks.contains(network.network.as_str()))
                },
            );
            for table in manifest.native_transfers.tables.iter().flatten() {
                let target = generate_table_full_name(
                    &manifest.name,
                    NATIVE_TRANSFER_CONTRACT_NAME,
                    &table.name,
                    table.database.as_deref(),
                );
                register(
                    &mut targets,
                    target,
                    Declaration { owner: "native_transfers", table, recovery_enabled },
                )?;
            }
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
