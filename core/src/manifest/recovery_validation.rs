//! A recovered physical table needs one complete manifest write model.
//!
//! Resolve destinations before applying recovery rules, so separate contracts or
//! native tables cannot hide incompatible operations behind a database override.
use std::collections::{BTreeMap, HashSet};

use super::ValidateManifestError;
use crate::{
    database::generate::generate_table_full_name,
    indexer::native_transfer::NATIVE_TRANSFER_CONTRACT_NAME,
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
        for part in &parts {
            crate::indexer::reorg::validate_sql_identifier(part, "recovered table identifier")
                .map_err(|error| invalid(error.to_string()))?;
        }
        if target != target.to_ascii_lowercase() {
            return Err(invalid("use lowercase identifiers, including the database schema override, so quoted forward writes and bare recovery SQL address the same table".into()));
        }
    }
    let identity = parts
        .iter()
        .map(|part| {
            let identifier = part.trim_matches('"');
            let mut end = identifier.len().min(63);
            // PostgreSQL drops a character crossing its byte limit entirely.
            while !identifier.is_char_boundary(end) {
                end -= 1;
            }
            &identifier[..end]
        })
        .collect::<Vec<_>>()
        .join(".");
    targets.entry(identity).or_default().push(declaration);
    Ok(())
}

/// Validate all recovery restrictions once the runtime-expanded destinations are known.
/// One declaration may watch many addresses/networks; multiple declarations of a
/// recovered target remain unsupported until their complete histories can be combined.
pub(super) fn validate(
    manifest: &Manifest,
    contracts: &[Contract],
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
                networks.iter().any(|network| recovered_networks.contains(network.network.as_str()))
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
