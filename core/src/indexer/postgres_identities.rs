//! Reserve the PostgreSQL names that setup and callbacks actually use.
//! Repeated network/address subscriptions of one declaration share its storage.
use super::PreparedSchema;
use crate::{
    database::{
        generate::{generate_internal_factory_event_table_name, GenerateTablesForIndexerSqlError},
        postgres::generate::{
            generate_internal_cron_table_name, GenerateInternalFactoryEventTableNameParams,
        },
    },
    helpers::{camel_to_snake, postgres_catalog_identifier},
};
use std::collections::HashMap;

#[derive(Default)]
struct Owners(HashMap<String, String>);

impl Owners {
    fn catalog_key(name: &str) -> String {
        name.to_ascii_lowercase()
            .split('.')
            .map(postgres_catalog_identifier)
            .collect::<Vec<_>>()
            .join(".")
    }

    fn claim(&mut self, name: &str, owner: String) -> Result<(), String> {
        // DDL uses bare schema/table identifiers. PostgreSQL folds ASCII case
        // and clips each component (not the entire qualified name) in its catalog.
        let key = Self::catalog_key(name);
        match self.0.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(owner);
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() != &owner => {
                return Err(format!(
                    "PostgreSQL storage collision at '{}': {} and {}. Choose distinct storage destinations.",
                    entry.key(), entry.get(), owner
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

impl PreparedSchema {
    /// Backend-specific admission is cached on the immutable snapshot, including failures.
    pub(crate) fn validate_postgres_identities(
        &self,
    ) -> Result<(), GenerateTablesForIndexerSqlError> {
        self.postgres_admission
            .get_or_init(|| self.check_postgres_owners())
            .clone()
            .map_err(GenerateTablesForIndexerSqlError::InvalidPreparedSchema)
    }

    fn check_postgres_owners(&self) -> Result<(), String> {
        let mut schemas = Owners::default();
        let mut relations = Owners::default();
        let mut raw_auxiliaries = HashMap::new();
        schemas.claim("rindexer_internal", "rindexer internal schema".into())?;
        let indexer = camel_to_snake(&self.indexer().name);
        for name in [
            "latest_block",
            "reorg_block_hashes",
            "derived_op_log",
            "event_applications",
            "event_application_streams",
            "stream_outbox",
        ] {
            relations.claim(
                &format!("rindexer_internal.{name}"),
                format!("rindexer internal table {name}"),
            )?;
        }
        for name in [
            "idx_derived_op_log_reorg",
            "event_applications_block_range",
            "stream_outbox_pending",
            "stream_outbox_recovery",
        ] {
            relations
                .claim(&format!("rindexer_internal.{name}"), format!("rindexer index {name}"))?;
        }
        for suffix in [
            "last_known_indexes_dropping_sql",
            "last_known_relationship_dropping_sql",
            "last_run_migrations_sql",
        ] {
            relations.claim(
                &format!("rindexer_internal.{indexer}_{suffix}"),
                format!("indexer metadata {suffix}"),
            )?;
        }
        for contract in self.contracts().iter().chain(self.native()) {
            let logical = &contract.declaration.name;
            schemas.claim(&contract.schema_name, format!("contract {logical}"))?;
            for event in &contract.events {
                let source = format!("{logical}::{}", event.info.name);
                if let Some(raw) = &event.raw_table {
                    relations.claim(raw, format!("raw event {source}"))?;
                    // Raw tables declare rindexer_id SERIAL PRIMARY KEY. A subsequent
                    // CREATE TABLE IF NOT EXISTS with either plain default name may
                    // silently accept the index or sequence instead of creating a table.
                    // Only check unshortened defaults; server-generated names clipped
                    // or disambiguated with numbers depend on catalog state.
                    let (_, table) = raw.split_once('.').expect("prepared raw table has a schema");
                    for (suffix, kind) in
                        [("_pkey", "primary key index"), ("_rindexer_id_seq", "serial sequence")]
                    {
                        if table.len() + suffix.len() <= 63 {
                            raw_auxiliaries
                                .entry(format!("{raw}{suffix}"))
                                .or_insert_with(|| format!("{kind} for raw event {source}"));
                        }
                    }
                }
                relations.claim(
                    &format!("rindexer_internal.{}", event.checkpoint_table),
                    format!("checkpoint for {source}"),
                )?;
            }
            for factory in
                contract.declaration.details.iter().filter_map(|detail| detail.factory.as_ref())
            {
                let inputs = factory.input_names();
                let name = generate_internal_factory_event_table_name(
                    &GenerateInternalFactoryEventTableNameParams {
                        indexer_name: self.indexer().name.clone(),
                        contract_name: factory.name.clone(),
                        event_name: factory.event_name.clone(),
                        input_names: inputs.clone(),
                    },
                );
                relations.claim(
                    &format!("rindexer_internal.{name}"),
                    format!("factory {}::{} {inputs:?}", factory.name, factory.event_name),
                )?;
            }
            for runtime in contract.tables.iter() {
                let table = runtime.table.definition();
                for (i, cron) in table.cron.iter().flatten().enumerate() {
                    if cron.start_block.is_some() {
                        let name = generate_internal_cron_table_name(
                            &contract.schema_name,
                            &table.name,
                            i,
                        );
                        relations.claim(
                            &format!("rindexer_internal.{name}"),
                            format!("cron {logical}::{}[{i}]", table.name),
                        )?;
                    }
                }
            }
        }
        // PostgreSQL disambiguates auxiliary-versus-auxiliary names. Only
        // disallow a raw, custom, or engine relation claiming an auxiliary.
        for (name, owner) in raw_auxiliaries {
            relations.claim(&name, owner)?;
        }
        // Non-recovered custom declarations may intentionally share a table.
        // Preserve that policy; a custom destination still cannot take a raw
        // or internal relation. Recovery separately validates custom writers.
        for contract in self.contracts().iter().chain(self.native()) {
            for runtime in contract.tables.iter() {
                let name = &runtime.full_table_name;
                let key = Owners::catalog_key(name);
                if let Some(owner) = relations.0.get(&key) {
                    return Err(format!(
                        "PostgreSQL storage collision at '{key}': {owner} and custom table {}::{}. Choose distinct storage destinations.",
                        contract.declaration.name, runtime.table.name()
                    ));
                }
            }
        }
        Ok(())
    }
}
