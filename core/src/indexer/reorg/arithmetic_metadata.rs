//! Recover replay ordering from the same canonical inputs used for arithmetic reversal.
use super::*;

impl ReorgTask {
    pub(super) async fn restore_arithmetic_metadata(
        &self,
        pg: &PgTransaction<'_>,
        plan: &DerivedTableRollbackPlan,
        provider: Option<&Arc<dyn ChainProvider>>,
    ) -> anyhow::Result<()> {
        use crate::manifest::contract::injected_columns::*;
        let empty_types = HashMap::new();
        for table in self.derived_tables.iter().filter(|table| !table.rollback_ops.is_empty()) {
            let mut names = vec![
                RINDEXER_SEQUENCE_ID,
                BLOCK_NUMBER,
                BLOCK_TIMESTAMP,
                TX_HASH,
                BLOCK_HASH,
                CONTRACT_ADDRESS,
            ];
            if table.cross_chain {
                names.push("network");
            }
            let columns = pg.query(
                "SELECT attname::text FROM pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped AND attname::text = ANY($2)",
                &[&table.full_table_name, &names],
            ).await?.into_iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>();
            if columns.is_empty() {
                continue;
            }
            // A shared row can be written by callbacks on either network. Keep its
            // canonical candidates and restored metadata in the same locked snapshot.
            if table.cross_chain {
                pg.batch_execute(&format!(
                    "LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE",
                    table.full_table_name
                ))
                .await?;
            }
            let scope = if table.cross_chain {
                String::new()
            } else {
                format!(" AND dt.network = '{}'", self.network)
            };
            let affected = format!("dt.rindexer_block_number >= {}{scope}", self.fork_point);
            let mut candidates = Vec::new();
            for (index, operation) in table.rollback_ops.iter().enumerate() {
                let metadata = plan.operation(&table.full_table_name, index);
                let source = ReversalSource {
                    dialect: SqlDialect::Postgres,
                    iterate: metadata.map_or(&[], |metadata| metadata.iterate.as_slice()),
                    source_column_types: metadata
                        .map_or(&empty_types, |metadata| &metadata.source_column_types),
                };
                let keys = operation
                    .where_columns
                    .iter()
                    .map(|(derived, event)| {
                        let value = source.operand(event)?;
                        let value = match metadata
                            .and_then(|metadata| metadata.derived_column_types.get(derived))
                        {
                            Some(column_type) => SqlDialect::Postgres.cast(value, column_type),
                            None => value,
                        };
                        Ok(format!("'{derived}', {value}"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?
                    .join(", ");
                let key = format!("jsonb_build_object({keys})");
                let network = if table.cross_chain {
                    String::new()
                } else {
                    format!(" AND _rindexer_event.network = '{}'", self.network)
                };
                let condition = operation
                    .condition
                    .as_ref()
                    .map(|condition| {
                        render_reorg_condition(condition, &source)
                            .map(|condition| format!(" AND ({condition})"))
                    })
                    .transpose()?
                    .unwrap_or_default();
                // Keep all 128 position bits in PostgreSQL NUMERIC, and use a
                // string in JSON so no floating-point decoder can round it.
                // Ordering is needed even when an older target exposes fewer fields.
                let mut fields = vec!["'network', _rindexer_event.network".into(), format!("'{RINDEXER_SEQUENCE_ID}', (_rindexer_event.block_number::numeric * 18446744073709551616 + _rindexer_event.tx_index::numeric * 4294967296 + _rindexer_event.log_index::numeric)::text")];
                fields.extend(
                    columns
                        .iter()
                        .filter(|name| {
                            name.as_str() != RINDEXER_SEQUENCE_ID && name.as_str() != "network"
                        })
                        .map(|name| format!("'{name}', {}", source.column(name))),
                );
                let fields = fields.join(", ");
                candidates.push(format!(
                    "SELECT {key} AS where_key, jsonb_build_object({fields}) AS row_metadata FROM {} WHERE EXISTS (SELECT 1 FROM {} dt WHERE {affected} AND to_jsonb(dt) @> {key}){network}{condition}",
                    source.from(&operation.event_table), table.full_table_name,
                ));
            }
            // A mixed table's newest canonical contributor can be a set/max/min
            // operation. Its already-recorded metadata participates in the same
            // selection, rather than overwriting the arithmetic choice afterward.
            if !table.journal_columns.is_empty() {
                let canonical = if table.cross_chain {
                    format!("(network <> '{}' OR block_number < {})", self.network, self.fork_point)
                } else {
                    format!("network = '{}' AND block_number < {}", self.network, self.fork_point)
                };
                candidates.push(format!("SELECT where_key, row_metadata || jsonb_build_object('network', network) AS row_metadata FROM rindexer_internal.derived_op_log WHERE derived_table = '{}' AND {canonical}", table.full_table_name));
            }
            let temporary =
                format!("_rindexer_canonical_{}", generate_random_id(20).to_ascii_lowercase());
            pg.batch_execute(&format!(
                "CREATE TEMP TABLE {temporary} ON COMMIT DROP AS {}",
                candidates.join(" UNION ALL ")
            ))
            .await
            .with_context(|| {
                format!("Cannot reconstruct canonical row identity for {}", table.full_table_name)
            })?;
            if columns.iter().any(|name| name == BLOCK_TIMESTAMP) {
                let missing = pg.query(&format!("SELECT DISTINCT row_metadata->>'{BLOCK_NUMBER}', row_metadata->>'{BLOCK_HASH}' FROM {temporary} WHERE row_metadata->>'{BLOCK_TIMESTAMP}' IS NULL"), &[]).await?;
                for row in missing {
                    let number = row.get::<_, String>(0).parse::<u64>()?;
                    let expected = row.get::<_, String>(1).parse::<B256>()?;
                    let provider = provider.context("Cannot restore required row timestamp: canonical source timestamp is absent and no provider is available")?;
                    let headers = super::super::fetch_canonical_headers(
                        provider.as_ref(),
                        &[U64::from(number)],
                    )
                    .await?;
                    let header = &headers[0].header;
                    anyhow::ensure!(header.hash == expected, "Cannot restore row timestamp at block {number}: source hash {expected} differs from canonical hash {}", header.hash);
                    let timestamp = chrono::DateTime::from_timestamp(
                        i64::try_from(header.timestamp).context(
                            "Cannot restore row timestamp beyond the signed seconds range",
                        )?,
                        0,
                    )
                    .context("Cannot restore row timestamp outside the supported date range")?
                    .to_rfc3339();
                    pg.execute(&format!("UPDATE {temporary} SET row_metadata = jsonb_set(row_metadata, '{{{BLOCK_TIMESTAMP}}}', to_jsonb($1::text)) WHERE row_metadata->>'{BLOCK_NUMBER}' = $2 AND row_metadata->>'{BLOCK_HASH}' = $3"), &[&timestamp, &number.to_string(), &expected.to_string()]).await?;
                }
            }
            let key_match = "to_jsonb(dt) @> sub.where_key";
            pg.batch_execute(&format!("DELETE FROM {} dt WHERE {affected} AND NOT EXISTS (SELECT 1 FROM {temporary} sub WHERE {key_match})", table.full_table_name)).await?;
            let restored = columns
                .iter()
                .map(|name| format!("restored.{name}"))
                .collect::<Vec<_>>()
                .join(", ");
            pg.batch_execute(&format!(
                "UPDATE {} dt SET ({}) = (SELECT {restored} FROM (SELECT row_metadata FROM {temporary} sub WHERE {key_match} ORDER BY (row_metadata->>'{RINDEXER_SEQUENCE_ID}')::numeric DESC, (row_metadata->>'network') COLLATE \"C\" DESC LIMIT 1) latest CROSS JOIN LATERAL jsonb_populate_record(dt, latest.row_metadata) restored) WHERE {affected}",
                table.full_table_name, columns.join(", "),
            )).await.with_context(|| format!("Cannot restore arithmetic sequence and provenance for {}", table.full_table_name))?;
        }
        Ok(())
    }
}
