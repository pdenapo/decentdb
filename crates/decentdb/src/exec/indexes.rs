//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn build_runtime_index(
    index: &IndexSchema,
    runtime: &EngineRuntime,
    page_size: u32,
) -> Result<RuntimeIndex> {
    let table = runtime.catalog.table(&index.table_name).ok_or_else(|| {
        DbError::corruption(format!(
            "index {} references missing table {}",
            index.name, index.table_name
        ))
    })?;
    let source = runtime.table_row_source(&index.table_name).ok_or_else(|| {
        DbError::corruption(format!("table data for {} is missing", index.table_name))
    })?;

    match index.kind {
        IndexKind::Btree => {
            let int64_keys = btree_uses_typed_int64_keys(index, table);
            let uuid_keys = btree_uses_typed_uuid_keys(index, table);
            let mut covering = covering_payloads_for_index(index, table);
            if index.unique && int64_keys {
                let mut keys = UniqueInt64Keys::new();
                for row in source.rows() {
                    let row = row?;
                    let Some(key) = compute_index_key(runtime, index, table, row.values())? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Int64(key) = key else {
                        return Err(DbError::internal(
                            "typed INT64 runtime index received an encoded key",
                        ));
                    };
                    if keys.insert(key, row.row_id()).is_some() {
                        return Err(DbError::corruption(format!(
                            "unique index {} contains duplicate keys",
                            index.name
                        )));
                    }
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) =
                            covering_payload_values_for_row(index, table, row.values())
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::UniqueInt64(Arc::new(keys), BTreeSet::new()),
                    covering,
                })
            } else if index.unique && uuid_keys {
                let mut keys = BTreeMap::<[u8; 16], i64>::new();
                for row in source.rows() {
                    let row = row?;
                    let Some(key) = compute_index_key(runtime, index, table, row.values())? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Uuid(key) = key else {
                        return Err(DbError::internal(
                            "typed UUID runtime index received an encoded key",
                        ));
                    };
                    if keys.insert(key, row.row_id()).is_some() {
                        return Err(DbError::corruption(format!(
                            "unique index {} contains duplicate keys",
                            index.name
                        )));
                    }
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) =
                            covering_payload_values_for_row(index, table, row.values())
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::UniqueUuid(Arc::new(keys), BTreeSet::new()),
                    covering,
                })
            } else if index.unique {
                let mut keys = BTreeMap::<RuntimeEncodedKey, i64>::new();
                for row in source.rows() {
                    let row = row?;
                    let Some(key) = compute_index_key(runtime, index, table, row.values())? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Encoded(key) = key else {
                        return Err(DbError::internal(
                            "encoded runtime index received an INT64 key",
                        ));
                    };
                    if keys.insert(key, row.row_id()).is_some() {
                        return Err(DbError::corruption(format!(
                            "unique index {} contains duplicate keys",
                            index.name
                        )));
                    }
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) =
                            covering_payload_values_for_row(index, table, row.values())
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::UniqueEncoded(Arc::new(keys), BTreeSet::new()),
                    covering,
                })
            } else if int64_keys {
                let mut keys = NonUniqueInt64Keys::new();
                for row in source.rows() {
                    let row = row?;
                    let Some(key) = compute_index_key(runtime, index, table, row.values())? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Int64(key) = key else {
                        return Err(DbError::internal(
                            "typed INT64 runtime index received an encoded key",
                        ));
                    };
                    keys.insert_row_id(key, row.row_id());
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) =
                            covering_payload_values_for_row(index, table, row.values())
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::NonUniqueInt64(Arc::new(keys), BTreeSet::new()),
                    covering,
                })
            } else if uuid_keys {
                let mut keys = BTreeMap::<[u8; 16], Vec<i64>>::new();
                for row in source.rows() {
                    let row = row?;
                    let Some(key) = compute_index_key(runtime, index, table, row.values())? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Uuid(key) = key else {
                        return Err(DbError::internal(
                            "typed UUID runtime index received an encoded key",
                        ));
                    };
                    keys.entry(key).or_default().push(row.row_id());
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) =
                            covering_payload_values_for_row(index, table, row.values())
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::NonUniqueUuid(Arc::new(keys), BTreeSet::new()),
                    covering,
                })
            } else {
                let mut keys = BTreeMap::<RuntimeEncodedKey, RuntimeEncodedRowIds>::new();
                // Pre-parse the partial-index predicate once instead of
                // re-parsing the predicate SQL for every row in the table
                // (row_satisfies_index_predicate parses on each call). Also
                // pre-resolve the single indexed column position for the
                // common single-column plain-column index so the build loop
                // avoids per-row column lookups and Value clones.
                let predicate_expr = index
                    .predicate_sql
                    .as_ref()
                    .map(|sql| crate::sql::parser::parse_expression_sql(sql))
                    .transpose()?;
                let single_column_position = single_plain_index_column_position(index, table);
                let multi_column_positions = if single_column_position.is_none() {
                    plain_index_column_positions(index, table)
                } else {
                    None
                };
                let has_virtual_generated = !generated_columns_are_stored(table);
                for row in source.rows() {
                    let row = row?;
                    let values = row.values();
                    if let Some(predicate_expr) = &predicate_expr {
                        let row_materialized = if has_virtual_generated {
                            let mut materialized = values.to_vec();
                            runtime.apply_virtual_generated_columns(table, &mut materialized)?;
                            Cow::Owned(materialized)
                        } else {
                            Cow::Borrowed(values)
                        };
                        let row_for_eval = row_materialized.as_ref();
                        let dataset = table_row_dataset(table, row_for_eval, &table.name);
                        let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
                        if !matches!(
                            runtime.eval_expr(
                                predicate_expr,
                                &dataset,
                                bindings,
                                &[],
                                &BTreeMap::new(),
                                None
                            )?,
                            Value::Bool(true)
                        ) {
                            continue;
                        }
                    }
                    let key = if let Some(position) = single_column_position {
                        // Fast path: encode the single indexed column value
                        // directly from the borrowed row slice, avoiding the
                        // intermediate Value clone that compute_index_values
                        // would perform.
                        encode_runtime_index_key(&values[position])?
                    } else if let Some(positions) = &multi_column_positions {
                        // Fast path for composite plain-column indexes: read
                        // each indexed column value by position and encode the
                        // composite key without building a Dataset.
                        let key_values: Vec<Value> = positions
                            .iter()
                            .map(|position| values.get(*position).cloned().unwrap_or(Value::Null))
                            .collect();
                        if index.unique && key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        RuntimeEncodedKey::from_vec(Row::new(key_values).encode()?)
                    } else {
                        let Some(encoded) = compute_index_key(runtime, index, table, values)?
                        else {
                            continue;
                        };
                        let RuntimeBtreeKey::Encoded(encoded) = encoded else {
                            return Err(DbError::internal(
                                "encoded runtime index received an INT64 key",
                            ));
                        };
                        encoded
                    };
                    match keys.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(RuntimeEncodedRowIds::one(row.row_id()));
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().push(row.row_id());
                        }
                    }
                    if let Some(covering) = covering.as_mut() {
                        if let Some(values) = covering_payload_values_for_row(index, table, values)
                        {
                            covering.insert_row_values(row.row_id(), values);
                        }
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::NonUniqueEncoded(
                        Arc::new(RuntimeEncodedPostings::new(keys)),
                        BTreeSet::new(),
                    ),
                    covering,
                })
            }
        }
        IndexKind::Trigram => {
            let mut trigram = TrigramIndex::new(page_size, 100_000);
            let mut builder = TrigramIndexBuilder::new();
            // Fast path: trigram indexes are constrained by DDL to a single
            // plain text column with no predicate. Resolve its position once
            // and read the text directly, avoiding the per-row Dataset
            // construction in compute_index_values.
            let single_text_position = plain_single_text_index_column_position(index, table);
            let has_predicate = index.predicate_sql.is_some();
            let predicate_expr = index
                .predicate_sql
                .as_ref()
                .map(|sql| crate::sql::parser::parse_expression_sql(sql))
                .transpose()?;
            let has_virtual_generated = !generated_columns_are_stored(table);
            for row in source.rows() {
                let row = row?;
                let values = row.values();
                if has_predicate {
                    if let Some(predicate_expr) = &predicate_expr {
                        let row_materialized = if has_virtual_generated {
                            let mut materialized = values.to_vec();
                            runtime.apply_virtual_generated_columns(table, &mut materialized)?;
                            Cow::Owned(materialized)
                        } else {
                            Cow::Borrowed(values)
                        };
                        let row_for_eval = row_materialized.as_ref();
                        let dataset = table_row_dataset(table, row_for_eval, &table.name);
                        let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
                        if !matches!(
                            runtime.eval_expr(
                                predicate_expr,
                                &dataset,
                                bindings,
                                &[],
                                &BTreeMap::new(),
                                None
                            )?,
                            Value::Bool(true)
                        ) {
                            continue;
                        }
                    }
                }
                let text = if let Some(position) = single_text_position {
                    match values.get(position) {
                        Some(Value::Text(text)) => Some(text.clone()),
                        // NULL or non-text: skip, matching compute_index_values
                        // which would error on non-text for a trigram index.
                        _ => None,
                    }
                } else {
                    compute_index_values(runtime, index, table, values)?
                        .into_iter()
                        .next()
                        .and_then(|value| match value {
                            Value::Text(text) => Some(text),
                            _ => None,
                        })
                };
                if let Some(text) = text {
                    builder.insert(row.row_id() as u64, &text);
                }
            }
            builder.finish_into(&mut trigram)?;
            Ok(RuntimeIndex::Trigram { index: trigram })
        }
        IndexKind::Spatial => {
            let backend = spatial_index_backend(index, table)?;
            let mut spatial = SpatialRuntimeIndex::new(backend);
            for row in source.rows() {
                let row = row?;
                if let Some(value) =
                    spatial_index_value_for_row(runtime, index, table, row.values())?
                {
                    spatial.insert(row.row_id(), value).map_err(spatial_error)?;
                }
            }
            Ok(RuntimeIndex::Spatial { index: spatial })
        }
        IndexKind::FullText => {
            let config = index
                .full_text
                .clone()
                .ok_or_else(|| DbError::corruption("fulltext index is missing analyzer config"))?;
            let mut fulltext = FullTextIndexBuilder::with_capacity(config, source.row_count());
            // Fast path: fulltext indexes are constrained by DDL to plain text
            // columns with no predicate. Resolve their positions once and read
            // the text directly, avoiding the per-row Dataset construction in
            // full_text_fields_for_row / compute_index_values.
            let text_positions = plain_text_index_column_positions(index, table);
            let has_predicate = index.predicate_sql.is_some();
            let predicate_expr = index
                .predicate_sql
                .as_ref()
                .map(|sql| crate::sql::parser::parse_expression_sql(sql))
                .transpose()?;
            let has_virtual_generated = !generated_columns_are_stored(table);
            for row in source.rows() {
                let row = row?;
                let values = row.values();
                if has_predicate {
                    if let Some(predicate_expr) = &predicate_expr {
                        let row_materialized = if has_virtual_generated {
                            let mut materialized = values.to_vec();
                            runtime.apply_virtual_generated_columns(table, &mut materialized)?;
                            Cow::Owned(materialized)
                        } else {
                            Cow::Borrowed(values)
                        };
                        let row_for_eval = row_materialized.as_ref();
                        let dataset = table_row_dataset(table, row_for_eval, &table.name);
                        let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
                        if !matches!(
                            runtime.eval_expr(
                                predicate_expr,
                                &dataset,
                                bindings,
                                &[],
                                &BTreeMap::new(),
                                None
                            )?,
                            Value::Bool(true)
                        ) {
                            continue;
                        }
                    }
                }
                if let Some(positions) = &text_positions {
                    match positions.len() {
                        1 => {
                            let text_ref = positions
                                .first()
                                .and_then(|position| values.get(*position))
                                .and_then(|value| value.as_text());
                            let fields = [text_ref];
                            fulltext.add_row(row.row_id() as u64, &fields);
                        }
                        2 => {
                            let fields = [
                                values.get(positions[0]).and_then(Value::as_text),
                                values.get(positions[1]).and_then(Value::as_text),
                            ];
                            fulltext.add_row(row.row_id() as u64, &fields);
                        }
                        _ => {
                            let field_refs: Vec<Option<&str>> = positions
                                .iter()
                                .map(|position| match values.get(*position) {
                                    Some(Value::Text(text)) => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect();
                            fulltext.add_row(row.row_id() as u64, &field_refs);
                        }
                    }
                } else {
                    let fields = full_text_fields_for_row(runtime, index, table, values)?;
                    let field_refs = fields.iter().map(Option::as_deref).collect::<Vec<_>>();
                    fulltext.add_row(row.row_id() as u64, &field_refs);
                }
            }
            Ok(RuntimeIndex::FullText {
                index: fulltext.finish(),
            })
        }
    }
}

impl EngineRuntime {
    pub(crate) fn refresh_paged_lookup_cache_and_pk_index(
        &mut self,
        db: &crate::db::Db,
        store: &DbTxnPageStore<'_>,
        table_name: &str,
        state: PersistedTableState,
    ) -> Result<Option<PageId>> {
        let needs_locator_cache = self.should_cache_deferred_paged_row_locators(table_name);
        if !db.config().persistent_pk_index && !needs_locator_cache {
            self.deferred_paged_row_locator_caches_mut()
                .remove(table_name);
            return Ok(None);
        }

        let chunk_payloads = read_paged_table_chunk_payloads(store, state)?;
        self.refresh_paged_lookup_cache_and_pk_index_from_chunks(
            db,
            table_name,
            state,
            &chunk_payloads,
        )
    }
    pub(crate) fn refresh_paged_lookup_cache_and_pk_index_from_chunks(
        &mut self,
        db: &crate::db::Db,
        table_name: &str,
        state: PersistedTableState,
        chunk_payloads: &[TablePageManifestChunk],
    ) -> Result<Option<PageId>> {
        let needs_locator_cache = self.should_cache_deferred_paged_row_locators(table_name);
        if needs_locator_cache {
            self.cache_deferred_paged_row_locators(table_name, state, chunk_payloads)?;
        } else {
            self.deferred_paged_row_locator_caches_mut()
                .remove(table_name);
        }

        if db.config().persistent_pk_index {
            build_persistent_pk_index_root_from_chunk_payloads(db, chunk_payloads)
        } else {
            Ok(None)
        }
    }
    pub(crate) fn rebuild_indexes(&mut self, page_size: u32) -> Result<()> {
        let indexes = self.catalog.indexes.values().cloned().collect::<Vec<_>>();
        let mut rebuilt: BTreeMap<String, Arc<RuntimeIndex>> = BTreeMap::new();
        for index in indexes {
            rebuilt.insert(
                index.name.clone(),
                Arc::new(build_runtime_index(&index, self, page_size)?),
            );
        }
        *self.indexes_mut() = rebuilt;
        for index in self.catalog_mut().indexes.values_mut() {
            index.fresh = true;
        }
        self.manifest_template = None;
        self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        Ok(())
    }
    pub(crate) fn rebuild_index(&mut self, name: &str, page_size: u32) -> Result<()> {
        let index = self
            .catalog
            .index(name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown index {name}")))?;
        let rebuilt = build_runtime_index(&index, self, page_size)?;
        self.indexes_mut()
            .insert(name.to_string(), Arc::new(rebuilt));
        if let Some(index) = self.catalog_mut().indexes.get_mut(name) {
            index.fresh = true;
        }
        self.manifest_template = None;
        self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        Ok(())
    }
    pub(crate) fn verify_index(&self, name: &str, page_size: u32) -> Result<()> {
        self.catalog
            .index(name)
            .ok_or_else(|| DbError::sql(format!("unknown index {name}")))?;
        let existing = self.index(name).map_or(0, runtime_index_entry_count);
        let mut rebuilt = self.clone();
        rebuilt.rebuild_index(name, page_size)?;
        let actual = rebuilt.index(name).map_or(0, runtime_index_entry_count);
        if existing != actual {
            return Err(DbError::corruption(format!(
                "index {name} verification failed: expected {existing} entries, rebuilt {actual}"
            )));
        }
        Ok(())
    }
    pub(super) fn mark_indexes_stale_for_table(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        let index_names = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table_name))
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        self.mark_named_indexes_stale(&index_names);
    }
    /// Mark only the named indexes (and their catalog entries) as stale,
    /// discarding any in-memory runtime index for them. Used when a DML
    /// successfully incrementally updates some indexes on a table but fails to
    /// incrementally update others — the successful ones stay fresh and the
    /// failed ones are rebuilt on next access.
    pub(super) fn mark_named_indexes_stale(&mut self, index_names: &[String]) {
        if index_names.is_empty() {
            return;
        }
        let mut changed = false;
        {
            let catalog = self.catalog_mut();
            for name in index_names {
                if let Some(index) = catalog.indexes.get_mut(name) {
                    if index.fresh {
                        index.fresh = false;
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.manifest_template = None;
        }
        let indexes = self.indexes_mut();
        let mut any_removed = false;
        for name in index_names {
            if indexes.remove(name).is_some() {
                any_removed = true;
            }
        }
        if changed || any_removed {
            self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        }
    }
    pub(super) fn prepare_insert_index_updates(
        &mut self,
        table_name: &str,
        row: &StoredRow,
        page_size: u32,
    ) -> Result<Vec<PendingIndexInsert>> {
        if self.visible_table_is_temporary(table_name) {
            return Ok(Vec::new());
        }
        let Some(canonical_table_name) = self.canonical_catalog_table_name(table_name) else {
            return Ok(Vec::new());
        };
        let table = self
            .table_schema(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| {
                identifiers_equal(&index.table_name, &canonical_table_name) && index.fresh
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut updates = Vec::new();

        for index in indexes {
            if !self.indexes.contains_key(&index.name) {
                self.rebuild_index(&index.name, page_size)?;
            }

            match index.kind {
                IndexKind::Btree => {
                    let Some(key) = compute_index_key(self, &index, &table, &row.values)? else {
                        continue;
                    };
                    if index.unique {
                        self.remove_tombstoned_unique_btree_entries_for_insert(
                            &canonical_table_name,
                            &index,
                            &key,
                        )?;
                    }
                    updates.push(PendingIndexInsert::Btree {
                        name: index.name.clone(),
                        key,
                        row_id: row.row_id,
                        covering_values: covering_payload_values_for_row(
                            &index,
                            &table,
                            &row.values,
                        ),
                    });
                }
                IndexKind::Trigram => {
                    if !row_satisfies_index_predicate(self, &index, &table, &row.values)? {
                        continue;
                    }
                    let text = compute_index_values(self, &index, &table, &row.values)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            DbError::constraint("trigram index requires a single text expression")
                        })?;
                    let Value::Text(text) = text else {
                        return Err(DbError::constraint(
                            "trigram index requires a single text expression",
                        ));
                    };
                    updates.push(PendingIndexInsert::Trigram {
                        name: index.name.clone(),
                        row_id: row.row_id as u64,
                        text,
                    });
                }
                IndexKind::Spatial => {
                    if let Some(value) =
                        spatial_index_value_for_row(self, &index, &table, &row.values)?
                    {
                        updates.push(PendingIndexInsert::Spatial {
                            name: index.name.clone(),
                            row_id: row.row_id,
                            value,
                        });
                    }
                }
                IndexKind::FullText => {
                    if !row_satisfies_index_predicate(self, &index, &table, &row.values)? {
                        continue;
                    }
                    let fields = full_text_fields_for_row(self, &index, &table, &row.values)?;
                    updates.push(PendingIndexInsert::FullText {
                        name: index.name.clone(),
                        row_id: row.row_id as u64,
                        fields,
                    });
                }
            }
        }

        Ok(updates)
    }
    fn remove_tombstoned_unique_btree_entries_for_insert(
        &mut self,
        table_name: &str,
        index: &IndexSchema,
        key: &RuntimeBtreeKey,
    ) -> Result<()> {
        let Some(row_source) = self.visible_table_row_source(table_name) else {
            return Ok(());
        };
        if !row_source.has_tombstoned_rows() {
            return Ok(());
        }
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(());
        };
        let mut stale_row_ids = Vec::new();
        for row_id in keys.row_ids_for_key(key) {
            if row_source.row_by_id(row_id)?.is_none() {
                stale_row_ids.push(row_id);
            }
        }
        if stale_row_ids.is_empty() {
            return Ok(());
        }
        let Some(RuntimeIndex::Btree { keys, covering }) = self.index_mut(&index.name) else {
            return Ok(());
        };
        for row_id in stale_row_ids {
            keys.remove_row_id(key, row_id)?;
            if let Some(covering) = covering.as_mut() {
                covering.remove_row_id(row_id);
            }
        }
        Ok(())
    }
    pub(super) fn apply_insert_index_updates(
        &mut self,
        updates: Vec<PendingIndexInsert>,
    ) -> Result<()> {
        for update in updates {
            match update {
                PendingIndexInsert::Btree {
                    name,
                    key,
                    row_id,
                    covering_values,
                } => match self.index_mut(&name) {
                    Some(RuntimeIndex::Btree { keys, covering }) => {
                        keys.insert_row_id(key, row_id)?;
                        if let (Some(covering), Some(values)) = (covering.as_mut(), covering_values)
                        {
                            covering.insert_row_values(row_id, values);
                        }
                    }
                    Some(_) => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is not a BTREE index"
                        )))
                    }
                    None => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is missing"
                        )))
                    }
                },
                PendingIndexInsert::Trigram { name, row_id, text } => match self.index_mut(&name) {
                    Some(RuntimeIndex::Trigram { index }) => {
                        index.queue_insert(row_id, &text);
                    }
                    Some(_) => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is not a trigram index"
                        )))
                    }
                    None => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is missing"
                        )))
                    }
                },
                PendingIndexInsert::Spatial {
                    name,
                    row_id,
                    value,
                } => match self.index_mut(&name) {
                    Some(RuntimeIndex::Spatial { index }) => {
                        index.insert(row_id, value).map_err(spatial_error)?;
                    }
                    Some(_) => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is not a SPATIAL index"
                        )))
                    }
                    None => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is missing"
                        )))
                    }
                },
                PendingIndexInsert::FullText {
                    name,
                    row_id,
                    fields,
                } => match self.index_mut(&name) {
                    Some(RuntimeIndex::FullText { index }) => {
                        let refs = fields.iter().map(Option::as_deref).collect::<Vec<_>>();
                        index.insert_document(row_id, &refs);
                    }
                    Some(_) => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is not a fulltext index"
                        )))
                    }
                    None => {
                        return Err(DbError::internal(format!(
                            "runtime index {name} is missing"
                        )))
                    }
                },
            }
        }
        Ok(())
    }
    pub(crate) fn indexes_for_table(&self, table_name: &str) -> Vec<&IndexSchema> {
        let (qualifier, object) = compat_schema_qualified_name(table_name);
        let mut indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| {
                qualifier != Some(CompatSchemaQualifier::Temp)
                    && identifiers_equal(&index.table_name, object)
            })
            .chain(self.temp_indexes.values().filter(|index| {
                qualifier != Some(CompatSchemaQualifier::Main)
                    && identifiers_equal(&index.table_name, object)
            }))
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        indexes
    }
    pub(crate) fn index_by_name(&self, index_name: &str) -> Option<&IndexSchema> {
        let (qualifier, object) = compat_schema_qualified_name(index_name);
        match qualifier {
            Some(CompatSchemaQualifier::Main) => map_get_ci(&self.catalog.indexes, object),
            Some(CompatSchemaQualifier::Temp) => map_get_ci(&self.temp_indexes, object),
            None => map_get_ci(&self.catalog.indexes, object)
                .or_else(|| map_get_ci(&self.temp_indexes, object)),
        }
    }
}
