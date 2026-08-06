//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn compact_paged_table_state_for_checkpoint<S: PageStore>(
    store: &mut S,
    state: PersistedTableState,
) -> Result<(PersistedTableState, bool)> {
    if state.pointer.head_page_id == 0 || !state.pointer.is_table_paged_manifest() {
        return Ok((state, false));
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let mut manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut changed = false;
    let chunk_compaction_min_bytes = paged_table_checkpoint_compaction_min_bytes(store.page_size());
    let mut freed_pointers: Vec<OverflowPointer> = Vec::new();
    for chunk in &mut manifest.chunks {
        let needs_merge = !chunk.tombstoned_row_ids.is_empty() || chunk.overlay_pointer.is_some();
        if !needs_merge {
            if chunk.pointer.head_page_id == 0
                || chunk.pointer.is_compressed()
                || usize::try_from(chunk.pointer.logical_len)
                    .ok()
                    .is_none_or(|len| len < chunk_compaction_min_bytes)
            {
                continue;
            }
            let payload = read_overflow(store, chunk.pointer)?;
            let pointer = rewrite_overflow(
                store,
                chunk.pointer,
                &payload,
                CompressionMode::AutoMinBytes(chunk_compaction_min_bytes),
            )?;
            if pointer != chunk.pointer {
                chunk.pointer = pointer;
                changed = true;
            }
            continue;
        }

        // Fold tombstones and overlay into a new base payload.
        let base_payload = read_overflow(store, chunk.pointer)?;
        let base_rows = decode_table_payload_rows(&base_payload)?;
        let mut merged_rows: BTreeMap<i64, StoredRow> = BTreeMap::new();
        for row in base_rows {
            if !chunk.tombstoned_row_ids.contains(&row.row_id) {
                merged_rows.insert(row.row_id, row);
            }
        }
        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let overlay_payload = read_overflow(store, overlay_pointer)?;
            let overlay_rows = decode_table_payload_rows(&overlay_payload)?;
            for row in overlay_rows {
                merged_rows.insert(row.row_id, row);
            }
        }
        let merged: Vec<StoredRow> = merged_rows.into_values().collect();
        let merged_len = merged.len();
        let new_payload = encode_table_payload(&TableData::from_rows(merged))?;
        let new_checksum = crc32c_parts(&[new_payload.as_slice()]);
        let new_pointer = write_overflow(
            store,
            &new_payload,
            CompressionMode::AutoMinBytes(chunk_compaction_min_bytes),
        )?;
        if chunk.pointer.head_page_id != 0 {
            freed_pointers.push(chunk.pointer);
        }
        if let Some(overlay_pointer) = chunk.overlay_pointer {
            if overlay_pointer.head_page_id != 0 {
                freed_pointers.push(overlay_pointer);
            }
        }
        chunk.pointer = new_pointer;
        chunk.checksum = new_checksum;
        chunk.row_count = merged_len;
        chunk.tombstoned_row_ids.clear();
        chunk.overlay_pointer = None;
        chunk.overlay_checksum = None;
        changed = true;
    }

    let should_rewrite_manifest = changed
        || (!state.pointer.is_compressed()
            && usize::try_from(state.pointer.logical_len)
                .ok()
                .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES));
    if !should_rewrite_manifest {
        return Ok((state, false));
    }

    let manifest_payload = encode_paged_table_manifest_payload(&manifest)?;
    let checksum = crc32c_parts(&[manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        state.pointer.with_table_paged_manifest(false),
        &manifest_payload,
        CompressionMode::Auto,
    )?
    .with_table_paged_manifest(true);
    let tail = if pointer.is_compressed() {
        OverflowTailInfo::default()
    } else {
        read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default()
    };
    let new_state = PersistedTableState {
        pointer,
        checksum,
        row_count: state.row_count,
        tail,
        pk_index_root: state.pk_index_root,
    };

    // Free old base and overlay pages that were replaced by merge compaction.
    // (Non-merge rewrites use rewrite_overflow which reuses/frees old pages
    // on its own.)
    for old_pointer in freed_pointers {
        if old_pointer.head_page_id != 0 {
            free_overflow(store, old_pointer.head_page_id)?;
        }
    }

    Ok((new_state, new_state != state || changed))
}

impl EngineRuntime {
    pub(crate) fn deferred_tables_mut(&mut self) -> &mut BTreeSet<String> {
        Arc::make_mut(&mut self.deferred_tables)
    }
    pub(crate) fn compact_dirty_resident_storage_after_transaction_commit(&mut self) -> usize {
        if self.dirty_tables.is_empty() {
            return 0;
        }

        let dirty_tables = self.dirty_tables.iter().cloned().collect::<Vec<_>>();
        let mut freed = 0usize;
        {
            let tables = Arc::make_mut(&mut self.tables);
            for table_name in &dirty_tables {
                if let Some(row_source) = tables.get_mut(table_name) {
                    freed = freed.saturating_add(row_source.shrink_resident_to_fit_if_unique());
                }
            }
        }

        let dirty_index_names = self
            .catalog
            .indexes
            .values()
            .filter(|index| {
                dirty_tables
                    .iter()
                    .any(|table_name| identifiers_equal(table_name, &index.table_name))
            })
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        if !dirty_index_names.is_empty() {
            let indexes = Arc::make_mut(&mut self.indexes);
            for index_name in dirty_index_names {
                if let Some(index) = indexes.get_mut(&index_name).and_then(Arc::get_mut) {
                    freed = freed.saturating_add(index.shrink_to_fit_if_unique());
                }
            }
        }

        freed
    }
    #[cfg(test)]
    pub(crate) fn has_deferred_paged_row_locator_cache_for_tests(&self, table_name: &str) -> bool {
        self.deferred_paged_row_locator_caches
            .contains_key(table_name)
    }
    pub(crate) fn load_from_storage(
        pager: &PagerHandle,
        wal: &WalHandle,
        schema_cookie: u32,
        config: &crate::config::DbConfig,
    ) -> Result<(Self, u64)> {
        let reader = wal.begin_reader_with_pager(pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        let runtime =
            Self::load_from_storage_at_snapshot(pager, wal, schema_cookie, config, snapshot_lsn)?;
        drop(reader);
        Ok((runtime, snapshot_lsn))
    }
    pub(crate) fn load_from_storage_at_snapshot(
        pager: &PagerHandle,
        wal: &WalHandle,
        schema_cookie: u32,
        config: &crate::config::DbConfig,
        snapshot_lsn: u64,
    ) -> Result<Self> {
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let root_page = store.read_page(page::CATALOG_ROOT_PAGE_ID)?;
        let root = decode_root_header(&root_page)?;
        let mut runtime = if let Some(root) = root {
            let payload = if root.pointer.logical_len == 0 || root.pointer.head_page_id == 0 {
                Vec::new()
            } else {
                read_overflow(&store, root.pointer)?
            };
            if crc32c_parts(&[payload.as_slice()]) != root.payload_checksum {
                return Err(DbError::corruption("catalog state checksum mismatch"));
            }
            let mut runtime = if payload.is_empty() {
                Self::from_config(root.schema_cookie, config)
            } else if payload.starts_with(LEGACY_RUNTIME_PAYLOAD_MAGIC) {
                let mut runtime = decode_runtime_payload(&payload)?;
                runtime.mark_all_tables_dirty();
                runtime
            } else if payload.starts_with(MANIFEST_PAYLOAD_MAGIC) {
                decode_manifest_payload(&store, &payload)?
            } else {
                return Err(DbError::corruption("unknown catalog state payload magic"));
            };
            let root_schema_cookie = root.schema_cookie;
            runtime.root_state = Some(root);
            runtime.catalog_mut().schema_cookie = root_schema_cookie;
            runtime.paged_row_storage = config.paged_row_storage;
            runtime.extension_trust_anchors = Arc::new(config.extension_trust_anchors.clone());
            runtime.extension_unsigned_development_mode =
                config.extension_unsigned_development_mode;
            runtime
        } else {
            Self::from_config(schema_cookie, config)
        };
        runtime
            .payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned")
            .set_max_entries(config.cached_payloads_max_entries);
        if runtime.root_state.is_none() {
            runtime.catalog_mut().schema_cookie = schema_cookie;
        }
        // Materialize any deferred tables under the same reader guard so
        // that overflow pointers from the manifest are read against the
        // same WAL snapshot. If deferred loading uses a later snapshot,
        // a concurrent writer may have extended the overflow chain, causing
        // a length mismatch.
        //
        // ADR 0143 Phase B (opt-in): when
        // `DbConfig::defer_table_materialization` is true we intentionally
        // skip the eager materialize+rebuild here so that `Db::open` does
        // not allocate `Vec<StoredRow>` for every persisted table. The
        // per-statement/transaction lazy-load path in `db.rs` now pins a
        // single reader snapshot across both the manifest refresh and the
        // overflow payload read so first-use materialization does not mix
        // snapshots under concurrent checkpoints.
        if !runtime.deferred_tables.is_empty() && !config.defer_table_materialization {
            runtime.materialize_deferred_tables_with_store(&store, pager.page_size(), None)?;
        }
        if !config.defer_table_materialization || runtime.deferred_tables.is_empty() {
            runtime.rebuild_indexes(pager.page_size())?;
        }
        Ok(runtime)
    }
    /// Returns `true` when one or more tables still have their row data
    /// deferred (not yet loaded from storage).
    #[must_use]
    pub(crate) fn has_deferred_tables(&self) -> bool {
        !self.deferred_tables.is_empty()
    }
    /// Returns an iterator over deferred table names.
    #[allow(clippy::double_must_use)]
    #[must_use]
    pub(crate) fn deferred_table_names(&self) -> impl Iterator<Item = &String> {
        self.deferred_tables.iter()
    }
    /// Materializes all deferred table data from storage, then rebuilds
    /// indexes.  After this call `deferred_tables` is empty and the runtime
    /// is fully populated.
    pub(crate) fn load_deferred_tables(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
    ) -> Result<()> {
        self.load_deferred_tables_with_snapshot(pager, wal, page_size, None, None)
    }
    pub(crate) fn load_deferred_tables_at_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        snapshot_lsn: u64,
    ) -> Result<()> {
        self.load_deferred_tables_with_snapshot(pager, wal, page_size, None, Some(snapshot_lsn))
    }
    /// Loads a subset of deferred tables, specified by name.
    ///
    /// This is used for per-table on-demand loading where only the tables
    /// referenced by the current SQL statement are materialized.
    #[allow(dead_code)]
    pub(crate) fn load_deferred_tables_filtered(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: &BTreeSet<String>,
    ) -> Result<()> {
        self.load_deferred_tables_with_snapshot(pager, wal, page_size, Some(filter), None)
    }
    pub(crate) fn load_deferred_tables_filtered_at_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: &BTreeSet<String>,
        snapshot_lsn: u64,
    ) -> Result<()> {
        self.load_deferred_tables_with_snapshot(
            pager,
            wal,
            page_size,
            Some(filter),
            Some(snapshot_lsn),
        )
    }
    pub(crate) fn load_deferred_table_row_sources_filtered(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: &BTreeSet<String>,
    ) -> Result<()> {
        self.load_deferred_table_row_sources_with_snapshot(
            pager,
            wal,
            page_size,
            Some(filter),
            None,
        )
    }
    pub(crate) fn load_deferred_table_row_sources_at_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        snapshot_lsn: u64,
    ) -> Result<()> {
        self.load_deferred_table_row_sources_with_snapshot(
            pager,
            wal,
            page_size,
            None,
            Some(snapshot_lsn),
        )
    }
    pub(crate) fn load_deferred_table_row_sources(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
    ) -> Result<()> {
        self.load_deferred_table_row_sources_with_snapshot(pager, wal, page_size, None, None)
    }
    fn load_deferred_table_row_sources_with_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: Option<&BTreeSet<String>>,
        snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        if self.deferred_tables.is_empty() {
            return Ok(());
        }
        let Some(snapshot_lsn) = snapshot_lsn else {
            let reader = wal.begin_reader_with_pager(pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            let store = SnapshotPageStore {
                pager,
                wal,
                snapshot_lsn,
            };
            self.materialize_deferred_table_row_sources_with_store(&store, page_size, filter)?;
            drop(reader);
            return Ok(());
        };

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        self.materialize_deferred_table_row_sources_with_store(&store, page_size, filter)?;
        Ok(())
    }
    pub(crate) fn load_deferred_table_row_sources_filtered_at_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: &BTreeSet<String>,
        snapshot_lsn: u64,
    ) -> Result<()> {
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        self.materialize_deferred_table_row_sources_with_store(&store, page_size, Some(filter))
    }
    pub(crate) fn hydrate_deferred_runtime_index_at_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        table_name: &str,
        index_name: &str,
        snapshot_lsn: u64,
    ) -> Result<()> {
        let Some(canonical_table_name) = self
            .deferred_tables
            .iter()
            .find(|deferred| identifiers_equal(deferred, table_name))
            .cloned()
        else {
            return Ok(());
        };

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let state = *self
            .persisted_tables
            .get(&canonical_table_name)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "deferred table '{canonical_table_name}' has no persisted state"
                ))
            })?;
        let table_schema = self
            .catalog
            .table(&canonical_table_name)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "deferred table '{canonical_table_name}' has no schema"
                ))
            })?
            .clone();
        let index_schema = self
            .catalog
            .index(index_name)
            .ok_or_else(|| DbError::sql(format!("unknown index {index_name}")))?
            .clone();
        let cache_key = DeferredRuntimeBtreeIndexCacheKey::new(&table_schema, &index_schema, state);
        if let Some(entry) = cached_deferred_runtime_btree_index(&cache_key)? {
            self.indexes_mut()
                .insert(index_schema.name.clone(), Arc::clone(&entry.runtime_index));
            if let Some(locator_cache) = entry.paged_locator_cache.as_ref() {
                self.deferred_paged_row_locator_caches_mut()
                    .insert(canonical_table_name, Arc::clone(locator_cache));
            }
            return Ok(());
        }

        let row_source = if state.pointer.is_table_paged_manifest() {
            TableRowSource::Paged(Arc::new(read_table_page_manifest_from_state(
                &store, state,
            )?))
        } else {
            TableRowSource::Resident(Arc::new(decode_persisted_table_data(&store, state)?))
        };
        let row_count = row_source.row_count();
        if let Some(ps) = self.persisted_tables_mut().get_mut(&canonical_table_name) {
            ps.row_count = row_count;
            ps.tail = read_uncompressed_overflow_tail(&store, ps.pointer)?.unwrap_or_default();
        }
        self.tables_mut()
            .insert(canonical_table_name.clone(), row_source);
        self.deferred_tables_mut().remove(&canonical_table_name);
        if !self.indexes.contains_key(&index_schema.name) {
            self.rebuild_index(&index_schema.name, page_size)?;
        }
        if state.pointer.is_table_paged_manifest() {
            let Some(TableRowSource::Paged(manifest)) = self.tables.get(&canonical_table_name)
            else {
                return Err(DbError::internal(format!(
                    "paged row source for {canonical_table_name} is missing after index hydration"
                )));
            };
            let chunks = Arc::clone(&manifest.chunks);
            self.cache_deferred_paged_row_locators(&canonical_table_name, state, chunks.as_ref())?;
        }
        if let Some(runtime_index) = self.indexes.get(&index_schema.name).cloned() {
            let paged_locator_cache = self
                .deferred_paged_row_locator_caches
                .get(&canonical_table_name)
                .cloned();
            cache_deferred_runtime_btree_index(
                cache_key,
                DeferredRuntimeBtreeIndexCacheEntry {
                    runtime_index,
                    paged_locator_cache,
                },
            )?;
        }
        let _ = self.redefer_persisted_tables(&[canonical_table_name.as_str()]);
        Ok(())
    }
    fn load_deferred_tables_with_snapshot(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
        filter: Option<&BTreeSet<String>>,
        snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        if self.deferred_tables.is_empty() {
            return Ok(());
        }
        let Some(snapshot_lsn) = snapshot_lsn else {
            let reader = wal.begin_reader_with_pager(pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            let store = SnapshotPageStore {
                pager,
                wal,
                snapshot_lsn,
            };
            self.materialize_deferred_tables_with_store(&store, page_size, filter)?;
            drop(reader);
            return Ok(());
        };

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        self.materialize_deferred_tables_with_store(&store, page_size, filter)?;
        Ok(())
    }
    /// Loads deferred tables using an existing `SnapshotPageStore`.
    ///
    /// This ensures the overflow pointers recorded in `persisted_tables`
    /// (from the manifest) are read against the same WAL snapshot that
    /// produced those pointers, avoiding length mismatches when a
    /// concurrent writer extends the overflow chain.
    ///
    /// When `filter` is `Some`, only the named tables are materialized.
    /// When `None`, all deferred tables are materialized (legacy behavior).
    fn materialize_deferred_tables_with_store<S: PageStore>(
        &mut self,
        store: &S,
        page_size: u32,
        filter: Option<&BTreeSet<String>>,
    ) -> Result<()> {
        let table_names: Vec<String> = if let Some(f) = filter {
            self.deferred_tables
                .iter()
                .filter(|table_name| {
                    f.iter()
                        .any(|filter_name| filter_name.eq_ignore_ascii_case(table_name))
                })
                .cloned()
                .collect()
        } else {
            self.deferred_tables.iter().cloned().collect()
        };
        if table_names.is_empty() {
            return Ok(());
        }
        for table_name in &table_names {
            let state = *self.persisted_tables.get(table_name).ok_or_else(|| {
                DbError::internal(format!(
                    "deferred table '{table_name}' has no persisted state"
                ))
            })?;
            let data = decode_persisted_table_data(store, state)?;

            if let Some(ps) = self.persisted_tables_mut().get_mut(table_name) {
                ps.row_count = data.row_count();
                ps.tail = read_uncompressed_overflow_tail(store, ps.pointer)?.unwrap_or_default();
            }
            self.tables_mut().insert(table_name.clone(), data.into());
            self.deferred_tables_mut().remove(table_name);
        }
        self.rebuild_stale_indexes(page_size)?;
        Ok(())
    }
    fn materialize_deferred_table_row_sources_with_store<S: PageStore>(
        &mut self,
        store: &S,
        page_size: u32,
        filter: Option<&BTreeSet<String>>,
    ) -> Result<()> {
        let table_names: Vec<String> = if let Some(f) = filter {
            self.deferred_tables
                .iter()
                .filter(|table_name| {
                    f.iter()
                        .any(|filter_name| filter_name.eq_ignore_ascii_case(table_name))
                })
                .cloned()
                .collect()
        } else {
            self.deferred_tables.iter().cloned().collect()
        };
        if table_names.is_empty() {
            return Ok(());
        }
        for table_name in &table_names {
            let state = *self.persisted_tables.get(table_name).ok_or_else(|| {
                DbError::internal(format!(
                    "deferred table '{table_name}' has no persisted state"
                ))
            })?;
            let row_source = if state.pointer.is_table_paged_manifest() {
                TableRowSource::Paged(Arc::new(read_table_page_manifest_from_state(store, state)?))
            } else {
                TableRowSource::Resident(Arc::new(decode_persisted_table_data(store, state)?))
            };
            let row_count = row_source.row_count();
            if let Some(ps) = self.persisted_tables_mut().get_mut(table_name) {
                ps.row_count = row_count;
                ps.tail = read_uncompressed_overflow_tail(store, ps.pointer)?.unwrap_or_default();
            }
            self.tables_mut().insert(table_name.clone(), row_source);
            self.deferred_tables_mut().remove(table_name);
        }
        self.rebuild_stale_indexes(page_size)?;
        Ok(())
    }
    pub(crate) fn prepare_resident_payload_offset_caches<S: PageStore>(
        &mut self,
        store: &S,
        config: &crate::config::DbConfig,
    ) -> Result<bool> {
        if !preserve_resident_payload_offsets_for_delete_tombstones(config) {
            return Ok(false);
        }
        let table_names = self
            .persisted_tables
            .iter()
            .filter_map(|(table_name, state)| {
                if state.pointer.head_page_id != 0
                    && !state.pointer.is_table_paged_manifest()
                    && !state.pointer.is_compressed()
                    && !self.overflow_chain_caches.contains_key(table_name)
                {
                    Some((table_name.clone(), state.pointer.head_page_id))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if table_names.is_empty() {
            return Ok(false);
        }
        for (table_name, head_page_id) in table_names {
            let chain_cache = build_overflow_chain_cache(store, head_page_id)?;
            self.overflow_chain_caches.insert(table_name, chain_cache);
        }
        Ok(true)
    }
    pub(crate) fn backfill_missing_persistent_pk_index_for_table(
        &mut self,
        db: &crate::db::Db,
        table_name: &str,
    ) -> Result<bool> {
        let Some(canonical_table_name) = self
            .catalog
            .tables
            .keys()
            .find(|candidate| identifiers_equal(candidate, table_name))
            .cloned()
        else {
            return Ok(false);
        };
        let Some(table) = self.catalog.tables.get(&canonical_table_name) else {
            return Ok(false);
        };
        if table.pk_index_root.is_some()
            || self
                .persisted_tables
                .get(&canonical_table_name)
                .is_none_or(|state| state.pointer.head_page_id == 0)
        {
            return Ok(false);
        }
        self.backfill_missing_persistent_pk_index_for_canonical_table(
            db,
            canonical_table_name.as_str(),
        )
    }
    fn backfill_missing_persistent_pk_index_for_canonical_table(
        &mut self,
        db: &crate::db::Db,
        table_name: &str,
    ) -> Result<bool> {
        let Some(previous_state) = self.persisted_tables.get(table_name).copied() else {
            return Ok(false);
        };
        if self
            .catalog
            .tables
            .get(table_name)
            .is_some_and(|table| table.pk_index_root.is_some())
            || previous_state.pointer.head_page_id == 0
        {
            return Ok(false);
        }

        let mut store = DbTxnPageStore { db };
        if previous_state.pointer.is_table_paged_manifest() {
            let chunk_payloads = read_paged_table_chunk_payloads(&store, previous_state)?;
            let pk_index_root =
                build_persistent_pk_index_root_from_chunk_payloads(db, &chunk_payloads)?;
            replace_table_pk_index_root(self, db, table_name, pk_index_root)?;
            return Ok(true);
        }

        let payload = Arc::new(read_overflow(&store, previous_state.pointer)?);
        let pointer = if previous_state.pointer.is_compressed() {
            rewrite_overflow(
                &mut store,
                previous_state.pointer,
                payload.as_slice(),
                CompressionMode::Never,
            )?
        } else {
            previous_state.pointer
        };
        let tail = read_uncompressed_overflow_tail(&store, pointer)?.unwrap_or_default();
        let checksum = crc32c_parts(&[payload.as_slice()]);
        self.persisted_tables_mut().insert(
            table_name.to_string(),
            PersistedTableState {
                pointer,
                checksum,
                row_count: previous_state.row_count,
                tail,
                pk_index_root: previous_state.pk_index_root,
            },
        );
        self.cache_payload_insert(table_name.to_string(), Arc::clone(&payload));
        self.overflow_chain_caches.remove(table_name);
        let chain_cache = build_overflow_chain_cache(&store, pointer.head_page_id)?;
        self.overflow_chain_caches
            .insert(table_name.to_string(), chain_cache);

        let pk_index_root = build_persistent_pk_index_root(db, payload.as_slice())?;
        replace_table_pk_index_root(self, db, table_name, pk_index_root)?;
        Ok(true)
    }
    pub(crate) fn backfill_paged_row_storage(&mut self, db: &crate::db::Db) -> Result<bool> {
        let table_names = self
            .catalog
            .tables
            .keys()
            .filter_map(|table_name| {
                self.persisted_tables
                    .get(table_name)
                    .filter(|state| {
                        state.pointer.head_page_id != 0 && !state.pointer.is_table_paged_manifest()
                    })
                    .map(|_| table_name.clone())
            })
            .collect::<Vec<_>>();
        if table_names.is_empty() {
            return Ok(false);
        }

        let mut changed = false;
        let mut store = DbTxnPageStore { db };
        for table_name in table_names {
            let Some(previous_state) = self.persisted_tables.get(&table_name).copied() else {
                continue;
            };
            let new_state = wrap_legacy_table_state_as_paged_manifest(&mut store, previous_state)?;
            self.persisted_tables_mut()
                .insert(table_name.clone(), new_state);
            self.overflow_chain_caches.remove(&table_name);
            if db.config().persistent_pk_index {
                let chunk_payloads = read_paged_table_chunk_payloads(&store, new_state)?;
                let pk_index_root =
                    build_persistent_pk_index_root_from_chunk_payloads(db, &chunk_payloads)?;
                let manifest = TablePageManifest::from_chunks(chunk_payloads)?;
                let payload = Arc::new(encode_legacy_table_payload_from_manifest(&manifest)?);
                replace_table_pk_index_root(self, db, &table_name, pk_index_root)?;
                self.cache_payload_insert(table_name.clone(), payload);
            }
            changed = true;
        }
        Ok(changed)
    }
    pub(crate) fn compact_persisted_payloads_for_checkpoint(
        &mut self,
        db: &crate::db::Db,
    ) -> Result<bool> {
        let old_root = self.root_state;
        let mut changed = false;
        {
            let mut store = DbTxnPageStore { db };
            let table_names = self.persisted_tables.keys().cloned().collect::<Vec<_>>();
            for table_name in table_names {
                let Some(previous_state) = self.persisted_tables.get(&table_name).copied() else {
                    continue;
                };
                let previous_pointer = previous_state.pointer;
                if previous_pointer.head_page_id == 0 {
                    continue;
                }
                if previous_pointer.is_table_paged_manifest() {
                    let (new_state, table_changed) =
                        compact_paged_table_state_for_checkpoint(&mut store, previous_state)?;
                    if table_changed {
                        self.persisted_tables_mut()
                            .insert(table_name.clone(), new_state);
                        if db.config().persistent_pk_index {
                            let chunk_payloads =
                                read_paged_table_chunk_payloads(&store, new_state)?;
                            let pk_index_root = build_persistent_pk_index_root_from_chunk_payloads(
                                db,
                                &chunk_payloads,
                            )?;
                            replace_table_pk_index_root(self, db, &table_name, pk_index_root)?;
                        }
                        changed = true;
                    }
                    continue;
                }
                if previous_state.pk_index_root.is_some()
                    || previous_pointer.is_compressed()
                    || usize::try_from(previous_pointer.logical_len)
                        .ok()
                        .is_none_or(|len| len < AUTO_MIN_PAYLOAD_BYTES)
                {
                    continue;
                }
                let payload = if let Some(cached) = self.cached_payload(&table_name) {
                    cached
                } else {
                    Arc::new(read_overflow(&store, previous_pointer)?)
                };
                let pointer = rewrite_overflow(
                    &mut store,
                    previous_pointer,
                    payload.as_slice(),
                    CompressionMode::Auto,
                )?;
                if pointer != previous_pointer {
                    changed = true;
                }
                let tail = if pointer.is_compressed() {
                    OverflowTailInfo::default()
                } else {
                    read_uncompressed_overflow_tail(&store, pointer)?.unwrap_or_default()
                };
                self.persisted_tables_mut().insert(
                    table_name.clone(),
                    PersistedTableState {
                        pointer,
                        checksum: previous_state.checksum,
                        row_count: previous_state.row_count,
                        tail,
                        pk_index_root: previous_state.pk_index_root,
                    },
                );
                self.cache_payload_insert(table_name.clone(), payload);
                self.overflow_chain_caches.remove(&table_name);
            }
        }

        let (checksum, pointer) = {
            let manifest = self.manifest_payload()?;
            let checksum = crc32c_parts(&[manifest]);
            let previous_manifest_pointer = old_root.map_or(
                OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                |root| root.pointer,
            );
            let pointer = {
                let mut store = DbTxnPageStore { db };
                rewrite_overflow(
                    &mut store,
                    previous_manifest_pointer,
                    manifest,
                    CompressionMode::Auto,
                )?
            };
            (checksum, pointer)
        };

        let new_root = RootHeader {
            schema_cookie: self.catalog.schema_cookie,
            payload_checksum: checksum,
            pointer,
        };
        if old_root != Some(new_root) {
            let root_page = encode_root_header(db.config().page_size, new_root);
            db.write_page_owned(page::CATALOG_ROOT_PAGE_ID, root_page)?;
            self.root_state = Some(new_root);
            changed = true;
        }
        Ok(changed)
    }
    pub(crate) fn redefer_persisted_tables(&mut self, names: &[&str]) -> usize {
        let mut freed_bytes = 0usize;
        for name in names {
            let Some(table_name) = self.canonical_catalog_table_name(name) else {
                continue;
            };
            if self
                .persisted_tables
                .get(&table_name)
                .is_some_and(|state| state.pointer.is_table_paged_manifest())
            {
                if let Some(row_source) = self.tables_mut().remove(&table_name) {
                    freed_bytes = freed_bytes.saturating_add(row_source.approximate_heap_bytes());
                    self.deferred_tables_mut().insert(table_name.clone());
                    self.dirty_tables_mut().remove(&table_name);
                    self.paged_mutations.remove(&table_name);
                }
            }
        }
        freed_bytes
    }
    pub(crate) fn redefer_all_persisted_paged_tables(&mut self) -> usize {
        let paged_names: Vec<String> = self
            .persisted_tables
            .iter()
            .filter_map(|(name, state)| {
                if state.pointer.is_table_paged_manifest() && self.tables.contains_key(name) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        let name_refs: Vec<&str> = paged_names.iter().map(|s| s.as_str()).collect();
        self.redefer_persisted_tables(&name_refs)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_execute_observed_current_linear_three_table_view(
        &self,
        table_row_readers: &[DeferredViewTableRowReader<'_>],
        join_steps: &[DeferredViewJoinStep],
        join_keys: &[&RuntimeBtreeKeys],
        key_projection_indexes: &[Option<usize>],
        table_projections: &[DeferredViewTableProjection],
        projection_indexes: &[DeferredViewProjectionSource],
        lookup_row_id: i64,
        column_names: Vec<String>,
        linear_tail_can_move: bool,
    ) -> Result<Option<QueryResult>> {
        if table_row_readers.len() != 3
            || join_steps.len() != 2
            || table_projections.len() != 3
            || join_keys.len() != 2
            || key_projection_indexes.len() != 2
        {
            return Ok(None);
        }
        let step0 = &join_steps[0];
        let step1 = &join_steps[1];
        if step0.previous_table_index != 0
            || step0.current_table_index != 1
            || step1.previous_table_index != 1
            || step1.current_table_index != 2
        {
            return Ok(None);
        }
        let [keys0, keys1] = join_keys else {
            return Ok(None);
        };
        let [key0_projection_index, key1_projection_index] = key_projection_indexes else {
            return Ok(None);
        };
        let Some(source_row) = table_row_readers[0].read_projected_from_observed_cache(
            lookup_row_id,
            &table_projections[0].projection_indexes,
        )?
        else {
            return Ok(None);
        };
        let Some(source_row) = source_row else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };

        let key0_row_ids = match *key0_projection_index {
            Some(projection_index) => {
                let Some(key_value) = source_row.values.get(projection_index) else {
                    return Err(DbError::internal(
                        "observed-current view root row is shorter than planned schema",
                    ));
                };
                if matches!(key_value, Value::Null) {
                    return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
                }
                keys0.row_ids_for_value_set(key_value)?
            }
            None => keys0.row_ids_for_row_id(source_row.row_id),
        };

        let mut cache_available = true;
        let mut rows = Vec::with_capacity(64);
        key0_row_ids.visit_until(|row1_id| {
            let Some(row1) = table_row_readers[1].read_projected_from_observed_cache(
                row1_id,
                &table_projections[1].projection_indexes,
            )?
            else {
                cache_available = false;
                return Ok(true);
            };
            let Some(row1) = row1 else {
                return Ok(false);
            };
            let key1_row_ids = match *key1_projection_index {
                Some(projection_index) => {
                    let Some(key_value) = row1.values.get(projection_index) else {
                        return Err(DbError::internal(
                            "observed-current view join row is shorter than planned schema",
                        ));
                    };
                    if matches!(key_value, Value::Null) {
                        return Ok(false);
                    }
                    keys1.row_ids_for_value_set(key_value)?
                }
                None => keys1.row_ids_for_row_id(row1.row_id),
            };
            key1_row_ids.visit_until(|row2_id| {
                let Some(row2) = table_row_readers[2].read_projected_from_observed_cache(
                    row2_id,
                    &table_projections[2].projection_indexes,
                )?
                else {
                    cache_available = false;
                    return Ok(true);
                };
                let Some(row2) = row2 else {
                    return Ok(false);
                };
                rows.push(collect_deferred_view_query_row_from_linear_tail(
                    &source_row,
                    &row1,
                    row2,
                    projection_indexes,
                    "observed-current view row-id projection",
                    linear_tail_can_move,
                )?);
                Ok(false)
            })
        })?;
        if !cache_available {
            return Ok(None);
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    pub(crate) fn deferred_view_join_step(
        &self,
        table_bindings: &[TableBindingRef<'_>],
        table_schemas: &[&TableSchema],
        constraint: &Expr,
        current_table_index: usize,
    ) -> Result<Option<DeferredViewJoinStep>> {
        let Some(equalities) = simple_join_equalities(constraint) else {
            return Ok(None);
        };
        let current_binding = table_bindings[current_table_index];
        let mut matched = None;
        for (left_ref, right_ref) in equalities {
            for (previous_ref, current_ref) in [(left_ref, right_ref), (right_ref, left_ref)] {
                if !matches_table_binding(current_binding, current_ref.table) {
                    continue;
                }
                let Some(previous_table_index) = table_bindings[..current_table_index]
                    .iter()
                    .position(|binding| matches_table_binding(*binding, previous_ref.table))
                else {
                    continue;
                };
                let Some(previous_column_index) =
                    schema_column_index(table_schemas[previous_table_index], previous_ref.column)
                else {
                    return Ok(None);
                };
                let previous_is_rowid_alias =
                    rowid_alias_column_index(table_schemas[previous_table_index])
                        == Some(previous_column_index);
                let Some(current_index) = self
                    .simple_btree_index_for_table_column(current_binding.name, current_ref.column)
                else {
                    return Ok(None);
                };
                if matched
                    .replace(DeferredViewJoinStep {
                        previous_table_index,
                        previous_column_index,
                        current_table_index,
                        current_index_name: current_index.name.clone(),
                        previous_is_rowid_alias,
                    })
                    .is_some()
                {
                    return Ok(None);
                }
                if schema_column_index(table_schemas[current_table_index], current_ref.column)
                    .is_none()
                {
                    return Err(DbError::internal(
                        "deferred view join current column is missing from schema",
                    ));
                }
            }
        }
        Ok(matched)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_deferred_view_limit_rows_from_root<S: PageStore>(
        &self,
        store: &S,
        table_row_readers: &[DeferredViewTableRowReader<'_>],
        join_steps: &[DeferredViewJoinStep],
        join_keys: &[&RuntimeBtreeKeys],
        key_projection_indexes: &[Option<usize>],
        table_projections: &[DeferredViewTableProjection],
        projection_indexes: &[DeferredViewProjectionSource],
        root_row: StoredRow,
        offset_remaining: &mut usize,
        limit_remaining: &mut usize,
        rows: &mut Vec<QueryRow>,
        partial_rows: &mut Vec<StoredRow>,
        chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
        use_persistent_pk_index: bool,
        linear_tail_can_move: bool,
    ) -> Result<bool> {
        if let Some(stopped) = self.stream_deferred_view_linear_three_table_rows_from_root(
            store,
            table_row_readers,
            join_steps,
            join_keys,
            key_projection_indexes,
            table_projections,
            &root_row,
            use_persistent_pk_index,
            chunk_payload_cache,
            &mut |root_row, row1, row2| {
                if *offset_remaining > 0 {
                    *offset_remaining -= 1;
                    return Ok(false);
                }
                if *limit_remaining == 0 {
                    return Ok(true);
                }
                let row = collect_deferred_view_query_row_from_linear_tail(
                    root_row,
                    row1,
                    row2,
                    projection_indexes,
                    "deferred view limit projection",
                    linear_tail_can_move,
                )?;
                rows.push(row);
                *limit_remaining = (*limit_remaining).saturating_sub(1);
                Ok(*limit_remaining == 0)
            },
        )? {
            return Ok(stopped);
        }

        let Some(stopped) = self.stream_deferred_view_join_rows_from_root(
            store,
            table_row_readers,
            join_steps,
            join_keys,
            key_projection_indexes,
            table_projections,
            root_row,
            partial_rows,
            use_persistent_pk_index,
            true,
            chunk_payload_cache,
            &mut |partial| {
                if *offset_remaining > 0 {
                    *offset_remaining -= 1;
                    return Ok(false);
                }
                if *limit_remaining == 0 {
                    return Ok(true);
                }
                let values = collect_deferred_view_projection_values(
                    partial,
                    projection_indexes,
                    "deferred view limit projection",
                )?;
                rows.push(QueryRow::new(values));
                *limit_remaining = (*limit_remaining).saturating_sub(1);
                Ok(*limit_remaining == 0)
            },
        )?
        else {
            return Err(DbError::internal(
                "index was unavailable while executing deferred view limit join",
            ));
        };
        Ok(stopped)
    }
}
