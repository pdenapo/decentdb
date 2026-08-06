//! Thematic extraction (mechanical split; no behavior change).

use super::*;

#[cfg(test)]
pub(crate) fn paged_row_append_plan_count() -> u64 {
    PAGED_ROW_APPEND_PLAN_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn try_apply_paged_row_changes_to_manifest_update_only(
    manifest: &TablePageManifest,
    row_changes: &BTreeMap<i64, Option<Vec<Value>>>,
) -> Result<Option<TablePageManifest>> {
    if row_changes.is_empty() {
        return Ok(None);
    }

    let mut planned_changes = Vec::with_capacity(row_changes.len());
    for (row_id, change) in row_changes {
        let Some(next_values) = change.as_ref() else {
            return Ok(None);
        };
        let Some((_, entry)) = manifest.rows.entry_for_row_id(*row_id)? else {
            return Ok(None);
        };
        if entry.is_overlay {
            return Ok(None);
        }
        let chunk_index = usize::try_from(entry.chunk_index).map_err(|_| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        planned_changes.push((*row_id, chunk_index, next_values.clone()));
    }

    let mut updated_manifest = manifest.clone();
    Arc::make_mut(&mut updated_manifest.rows).sparse_mut()?;
    let chunks = Arc::make_mut(&mut updated_manifest.chunks);
    let tombstoned_row_ids = Arc::make_mut(&mut updated_manifest.tombstoned_row_ids);

    for (row_id, chunk_index, next_values) in planned_changes {
        let chunk = chunks.get_mut(chunk_index).ok_or_else(|| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        if chunk.tombstoned_row_ids.contains(&row_id) {
            return Ok(None);
        }

        let overlay_payload = chunk.overlay_payload.get_or_insert_with(|| {
            let mut payload = Vec::with_capacity(TABLE_PAYLOAD_MAGIC.len() + 4 + 128);
            payload.extend_from_slice(TABLE_PAYLOAD_MAGIC);
            payload.extend_from_slice(&0_u32.to_le_bytes());
            Arc::new(payload)
        });
        let mut encoded_values = Vec::with_capacity(128);
        Row::encode_values_into(&next_values, &mut encoded_values)?;
        append_encoded_table_payload_row(Arc::make_mut(overlay_payload), row_id, &encoded_values)?;
        Arc::make_mut(&mut chunk.tombstoned_row_ids).insert(row_id);
        chunk.overlay_pointer = None;
        chunk.overlay_checksum = None;
        tombstoned_row_ids.insert(row_id);
    }

    Ok(Some(updated_manifest))
}

pub(crate) fn patch_manifest_table_next_row_id(
    payload: &mut [u8],
    offset: usize,
    next_row_id: i64,
) -> Result<()> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| DbError::internal("manifest next_row_id offset overflow"))?;
    if end > payload.len() {
        return Err(DbError::internal(
            "manifest next_row_id offset exceeded payload length",
        ));
    }
    payload[offset..end].copy_from_slice(&next_row_id.to_le_bytes());
    Ok(())
}

pub(crate) fn patch_manifest_table_state(
    payload: &mut [u8],
    offset: usize,
    state: PersistedTableState,
) -> Result<()> {
    let end = offset
        .checked_add(13)
        .ok_or_else(|| DbError::internal("manifest table-state offset overflow"))?;
    if end > payload.len() {
        return Err(DbError::internal(
            "manifest table-state offset exceeded payload length",
        ));
    }
    payload[offset..offset + 4].copy_from_slice(&state.checksum.to_le_bytes());
    payload[offset + 4..offset + 8].copy_from_slice(&state.pointer.head_page_id.to_le_bytes());
    payload[offset + 8..offset + 12].copy_from_slice(&state.pointer.logical_len.to_le_bytes());
    payload[offset + 12] = state.pointer.flags;
    Ok(())
}

pub(crate) fn patch_manifest_table_pk_index_root(
    payload: &mut [u8],
    offset: usize,
    pk_index_root: Option<PageId>,
) -> Result<()> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| DbError::internal("manifest pk_index_root offset overflow"))?;
    if end > payload.len() {
        return Err(DbError::internal(
            "manifest pk_index_root offset exceeded payload length",
        ));
    }
    payload[offset..end].copy_from_slice(&pk_index_root.unwrap_or(0).to_le_bytes());
    Ok(())
}

pub(crate) fn paged_table_target_chunk_bytes(page_size: u32) -> usize {
    (page_size as usize)
        .saturating_mul(PAGED_TABLE_TARGET_CHUNK_PAGES)
        .max(TABLE_PAYLOAD_MAGIC.len() + 4)
}

pub(crate) fn paged_table_checkpoint_compaction_min_bytes(page_size: u32) -> usize {
    paged_table_target_chunk_bytes(page_size).saturating_div(2)
}

pub(crate) fn apply_paged_row_deletions_to_manifest(
    manifest: &TablePageManifest,
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<TablePageManifest> {
    if deleted_row_ids.is_empty() {
        return Ok(manifest.clone());
    }

    if let Some(updated) =
        try_apply_paged_row_deletions_to_manifest_without_base_decode(manifest, deleted_row_ids)?
    {
        return Ok(updated);
    }

    // Partition deleted row ids by the chunk that owns them, using the
    // manifest entry index. This avoids decoding any base payload row during a
    // pure bulk delete: base rows are immutable, so tombstoning by id is
    // sufficient, and only overlay rows that are updated-then-deleted need to
    // be decoded and dropped.
    let mut deletes_by_chunk: Vec<BTreeSet<i64>> = (0..manifest.chunks.len())
        .map(|_| BTreeSet::new())
        .collect();
    for &row_id in deleted_row_ids {
        let Some(chunk_index) = manifest.chunk_index_for_row_id(row_id) else {
            // Row id is not present in the manifest (already gone or never
            // existed). Skip it; callers already validated existence.
            continue;
        };
        if let Some(set) = deletes_by_chunk.get_mut(chunk_index) {
            set.insert(row_id);
        }
    }

    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut changed_chunk_indexes = BTreeSet::new();
    for (chunk_index, chunk) in manifest.chunks.iter().enumerate() {
        let Some(chunk_deletes) = deletes_by_chunk.get(chunk_index) else {
            new_chunks.push(chunk.clone());
            continue;
        };
        if chunk_deletes.is_empty() && chunk.overlay_payload.is_none() {
            new_chunks.push(chunk.clone());
            continue;
        }

        let mut new_tombstones: BTreeSet<i64> = chunk.tombstoned_row_ids.iter().copied().collect();
        let mut chunk_changed = false;
        let mut overlay_rows: BTreeMap<i64, StoredRow> = BTreeMap::new();

        // Drop overlay rows that are being deleted; keep the rest verbatim.
        if let Some(overlay_payload) = &chunk.overlay_payload {
            let previous_overlay_rows = decode_table_payload_rows(overlay_payload.as_slice())?;
            for previous_row in previous_overlay_rows {
                if chunk_deletes.contains(&previous_row.row_id) {
                    chunk_changed = true;
                } else {
                    overlay_rows.insert(previous_row.row_id, previous_row);
                }
            }
        }

        // Tombstone every deleted id that lives in this chunk's base payload.
        for &id in chunk_deletes {
            if new_tombstones.insert(id) {
                chunk_changed = true;
            }
        }

        if !chunk_changed {
            new_chunks.push(chunk.clone());
            continue;
        }

        let overlay_payload = if overlay_rows.is_empty() {
            None
        } else {
            let rows: Vec<StoredRow> = overlay_rows.into_values().collect();
            Some(Arc::new(encode_table_payload(&TableData::from_rows(rows))?))
        };

        new_chunks.push(TablePageManifestChunk {
            pointer: chunk.pointer,
            checksum: chunk.checksum,
            row_count: chunk.row_count,
            payload: Arc::clone(&chunk.payload),
            tombstoned_row_ids: Arc::new(new_tombstones),
            overlay_pointer: None,
            overlay_checksum: None,
            overlay_payload,
        });
        changed_chunk_indexes.insert(chunk_index);
    }

    rebuild_table_page_manifest_after_sparse_chunk_changes(
        manifest,
        new_chunks,
        &changed_chunk_indexes,
    )
}

pub(crate) fn try_apply_paged_row_deletions_to_manifest_without_base_decode(
    manifest: &TablePageManifest,
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<Option<TablePageManifest>> {
    let mut planned_deletions = Vec::new();
    try_reserve_paged_directory(
        &mut planned_deletions,
        deleted_row_ids.len(),
        "paged row deletions",
    )?;
    for &row_id in deleted_row_ids {
        let Some((entry_index, entry)) = manifest.rows.entry_for_row_id(row_id)? else {
            continue;
        };
        if entry.is_overlay {
            return Ok(None);
        }
        let chunk_index = usize::try_from(entry.chunk_index).map_err(|_| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        planned_deletions.push((entry_index, chunk_index, row_id));
    }

    if planned_deletions.is_empty() {
        return Ok(Some(manifest.clone()));
    }

    planned_deletions.sort_unstable_by_key(|(entry_index, _, _)| *entry_index);

    let mut updated_manifest = manifest.clone();
    let chunks = Arc::make_mut(&mut updated_manifest.chunks);
    let tombstoned_row_ids = Arc::make_mut(&mut updated_manifest.tombstoned_row_ids);

    let mut remaining_deletions = planned_deletions.iter().peekable();
    let rebuilt_len = manifest.rows.len().saturating_sub(planned_deletions.len());
    let mut rebuilt_rows = Vec::new();
    try_reserve_paged_directory(&mut rebuilt_rows, rebuilt_len, "sparse paged row entries")?;
    for (entry_index, entry) in manifest.rows.iter().enumerate() {
        let entry = entry?;
        if remaining_deletions
            .peek()
            .is_some_and(|(delete_entry_index, _, _)| *delete_entry_index == entry_index)
        {
            remaining_deletions.next();
            continue;
        }
        rebuilt_rows.push(entry);
    }
    updated_manifest.rows = Arc::new(TablePageDirectory::Sparse(rebuilt_rows));

    for &(_, chunk_index, row_id) in &planned_deletions {
        let chunk = chunks.get_mut(chunk_index).ok_or_else(|| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        chunk.row_count = chunk
            .row_count
            .checked_sub(1)
            .ok_or_else(|| DbError::corruption("paged table chunk row count underflow"))?;
        Arc::make_mut(&mut chunk.tombstoned_row_ids).insert(row_id);
        tombstoned_row_ids.insert(row_id);
    }

    Ok(Some(updated_manifest))
}

pub(crate) fn apply_paged_row_changes_to_manifest(
    manifest: &TablePageManifest,
    row_changes: &BTreeMap<i64, Option<Vec<Value>>>,
) -> Result<TablePageManifest> {
    if row_changes.is_empty() {
        return Ok(manifest.clone());
    }
    if let Some(updated) =
        try_apply_paged_row_changes_to_manifest_update_only(manifest, row_changes)?
    {
        return Ok(updated);
    }

    let mut changes_by_chunk: Vec<BTreeMap<i64, &Option<Vec<Value>>>> = (0..manifest.chunks.len())
        .map(|_| BTreeMap::new())
        .collect();
    for (row_id, change) in row_changes {
        let Some(chunk_index) = manifest.chunk_index_for_row_id(*row_id) else {
            continue;
        };
        if let Some(chunk_changes) = changes_by_chunk.get_mut(chunk_index) {
            chunk_changes.insert(*row_id, change);
        }
    }

    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut changed_chunk_indexes = BTreeSet::new();
    for (chunk_index, chunk) in manifest.chunks.iter().enumerate() {
        let Some(chunk_changes) = changes_by_chunk.get(chunk_index) else {
            new_chunks.push(chunk.clone());
            continue;
        };
        if chunk_changes.is_empty() {
            new_chunks.push(chunk.clone());
            continue;
        }

        let mut new_tombstones: BTreeSet<i64> = chunk.tombstoned_row_ids.iter().copied().collect();
        // Use BTreeMap so each row_id appears at most once in the overlay.
        let mut overlay_rows: BTreeMap<i64, StoredRow> = BTreeMap::new();
        let mut reactivated_base_rows = BTreeSet::new();
        let mut chunk_changed = false;

        // Scan the base payload: tombstone touched rows and queue their
        // replacements in the overlay map.
        let previous_rows = decode_table_payload_rows(chunk.payload.as_slice())?;
        for previous_row in previous_rows {
            match chunk_changes.get(&previous_row.row_id).copied() {
                Some(Some(next_values)) => {
                    chunk_changed = true;
                    if chunk.tombstoned_row_ids.contains(&previous_row.row_id)
                        && previous_row.values == *next_values
                    {
                        new_tombstones.remove(&previous_row.row_id);
                        reactivated_base_rows.insert(previous_row.row_id);
                        overlay_rows.remove(&previous_row.row_id);
                    } else {
                        new_tombstones.insert(previous_row.row_id);
                        overlay_rows.insert(
                            previous_row.row_id,
                            StoredRow {
                                row_id: previous_row.row_id,
                                values: next_values.clone(),
                            },
                        );
                    }
                }
                Some(None) => {
                    chunk_changed = true;
                    new_tombstones.insert(previous_row.row_id);
                }
                None => {}
            }
        }

        // Scan any existing overlay: replace rows that are re-updated,
        // keep untouched rows, drop rows that are deleted.
        if let Some(overlay_payload) = &chunk.overlay_payload {
            let previous_overlay_rows = decode_table_payload_rows(overlay_payload.as_slice())?;
            for previous_row in previous_overlay_rows {
                match chunk_changes.get(&previous_row.row_id).copied() {
                    Some(Some(next_values)) => {
                        chunk_changed = true;
                        if reactivated_base_rows.contains(&previous_row.row_id) {
                            overlay_rows.remove(&previous_row.row_id);
                        } else {
                            overlay_rows.insert(
                                previous_row.row_id,
                                StoredRow {
                                    row_id: previous_row.row_id,
                                    values: next_values.clone(),
                                },
                            );
                        }
                    }
                    Some(None) => {
                        chunk_changed = true;
                        overlay_rows.remove(&previous_row.row_id);
                    }
                    None => {
                        overlay_rows
                            .entry(previous_row.row_id)
                            .or_insert(previous_row);
                    }
                }
            }
        }

        if !chunk_changed {
            new_chunks.push(chunk.clone());
            continue;
        }

        let overlay_payload = if overlay_rows.is_empty() {
            None
        } else {
            let rows: Vec<StoredRow> = overlay_rows.into_values().collect();
            Some(Arc::new(encode_table_payload(&TableData::from_rows(rows))?))
        };

        new_chunks.push(TablePageManifestChunk {
            pointer: chunk.pointer,
            checksum: chunk.checksum,
            row_count: chunk.row_count,
            payload: Arc::clone(&chunk.payload),
            tombstoned_row_ids: Arc::new(new_tombstones),
            overlay_pointer: None,
            overlay_checksum: None,
            overlay_payload,
        });
        changed_chunk_indexes.insert(chunk_index);
    }

    rebuild_table_page_manifest_after_sparse_chunk_changes(
        manifest,
        new_chunks,
        &changed_chunk_indexes,
    )
}

pub(crate) fn rebuild_table_page_manifest_after_sparse_chunk_changes(
    manifest: &TablePageManifest,
    mut new_chunks: Vec<TablePageManifestChunk>,
    changed_chunk_indexes: &BTreeSet<usize>,
) -> Result<TablePageManifest> {
    if changed_chunk_indexes.is_empty() {
        return Ok(manifest.clone());
    }

    let tombstoned_row_ids = new_chunks
        .iter()
        .flat_map(|chunk| chunk.tombstoned_row_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    try_reserve_paged_directory(&mut rows, manifest.rows.len(), "sparse paged row entries")?;
    for entry in manifest.rows.iter() {
        let entry = entry?;
        if !changed_chunk_indexes.contains(&(entry.chunk_index as usize)) {
            rows.push(entry);
        }
    }
    for chunk_index in changed_chunk_indexes {
        let Some(chunk) = new_chunks.get(*chunk_index) else {
            return Err(DbError::corruption(
                "paged table changed chunk index exceeded chunk list length",
            ));
        };
        let chunk_rows = table_page_entries_for_chunk(*chunk_index, chunk)?;
        if let Some(chunk) = new_chunks.get_mut(*chunk_index) {
            chunk.row_count = chunk_rows.len();
        }
        rows.extend(chunk_rows);
    }
    rows.sort_by_key(|entry| entry.row_id);
    #[cfg(debug_assertions)]
    {
        for window in rows.windows(2) {
            assert_ne!(
                window[0].row_id, window[1].row_id,
                "duplicate row_id in TablePageManifest rows"
            );
        }
    }

    Ok(TablePageManifest {
        chunks: Arc::new(new_chunks),
        rows: Arc::new(TablePageDirectory::Sparse(rows)),
        tombstoned_row_ids: Arc::new(tombstoned_row_ids),
    })
}

pub(crate) fn read_table_page_manifest_from_state<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<TablePageManifest> {
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return TablePageManifest::from_chunks(Vec::new());
    }
    if state.pointer.is_table_paged_manifest() {
        return TablePageManifest::from_chunks(read_paged_table_chunk_payloads(store, state)?);
    }
    let payload = Arc::new(read_overflow(store, state.pointer)?);
    if crc32c_parts(&[payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption("table payload checksum mismatch"));
    }
    TablePageManifest::from_payload(payload)
}

pub(crate) fn table_page_manifest_chunk_is_plain(chunk: &TablePageManifestChunk) -> bool {
    chunk.tombstoned_row_ids.is_empty()
        && chunk.overlay_pointer.is_none()
        && chunk.overlay_checksum.is_none()
        && chunk.overlay_payload.is_none()
}

pub(crate) fn table_page_manifest_chunk_visible_row_count(
    chunk: &TablePageManifestChunk,
) -> Result<usize> {
    let base_physical = read_table_payload_row_count_from_bytes(&chunk.payload)?;
    let overlay_physical = chunk
        .overlay_payload
        .as_ref()
        .map(|payload| read_table_payload_row_count_from_bytes(payload))
        .transpose()?
        .unwrap_or(0);
    Ok(base_physical
        .saturating_sub(chunk.tombstoned_row_ids.len())
        .saturating_add(overlay_physical))
}

pub(crate) fn table_page_manifest_with_persisted_chunks(
    current: &TablePageManifest,
    persisted_chunks: &[TablePageManifestChunk],
) -> TablePageManifest {
    TablePageManifest {
        chunks: Arc::new(persisted_chunks.to_vec()),
        rows: Arc::clone(&current.rows),
        tombstoned_row_ids: Arc::clone(&current.tombstoned_row_ids),
    }
}

pub(crate) fn try_append_only_paged_table_from_manifest<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    manifest: &TablePageManifest,
) -> Result<Option<(PersistedTableState, Vec<TablePageManifestChunk>)>> {
    if previous_state.pointer.head_page_id == 0 || !previous_state.pointer.is_table_paged_manifest()
    {
        return Ok(None);
    }
    if manifest.chunks.is_empty() {
        return Ok(None);
    }

    let manifest_payload = read_overflow(store, previous_state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != previous_state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let previous_manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    if previous_manifest.chunks.is_empty() || manifest.chunks.len() < previous_manifest.chunks.len()
    {
        return Ok(None);
    }

    let previous_tail_index = previous_manifest.chunks.len() - 1;
    let mut first_changed_index = None;
    let mut current_checksums = Vec::with_capacity(manifest.chunks.len());
    for (index, current_chunk) in manifest.chunks.iter().enumerate() {
        if !table_page_manifest_chunk_is_plain(current_chunk) {
            return Ok(None);
        }
        let checksum = crc32c_parts(&[current_chunk.payload.as_slice()]);
        current_checksums.push(checksum);
        let Some(previous_chunk) = previous_manifest.chunks.get(index) else {
            continue;
        };
        if !persisted_paged_chunk_is_plain(previous_chunk) {
            return Ok(None);
        }
        if previous_chunk.checksum == checksum
            && previous_chunk.row_count == current_chunk.row_count
        {
            continue;
        }
        if index != previous_tail_index {
            return Ok(None);
        }
        first_changed_index = Some(index);
    }

    if first_changed_index.is_none() && manifest.chunks.len() == previous_manifest.chunks.len() {
        return Ok(None);
    }

    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut persisted_chunks = Vec::with_capacity(manifest.chunks.len());

    for (index, current_chunk) in manifest.chunks.iter().enumerate() {
        let checksum = current_checksums[index];
        if let Some(previous_chunk) = previous_manifest.chunks.get(index) {
            if previous_chunk.checksum == checksum
                && previous_chunk.row_count == current_chunk.row_count
            {
                new_chunks.push(previous_chunk.clone());
                persisted_chunks.push(persisted_chunk_from_current(
                    previous_chunk.pointer,
                    previous_chunk.checksum,
                    previous_chunk.row_count,
                    current_chunk,
                ));
                continue;
            }

            let pointer = rewrite_overflow(
                store,
                previous_chunk.pointer,
                current_chunk.payload.as_slice(),
                CompressionMode::Never,
            )?;
            new_chunks.push(PersistedTableChunkState {
                pointer,
                checksum,
                row_count: current_chunk.row_count,
                tombstoned_row_ids: Vec::new(),
                overlay_pointer: None,
                overlay_checksum: None,
            });
            persisted_chunks.push(persisted_chunk_from_current(
                pointer,
                checksum,
                current_chunk.row_count,
                current_chunk,
            ));
            continue;
        }

        let pointer = write_overflow(
            store,
            current_chunk.payload.as_slice(),
            CompressionMode::Never,
        )?;
        new_chunks.push(PersistedTableChunkState {
            pointer,
            checksum,
            row_count: current_chunk.row_count,
            tombstoned_row_ids: Vec::new(),
            overlay_pointer: None,
            overlay_checksum: None,
        });
        persisted_chunks.push(persisted_chunk_from_current(
            pointer,
            checksum,
            current_chunk.row_count,
            current_chunk,
        ));
    }

    let updated_manifest_payload =
        encode_paged_table_manifest_payload(&PersistedPagedTableManifest { chunks: new_chunks })?;
    let checksum = crc32c_parts(&[updated_manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        previous_state.pointer.with_table_paged_manifest(false),
        &updated_manifest_payload,
        CompressionMode::Never,
    )?
    .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();

    Ok(Some((
        PersistedTableState {
            pointer,
            checksum,
            row_count: manifest.row_count(),
            tail,
            pk_index_root: previous_state.pk_index_root,
        },
        persisted_chunks,
    )))
}

pub(crate) fn paged_table_state_needs_checkpoint_compaction<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<bool> {
    if state.pointer.head_page_id == 0 || !state.pointer.is_table_paged_manifest() {
        return Ok(false);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let chunk_compaction_min_bytes = paged_table_checkpoint_compaction_min_bytes(store.page_size());
    for chunk in &manifest.chunks {
        if !chunk.tombstoned_row_ids.is_empty() || chunk.overlay_pointer.is_some() {
            return Ok(true);
        }
        if chunk.pointer.head_page_id != 0
            && !chunk.pointer.is_compressed()
            && usize::try_from(chunk.pointer.logical_len)
                .ok()
                .is_some_and(|len| len >= chunk_compaction_min_bytes)
        {
            return Ok(true);
        }
    }

    Ok(!state.pointer.is_compressed()
        && usize::try_from(state.pointer.logical_len)
            .ok()
            .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES))
}

pub(crate) fn rewrite_paged_table_from_resident<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    data: &TableData,
    page_size: u32,
) -> Result<PersistedTableState> {
    if previous_state.pointer.head_page_id == 0 || !previous_state.pointer.is_table_paged_manifest()
    {
        let encoded_chunks = encode_paged_table_chunks(data, page_size)?;
        return persist_paged_table(store, previous_state, &encoded_chunks, data.rows.len());
    }

    let manifest_payload = read_overflow(store, previous_state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != previous_state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut current_rows_by_id = Int64Map::default();
    for row in data.visible_rows() {
        current_rows_by_id.insert(row.row_id, row);
    }

    let mut seen_old_row_ids = std::collections::BTreeSet::new();
    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut replaced_chunk_pointers = Vec::new();
    let mut changed = false;

    for chunk in manifest.chunks {
        let payload = read_overflow(store, chunk.pointer)?;
        if crc32c_parts(&[payload.as_slice()]) != chunk.checksum {
            return Err(DbError::corruption("paged table chunk checksum mismatch"));
        }
        let previous_rows = decode_table_payload_rows(payload.as_slice())?;
        let mut current_chunk_rows = Vec::with_capacity(previous_rows.len());
        let mut chunk_changed = false;

        for previous_row in &previous_rows {
            seen_old_row_ids.insert(previous_row.row_id);
            if let Some(current_row) = current_rows_by_id.get(&previous_row.row_id).copied() {
                if current_row.values != previous_row.values {
                    chunk_changed = true;
                }
                current_chunk_rows.push(current_row.clone());
            } else {
                chunk_changed = true;
            }
        }

        if !chunk_changed {
            new_chunks.push(chunk);
            continue;
        }

        changed = true;
        replaced_chunk_pointers.push(chunk.pointer);
        for encoded_chunk in encode_paged_table_chunks_from_rows(&current_chunk_rows, page_size)? {
            let pointer = write_overflow(store, &encoded_chunk.payload, CompressionMode::Never)?;
            new_chunks.push(PersistedTableChunkState {
                pointer,
                checksum: encoded_chunk.checksum,
                row_count: encoded_chunk.row_count,
                tombstoned_row_ids: Vec::new(),
                overlay_pointer: None,
                overlay_checksum: None,
            });
        }
    }

    let appended_rows = data
        .visible_rows()
        .filter(|row| !seen_old_row_ids.contains(&row.row_id))
        .cloned()
        .collect::<Vec<_>>();
    if !appended_rows.is_empty() {
        changed = true;
        for encoded_chunk in encode_paged_table_chunks_from_rows(&appended_rows, page_size)? {
            let pointer = write_overflow(store, &encoded_chunk.payload, CompressionMode::Never)?;
            new_chunks.push(PersistedTableChunkState {
                pointer,
                checksum: encoded_chunk.checksum,
                row_count: encoded_chunk.row_count,
                tombstoned_row_ids: Vec::new(),
                overlay_pointer: None,
                overlay_checksum: None,
            });
        }
    }

    if !changed {
        return Ok(previous_state);
    }

    if new_chunks.is_empty() {
        free_persisted_table_bytes(store, previous_state)?;
        return Ok(PersistedTableState {
            pointer: OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            },
            checksum: 0,
            row_count: 0,
            tail: OverflowTailInfo::default(),
            pk_index_root: previous_state.pk_index_root,
        });
    }

    let updated_manifest_payload =
        encode_paged_table_manifest_payload(&PersistedPagedTableManifest { chunks: new_chunks })?;
    let checksum = crc32c_parts(&[updated_manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        previous_state.pointer.with_table_paged_manifest(false),
        &updated_manifest_payload,
        CompressionMode::Never,
    )?
    .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();

    for replaced_pointer in replaced_chunk_pointers {
        if replaced_pointer.head_page_id != 0 {
            free_overflow(store, replaced_pointer.head_page_id)?;
        }
    }

    Ok(PersistedTableState {
        pointer,
        checksum,
        row_count: data.row_count(),
        tail,
        pk_index_root: previous_state.pk_index_root,
    })
}

/// Delete-only variant of [`rewrite_paged_table_from_resident`]. When the
/// transaction only deleted rows (no updates, no appends), the surviving rows
/// are a strict subset of the previous on-disk rows. Chunks that contain no
/// deleted row id are byte-for-byte unchanged and can be reused verbatim
/// without decoding their row values; only chunks that actually hold deleted
/// rows are re-encoded. This avoids decoding every row's values during a bulk
/// delete on a paged table, which previously dominated commit wall time.
pub(crate) fn rewrite_paged_table_from_resident_delete_only<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    data: &TableData,
    page_size: u32,
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<PersistedTableState> {
    if previous_state.pointer.head_page_id == 0 || !previous_state.pointer.is_table_paged_manifest()
    {
        let encoded_chunks = encode_paged_table_chunks(data, page_size)?;
        return persist_paged_table(store, previous_state, &encoded_chunks, data.rows.len());
    }

    let manifest_payload = read_overflow(store, previous_state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != previous_state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;

    // Index the surviving rows by id so we can re-encode affected chunks from
    // the resident data without re-reading them from disk.
    let mut current_rows_by_id = Int64Map::default();
    for row in data.visible_rows() {
        current_rows_by_id.insert(row.row_id, row);
    }

    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut replaced_chunk_pointers = Vec::new();
    let mut changed = false;

    for chunk in manifest.chunks {
        let payload = read_overflow(store, chunk.pointer)?;
        if crc32c_parts(&[payload.as_slice()]) != chunk.checksum {
            return Err(DbError::corruption("paged table chunk checksum mismatch"));
        }
        // Fast path: scan row ids only (skip value decode) to determine whether
        // this chunk contains any deleted row. If not, reuse the chunk as-is.
        let chunk_row_ids = scan_table_payload_row_ids(payload.as_slice())?;
        let chunk_has_deletes = chunk_row_ids.iter().any(|id| deleted_row_ids.contains(id));
        if !chunk_has_deletes {
            new_chunks.push(chunk);
            continue;
        }

        changed = true;
        replaced_chunk_pointers.push(chunk.pointer);
        let previous_rows = decode_table_payload_rows(payload.as_slice())?;
        let mut current_chunk_rows = Vec::with_capacity(previous_rows.len());
        for previous_row in &previous_rows {
            if let Some(current_row) = current_rows_by_id.get(&previous_row.row_id).copied() {
                current_chunk_rows.push(current_row.clone());
            }
        }
        for encoded_chunk in encode_paged_table_chunks_from_rows(&current_chunk_rows, page_size)? {
            let pointer = write_overflow(store, &encoded_chunk.payload, CompressionMode::Never)?;
            new_chunks.push(PersistedTableChunkState {
                pointer,
                checksum: encoded_chunk.checksum,
                row_count: encoded_chunk.row_count,
                tombstoned_row_ids: Vec::new(),
                overlay_pointer: None,
                overlay_checksum: None,
            });
        }
    }

    if !changed {
        return Ok(previous_state);
    }

    if new_chunks.is_empty() {
        free_persisted_table_bytes(store, previous_state)?;
        return Ok(PersistedTableState {
            pointer: OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            },
            checksum: 0,
            row_count: 0,
            tail: OverflowTailInfo::default(),
            pk_index_root: previous_state.pk_index_root,
        });
    }

    let updated_manifest_payload =
        encode_paged_table_manifest_payload(&PersistedPagedTableManifest { chunks: new_chunks })?;
    let checksum = crc32c_parts(&[updated_manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        previous_state.pointer.with_table_paged_manifest(false),
        &updated_manifest_payload,
        CompressionMode::Never,
    )?
    .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();

    for replaced_pointer in replaced_chunk_pointers {
        if replaced_pointer.head_page_id != 0 {
            free_overflow(store, replaced_pointer.head_page_id)?;
        }
    }

    Ok(PersistedTableState {
        pointer,
        checksum,
        row_count: data.row_count(),
        tail,
        pk_index_root: previous_state.pk_index_root,
    })
}

pub(crate) fn rewrite_paged_table_from_manifest<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    manifest: &TablePageManifest,
) -> Result<(PersistedTableState, Vec<TablePageManifestChunk>)> {
    if manifest.chunks.is_empty() {
        if previous_state.pointer.head_page_id != 0 {
            free_persisted_table_bytes(store, previous_state)?;
        }
        return Ok((
            PersistedTableState {
                pointer: OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                checksum: 0,
                row_count: 0,
                tail: OverflowTailInfo::default(),
                pk_index_root: previous_state.pk_index_root,
            },
            Vec::new(),
        ));
    }

    let previous_chunks = if previous_state.pointer.head_page_id != 0
        && previous_state.pointer.is_table_paged_manifest()
    {
        let manifest_payload = read_overflow(store, previous_state.pointer)?;
        if crc32c_parts(&[manifest_payload.as_slice()]) != previous_state.checksum {
            return Err(DbError::corruption(
                "paged table manifest checksum mismatch",
            ));
        }
        decode_paged_table_manifest_payload(&manifest_payload)?.chunks
    } else {
        Vec::new()
    };
    let mut previous_payloads = None;
    let mut reused_previous = vec![false; previous_chunks.len()];
    let mut replaced_overlay_pointers = Vec::new();

    let mut new_chunks = Vec::with_capacity(manifest.chunks.len());
    let mut persisted_chunks = Vec::with_capacity(manifest.chunks.len());

    for (current_index, current_chunk) in manifest.chunks.iter().enumerate() {
        if let Some(chunk_state) = previous_chunks.get(current_index) {
            if persisted_chunk_metadata_matches_current(chunk_state, current_chunk) {
                reused_previous[current_index] = true;
                persisted_chunks.push(TablePageManifestChunk {
                    pointer: chunk_state.pointer,
                    checksum: chunk_state.checksum,
                    row_count: chunk_state.row_count,
                    payload: Arc::clone(&current_chunk.payload),
                    tombstoned_row_ids: Arc::clone(&current_chunk.tombstoned_row_ids),
                    overlay_pointer: chunk_state.overlay_pointer,
                    overlay_checksum: chunk_state.overlay_checksum,
                    overlay_payload: current_chunk.overlay_payload.clone(),
                });
                new_chunks.push(chunk_state.clone());
                continue;
            }
            if chunk_state.pointer.head_page_id != 0
                && chunk_state.pointer == current_chunk.pointer
                && chunk_state.checksum == current_chunk.checksum
            {
                reused_previous[current_index] = true;
                let current_overlay_checksum = current_chunk
                    .overlay_payload
                    .as_ref()
                    .map(|payload| crc32c_parts(&[payload.as_slice()]));
                let (overlay_pointer, overlay_checksum) = match (
                    &current_chunk.overlay_payload,
                    current_chunk.overlay_pointer,
                    current_chunk.overlay_checksum,
                ) {
                    (Some(_), Some(pointer), Some(checksum))
                        if Some(pointer) == chunk_state.overlay_pointer
                            && Some(checksum) == chunk_state.overlay_checksum =>
                    {
                        (Some(pointer), Some(checksum))
                    }
                    (Some(overlay_payload), _, _) => {
                        let pointer = write_overflow(
                            store,
                            overlay_payload.as_slice(),
                            CompressionMode::Never,
                        )?;
                        let checksum = current_overlay_checksum.ok_or_else(|| {
                            DbError::internal("overlay checksum missing for paged table chunk")
                        })?;
                        (Some(pointer), Some(checksum))
                    }
                    (None, _, _) => (None, None),
                };
                if let Some(previous_overlay_pointer) = chunk_state.overlay_pointer {
                    if Some(previous_overlay_pointer) != overlay_pointer
                        && previous_overlay_pointer.head_page_id != 0
                    {
                        replaced_overlay_pointers.push(previous_overlay_pointer);
                    }
                }
                let visible = table_page_manifest_chunk_visible_row_count(current_chunk)?;
                new_chunks.push(PersistedTableChunkState {
                    pointer: chunk_state.pointer,
                    checksum: chunk_state.checksum,
                    row_count: visible,
                    tombstoned_row_ids: current_chunk.tombstoned_row_ids.iter().copied().collect(),
                    overlay_pointer,
                    overlay_checksum,
                });
                persisted_chunks.push(TablePageManifestChunk {
                    pointer: chunk_state.pointer,
                    checksum: chunk_state.checksum,
                    row_count: visible,
                    payload: Arc::clone(&current_chunk.payload),
                    tombstoned_row_ids: Arc::clone(&current_chunk.tombstoned_row_ids),
                    overlay_pointer,
                    overlay_checksum,
                    overlay_payload: current_chunk.overlay_payload.clone(),
                });
                continue;
            }
        }

        if previous_payloads.is_none() {
            if previous_state.pointer.head_page_id != 0
                && previous_state.pointer.is_table_paged_manifest()
            {
                previous_payloads = Some(read_paged_table_chunk_payloads(store, previous_state)?);
            } else {
                previous_payloads = Some(Vec::new());
            }
        }
        let reusable_previous = previous_payloads
            .as_ref()
            .expect("previous payloads loaded");
        let checksum = crc32c_parts(&[current_chunk.payload.as_slice()]);
        let current_overlay_checksum = current_chunk
            .overlay_payload
            .as_ref()
            .map(|payload| crc32c_parts(&[payload.as_slice()]));
        let reused_index = reusable_previous
            .iter()
            .enumerate()
            .find_map(|(index, payload)| {
                let chunk_state = previous_chunks.get(index)?;
                let overlay_match = match (
                    &payload.overlay_payload,
                    &current_chunk.overlay_payload,
                    chunk_state.overlay_checksum,
                    current_overlay_checksum,
                ) {
                    (None, None, None, None) => true,
                    (Some(previous), Some(current), Some(previous_checksum), Some(checksum)) => {
                        previous_checksum == checksum && previous.as_slice() == current.as_slice()
                    }
                    _ => false,
                };
                (!reused_previous[index]
                    && chunk_state.checksum == checksum
                    && payload.payload.as_slice() == current_chunk.payload.as_slice()
                    && chunk_state.row_count == current_chunk.row_count
                    && chunk_state.tombstoned_row_ids.len()
                        == current_chunk.tombstoned_row_ids.len()
                    && chunk_state
                        .tombstoned_row_ids
                        .iter()
                        .all(|id| current_chunk.tombstoned_row_ids.contains(id))
                    && overlay_match)
                    .then_some(index)
            });
        if let Some(index) = reused_index {
            reused_previous[index] = true;
            let chunk_state = previous_chunks[index].clone();
            persisted_chunks.push(TablePageManifestChunk {
                pointer: chunk_state.pointer,
                checksum: chunk_state.checksum,
                row_count: chunk_state.row_count,
                payload: Arc::clone(&current_chunk.payload),
                tombstoned_row_ids: Arc::clone(&current_chunk.tombstoned_row_ids),
                overlay_pointer: chunk_state.overlay_pointer,
                overlay_checksum: chunk_state.overlay_checksum,
                overlay_payload: current_chunk.overlay_payload.clone(),
            });
            new_chunks.push(chunk_state);
            continue;
        }

        let pointer = write_overflow(store, &current_chunk.payload, CompressionMode::Never)?;
        let (overlay_pointer, overlay_checksum) =
            if let Some(overlay_payload) = &current_chunk.overlay_payload {
                let overlay_pointer =
                    write_overflow(store, overlay_payload.as_slice(), CompressionMode::Never)?;
                let overlay_checksum = crc32c_parts(&[overlay_payload.as_slice()]);
                (Some(overlay_pointer), Some(overlay_checksum))
            } else {
                (None, None)
            };
        let visible = table_page_manifest_chunk_visible_row_count(current_chunk)?;
        new_chunks.push(PersistedTableChunkState {
            pointer,
            checksum,
            row_count: visible,
            tombstoned_row_ids: current_chunk.tombstoned_row_ids.iter().copied().collect(),
            overlay_pointer,
            overlay_checksum,
        });
        persisted_chunks.push(TablePageManifestChunk {
            pointer,
            checksum,
            row_count: visible,
            payload: Arc::clone(&current_chunk.payload),
            tombstoned_row_ids: Arc::clone(&current_chunk.tombstoned_row_ids),
            overlay_pointer,
            overlay_checksum,
            overlay_payload: current_chunk.overlay_payload.clone(),
        });
    }

    if new_chunks == previous_chunks {
        return Ok((previous_state, persisted_chunks));
    }

    let updated_manifest_payload =
        encode_paged_table_manifest_payload(&PersistedPagedTableManifest { chunks: new_chunks })?;
    let checksum = crc32c_parts(&[updated_manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        previous_state.pointer.with_table_paged_manifest(false),
        &updated_manifest_payload,
        CompressionMode::Never,
    )?
    .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();

    for (index, chunk_state) in previous_chunks.iter().enumerate() {
        if reused_previous[index] || chunk_state.pointer.head_page_id == 0 {
            continue;
        }
        free_overflow(store, chunk_state.pointer.head_page_id)?;
        if let Some(overlay_pointer) = chunk_state.overlay_pointer {
            if overlay_pointer.head_page_id != 0 {
                free_overflow(store, overlay_pointer.head_page_id)?;
            }
        }
    }
    for overlay_pointer in replaced_overlay_pointers {
        if overlay_pointer.head_page_id != 0 {
            free_overflow(store, overlay_pointer.head_page_id)?;
        }
    }

    Ok((
        PersistedTableState {
            pointer,
            checksum,
            row_count: manifest.row_count(),
            tail,
            pk_index_root: previous_state.pk_index_root,
        },
        persisted_chunks,
    ))
}

pub(crate) fn splice_updated_rows_payload_in_place(
    payload: &mut [u8],
    data: &TableData,
    dirty_indices: &[usize],
) -> Result<Option<SpliceDirtyRange>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }
    let old_row_count =
        u32::from_le_bytes(payload[8..12].try_into().expect("row-count header length")) as usize;
    if old_row_count != data.rows.len() {
        return Ok(None);
    }

    let mut sorted_dirty: Vec<usize> = dirty_indices.to_vec();
    sorted_dirty.sort_unstable();
    sorted_dirty.dedup();
    if sorted_dirty.is_empty() {
        return Ok(Some(SpliceDirtyRange {
            first_dirty_byte: payload.len(),
            last_dirty_byte: payload.len(),
        }));
    }

    let mut row_spans: Vec<(usize, usize)> = Vec::with_capacity(sorted_dirty.len());
    let mut scan_offset = HEADER_LEN;
    let mut dirty_cursor = 0;
    let mut row_idx = 0;
    while dirty_cursor < sorted_dirty.len() && scan_offset + 12 <= payload.len() {
        if row_idx >= old_row_count {
            break;
        }
        let row_id = i64::from_le_bytes(
            payload[scan_offset..scan_offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let row_data_len = u32::from_le_bytes(
            payload[scan_offset + 8..scan_offset + 12]
                .try_into()
                .expect("row data len"),
        ) as usize;
        let row_end = scan_offset.saturating_add(12).saturating_add(row_data_len);
        if row_end > payload.len() {
            return Ok(None);
        }
        if row_idx == sorted_dirty[dirty_cursor] {
            let Some(row) = data.rows.get(row_idx) else {
                return Ok(None);
            };
            if row.row_id != row_id {
                return Ok(None);
            }
            row_spans.push((scan_offset, row_end));
            dirty_cursor += 1;
            if dirty_cursor == sorted_dirty.len() {
                break;
            }
        }
        scan_offset = row_end;
        row_idx += 1;
    }
    if row_spans.len() != sorted_dirty.len() {
        return Ok(None);
    }

    let mut encoded_rows: Vec<Vec<u8>> = Vec::with_capacity(sorted_dirty.len());
    let mut encoded_row = Vec::with_capacity(128);
    for (span_idx, &dirty_row) in sorted_dirty.iter().enumerate() {
        let Some(row) = data.rows.get(dirty_row) else {
            return Ok(None);
        };
        let (span_start, span_end) = row_spans[span_idx];
        let old_row_body_len = span_end.saturating_sub(span_start).saturating_sub(12);
        encoded_row.clear();
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        if encoded_row.len() > old_row_body_len {
            return Ok(None);
        }
        encoded_rows.push(encoded_row.clone());
    }

    for (span_idx, encoded_row) in encoded_rows.iter().enumerate() {
        let (span_start, span_end) = row_spans[span_idx];
        let body_start = span_start.saturating_add(12);
        let body_written_end = body_start.saturating_add(encoded_row.len());
        payload[body_start..body_written_end].copy_from_slice(encoded_row);
        payload[body_written_end..span_end].fill(0);
    }

    Ok(Some(SpliceDirtyRange {
        first_dirty_byte: row_spans
            .first()
            .map_or(0, |span| span.0.saturating_add(12)),
        last_dirty_byte: row_spans.last().map_or(payload.len(), |span| span.1),
    }))
}

/// ADR 0200: mark the given row ids as deleted in place by setting the
/// tombstone flag on each slot's `row_body_len` field. The body bytes are left
/// untouched, so only four bytes per deleted row change and the payload length
/// is unchanged. This collapses a scattered delete from "rewrite every byte
/// after the first deletion" down to "patch one length field per deleted row".
///
/// Returns the dirty byte ranges to persist, or `None` when the payload is not
/// a recognizable resident table payload or a targeted row id is absent / already
/// tombstoned — in which case the caller falls back to the splice / full
/// re-encode path.
pub(crate) fn tombstone_deleted_rows_payload_in_place(
    payload: &mut [u8],
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<Option<Vec<Range<usize>>>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }
    let row_count =
        u32::from_le_bytes(payload[8..12].try_into().expect("row-count header length")) as usize;

    let mut remaining = deleted_row_ids.len();
    let mut dirty_ranges = Vec::with_capacity(deleted_row_ids.len());
    let mut offset = HEADER_LEN;
    let mut scanned_rows = 0usize;
    while remaining > 0 && offset + 12 <= payload.len() {
        if scanned_rows >= row_count {
            break;
        }
        let row_id = i64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let len_field_offset = offset + 8;
        let raw_len = u32::from_le_bytes(
            payload[len_field_offset..len_field_offset + 4]
                .try_into()
                .expect("row data len"),
        );
        let (is_tombstone, body_len) = split_table_payload_row_len(raw_len);
        let Some(row_end) = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
        else {
            return Ok(None);
        };
        if row_end > payload.len() {
            return Ok(None);
        }
        if !is_tombstone && deleted_row_ids.contains(&row_id) {
            let flagged = raw_len | TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG;
            payload[len_field_offset..len_field_offset + 4].copy_from_slice(&flagged.to_le_bytes());
            dirty_ranges.push(len_field_offset..len_field_offset + 4);
            remaining -= 1;
        }
        offset = row_end;
        scanned_rows += 1;
    }

    if remaining > 0 {
        // A targeted row id was not found as a live slot. Fall back so the
        // caller re-encodes from the authoritative resident rows.
        return Ok(None);
    }
    Ok(Some(dirty_ranges))
}

pub(crate) fn tombstone_deleted_rows_payload_by_locator(
    payload: &mut [u8],
    deleted_row_ids: &BTreeSet<i64>,
    locators: &Int64Map<u32>,
) -> Result<Option<Vec<Range<usize>>>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }

    let mut dirty_ranges = Vec::with_capacity(deleted_row_ids.len());
    for row_id in deleted_row_ids {
        let Some(&len_field_offset) = locators.get(row_id) else {
            return Ok(None);
        };
        let len_field_offset = len_field_offset as usize;
        if len_field_offset < 8 || len_field_offset + 4 > payload.len() {
            return Ok(None);
        }
        let row_id_offset = len_field_offset - 8;
        let actual_row_id = i64::from_le_bytes(
            payload[row_id_offset..row_id_offset + 8]
                .try_into()
                .expect("row id length"),
        );
        if actual_row_id != *row_id {
            return Ok(None);
        }
        let raw_len = u32::from_le_bytes(
            payload[len_field_offset..len_field_offset + 4]
                .try_into()
                .expect("row data len"),
        );
        let (is_tombstone, body_len) = split_table_payload_row_len(raw_len);
        let Some(row_end) = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
        else {
            return Ok(None);
        };
        if row_end > payload.len() || is_tombstone {
            return Ok(None);
        }

        let flagged = raw_len | TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG;
        payload[len_field_offset..len_field_offset + 4].copy_from_slice(&flagged.to_le_bytes());
        dirty_ranges.push(len_field_offset..len_field_offset + 4);
    }
    Ok(Some(dirty_ranges))
}

pub(crate) fn tombstone_deleted_rows_cached_payload_by_locator(
    payload: &mut [u8],
    deleted_row_ids: &BTreeSet<i64>,
    locators: &Int64Map<u32>,
    previous_checksum: u32,
) -> Result<Option<(Vec<Range<usize>>, u32)>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(Some((Vec::new(), previous_checksum)));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }

    let mut patches = Vec::with_capacity(deleted_row_ids.len());
    let mut span_start = usize::MAX;
    let mut span_end = 0usize;
    for row_id in deleted_row_ids {
        let Some(&len_field_offset) = locators.get(row_id) else {
            return Ok(None);
        };
        let len_field_offset = len_field_offset as usize;
        if len_field_offset < 8 || len_field_offset + 4 > payload.len() {
            return Ok(None);
        }
        let row_id_offset = len_field_offset - 8;
        let actual_row_id = i64::from_le_bytes(
            payload[row_id_offset..row_id_offset + 8]
                .try_into()
                .expect("row id length"),
        );
        if actual_row_id != *row_id {
            return Ok(None);
        }
        let raw_len = u32::from_le_bytes(
            payload[len_field_offset..len_field_offset + 4]
                .try_into()
                .expect("row data len"),
        );
        let (is_tombstone, body_len) = split_table_payload_row_len(raw_len);
        let Some(row_end) = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
        else {
            return Ok(None);
        };
        if row_end > payload.len() || is_tombstone {
            return Ok(None);
        }
        let new_bytes = (raw_len | TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG).to_le_bytes();
        span_start = span_start.min(len_field_offset);
        span_end = span_end.max(len_field_offset + 4);
        patches.push((len_field_offset, new_bytes));
    }

    if patches.is_empty() {
        return Ok(Some((Vec::new(), previous_checksum)));
    }
    let old_span = payload[span_start..span_end].to_vec();
    let mut dirty_ranges = Vec::with_capacity(patches.len());
    for (offset, new_bytes) in patches {
        if payload[offset..offset + 4] != new_bytes {
            payload[offset..offset + 4].copy_from_slice(&new_bytes);
            dirty_ranges.push(offset..offset + 4);
        }
    }
    let checksum = crc32c_patch_bytes(
        previous_checksum,
        payload.len(),
        span_start,
        &old_span,
        &payload[span_start..span_end],
    )
    .ok_or_else(|| DbError::internal("resident tombstone checksum patch failed"))?;
    Ok(Some((dirty_ranges, checksum)))
}

pub(crate) fn tombstone_deleted_rows_overflow_by_locator<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    chain_cache: &OverflowChainCache,
    deleted_row_ids: &BTreeSet<i64>,
    locators: &Int64Map<u32>,
    live_row_count: usize,
) -> Result<Option<(PersistedTableState, OverflowChainCache)>> {
    if deleted_row_ids.is_empty() {
        return Ok(Some((previous_state, chain_cache.clone())));
    }
    let pointer = previous_state.pointer;
    if pointer.head_page_id == 0
        || pointer.logical_len == 0
        || pointer.is_compressed()
        || pointer.is_table_paged_manifest()
    {
        return Ok(None);
    }
    if locators.is_empty() {
        return Ok(None);
    }
    let dead_after = locators.len().saturating_sub(live_row_count);
    if live_row_count == 0 || dead_after > live_row_count {
        return Ok(None);
    }

    let logical_len = pointer.logical_len as usize;
    let mut patch_bytes = Vec::with_capacity(deleted_row_ids.len());
    for row_id in deleted_row_ids {
        let Some(&len_field_offset) = locators.get(row_id) else {
            return Ok(None);
        };
        let len_field_offset = len_field_offset as usize;
        if len_field_offset < 8 || len_field_offset + 4 > logical_len {
            return Ok(None);
        }

        let mut row_id_bytes = [0_u8; 8];
        if !read_overflow_cached_logical_bytes(
            store,
            pointer,
            &chain_cache.page_ids,
            len_field_offset - 8,
            &mut row_id_bytes,
        )? {
            return Ok(None);
        }
        let actual_row_id = i64::from_le_bytes(row_id_bytes);
        if actual_row_id != *row_id {
            return Ok(None);
        }

        let mut len_bytes = [0_u8; 4];
        if !read_overflow_cached_logical_bytes(
            store,
            pointer,
            &chain_cache.page_ids,
            len_field_offset,
            &mut len_bytes,
        )? {
            return Ok(None);
        }
        let raw_len = u32::from_le_bytes(len_bytes);
        let (is_tombstone, body_len) = split_table_payload_row_len(raw_len);
        let Some(row_end) = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
        else {
            return Ok(None);
        };
        if row_end > logical_len || is_tombstone {
            return Ok(None);
        }

        patch_bytes.push((
            len_field_offset,
            (raw_len | TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG).to_le_bytes(),
        ));
    }

    let patches = patch_bytes
        .iter()
        .map(|(offset, bytes)| OverflowBytePatch {
            offset: *offset,
            bytes: bytes.as_slice(),
        })
        .collect::<Vec<_>>();
    let (pointer, new_chain_cache, tail, checksum) =
        rewrite_overflow_cached_with_sparse_byte_patches(
            store,
            pointer,
            previous_state.checksum,
            &chain_cache.page_ids,
            &patches,
        )?;
    Ok(Some((
        PersistedTableState {
            pointer,
            checksum,
            row_count: live_row_count,
            tail,
            pk_index_root: previous_state.pk_index_root,
        },
        new_chain_cache,
    )))
}

pub(crate) fn splice_deleted_rows_payload_in_place(
    payload: &mut Vec<u8>,
    data: &TableData,
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<Option<Vec<Range<usize>>>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(Some(single_dirty_range(payload.len()..payload.len())));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }
    let old_row_count =
        u32::from_le_bytes(payload[8..12].try_into().expect("row-count header length")) as usize;
    if old_row_count != data.rows.len().saturating_add(deleted_row_ids.len()) {
        return Ok(None);
    }

    let mut deleted_spans: Vec<(usize, usize)> = Vec::with_capacity(deleted_row_ids.len());
    let mut remaining = deleted_row_ids.len();
    let mut scan_offset = HEADER_LEN;
    let mut scanned_rows = 0usize;
    while remaining > 0 && scan_offset + 12 <= payload.len() {
        if scanned_rows >= old_row_count {
            break;
        }
        let row_id = i64::from_le_bytes(
            payload[scan_offset..scan_offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let row_data_len = u32::from_le_bytes(
            payload[scan_offset + 8..scan_offset + 12]
                .try_into()
                .expect("row data len"),
        ) as usize;
        let row_end = scan_offset.saturating_add(12).saturating_add(row_data_len);
        if row_end > payload.len() {
            return Ok(None);
        }
        if deleted_row_ids.contains(&row_id) {
            deleted_spans.push((scan_offset, row_end));
            remaining -= 1;
        }
        scan_offset = row_end;
        scanned_rows += 1;
    }

    if remaining > 0 || deleted_spans.len() != deleted_row_ids.len() {
        return Ok(None);
    }

    let original_len = payload.len();
    let first_deleted_byte = deleted_spans.first().map_or(HEADER_LEN, |span| span.0);
    payload[8..12].copy_from_slice(
        &u32::try_from(data.rows.len())
            .map_err(|_| DbError::constraint("table row count exceeds u32"))?
            .to_le_bytes(),
    );

    let mut copy_from = HEADER_LEN;
    let mut write_at = HEADER_LEN;
    for (span_start, span_end) in deleted_spans {
        if copy_from < span_start {
            if copy_from != write_at {
                payload.copy_within(copy_from..span_start, write_at);
            }
            write_at += span_start - copy_from;
        }
        copy_from = span_end;
    }
    if copy_from < original_len {
        let tail_len = original_len - copy_from;
        if copy_from != write_at {
            payload.copy_within(copy_from..original_len, write_at);
        }
        write_at += tail_len;
    }
    payload.truncate(write_at);

    let mut ranges = single_dirty_range(8..HEADER_LEN);
    if !payload.is_empty() {
        let tail_dirty_start = first_deleted_byte.saturating_sub(1).min(payload.len());
        if tail_dirty_start < payload.len() {
            ranges.push(tail_dirty_start..payload.len());
        }
    }
    Ok(Some(ranges))
}

pub(crate) fn splice_updated_rows_payload(
    old: &[u8],
    data: &TableData,
    dirty_indices: &[usize],
) -> Result<SpliceResult> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if old.len() < HEADER_LEN || old[..8] != *TABLE_PAYLOAD_MAGIC {
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }
    let old_row_count =
        u32::from_le_bytes(old[8..12].try_into().expect("row-count header length")) as usize;
    if old_row_count != data.rows.len() {
        // Row count changed (e.g. concurrent insert/delete after the cache
        // was stored) — fall back to full encode for safety.
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }

    // Fast path: only a handful of rows changed.  Scan the old payload to
    // locate byte ranges of each dirty row, then splice new encodings in.
    //
    // Row wire format:
    //   row_id       (8 bytes, i64 LE)
    //   row_data_len (4 bytes, u32 LE)
    //   row_data     (row_data_len bytes)
    //
    // We need the byte offset of each dirty row (and the one after it) so
    // we can copy unchanged prefix / suffix regions.

    // Sort dirty indices so we splice left-to-right.
    let mut sorted_dirty: Vec<usize> = dirty_indices.to_vec();
    sorted_dirty.sort_unstable();
    sorted_dirty.dedup();

    // Scan the old payload to locate dirty row byte ranges.
    // row_spans[i] = (start, end) byte offsets in `old` for dirty row i.
    let mut row_spans: Vec<(usize, usize)> = Vec::with_capacity(sorted_dirty.len());
    let mut scan_offset = HEADER_LEN;
    let mut dirty_cursor = 0;
    let mut row_idx = 0;
    while dirty_cursor < sorted_dirty.len() && scan_offset + 12 <= old.len() {
        if row_idx >= old_row_count {
            break;
        }
        let rd_len = u32::from_le_bytes(
            old[scan_offset + 8..scan_offset + 12]
                .try_into()
                .expect("row data len"),
        ) as usize;
        let row_end = scan_offset + 12 + rd_len;
        if row_end > old.len() {
            let payload = encode_table_payload(data)?;
            let payload_len = payload.len();
            return Ok(SpliceResult {
                payload,
                first_dirty_byte: 0,
                last_dirty_byte: payload_len,
                pk_locator_preserved: false,
            });
        }
        if row_idx == sorted_dirty[dirty_cursor] {
            row_spans.push((scan_offset, row_end));
            dirty_cursor += 1;
            if dirty_cursor == sorted_dirty.len() {
                break;
            }
        }
        scan_offset = row_end;
        row_idx += 1;
    }

    if row_spans.len() != sorted_dirty.len() {
        // Could not find all dirty rows in the old payload; fall back.
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }

    if row_spans.is_empty() {
        return Ok(SpliceResult {
            payload: old.to_vec(),
            first_dirty_byte: 0,
            last_dirty_byte: old.len(),
            pk_locator_preserved: false,
        });
    }

    let mut can_preserve_body = true;
    let mut encoded_rows: Vec<Vec<u8>> = Vec::with_capacity(sorted_dirty.len());
    let mut encoded_row = Vec::with_capacity(128);
    for (span_idx, &dirty_row) in sorted_dirty.iter().enumerate() {
        if dirty_row >= data.rows.len() {
            let payload = encode_table_payload(data)?;
            let payload_len = payload.len();
            return Ok(SpliceResult {
                payload,
                first_dirty_byte: 0,
                last_dirty_byte: payload_len,
                pk_locator_preserved: false,
            });
        }
        let (span_start, span_end) = row_spans[span_idx];
        let old_row_body_len = span_end.saturating_sub(span_start).saturating_sub(12);

        let row = &data.rows[dirty_row];
        encoded_row.clear();
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        if encoded_row.len() > old_row_body_len {
            can_preserve_body = false;
        }
        encoded_rows.push(encoded_row.clone());
    }

    let mut output = Vec::with_capacity(if can_preserve_body {
        old.len()
    } else {
        old.len().saturating_add(sorted_dirty.len() * 32)
    });
    output.extend_from_slice(&old[..8]); // magic
    encode_u32(&mut output, data.rows.len() as u32);

    let first_dirty_byte = if can_preserve_body {
        row_spans.first().map_or(0, |s| s.0.saturating_add(12))
    } else {
        row_spans.first().map_or(0, |s| s.0)
    };
    let last_dirty_byte = if can_preserve_body {
        row_spans.last().map_or(first_dirty_byte, |s| s.1)
    } else {
        old.len()
    };

    let mut copy_from = HEADER_LEN;
    for (span_idx, &dirty_row) in sorted_dirty.iter().enumerate() {
        let (span_start, span_end) = row_spans[span_idx];
        let old_row_body_len = span_end.saturating_sub(span_start).saturating_sub(12);
        let encoded_row = &encoded_rows[span_idx];

        // Copy unchanged bytes before this dirty row.
        if copy_from < span_start {
            output.extend_from_slice(&old[copy_from..span_start]);
        }

        // Encode the updated row.
        let row = &data.rows[dirty_row];
        encode_i64(&mut output, row.row_id);
        if can_preserve_body {
            let row_body_len = u32::try_from(old_row_body_len)
                .map_err(|_| DbError::constraint("table row body length exceeds u32"))?;
            output.extend_from_slice(&row_body_len.to_le_bytes());
            output.extend_from_slice(encoded_row);
            output.extend(std::iter::repeat_n(
                0u8,
                old_row_body_len.saturating_sub(encoded_row.len()),
            ));
        } else {
            encode_u32(
                &mut output,
                u32::try_from(encoded_row.len())
                    .map_err(|_| DbError::constraint("table row body length exceeds u32"))?,
            );
            output.extend_from_slice(encoded_row);
        }

        copy_from = span_end;
    }
    // Copy any remaining unchanged tail.
    if copy_from < old.len() {
        output.extend_from_slice(&old[copy_from..]);
    }

    Ok(SpliceResult {
        payload: output,
        first_dirty_byte,
        last_dirty_byte,
        pk_locator_preserved: can_preserve_body,
    })
}

pub(crate) fn splice_deleted_rows_payload(
    old: &[u8],
    data: &TableData,
    deleted_row_ids: &BTreeSet<i64>,
) -> Result<SpliceResult> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(SpliceResult {
            payload: old.to_vec(),
            first_dirty_byte: old.len(),
            last_dirty_byte: old.len(),
            pk_locator_preserved: false,
        });
    }
    if old.len() < HEADER_LEN || old[..8] != *TABLE_PAYLOAD_MAGIC {
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }
    let old_row_count =
        u32::from_le_bytes(old[8..12].try_into().expect("row-count header length")) as usize;
    if old_row_count != data.rows.len().saturating_add(deleted_row_ids.len()) {
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }

    let mut deleted_spans: Vec<(usize, usize)> = Vec::with_capacity(deleted_row_ids.len());
    let mut remaining = deleted_row_ids.len();
    let mut scan_offset = HEADER_LEN;
    while remaining > 0 && scan_offset + 12 <= old.len() {
        let row_id = i64::from_le_bytes(
            old[scan_offset..scan_offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let row_data_len = u32::from_le_bytes(
            old[scan_offset + 8..scan_offset + 12]
                .try_into()
                .expect("row data len"),
        ) as usize;
        let row_end = scan_offset.saturating_add(12).saturating_add(row_data_len);
        if row_end > old.len() {
            let payload = encode_table_payload(data)?;
            let payload_len = payload.len();
            return Ok(SpliceResult {
                payload,
                first_dirty_byte: 0,
                last_dirty_byte: payload_len,
                pk_locator_preserved: false,
            });
        }
        if deleted_row_ids.contains(&row_id) {
            deleted_spans.push((scan_offset, row_end));
            remaining -= 1;
        }
        scan_offset = row_end;
    }

    if remaining > 0 || deleted_spans.len() != deleted_row_ids.len() {
        let payload = encode_table_payload(data)?;
        let payload_len = payload.len();
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
            last_dirty_byte: payload_len,
            pk_locator_preserved: false,
        });
    }

    let first_dirty_byte = deleted_spans.first().map_or(0, |span| span.0);
    let mut output = Vec::with_capacity(old.len());
    output.extend_from_slice(&old[..TABLE_PAYLOAD_MAGIC.len()]);
    encode_u32(&mut output, data.rows.len() as u32);

    let mut copy_from = HEADER_LEN;
    for (span_start, span_end) in deleted_spans {
        if copy_from < span_start {
            output.extend_from_slice(&old[copy_from..span_start]);
        }
        copy_from = span_end;
    }
    if copy_from < old.len() {
        output.extend_from_slice(&old[copy_from..]);
    }

    let last_dirty_byte = output.len();
    Ok(SpliceResult {
        payload: output,
        first_dirty_byte,
        last_dirty_byte,
        pk_locator_preserved: false,
    })
}

pub(crate) fn manifest_chunk_index_for_row_position(
    chunks: &[PersistedTableChunkState],
    row_position: usize,
) -> Option<usize> {
    let mut start = 0usize;
    for (index, chunk) in chunks.iter().enumerate() {
        let end = start.saturating_add(chunk.row_count);
        if row_position < end {
            return Some(index);
        }
        start = end;
    }
    None
}

impl EngineRuntime {
    pub(crate) fn deferred_paged_row_locator_caches_mut(
        &mut self,
    ) -> &mut BTreeMap<String, Arc<DeferredPagedRowLocatorCache>> {
        Arc::make_mut(&mut self.deferred_paged_row_locator_caches)
    }
    #[cfg(test)]
    pub(crate) fn deferred_paged_row_locator_cache_is_dense_for_tests(
        &self,
        table_name: &str,
    ) -> Option<bool> {
        let canonical = map_key_ci(self.deferred_paged_row_locator_caches.as_ref(), table_name)?;
        self.deferred_paged_row_locator_caches
            .get(&canonical)
            .map(|cache| cache.locators.is_dense())
    }
    #[cfg(test)]
    pub(crate) fn deferred_paged_row_locator_cache_sparse_len_for_tests(
        &self,
        table_name: &str,
    ) -> Option<usize> {
        let canonical = map_key_ci(self.deferred_paged_row_locator_caches.as_ref(), table_name)?;
        self.deferred_paged_row_locator_caches
            .get(&canonical)
            .map(|cache| cache.locators.sparse_len())
    }
    pub(crate) fn manifest_payload(&mut self) -> Result<&[u8]> {
        let use_template =
            self.manifest_template.as_ref().is_some_and(|template| {
                template.schema_cookie == self.catalog.schema_cookie
                    && template.table_next_row_id_offsets.len() == self.catalog.tables.len()
                    && template.table_state_offsets.len() == self.catalog.tables.len()
                    && template.table_pk_index_root_offsets.len() == self.catalog.tables.len()
                    && self.catalog.tables.keys().all(|table_name| {
                        template.table_next_row_id_offsets.contains_key(table_name)
                    })
                    && self
                        .catalog
                        .tables
                        .keys()
                        .all(|table_name| template.table_state_offsets.contains_key(table_name))
                    && self.catalog.tables.keys().all(|table_name| {
                        template
                            .table_pk_index_root_offsets
                            .contains_key(table_name)
                    })
            });

        if !use_template {
            let encoded = encode_manifest_payload_with_offsets(self, &self.persisted_tables)?;
            self.manifest_template = Some(ManifestTemplate {
                schema_cookie: self.catalog.schema_cookie,
                table_next_row_id_offsets: encoded.table_next_row_id_offsets,
                table_state_offsets: encoded.table_state_offsets,
                table_pk_index_root_offsets: encoded.table_pk_index_root_offsets,
                bytes: encoded.bytes,
            });
        }

        let template = self
            .manifest_template
            .as_mut()
            .ok_or_else(|| DbError::internal("manifest template was not initialized"))?;
        for (table_name, offset) in &template.table_next_row_id_offsets {
            let next_row_id = self
                .catalog
                .tables
                .get(table_name)
                .map(|table| table.next_row_id)
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "manifest next_row_id offset referenced unknown table {table_name}"
                    ))
                })?;
            patch_manifest_table_next_row_id(&mut template.bytes, *offset, next_row_id)?;
        }
        for (table_name, offset) in &template.table_state_offsets {
            let state = self
                .persisted_tables
                .get(table_name)
                .copied()
                .unwrap_or_default();
            patch_manifest_table_state(&mut template.bytes, *offset, state)?;
        }
        for (table_name, offset) in &template.table_pk_index_root_offsets {
            let pk_index_root = self
                .catalog
                .tables
                .get(table_name)
                .map(|table| table.pk_index_root)
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "manifest pk_index_root offset referenced unknown table {table_name}"
                    ))
                })?;
            patch_manifest_table_pk_index_root(&mut template.bytes, *offset, pk_index_root)?;
        }
        Ok(template.bytes.as_slice())
    }
}
