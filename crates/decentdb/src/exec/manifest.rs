//! Thematic extraction (mechanical split; no behavior change).

use super::*;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DensePagedRowDirectory {
    pub(crate) start_row_id: i64,
    pub(crate) locators: Vec<RowLocatorV1>,
    /// Cumulative exclusive row positions for each physical chunk.
    pub(crate) chunk_ends: Vec<usize>,
}

impl DensePagedRowDirectory {
    pub(crate) fn empty(chunk_count: usize) -> Result<Self> {
        let mut chunk_ends = Vec::new();
        try_reserve_paged_directory(&mut chunk_ends, chunk_count, "dense chunk ranges")?;
        chunk_ends.resize(chunk_count, 0);
        Ok(Self {
            start_row_id: 0,
            locators: Vec::new(),
            chunk_ends,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.locators.len()
    }

    fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }

    pub(crate) fn first_row_id(&self) -> Option<i64> {
        (!self.is_empty()).then_some(self.start_row_id)
    }

    pub(crate) fn row_id_at(&self, position: usize) -> Option<i64> {
        if position >= self.len() {
            return None;
        }
        let position = i128::try_from(position).ok()?;
        i64::try_from(i128::from(self.start_row_id) + position).ok()
    }

    pub(crate) fn position_for_row_id(&self, row_id: i64) -> Option<usize> {
        let offset = i128::from(row_id) - i128::from(self.start_row_id);
        if offset < 0 {
            return None;
        }
        usize::try_from(offset)
            .ok()
            .filter(|position| *position < self.len())
    }

    pub(crate) fn chunk_index_at(&self, position: usize) -> Option<usize> {
        if position >= self.len() {
            return None;
        }
        let chunk_index = self.chunk_ends.partition_point(|end| *end <= position);
        (chunk_index < self.chunk_ends.len()).then_some(chunk_index)
    }

    pub(crate) fn entry_at(&self, position: usize) -> Result<Option<TablePageEntry>> {
        let Some(row_id) = self.row_id_at(position) else {
            return Ok(None);
        };
        let chunk_index = self
            .chunk_index_at(position)
            .ok_or_else(|| DbError::corruption("dense paged row position has no owning chunk"))?;
        let locator = *self.locators.get(position).ok_or_else(|| {
            DbError::corruption("dense paged row locator position exceeded directory length")
        })?;
        Ok(Some(TablePageEntry {
            row_id,
            chunk_index: u32::try_from(chunk_index)
                .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?,
            is_overlay: false,
            locator,
        }))
    }

    pub(crate) fn try_prepare_append(
        &mut self,
        row_id: i64,
        chunk_index: usize,
        is_overlay: bool,
    ) -> Result<Option<bool>> {
        if is_overlay {
            return Ok(None);
        }
        let next_position = self.len();
        if !self.is_empty()
            && self
                .row_id_at(next_position.saturating_sub(1))
                .and_then(|last| last.checked_add(1))
                != Some(row_id)
        {
            return Ok(None);
        }

        let add_chunk = if chunk_index >= self.chunk_ends.len() {
            if chunk_index != self.chunk_ends.len() {
                return Ok(None);
            }
            u32::try_from(chunk_index)
                .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?;
            true
        } else if chunk_index + 1 != self.chunk_ends.len() {
            return Ok(None);
        } else {
            false
        };

        if self.locators.len() == self.locators.capacity() {
            try_reserve_paged_directory_amortized(&mut self.locators, 1, "dense row locators")?;
        }
        if add_chunk {
            try_reserve_paged_directory(&mut self.chunk_ends, 1, "dense chunk ranges")?;
        }
        Ok(Some(add_chunk))
    }

    #[cfg(test)]
    pub(crate) fn try_append(
        &mut self,
        row_id: i64,
        chunk_index: usize,
        is_overlay: bool,
        locator: RowLocatorV1,
    ) -> Result<bool> {
        let Some(add_chunk) = self.try_prepare_append(row_id, chunk_index, is_overlay)? else {
            return Ok(false);
        };
        self.append_prepared(row_id, chunk_index, add_chunk, locator)?;
        Ok(true)
    }

    fn append_prepared(
        &mut self,
        row_id: i64,
        chunk_index: usize,
        add_chunk: bool,
        locator: RowLocatorV1,
    ) -> Result<()> {
        let next_position = self.len();
        if self.is_empty() {
            self.start_row_id = row_id;
        }
        if add_chunk {
            self.chunk_ends.push(next_position);
        }
        self.locators.push(locator);
        let Some(chunk_end) = self.chunk_ends.get_mut(chunk_index) else {
            return Err(DbError::corruption(
                "dense paged append chunk range is missing",
            ));
        };
        *chunk_end = self.locators.len();
        Ok(())
    }

    pub(crate) fn to_sparse(&self) -> Result<Vec<TablePageEntry>> {
        let mut entries = Vec::new();
        try_reserve_paged_directory(&mut entries, self.len(), "sparse paged row entries")?;
        for position in 0..self.len() {
            entries.push(self.entry_at(position)?.ok_or_else(|| {
                DbError::corruption("dense paged row directory ended before its locator count")
            })?);
        }
        Ok(entries)
    }

    pub(crate) fn approximate_heap_bytes(&self) -> usize {
        self.locators
            .capacity()
            .saturating_mul(std::mem::size_of::<RowLocatorV1>())
            .saturating_add(
                self.chunk_ends
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TablePageManifest {
    pub(crate) chunks: Arc<Vec<TablePageManifestChunk>>,
    pub(crate) rows: Arc<TablePageDirectory>,
    pub(crate) tombstoned_row_ids: Arc<BTreeSet<i64>>,
}

impl TablePageManifest {
    pub(crate) fn from_payload(payload: Arc<Vec<u8>>) -> Result<Self> {
        let row_count = if payload.is_empty() {
            0
        } else {
            read_table_payload_row_count_from_bytes(payload.as_slice())?
        };
        let chunk = TablePageManifestChunk {
            pointer: OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            },
            checksum: 0,
            row_count,
            payload,
            tombstoned_row_ids: Arc::new(BTreeSet::new()),
            overlay_pointer: None,
            overlay_checksum: None,
            overlay_payload: None,
        };
        Self::from_chunks(vec![chunk])
    }

    pub(crate) fn from_chunks(chunks: Vec<TablePageManifestChunk>) -> Result<Self> {
        let tombstoned_row_ids = chunks
            .iter()
            .flat_map(|chunk| chunk.tombstoned_row_ids.iter().copied())
            .collect::<BTreeSet<_>>();

        if let Some(directory) = try_build_dense_paged_row_directory(&chunks)? {
            return Ok(Self {
                chunks: Arc::new(chunks),
                rows: Arc::new(TablePageDirectory::Dense(directory)),
                tombstoned_row_ids: Arc::new(tombstoned_row_ids),
            });
        }

        // Collect tombstoned row IDs into a set per chunk
        let chunk_tombstones: Vec<BTreeSet<i64>> = chunks
            .iter()
            .map(|c| c.tombstoned_row_ids.iter().copied().collect())
            .collect();

        // Collect overlay row IDs per chunk into a set
        let chunk_overlay_row_ids: Vec<BTreeSet<i64>> = chunks
            .iter()
            .map(|c| {
                let mut set = BTreeSet::new();
                if let Some(overlay_payload) = &c.overlay_payload {
                    if !overlay_payload.is_empty() {
                        let mut cursor = Cursor::new(overlay_payload.as_slice());
                        let magic = cursor
                            .read_slice(TABLE_PAYLOAD_MAGIC.len())
                            .unwrap_or_default();
                        if magic == *TABLE_PAYLOAD_MAGIC {
                            let row_count = cursor.read_u32().unwrap_or(0) as usize;
                            for _ in 0..row_count {
                                let row_id = cursor.read_i64().unwrap_or(0);
                                let row_bytes_len = cursor.read_u32().unwrap_or(0) as usize;
                                if cursor.read_slice(row_bytes_len).is_err() {
                                    break;
                                }
                                set.insert(row_id);
                            }
                        }
                    }
                }
                set
            })
            .collect();

        let expected_rows = chunks.iter().try_fold(0usize, |total, chunk| {
            total
                .checked_add(chunk.row_count)
                .ok_or_else(|| DbError::constraint("paged table row count overflow"))
        })?;
        let mut rows = Vec::new();
        try_reserve_paged_directory(&mut rows, expected_rows, "sparse paged row entries")?;
        let mut base_row_entries = Vec::new();
        let mut overlay_row_entries = Vec::new();

        for (chunk_index, chunk) in chunks.iter().enumerate() {
            base_row_entries.clear();
            overlay_row_entries.clear();

            let tombstones = &chunk_tombstones[chunk_index];
            let overlay_ids = &chunk_overlay_row_ids[chunk_index];

            if !chunk.payload.is_empty() {
                let mut cursor = Cursor::new(chunk.payload.as_slice());
                let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
                if magic != TABLE_PAYLOAD_MAGIC {
                    return Err(DbError::corruption("table payload magic is invalid"));
                }
                let row_count = cursor.read_u32()? as usize;
                for _ in 0..row_count {
                    let row_id = cursor.read_i64()?;
                    let (is_tombstone, row_bytes_len) =
                        split_table_payload_row_len(cursor.read_u32()?);
                    let row_bytes_offset = cursor.offset;
                    if is_tombstone {
                        cursor.read_slice(row_bytes_len)?;
                        continue;
                    }
                    #[cfg(debug_assertions)]
                    {
                        let row_bytes = cursor.read_slice(row_bytes_len)?;
                        Row::decode(row_bytes)?;
                    }
                    #[cfg(not(debug_assertions))]
                    {
                        cursor.read_slice(row_bytes_len)?;
                    }
                    if tombstones.contains(&row_id) || overlay_ids.contains(&row_id) {
                        continue; // skip tombstoned and overlaid base rows
                    }
                    base_row_entries.push(TablePageEntry {
                        row_id,
                        chunk_index: u32::try_from(chunk_index)
                            .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?,
                        is_overlay: false,
                        locator: RowLocatorV1 {
                            byte_offset: u32::try_from(row_bytes_offset).map_err(|_| {
                                DbError::constraint("row locator offset exceeds u32")
                            })?,
                            byte_len: u32::try_from(row_bytes_len).map_err(|_| {
                                DbError::constraint("row locator length exceeds u32")
                            })?,
                        },
                    });
                }
            }

            if let Some(overlay_payload) = &chunk.overlay_payload {
                if !overlay_payload.is_empty() {
                    let mut cursor = Cursor::new(overlay_payload.as_slice());
                    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
                    if magic != TABLE_PAYLOAD_MAGIC {
                        return Err(DbError::corruption("table payload magic is invalid"));
                    }
                    let row_count = cursor.read_u32()? as usize;
                    for _ in 0..row_count {
                        let row_id = cursor.read_i64()?;
                        let (is_tombstone, row_bytes_len) =
                            split_table_payload_row_len(cursor.read_u32()?);
                        let row_bytes_offset = cursor.offset;
                        if is_tombstone {
                            cursor.read_slice(row_bytes_len)?;
                            continue;
                        }
                        #[cfg(debug_assertions)]
                        {
                            let row_bytes = cursor.read_slice(row_bytes_len)?;
                            Row::decode(row_bytes)?;
                        }
                        #[cfg(not(debug_assertions))]
                        {
                            cursor.read_slice(row_bytes_len)?;
                        }
                        overlay_row_entries.push(TablePageEntry {
                            row_id,
                            chunk_index: u32::try_from(chunk_index).map_err(|_| {
                                DbError::constraint("table chunk index exceeds u32")
                            })?,
                            is_overlay: true,
                            locator: RowLocatorV1 {
                                byte_offset: u32::try_from(row_bytes_offset).map_err(|_| {
                                    DbError::constraint("row locator offset exceeds u32")
                                })?,
                                byte_len: u32::try_from(row_bytes_len).map_err(|_| {
                                    DbError::constraint("row locator length exceeds u32")
                                })?,
                            },
                        });
                    }
                }
            }

            // Add base entries first, then overlay entries. Since overlay row_ids
            // are not in base_row_entries, the merged list has unique row_ids per chunk.
            rows.extend_from_slice(&base_row_entries);
            rows.extend_from_slice(&overlay_row_entries);
        }

        // Now rows are globally sorted by (chunk_index, row_id), but
        // row_by_id needs them sorted by row_id. Sort and verify no duplicates.
        rows.sort_by_key(|entry| entry.row_id);

        // Sanity check: after deduplication, there should be no duplicate row_ids
        // because overlay_ids were used to skip overlaid base rows.
        #[cfg(debug_assertions)]
        {
            for window in rows.windows(2) {
                assert_ne!(
                    window[0].row_id, window[1].row_id,
                    "duplicate row_id in TablePageManifest rows"
                );
            }
        }

        Ok(Self {
            chunks: Arc::new(chunks),
            rows: Arc::new(TablePageDirectory::Sparse(rows)),
            tombstoned_row_ids: Arc::new(tombstoned_row_ids),
        })
    }

    pub(crate) fn from_rows(rows: &[StoredRow], page_size: u32) -> Result<Self> {
        let chunks = encode_paged_table_chunks_from_rows(rows, page_size)?
            .into_iter()
            .map(|chunk| TablePageManifestChunk {
                pointer: OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                checksum: chunk.checksum,
                row_count: chunk.row_count,
                payload: Arc::new(chunk.payload),
                tombstoned_row_ids: Arc::new(BTreeSet::new()),
                overlay_pointer: None,
                overlay_checksum: None,
                overlay_payload: None,
            })
            .collect();
        Self::from_chunks(chunks)
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn append_row(&mut self, row: &StoredRow, page_size: u32) -> Result<()> {
        let mut encoded_values = Vec::with_capacity(64);
        self.append_row_with_scratch(row, page_size, &mut encoded_values)
    }

    pub(crate) fn append_row_with_scratch(
        &mut self,
        row: &StoredRow,
        page_size: u32,
        encoded_values: &mut Vec<u8>,
    ) -> Result<()> {
        let prepared = self.try_prepare_append_row_with_scratch(row, page_size, encoded_values)?;
        self.append_prepared_row_with_scratch(row, page_size, encoded_values, prepared)
    }

    pub(crate) fn try_prepare_append_row_with_scratch(
        &mut self,
        row: &StoredRow,
        page_size: u32,
        encoded_values: &mut Vec<u8>,
    ) -> Result<PreparedTablePageAppend> {
        Row::encode_values_into(&row.values, encoded_values)?;
        let encoded_row_len = 8usize
            .saturating_add(4)
            .saturating_add(encoded_values.len());
        let (planned_chunk_index, planned_is_overlay) =
            self.planned_append_target(row.row_id, encoded_row_len, page_size)?;
        let entry_chunk_index = u32::try_from(planned_chunk_index)
            .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?;
        let directory = Arc::make_mut(&mut self.rows).try_prepare_append(
            row.row_id,
            planned_chunk_index,
            planned_is_overlay,
        )?;
        Ok(PreparedTablePageAppend {
            chunk_index: planned_chunk_index,
            entry_chunk_index,
            is_overlay: planned_is_overlay,
            directory,
        })
    }

    fn planned_append_target(
        &self,
        row_id: i64,
        encoded_row_len: usize,
        page_size: u32,
    ) -> Result<(usize, bool)> {
        #[cfg(test)]
        PAGED_ROW_APPEND_PLAN_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        if self.tombstoned_row_ids.contains(&row_id) {
            let chunk_index = self
                .chunks
                .iter()
                .position(|chunk| chunk.tombstoned_row_ids.contains(&row_id))
                .ok_or_else(|| {
                    DbError::corruption(
                        "paged table tombstone index referenced a missing chunk tombstone",
                    )
                })?;
            return Ok((chunk_index, true));
        }
        let target_chunk_bytes = paged_table_target_chunk_bytes(page_size);
        let chunk_index = if self.chunks.last().is_some_and(|chunk| {
            chunk.payload.len().saturating_add(encoded_row_len) <= target_chunk_bytes
        }) {
            self.chunks.len() - 1
        } else {
            self.chunks.len()
        };
        Ok((chunk_index, false))
    }

    pub(crate) fn append_prepared_row_with_scratch(
        &mut self,
        row: &StoredRow,
        page_size: u32,
        encoded_values: &[u8],
        prepared: PreparedTablePageAppend,
    ) -> Result<()> {
        let encoded_row_len = 8usize
            .saturating_add(4)
            .saturating_add(encoded_values.len());
        let target_chunk_bytes = paged_table_target_chunk_bytes(page_size);

        let chunks = Arc::make_mut(&mut self.chunks);
        let locator = if prepared.is_overlay {
            let chunk_index = prepared.chunk_index;
            let chunk = chunks
                .get_mut(chunk_index)
                .ok_or_else(|| DbError::internal("paged append chunk index was out of bounds"))?;
            let overlay_payload = chunk.overlay_payload.get_or_insert_with(|| {
                let mut payload = Vec::with_capacity(
                    target_chunk_bytes.max(TABLE_PAYLOAD_MAGIC.len() + 4 + encoded_row_len),
                );
                payload.extend_from_slice(TABLE_PAYLOAD_MAGIC);
                payload.extend_from_slice(&0_u32.to_le_bytes());
                Arc::new(payload)
            });
            let locator = append_encoded_table_payload_row(
                Arc::make_mut(overlay_payload),
                row.row_id,
                encoded_values,
            )?;
            chunk.overlay_checksum = None;
            chunk.row_count = chunk
                .row_count
                .checked_add(1)
                .ok_or_else(|| DbError::constraint("paged table chunk row count overflow"))?;
            locator
        } else {
            let chunk_index = prepared.chunk_index;
            if chunk_index == chunks.len() {
                let mut payload =
                    Vec::with_capacity(target_chunk_bytes.max(TABLE_PAYLOAD_MAGIC.len() + 4));
                payload.extend_from_slice(TABLE_PAYLOAD_MAGIC);
                payload.extend_from_slice(&0_u32.to_le_bytes());
                chunks.push(TablePageManifestChunk {
                    pointer: OverflowPointer {
                        head_page_id: 0,
                        logical_len: 0,
                        flags: 0,
                    },
                    checksum: 0,
                    row_count: 0,
                    payload: Arc::new(payload),
                    tombstoned_row_ids: Arc::new(BTreeSet::new()),
                    overlay_pointer: None,
                    overlay_checksum: None,
                    overlay_payload: None,
                });
            }

            let chunk = chunks
                .get_mut(chunk_index)
                .ok_or_else(|| DbError::internal("paged append chunk index was out of bounds"))?;
            let locator = append_encoded_table_payload_row(
                Arc::make_mut(&mut chunk.payload),
                row.row_id,
                encoded_values,
            )?;
            chunk.pointer = OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            };
            chunk.checksum = 0;
            chunk.row_count = chunk
                .row_count
                .checked_add(1)
                .ok_or_else(|| DbError::constraint("paged table chunk row count overflow"))?;
            locator
        };

        let entry = TablePageEntry {
            row_id: row.row_id,
            chunk_index: prepared.entry_chunk_index,
            is_overlay: prepared.is_overlay,
            locator,
        };
        let directory = Arc::make_mut(&mut self.rows);
        let rows = match (directory, prepared.directory) {
            (
                TablePageDirectory::Dense(rows),
                PreparedTablePageDirectoryAppend::Dense { add_chunk },
            ) => {
                debug_assert!(!prepared.is_overlay);
                rows.append_prepared(row.row_id, prepared.chunk_index, add_chunk, locator)?;
                return Ok(());
            }
            (TablePageDirectory::Sparse(rows), PreparedTablePageDirectoryAppend::Sparse) => rows,
            _ => {
                return Err(DbError::corruption(
                    "paged row directory changed after append preparation",
                ));
            }
        };
        if rows
            .last()
            .is_none_or(|existing| existing.row_id < entry.row_id)
        {
            rows.push(entry);
            return Ok(());
        }
        match rows.binary_search_by_key(&entry.row_id, |existing| existing.row_id) {
            Ok(_) => Err(DbError::constraint(
                "duplicate row id in paged table append",
            )),
            Err(position) => {
                rows.insert(position, entry);
                Ok(())
            }
        }
    }

    pub(crate) fn row_by_id(&self, row_id: i64) -> Result<Option<TableRowRef<'_>>> {
        let Some((position, _)) = self.rows.entry_for_row_id(row_id)? else {
            return Ok(None);
        };
        self.row_at_position(position)
    }

    pub(crate) fn row_ids_in_range(&self, low: i64, high: i64) -> Vec<i64> {
        self.rows.row_ids_in_range(low, high)
    }

    /// Returns the chunk index owning `row_id`, if present. Used by the bulk
    /// delete manifest rebuild to avoid decoding base payloads.
    pub(crate) fn chunk_index_for_row_id(&self, row_id: i64) -> Option<usize> {
        self.rows
            .entry_for_row_id(row_id)
            .ok()
            .flatten()
            .map(|(_, entry)| entry.chunk_index as usize)
    }

    pub(crate) fn projected_values_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        let Some((position, _)) = self.rows.entry_for_row_id(row_id)? else {
            return Ok(None);
        };
        self.projected_values_at_position(position, projection_indexes)
    }

    fn row_bytes_for_entry<'a>(
        &'a self,
        entry: TablePageEntry,
        chunk: &'a TablePageManifestChunk,
    ) -> Result<Option<&'a [u8]>> {
        if entry.is_overlay {
            let payload = chunk
                .overlay_payload
                .as_ref()
                .ok_or_else(|| DbError::corruption("paged table overlay chunk is missing"))?;
            return Self::row_bytes_from_locator(payload.as_slice(), entry.locator).map(Some);
        }

        if !self.tombstoned_row_ids.is_empty() && chunk.tombstoned_row_ids.contains(&entry.row_id) {
            let Some(overlay_payload) = &chunk.overlay_payload else {
                return Ok(None);
            };
            return Self::row_bytes_from_tombstoned_base(overlay_payload.as_slice(), entry.row_id)
                .map(Some);
        }

        let start = entry.locator.byte_offset as usize;
        let end = start
            .checked_add(entry.locator.byte_len as usize)
            .ok_or_else(|| DbError::corruption("paged row locator exceeded address space"))?;
        let row_bytes = chunk
            .payload
            .as_slice()
            .get(start..end)
            .ok_or_else(|| DbError::corruption("paged row locator exceeded payload length"))?;
        Ok(Some(row_bytes))
    }

    pub(crate) fn row_bytes_from_locator(payload: &[u8], locator: RowLocatorV1) -> Result<&[u8]> {
        let start = locator.byte_offset as usize;
        let end = start
            .checked_add(locator.byte_len as usize)
            .ok_or_else(|| DbError::corruption("paged row locator exceeded address space"))?;
        payload
            .get(start..end)
            .ok_or_else(|| DbError::corruption("paged row locator exceeded payload length"))
    }

    fn row_bytes_from_tombstoned_base(overlay_payload: &[u8], row_id: i64) -> Result<&[u8]> {
        if overlay_payload.is_empty() {
            return Err(DbError::corruption(
                "paged table overlay row is missing from overlay payload",
            ));
        }
        let mut cursor = Cursor::new(overlay_payload);
        let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
        if magic != TABLE_PAYLOAD_MAGIC {
            return Err(DbError::corruption("table payload magic is invalid"));
        }
        let row_count = cursor.read_u32()? as usize;
        let mut matched_row_bytes = None;
        for _ in 0..row_count {
            let current_row_id = cursor.read_i64()?;
            let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
            let row_bytes = cursor.read_slice(row_bytes_len)?;
            if is_tombstone {
                continue;
            }
            if current_row_id == row_id {
                matched_row_bytes = Some(row_bytes);
            }
        }
        matched_row_bytes.ok_or_else(|| {
            DbError::corruption("paged table overlay row is missing from overlay payload")
        })
    }

    pub(crate) fn row_at_position(&self, position: usize) -> Result<Option<TableRowRef<'_>>> {
        let Some(entry) = self.rows.entry_at(position)? else {
            return Ok(None);
        };
        let chunk = self.chunks.get(entry.chunk_index as usize).ok_or_else(|| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        let Some(row_bytes) = self.row_bytes_for_entry(entry, chunk)? else {
            return Ok(None);
        };
        let row = Row::decode(row_bytes)?;
        Ok(Some(TableRowRef::Decoded(StoredRow {
            row_id: entry.row_id,
            values: row.into_values(),
        })))
    }

    fn projected_values_at_position(
        &self,
        position: usize,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        let Some(entry) = self.rows.entry_at(position)? else {
            return Ok(None);
        };
        let chunk = self.chunks.get(entry.chunk_index as usize).ok_or_else(|| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        let Some(row_bytes) = self.row_bytes_for_entry(entry, chunk)? else {
            return Ok(None);
        };
        Row::decode_projection_sorted_unique_with_overflow::<page::InMemoryPageStore>(
            row_bytes,
            None,
            projection_indexes,
        )
        .map(Some)
    }

    pub(crate) fn full_query_row_by_id(&self, row_id: i64) -> Result<Option<QueryRow>> {
        let Some((_, entry)) = self.rows.entry_for_row_id(row_id)? else {
            return Ok(None);
        };
        let chunk = self.chunks.get(entry.chunk_index as usize).ok_or_else(|| {
            DbError::corruption("paged table chunk index exceeded chunk list length")
        })?;
        let Some(row_bytes) = self.row_bytes_for_entry(entry, chunk)? else {
            return Ok(None);
        };
        Row::decode(row_bytes)
            .map(Row::into_values)
            .map(QueryRow::new)
            .map(Some)
    }

    pub(crate) fn visit_int64_column_values<F>(
        &self,
        column_index: usize,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(i64, Option<i64>) -> Result<()>,
    {
        for entry in self.rows.iter() {
            let entry = entry?;
            let chunk = self.chunks.get(entry.chunk_index as usize).ok_or_else(|| {
                DbError::corruption("paged table chunk index exceeded chunk list length")
            })?;
            let Some(row_bytes) = self.row_bytes_for_entry(entry, chunk)? else {
                continue;
            };
            visitor(entry.row_id, Row::decode_int64_at(row_bytes, column_index)?)?;
        }
        Ok(())
    }

    pub(crate) fn visit_float64_column_values<F>(
        &self,
        column_index: usize,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(i64, Option<f64>) -> Result<()>,
    {
        for entry in self.rows.iter() {
            let entry = entry?;
            let chunk = self.chunks.get(entry.chunk_index as usize).ok_or_else(|| {
                DbError::corruption("paged table chunk index exceeded chunk list length")
            })?;
            let Some(row_bytes) = self.row_bytes_for_entry(entry, chunk)? else {
                continue;
            };
            visitor(
                entry.row_id,
                Row::decode_float64_at(row_bytes, column_index)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn rows(&self) -> TablePageRowIter<'_> {
        TablePageRowIter {
            manifest: self,
            position: 0,
        }
    }

    pub(crate) fn approximate_heap_bytes(&self) -> usize {
        let chunks_bytes = self
            .chunks
            .capacity()
            .saturating_mul(std::mem::size_of::<TablePageManifestChunk>());
        let chunk_payload_bytes = self.chunks.iter().fold(0usize, |bytes, chunk| {
            bytes
                .saturating_add(chunk.payload.capacity())
                .saturating_add(
                    chunk
                        .overlay_payload
                        .as_ref()
                        .map_or(0, |payload| payload.capacity()),
                )
                .saturating_add(
                    chunk
                        .tombstoned_row_ids
                        .len()
                        .saturating_mul(std::mem::size_of::<i64>()),
                )
        });
        chunks_bytes
            .saturating_add(chunk_payload_bytes)
            .saturating_add(
                self.tombstoned_row_ids
                    .len()
                    .saturating_mul(std::mem::size_of::<i64>()),
            )
            .saturating_add(self.rows.approximate_heap_bytes())
    }
}
