//! Thematic extraction (mechanical split; no behavior change).

use super::*;

#[derive(Debug)]
pub(crate) struct TableData {
    pub(crate) rows: Arc<Vec<StoredRow>>,
    tombstoned_row_ids: BTreeSet<i64>,
    rows_sorted_by_id: bool,
    cached_heap_bytes: usize,
}

impl Default for TableData {
    fn default() -> Self {
        Self {
            rows: Arc::new(Vec::new()),
            tombstoned_row_ids: BTreeSet::new(),
            rows_sorted_by_id: true,
            cached_heap_bytes: 0,
        }
    }
}

impl Clone for TableData {
    fn clone(&self) -> Self {
        Self {
            rows: Arc::clone(&self.rows),
            tombstoned_row_ids: self.tombstoned_row_ids.clone(),
            rows_sorted_by_id: self.rows_sorted_by_id,
            cached_heap_bytes: self.cached_heap_bytes,
        }
    }
}

impl PartialEq for TableData {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
            && self.tombstoned_row_ids == other.tombstoned_row_ids
            && self.rows_sorted_by_id == other.rows_sorted_by_id
    }
}

impl TableData {
    pub(crate) fn from_rows(rows: Vec<StoredRow>) -> Self {
        let rows_sorted_by_id = rows.windows(2).all(|pair| pair[0].row_id <= pair[1].row_id);
        let mut data = Self {
            rows: Arc::new(rows),
            tombstoned_row_ids: BTreeSet::new(),
            rows_sorted_by_id,
            cached_heap_bytes: 0,
        };
        data.cached_heap_bytes = data.compute_heap_bytes();
        data
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows
            .len()
            .saturating_sub(self.tombstoned_row_ids.len())
    }

    fn is_row_tombstoned(&self, row_id: i64) -> bool {
        self.tombstoned_row_ids.contains(&row_id)
    }

    pub(crate) fn has_tombstoned_rows(&self) -> bool {
        !self.tombstoned_row_ids.is_empty()
    }

    pub(crate) fn visible_rows(&self) -> impl Iterator<Item = &StoredRow> {
        let has_tombstones = !self.tombstoned_row_ids.is_empty();
        self.rows
            .iter()
            .filter(move |row| !has_tombstones || !self.is_row_tombstoned(row.row_id))
    }

    fn mark_row_deleted(&mut self, row_id: i64) -> bool {
        if self.row_index_by_id(row_id).is_some() {
            self.tombstoned_row_ids.insert(row_id)
        } else {
            false
        }
    }

    pub(crate) fn mark_rows_deleted<'a, I>(&mut self, row_ids: I) -> usize
    where
        I: IntoIterator<Item = &'a i64>,
    {
        row_ids
            .into_iter()
            .filter(|row_id| self.mark_row_deleted(**row_id))
            .count()
    }

    pub(crate) fn mark_existing_row_set_deleted(&mut self, row_ids: &BTreeSet<i64>) -> usize {
        if row_ids.is_empty() {
            return 0;
        }
        let before = self.tombstoned_row_ids.len();
        if self.tombstoned_row_ids.is_empty() {
            self.tombstoned_row_ids = row_ids.clone();
        } else {
            self.tombstoned_row_ids.extend(row_ids.iter().copied());
        }
        self.tombstoned_row_ids.len().saturating_sub(before)
    }

    #[cfg(test)]
    pub(crate) fn reserve_rows(&mut self, additional: usize) {
        let rows = Arc::make_mut(&mut self.rows);
        let old_capacity = rows.capacity();
        rows.reserve(additional);
        self.cached_heap_bytes = self.cached_heap_bytes.saturating_add(
            rows.capacity()
                .saturating_sub(old_capacity)
                .saturating_mul(std::mem::size_of::<StoredRow>()),
        );
    }

    pub(crate) fn clear_rows(&mut self) {
        self.rows = Arc::new(Vec::new());
        self.tombstoned_row_ids.clear();
        self.rows_sorted_by_id = true;
        self.cached_heap_bytes = 0;
    }

    fn shrink_to_fit_if_unique(&mut self) -> usize {
        let Some(rows) = Arc::get_mut(&mut self.rows) else {
            return 0;
        };
        let old_capacity = rows.capacity();
        rows.shrink_to_fit();
        let freed = old_capacity
            .saturating_sub(rows.capacity())
            .saturating_mul(std::mem::size_of::<StoredRow>());
        self.cached_heap_bytes = self.cached_heap_bytes.saturating_sub(freed);
        freed
    }

    pub(crate) fn mutate_visible_rows<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut StoredRow) -> Result<()>,
    {
        let rows = Arc::make_mut(&mut self.rows);
        for row in rows.iter_mut() {
            if !self.tombstoned_row_ids.contains(&row.row_id) {
                f(row)?;
            }
        }
        self.cached_heap_bytes = self.compute_heap_bytes();
        Ok(())
    }

    pub(super) fn row_index_by_id(&self, row_id: i64) -> Option<usize> {
        if !self.tombstoned_row_ids.is_empty() && self.is_row_tombstoned(row_id) {
            return None;
        }
        if let Some(index) = row_id
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
        {
            if let Some(row) = self.rows.get(index) {
                if row.row_id == row_id {
                    return Some(index);
                }
            }
        }

        if self.rows_sorted_by_id {
            if let Ok(index) = self.rows.binary_search_by_key(&row_id, |row| row.row_id) {
                return Some(index);
            }
        }

        self.rows.iter().position(|row| row.row_id == row_id)
    }

    pub(crate) fn row_ids_in_range(&self, low: i64, high: i64) -> Vec<i64> {
        if low > high {
            return Vec::new();
        }
        if self.rows_sorted_by_id {
            let rows = self.rows.as_ref();
            let start = rows.partition_point(|row| row.row_id < low);
            let end = start + rows[start..].partition_point(|row| row.row_id <= high);
            if self.tombstoned_row_ids.is_empty() {
                return rows[start..end].iter().map(|row| row.row_id).collect();
            }
            return rows[start..end]
                .iter()
                .filter_map(|row| {
                    if self.is_row_tombstoned(row.row_id) {
                        None
                    } else {
                        Some(row.row_id)
                    }
                })
                .collect();
        }
        self.rows
            .iter()
            .filter_map(|row| {
                if row.row_id >= low && row.row_id <= high && !self.is_row_tombstoned(row.row_id) {
                    Some(row.row_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn row_by_id(&self, row_id: i64) -> Option<&StoredRow> {
        self.row_index_by_id(row_id)
            .and_then(|index| self.rows.get(index))
    }

    fn projected_values_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        Ok(self
            .row_by_id(row_id)
            .map(|row| project_simple_projection_value_vec(&row.values, projection_indexes)))
    }

    fn projected_query_row_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<QueryRow>> {
        Ok(self
            .row_by_id(row_id)
            .map(|row| project_simple_projection_values(&row.values, projection_indexes)))
    }

    fn full_query_row_by_id(&self, row_id: i64) -> Option<QueryRow> {
        self.row_by_id(row_id)
            .map(|row| QueryRow::new(row.values.clone()))
    }

    fn projected_query_rows_in_id_range(
        &self,
        low: i64,
        high_exclusive: i64,
        limit: usize,
        offset: usize,
        projection_indexes: &[usize],
    ) -> Option<Vec<QueryRow>> {
        if !self.rows_sorted_by_id || high_exclusive <= low {
            return None;
        }
        let rows = self.rows.as_ref();
        let start = rows.partition_point(|row| row.row_id < low);
        let end = start + rows[start..].partition_point(|row| row.row_id < high_exclusive);
        let mut skipped = 0usize;
        let mut projected = Vec::with_capacity(limit.min(end.saturating_sub(start)));
        for row in &rows[start..end] {
            if self.is_row_tombstoned(row.row_id) {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if projected.len() >= limit {
                break;
            }
            projected.push(project_simple_projection_values(
                &row.values,
                projection_indexes,
            ));
        }
        Some(projected)
    }

    fn visit_int64_column_values<F>(&self, column_index: usize, mut visitor: F) -> Result<()>
    where
        F: FnMut(i64, Option<i64>) -> Result<()>,
    {
        for row in self.visible_rows() {
            let value = int64_column_value(row.values.get(column_index))?;
            visitor(row.row_id, value)?;
        }
        Ok(())
    }

    fn visit_float64_column_values<F>(&self, column_index: usize, mut visitor: F) -> Result<()>
    where
        F: FnMut(i64, Option<f64>) -> Result<()>,
    {
        for row in self.visible_rows() {
            let value = float64_column_value(row.values.get(column_index))?;
            visitor(row.row_id, value)?;
        }
        Ok(())
    }

    /// Approximate heap residency of this table's row vector. Includes
    /// `Vec<StoredRow>` capacity plus each row's `Vec<Value>` capacity plus
    /// each `Value`'s heap allocations. Excludes the `TableData` struct
    /// itself. Used by storage instrumentation (ADR 0143 Phase A). This value
    /// is cached and maintained by row mutation helpers.
    #[must_use]
    pub(crate) fn approximate_heap_bytes(&self) -> usize {
        self.cached_heap_bytes
    }

    pub(crate) fn compute_heap_bytes(&self) -> usize {
        let row_struct = std::mem::size_of::<StoredRow>();
        let mut total = self.rows.capacity() * row_struct;
        for row in self.rows.iter() {
            total += Self::row_heap_bytes(row);
        }
        total
    }

    fn row_heap_bytes(row: &StoredRow) -> usize {
        let value_struct = std::mem::size_of::<crate::record::value::Value>();
        row.values.capacity() * value_struct
            + row
                .values
                .iter()
                .map(Value::approximate_heap_bytes)
                .sum::<usize>()
    }

    pub(crate) fn push_row(&mut self, row: StoredRow) {
        if self.tombstoned_row_ids.is_empty() {
            self.push_fresh_row(row);
            return;
        }
        if self.tombstoned_row_ids.remove(&row.row_id) {
            if let Some(index) = self
                .rows
                .iter()
                .position(|candidate| candidate.row_id == row.row_id)
            {
                let rows = Arc::make_mut(&mut self.rows);
                let old_heap_bytes = Self::row_heap_bytes(&rows[index]);
                rows[index] = row;
                let new_heap_bytes = Self::row_heap_bytes(&rows[index]);
                self.cached_heap_bytes = self
                    .cached_heap_bytes
                    .saturating_sub(old_heap_bytes)
                    .saturating_add(new_heap_bytes);
                return;
            }
        }
        self.push_fresh_row(row);
    }

    fn push_fresh_row(&mut self, row: StoredRow) {
        let rows = Arc::make_mut(&mut self.rows);
        let old_capacity = rows.capacity();
        if rows.len() == old_capacity {
            let additional = if old_capacity < 1024 {
                old_capacity.max(8)
            } else {
                old_capacity / 2
            };
            rows.reserve_exact(additional);
        }
        let row_heap_bytes = Self::row_heap_bytes(&row);
        if rows
            .last()
            .is_some_and(|previous| previous.row_id > row.row_id)
        {
            self.rows_sorted_by_id = false;
        }
        rows.push(row);
        self.cached_heap_bytes = self
            .cached_heap_bytes
            .saturating_add(row_heap_bytes)
            .saturating_add(
                rows.capacity()
                    .saturating_sub(old_capacity)
                    .saturating_mul(std::mem::size_of::<StoredRow>()),
            );
    }

    #[cfg(test)]
    pub(crate) fn remove_row(&mut self, row_index: usize) -> StoredRow {
        let rows = Arc::make_mut(&mut self.rows);
        let row = rows.remove(row_index);
        self.tombstoned_row_ids.remove(&row.row_id);
        self.cached_heap_bytes = self
            .cached_heap_bytes
            .saturating_sub(Self::row_heap_bytes(&row));
        row
    }

    #[cfg(test)]
    pub(crate) fn retain_rows<F>(&mut self, mut keep: F)
    where
        F: FnMut(&StoredRow) -> bool,
    {
        let mut removed_heap_bytes = 0usize;
        let rows = Arc::make_mut(&mut self.rows);
        rows.retain(|row| {
            let retain = keep(row);
            if !retain {
                removed_heap_bytes = removed_heap_bytes.saturating_add(Self::row_heap_bytes(row));
                self.tombstoned_row_ids.remove(&row.row_id);
            }
            retain
        });
        self.cached_heap_bytes = self.cached_heap_bytes.saturating_sub(removed_heap_bytes);
    }

    pub(crate) fn replace_value(
        &mut self,
        row_index: usize,
        column_index: usize,
        value: Value,
    ) -> Option<()> {
        if self
            .rows
            .get(row_index)
            .is_some_and(|row| self.is_row_tombstoned(row.row_id))
        {
            return None;
        }
        let rows = Arc::make_mut(&mut self.rows);
        let row = rows.get_mut(row_index)?;
        let slot = row.values.get_mut(column_index)?;
        let old_heap_bytes = slot.approximate_heap_bytes();
        *slot = value;
        let new_heap_bytes = slot.approximate_heap_bytes();
        self.cached_heap_bytes = self
            .cached_heap_bytes
            .saturating_sub(old_heap_bytes)
            .saturating_add(new_heap_bytes);
        Some(())
    }

    pub(crate) fn replace_row_values(
        &mut self,
        row_index: usize,
        values: Vec<Value>,
    ) -> Option<()> {
        if self
            .rows
            .get(row_index)
            .is_some_and(|row| self.is_row_tombstoned(row.row_id))
        {
            return None;
        }
        let rows = Arc::make_mut(&mut self.rows);
        let row = rows.get_mut(row_index)?;
        let old_heap_bytes = Self::row_heap_bytes(row);
        row.values = values;
        let new_heap_bytes = Self::row_heap_bytes(row);
        self.cached_heap_bytes = self
            .cached_heap_bytes
            .saturating_sub(old_heap_bytes)
            .saturating_add(new_heap_bytes);
        Some(())
    }
}

pub(crate) enum TableRowIter<'a> {
    Empty(std::iter::Empty<Result<TableRowRef<'a>>>),
    Resident(TableDataRowIter<'a>),
    Paged(TablePageRowIter<'a>),
}

impl<'a> Iterator for TableRowIter<'a> {
    type Item = Result<TableRowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty(iter) => iter.next(),
            Self::Resident(iter) => iter.next().map(|row| Ok(TableRowRef::Resident(row))),
            Self::Paged(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Empty(iter) => iter.size_hint(),
            Self::Resident(iter) => iter.size_hint(),
            Self::Paged(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for TableRowIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Empty(iter) => iter.len(),
            Self::Resident(iter) => iter.len(),
            Self::Paged(iter) => iter.len(),
        }
    }
}

pub(crate) struct TableDataRowIter<'a> {
    rows: std::slice::Iter<'a, StoredRow>,
    tombstoned_row_ids: &'a BTreeSet<i64>,
    remaining: usize,
}

impl<'a> TableDataRowIter<'a> {
    fn new(data: &'a TableData) -> Self {
        Self {
            rows: data.rows.iter(),
            tombstoned_row_ids: &data.tombstoned_row_ids,
            remaining: data.row_count(),
        }
    }
}

impl<'a> Iterator for TableDataRowIter<'a> {
    type Item = &'a StoredRow;

    fn next(&mut self) -> Option<Self::Item> {
        for row in self.rows.by_ref() {
            if self.tombstoned_row_ids.contains(&row.row_id) {
                continue;
            }
            self.remaining = self.remaining.saturating_sub(1);
            return Some(row);
        }
        self.remaining = 0;
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for TableDataRowIter<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<'a> TableRowIter<'a> {
    pub(crate) fn empty() -> Self {
        Self::Empty(std::iter::empty())
    }
}

pub(crate) struct TablePageRowIter<'a> {
    pub(crate) manifest: &'a TablePageManifest,
    pub(crate) position: usize,
}

impl<'a> Iterator for TablePageRowIter<'a> {
    type Item = Result<TableRowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.position;
        if position >= self.manifest.row_count() {
            return None;
        }
        self.position += 1;
        Some(self.manifest.row_at_position(position).and_then(|row| {
            row.ok_or_else(|| {
                DbError::corruption("paged row iterator advanced beyond manifest bounds")
            })
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.manifest.row_count().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TablePageRowIter<'_> {
    fn len(&self) -> usize {
        self.manifest.row_count().saturating_sub(self.position)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum VisibleTableRowSource<'a> {
    Temp(&'a TableData),
    Base(&'a TableRowSource),
}

impl<'a> VisibleTableRowSource<'a> {
    pub(crate) fn rows(&self) -> TableRowIter<'a> {
        match self {
            Self::Temp(data) => TableRowIter::Resident(TableDataRowIter::new(data)),
            Self::Base(source) => source.rows(),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        match self {
            Self::Temp(data) => data.row_count(),
            Self::Base(source) => source.row_count(),
        }
    }

    pub(crate) fn has_tombstoned_rows(&self) -> bool {
        match self {
            Self::Temp(data) => data.has_tombstoned_rows(),
            Self::Base(source) => source.has_tombstoned_rows(),
        }
    }

    pub(crate) fn row_by_id(&self, row_id: i64) -> Result<Option<TableRowRef<'a>>> {
        match self {
            Self::Temp(data) => Ok(data.row_by_id(row_id).map(TableRowRef::Resident)),
            Self::Base(source) => source.row_by_id(row_id),
        }
    }

    pub(crate) fn row_ids_in_range(&self, low: i64, high: i64) -> Vec<i64> {
        if low > high {
            return Vec::new();
        }
        match self {
            Self::Temp(data) => data
                .rows
                .iter()
                .filter_map(|row| {
                    if row.row_id >= low
                        && row.row_id <= high
                        && !data.is_row_tombstoned(row.row_id)
                    {
                        Some(row.row_id)
                    } else {
                        None
                    }
                })
                .collect(),
            Self::Base(source) => source.row_ids_in_range(low, high),
        }
    }

    pub(crate) fn projected_values_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        match self {
            Self::Temp(data) => data.projected_values_by_id(row_id, projection_indexes),
            Self::Base(source) => source.projected_values_by_id(row_id, projection_indexes),
        }
    }

    pub(crate) fn projected_query_row_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<QueryRow>> {
        match self {
            Self::Temp(data) => data.projected_query_row_by_id(row_id, projection_indexes),
            Self::Base(source) => source.projected_query_row_by_id(row_id, projection_indexes),
        }
    }

    pub(crate) fn full_query_row_by_id(&self, row_id: i64) -> Result<Option<QueryRow>> {
        match self {
            Self::Temp(data) => Ok(data.full_query_row_by_id(row_id)),
            Self::Base(source) => source.full_query_row_by_id(row_id),
        }
    }

    pub(crate) fn projected_query_rows_in_id_range(
        &self,
        low: i64,
        high_exclusive: i64,
        limit: usize,
        offset: usize,
        projection_indexes: &[usize],
    ) -> Option<Vec<QueryRow>> {
        match self {
            Self::Temp(data) => data.projected_query_rows_in_id_range(
                low,
                high_exclusive,
                limit,
                offset,
                projection_indexes,
            ),
            Self::Base(source) => source.projected_query_rows_in_id_range(
                low,
                high_exclusive,
                limit,
                offset,
                projection_indexes,
            ),
        }
    }

    pub(crate) fn row_at_position(&self, position: usize) -> Result<Option<TableRowRef<'a>>> {
        match self {
            Self::Temp(data) => Ok(data.visible_rows().nth(position).map(TableRowRef::Resident)),
            Self::Base(source) => source.row_at_position(position),
        }
    }

    pub(crate) fn visit_int64_column_values<F>(&self, column_index: usize, visitor: F) -> Result<()>
    where
        F: FnMut(i64, Option<i64>) -> Result<()>,
    {
        match self {
            Self::Temp(data) => data.visit_int64_column_values(column_index, visitor),
            Self::Base(source) => source.visit_int64_column_values(column_index, visitor),
        }
    }

    pub(crate) fn visit_float64_column_values<F>(
        &self,
        column_index: usize,
        visitor: F,
    ) -> Result<()>
    where
        F: FnMut(i64, Option<f64>) -> Result<()>,
    {
        match self {
            Self::Temp(data) => data.visit_float64_column_values(column_index, visitor),
            Self::Base(source) => source.visit_float64_column_values(column_index, visitor),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TableRowSource {
    Resident(Arc<TableData>),
    Paged(Arc<TablePageManifest>),
}

impl TableRowSource {
    pub(crate) fn rows(&self) -> TableRowIter<'_> {
        match self {
            Self::Resident(data) => TableRowIter::Resident(TableDataRowIter::new(data)),
            Self::Paged(manifest) => TableRowIter::Paged(manifest.rows()),
        }
    }

    pub(crate) fn resident_data(&self) -> &TableData {
        match self {
            Self::Resident(data) => data.as_ref(),
            // Invariant: this method is only called for resident row sources.
            Self::Paged(_) => unreachable!("paged row sources are not resident table data"),
        }
    }

    pub(crate) fn resident_data_mut(&mut self) -> &mut TableData {
        match self {
            Self::Resident(data) => Arc::make_mut(data),
            // Invariant: this method is only called for mutable resident row sources.
            Self::Paged(_) => unreachable!("paged row sources are not mutable resident table data"),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        match self {
            Self::Resident(data) => data.row_count(),
            Self::Paged(manifest) => manifest.row_count(),
        }
    }

    pub(crate) fn has_tombstoned_rows(&self) -> bool {
        match self {
            Self::Resident(data) => data.has_tombstoned_rows(),
            Self::Paged(manifest) => !manifest.tombstoned_row_ids.is_empty(),
        }
    }

    pub(crate) fn row_by_id(&self, row_id: i64) -> Result<Option<TableRowRef<'_>>> {
        match self {
            Self::Resident(data) => Ok(data.row_by_id(row_id).map(TableRowRef::Resident)),
            Self::Paged(manifest) => manifest.row_by_id(row_id),
        }
    }

    pub(crate) fn row_ids_in_range(&self, low: i64, high: i64) -> Vec<i64> {
        if low > high {
            return Vec::new();
        }
        match self {
            Self::Resident(data) => data.row_ids_in_range(low, high),
            Self::Paged(manifest) => manifest.row_ids_in_range(low, high),
        }
    }

    fn projected_values_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        match self {
            Self::Resident(data) => data.projected_values_by_id(row_id, projection_indexes),
            Self::Paged(manifest) => manifest.projected_values_by_id(row_id, projection_indexes),
        }
    }

    fn projected_query_row_by_id(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<QueryRow>> {
        match self {
            Self::Resident(data) => data.projected_query_row_by_id(row_id, projection_indexes),
            Self::Paged(manifest) => manifest
                .projected_values_by_id(row_id, projection_indexes)
                .map(|values| values.map(QueryRow::new)),
        }
    }

    fn full_query_row_by_id(&self, row_id: i64) -> Result<Option<QueryRow>> {
        match self {
            Self::Resident(data) => Ok(data.full_query_row_by_id(row_id)),
            Self::Paged(manifest) => manifest.full_query_row_by_id(row_id),
        }
    }

    fn projected_query_rows_in_id_range(
        &self,
        low: i64,
        high_exclusive: i64,
        limit: usize,
        offset: usize,
        projection_indexes: &[usize],
    ) -> Option<Vec<QueryRow>> {
        match self {
            Self::Resident(data) => data.projected_query_rows_in_id_range(
                low,
                high_exclusive,
                limit,
                offset,
                projection_indexes,
            ),
            Self::Paged(_) => None,
        }
    }

    fn row_at_position(&self, position: usize) -> Result<Option<TableRowRef<'_>>> {
        match self {
            Self::Resident(data) => {
                Ok(data.visible_rows().nth(position).map(TableRowRef::Resident))
            }
            Self::Paged(manifest) => manifest.row_at_position(position),
        }
    }

    fn visit_int64_column_values<F>(&self, column_index: usize, visitor: F) -> Result<()>
    where
        F: FnMut(i64, Option<i64>) -> Result<()>,
    {
        match self {
            Self::Resident(data) => data.visit_int64_column_values(column_index, visitor),
            Self::Paged(manifest) => manifest.visit_int64_column_values(column_index, visitor),
        }
    }

    fn visit_float64_column_values<F>(&self, column_index: usize, visitor: F) -> Result<()>
    where
        F: FnMut(i64, Option<f64>) -> Result<()>,
    {
        match self {
            Self::Resident(data) => data.visit_float64_column_values(column_index, visitor),
            Self::Paged(manifest) => manifest.visit_float64_column_values(column_index, visitor),
        }
    }

    pub(crate) fn approximate_heap_bytes(&self) -> usize {
        match self {
            Self::Resident(data) => data.approximate_heap_bytes(),
            Self::Paged(manifest) => manifest.approximate_heap_bytes(),
        }
    }

    pub(crate) fn shrink_resident_to_fit_if_unique(&mut self) -> usize {
        match self {
            Self::Resident(data) => Arc::get_mut(data)
                .map(TableData::shrink_to_fit_if_unique)
                .unwrap_or(0),
            Self::Paged(_) => 0,
        }
    }

    pub(crate) fn paged_manifest(&self) -> Option<&TablePageManifest> {
        match self {
            Self::Resident(_) => None,
            Self::Paged(manifest) => Some(manifest),
        }
    }
}
