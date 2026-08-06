//! Thematic extraction (mechanical split; no behavior change).

use super::*;

/// Runtime storage for a unique typed `INT64` index.
///
/// Integer primary keys commonly map a contiguous key range to identical row
/// IDs. Keeping that relation as a range avoids allocating and populating a
/// hash-map entry for every row while retaining a conservative sparse fallback
/// for every other unique integer index shape (ADR 0203).
#[derive(Clone, Debug)]
pub(crate) enum UniqueInt64Keys {
    DenseIdentity { start: i64, len: usize },
    Sparse(Int64Map<i64>),
}

impl Default for UniqueInt64Keys {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueInt64Keys {
    pub(crate) fn new() -> Self {
        Self::DenseIdentity { start: 0, len: 0 }
    }

    fn dense_value_at(start: i64, offset: usize) -> Option<i64> {
        let offset = i128::try_from(offset).ok()?;
        i64::try_from(i128::from(start) + offset).ok()
    }

    pub(crate) fn dense_contains(start: i64, len: usize, key: i64) -> bool {
        let offset = i128::from(key) - i128::from(start);
        offset >= 0 && usize::try_from(offset).is_ok_and(|offset| offset < len)
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::DenseIdentity { len, .. } => *len,
            Self::Sparse(keys) => keys.len(),
        }
    }

    pub(crate) fn get(&self, key: &i64) -> Option<i64> {
        match self {
            Self::DenseIdentity { start, len } if Self::dense_contains(*start, *len, *key) => {
                Some(*key)
            }
            Self::DenseIdentity { .. } => None,
            Self::Sparse(keys) => keys.get(key).copied(),
        }
    }

    pub(crate) fn iter(&self) -> UniqueInt64KeysIter<'_> {
        match self {
            Self::DenseIdentity { start, len } => UniqueInt64KeysIter::Dense {
                start: *start,
                offset: 0,
                len: *len,
            },
            Self::Sparse(keys) => UniqueInt64KeysIter::Sparse(keys.iter()),
        }
    }

    fn materialize_sparse(&mut self) {
        let Self::DenseIdentity { start, len } = self else {
            return;
        };
        let start = *start;
        let len = *len;
        let mut keys = HashMap::with_capacity_and_hasher(len, Int64HashBuilder::default());
        for offset in 0..len {
            let Some(value) = Self::dense_value_at(start, offset) else {
                debug_assert!(false, "dense INT64 identity range exceeded i64 bounds");
                break;
            };
            keys.insert(value, value);
        }
        *self = Self::Sparse(keys);
    }

    /// Insert a mapping and return the previous row ID, matching
    /// `HashMap::insert` semantics.
    pub(crate) fn insert(&mut self, key: i64, row_id: i64) -> Option<i64> {
        match self {
            Self::DenseIdentity { start, len } => {
                if Self::dense_contains(*start, *len, key) {
                    if key == row_id {
                        return Some(key);
                    }
                } else if key == row_id {
                    if *len == 0 {
                        *start = key;
                        *len = 1;
                        return None;
                    }
                    if Self::dense_value_at(*start, *len) == Some(key) {
                        *len = len.saturating_add(1);
                        return None;
                    }
                    if start.checked_sub(1) == Some(key) {
                        *start = key;
                        *len = len.saturating_add(1);
                        return None;
                    }
                }
                self.materialize_sparse();
                let Self::Sparse(keys) = self else {
                    return None;
                };
                keys.insert(key, row_id)
            }
            Self::Sparse(keys) => keys.insert(key, row_id),
        }
    }

    fn remove_row_id_mapping(&mut self, row_id: i64) {
        match self {
            Self::DenseIdentity { start, len } if Self::dense_contains(*start, *len, row_id) => {
                if *len == 1 {
                    *len = 0;
                } else if *start == row_id {
                    *start = start.saturating_add(1);
                    *len -= 1;
                } else if Self::dense_value_at(*start, len.saturating_sub(1)) == Some(row_id) {
                    *len -= 1;
                } else {
                    self.materialize_sparse();
                    if let Self::Sparse(keys) = self {
                        keys.remove(&row_id);
                    }
                }
            }
            Self::DenseIdentity { .. } => {}
            Self::Sparse(keys) => keys.retain(|_, existing| *existing != row_id),
        }
    }

    fn shrink_to_fit(&mut self) -> usize {
        let Self::Sparse(keys) = self else {
            return 0;
        };
        let old_capacity = keys.capacity();
        keys.shrink_to_fit();
        old_capacity
            .saturating_sub(keys.capacity())
            .saturating_mul(std::mem::size_of::<(i64, i64)>())
    }

    #[cfg(test)]
    pub(crate) fn is_dense_identity(&self) -> bool {
        matches!(self, Self::DenseIdentity { .. })
    }
}

pub(crate) enum UniqueInt64KeysIter<'a> {
    Dense {
        start: i64,
        offset: usize,
        len: usize,
    },
    Sparse(std::collections::hash_map::Iter<'a, i64, i64>),
}

impl Iterator for UniqueInt64KeysIter<'_> {
    type Item = (i64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense { start, offset, len } => {
                if *offset >= *len {
                    return None;
                }
                let value = UniqueInt64Keys::dense_value_at(*start, *offset)?;
                *offset += 1;
                Some((value, value))
            }
            Self::Sparse(iter) => iter.next().map(|(key, row_id)| (*key, *row_id)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense { offset, len, .. } => {
                let remaining = len.saturating_sub(*offset);
                (remaining, Some(remaining))
            }
            Self::Sparse(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for UniqueInt64KeysIter<'_> {}

pub(crate) enum RuntimeInt64RowIdsIter<'a> {
    One(Option<i64>),
    Contiguous {
        start: i64,
        offset: usize,
        len: usize,
    },
    Many(std::slice::Iter<'a, i64>),
}

impl Iterator for RuntimeInt64RowIdsIter<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(row_id) => row_id.take(),
            Self::Contiguous { start, offset, len } => {
                if *offset >= *len {
                    return None;
                }
                let row_id = RuntimeInt64RowIds::value_at(*start, *offset)?;
                *offset += 1;
                Some(row_id)
            }
            Self::Many(row_ids) => row_ids.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::One(row_id) => usize::from(row_id.is_some()),
            Self::Contiguous { offset, len, .. } => len.saturating_sub(*offset),
            Self::Many(row_ids) => row_ids.len(),
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RuntimeInt64RowIdsIter<'_> {}

/// Runtime key-domain storage for a non-unique typed `INT64` index.
///
/// Dense mode is intentionally conservative: insertions may repeat only the
/// current final key or append its immediate successor. Gaps and out-of-order
/// key insertion materialize a sparse identity-hashed map, ensuring arbitrary
/// workloads keep general hash-map behavior while grouped benchmark-shaped
/// foreign-key indexes avoid per-key hash buckets.
#[derive(Clone, Debug)]
pub(crate) enum NonUniqueInt64Keys {
    Dense {
        start: i64,
        postings: Vec<RuntimeInt64RowIds>,
    },
    Sparse(Int64Map<RuntimeInt64RowIds>),
}

impl Default for NonUniqueInt64Keys {
    fn default() -> Self {
        Self::new()
    }
}

impl NonUniqueInt64Keys {
    pub(crate) fn new() -> Self {
        Self::Dense {
            start: 0,
            postings: Vec::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Dense { postings, .. } => postings.len(),
            Self::Sparse(keys) => keys.len(),
        }
    }

    pub(crate) fn get(&self, key: &i64) -> Option<&RuntimeInt64RowIds> {
        match self {
            Self::Dense { start, postings }
                if UniqueInt64Keys::dense_contains(*start, postings.len(), *key) =>
            {
                let offset = usize::try_from(i128::from(*key) - i128::from(*start)).ok()?;
                postings.get(offset)
            }
            Self::Dense { .. } => None,
            Self::Sparse(keys) => keys.get(key),
        }
    }

    fn get_mut(&mut self, key: &i64) -> Option<&mut RuntimeInt64RowIds> {
        match self {
            Self::Dense { start, postings }
                if UniqueInt64Keys::dense_contains(*start, postings.len(), *key) =>
            {
                let offset = usize::try_from(i128::from(*key) - i128::from(*start)).ok()?;
                postings.get_mut(offset)
            }
            Self::Dense { .. } => None,
            Self::Sparse(keys) => keys.get_mut(key),
        }
    }

    pub(crate) fn iter(&self) -> NonUniqueInt64KeysIter<'_> {
        match self {
            Self::Dense { start, postings } => NonUniqueInt64KeysIter::Dense {
                start: *start,
                offset: 0,
                postings: postings.iter(),
            },
            Self::Sparse(keys) => NonUniqueInt64KeysIter::Sparse(keys.iter()),
        }
    }

    fn values(&self) -> NonUniqueInt64Values<'_> {
        match self {
            Self::Dense { postings, .. } => NonUniqueInt64Values::Dense(postings.iter()),
            Self::Sparse(keys) => NonUniqueInt64Values::Sparse(keys.values()),
        }
    }

    fn materialize_sparse(&mut self) {
        let Self::Dense { start, postings } = self else {
            return;
        };
        let start = *start;
        let postings = std::mem::take(postings);
        let mut keys =
            Int64Map::with_capacity_and_hasher(postings.len(), Int64HashBuilder::default());
        for (offset, row_ids) in postings.into_iter().enumerate() {
            let Some(key) = UniqueInt64Keys::dense_value_at(start, offset) else {
                debug_assert!(false, "dense non-unique INT64 range exceeded i64 bounds");
                break;
            };
            keys.insert(key, row_ids);
        }
        *self = Self::Sparse(keys);
    }

    pub(crate) fn insert_row_id(&mut self, key: i64, row_id: i64) {
        match self {
            Self::Dense { start, postings } => {
                if postings.is_empty() {
                    *start = key;
                    postings.push(RuntimeInt64RowIds::one(row_id));
                    return;
                }
                let last_offset = postings.len().saturating_sub(1);
                if UniqueInt64Keys::dense_value_at(*start, last_offset) == Some(key) {
                    if let Some(posting) = postings.last_mut() {
                        posting.push(row_id);
                    }
                    return;
                }
                if UniqueInt64Keys::dense_value_at(*start, postings.len()) == Some(key) {
                    postings.push(RuntimeInt64RowIds::one(row_id));
                    return;
                }
                self.materialize_sparse();
                self.insert_row_id(key, row_id);
            }
            Self::Sparse(keys) => match keys.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(RuntimeInt64RowIds::one(row_id));
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(row_id);
                }
            },
        }
    }

    fn remove_row_id_mapping(&mut self, row_id: i64) {
        match self {
            Self::Dense { start, postings } => {
                for posting in postings.iter_mut() {
                    if posting.contains(&row_id) {
                        posting.retain(|existing| *existing != row_id);
                    }
                }
                while postings.last().is_some_and(RuntimeInt64RowIds::is_empty) {
                    postings.pop();
                }
                while postings.first().is_some_and(RuntimeInt64RowIds::is_empty) {
                    postings.remove(0);
                    *start = start.saturating_add(1);
                }
                if postings.iter().any(RuntimeInt64RowIds::is_empty) {
                    self.materialize_sparse();
                    if let Self::Sparse(keys) = self {
                        keys.retain(|_, posting| !posting.is_empty());
                    }
                }
            }
            Self::Sparse(keys) => {
                for posting in keys.values_mut() {
                    if posting.contains(&row_id) {
                        posting.retain(|existing| *existing != row_id);
                    }
                }
                keys.retain(|_, posting| !posting.is_empty());
            }
        }
    }

    fn remove_empty_key(&mut self, key: i64) {
        if self.get(&key).is_none_or(|posting| !posting.is_empty()) {
            return;
        }
        match self {
            Self::Dense { start, postings } => {
                let Some(offset) = usize::try_from(i128::from(key) - i128::from(*start)).ok()
                else {
                    return;
                };
                if offset == postings.len().saturating_sub(1) {
                    postings.pop();
                } else if offset == 0 {
                    postings.remove(0);
                    *start = start.saturating_add(1);
                } else {
                    self.materialize_sparse();
                    if let Self::Sparse(keys) = self {
                        keys.remove(&key);
                    }
                }
            }
            Self::Sparse(keys) => {
                keys.remove(&key);
            }
        }
    }

    fn shrink_to_fit(&mut self) -> usize {
        match self {
            Self::Dense { postings, .. } => {
                let old_capacity = postings.capacity();
                let mut freed = postings.iter_mut().fold(0usize, |freed, posting| {
                    freed.saturating_add(posting.shrink_to_fit())
                });
                postings.shrink_to_fit();
                freed = freed.saturating_add(
                    old_capacity
                        .saturating_sub(postings.capacity())
                        .saturating_mul(std::mem::size_of::<RuntimeInt64RowIds>()),
                );
                freed
            }
            Self::Sparse(keys) => {
                let old_capacity = keys.capacity();
                let mut freed = keys.values_mut().fold(0usize, |freed, posting| {
                    freed.saturating_add(posting.shrink_to_fit())
                });
                keys.shrink_to_fit();
                freed = freed.saturating_add(
                    old_capacity
                        .saturating_sub(keys.capacity())
                        .saturating_mul(std::mem::size_of::<(i64, RuntimeInt64RowIds)>()),
                );
                freed
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn is_dense(&self) -> bool {
        matches!(self, Self::Dense { .. })
    }
}

pub(crate) enum NonUniqueInt64KeysIter<'a> {
    Dense {
        start: i64,
        offset: usize,
        postings: std::slice::Iter<'a, RuntimeInt64RowIds>,
    },
    Sparse(std::collections::hash_map::Iter<'a, i64, RuntimeInt64RowIds>),
}

impl<'a> Iterator for NonUniqueInt64KeysIter<'a> {
    type Item = (i64, &'a RuntimeInt64RowIds);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense {
                start,
                offset,
                postings,
            } => {
                let row_ids = postings.next()?;
                let key = UniqueInt64Keys::dense_value_at(*start, *offset)?;
                *offset += 1;
                Some((key, row_ids))
            }
            Self::Sparse(keys) => keys.next().map(|(key, row_ids)| (*key, row_ids)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense { postings, .. } => postings.size_hint(),
            Self::Sparse(keys) => keys.size_hint(),
        }
    }
}

impl ExactSizeIterator for NonUniqueInt64KeysIter<'_> {}

pub(crate) enum NonUniqueInt64Values<'a> {
    Dense(std::slice::Iter<'a, RuntimeInt64RowIds>),
    Sparse(std::collections::hash_map::Values<'a, i64, RuntimeInt64RowIds>),
}

impl<'a> Iterator for NonUniqueInt64Values<'a> {
    type Item = &'a RuntimeInt64RowIds;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(postings) => postings.next(),
            Self::Sparse(postings) => postings.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Dense(postings) => postings.size_hint(),
            Self::Sparse(postings) => postings.size_hint(),
        }
    }
}

impl ExactSizeIterator for NonUniqueInt64Values<'_> {}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeBtreeKeys {
    UniqueEncoded(Arc<BTreeMap<RuntimeEncodedKey, i64>>, BTreeSet<i64>),
    NonUniqueEncoded(Arc<RuntimeEncodedPostings>, BTreeSet<i64>),
    UniqueInt64(Arc<UniqueInt64Keys>, BTreeSet<i64>),
    NonUniqueInt64(Arc<NonUniqueInt64Keys>, BTreeSet<i64>),
    UniqueUuid(Arc<BTreeMap<[u8; 16], i64>>, BTreeSet<i64>),
    NonUniqueUuid(Arc<BTreeMap<[u8; 16], Vec<i64>>>, BTreeSet<i64>),
}

/// Non-unique encoded-key map plus exact state for whether any posting can
/// release capacity at commit. High-cardinality text indexes normally contain
/// only singleton values, so commit can skip an otherwise
/// linear scan over every key.
#[derive(Debug)]
pub(crate) struct RuntimeEncodedPostings {
    entries: BTreeMap<RuntimeEncodedKey, RuntimeEncodedRowIds>,
    shrinkable_postings: usize,
}

impl Clone for RuntimeEncodedPostings {
    fn clone(&self) -> Self {
        // Cloning a Vec is allowed to choose a capacity different from the
        // source. Recompute rather than copying the count so Arc::make_mut's
        // COW clone cannot leave shrink bookkeeping stale.
        Self::new(self.entries.clone())
    }
}

impl RuntimeEncodedPostings {
    pub(crate) fn new(entries: BTreeMap<RuntimeEncodedKey, RuntimeEncodedRowIds>) -> Self {
        let shrinkable_postings = entries
            .values()
            .filter(|row_ids| row_ids.is_shrinkable())
            .count();
        Self {
            entries,
            shrinkable_postings,
        }
    }

    fn adjust_shrinkable_count(&mut self, was_shrinkable: bool, is_shrinkable: bool) {
        match (was_shrinkable, is_shrinkable) {
            (false, true) => self.shrinkable_postings = self.shrinkable_postings.saturating_add(1),
            (true, false) => self.shrinkable_postings = self.shrinkable_postings.saturating_sub(1),
            _ => {}
        }
    }

    pub(crate) fn insert_row_id(&mut self, key: RuntimeEncodedKey, row_id: i64) {
        use std::collections::btree_map::Entry;

        let (was_shrinkable, is_shrinkable) = match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(RuntimeEncodedRowIds::one(row_id));
                (false, false)
            }
            Entry::Occupied(mut entry) => {
                let row_ids = entry.get_mut();
                let was_shrinkable = row_ids.is_shrinkable();
                row_ids.push(row_id);
                (was_shrinkable, row_ids.is_shrinkable())
            }
        };
        self.adjust_shrinkable_count(was_shrinkable, is_shrinkable);
    }

    pub(crate) fn remove_row_id_everywhere(&mut self, row_id: i64) {
        for row_ids in self.entries.values_mut() {
            row_ids.retain(|existing| *existing != row_id);
        }
        self.entries.retain(|_, row_ids| !row_ids.is_empty());
        self.shrinkable_postings = self
            .entries
            .values()
            .filter(|row_ids| row_ids.is_shrinkable())
            .count();
    }

    pub(crate) fn remove_row_id_for_key(&mut self, key: &[u8], row_id: i64) {
        let Some(row_ids) = self.entries.get_mut(key) else {
            return;
        };
        let was_shrinkable = row_ids.is_shrinkable();
        row_ids.retain(|existing| *existing != row_id);
        let is_empty = row_ids.is_empty();
        let is_shrinkable = !is_empty && row_ids.is_shrinkable();
        if is_empty {
            self.entries.remove(key);
        }
        self.adjust_shrinkable_count(was_shrinkable, is_shrinkable);
    }

    pub(crate) fn shrink_to_fit(&mut self) -> usize {
        if self.shrinkable_postings == 0 {
            return 0;
        }
        let freed = self.entries.values_mut().fold(0usize, |freed, row_ids| {
            freed.saturating_add(row_ids.shrink_to_fit())
        });
        // Vec::shrink_to_fit is explicitly best-effort. Recompute from the
        // allocator's actual post-shrink capacities so zero can never become a
        // false-clean state that permanently suppresses later compaction.
        self.shrinkable_postings = self
            .entries
            .values()
            .filter(|row_ids| row_ids.is_shrinkable())
            .count();
        freed
    }

    #[cfg(test)]
    pub(crate) fn shrinkable_posting_count(&self) -> usize {
        self.shrinkable_postings
    }
}

impl std::ops::Deref for RuntimeEncodedPostings {
    type Target = BTreeMap<RuntimeEncodedKey, RuntimeEncodedRowIds>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

/// Row IDs stored beneath one encoded key in a non-unique runtime index.
///
/// Encoded indexes are commonly declared non-unique even when their data is
/// high-cardinality. On 64-bit targets, keeping the first row ID inline avoids
/// a heap allocation for every such key while preserving the insertion order
/// used by index scans; a second row promotes the singleton to the existing
/// `Vec` layout. Supported 32-bit targets use `Vec` directly because an
/// `i64`-carrying enum would exceed the former three-word object footprint.
#[cfg(target_pointer_width = "64")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeEncodedRowIds {
    One(i64),
    Many(Vec<i64>),
}

#[cfg(target_pointer_width = "32")]
#[derive(Clone, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct RuntimeEncodedRowIds(Vec<i64>);

impl RuntimeEncodedRowIds {
    pub(crate) fn one(row_id: i64) -> Self {
        #[cfg(target_pointer_width = "64")]
        {
            Self::One(row_id)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Self(vec![row_id])
        }
    }

    #[cfg(test)]
    pub(crate) fn many(row_ids: Vec<i64>) -> Self {
        #[cfg(target_pointer_width = "64")]
        {
            Self::Many(row_ids)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Self(row_ids)
        }
    }

    pub(crate) fn as_slice(&self) -> &[i64] {
        #[cfg(target_pointer_width = "64")]
        {
            match self {
                Self::One(row_id) => std::slice::from_ref(row_id),
                Self::Many(row_ids) => row_ids.as_slice(),
            }
        }
        #[cfg(target_pointer_width = "32")]
        {
            self.0.as_slice()
        }
    }

    pub(crate) fn push(&mut self, row_id: i64) {
        #[cfg(target_pointer_width = "64")]
        {
            match self {
                Self::One(first_row_id) => {
                    // Match Vec's small-allocation growth behavior so postings
                    // with a few duplicates do not immediately reallocate.
                    let mut row_ids = Vec::with_capacity(4);
                    row_ids.push(*first_row_id);
                    row_ids.push(row_id);
                    *self = Self::Many(row_ids);
                }
                Self::Many(row_ids) => row_ids.push(row_id),
            }
        }
        #[cfg(target_pointer_width = "32")]
        {
            self.0.push(row_id);
        }
    }

    pub(crate) fn is_shrinkable(&self) -> bool {
        #[cfg(target_pointer_width = "64")]
        {
            match self {
                Self::One(_) => false,
                Self::Many(row_ids) => row_ids.len() == 1 || row_ids.capacity() > row_ids.len(),
            }
        }
        #[cfg(target_pointer_width = "32")]
        {
            self.0.capacity() > self.0.len()
        }
    }

    pub(crate) fn retain(&mut self, retain: impl FnMut(&i64) -> bool) {
        #[cfg(target_pointer_width = "64")]
        {
            let mut retain = retain;
            match self {
                Self::One(row_id) => {
                    if !retain(row_id) {
                        // Empty postings are transient: every map-owning caller
                        // removes the entry immediately after retaining.
                        *self = Self::Many(Vec::new());
                    }
                }
                Self::Many(row_ids) => row_ids.retain(retain),
            }
        }
        #[cfg(target_pointer_width = "32")]
        {
            self.0.retain(retain);
        }
    }

    pub(crate) fn shrink_to_fit(&mut self) -> usize {
        #[cfg(target_pointer_width = "64")]
        {
            let Self::Many(row_ids) = self else {
                return 0;
            };
            let old_capacity = row_ids.capacity();
            if row_ids.len() == 1 {
                let row_id = row_ids[0];
                *self = Self::One(row_id);
                return old_capacity.saturating_mul(std::mem::size_of::<i64>());
            }
            row_ids.shrink_to_fit();
            old_capacity
                .saturating_sub(row_ids.capacity())
                .saturating_mul(std::mem::size_of::<i64>())
        }
        #[cfg(target_pointer_width = "32")]
        {
            let old_capacity = self.0.capacity();
            self.0.shrink_to_fit();
            old_capacity
                .saturating_sub(self.0.capacity())
                .saturating_mul(std::mem::size_of::<i64>())
        }
    }

    #[cfg(test)]
    pub(crate) fn is_inline_singleton(&self) -> bool {
        #[cfg(target_pointer_width = "64")]
        {
            matches!(self, Self::One(_))
        }
        #[cfg(target_pointer_width = "32")]
        {
            false
        }
    }
}

impl std::ops::Deref for RuntimeEncodedRowIds {
    type Target = [i64];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl RuntimeBtreeKeys {
    fn shrink_row_id_vecs<'a>(row_ids: impl Iterator<Item = &'a mut Vec<i64>>) -> usize {
        let mut freed = 0usize;
        for row_ids in row_ids {
            let old_capacity = row_ids.capacity();
            row_ids.shrink_to_fit();
            freed = freed.saturating_add(
                old_capacity
                    .saturating_sub(row_ids.capacity())
                    .saturating_mul(std::mem::size_of::<i64>()),
            );
        }
        freed
    }

    pub(crate) fn shrink_to_fit_if_unique(&mut self) -> usize {
        match self {
            Self::UniqueEncoded(_, _)
            | Self::UniqueUuid(_, _)
            | Self::NonUniqueEncoded(_, _)
            | Self::NonUniqueUuid(_, _) => {
                let mut freed = 0usize;
                match self {
                    Self::NonUniqueEncoded(keys, _) => {
                        if let Some(keys) = Arc::get_mut(keys) {
                            freed = freed.saturating_add(keys.shrink_to_fit());
                        }
                    }
                    Self::NonUniqueUuid(keys, _) => {
                        if let Some(keys) = Arc::get_mut(keys) {
                            freed =
                                freed.saturating_add(Self::shrink_row_id_vecs(keys.values_mut()));
                        }
                    }
                    _ => {}
                }
                freed
            }
            Self::UniqueInt64(keys, _) => Arc::get_mut(keys)
                .map(UniqueInt64Keys::shrink_to_fit)
                .unwrap_or(0),
            Self::NonUniqueInt64(keys, _) => {
                let Some(keys) = Arc::get_mut(keys) else {
                    return 0;
                };
                keys.shrink_to_fit()
            }
        }
    }

    fn push_non_unique_row_id(row_ids: &mut Vec<i64>, row_id: i64) {
        // Keep Vec's geometric growth policy on the insert hot path. The
        // explicit 1.5x `reserve_exact` policy caused several extra
        // reallocations for the common 50-150-row posting lists while the
        // post-commit shrink pass already recovers excess capacity.
        row_ids.push(row_id);
    }

    fn visible_single(row_id: i64, deleted_row_ids: &BTreeSet<i64>) -> RuntimeRowIdSet<'_> {
        if deleted_row_ids.is_empty() {
            return RuntimeRowIdSet::Single(row_id);
        }
        if deleted_row_ids.contains(&row_id) {
            RuntimeRowIdSet::Empty
        } else {
            RuntimeRowIdSet::Single(row_id)
        }
    }

    fn visible_many<'a>(
        row_ids: &'a [i64],
        deleted_row_ids: &BTreeSet<i64>,
    ) -> RuntimeRowIdSet<'a> {
        if deleted_row_ids.is_empty() {
            return RuntimeRowIdSet::Many(row_ids);
        }
        let mut first_deleted = None;
        for (index, row_id) in row_ids.iter().copied().enumerate() {
            if deleted_row_ids.contains(&row_id) {
                first_deleted = Some(index);
                break;
            }
        }
        let Some(first_deleted) = first_deleted else {
            return RuntimeRowIdSet::Many(row_ids);
        };

        let mut visible = Vec::new();
        for row_id in row_ids[..first_deleted].iter().copied() {
            if !deleted_row_ids.contains(&row_id) {
                visible.push(row_id);
            }
        }
        for row_id in row_ids[first_deleted..].iter().copied() {
            if !deleted_row_ids.contains(&row_id) {
                visible.push(row_id);
            }
        }
        if visible.is_empty() {
            RuntimeRowIdSet::Empty
        } else {
            RuntimeRowIdSet::Owned(visible)
        }
    }

    fn visible_encoded_row_ids<'a>(
        row_ids: &'a RuntimeEncodedRowIds,
        deleted_row_ids: &'a BTreeSet<i64>,
    ) -> RuntimeRowIdSet<'a> {
        match row_ids.as_slice() {
            [row_id] => Self::visible_single(*row_id, deleted_row_ids),
            row_ids => Self::visible_many(row_ids, deleted_row_ids),
        }
    }

    fn visible_int64_row_ids<'a>(
        row_ids: &'a RuntimeInt64RowIds,
        deleted_row_ids: &'a BTreeSet<i64>,
    ) -> RuntimeRowIdSet<'a> {
        match row_ids {
            RuntimeInt64RowIds::One(row_id) => Self::visible_single(*row_id, deleted_row_ids),
            RuntimeInt64RowIds::Contiguous { start, len } => {
                if deleted_row_ids.is_empty() {
                    return RuntimeRowIdSet::Contiguous {
                        start: *start,
                        len: *len,
                    };
                }
                let Some(end) = len
                    .checked_sub(1)
                    .and_then(|offset| RuntimeInt64RowIds::value_at(*start, offset))
                else {
                    return RuntimeRowIdSet::Empty;
                };
                if deleted_row_ids.range(*start..=end).next().is_none() {
                    return RuntimeRowIdSet::Contiguous {
                        start: *start,
                        len: *len,
                    };
                }
                let visible = row_ids
                    .iter()
                    .filter(|row_id| !deleted_row_ids.contains(row_id))
                    .collect::<Vec<_>>();
                if visible.is_empty() {
                    RuntimeRowIdSet::Empty
                } else {
                    RuntimeRowIdSet::Owned(visible)
                }
            }
            RuntimeInt64RowIds::Many(row_ids) => Self::visible_many(row_ids, deleted_row_ids),
        }
    }

    pub(crate) fn row_ids_for_row_id(&self, row_id: i64) -> RuntimeRowIdSet<'_> {
        match self {
            Self::UniqueInt64(keys, deleted) => {
                keys.get(&row_id).map_or(RuntimeRowIdSet::Empty, |row_id| {
                    Self::visible_single(row_id, deleted)
                })
            }
            Self::NonUniqueInt64(keys, deleted) => keys
                .get(&row_id)
                .map(|row_ids| Self::visible_int64_row_ids(row_ids, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            Self::UniqueEncoded(..)
            | Self::NonUniqueEncoded(..)
            | Self::UniqueUuid(..)
            | Self::NonUniqueUuid(..) => RuntimeRowIdSet::Empty,
        }
    }

    pub(crate) fn row_id_set_for_key(&self, key: &RuntimeBtreeKey) -> RuntimeRowIdSet<'_> {
        match (self, key) {
            (Self::UniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key))
                if deleted.is_empty() =>
            {
                keys.get(key)
                    .copied()
                    .map_or(RuntimeRowIdSet::Empty, RuntimeRowIdSet::Single)
            }
            (Self::UniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .copied()
                .map(|row_id| Self::visible_single(row_id, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::NonUniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .map(|row_ids| Self::visible_encoded_row_ids(row_ids, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::UniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key))
                if deleted.is_empty() =>
            {
                keys.get(key)
                    .map_or(RuntimeRowIdSet::Empty, RuntimeRowIdSet::Single)
            }
            (Self::UniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .map(|row_id| Self::visible_single(row_id, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::NonUniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .map(|row_ids| Self::visible_int64_row_ids(row_ids, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::UniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) if deleted.is_empty() => {
                keys.get(key)
                    .copied()
                    .map_or(RuntimeRowIdSet::Empty, RuntimeRowIdSet::Single)
            }
            (Self::UniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => keys
                .get(key)
                .copied()
                .map(|row_id| Self::visible_single(row_id, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::NonUniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key))
                if deleted.is_empty() =>
            {
                keys.get(key)
                    .map(|row_ids| RuntimeRowIdSet::Many(row_ids.as_slice()))
                    .unwrap_or(RuntimeRowIdSet::Empty)
            }
            (Self::NonUniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => keys
                .get(key)
                .map(|row_ids| Self::visible_many(row_ids, deleted))
                .unwrap_or(RuntimeRowIdSet::Empty),
            _ => RuntimeRowIdSet::Empty,
        }
    }

    pub(super) fn row_ids_for_key(&self, key: &RuntimeBtreeKey) -> Vec<i64> {
        let row_ids = self.row_id_set_for_key(key);
        let mut values = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| values.push(row_id));
        values
    }

    pub(super) fn row_ids_for_encoded_key_prefix(&self, prefix: &[Value]) -> Result<Vec<i64>> {
        if prefix.is_empty() {
            return Ok(Vec::new());
        }
        self.row_ids_for_encoded_key_prefixes(std::slice::from_ref(&prefix))
    }

    pub(super) fn row_ids_for_encoded_key_prefixes(
        &self,
        prefixes: &[&[Value]],
    ) -> Result<Vec<i64>> {
        if prefixes.is_empty() || prefixes.iter().any(|prefix| prefix.is_empty()) {
            return Ok(Vec::new());
        }
        let mut row_ids = Vec::new();
        let mut collect_matching_row_ids =
            |encoded_key: &[u8], entry_row_ids: &[i64]| -> Result<()> {
                let mut matched = false;
                for prefix in prefixes {
                    if Row::encoded_prefix_matches(encoded_key, prefix)? {
                        matched = true;
                        break;
                    }
                }
                if matched {
                    row_ids.extend(entry_row_ids.iter().copied());
                }
                Ok(())
            };

        match self {
            Self::UniqueEncoded(keys, deleted) if deleted.is_empty() => {
                for (encoded_key, row_id) in keys.iter() {
                    collect_matching_row_ids(encoded_key, std::slice::from_ref(row_id))?;
                }
            }
            Self::UniqueEncoded(keys, deleted) => {
                for (encoded_key, row_id) in keys.iter() {
                    if deleted.contains(row_id) {
                        continue;
                    }
                    collect_matching_row_ids(encoded_key, std::slice::from_ref(row_id))?;
                }
            }
            Self::NonUniqueEncoded(keys, deleted) if deleted.is_empty() => {
                for (encoded_key, entry_row_ids) in keys.iter() {
                    collect_matching_row_ids(encoded_key, entry_row_ids)?;
                }
            }
            Self::NonUniqueEncoded(keys, deleted) => {
                for (encoded_key, entry_row_ids) in keys.iter() {
                    let visible = entry_row_ids
                        .iter()
                        .copied()
                        .filter(|row_id| !deleted.contains(row_id))
                        .collect::<Vec<_>>();
                    collect_matching_row_ids(encoded_key, &visible)?;
                }
            }
            Self::UniqueInt64(_, _)
            | Self::NonUniqueInt64(_, _)
            | Self::UniqueUuid(_, _)
            | Self::NonUniqueUuid(_, _) => {}
        }

        Ok(row_ids)
    }

    pub(crate) fn row_ids_for_value_set(&self, value: &Value) -> Result<RuntimeRowIdSet<'_>> {
        match self {
            Self::UniqueEncoded(_, _) | Self::NonUniqueEncoded(_, _) => {
                let key = RuntimeBtreeKey::Encoded(encode_runtime_index_key(value)?);
                Ok(self.row_id_set_for_key(&key))
            }
            Self::UniqueInt64(_, _) | Self::NonUniqueInt64(_, _) => match value {
                Value::Int64(value) => Ok(self.row_id_set_for_key(&RuntimeBtreeKey::Int64(*value))),
                _ => Ok(RuntimeRowIdSet::Empty),
            },
            Self::UniqueUuid(_, _) | Self::NonUniqueUuid(_, _) => match value {
                Value::Uuid(value) => Ok(self.row_id_set_for_key(&RuntimeBtreeKey::Uuid(*value))),
                _ => Ok(RuntimeRowIdSet::Empty),
            },
        }
    }

    pub(super) fn row_ids_for_value(&self, value: &Value) -> Result<Vec<i64>> {
        let row_ids = self.row_ids_for_value_set(value)?;
        let mut values = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| values.push(row_id));
        Ok(values)
    }

    pub(super) fn row_ids_for_values(&self, values: &[&Value]) -> Result<Vec<i64>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let mut row_ids = Vec::new();
        match self {
            Self::UniqueEncoded(keys, deleted) => {
                for value in values {
                    let key = encode_runtime_index_key(value)?;
                    if let Some(row_id) = keys.get(&key) {
                        if !deleted.contains(row_id) {
                            row_ids.push(*row_id);
                        }
                    }
                }
            }
            Self::NonUniqueEncoded(keys, deleted) => {
                for value in values {
                    let key = encode_runtime_index_key(value)?;
                    if let Some(entry_row_ids) = keys.get(&key) {
                        row_ids.extend(
                            entry_row_ids
                                .iter()
                                .copied()
                                .filter(|row_id| !deleted.contains(row_id)),
                        );
                    }
                }
            }
            Self::UniqueInt64(keys, deleted) => {
                for value in values {
                    if let Value::Int64(value) = value {
                        if let Some(row_id) = keys.get(value) {
                            if !deleted.contains(&row_id) {
                                row_ids.push(row_id);
                            }
                        }
                    }
                }
            }
            Self::NonUniqueInt64(keys, deleted) => {
                for value in values {
                    if let Value::Int64(value) = value {
                        if let Some(entry_row_ids) = keys.get(value) {
                            row_ids.extend(
                                entry_row_ids
                                    .iter()
                                    .filter(|row_id| !deleted.contains(row_id)),
                            );
                        }
                    }
                }
            }
            Self::UniqueUuid(keys, deleted) => {
                for value in values {
                    if let Value::Uuid(value) = value {
                        if let Some(row_id) = keys.get(value) {
                            if !deleted.contains(row_id) {
                                row_ids.push(*row_id);
                            }
                        }
                    }
                }
            }
            Self::NonUniqueUuid(keys, deleted) => {
                for value in values {
                    if let Value::Uuid(value) = value {
                        if let Some(entry_row_ids) = keys.get(value) {
                            row_ids.extend(
                                entry_row_ids
                                    .iter()
                                    .copied()
                                    .filter(|row_id| !deleted.contains(row_id)),
                            );
                        }
                    }
                }
            }
        }
        Ok(row_ids)
    }

    pub(crate) fn distinct_key_counts(&self) -> Vec<(RuntimeBtreeKey, usize)> {
        match self {
            Self::UniqueEncoded(keys, deleted) => keys
                .iter()
                .filter(|(_, row_id)| !deleted.contains(row_id))
                .map(|(key, _)| (RuntimeBtreeKey::Encoded(key.clone()), 1))
                .collect(),
            Self::NonUniqueEncoded(keys, deleted) => keys
                .iter()
                .map(|(key, row_ids)| {
                    (
                        RuntimeBtreeKey::Encoded(key.clone()),
                        row_ids
                            .iter()
                            .filter(|row_id| !deleted.contains(row_id))
                            .count(),
                    )
                })
                .filter(|(_, count)| *count > 0)
                .collect(),
            Self::UniqueInt64(keys, deleted) => keys
                .iter()
                .filter(|(_, row_id)| !deleted.contains(row_id))
                .map(|(key, _)| (RuntimeBtreeKey::Int64(key), 1))
                .collect(),
            Self::NonUniqueInt64(keys, deleted) => keys
                .iter()
                .map(|(key, row_ids)| {
                    (
                        RuntimeBtreeKey::Int64(key),
                        row_ids
                            .iter()
                            .filter(|row_id| !deleted.contains(row_id))
                            .count(),
                    )
                })
                .filter(|(_, count)| *count > 0)
                .collect(),
            Self::UniqueUuid(keys, deleted) => keys
                .iter()
                .filter(|(_, row_id)| !deleted.contains(row_id))
                .map(|(key, _)| (RuntimeBtreeKey::Uuid(*key), 1))
                .collect(),
            Self::NonUniqueUuid(keys, deleted) => keys
                .iter()
                .map(|(key, row_ids)| {
                    (
                        RuntimeBtreeKey::Uuid(*key),
                        row_ids
                            .iter()
                            .filter(|row_id| !deleted.contains(row_id))
                            .count(),
                    )
                })
                .filter(|(_, count)| *count > 0)
                .collect(),
        }
    }

    #[cfg(test)]
    pub(super) fn contains_any(&self, key: &RuntimeBtreeKey) -> bool {
        match (self, key) {
            (Self::UniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .is_some_and(|row_id| !deleted.contains(row_id)),
            (Self::NonUniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .is_some_and(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(row_id))),
            (Self::UniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .is_some_and(|row_id| !deleted.contains(&row_id)),
            (Self::NonUniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .is_some_and(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(&row_id))),
            (Self::UniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => keys
                .get(key)
                .is_some_and(|row_id| !deleted.contains(row_id)),
            (Self::NonUniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => keys
                .get(key)
                .is_some_and(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(row_id))),
            _ => false,
        }
    }

    pub(super) fn insert_row_id(&mut self, key: RuntimeBtreeKey, row_id: i64) -> Result<()> {
        match (self, key) {
            (Self::UniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => {
                if deleted.remove(&row_id) {
                    Arc::make_mut(keys).retain(|_, existing| *existing != row_id);
                }
                if let Some(existing) = keys.get(&key).copied() {
                    if deleted.remove(&existing) {
                        Arc::make_mut(keys).insert(key, row_id);
                        return Ok(());
                    }
                    return Err(DbError::internal(
                        "unique runtime BTREE index received a duplicate key insert",
                    ));
                }
                Arc::make_mut(keys).insert(key, row_id);
            }
            (Self::NonUniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => {
                let keys = Arc::make_mut(keys);
                if !deleted.is_empty() && deleted.remove(&row_id) {
                    keys.remove_row_id_everywhere(row_id);
                }
                keys.insert_row_id(key, row_id);
            }
            (Self::UniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => {
                let revived = deleted.remove(&row_id);
                let keys = Arc::make_mut(keys);
                if revived {
                    if keys.get(&key) == Some(row_id) {
                        return Ok(());
                    }
                    keys.remove_row_id_mapping(row_id);
                }
                if let Some(existing) = keys.get(&key) {
                    if deleted.remove(&existing) {
                        keys.insert(key, row_id);
                        return Ok(());
                    }
                    return Err(DbError::internal(
                        "unique runtime BTREE index received a duplicate key insert",
                    ));
                }
                keys.insert(key, row_id);
            }
            (Self::NonUniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => {
                let keys = Arc::make_mut(keys);
                if deleted.remove(&row_id) {
                    keys.remove_row_id_mapping(row_id);
                }
                keys.insert_row_id(key, row_id);
            }
            (Self::UniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => {
                if deleted.remove(&row_id) {
                    Arc::make_mut(keys).retain(|_, existing| *existing != row_id);
                }
                if let Some(existing) = keys.get(&key).copied() {
                    if deleted.remove(&existing) {
                        Arc::make_mut(keys).insert(key, row_id);
                        return Ok(());
                    }
                    return Err(DbError::internal(
                        "unique runtime BTREE index received a duplicate key insert",
                    ));
                }
                Arc::make_mut(keys).insert(key, row_id);
            }
            (Self::NonUniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => {
                let keys = Arc::make_mut(keys);
                if deleted.remove(&row_id) {
                    for row_ids in keys.values_mut() {
                        row_ids.retain(|existing| *existing != row_id);
                    }
                    keys.retain(|_, row_ids| !row_ids.is_empty());
                }
                Self::push_non_unique_row_id(keys.entry(key).or_default(), row_id);
            }
            _ => {
                return Err(DbError::internal(
                    "runtime BTREE key type did not match the runtime index representation",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn move_row_id(
        &mut self,
        old_key: &RuntimeBtreeKey,
        new_key: RuntimeBtreeKey,
        row_id: i64,
    ) -> Result<bool> {
        match (self, old_key, new_key) {
            (
                Self::NonUniqueEncoded(keys, deleted),
                RuntimeBtreeKey::Encoded(old_key),
                RuntimeBtreeKey::Encoded(new_key),
            ) if !deleted.contains(&row_id) => {
                let keys = Arc::make_mut(keys);
                keys.remove_row_id_for_key(old_key.as_slice(), row_id);
                keys.insert_row_id(new_key, row_id);
                Ok(true)
            }
            (
                Self::NonUniqueInt64(keys, deleted),
                RuntimeBtreeKey::Int64(old_key),
                RuntimeBtreeKey::Int64(new_key),
            ) if !deleted.contains(&row_id) => {
                let keys = Arc::make_mut(keys);
                if let Some(row_ids) = keys.get_mut(old_key) {
                    row_ids.retain(|existing| *existing != row_id);
                }
                keys.remove_empty_key(*old_key);
                keys.insert_row_id(new_key, row_id);
                Ok(true)
            }
            (
                Self::NonUniqueUuid(keys, deleted),
                RuntimeBtreeKey::Uuid(old_key),
                RuntimeBtreeKey::Uuid(new_key),
            ) if !deleted.contains(&row_id) => {
                let keys = Arc::make_mut(keys);
                if let Some(row_ids) = keys.get_mut(old_key) {
                    row_ids.retain(|existing| *existing != row_id);
                }
                if keys.get(old_key).is_some_and(Vec::is_empty) {
                    keys.remove(old_key);
                }
                Self::push_non_unique_row_id(keys.entry(new_key).or_default(), row_id);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(super) fn remove_row_id(&mut self, key: &RuntimeBtreeKey, row_id: i64) -> Result<()> {
        match (self, key) {
            (Self::UniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => {
                if let Some(existing) = keys.get(key).copied() {
                    if existing != row_id {
                        return Err(DbError::internal(
                            "unique runtime BTREE index row-id mismatch during delete",
                        ));
                    }
                    deleted.insert(row_id);
                }
            }
            (Self::NonUniqueEncoded(keys, deleted), RuntimeBtreeKey::Encoded(key)) => {
                if keys
                    .get(key)
                    .is_some_and(|row_ids| row_ids.contains(&row_id))
                {
                    deleted.insert(row_id);
                }
            }
            (Self::UniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => {
                if let Some(existing) = keys.get(key) {
                    if existing != row_id {
                        return Err(DbError::internal(
                            "unique runtime BTREE index row-id mismatch during delete",
                        ));
                    }
                    deleted.insert(row_id);
                }
            }
            (Self::NonUniqueInt64(keys, deleted), RuntimeBtreeKey::Int64(key)) => {
                if keys
                    .get(key)
                    .is_some_and(|row_ids| row_ids.contains(&row_id))
                {
                    deleted.insert(row_id);
                }
            }
            (Self::UniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => {
                if let Some(existing) = keys.get(key).copied() {
                    if existing != row_id {
                        return Err(DbError::internal(
                            "unique runtime BTREE index row-id mismatch during delete",
                        ));
                    }
                    deleted.insert(row_id);
                }
            }
            (Self::NonUniqueUuid(keys, deleted), RuntimeBtreeKey::Uuid(key)) => {
                if keys
                    .get(key)
                    .is_some_and(|row_ids| row_ids.contains(&row_id))
                {
                    deleted.insert(row_id);
                }
            }
            _ => {
                return Err(DbError::internal(
                    "runtime BTREE key type did not match the runtime index representation",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn mark_row_ids_deleted<I>(&mut self, row_ids: I)
    where
        I: IntoIterator<Item = i64>,
    {
        match self {
            Self::UniqueEncoded(_, deleted)
            | Self::NonUniqueEncoded(_, deleted)
            | Self::UniqueInt64(_, deleted)
            | Self::NonUniqueInt64(_, deleted)
            | Self::UniqueUuid(_, deleted)
            | Self::NonUniqueUuid(_, deleted) => {
                deleted.extend(row_ids);
            }
        }
    }

    pub(crate) fn total_row_id_count(&self) -> usize {
        match self {
            Self::UniqueEncoded(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueEncoded(keys, deleted) => keys
                .values()
                .map(|row_ids| {
                    row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count()
                })
                .sum(),
            Self::UniqueInt64(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueInt64(keys, deleted) if deleted.is_empty() => {
                keys.values().map(RuntimeInt64RowIds::len).sum()
            }
            Self::NonUniqueInt64(keys, deleted) => keys
                .values()
                .map(|row_ids| {
                    row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count()
                })
                .sum(),
            Self::UniqueUuid(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueUuid(keys, deleted) => keys
                .values()
                .map(|row_ids| {
                    row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count()
                })
                .sum(),
        }
    }

    pub(crate) fn distinct_key_count(&self) -> usize {
        match self {
            Self::UniqueEncoded(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueEncoded(keys, deleted) => keys
                .values()
                .filter(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(row_id)))
                .count(),
            Self::UniqueInt64(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueInt64(keys, deleted) => keys
                .values()
                .filter(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(&row_id)))
                .count(),
            Self::UniqueUuid(keys, deleted) => keys.len().saturating_sub(deleted.len()),
            Self::NonUniqueUuid(keys, deleted) => keys
                .values()
                .filter(|row_ids| row_ids.iter().any(|row_id| !deleted.contains(row_id)))
                .count(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::UniqueEncoded(keys, deleted) => keys.len() == deleted.len(),
            Self::NonUniqueEncoded(keys, deleted) => keys
                .values()
                .all(|row_ids| row_ids.iter().all(|row_id| deleted.contains(row_id))),
            Self::UniqueInt64(keys, deleted) => keys.len() == deleted.len(),
            Self::NonUniqueInt64(keys, deleted) => keys
                .values()
                .all(|row_ids| row_ids.iter().all(|row_id| deleted.contains(&row_id))),
            Self::UniqueUuid(keys, deleted) => keys.len() == deleted.len(),
            Self::NonUniqueUuid(keys, deleted) => keys
                .values()
                .all(|row_ids| row_ids.iter().all(|row_id| deleted.contains(row_id))),
        }
    }
}
