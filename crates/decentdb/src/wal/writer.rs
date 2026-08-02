//! WAL append and durability logic.
//!
//! Implements:
//! - design/adr/0003-snapshot-lsn-atomicity.md
//! - design/adr/0204-bounded-wal-commit-preparation.md
//! - design/adr/0206-on-disk-full-wal-publication.md
//! - design/adr/0210-bounded-pipelined-checkpoint-copyback.md

use std::cell::RefCell;
use std::collections::{HashSet, TryReserveError};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::WalSyncMode;
use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::write_all_at_many;

use super::delta::{encode_page_delta_into, DELTA_FRAME_PAYLOAD_SIZE};
use super::format::{
    FrameEncoding, FrameType, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, WAL_HEADER_SIZE,
};
use super::index::WalVersion;
use super::recovery;
use super::{
    PreparedWalPage, PreparedWalPayload, WalBasePage, WalBaseSource, WalHandle, WalWriteState,
};

const WAL_PREALLOC_CHUNK_BYTES: u64 = 16 << 20;
/// Maximum encoded page-frame bytes prepared at once. Base-page lookup uses
/// the same page count, so the two dominant transient buffers stay near 8 MiB
/// combined with the default 4 KiB page size instead of scaling with the
/// transaction.
const WAL_PREPARE_BATCH_BYTES: usize = 4 << 20;
/// Reuse encoded-frame scratch for ordinary commits, but release larger
/// batches once their bytes have been written. Large buffers commonly cross
/// the system allocator's mmap threshold and otherwise remain resident for
/// the lifetime of the database handle despite having length zero.
const WAL_PREPARE_RETAINED_FRAME_BYTES: usize = 256 << 10;
const COMMIT_FRAME_BYTES: [u8; FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE] =
    [FrameType::Commit as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

#[derive(Clone, Copy, Debug)]
struct PreparedWalCommit {
    commit_start_lsn: u64,
    end_lsn: u64,
    retain_history_hint: bool,
}

thread_local! {
    static DEFERRED_GROUP_COMMIT: RefCell<Option<DeferredGroupCommitState>> =
        const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Default)]
struct DeferredGroupCommitState {
    pending: bool,
}

/// Thread-local guard used by the queued write executor to make several
/// synchronous commits share one physical WAL sync without acknowledging any
/// successful queued request before the covering sync completes.
#[derive(Debug)]
pub(crate) struct DeferredGroupCommitGuard {
    active: bool,
}

impl Drop for DeferredGroupCommitGuard {
    fn drop(&mut self) {
        if self.active {
            DEFERRED_GROUP_COMMIT.with(|slot| {
                *slot.borrow_mut() = None;
            });
        }
    }
}

pub(crate) fn begin_deferred_group_commit() -> DeferredGroupCommitGuard {
    DEFERRED_GROUP_COMMIT.with(|slot| {
        *slot.borrow_mut() = Some(DeferredGroupCommitState::default());
    });
    DeferredGroupCommitGuard { active: true }
}

pub(crate) fn flush_deferred_group_commit(wal: &WalHandle) -> Result<bool> {
    let state = DEFERRED_GROUP_COMMIT.with(|slot| *slot.borrow());
    let Some(state) = state else {
        return Ok(false);
    };
    if !state.pending {
        return Ok(false);
    }

    match wal.inner.sync_mode {
        WalSyncMode::Full => {
            wal.inner.file.sync_data()?;
            clear_checkpoint_tail_sync_needed(wal);
        }
        WalSyncMode::Normal => {
            wal.inner.file.sync_data()?;
            clear_checkpoint_tail_sync_needed(wal);
        }
        WalSyncMode::AsyncCommit { .. } | WalSyncMode::TestingOnlyUnsafeNoSync => {}
    }

    DEFERRED_GROUP_COMMIT.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            state.pending = false;
        }
    });
    Ok(true)
}

fn defer_sync_if_active() -> bool {
    DEFERRED_GROUP_COMMIT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(state) = slot.as_mut() else {
            return false;
        };
        state.pending = true;
        true
    })
}

pub(crate) fn commit_pages(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
) -> Result<u64> {
    let _process_guard = wal.lock_process_writer()?;
    wal.refresh_from_coordination(pager)?;
    let mut writer_state = wal
        .inner
        .write_lock
        .lock()
        .map_err(|_| DbError::internal("wal write lock poisoned"))?;

    let mut offset = wal.latest_snapshot();
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let latest_snapshot = wal.latest_snapshot();
    let prepared_commit = prepare_and_write_commit(
        wal,
        pager,
        pages,
        offset,
        latest_snapshot,
        &mut writer_state,
    )?;
    offset = prepared_commit.end_lsn;
    if let Err(error) = sync_for_mode(wal, prepared_commit.end_lsn) {
        discard_prepared_commit_state(&mut writer_state);
        return Err(error);
    }

    let should_demote_cold_versions;
    {
        let mut index = wal
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history = prepared_commit.retain_history_hint
            || wal.inner.reader_registry.active_reader_count()? > 0
            || wal.retained_snapshot_lsn().is_some();
        let mut version_lsn = prepared_commit.commit_start_lsn;
        let prepared_count = writer_state.prepared_pages.len();
        for prepared in writer_state.prepared_pages.drain(..) {
            version_lsn += prepared.encoded_len as u64;
            let page_id = prepared.page_id;
            let (version, is_resident) = prepared_page_version(prepared, version_lsn);
            index.add_version(page_id, version, retain_history);
            if !retain_history && (!is_resident || wal.inner.resident_versions_per_page > 0) {
                index.unmark_dirty_since_demote(page_id);
            }
        }
        // Update wal_end_lsn inside the index lock so that begin_reader()
        // (which also holds the index lock) always sees a wal_end_lsn
        // consistent with the index contents.
        wal.inner
            .max_page_count
            .fetch_max(max_page_count, Ordering::AcqRel);
        wal.inner.wal_end_lsn.store(offset, Ordering::Release);
        // Track work since the last checkpoint for the size-based trigger
        // (ADR 0137). Saturating add: a u32 is enough headroom for a single
        // checkpoint window even on the largest practical workloads, and
        // saturation is safe because the trigger compares >= threshold.
        let prepared_count_u32 = u32::try_from(prepared_count).unwrap_or(u32::MAX);
        wal.inner
            .pages_since_checkpoint
            .fetch_add(prepared_count_u32, Ordering::AcqRel);
        should_demote_cold_versions =
            wal.inner.wal_index_hot_set_pages != 0 || index.has_dirty_since_demote();
    }
    wal.publish_process_commit(offset)?;
    drop(writer_state);
    if should_demote_cold_versions {
        // The commit is already durable, locally visible, and process-
        // published. Sidecar spilling is rebuildable cache maintenance; its
        // failure restores the popped hot-index version and must not turn a
        // committed transaction into an ambiguous API error.
        let _ = demote_cold_versions(wal);
    }
    // Auto-checkpoint is post-commit maintenance. The transaction is already
    // durable and published, so a checkpoint/cache error cannot be reported as
    // if the commit itself failed; the next write or explicit checkpoint can
    // retry it.
    let _ = maybe_auto_checkpoint(wal, pager);
    Ok(offset)
}

pub(crate) fn commit_pages_if_latest(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
    expected_latest_lsn: u64,
    expected_checkpoint_epoch: u64,
) -> Result<u64> {
    let _process_guard = wal.lock_process_writer()?;
    wal.refresh_from_coordination(pager)?;
    let mut writer_state = wal
        .inner
        .write_lock
        .lock()
        .map_err(|_| DbError::internal("wal write lock poisoned"))?;

    let latest = wal.latest_snapshot();
    if latest != expected_latest_lsn {
        // Distinguish a benign checkpoint (which is the only operation that
        // can move `wal_end_lsn` while we hold no foreign-writer lock — see
        // ADR 0058 background checkpoint worker) from a real concurrent
        // writer commit (multi-connection OCC, see ADR 0023). A checkpoint
        // bumps `checkpoint_epoch`; a foreign writer does not. If the epoch
        // advanced and no foreign commit occurred, the checkpoint preserved
        // every durable page, so our staged pages are safe to append at the
        // current WAL end.
        let current_epoch = wal.inner.checkpoint_epoch.load(Ordering::Acquire);
        if current_epoch == expected_checkpoint_epoch {
            return Err(DbError::transaction(format!(
                "transaction conflict: WAL advanced from {expected_latest_lsn} to {latest}"
            )));
        }
    }

    let mut offset = latest;
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let prepared_commit =
        prepare_and_write_commit(wal, pager, pages, offset, latest, &mut writer_state)?;
    offset = prepared_commit.end_lsn;
    if let Err(error) = sync_for_mode(wal, prepared_commit.end_lsn) {
        discard_prepared_commit_state(&mut writer_state);
        return Err(error);
    }

    let should_demote_cold_versions;
    {
        let mut index = wal
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history = prepared_commit.retain_history_hint
            || wal.inner.reader_registry.active_reader_count()? > 0
            || wal.retained_snapshot_lsn().is_some();
        let mut version_lsn = prepared_commit.commit_start_lsn;
        let prepared_count = writer_state.prepared_pages.len();
        for prepared in writer_state.prepared_pages.drain(..) {
            version_lsn += prepared.encoded_len as u64;
            let page_id = prepared.page_id;
            let (version, is_resident) = prepared_page_version(prepared, version_lsn);
            index.add_version(page_id, version, retain_history);
            if !retain_history && (!is_resident || wal.inner.resident_versions_per_page > 0) {
                index.unmark_dirty_since_demote(page_id);
            }
        }
        // Update wal_end_lsn inside the index lock — same rationale as
        // commit_pages above.
        wal.inner
            .max_page_count
            .fetch_max(max_page_count, Ordering::AcqRel);
        wal.inner.wal_end_lsn.store(offset, Ordering::Release);
        let prepared_count_u32 = u32::try_from(prepared_count).unwrap_or(u32::MAX);
        wal.inner
            .pages_since_checkpoint
            .fetch_add(prepared_count_u32, Ordering::AcqRel);
        should_demote_cold_versions =
            wal.inner.wal_index_hot_set_pages != 0 || index.has_dirty_since_demote();
    }
    wal.publish_process_commit(offset)?;
    drop(writer_state);
    if should_demote_cold_versions {
        let _ = demote_cold_versions(wal);
    }
    let _ = maybe_auto_checkpoint(wal, pager);
    Ok(offset)
}

fn prepared_page_version(prepared: PreparedWalPage, version_lsn: u64) -> (WalVersion, bool) {
    let PreparedWalPage {
        encoded_len,
        frame_offset,
        payload,
        ..
    } = prepared;
    match payload {
        PreparedWalPayload::OnDiskFullPage => (
            WalVersion::on_disk(
                version_lsn,
                frame_offset,
                encoded_len as u32,
                FrameEncoding::Page,
            ),
            false,
        ),
        PreparedWalPayload::Resident { data, encoding } => (
            WalVersion::resident(
                version_lsn,
                frame_offset,
                encoded_len as u32,
                encoding,
                Arc::from(data),
            ),
            true,
        ),
    }
}

/// Encode and write one transaction's page frames with bounded transient
/// storage, then append the existing commit marker and publish the logical end
/// in the WAL header. The caller remains responsible for the durability sync
/// and in-memory index publication.
///
/// Page-frame batches written before the final marker are deliberately
/// unpublished: the header continues to expose the preceding logical end. A
/// failed preparation therefore leaves only an ignored physical tail, which a
/// later commit overwrites from the unchanged logical end.
fn prepare_and_write_commit(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    commit_start_lsn: u64,
    snapshot_lsn: u64,
    writer_state: &mut WalWriteState,
) -> Result<PreparedWalCommit> {
    let page_count = pages.len();
    let page_frame_len = FRAME_HEADER_SIZE + wal.inner.page_size as usize + FRAME_TRAILER_SIZE;
    let pages_per_batch = wal_prepare_batch_page_limit(wal.inner.page_size);
    let batch_page_capacity = page_count.clamp(1, pages_per_batch);
    let encoded_batch_capacity = batch_page_capacity
        .saturating_mul(page_frame_len)
        .saturating_add(COMMIT_FRAME_BYTES.len());

    writer_state.page_batch.clear();
    writer_state.base_pages.clear();
    if writer_state.base_pages.capacity() > batch_page_capacity {
        writer_state.base_pages.shrink_to(batch_page_capacity);
    }
    writer_state.prepared_pages.clear();

    let result = (|| {
        reset_bounded_buffer(
            &mut writer_state.page_batch,
            encoded_batch_capacity,
            "encoded-frame scratch",
        )?;
        try_reserve_vec_exact(
            &mut writer_state.prepared_pages,
            page_count,
            "prepared-page metadata",
        )?;
        try_reserve_vec_exact(
            &mut writer_state.delta_scratch,
            DELTA_FRAME_PAYLOAD_SIZE,
            "delta scratch",
        )?;

        let mut input_pages = pages.into_iter();
        let page_chunk_capacity = batch_page_capacity.min(page_count);
        let mut page_chunk = Vec::new();
        try_reserve_vec_exact(&mut page_chunk, page_chunk_capacity, "page-group metadata")?;
        let mut seen_page_ids = HashSet::new();
        seen_page_ids.try_reserve(page_count).map_err(|error| {
            wal_prepare_allocation_error("duplicate-page set", page_count, error)
        })?;
        let mut offset = commit_start_lsn;
        let mut retain_history_hint = false;
        let mut logical_end_written = false;

        loop {
            page_chunk.extend(input_pages.by_ref().take(pages_per_batch));
            if page_chunk.is_empty() {
                break;
            }

            // One index-lock acquisition serves the whole bounded batch. The
            // index cannot change while this writer owns the WAL write lock,
            // and readers continue to observe the preceding logical end.
            retain_history_hint |= lookup_base_pages_batch(
                wal,
                pager,
                &page_chunk,
                snapshot_lsn,
                &mut writer_state.base_pages,
            )?;

            writer_state.page_batch.clear();
            for (index, (page_id, payload)) in page_chunk.drain(..).enumerate() {
                let frame_offset = offset + writer_state.page_batch.len() as u64;
                // Delta recovery uses the pre-commit index as its base. A
                // repeated page in this transaction must therefore remain a
                // full frame even when the repeat crosses a preparation-batch
                // boundary.
                let duplicate_in_commit = !seen_page_ids.insert(page_id);
                let (encoded_len, encoding) = if duplicate_in_commit {
                    (
                        append_page_frame(
                            &mut writer_state.page_batch,
                            page_id,
                            &payload,
                            wal.inner.page_size,
                        )?,
                        FrameEncoding::Page,
                    )
                } else {
                    let base = writer_state
                        .base_pages
                        .get(index)
                        .and_then(|base| base.as_ref())
                        .map(|(data, from_wal)| (&data[..], *from_wal));
                    append_best_page_frame_with_base(
                        &mut writer_state.page_batch,
                        &mut writer_state.delta_scratch,
                        wal,
                        page_id,
                        &payload,
                        base,
                        retain_history_hint,
                    )?
                };
                let prepared_payload = if encoding == FrameEncoding::Page && !retain_history_hint {
                    // The encoded full frame is self-contained. Once this
                    // bounded group is written, keeping the input page vector
                    // would only duplicate bytes already owned by the WAL.
                    PreparedWalPayload::OnDiskFullPage
                } else {
                    PreparedWalPayload::Resident {
                        data: payload,
                        encoding,
                    }
                };
                writer_state.prepared_pages.push(PreparedWalPage {
                    page_id,
                    encoded_len,
                    frame_offset,
                    payload: prepared_payload,
                });
            }
            writer_state.base_pages.clear();

            if input_pages.len() == 0 {
                // Fold the unchanged commit marker into the final frame
                // group. A normal small commit now needs one frame/marker
                // write plus the header write, matching the pre-streaming
                // path while preserving the same publication boundary.
                writer_state
                    .page_batch
                    .extend_from_slice(&COMMIT_FRAME_BYTES);
                let end_lsn = offset + writer_state.page_batch.len() as u64;
                ensure_capacity(wal, end_lsn)?;
                let end_offset_bytes = end_lsn.to_le_bytes();
                write_all_at_many(
                    wal.inner.file.as_ref(),
                    &[
                        (offset, writer_state.page_batch.as_slice()),
                        (16, &end_offset_bytes),
                    ],
                )?;
                offset = end_lsn;
                logical_end_written = true;
            } else {
                let batch_end_lsn = offset + writer_state.page_batch.len() as u64;
                ensure_capacity(wal, batch_end_lsn)?;
                write_all_at_many(
                    wal.inner.file.as_ref(),
                    &[(offset, writer_state.page_batch.as_slice())],
                )?;
                offset = batch_end_lsn;
            }
        }

        // Empty commits have no final page group to carry the marker.
        if !logical_end_written {
            writer_state.page_batch.clear();
            writer_state
                .page_batch
                .extend_from_slice(&COMMIT_FRAME_BYTES);
            let end_lsn = offset + writer_state.page_batch.len() as u64;
            ensure_capacity(wal, end_lsn)?;
            let end_offset_bytes = end_lsn.to_le_bytes();
            write_all_at_many(
                wal.inner.file.as_ref(),
                &[
                    (offset, writer_state.page_batch.as_slice()),
                    (16, &end_offset_bytes),
                ],
            )?;
            offset = end_lsn;
        }
        clear_encoded_frame_scratch(&mut writer_state.page_batch);

        Ok(PreparedWalCommit {
            commit_start_lsn,
            end_lsn: offset,
            retain_history_hint,
        })
    })();

    if result.is_err() {
        discard_prepared_commit_state(writer_state);
    }
    result
}

fn wal_prepare_batch_page_limit(page_size: u32) -> usize {
    let page_frame_len = FRAME_HEADER_SIZE + page_size as usize + FRAME_TRAILER_SIZE;
    (WAL_PREPARE_BATCH_BYTES / page_frame_len).max(1)
}

fn reset_bounded_buffer(
    buffer: &mut Vec<u8>,
    target_capacity: usize,
    allocation_name: &str,
) -> Result<()> {
    buffer.clear();
    if buffer.capacity() > target_capacity {
        buffer.shrink_to(target_capacity);
    }
    try_reserve_vec_exact(buffer, target_capacity, allocation_name)
}

fn try_reserve_vec_exact<T>(
    buffer: &mut Vec<T>,
    target_capacity: usize,
    allocation_name: &str,
) -> Result<()> {
    if buffer.capacity() >= target_capacity {
        return Ok(());
    }
    let additional = target_capacity.saturating_sub(buffer.len());
    buffer
        .try_reserve_exact(additional)
        .map_err(|error| wal_prepare_allocation_error(allocation_name, target_capacity, error))
}

fn wal_prepare_allocation_error(
    allocation_name: &str,
    requested_capacity: usize,
    error: TryReserveError,
) -> DbError {
    DbError::internal(format!(
        "allocate WAL commit preparation {allocation_name} (capacity {requested_capacity}): {error}"
    ))
}

fn discard_prepared_commit_state(writer_state: &mut WalWriteState) {
    clear_encoded_frame_scratch(&mut writer_state.page_batch);
    writer_state.base_pages.clear();
    writer_state.prepared_pages.clear();
}

fn clear_encoded_frame_scratch(buffer: &mut Vec<u8>) {
    buffer.clear();
    if buffer.capacity() > WAL_PREPARE_RETAINED_FRAME_BYTES {
        // Swapping with an empty Vec releases a large allocation without an
        // infallible cleanup-time allocation for a smaller replacement. Small
        // and ordinary commit buffers retain their capacity for reuse.
        *buffer = Vec::new();
    }
}

#[derive(Debug)]
pub(crate) struct TruncateToHeaderOutcome {
    pub(crate) post_logical_error: Option<DbError>,
    pub(crate) final_sync_succeeded: bool,
}

/// Establish the live logical WAL reset and shrink the physical file, but do
/// not perform the final durability sync. The checkpoint caller may hand that
/// sync to its existing copyback worker so independent index cleanup can run
/// concurrently. It must call `finish_truncate_to_header` before publication
/// or return.
pub(crate) fn truncate_to_header_before_sync(wal: &WalHandle) -> Result<TruncateToHeaderOutcome> {
    // This is the last fallible in-memory reconciliation step. Run it before
    // publishing the zero-end header/atomic so every path that exposes a
    // logical reset reaches unconditional index + sidecar invalidation in the
    // checkpoint caller. The old tail was made durable before copyback, so a
    // later header-write failure with conservative zeroed watermarks is safe;
    // the next high-offset commit advances them again.
    if let Some(async_commit) = wal.inner.async_commit.as_ref() {
        async_commit.rebase_clean_lsn(0)?;
    }
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, 0)?;
    wal.inner.wal_end_lsn.store(0, Ordering::Release);
    wal.inner
        .checkpoint_tail_sync_needed
        .store(false, Ordering::Release);
    wal.inner
        .checkpoint_tail_locally_synced
        .store(true, Ordering::Release);

    let mut post_logical_error = None;
    match wal.inner.file.set_len(WAL_HEADER_SIZE) {
        Ok(()) => {
            wal.inner
                .allocated_len
                .store(WAL_HEADER_SIZE, Ordering::Release);
        }
        Err(error) => {
            post_logical_error = Some(error);
        }
    }

    Ok(TruncateToHeaderOutcome {
        post_logical_error,
        final_sync_succeeded: false,
    })
}

/// Finish the durability barrier for a logically reset WAL.
pub(crate) fn sync_truncated_wal(wal: &WalHandle) -> Result<()> {
    // The checkpoint has replaced the WAL header and shortened the file. The
    // VFS `sync_data` contract durably persists both file contents and the
    // file length, which are the only recovery-relevant parts of this
    // mutation. Avoid forcing unrelated inode/timestamp metadata here: on
    // Linux this selects fdatasync rather than fsync, while platforms whose
    // durable primitive cannot make that distinction keep the same full
    // flush. Commit-time WAL barriers use this same content-and-length
    // durability contract.
    wal.inner.file.sync_data()
}

pub(crate) fn finish_truncate_to_header(
    mut outcome: TruncateToHeaderOutcome,
    sync_result: Result<()>,
) -> TruncateToHeaderOutcome {
    outcome.final_sync_succeeded = match sync_result {
        Ok(()) => true,
        Err(error) => {
            if outcome.post_logical_error.is_none() {
                outcome.post_logical_error = Some(error);
            }
            false
        }
    };
    outcome
}

pub(crate) fn sync_checkpoint_tail(wal: &WalHandle) -> Result<()> {
    if wal.latest_snapshot() == 0 {
        return Ok(());
    }
    if let Some(async_commit) = wal.inner.async_commit.as_ref() {
        return async_commit.sync_for_checkpoint_tail();
    }
    sync_durably(wal)
}

/// Sync helper used by per-commit paths. Under `AsyncCommit` this is a no-op
/// and the background flusher will catch up; the writer records the new end
/// LSN with the flusher state so any concurrent `flush_to_durable` knows the
/// target. Under all other modes this performs the appropriate synchronous
/// fsync.
fn sync_for_mode(wal: &WalHandle, new_end_lsn: u64) -> Result<()> {
    match wal.inner.sync_mode {
        WalSyncMode::Full => {
            if defer_sync_if_active() {
                return Ok(());
            }
            // `sync_data` persists WAL bytes and any file-length growth under
            // the VFS durability contract. Recovery does not depend on
            // timestamps, ownership, or other inode metadata.
            let result = wal.inner.file.sync_data();
            if result.is_ok() {
                clear_checkpoint_tail_sync_needed(wal);
            }
            result
        }
        WalSyncMode::Normal => {
            if defer_sync_if_active() {
                return Ok(());
            }
            let result = wal.inner.file.sync_data();
            if result.is_ok() {
                clear_checkpoint_tail_sync_needed(wal);
            }
            result
        }
        WalSyncMode::AsyncCommit { .. } => {
            // SAFETY (durability): we publish the new dirty watermark *before*
            // returning so a subsequent `Db::sync()` cannot complete without
            // observing this commit.
            if let Some(state) = wal.inner.async_commit.as_ref() {
                state.note_write(new_end_lsn);
            }
            Ok(())
        }
        WalSyncMode::TestingOnlyUnsafeNoSync => Ok(()),
    }
}

fn clear_checkpoint_tail_sync_needed(wal: &WalHandle) {
    wal.inner
        .checkpoint_tail_sync_needed
        .store(false, Ordering::Release);
    wal.inner
        .checkpoint_tail_locally_synced
        .store(true, Ordering::Release);
}

/// Force-sync helper for paths that must be durable regardless of sync mode
/// (WAL truncation). Bypasses the AsyncCommit deferral.
fn sync_durably(wal: &WalHandle) -> Result<()> {
    wal.inner.file.sync_data()
}

fn ensure_capacity(wal: &WalHandle, required_len: u64) -> Result<bool> {
    let current_len = wal.inner.allocated_len.load(Ordering::Acquire);
    if current_len >= required_len {
        return Ok(false);
    }
    let target_len = required_len
        .div_ceil(WAL_PREALLOC_CHUNK_BYTES)
        .saturating_mul(WAL_PREALLOC_CHUNK_BYTES);
    wal.inner.file.set_len(target_len)?;
    wal.inner.allocated_len.store(target_len, Ordering::Release);
    Ok(true)
}

fn append_page_frame(
    output: &mut Vec<u8>,
    page_id: PageId,
    payload: &[u8],
    page_size: u32,
) -> Result<usize> {
    if page_id == 0 {
        return Err(DbError::corruption(
            "page WAL frames must have a non-zero page id",
        ));
    }
    if payload.len() != page_size as usize {
        return Err(DbError::internal(format!(
            "WAL frame payload length {} does not match expected payload length {}",
            payload.len(),
            page_size
        )));
    }
    let frame_len = FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE;
    output.push(FrameType::Page as u8);
    output.extend_from_slice(&page_id.to_le_bytes());
    output.extend_from_slice(payload);
    output.extend_from_slice(&[0_u8; FRAME_TRAILER_SIZE]);
    Ok(frame_len)
}

fn append_best_page_frame_with_base(
    output: &mut Vec<u8>,
    delta_scratch: &mut Vec<u8>,
    wal: &WalHandle,
    page_id: PageId,
    payload: &[u8],
    base_page: Option<(&[u8], WalBaseSource)>,
    _retain_history_hint: bool,
) -> Result<(usize, FrameEncoding)> {
    // Page 1 carries the fixed database header and is also updated directly
    // when the schema cookie changes. Replaying old header deltas against a
    // newer on-disk header is unsafe, so keep header WAL frames self-contained.
    if page_id == crate::storage::page::HEADER_PAGE_ID {
        return append_page_frame(output, page_id, payload, wal.inner.page_size)
            .map(|len| (len, FrameEncoding::Page));
    }
    if let Some((base, source)) = base_page {
        if (source == WalBaseSource::MainDatabase
            || (source == WalBaseSource::ResidentWal && can_encode_against_resident_wal_base(wal)))
            && encode_page_delta_into(delta_scratch, base, payload)
        {
            return append_page_delta_frame(output, page_id, delta_scratch)
                .map(|len| (len, FrameEncoding::PageDelta));
        }
    }
    append_page_frame(output, page_id, payload, wal.inner.page_size)
        .map(|len| (len, FrameEncoding::Page))
}

/// Look up base pages for one bounded preparation batch under one index lock.
fn lookup_base_pages_batch(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: &[(PageId, Vec<u8>)],
    snapshot_lsn: u64,
    output: &mut Vec<WalBasePage>,
) -> Result<bool> {
    let mut index = wal
        .inner
        .index
        .lock()
        .map_err(|_| DbError::internal("wal index lock poisoned"))?;
    let retain_history_hint = wal.inner.reader_registry.active_reader_count()? > 0
        || wal.retained_snapshot_lsn().is_some();
    output.clear();
    try_reserve_vec_exact(output, pages.len(), "base-page metadata")?;
    for (page_id, _) in pages {
        // A bounded hot-set may have moved the latest committed version out
        // of the in-memory index. Consult it before falling back to the main
        // database: a delta encoded against that stale fallback would later
        // be replayed on top of the newer WAL version and corrupt the page.
        if index.latest_visible(*page_id, snapshot_lsn).is_none() {
            wal.promote_spilled_latest_locked(&mut index, *page_id, snapshot_lsn)?;
        } else if let Some(sidecar) = &wal.inner.index_sidecar {
            // Remove any duplicate cache record before the WAL commit header
            // can be published. Sidecar I/O errors therefore leave the old
            // logical WAL end untouched instead of creating an ambiguous
            // committed-but-unpublished local state.
            sidecar
                .lock()
                .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?
                .clear_latest(*page_id)?;
        }
        if let Some(version) = index.latest_visible(*page_id, snapshot_lsn) {
            let source = match version.payload {
                super::index::WalVersionPayload::Resident { .. } => WalBaseSource::ResidentWal,
                super::index::WalVersionPayload::OnDisk { .. } => WalBaseSource::OnDiskWal,
            };
            let page = wal.materialize_version_locked(&index, pager, *page_id, version)?;
            output.push(Some((page, source)));
        } else if let Ok(page) = pager.read_page(*page_id) {
            output.push(Some((page, WalBaseSource::MainDatabase)));
        } else {
            output.push(None);
        }
    }
    Ok(retain_history_hint)
}

fn can_encode_against_resident_wal_base(wal: &WalHandle) -> bool {
    wal.inner.resident_versions_per_page > 0 && wal.inner.wal_index_hot_set_pages == 0
}

fn demote_cold_versions(wal: &WalHandle) -> Result<()> {
    let retain_recent = wal.inner.resident_versions_per_page;
    let min_reader_snapshot = match (
        wal.inner.reader_registry.min_snapshot_lsn()?,
        wal.retained_snapshot_lsn(),
    ) {
        (Some(reader_lsn), Some(retained_lsn)) => Some(reader_lsn.min(retained_lsn)),
        (Some(reader_lsn), None) => Some(reader_lsn),
        (None, Some(retained_lsn)) => Some(retained_lsn),
        (None, None) => None,
    };
    if min_reader_snapshot.is_some() {
        // Delta materialization may need a stable base page chain. While any
        // snapshot is active, keep newly-written versions resident so readers
        // never have to reconstruct an on-disk delta against a main-db page
        // that checkpoint copyback can be updating concurrently.
        return Ok(());
    }
    let mut index = wal
        .inner
        .index
        .lock()
        .map_err(|_| DbError::internal("wal index lock poisoned"))?;
    index.demote_cold(min_reader_snapshot, retain_recent);
    if min_reader_snapshot.is_none() {
        if let Some(sidecar) = &wal.inner.index_sidecar {
            let mut sidecar = sidecar
                .lock()
                .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?;
            wal.spill_excess_hot_pages_locked(&mut index, &mut sidecar)?;
        }
    }
    Ok(())
}

/// Evaluate the size-based auto-checkpoint thresholds (ADR 0137) and trigger
/// a synchronous checkpoint when both thresholds and reader gating allow it.
///
/// MUST be called with the writer state guard already dropped — `checkpoint`
/// re-acquires it. Best-effort: when readers are active or another checkpoint
/// is already pending, this is a silent no-op; the next reader-free commit
/// re-evaluates and may trigger then.
fn maybe_auto_checkpoint(wal: &WalHandle, pager: &PagerHandle) -> Result<()> {
    let cfg = wal.inner.auto_checkpoint;
    let pages_threshold = cfg.threshold_pages;
    let bytes_threshold = cfg.threshold_bytes;
    if pages_threshold == 0 && bytes_threshold == 0 {
        return Ok(());
    }

    let pages_since = wal.inner.pages_since_checkpoint.load(Ordering::Acquire);
    let pages_hit = pages_threshold != 0 && pages_since >= pages_threshold;

    let bytes_since = wal.latest_snapshot().saturating_sub(WAL_HEADER_SIZE);
    let bytes_hit = bytes_threshold != 0 && bytes_since >= bytes_threshold;

    if !pages_hit && !bytes_hit {
        return Ok(());
    }
    if wal.inner.checkpoint_pending.load(Ordering::Acquire) {
        return Ok(());
    }
    // Shared WAL handles have independent Db pager caches. Automatic
    // checkpoint copyback can invalidate another handle's cached main-db pages,
    // so shared file-backed databases require explicit checkpointing until
    // pager cache invalidation is coordinated across handles.
    if wal.is_shared() {
        return Ok(());
    }

    // ADR 0058: prefer the background worker when configured so the writer's
    // commit hot path is not blocked by checkpoint copyback. Start it lazily
    // on the first real threshold hit so ordinary opens do not pay thread
    // creation cost.
    if wal.inner.background_checkpoint_worker {
        if wal.inner.bg_checkpointer.get().is_none() {
            let bg = super::background::BgCheckpointer::start(
                std::sync::Arc::downgrade(&wal.inner),
                pager.clone(),
            )?;
            // A racing commit may have installed a worker first; dropping the
            // loser shuts its thread down cleanly, matching the previous
            // `OnceLock::get_or_init` behavior.
            let _ = wal.inner.bg_checkpointer.set(bg);
        }
        if let Some(bg) = wal.inner.bg_checkpointer.get() {
            bg.wake();
        }
        return Ok(());
    }

    // Skip when readers are active so we preserve ADR 0019 retention semantics
    // and avoid a redundant `prune_at_or_below` pass that would not actually
    // free memory.
    if wal.inner.reader_registry.active_reader_count()? > 0 || wal.retained_snapshot_lsn().is_some()
    {
        return Ok(());
    }

    super::checkpoint::checkpoint(wal, pager, cfg.checkpoint_timeout_sec)
}

fn append_page_delta_frame(output: &mut Vec<u8>, page_id: PageId, payload: &[u8]) -> Result<usize> {
    if page_id == 0 {
        return Err(DbError::corruption(
            "page WAL frames must have a non-zero page id",
        ));
    }
    let frame_len = FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE;
    output.push(FrameType::PageDelta as u8);
    output.extend_from_slice(&page_id.to_le_bytes());
    output.extend_from_slice(payload);
    output.extend_from_slice(&[0_u8; FRAME_TRAILER_SIZE]);
    Ok(frame_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page;

    #[test]
    fn append_page_frame_rejects_zero_page_id() {
        let mut out = Vec::new();
        let payload = vec![0u8; page::DEFAULT_PAGE_SIZE as usize];
        let res = append_page_frame(&mut out, 0, &payload, page::DEFAULT_PAGE_SIZE);
        assert!(res.is_err());
    }

    #[test]
    fn append_page_frame_rejects_size_mismatch() {
        let mut out = Vec::new();
        let payload = vec![0u8; 10];
        let res = append_page_frame(&mut out, 1, &payload, page::DEFAULT_PAGE_SIZE);
        assert!(res.is_err());
    }

    #[test]
    fn append_page_frame_encodes_frame() {
        let mut out = Vec::new();
        let payload = vec![0xAA; page::DEFAULT_PAGE_SIZE as usize];
        let res =
            append_page_frame(&mut out, 5, &payload, page::DEFAULT_PAGE_SIZE).expect("append");
        assert_eq!(res, FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE);
        assert_eq!(out[0], FrameType::Page as u8);
        // page id le bytes
        let id = u32::from_le_bytes(out[1..5].try_into().expect("id bytes"));
        assert_eq!(id, 5);
    }

    #[test]
    fn encoded_frame_scratch_retains_ordinary_capacity() {
        let mut buffer = Vec::with_capacity(32 * 1024);
        buffer.resize(8 * 1024, 0xA5);
        let capacity = buffer.capacity();

        clear_encoded_frame_scratch(&mut buffer);

        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), capacity);
    }

    #[test]
    fn encoded_frame_scratch_releases_capacity_above_retained_bound() {
        let mut buffer = Vec::with_capacity(WAL_PREPARE_RETAINED_FRAME_BYTES + 1);
        buffer.resize(WAL_PREPARE_RETAINED_FRAME_BYTES + 1, 0xA5);

        clear_encoded_frame_scratch(&mut buffer);

        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 0);
    }

    // --- ADR 0137: size-based auto-checkpoint trigger ---

    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use crate::config::{DbConfig, WalSyncMode};
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::faulty::{self, FailAction, Failpoint, FaultyVfs};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{FileKind, OpenMode, Vfs, VfsFile, VfsHandle};
    use crate::wal::format::FrameEncoding;

    use super::super::WalHandle;

    fn setup_wal(
        threshold_pages: u32,
        threshold_bytes: u64,
    ) -> (VfsHandle, PagerHandle, WalHandle) {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let path = Path::new(":memory:");
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create db file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap");
        let pager = PagerHandle::open(Arc::clone(&file), header, 1).expect("pager");
        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            wal_checkpoint_threshold_pages: threshold_pages,
            wal_checkpoint_threshold_bytes: threshold_bytes,
            release_freed_memory_after_checkpoint: false,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("acquire");
        (vfs, pager, wal)
    }

    fn payload(byte: u8) -> Vec<u8> {
        vec![byte; page::DEFAULT_PAGE_SIZE as usize]
    }

    fn setup_wal_with_vfs(
        vfs: VfsHandle,
        path: &Path,
    ) -> (VfsHandle, PagerHandle, WalHandle, DbConfig) {
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create db file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap");
        let pager = PagerHandle::open(Arc::clone(&file), header, 1).expect("pager");
        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            release_freed_memory_after_checkpoint: false,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("acquire");
        (vfs, pager, wal, cfg)
    }

    fn patterned_payload(page_id: PageId) -> Vec<u8> {
        let mut bytes = vec![0_u8; page::DEFAULT_PAGE_SIZE as usize];
        for (offset, byte) in bytes.iter_mut().enumerate() {
            let mixed = page_id
                .wrapping_mul(31)
                .wrapping_add(u32::try_from(offset).unwrap_or(u32::MAX));
            *byte = mixed.to_le_bytes()[0];
        }
        bytes
    }

    struct FailpointCleanup;

    impl Drop for FailpointCleanup {
        fn drop(&mut self) {
            let _ = faulty::clear_failpoints();
        }
    }

    #[derive(Debug)]
    struct CountingWalVfs {
        inner: MemVfs,
        wal_writes: Arc<AtomicUsize>,
    }

    impl Vfs for CountingWalVfs {
        fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
            let inner = self.inner.open(path, mode, kind)?;
            Ok(Arc::new(CountingWalFile {
                inner,
                wal_writes: Arc::clone(&self.wal_writes),
            }))
        }

        fn file_exists(&self, path: &Path) -> Result<bool> {
            self.inner.file_exists(path)
        }

        fn remove_file(&self, path: &Path) -> Result<()> {
            self.inner.remove_file(path)
        }

        fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
            self.inner.canonicalize_path(path)
        }

        fn is_memory(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct CountingWalFile {
        inner: Arc<dyn VfsFile>,
        wal_writes: Arc<AtomicUsize>,
    }

    impl VfsFile for CountingWalFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
            if self.inner.kind() == FileKind::Wal {
                self.wal_writes.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.write_at(offset, buf)
        }

        fn advise_sequential(&self) -> Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> Result<()> {
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> Result<()> {
            self.inner.set_len(len)
        }
    }

    #[test]
    fn single_page_commit_uses_two_wal_writes_and_reopens() {
        let wal_writes = Arc::new(AtomicUsize::new(0));
        let counting_vfs: Arc<dyn Vfs> = Arc::new(CountingWalVfs {
            inner: MemVfs::default(),
            wal_writes: Arc::clone(&wal_writes),
        });
        let vfs = VfsHandle::from_vfs(counting_vfs);
        let path = Path::new("two-write-small-commit.ddb");
        let (vfs, pager, wal, cfg) = setup_wal_with_vfs(vfs, path);
        wal_writes.store(0, AtomicOrdering::Release);
        let page_id = page::CATALOG_ROOT_PAGE_ID + 60;
        let page_payload = patterned_payload(page_id);
        let committed_lsn = wal
            .commit_pages(&pager, vec![(page_id, page_payload.clone())], page_id)
            .expect("commit one page");
        assert_eq!(
            wal_writes.load(AtomicOrdering::Acquire),
            2,
            "small commit should write frames+marker once and logical header end once"
        );

        drop(wal);
        let recovered =
            WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("reopen two-write commit");
        assert_eq!(recovered.latest_snapshot(), committed_lsn);
        assert_eq!(
            recovered
                .read_page_at_snapshot(&pager, page_id, committed_lsn)
                .expect("read recovered page")
                .expect("recovered page exists")
                .as_ref(),
            page_payload.as_slice()
        );
    }

    #[test]
    fn large_commit_bounds_preparation_buffers_and_recovers_every_page() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(mem_vfs);
        let path = Path::new("bounded-large-commit.ddb");
        let (vfs, pager, wal, cfg) = setup_wal_with_vfs(vfs, path);
        let batch_page_limit = wal_prepare_batch_page_limit(page::DEFAULT_PAGE_SIZE);
        let page_count = batch_page_limit * 2 + 17;
        let first_page_id = page::CATALOG_ROOT_PAGE_ID + 100;
        let pages = (0..page_count)
            .map(|index| {
                let page_id = first_page_id + u32::try_from(index).expect("test page id");
                (page_id, patterned_payload(page_id))
            })
            .collect::<Vec<_>>();
        let last_page_id = first_page_id + u32::try_from(page_count - 1).expect("last page id");

        let committed_lsn = wal
            .commit_pages(&pager, pages, last_page_id)
            .expect("large bounded commit");
        assert_eq!(wal.latest_snapshot(), committed_lsn);
        assert_eq!(
            wal.version_count().expect("version count"),
            page_count,
            "every page should be indexed after the durability boundary"
        );
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (0, page_count),
            "reader-free full frames must not retain page-sized index payloads"
        );
        {
            let writer_state = wal.inner.write_lock.lock().expect("writer state");
            assert!(
                writer_state.page_batch.capacity() <= WAL_PREPARE_RETAINED_FRAME_BYTES,
                "encoded frame scratch retained more than {} bytes: {} bytes",
                WAL_PREPARE_RETAINED_FRAME_BYTES,
                writer_state.page_batch.capacity()
            );
            assert!(
                writer_state.base_pages.capacity() <= batch_page_limit,
                "base-page pointer scratch grew beyond one bounded batch: {} entries",
                writer_state.base_pages.capacity()
            );
            assert!(writer_state.page_batch.is_empty());
            assert!(writer_state.base_pages.is_empty());
            assert!(writer_state.prepared_pages.is_empty());
        }
        let last = wal
            .read_page_at_snapshot(&pager, last_page_id, committed_lsn)
            .expect("read last committed page")
            .expect("last page in WAL");
        assert_eq!(last.as_ref(), patterned_payload(last_page_id).as_slice());

        drop(wal);
        let recovered =
            WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("recover large commit");
        assert_eq!(recovered.latest_snapshot(), committed_lsn);
        assert_eq!(
            recovered
                .version_counts_by_payload()
                .expect("recovered payload counts"),
            (page_count, 0),
            "recovery retains its existing hot-set policy"
        );
        let first = recovered
            .read_page_at_snapshot(&pager, first_page_id, committed_lsn)
            .expect("read first recovered page")
            .expect("first recovered page in WAL");
        assert_eq!(first.as_ref(), patterned_payload(first_page_id).as_slice());
        let last = recovered
            .read_page_at_snapshot(&pager, last_page_id, committed_lsn)
            .expect("read last recovered page")
            .expect("last recovered page in WAL");
        assert_eq!(last.as_ref(), patterned_payload(last_page_id).as_slice());
    }

    #[test]
    fn checkpoint_materializes_mixed_resident_delta_and_on_disk_full_page() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(mem_vfs);
        let path = Path::new("mixed-checkpoint.ddb");
        let (_vfs, pager, wal, _cfg) = setup_wal_with_vfs(vfs, path);
        let delta_page_id = page::CATALOG_ROOT_PAGE_ID + 80;
        let full_page_id = delta_page_id + 1;
        let base = vec![0x31; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(delta_page_id, &base)
            .expect("seed delta base");
        let mut delta_page = base;
        delta_page[128..136].copy_from_slice(b"resident");
        let full_page = patterned_payload(full_page_id);

        wal.commit_pages(
            &pager,
            vec![
                (delta_page_id, delta_page.clone()),
                (full_page_id, full_page.clone()),
            ],
            full_page_id,
        )
        .expect("commit mixed payload representations");
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (1, 1)
        );

        wal.checkpoint(&pager, 0).expect("checkpoint mixed WAL");
        assert_eq!(wal.version_count().expect("post-checkpoint versions"), 0);
        assert_eq!(
            pager
                .read_page(delta_page_id)
                .expect("read checkpointed delta")
                .as_ref(),
            delta_page.as_slice()
        );
        assert_eq!(
            pager
                .read_page(full_page_id)
                .expect("read checkpointed full page")
                .as_ref(),
            full_page.as_slice()
        );
    }

    #[test]
    fn later_writes_only_delta_encode_against_safe_resident_wal_bases() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(mem_vfs);
        let path = Path::new("safe-delta-bases.ddb");
        let (_vfs, pager, wal, _cfg) = setup_wal_with_vfs(vfs, path);
        let on_disk_page_id = page::CATALOG_ROOT_PAGE_ID + 90;
        let first_full = vec![0x41; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(
            &pager,
            vec![(on_disk_page_id, first_full.clone())],
            on_disk_page_id,
        )
        .expect("commit first full page");

        let mut second_full = first_full;
        second_full[17] = 0x52;
        let second_lsn = wal
            .commit_pages(
                &pager,
                vec![(on_disk_page_id, second_full.clone())],
                on_disk_page_id,
            )
            .expect("replace on-disk full page");
        {
            let index = wal.inner.index.lock().expect("WAL index");
            let version = index
                .latest_visible(on_disk_page_id, second_lsn)
                .expect("latest full version");
            assert_eq!(version.payload.wal_metadata().2, FrameEncoding::Page);
            assert!(matches!(
                version.payload,
                super::super::index::WalVersionPayload::OnDisk { .. }
            ));
        }
        assert_eq!(
            wal.read_page_at_snapshot(&pager, on_disk_page_id, second_lsn)
                .expect("read second full page")
                .expect("full page visible")
                .as_ref(),
            second_full.as_slice()
        );

        let resident_page_id = on_disk_page_id + 1;
        let base = vec![0x63; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(resident_page_id, &base)
            .expect("seed resident delta base");
        let mut first_delta = base;
        first_delta[32] = 0x74;
        wal.commit_pages(
            &pager,
            vec![(resident_page_id, first_delta.clone())],
            resident_page_id,
        )
        .expect("commit resident delta");
        let mut second_delta = first_delta;
        second_delta[33] = 0x75;
        let delta_lsn = wal
            .commit_pages(
                &pager,
                vec![(resident_page_id, second_delta.clone())],
                resident_page_id,
            )
            .expect("commit delta against resident WAL base");
        {
            let index = wal.inner.index.lock().expect("WAL index");
            let version = index
                .latest_visible(resident_page_id, delta_lsn)
                .expect("latest delta version");
            assert_eq!(version.payload.wal_metadata().2, FrameEncoding::PageDelta);
            assert!(matches!(
                version.payload,
                super::super::index::WalVersionPayload::Resident { .. }
            ));
        }
        assert_eq!(
            wal.read_page_at_snapshot(&pager, resident_page_id, delta_lsn)
                .expect("read second delta")
                .expect("delta visible")
                .as_ref(),
            second_delta.as_slice()
        );
    }

    #[test]
    fn rewrite_uses_spilled_latest_wal_page_before_main_database_fallback() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(mem_vfs);
        let path = Path::new("spilled-rewrite-base.ddb");
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create database file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(Arc::clone(&file), header, 1).expect("open pager");
        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            wal_index_hot_set_pages: 1,
            release_freed_memory_after_checkpoint: false,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("acquire WAL");

        let page_a = page::CATALOG_ROOT_PAGE_ID + 100;
        let page_b = page_a + 1;
        let main_database_a = vec![0x11; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_a, &main_database_a)
            .expect("seed page A in main database");

        // The first WAL image intentionally differs from the database at
        // almost every byte, while the final image differs from the database
        // at only one byte. Encoding the final image against the stale main
        // database looks attractive, but recovery would apply that delta on
        // top of this newer WAL image and corrupt every unchanged byte.
        let first_wal_a = vec![0xA5; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(page_a, first_wal_a)], page_b)
            .expect("commit first page A image");
        wal.commit_pages(&pager, vec![(page_b, payload(0xB6))], page_b)
            .expect("commit page B and spill page A");
        assert!(
            !wal.inner
                .index
                .lock()
                .expect("WAL index")
                .contains_page(page_a),
            "one-page hot set should spill page A before it is rewritten"
        );

        let mut latest_a = main_database_a;
        latest_a[37] = 0x7C;
        let latest_lsn = wal
            .commit_pages(&pager, vec![(page_a, latest_a.clone())], page_b)
            .expect("rewrite spilled page A");
        {
            let index = wal.inner.index.lock().expect("WAL index");
            let version = index
                .latest_visible(page_a, latest_lsn)
                .expect("latest page A version");
            assert_eq!(
                version.payload.wal_metadata().2,
                FrameEncoding::Page,
                "a rewrite based on an on-disk WAL version must be self-contained"
            );
        }

        drop(wal);
        let recovered =
            WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("reopen WAL after rewrite");
        assert_eq!(recovered.latest_snapshot(), latest_lsn);
        assert_eq!(
            recovered
                .read_page_at_snapshot(&pager, page_a, latest_lsn)
                .expect("read recovered page A")
                .expect("page A should remain in WAL")
                .as_ref(),
            latest_a.as_slice(),
            "recovery must preserve the latest full page A image"
        );
    }

    #[test]
    fn duplicate_page_across_preparation_batches_stays_full() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(mem_vfs);
        let path = Path::new("bounded-duplicate-page.ddb");
        let (_vfs, pager, wal, _cfg) = setup_wal_with_vfs(vfs, path);
        let batch_page_limit = wal_prepare_batch_page_limit(page::DEFAULT_PAGE_SIZE);
        let duplicate_page_id = page::CATALOG_ROOT_PAGE_ID + 100;
        let mut first = vec![0_u8; page::DEFAULT_PAGE_SIZE as usize];
        first[17] = 1;
        let mut latest = first.clone();
        latest[17] = 2;

        let mut pages = Vec::with_capacity(batch_page_limit + 1);
        pages.push((duplicate_page_id, first));
        for index in 1..batch_page_limit {
            let page_id = duplicate_page_id + u32::try_from(index).expect("test page id");
            pages.push((page_id, patterned_payload(page_id)));
        }
        pages.push((duplicate_page_id, latest.clone()));

        let committed_lsn = wal
            .commit_pages_if_latest(
                &pager,
                pages,
                duplicate_page_id + u32::try_from(batch_page_limit).expect("max page id"),
                0,
                wal.checkpoint_epoch(),
            )
            .expect("commit cross-batch duplicate");
        let visible = wal
            .read_page_at_snapshot(&pager, duplicate_page_id, committed_lsn)
            .expect("read duplicate page")
            .expect("duplicate page visible");
        assert_eq!(visible.as_ref(), latest.as_slice());

        let index = wal.inner.index.lock().expect("WAL index");
        let version = index
            .latest_visible(duplicate_page_id, committed_lsn)
            .expect("latest duplicate version");
        let (_, _, encoding) = version.payload.wal_metadata();
        assert_eq!(
            encoding,
            FrameEncoding::Page,
            "a duplicate must not delta-encode against the pre-commit base"
        );
    }

    #[test]
    fn failed_later_preparation_batch_leaves_old_logical_end_recoverable() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let inner: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let faulty_vfs: Arc<dyn Vfs> = Arc::new(FaultyVfs::wrap(inner));
        let vfs = VfsHandle::from_vfs(faulty_vfs);
        let path = Path::new("bounded-failed-tail.ddb");
        let (vfs, pager, wal, cfg) = setup_wal_with_vfs(vfs, path);
        let seed_page_id = page::CATALOG_ROOT_PAGE_ID + 50;
        let seed_payload = patterned_payload(seed_page_id);
        let seed_lsn = wal
            .commit_pages(
                &pager,
                vec![(seed_page_id, seed_payload.clone())],
                seed_page_id,
            )
            .expect("commit old logical state");
        let batch_page_limit = wal_prepare_batch_page_limit(page::DEFAULT_PAGE_SIZE);
        let first_page_id = page::CATALOG_ROOT_PAGE_ID + 100;
        let pages = (0..=batch_page_limit)
            .map(|index| {
                let page_id = first_page_id + u32::try_from(index).expect("test page id");
                (page_id, patterned_payload(page_id))
            })
            .collect::<Vec<_>>();

        faulty::install_failpoint(Failpoint {
            label: "wal.write_commit".to_string(),
            trigger_on: 1,
            action: FailAction::Error,
        })
        .expect("install final-batch write failure");
        let error = wal
            .commit_pages(
                &pager,
                pages,
                first_page_id + u32::try_from(batch_page_limit).expect("max page id"),
            )
            .expect_err("second frame batch must fail");
        assert!(matches!(error, DbError::Io { .. }));
        assert_eq!(
            wal.latest_snapshot(),
            seed_lsn,
            "failed tail must not replace the old logical end"
        );
        assert_eq!(wal.version_count().expect("version count"), 1);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, seed_page_id, seed_lsn)
                .expect("read old state after failure")
                .expect("old page remains visible")
                .as_ref(),
            seed_payload.as_slice()
        );
        {
            let writer_state = wal.inner.write_lock.lock().expect("writer state");
            assert!(writer_state.page_batch.is_empty());
            assert!(
                writer_state.page_batch.capacity() <= WAL_PREPARE_RETAINED_FRAME_BYTES,
                "failed commit retained {} encoded-frame scratch bytes",
                writer_state.page_batch.capacity()
            );
            assert!(writer_state.base_pages.is_empty());
            assert!(writer_state.prepared_pages.is_empty());
        }

        faulty::clear_failpoints().expect("clear write failure");
        drop(wal);
        let recovered = WalHandle::acquire(&vfs, path, &cfg, &pager, None)
            .expect("recover while ignoring unpublished tail");
        assert_eq!(recovered.latest_snapshot(), seed_lsn);
        assert_eq!(recovered.version_count().expect("recovered versions"), 1);
        assert_eq!(
            recovered
                .read_page_at_snapshot(&pager, seed_page_id, seed_lsn)
                .expect("read recovered old state")
                .expect("recovered old page")
                .as_ref(),
            seed_payload.as_slice()
        );

        let replacement_page = first_page_id + 1;
        let replacement_payload = patterned_payload(replacement_page);
        let replacement_lsn = recovered
            .commit_pages(
                &pager,
                vec![(replacement_page, replacement_payload.clone())],
                replacement_page,
            )
            .expect("overwrite ignored tail with a later commit");
        let visible = recovered
            .read_page_at_snapshot(&pager, replacement_page, replacement_lsn)
            .expect("read replacement page")
            .expect("replacement page visible");
        assert_eq!(visible.as_ref(), replacement_payload.as_slice());
    }

    #[test]
    fn sync_failure_keeps_live_index_at_old_state_and_retry_overwrites_tail() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let inner: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let faulty_vfs: Arc<dyn Vfs> = Arc::new(FaultyVfs::wrap(inner));
        let vfs = VfsHandle::from_vfs(faulty_vfs);
        let path = Path::new("on-disk-publish-sync-failure.ddb");
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create db file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap db");
        let pager = PagerHandle::open(Arc::clone(&file), header, 1).expect("open pager");
        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::Normal,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("acquire WAL");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 70;
        let old_payload = vec![0x31; page::DEFAULT_PAGE_SIZE as usize];
        let old_lsn = wal
            .commit_pages(&pager, vec![(page_id, old_payload.clone())], page_id)
            .expect("commit old page");

        faulty::install_failpoint(Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: FailAction::Error,
        })
        .expect("install WAL sync failure");
        let failed_payload = vec![0x42; page::DEFAULT_PAGE_SIZE as usize];
        let error = wal
            .commit_pages(&pager, vec![(page_id, failed_payload)], page_id)
            .expect_err("sync failure must reject commit publication");
        assert!(matches!(error, DbError::Io { .. }));
        assert_eq!(wal.latest_snapshot(), old_lsn);
        assert_eq!(wal.version_count().expect("old version count"), 1);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_id, old_lsn)
                .expect("read old snapshot")
                .expect("old page visible")
                .as_ref(),
            old_payload.as_slice()
        );

        faulty::clear_failpoints().expect("clear sync failure");
        let retry_payload = vec![0x53; page::DEFAULT_PAGE_SIZE as usize];
        let retry_lsn = wal
            .commit_pages(&pager, vec![(page_id, retry_payload.clone())], page_id)
            .expect("overwrite ambiguous tail with retry");
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_id, retry_lsn)
                .expect("read retry")
                .expect("retry page visible")
                .as_ref(),
            retry_payload.as_slice()
        );

        drop(wal);
        let recovered =
            WalHandle::acquire(&vfs, path, &cfg, &pager, None).expect("recover retried commit");
        assert_eq!(recovered.latest_snapshot(), retry_lsn);
        assert_eq!(
            recovered
                .read_page_at_snapshot(&pager, page_id, retry_lsn)
                .expect("read recovered retry")
                .expect("recovered retry page")
                .as_ref(),
            retry_payload.as_slice()
        );
    }

    #[test]
    fn auto_checkpoint_disabled_by_zero_thresholds() {
        let (_vfs, pager, wal) = setup_wal(0, 0);
        for i in 1u32..=20 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        // No threshold => no auto-checkpoint => versions accumulate.
        assert!(wal.version_count().expect("count") >= 20);
        assert_eq!(wal.checkpoint_epoch(), 0);
    }

    #[test]
    fn auto_checkpoint_fires_on_page_threshold() {
        let (_vfs, pager, wal) = setup_wal(8, 0);
        for i in 1u32..=32 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        // With a page threshold of 8 over 32 commits we expect at least 4
        // checkpoint epochs to have advanced.
        assert!(
            wal.checkpoint_epoch() >= 4,
            "expected >=4 epochs, saw {}",
            wal.checkpoint_epoch()
        );
        // After the final auto-checkpoint with no readers the index is cleared.
        assert!(
            wal.version_count().expect("count") < 8,
            "expected bounded versions, saw {}",
            wal.version_count().expect("count")
        );
    }

    #[test]
    fn auto_checkpoint_fires_on_byte_threshold() {
        // One commit ≈ FRAME_HEADER + page + FRAME_TRAILER + COMMIT_FRAME.
        // Set the byte threshold low enough that two commits trigger.
        let (_vfs, pager, wal) = setup_wal(0, 1024);
        for i in 1u32..=10 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        assert!(
            wal.checkpoint_epoch() >= 1,
            "expected at least one auto-checkpoint, saw {}",
            wal.checkpoint_epoch()
        );
    }

    #[test]
    fn auto_checkpoint_skipped_while_reader_active() {
        let (_vfs, pager, wal) = setup_wal(2, 0);
        let pid = page::CATALOG_ROOT_PAGE_ID + 1;
        wal.commit_pages(&pager, vec![(pid, payload(0xAB))], pid)
            .expect("first commit");
        let _reader = wal.begin_reader().expect("reader");
        let initial_epoch = wal.checkpoint_epoch();
        for i in 2u32..=10 {
            let p = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(p, payload(i as u8))], p)
                .expect("commit");
        }
        // The active reader must block the auto-checkpoint trigger entirely.
        assert_eq!(wal.checkpoint_epoch(), initial_epoch);
        assert!(wal.version_count().expect("count") >= 10);
    }
}
