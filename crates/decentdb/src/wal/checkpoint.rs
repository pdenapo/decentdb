//! Reader-safe checkpoint copyback and WAL pruning.
//!
//! Implements:
//! - design/adr/0004-wal-checkpoint-strategy.md
//! - design/adr/0056-wal-index-pruning-on-checkpoint.md
//! - design/adr/0210-bounded-pipelined-checkpoint-copyback.md

use std::sync::atomic::Ordering;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;

use super::index::WalVersion;
use super::writer;
use super::{CheckpointWalReadAhead, WalHandle};

/// Bound checkpoint copyback scratch even when every dirty page is contiguous.
/// This is large enough to amortize positional-write overhead while avoiding
/// a database-sized allocation (and a second equally large TDE encryption
/// buffer) for full and huge checkpoints.
pub(super) const COPYBACK_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[cfg(test)]
std::thread_local! {
    static FORCE_IO_WORKER_SPAWN_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn force_io_worker_spawn_failure_for_current_thread(force: bool) {
    FORCE_IO_WORKER_SPAWN_FAILURE.with(|state| state.set(force));
}

pub(crate) fn checkpoint(wal: &WalHandle, pager: &PagerHandle, timeout_sec: u64) -> Result<()> {
    struct PendingReset<'a>(&'a WalHandle);

    impl Drop for PendingReset<'_> {
        fn drop(&mut self) {
            self.0
                .inner
                .checkpoint_pending
                .store(false, Ordering::SeqCst);
        }
    }

    let _process_guard = wal.lock_process_checkpoint()?;
    wal.refresh_from_coordination(pager)?;
    let _writer_guard = wal
        .inner
        .write_lock
        .lock()
        .map_err(|_| DbError::internal("wal write lock poisoned"))?;
    wal.inner.checkpoint_pending.store(true, Ordering::SeqCst);
    let _pending_reset = PendingReset(wal);
    {
        // Fence with begin_reader(), which holds the index lock while it
        // re-checks checkpoint_pending and registers its snapshot. Without
        // this handoff a checkpoint can compute safe_lsn before a reader that
        // already passed the pending check becomes visible in the registry,
        // then copy back newer pages underneath that reader's snapshot.
        let _index = wal
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
    }

    let current_lsn = wal.latest_snapshot();
    let active_reader_lsn = wal.inner.reader_registry.min_snapshot_lsn()?;
    let retained_snapshot_lsn = wal.retained_snapshot_lsn();
    let process_retention = wal.process_reader_retention()?;
    let process_readers_block_checkpoint = process_retention
        .as_ref()
        .is_some_and(|retention| retention.active_count > 0 || retention.truncation_blocked);
    let named_snapshot_retained =
        retained_snapshot_lsn.is_some_and(|snapshot_lsn| snapshot_lsn < current_lsn);
    if active_reader_lsn.is_some() || named_snapshot_retained || process_readers_block_checkpoint {
        // Copyback mutates main-db pages one at a time. Readers normally use
        // WAL versions for changed pages, but overflow and delta paths can
        // still fall back to main-db pages for stable bases. Keep the data
        // file unchanged while snapshots are live; a later reader-free
        // checkpoint will copy back and truncate the WAL.
        let _warnings = wal
            .inner
            .reader_registry
            .capture_long_reader_warnings(timeout_sec)?;
        return Ok(());
    }
    let safe_lsn = current_lsn;
    // `AsyncCommit` may have acknowledged WAL frames that are still only in the
    // OS page cache. Before mutating the main database, force those frames to a
    // durable WAL state; non-async modes are already durable here.
    wal.flush_to_durable()?;
    if wal.inner.process_coordinator.is_some()
        && wal.latest_snapshot() > 0
        && !wal
            .inner
            .checkpoint_tail_locally_synced
            .load(Ordering::Acquire)
    {
        wal.inner
            .checkpoint_tail_sync_needed
            .store(true, Ordering::Release);
    }
    if wal
        .inner
        .checkpoint_tail_sync_needed
        .load(Ordering::Acquire)
    {
        if let Err(error) = writer::sync_checkpoint_tail(wal) {
            wal.inner
                .checkpoint_tail_sync_needed
                .store(true, Ordering::Release);
            wal.inner
                .checkpoint_tail_locally_synced
                .store(false, Ordering::Release);
            return Err(error);
        }
        wal.inner
            .checkpoint_tail_sync_needed
            .store(false, Ordering::Release);
        wal.inner
            .checkpoint_tail_locally_synced
            .store(true, Ordering::Release);
    }

    let mut latest_versions = wal
        .inner
        .checkpoint_scratch
        .lock()
        .map_err(|_| DbError::internal("checkpoint scratch lock poisoned"))?;
    {
        let index = wal
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        index.populate_latest_versions_at_or_before(safe_lsn, &mut latest_versions);
    }
    if let Some(sidecar) = &wal.inner.index_sidecar {
        sidecar
            .lock()
            .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?
            .populate_latest_versions_at_or_before(safe_lsn, &mut latest_versions)?;
    }
    // Sort by page id so copyback writes pages in file order. This turns the
    // main-file copyback into bounded sequential writes instead of one
    // `pwrite` + one `statx` per page, and lets the kernel merge/flush the data
    // efficiently before the single durability sync below.
    latest_versions.sort_by_key(|(page_id, _)| *page_id);

    let page_size = pager.page_size() as usize;
    if page_size == 0 {
        return Err(DbError::corruption("checkpoint pager has zero page size"));
    }
    let mut io_worker =
        copyback_latest_versions(wal, pager, safe_lsn, page_size, &latest_versions)?;
    latest_versions.clear();
    drop(latest_versions);
    pager.invalidate_cache_after_local_checkpoint()?;
    // The header page is WAL-managed, so copyback may have just landed a
    // newer committed header (freelist head/count) on the main file. Reload
    // it so `truncate_freelist_tail` observes the committed freelist.
    pager.refresh_header_from_disk_after_local_checkpoint()?;
    if let Some(page_count) = pager.truncate_freelist_tail()? {
        wal.reset_max_page_count(page_count);
    } else {
        // Copyback writes update the pager's cached count and the checkpoint
        // gate excludes a concurrent resize, so an exact stat here is
        // redundant. The freelist-truncate branch still performs an exact
        // resize and installs its resulting count.
        wal.reset_max_page_count(pager.cached_page_count());
    }
    pager.set_last_checkpoint_lsn(safe_lsn)?;
    // Durability invariant (ADR 0004): the main database file must be durable
    // before the WAL — the only other copy of the committed pages — is
    // truncated below. The built-in VFS `sync_data` implementations make both
    // the copied page data and file length durable; see ADR 0004. Copyback may
    // have extended the file and `truncate_freelist_tail` may have shrunk it,
    // so persisting the length is part of this barrier.
    pager.sync_data()?;
    // Only truncate WAL when safe_lsn covers all committed data (i.e. no
    // readers were active when we started, so we wrote every version). If
    // safe_lsn < current_lsn, a reader that dropped after we computed safe_lsn
    // would cause us to lose post-safe_lsn commits if we truncated.
    if safe_lsn < current_lsn {
        unreachable!("reader-blocked checkpoints return before copyback");
    }
    wal.publish_process_checkpoint(safe_lsn, current_lsn)?;
    let pending_truncate = writer::truncate_to_header_before_sync(wal)?;
    // Eligible large checkpoints already paid to create an I/O worker for
    // copyback. Reuse it for the final WAL sync while the checkpoint thread
    // clears indexes and returns freed arenas. Publication and return still
    // wait for the worker and reconcile every error/panic below.
    let final_sync_started = if let Some(worker) = io_worker.as_ref() {
        worker.start_wal_sync().is_ok()
    } else {
        false
    };
    let mut cleanup_error = None;
    match wal.inner.index.lock() {
        Ok(mut index) => index.clear(),
        Err(poisoned) => {
            poisoned.into_inner().clear();
            cleanup_error.get_or_insert_with(|| DbError::internal("wal index lock poisoned"));
        }
    }
    if let Some(sidecar) = &wal.inner.index_sidecar {
        match sidecar.lock() {
            Ok(mut sidecar) => {
                if let Err(error) = sidecar.clear() {
                    cleanup_error.get_or_insert(error);
                }
            }
            Err(poisoned) => {
                cleanup_error
                    .get_or_insert_with(|| DbError::internal("wal index sidecar lock poisoned"));
                if let Err(error) = poisoned.into_inner().clear() {
                    cleanup_error.get_or_insert(error);
                }
            }
        }
    }
    wal.inner.checkpoint_epoch.fetch_add(1, Ordering::AcqRel);
    // Reset the size-based trigger counter (ADR 0137). The byte threshold
    // resets implicitly because `truncate_to_header` zeroes `wal_end_lsn`;
    // when readers prevented truncation the next commit will re-evaluate
    // against the still-larger WAL but with `pages_since_checkpoint = 0`.
    wal.inner.pages_since_checkpoint.store(0, Ordering::Release);

    // Return freed heap arenas to the OS on platforms where it helps.
    // No-op on non-Linux/non-glibc targets. ADR 0138.
    if wal.inner.auto_checkpoint.release_freed_after_checkpoint {
        super::platform::release_freed_heap();
    }

    let mut worker_error = None;
    let final_sync_result = if let Some(worker) = io_worker.as_mut() {
        let worker_sync_result = if final_sync_started {
            worker.wait_wal_sync()
        } else {
            Err(DbError::internal(
                "checkpoint I/O worker exited before accepting final WAL sync",
            ))
        };
        let worker_join_result = worker.stop_and_join();
        match (worker_sync_result, worker_join_result) {
            (Ok(sync_result), Ok(())) => sync_result,
            (Err(error), join_result) => {
                worker_error = Some(join_result.err().unwrap_or(error));
                // A worker/channel failure leaves final durability uncertain.
                // Re-run the idempotent sync locally before publication.
                writer::sync_truncated_wal(wal)
            }
            (Ok(_), Err(error)) => {
                worker_error = Some(error);
                writer::sync_truncated_wal(wal)
            }
        }
    } else {
        writer::sync_truncated_wal(wal)
    };
    let truncate_outcome = writer::finish_truncate_to_header(pending_truncate, final_sync_result);

    let mut final_publish_error = None;
    if truncate_outcome.final_sync_succeeded {
        if let Err(error) = wal.publish_process_checkpoint(safe_lsn, wal.latest_snapshot()) {
            final_publish_error = Some(error);
        }
    }

    if let Some(error) = truncate_outcome.post_logical_error {
        return Err(error);
    }
    if let Some(error) = worker_error {
        return Err(error);
    }
    if let Some(error) = final_publish_error {
        return Err(error);
    }
    if let Some(error) = cleanup_error {
        return Err(error);
    }

    Ok(())
}

fn copyback_latest_versions(
    wal: &WalHandle,
    pager: &PagerHandle,
    safe_lsn: u64,
    page_size: usize,
    latest_versions: &[(PageId, WalVersion)],
) -> Result<Option<CheckpointIoWorker>> {
    let batch_pages = (COPYBACK_BATCH_BYTES / page_size).max(1);
    let batch_capacity = batch_pages * page_size;
    let total_copyback_bytes = latest_versions.len().saturating_mul(page_size);
    let is_one_contiguous_run = latest_versions.windows(2).all(|versions| {
        versions[0]
            .0
            .checked_add(1)
            .is_some_and(|next_page_id| next_page_id == versions[1].0)
    });

    // Large contiguous checkpoints can overlap WAL materialization for the
    // next batch with the current database-file pwrite. Keep small and sparse
    // checkpoints on the simpler single-threaded path: they do not have
    // enough copyback work to amortize thread launch and channel handoff.
    if total_copyback_bytes > batch_capacity && is_one_contiguous_run {
        return copyback_contiguous_pipelined(
            wal,
            pager,
            safe_lsn,
            page_size,
            batch_capacity,
            latest_versions,
        );
    }

    copyback_sequential(
        wal,
        pager,
        safe_lsn,
        page_size,
        batch_capacity,
        total_copyback_bytes,
        latest_versions,
    )?;
    Ok(None)
}

fn copyback_sequential(
    wal: &WalHandle,
    pager: &PagerHandle,
    safe_lsn: u64,
    page_size: usize,
    batch_capacity: usize,
    total_copyback_bytes: usize,
    latest_versions: &[(PageId, WalVersion)],
) -> Result<()> {
    // Coalesce materialized pages into bounded contiguous byte runs. Each run
    // is flushed with a single positional write, amortizing syscall overhead
    // without allocating memory proportional to the database size.
    let initial_capacity = batch_capacity.min(total_copyback_bytes);
    let mut coalesce = allocate_copyback_buffer(initial_capacity)?;
    let mut run_start_page: Option<PageId> = None;
    let mut run_next_page: Option<PageId> = None;
    let mut wal_read_ahead = CheckpointWalReadAhead::default();
    for (page_id, version) in latest_versions {
        let continues_run = run_next_page == Some(*page_id);
        let fits_batch = coalesce.len() <= batch_capacity.saturating_sub(page_size);
        if run_start_page.is_some() && (!continues_run || !fits_batch) {
            flush_copyback_run(pager, &mut run_start_page, &mut coalesce)?;
        }
        if run_start_page.is_none() {
            run_start_page = Some(*page_id);
        }
        append_checkpoint_page(
            wal,
            pager,
            safe_lsn,
            page_size,
            *page_id,
            version,
            &mut wal_read_ahead,
            &mut coalesce,
        )?;
        run_next_page = page_id.checked_add(1);
    }
    flush_copyback_run(pager, &mut run_start_page, &mut coalesce)?;
    Ok(())
}

#[derive(Debug)]
struct CopybackBatch {
    start_page_id: PageId,
    bytes: Vec<u8>,
}

#[derive(Debug)]
enum CheckpointIoCommand {
    Copyback(CopybackBatch),
    SyncWal,
    Stop,
}

#[derive(Debug)]
struct CheckpointIoWorker {
    command_sender: std::sync::mpsc::SyncSender<CheckpointIoCommand>,
    copyback_receiver: std::sync::mpsc::Receiver<(Vec<u8>, Result<()>)>,
    wal_sync_receiver: std::sync::mpsc::Receiver<Result<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl CheckpointIoWorker {
    fn start(pager: PagerHandle, wal: WalHandle) -> std::io::Result<Self> {
        use std::sync::mpsc;

        #[cfg(test)]
        if FORCE_IO_WORKER_SPAWN_FAILURE.with(std::cell::Cell::get) {
            return Err(std::io::Error::other(
                "injected checkpoint I/O worker spawn failure",
            ));
        }

        let (command_sender, command_receiver) = mpsc::sync_channel(0);
        // Results are unbounded so a materialization failure can drop the
        // command sender and join the worker without leaving it blocked in a
        // result send. There is at most one outstanding item of either kind.
        let (copyback_sender, copyback_receiver) = mpsc::channel();
        let (wal_sync_sender, wal_sync_receiver) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("decentdb-checkpoint-io".into())
            .spawn(move || {
                while let Ok(command) = command_receiver.recv() {
                    match command {
                        CheckpointIoCommand::Copyback(batch) => {
                            let result = pager
                                .write_pages_contiguous_no_cache(batch.start_page_id, &batch.bytes);
                            if copyback_sender.send((batch.bytes, result)).is_err() {
                                break;
                            }
                        }
                        CheckpointIoCommand::SyncWal => {
                            if wal_sync_sender
                                .send(writer::sync_truncated_wal(&wal))
                                .is_err()
                            {
                                break;
                            }
                        }
                        CheckpointIoCommand::Stop => break,
                    }
                }
            })?;
        Ok(Self {
            command_sender,
            copyback_receiver,
            wal_sync_receiver,
            join: Some(join),
        })
    }

    fn send_copyback(&self, batch: CopybackBatch) -> Result<()> {
        self.command_sender
            .send(CheckpointIoCommand::Copyback(batch))
            .map_err(|_| {
                DbError::internal("checkpoint I/O worker exited before accepting copyback batch")
            })
    }

    fn receive_copyback(&self) -> Result<(Vec<u8>, Result<()>)> {
        self.copyback_receiver.recv().map_err(|_| {
            DbError::internal("checkpoint I/O worker exited before returning copyback batch")
        })
    }

    fn start_wal_sync(&self) -> Result<()> {
        self.command_sender
            .send(CheckpointIoCommand::SyncWal)
            .map_err(|_| {
                DbError::internal("checkpoint I/O worker exited before accepting final WAL sync")
            })
    }

    fn wait_wal_sync(&self) -> Result<Result<()>> {
        self.wal_sync_receiver.recv().map_err(|_| {
            DbError::internal("checkpoint I/O worker exited before returning final WAL sync")
        })
    }

    fn stop_and_join(&mut self) -> Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let _ = self.command_sender.send(CheckpointIoCommand::Stop);
        join.join()
            .map_err(|_| DbError::internal("checkpoint I/O worker panicked"))
    }
}

impl Drop for CheckpointIoWorker {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

fn copyback_contiguous_pipelined(
    wal: &WalHandle,
    pager: &PagerHandle,
    safe_lsn: u64,
    page_size: usize,
    batch_capacity: usize,
    latest_versions: &[(PageId, WalVersion)],
) -> Result<Option<CheckpointIoWorker>> {
    let mut worker = match CheckpointIoWorker::start(pager.clone(), wal.clone()) {
        Ok(worker) => worker,
        Err(_) => {
            // Resource exhaustion must not turn an otherwise valid checkpoint
            // into an error. Fall back before any copyback.
            copyback_sequential(
                wal,
                pager,
                safe_lsn,
                page_size,
                batch_capacity,
                latest_versions.len().saturating_mul(page_size),
                latest_versions,
            )?;
            return Ok(None);
        }
    };

    let pipeline_result = produce_copyback_batches(
        wal,
        pager,
        safe_lsn,
        page_size,
        batch_capacity,
        latest_versions,
        &worker,
    );
    if let Err(error) = pipeline_result {
        worker.stop_and_join()?;
        return Err(error);
    }
    Ok(Some(worker))
}

#[allow(clippy::too_many_arguments)]
fn produce_copyback_batches(
    wal: &WalHandle,
    pager: &PagerHandle,
    safe_lsn: u64,
    page_size: usize,
    batch_capacity: usize,
    latest_versions: &[(PageId, WalVersion)],
    worker: &CheckpointIoWorker,
) -> Result<()> {
    let mut current = allocate_copyback_buffer(batch_capacity)?;
    let mut current_start_page = latest_versions.first().map(|(page_id, _)| *page_id);
    let mut wal_read_ahead = CheckpointWalReadAhead::default();
    let mut outstanding = false;

    for (page_id, version) in latest_versions {
        if current.len() > batch_capacity.saturating_sub(page_size) {
            dispatch_copyback_batch(
                &mut current,
                &mut current_start_page,
                &mut outstanding,
                batch_capacity,
                worker,
            )?;
            current_start_page = Some(*page_id);
        }
        append_checkpoint_page(
            wal,
            pager,
            safe_lsn,
            page_size,
            *page_id,
            version,
            &mut wal_read_ahead,
            &mut current,
        )?;
    }
    if !current.is_empty() {
        dispatch_copyback_batch(
            &mut current,
            &mut current_start_page,
            &mut outstanding,
            batch_capacity,
            worker,
        )?;
    }
    if outstanding {
        let (_, result) = worker.receive_copyback()?;
        result?;
    }
    Ok(())
}

fn dispatch_copyback_batch(
    current: &mut Vec<u8>,
    current_start_page: &mut Option<PageId>,
    outstanding: &mut bool,
    batch_capacity: usize,
    worker: &CheckpointIoWorker,
) -> Result<()> {
    let bytes = std::mem::take(current);
    let start_page_id = current_start_page
        .take()
        .ok_or_else(|| DbError::internal("checkpoint copyback batch has no starting page"))?;
    if *outstanding {
        let (mut returned, result) = worker.receive_copyback()?;
        result?;
        returned.clear();
        *current = returned;
    }
    worker.send_copyback(CopybackBatch {
        start_page_id,
        bytes,
    })?;
    if !*outstanding {
        *current = allocate_copyback_buffer(batch_capacity)?;
    }
    *outstanding = true;
    Ok(())
}

fn allocate_copyback_buffer(capacity: usize) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(capacity).map_err(|error| {
        DbError::internal(format!(
            "allocate checkpoint copyback scratch ({capacity} bytes): {error}"
        ))
    })?;
    Ok(buffer)
}

#[allow(clippy::too_many_arguments)]
fn append_checkpoint_page(
    wal: &WalHandle,
    pager: &PagerHandle,
    safe_lsn: u64,
    page_size: usize,
    page_id: PageId,
    version: &WalVersion,
    wal_read_ahead: &mut CheckpointWalReadAhead,
    output: &mut Vec<u8>,
) -> Result<()> {
    let prior_len = output.len();
    wal.append_checkpoint_version_to(pager, page_id, version, safe_lsn, wal_read_ahead, output)?;
    if output.len() != prior_len.saturating_add(page_size) {
        return Err(DbError::corruption(format!(
            "checkpoint page {page_id} did not append exactly {page_size} bytes"
        )));
    }
    Ok(())
}

fn flush_copyback_run(
    pager: &PagerHandle,
    run_start_page: &mut Option<PageId>,
    coalesce: &mut Vec<u8>,
) -> Result<()> {
    if let Some(start_page_id) = run_start_page.take() {
        pager.write_pages_contiguous_no_cache(start_page_id, coalesce)?;
        coalesce.clear();
    }
    Ok(())
}
