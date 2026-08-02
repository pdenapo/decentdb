//! Background WAL flusher for `WalSyncMode::AsyncCommit`.
//!
//! Implements:
//! - design/adr/0135-async-commit-wal-group-commit.md
//!
//! Under `AsyncCommit` mode, commit calls return as soon as the WAL frame is
//! written; this module owns a single background thread per `SharedWalInner`
//! that periodically calls `sync_data` to advance a `durable_lsn` watermark.
//! The VFS contract includes file-length durability, so the same barrier
//! covers both ordinary WAL writes and allocation growth. Callers that need a hard
//! durability barrier use [`AsyncCommitState::flush_to_durable`].
//!
//! Shutdown is cooperative: dropping the state signals the flusher via an
//! `AtomicBool` + `Condvar`, joins the thread, and performs a final
//! synchronous flush so no committed work is lost on clean close.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{DbError, Result};
use crate::vfs::VfsFile;

#[cfg(test)]
std::thread_local! {
    static FORCE_REBASE_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn force_rebase_error_for_current_thread(force: bool) {
    FORCE_REBASE_ERROR.with(|state| state.set(force));
}

/// Shared state between the WAL writer, foreground sync barriers, and the
/// background flusher thread.
#[derive(Debug)]
pub(crate) struct AsyncCommitState {
    inner: Arc<AsyncCommitInner>,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct AsyncCommitInner {
    /// Most recently written WAL end LSN. Compared against `durable_lsn` to
    /// decide whether a background flush is needed.
    dirty_lsn: AtomicU64,
    /// Highest WAL LSN known to be on stable storage.
    durable_lsn: AtomicU64,
    /// Serializes foreground barriers with the background flusher so exactly
    /// one thread decides which dirty range a physical sync covers.
    flush_lock: Mutex<()>,
    /// Set by Drop to ask the flusher thread to exit.
    shutdown: AtomicBool,
    /// Flush interval in milliseconds. At least 1.
    interval_ms: u32,
    /// Wakeup channel used both to interrupt the interval sleep on shutdown
    /// and to notify barrier waiters when `durable_lsn` advances.
    wake: (Mutex<()>, Condvar),
    /// Backing file the flusher operates on. Held as `Arc<dyn VfsFile>` so
    /// the flusher does not depend on `SharedWalInner`'s lifetime — the state
    /// outlives its referent in the Drop ordering of SharedWalInner.
    file: Arc<dyn VfsFile>,
}

impl AsyncCommitState {
    pub(crate) fn new(file: Arc<dyn VfsFile>, initial_lsn: u64, interval_ms: u32) -> Result<Self> {
        let interval_ms = interval_ms.max(1);
        let inner = Arc::new(AsyncCommitInner {
            dirty_lsn: AtomicU64::new(initial_lsn),
            durable_lsn: AtomicU64::new(initial_lsn),
            flush_lock: Mutex::new(()),
            shutdown: AtomicBool::new(false),
            interval_ms,
            wake: (Mutex::new(()), Condvar::new()),
            file,
        });

        let flusher_inner = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("decentdb-wal-flusher".to_string())
            .spawn(move || flusher_loop(flusher_inner))
            .map_err(|source| DbError::io("spawn wal flusher thread", source))?;

        Ok(Self {
            inner,
            flusher: Mutex::new(Some(handle)),
        })
    }

    /// Records that the WAL has been extended to `new_end_lsn`. Called from
    /// the writer in place of a synchronous durability barrier.
    pub(crate) fn note_write(&self, new_end_lsn: u64) {
        // Use fetch_max so out-of-order calls (which shouldn't happen because
        // commits are serialized by the write lock, but defensive) cannot
        // regress the watermark.
        self.inner
            .dirty_lsn
            .fetch_max(new_end_lsn, Ordering::AcqRel);
        // No notify here: the flusher polls on its interval and does not
        // benefit from immediate wakeup. Only barrier waiters need notify,
        // which the flusher itself issues after each successful sync.
    }

    /// Makes every commit acknowledged before this call durable on disk. Returns
    /// immediately if there is nothing to flush.
    pub(crate) fn flush_to_durable(&self) -> Result<bool> {
        let target = self.inner.dirty_lsn.load(Ordering::Acquire);
        if self.inner.durable_lsn.load(Ordering::Acquire) >= target {
            return Ok(false);
        }
        let mut flushed = false;
        while self.inner.durable_lsn.load(Ordering::Acquire) < target {
            if self.inner.shutdown.load(Ordering::Acquire) {
                // On shutdown the Drop path will perform a final flush; we do
                // not want to deadlock if shutdown raced ahead of us.
                break;
            }
            perform_flush(&self.inner)?;
            flushed = true;
        }
        Ok(flushed)
    }

    /// Rebase async durability watermarks after a successful logical WAL
    /// truncate. The caller holds the WAL write lock, so no commit can publish
    /// a new lower post-truncate LSN while the counters are being reset.
    pub(crate) fn rebase_clean_lsn(&self, clean_lsn: u64) -> Result<()> {
        #[cfg(test)]
        if FORCE_REBASE_ERROR.with(std::cell::Cell::get) {
            return Err(DbError::internal("injected async-commit rebase failure"));
        }
        let _flush_guard = self
            .inner
            .flush_lock
            .lock()
            .map_err(|_| DbError::internal("async-commit flush lock poisoned"))?;
        self.inner.dirty_lsn.store(clean_lsn, Ordering::Release);
        self.inner.durable_lsn.store(clean_lsn, Ordering::Release);
        let (lock, cvar) = &self.inner.wake;
        let _guard = lock
            .lock()
            .map_err(|_| DbError::internal("async-commit wake lock poisoned"))?;
        cvar.notify_all();
        Ok(())
    }

    /// Conservatively sync an externally recovered WAL tail before checkpoint
    /// copyback. The local async watermarks cannot prove durability for bytes
    /// another process may have acknowledged under `AsyncCommit`, so this
    /// serializes with the background flusher and durably syncs both the WAL
    /// bytes and their addressing file length.
    pub(crate) fn sync_for_checkpoint_tail(&self) -> Result<()> {
        let _flush_guard = self
            .inner
            .flush_lock
            .lock()
            .map_err(|_| DbError::internal("async-commit flush lock poisoned"))?;
        self.inner.file.sync_data()
    }

    /// Highest LSN currently on disk, for diagnostics/tests.
    #[allow(dead_code)]
    pub(crate) fn durable_lsn(&self) -> u64 {
        self.inner.durable_lsn.load(Ordering::Acquire)
    }
}

impl Drop for AsyncCommitState {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        // Wake the flusher out of its interval sleep.
        {
            let (lock, cvar) = &self.inner.wake;
            let _guard = lock
                .lock()
                .expect("async-commit wake lock should not be poisoned");
            cvar.notify_all();
        }
        // Join cleanly so the final flush below races nothing.
        if let Some(handle) = self
            .flusher
            .lock()
            .expect("async-commit flusher slot should not be poisoned")
            .take()
        {
            let _ = handle.join();
        }
        // Final synchronous flush — guarantees that a clean close never loses
        // commits even if the last interval tick had not fired.
        let _ = perform_flush(&self.inner);
    }
}

fn flusher_loop(inner: Arc<AsyncCommitInner>) {
    let interval = Duration::from_millis(inner.interval_ms as u64);
    loop {
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        // Sleep on the condvar so shutdown can interrupt us promptly.
        {
            let (lock, cvar) = &inner.wake;
            let guard = lock
                .lock()
                .expect("async-commit wake lock should not be poisoned");
            let (_guard, _timeout) = cvar
                .wait_timeout(guard, interval)
                .expect("async-commit wake cvar should not be poisoned");
        }
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        let _ = perform_flush(&inner);
    }
}

fn perform_flush(inner: &AsyncCommitInner) -> Result<()> {
    let _flush_guard = inner
        .flush_lock
        .lock()
        .map_err(|_| DbError::internal("async-commit flush lock poisoned"))?;
    let target = inner.dirty_lsn.load(Ordering::Acquire);
    if inner.durable_lsn.load(Ordering::Acquire) >= target {
        return Ok(());
    }
    inner.file.sync_data()?;
    inner.durable_lsn.fetch_max(target, Ordering::AcqRel);
    // Notify any barrier waiters.
    let (lock, cvar) = &inner.wake;
    let _guard = lock
        .lock()
        .map_err(|_| DbError::internal("async-commit wake lock poisoned"))?;
    cvar.notify_all();
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::config::{DbConfig, WalSyncMode};
    use crate::vfs::faulty;
    use crate::{Db, DbError};

    struct FailpointCleanup;

    impl Drop for FailpointCleanup {
        fn drop(&mut self) {
            let _ = faulty::clear_failpoints();
        }
    }

    #[test]
    fn checkpoint_preflush_failure_happens_before_database_copyback() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-preflush-fails.ddb");
        let db = Db::create(
            &path,
            DbConfig {
                wal_sync_mode: WalSyncMode::AsyncCommit {
                    interval_ms: 60_000,
                },
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
        )
        .expect("create db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t (id, v) VALUES (1, 'still-in-wal')")
            .expect("insert row");

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install async preflush failure");
        faulty::install_failpoint(faulty::Failpoint {
            label: "db.write_page".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install database-copyback sentinel");
        let error = db
            .checkpoint_wal()
            .expect_err("async preflush failure must fail checkpoint");
        assert!(matches!(error, DbError::Io { .. }));
        let logs = faulty::failpoint_logs().expect("failpoint log");
        assert!(
            logs.iter()
                .any(|entry| { entry.label == "wal.fsync" && entry.outcome == "error" }),
            "checkpoint should fail at the foreground WAL data-and-length sync: {logs:?}"
        );
        assert!(
            logs.iter().all(|entry| entry.label != "db.write_page"),
            "checkpoint copied database pages before the async WAL preflush completed: {logs:?}"
        );
    }

    #[test]
    fn checkpoint_rebases_async_watermarks_for_next_small_commit_sync() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-rebases-async.ddb");
        let db = Db::create(
            &path,
            DbConfig {
                wal_sync_mode: WalSyncMode::AsyncCommit {
                    interval_ms: 60_000,
                },
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
        )
        .expect("create db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t (id, v) VALUES (1, 'before')")
            .expect("insert before checkpoint");
        db.checkpoint_wal().expect("checkpoint WAL");

        faulty::clear_failpoints().expect("clear checkpoint sync logs");
        db.execute("INSERT INTO t (id, v) VALUES (2, 'after')")
            .expect("insert after checkpoint");
        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install second-tail sync failure");
        let error = db
            .sync()
            .expect_err("post-checkpoint async sync must flush the new low-offset WAL tail");
        assert!(matches!(error, DbError::Io { .. }));
        let logs = faulty::failpoint_logs().expect("failpoint logs");
        assert!(
            logs.iter()
                .any(|entry| entry.label == "wal.fsync" && entry.outcome == "error"),
            "Db::sync skipped the post-checkpoint WAL tail after watermark rebase: {logs:?}"
        );
    }

    #[test]
    fn refresh_rebases_async_watermarks_after_external_checkpoint_truncation() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("external-checkpoint-rebases-async.ddb");
        let config = DbConfig {
            wal_sync_mode: WalSyncMode::AsyncCommit {
                interval_ms: 60_000,
            },
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let owner = Db::create(&path, config.clone()).expect("create owner");
        owner
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .expect("create table");
        owner
            .execute("INSERT INTO t (id, v) VALUES (1, 'before')")
            .expect("insert before external open");

        crate::evict_shared_wal(&path).expect("evict shared WAL to simulate independent opener");
        let peer = Db::open(&path, config).expect("open independent async peer");
        owner.checkpoint_wal().expect("owner checkpoints WAL");

        faulty::clear_failpoints().expect("clear checkpoint sync logs");
        peer.execute("INSERT INTO t (id, v) VALUES (2, 'after')")
            .expect("peer refreshes external checkpoint then inserts");
        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install peer sync failure");
        let error = peer
            .sync()
            .expect_err("peer sync must flush low-offset tail after external rebase");
        assert!(matches!(error, DbError::Io { .. }));
        let logs = faulty::failpoint_logs().expect("failpoint logs");
        assert!(
            logs.iter()
                .any(|entry| entry.label == "wal.fsync" && entry.outcome == "error"),
            "peer Db::sync skipped the low-offset WAL tail after external checkpoint rebase: {logs:?}"
        );
    }
}
