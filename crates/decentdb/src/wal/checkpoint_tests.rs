#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use tempfile::TempDir;

    use crate::config::{DbConfig, DbEncryptionConfig, ProcessCoordinationMode};
    use crate::vfs::faulty;
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{FileKind, OpenMode, Vfs, VfsFile, VfsFileLock, VfsHandle};
    use crate::wal::format::{FrameType, WAL_HEADER_SIZE};
    use crate::{Db, Value};

    use crate::wal::checkpoint::COPYBACK_BATCH_BYTES;

    #[derive(Debug)]
    struct MaxWriteVfs {
        inner: Arc<dyn Vfs>,
        max_database_write: Arc<AtomicUsize>,
        wal_reads: Arc<AtomicUsize>,
        wal_read_bytes: Arc<AtomicUsize>,
        db_sync_data: Arc<AtomicUsize>,
        wal_sync_data: Arc<AtomicUsize>,
        wal_sync_metadata: Arc<AtomicUsize>,
        sync_sequence: Arc<AtomicUsize>,
        db_sync_order: Arc<AtomicUsize>,
        wal_sync_order: Arc<AtomicUsize>,
    }

    impl Vfs for MaxWriteVfs {
        fn open(
            &self,
            path: &Path,
            mode: OpenMode,
            kind: FileKind,
        ) -> crate::Result<Arc<dyn VfsFile>> {
            let inner = self.inner.open(path, mode, kind)?;
            Ok(Arc::new(MaxWriteFile {
                inner,
                max_database_write: Arc::clone(&self.max_database_write),
                wal_reads: Arc::clone(&self.wal_reads),
                wal_read_bytes: Arc::clone(&self.wal_read_bytes),
                db_sync_data: Arc::clone(&self.db_sync_data),
                wal_sync_data: Arc::clone(&self.wal_sync_data),
                wal_sync_metadata: Arc::clone(&self.wal_sync_metadata),
                sync_sequence: Arc::clone(&self.sync_sequence),
                db_sync_order: Arc::clone(&self.db_sync_order),
                wal_sync_order: Arc::clone(&self.wal_sync_order),
            }))
        }

        fn file_exists(&self, path: &Path) -> crate::Result<bool> {
            self.inner.file_exists(path)
        }

        fn remove_file(&self, path: &Path) -> crate::Result<()> {
            self.inner.remove_file(path)
        }

        fn canonicalize_path(&self, path: &Path) -> crate::Result<PathBuf> {
            self.inner.canonicalize_path(path)
        }

        fn is_memory(&self) -> bool {
            self.inner.is_memory()
        }

        fn supports_file_locks(&self) -> bool {
            self.inner.supports_file_locks()
        }
    }

    #[derive(Debug)]
    struct MaxWriteFile {
        inner: Arc<dyn VfsFile>,
        max_database_write: Arc<AtomicUsize>,
        wal_reads: Arc<AtomicUsize>,
        wal_read_bytes: Arc<AtomicUsize>,
        db_sync_data: Arc<AtomicUsize>,
        wal_sync_data: Arc<AtomicUsize>,
        wal_sync_metadata: Arc<AtomicUsize>,
        sync_sequence: Arc<AtomicUsize>,
        db_sync_order: Arc<AtomicUsize>,
        wal_sync_order: Arc<AtomicUsize>,
    }

    impl VfsFile for MaxWriteFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
            if self.inner.kind() == FileKind::Wal {
                self.wal_reads.fetch_add(1, Ordering::Relaxed);
                self.wal_read_bytes.fetch_add(buf.len(), Ordering::Relaxed);
            }
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
            if self.inner.kind() == FileKind::Database {
                self.max_database_write
                    .fetch_max(buf.len(), Ordering::Relaxed);
            }
            self.inner.write_at(offset, buf)
        }

        fn advise_sequential(&self) -> crate::Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> crate::Result<()> {
            let order = self.sync_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            if self.inner.kind() == FileKind::Database {
                self.db_sync_data.fetch_add(1, Ordering::Relaxed);
                self.db_sync_order.store(order, Ordering::Relaxed);
            } else if self.inner.kind() == FileKind::Wal {
                self.wal_sync_data.fetch_add(1, Ordering::Relaxed);
                self.wal_sync_order.store(order, Ordering::Relaxed);
            }
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> crate::Result<()> {
            if self.inner.kind() == FileKind::Wal {
                self.wal_sync_metadata.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> crate::Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> crate::Result<()> {
            self.inner.set_len(len)
        }

        fn try_lock_range(
            &self,
            offset: u64,
            len: u64,
            exclusive: bool,
        ) -> crate::Result<Option<Box<dyn VfsFileLock>>> {
            self.inner.try_lock_range(offset, len, exclusive)
        }
    }

    const PIPELINE_PROBE_PASS: usize = 0;
    const PIPELINE_PROBE_OVERLAP: usize = 1;
    const PIPELINE_PROBE_ERROR_SECOND_WRITE: usize = 2;
    const PIPELINE_PROBE_PANIC_FIRST_WRITE: usize = 3;
    const PIPELINE_PROBE_MATERIALIZE_ERROR: usize = 4;
    const PIPELINE_PROBE_FINAL_SYNC_ERROR: usize = 5;
    const PIPELINE_PROBE_FINAL_SYNC_PANIC: usize = 6;

    #[derive(Debug)]
    struct PipelineProbeState {
        mode: AtomicUsize,
        armed: AtomicBool,
        database_batch_writes: AtomicUsize,
        wal_read_bytes: AtomicUsize,
        overlap_observed: AtomicBool,
        materialization_failure_observed: AtomicBool,
        foreign_database_batch_write: AtomicBool,
        wal_sync_attempts: AtomicUsize,
        foreign_wal_sync: AtomicBool,
        caller_thread: std::thread::ThreadId,
        wal_read_changed: Condvar,
        wal_read_lock: Mutex<()>,
    }

    impl PipelineProbeState {
        fn new(mode: usize) -> Self {
            Self {
                mode: AtomicUsize::new(mode),
                armed: AtomicBool::new(false),
                database_batch_writes: AtomicUsize::new(0),
                wal_read_bytes: AtomicUsize::new(0),
                overlap_observed: AtomicBool::new(false),
                materialization_failure_observed: AtomicBool::new(false),
                foreign_database_batch_write: AtomicBool::new(false),
                wal_sync_attempts: AtomicUsize::new(0),
                foreign_wal_sync: AtomicBool::new(false),
                caller_thread: std::thread::current().id(),
                wal_read_changed: Condvar::new(),
                wal_read_lock: Mutex::new(()),
            }
        }

        fn arm(&self) {
            self.database_batch_writes.store(0, Ordering::Relaxed);
            self.wal_read_bytes.store(0, Ordering::Relaxed);
            self.overlap_observed.store(false, Ordering::Relaxed);
            self.materialization_failure_observed
                .store(false, Ordering::Relaxed);
            self.foreign_database_batch_write
                .store(false, Ordering::Relaxed);
            self.wal_sync_attempts.store(0, Ordering::Relaxed);
            self.foreign_wal_sync.store(false, Ordering::Relaxed);
            self.armed.store(true, Ordering::Release);
        }
    }

    #[derive(Debug)]
    struct PipelineProbeVfs {
        inner: Arc<dyn Vfs>,
        state: Arc<PipelineProbeState>,
    }

    impl Vfs for PipelineProbeVfs {
        fn open(
            &self,
            path: &Path,
            mode: OpenMode,
            kind: FileKind,
        ) -> crate::Result<Arc<dyn VfsFile>> {
            Ok(Arc::new(PipelineProbeFile {
                inner: self.inner.open(path, mode, kind)?,
                state: Arc::clone(&self.state),
            }))
        }

        fn file_exists(&self, path: &Path) -> crate::Result<bool> {
            self.inner.file_exists(path)
        }

        fn remove_file(&self, path: &Path) -> crate::Result<()> {
            self.inner.remove_file(path)
        }

        fn canonicalize_path(&self, path: &Path) -> crate::Result<PathBuf> {
            self.inner.canonicalize_path(path)
        }

        fn is_memory(&self) -> bool {
            self.inner.is_memory()
        }

        fn supports_file_locks(&self) -> bool {
            self.inner.supports_file_locks()
        }
    }

    #[derive(Debug)]
    struct PipelineProbeFile {
        inner: Arc<dyn VfsFile>,
        state: Arc<PipelineProbeState>,
    }

    impl VfsFile for PipelineProbeFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
            if self.inner.kind() == FileKind::Wal
                && self.state.armed.load(Ordering::Acquire)
                && self.state.mode.load(Ordering::Relaxed) == PIPELINE_PROBE_MATERIALIZE_ERROR
                && self.state.database_batch_writes.load(Ordering::Acquire) > 0
            {
                self.state
                    .materialization_failure_observed
                    .store(true, Ordering::Release);
                self.state.wal_read_changed.notify_all();
                return Err(crate::DbError::io(
                    "injected pipelined checkpoint WAL materialization failure",
                    std::io::Error::other("injected pipeline WAL read failure"),
                ));
            }
            let read = self.inner.read_at(offset, buf)?;
            if self.inner.kind() == FileKind::Wal && self.state.armed.load(Ordering::Acquire) {
                self.state.wal_read_bytes.fetch_add(read, Ordering::Relaxed);
                self.state.wal_read_changed.notify_all();
            }
            Ok(read)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
            if self.inner.kind() == FileKind::Database
                && self.state.armed.load(Ordering::Acquire)
                && buf.len() == COPYBACK_BATCH_BYTES
            {
                let write_number = self
                    .state
                    .database_batch_writes
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                if std::thread::current().id() != self.state.caller_thread {
                    self.state
                        .foreign_database_batch_write
                        .store(true, Ordering::Release);
                }
                match self.state.mode.load(Ordering::Relaxed) {
                    PIPELINE_PROBE_OVERLAP if write_number == 1 => {
                        let reads_before_write = self.state.wal_read_bytes.load(Ordering::Acquire);
                        let read_guard = self.state.wal_read_lock.lock().map_err(|_| {
                            crate::DbError::internal("pipeline probe WAL-read lock poisoned")
                        })?;
                        let (_read_guard, _) = self
                            .state
                            .wal_read_changed
                            .wait_timeout_while(read_guard, Duration::from_secs(2), |_| {
                                self.state.wal_read_bytes.load(Ordering::Acquire)
                                    <= reads_before_write
                            })
                            .map_err(|_| {
                                crate::DbError::internal("pipeline probe WAL-read lock poisoned")
                            })?;
                        if self.state.wal_read_bytes.load(Ordering::Acquire) > reads_before_write {
                            self.state.overlap_observed.store(true, Ordering::Release);
                        }
                    }
                    PIPELINE_PROBE_ERROR_SECOND_WRITE if write_number == 2 => {
                        return Err(crate::DbError::io(
                            "injected pipelined checkpoint database write failure",
                            std::io::Error::other("injected pipeline write failure"),
                        ));
                    }
                    PIPELINE_PROBE_PANIC_FIRST_WRITE if write_number == 1 => {
                        panic!("injected pipelined checkpoint worker panic");
                    }
                    PIPELINE_PROBE_MATERIALIZE_ERROR if write_number == 1 => {
                        let read_guard = self.state.wal_read_lock.lock().map_err(|_| {
                            crate::DbError::internal("pipeline probe WAL-read lock poisoned")
                        })?;
                        let (_read_guard, _) = self
                            .state
                            .wal_read_changed
                            .wait_timeout_while(read_guard, Duration::from_secs(2), |_| {
                                !self
                                    .state
                                    .materialization_failure_observed
                                    .load(Ordering::Acquire)
                            })
                            .map_err(|_| {
                                crate::DbError::internal("pipeline probe WAL-read lock poisoned")
                            })?;
                    }
                    _ => {}
                }
            }
            self.inner.write_at(offset, buf)
        }

        fn advise_sequential(&self) -> crate::Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> crate::Result<()> {
            if self.inner.kind() == FileKind::Wal && self.state.armed.load(Ordering::Acquire) {
                let attempt = self.state.wal_sync_attempts.fetch_add(1, Ordering::Relaxed) + 1;
                if std::thread::current().id() != self.state.caller_thread {
                    self.state.foreign_wal_sync.store(true, Ordering::Release);
                }
                match self.state.mode.load(Ordering::Relaxed) {
                    PIPELINE_PROBE_FINAL_SYNC_ERROR if attempt == 1 => {
                        return Err(crate::DbError::io(
                            "injected checkpoint worker final WAL sync failure",
                            std::io::Error::other("injected final WAL sync failure"),
                        ));
                    }
                    PIPELINE_PROBE_FINAL_SYNC_PANIC if attempt == 1 => {
                        panic!("injected checkpoint worker final WAL sync panic");
                    }
                    _ => {}
                }
            }
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> crate::Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> crate::Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> crate::Result<()> {
            self.inner.set_len(len)
        }

        fn try_lock_range(
            &self,
            offset: u64,
            len: u64,
            exclusive: bool,
        ) -> crate::Result<Option<Box<dyn VfsFileLock>>> {
            self.inner.try_lock_range(offset, len, exclusive)
        }
    }

    fn create_pipeline_probe_database(
        path: &Path,
        mode: usize,
        encrypted: bool,
    ) -> (Db, VfsHandle, Arc<PipelineProbeState>, u32, Vec<u8>) {
        let state = Arc::new(PipelineProbeState::new(mode));
        let vfs = VfsHandle::from_vfs(Arc::new(PipelineProbeVfs {
            inner: Arc::new(MemVfs::default()),
            state: Arc::clone(&state),
        }));
        let config = DbConfig {
            encryption: encrypted
                .then(|| DbEncryptionConfig::from_key_bytes([0xC7; 32]).expect("encryption key")),
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            release_freed_memory_after_checkpoint: false,
            ..DbConfig::default()
        };
        let db = Db::create_with_vfs(path, config, vfs.clone()).expect("create probe database");
        let page_size = db.config().page_size as usize;
        let pages_per_batch = COPYBACK_BATCH_BYTES / page_size;
        let last_page_id = 2 + (pages_per_batch as u32 * 2) + 3;
        let page = vec![0x6D; page_size];
        db.begin_write().expect("begin probe write");
        for page_id in 3..=last_page_id {
            db.write_page(page_id, &page).expect("stage probe page");
        }
        db.commit().expect("commit probe pages");
        (db, vfs, state, last_page_id, page)
    }

    /// Checkpoint copyback must durably sync the main database file before the WAL —
    /// the only other copy of the committed pages — is truncated (ADR 0004).
    #[test]
    fn checkpoint_syncs_db_file_before_wal_truncation() {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let wal_read_bytes = Arc::new(AtomicUsize::new(0));
        let db_sync_data = Arc::new(AtomicUsize::new(0));
        let wal_sync_data = Arc::new(AtomicUsize::new(0));
        let wal_sync_metadata = Arc::new(AtomicUsize::new(0));
        let sync_sequence = Arc::new(AtomicUsize::new(0));
        let db_sync_order = Arc::new(AtomicUsize::new(0));
        let wal_sync_order = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(crate::vfs::os::OsVfs),
            max_database_write,
            wal_reads,
            wal_read_bytes,
            db_sync_data: Arc::clone(&db_sync_data),
            wal_sync_data: Arc::clone(&wal_sync_data),
            wal_sync_metadata: Arc::clone(&wal_sync_metadata),
            sync_sequence: Arc::clone(&sync_sequence),
            db_sync_order: Arc::clone(&db_sync_order),
            wal_sync_order: Arc::clone(&wal_sync_order),
        }));
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-syncs-db-file.ddb");
        let db = Db::open_or_create_with_vfs(
            &path,
            DbConfig {
                // Disable threshold-based and background checkpoints so the
                // explicit checkpoint below is the only one that runs.
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
            vfs,
        )
        .expect("open db");
        db.execute("CREATE TABLE synced (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create table");
        db.execute("INSERT INTO synced VALUES (1, 'durable')")
            .expect("insert row");

        assert!(
            wal_sync_data.load(Ordering::Relaxed) >= 2,
            "Full commits, including initial WAL allocation growth, must use the data-and-length durability barrier"
        );
        assert_eq!(
            wal_sync_metadata.load(Ordering::Relaxed),
            0,
            "Full commits must not force recovery-irrelevant WAL inode metadata"
        );

        // Ignore open/bootstrap noise; only the checkpoint may sync the db file.
        db_sync_data.store(0, Ordering::Relaxed);
        wal_sync_data.store(0, Ordering::Relaxed);
        wal_sync_metadata.store(0, Ordering::Relaxed);
        sync_sequence.store(0, Ordering::Relaxed);
        db_sync_order.store(0, Ordering::Relaxed);
        wal_sync_order.store(0, Ordering::Relaxed);
        db.checkpoint_wal().expect("checkpoint");

        // The built-in VFS contract requires sync_data to persist both data
        // and file length before WAL discard (ADR 0004).
        assert!(
            db_sync_data.load(Ordering::Relaxed) >= 1,
            "checkpoint must sync the database file before discarding the WAL"
        );
        assert_eq!(
            wal_sync_data.load(Ordering::Relaxed),
            1,
            "fresh same-handle Full checkpoint should perform exactly one WAL data-and-length sync after commit counter reset"
        );
        assert_eq!(
            wal_sync_metadata.load(Ordering::Relaxed),
            0,
            "checkpoint truncate should not flush recovery-irrelevant WAL inode metadata"
        );
        let db_order = db_sync_order.load(Ordering::Relaxed);
        let wal_order = wal_sync_order.load(Ordering::Relaxed);
        assert!(
            db_order > 0 && wal_order > db_order,
            "main database must become durable before the WAL truncate barrier: db order {db_order}, WAL order {wal_order}"
        );
        let storage = db.storage_info().expect("storage info");
        assert_eq!(
            storage.wal_end_lsn, 0,
            "checkpoint with no readers should truncate the WAL"
        );
        let result = db
            .execute("SELECT body FROM synced WHERE id = 1")
            .expect("read committed row");
        assert_eq!(result.rows()[0].values(), &[Value::Text("durable".into())]);
    }

    #[test]
    fn final_coordination_publish_failure_refreshes_peer_and_allows_append() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        Db::clear_failpoints().expect("clear failpoints");
        struct FailpointCleanup;
        impl Drop for FailpointCleanup {
            fn drop(&mut self) {
                let _ = Db::clear_failpoints();
            }
        }
        let _cleanup = FailpointCleanup;

        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-final-publish-failure.ddb");
        let config = DbConfig {
            process_coordination: ProcessCoordinationMode::Required,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            release_freed_memory_after_checkpoint: false,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config.clone()).expect("create coordinated database");
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
            .expect("insert row");
        let old_end = db
            .storage_info()
            .expect("storage before checkpoint")
            .wal_end_lsn;

        // Required coordination first publishes checkpoint-lock ownership,
        // then the pre-reset checkpoint barrier with the old WAL end. Fail the
        // third coordination write, which is the final zero-end publication
        // after the zero header is durable and the physical file reclaimed.
        Db::install_failpoint("coord.write", "error", 3, 0)
            .expect("install final coordination publication failure");
        let error = db
            .checkpoint_wal()
            .expect_err("final coordination publication must fail");
        assert!(matches!(error, crate::DbError::Io { .. }));
        let local_storage = db.storage_info().expect("local storage after error");
        assert_eq!(local_storage.wal_end_lsn, 0);
        assert_eq!(local_storage.wal_file_size, WAL_HEADER_SIZE);

        Db::clear_failpoints().expect("clear publication failure");
        let coordination_vfs = VfsHandle::for_path(&path);
        let database_file = coordination_vfs
            .open(&path, OpenMode::OpenExisting, FileKind::Database)
            .expect("open database for coordination snapshot");
        let header = crate::storage::read_database_header_vfs(database_file.as_ref())
            .expect("read database header");
        let coordinator = crate::wal::coordination::ProcessCoordinator::open(
            &coordination_vfs,
            &path,
            &header,
            ProcessCoordinationMode::Required,
            config.process_coordination_timeout_ms,
        )
        .expect("open coordinator")
        .expect("required coordinator");
        assert_eq!(
            coordinator
                .snapshot()
                .expect("stale coordination snapshot")
                .wal_end_lsn,
            old_end,
            "failed final publication must leave the earlier checkpoint barrier visible"
        );
        drop(coordinator);

        // Force a separate WAL handle. Its acquisition trusts the durable WAL
        // header/database, republishes the recovered zero end, and reads the
        // checkpointed row without scanning the now-removed tail.
        crate::evict_shared_wal(&path).expect("evict shared WAL registry entry");
        let peer = Db::open(&path, config.clone()).expect("independent peer refresh");
        let first = peer
            .execute("SELECT body FROM t WHERE id = 1")
            .expect("peer reads checkpointed row");
        assert_eq!(
            first.rows()[0].values(),
            &[Value::Text("before-checkpoint".into())]
        );

        // The original handle refreshes the peer's reconciled coordination
        // state before appending at the new low WAL offsets.
        db.execute("INSERT INTO t VALUES (2, 'after-error')")
            .expect("same-handle append after final publication error");
        drop(peer);
        drop(db);
        crate::evict_shared_wal(&path).expect("evict before final reopen");
        let reopened = Db::open(&path, config).expect("final reopen");
        for (id, expected) in [(1, "before-checkpoint"), (2, "after-error")] {
            let result = reopened
                .execute(&format!("SELECT body FROM t WHERE id = {id}"))
                .expect("read row after final reopen");
            assert_eq!(result.rows()[0].values(), &[Value::Text(expected.into())]);
        }
    }

    #[test]
    fn local_full_commit_clears_external_tail_checkpoint_presync_marker() {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let wal_read_bytes = Arc::new(AtomicUsize::new(0));
        let db_sync_data = Arc::new(AtomicUsize::new(0));
        let wal_sync_data = Arc::new(AtomicUsize::new(0));
        let wal_sync_metadata = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(crate::vfs::os::OsVfs),
            max_database_write,
            wal_reads,
            wal_read_bytes,
            db_sync_data,
            wal_sync_data: Arc::clone(&wal_sync_data),
            wal_sync_metadata: Arc::clone(&wal_sync_metadata),
            sync_sequence: Arc::new(AtomicUsize::new(0)),
            db_sync_order: Arc::new(AtomicUsize::new(0)),
            wal_sync_order: Arc::new(AtomicUsize::new(0)),
        }));
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-external-tail-covered-by-full-commit.ddb");
        let writer_config = DbConfig {
            wal_sync_mode: crate::WalSyncMode::AsyncCommit {
                interval_ms: 60_000,
            },
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let full_config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let writer =
            Db::create_with_vfs(&path, writer_config, vfs.clone()).expect("create async writer");
        writer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create table");
        writer
            .execute("INSERT INTO t VALUES (1, 'external')")
            .expect("ack external async row");

        crate::evict_shared_wal(&path).expect("evict shared WAL for independent opener");
        let peer =
            Db::open_existing_with_vfs(&path, full_config, vfs).expect("open full-sync peer");
        peer.execute("INSERT INTO t VALUES (2, 'covering-sync')")
            .expect("local Full commit durably covers recovered tail");

        wal_sync_data.store(0, Ordering::Relaxed);
        wal_sync_metadata.store(0, Ordering::Relaxed);
        peer.checkpoint_wal().expect("checkpoint covered tail");
        assert_eq!(
            wal_sync_data.load(Ordering::Relaxed),
            1,
            "Full commit should clear external-tail uncertainty; checkpoint should only data-sync the WAL truncate"
        );
        assert_eq!(
            wal_sync_metadata.load(Ordering::Relaxed),
            0,
            "covered tail should not require a checkpoint metadata pre-sync"
        );
    }

    #[test]
    fn database_sync_failure_leaves_wal_recoverable() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        Db::clear_failpoints().expect("clear failpoint state");
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-db-sync-failure.ddb");
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE synced (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create table");
        db.execute("INSERT INTO synced VALUES (1, 'recoverable')")
            .expect("insert row");

        Db::install_failpoint("db.fsync", "error", 1, 0).expect("install database sync failpoint");
        let error = db
            .checkpoint_wal()
            .expect_err("database sync failure must fail checkpoint");
        assert!(matches!(error, crate::DbError::Io { .. }));
        let storage = db.storage_info().expect("storage after failed checkpoint");
        assert!(
            storage.wal_end_lsn > 0,
            "WAL must remain published when the database durability barrier fails"
        );
        Db::clear_failpoints().expect("clear failpoints before reopen");
        drop(db);

        let reopened = Db::open(&path, config).expect("recover from retained WAL");
        let result = reopened
            .execute("SELECT body FROM synced WHERE id = 1")
            .expect("read recovered row");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("recoverable".into())]
        );
    }

    #[test]
    fn checkpoint_copyback_writes_are_bounded() {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(MemVfs::default()),
            max_database_write: Arc::clone(&max_database_write),
            wal_reads,
            wal_read_bytes: Arc::new(AtomicUsize::new(0)),
            db_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_metadata: Arc::new(AtomicUsize::new(0)),
            sync_sequence: Arc::new(AtomicUsize::new(0)),
            db_sync_order: Arc::new(AtomicUsize::new(0)),
            wal_sync_order: Arc::new(AtomicUsize::new(0)),
        }));
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let path = Path::new(":bounded-checkpoint-copyback:");
        let db = Db::create_with_vfs(path, config, vfs).expect("create in-memory database");
        let page_size = db.config().page_size as usize;
        let pages_per_batch = COPYBACK_BATCH_BYTES / page_size;
        let last_page_id = 2 + pages_per_batch as u32 + 2;
        let page = vec![0xA5; page_size];
        db.begin_write().expect("begin write");
        for page_id in 3..=last_page_id {
            db.write_page(page_id, &page).expect("stage page");
        }
        db.commit().expect("commit pages");

        max_database_write.store(0, Ordering::Relaxed);
        db.checkpoint_wal().expect("checkpoint pages");
        assert_eq!(
            max_database_write.load(Ordering::Relaxed),
            COPYBACK_BATCH_BYTES,
            "copyback should fill one bounded batch without issuing a larger write"
        );
        assert_eq!(
            db.read_page(last_page_id)
                .expect("read final copied page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn large_contiguous_checkpoint_overlaps_materialization_and_copyback() {
        let (db, _vfs, state, last_page_id, page) = create_pipeline_probe_database(
            Path::new(":checkpoint-pipeline-overlap:"),
            PIPELINE_PROBE_OVERLAP,
            false,
        );
        state.arm();

        db.checkpoint_wal().expect("pipelined checkpoint");

        assert!(
            state.overlap_observed.load(Ordering::Acquire),
            "the main thread should read the next WAL batch while the worker is writing the first database batch"
        );
        assert!(
            state.database_batch_writes.load(Ordering::Relaxed) >= 2,
            "probe workload should exercise at least two full copyback batches"
        );
        assert_eq!(
            db.read_page(last_page_id)
                .expect("read final copied page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn pipelined_copyback_error_retains_wal_for_recovery() {
        let path = Path::new(":checkpoint-pipeline-error:");
        let (db, vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_ERROR_SECOND_WRITE, false);
        let config = db.config().clone();
        state.arm();

        let error = db
            .checkpoint_wal()
            .expect_err("second pipelined database write should fail checkpoint");
        assert!(matches!(error, crate::DbError::Io { .. }));
        assert!(
            db.storage_info()
                .expect("storage after failed copyback")
                .wal_end_lsn
                > 0,
            "WAL must remain published after a copyback failure"
        );

        state.mode.store(PIPELINE_PROBE_PASS, Ordering::Relaxed);
        drop(db);
        let reopened = Db::open_existing_with_vfs(path, config, vfs).expect("recover retained WAL");
        assert_eq!(
            reopened
                .read_page(last_page_id)
                .expect("read recovered final page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn pipelined_copyback_worker_panic_is_reconciled_before_return() {
        let path = Path::new(":checkpoint-pipeline-panic:");
        let (db, _vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_PANIC_FIRST_WRITE, false);
        state.arm();

        let error = db
            .checkpoint_wal()
            .expect_err("copyback worker panic should fail checkpoint");
        assert!(matches!(error, crate::DbError::Internal { .. }));
        assert!(error.to_string().contains("checkpoint I/O worker panicked"));
        assert!(
            db.storage_info()
                .expect("storage after worker panic")
                .wal_end_lsn
                > 0,
            "WAL must remain published after a worker panic"
        );

        // The scoped worker has been joined and the checkpoint gates have
        // reconciled, so the same handle can safely retry.
        state.mode.store(PIPELINE_PROBE_PASS, Ordering::Relaxed);
        state.arm();
        db.checkpoint_wal().expect("checkpoint retry after panic");
        assert_eq!(
            db.storage_info()
                .expect("storage after successful retry")
                .wal_end_lsn,
            0
        );
        assert_eq!(
            db.read_page(last_page_id)
                .expect("read final page after retry")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn materialization_error_with_outstanding_copyback_retains_wal() {
        let path = Path::new(":checkpoint-pipeline-materialize-error:");
        let (db, vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_MATERIALIZE_ERROR, false);
        let config = db.config().clone();
        state.arm();

        let error = db
            .checkpoint_wal()
            .expect_err("WAL read failure should stop pipelined materialization");
        assert!(matches!(error, crate::DbError::Io { .. }));
        assert!(
            state
                .materialization_failure_observed
                .load(Ordering::Acquire),
            "failure should occur after the first copyback batch is outstanding"
        );
        assert!(
            db.storage_info()
                .expect("storage after materialization failure")
                .wal_end_lsn
                > 0,
            "WAL must remain published after materialization failure"
        );

        state.mode.store(PIPELINE_PROBE_PASS, Ordering::Relaxed);
        drop(db);
        let reopened = Db::open_existing_with_vfs(path, config, vfs).expect("recover retained WAL");
        assert_eq!(
            reopened
                .read_page(last_page_id)
                .expect("read recovered final page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn io_worker_spawn_failure_falls_back_before_copyback() {
        struct ResetSpawnFailure;
        impl Drop for ResetSpawnFailure {
            fn drop(&mut self) {
                crate::wal::checkpoint::force_io_worker_spawn_failure_for_current_thread(false);
            }
        }

        let (db, _vfs, state, last_page_id, page) = create_pipeline_probe_database(
            Path::new(":checkpoint-pipeline-spawn-fallback:"),
            PIPELINE_PROBE_PASS,
            false,
        );
        state.arm();
        crate::wal::checkpoint::force_io_worker_spawn_failure_for_current_thread(true);
        let _reset = ResetSpawnFailure;

        db.checkpoint_wal()
            .expect("sequential fallback after worker spawn failure");

        assert!(
            !state.foreign_database_batch_write.load(Ordering::Acquire),
            "fallback copyback should run entirely on the checkpoint caller"
        );
        assert_eq!(
            db.read_page(last_page_id)
                .expect("read fallback-copied final page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn encrypted_checkpoint_uses_bounded_pipeline_and_reopens() {
        let path = Path::new(":checkpoint-pipeline-encrypted:");
        let (db, vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_PASS, true);
        let config = db.config().clone();
        state.arm();

        db.checkpoint_wal().expect("encrypted pipelined checkpoint");
        assert!(
            state.foreign_database_batch_write.load(Ordering::Acquire),
            "TDE database batch writes should execute on the I/O worker"
        );
        assert!(
            state.foreign_wal_sync.load(Ordering::Acquire),
            "the same I/O worker should perform the final WAL sync"
        );

        drop(db);
        let reopened = Db::open_existing_with_vfs(path, config, vfs).expect("reopen encrypted db");
        assert_eq!(
            reopened
                .read_page(last_page_id)
                .expect("read encrypted final page")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn io_worker_final_sync_error_is_reconciled_after_logical_reset() {
        let path = Path::new(":checkpoint-pipeline-final-sync-error:");
        let (db, vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_FINAL_SYNC_ERROR, false);
        let config = db.config().clone();
        state.arm();

        let error = db
            .checkpoint_wal()
            .expect_err("final worker WAL sync failure should be returned");
        assert!(matches!(error, crate::DbError::Io { .. }));
        assert!(state.foreign_wal_sync.load(Ordering::Acquire));
        let storage = db.storage_info().expect("storage after final sync error");
        assert_eq!(
            storage.wal_end_lsn, 0,
            "late sync errors happen after local logical WAL reconciliation"
        );
        state.mode.store(PIPELINE_PROBE_PASS, Ordering::Relaxed);
        drop(db);
        let reopened = Db::open_existing_with_vfs(path, config, vfs)
            .expect("reopen after injected final sync failure");
        assert_eq!(
            reopened
                .read_page(last_page_id)
                .expect("read final page after final sync error")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn io_worker_final_sync_panic_retries_sync_locally_before_return() {
        let path = Path::new(":checkpoint-pipeline-final-sync-panic:");
        let (db, vfs, state, last_page_id, page) =
            create_pipeline_probe_database(path, PIPELINE_PROBE_FINAL_SYNC_PANIC, false);
        let config = db.config().clone();
        state.arm();

        let error = db
            .checkpoint_wal()
            .expect_err("final worker WAL sync panic should be returned");
        assert!(matches!(error, crate::DbError::Internal { .. }));
        assert!(error.to_string().contains("checkpoint I/O worker panicked"));
        assert!(state.foreign_wal_sync.load(Ordering::Acquire));
        assert!(
            state.wal_sync_attempts.load(Ordering::Acquire) >= 2,
            "uncertain worker sync must be retried on the checkpoint caller"
        );
        assert_eq!(
            db.storage_info()
                .expect("storage after final sync panic")
                .wal_end_lsn,
            0
        );

        state.mode.store(PIPELINE_PROBE_PASS, Ordering::Relaxed);
        drop(db);
        let reopened = Db::open_existing_with_vfs(path, config, vfs)
            .expect("reopen after locally retried final sync");
        assert_eq!(
            reopened
                .read_page(last_page_id)
                .expect("read final page after final sync panic")
                .as_ref(),
            page.as_slice()
        );
    }

    #[test]
    fn checkpoint_read_ahead_bounds_full_page_wal_reads() {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(MemVfs::default()),
            max_database_write,
            wal_reads: Arc::clone(&wal_reads),
            wal_read_bytes: Arc::new(AtomicUsize::new(0)),
            db_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_metadata: Arc::new(AtomicUsize::new(0)),
            sync_sequence: Arc::new(AtomicUsize::new(0)),
            db_sync_order: Arc::new(AtomicUsize::new(0)),
            wal_sync_order: Arc::new(AtomicUsize::new(0)),
        }));
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            release_freed_memory_after_checkpoint: false,
            ..DbConfig::default()
        };
        let path = Path::new(":checkpoint-read-ahead:");
        let db = Db::create_with_vfs(path, config, vfs).expect("create in-memory database");
        let page_size = db.config().page_size as usize;
        let first_page_id = 3;
        let page_count = 160_u32;
        let last_page_id = first_page_id + page_count - 1;
        db.begin_write().expect("begin write");
        for page_id in first_page_id..=last_page_id {
            db.write_page(page_id, &vec![page_id as u8; page_size])
                .expect("stage full page");
        }
        db.commit().expect("commit full pages");

        wal_reads.store(0, Ordering::Relaxed);
        db.checkpoint_wal().expect("checkpoint full pages");
        assert!(
            wal_reads.load(Ordering::Relaxed) <= 3,
            "a 256 KiB window should read 160 consecutive 4 KiB WAL frames in at most three calls"
        );
        assert_eq!(
            db.read_page(last_page_id)
                .expect("read final copied page")
                .as_ref(),
            vec![last_page_id as u8; page_size].as_slice()
        );
    }

    fn assert_nonmonotonic_checkpoint_read_volume(path: &Path, commit_order: &[u32]) {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let wal_read_bytes = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(MemVfs::default()),
            max_database_write,
            wal_reads: Arc::clone(&wal_reads),
            wal_read_bytes: Arc::clone(&wal_read_bytes),
            db_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_metadata: Arc::new(AtomicUsize::new(0)),
            sync_sequence: Arc::new(AtomicUsize::new(0)),
            db_sync_order: Arc::new(AtomicUsize::new(0)),
            wal_sync_order: Arc::new(AtomicUsize::new(0)),
        }));
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            release_freed_memory_after_checkpoint: false,
            ..DbConfig::default()
        };
        let db = Db::create_with_vfs(path, config, vfs).expect("create in-memory database");
        let page_size = db.config().page_size as usize;
        for page_id in commit_order.iter().copied() {
            db.begin_write().expect("begin page transaction");
            db.write_page(page_id, &vec![page_id as u8; page_size])
                .expect("stage full page");
            db.commit().expect("commit full page");
        }

        wal_reads.store(0, Ordering::Relaxed);
        wal_read_bytes.store(0, Ordering::Relaxed);
        db.checkpoint_wal().expect("checkpoint non-monotonic WAL");

        let generous_bound = commit_order
            .len()
            .saturating_mul(page_size.saturating_add(128))
            .saturating_mul(3);
        assert!(
            wal_read_bytes.load(Ordering::Relaxed) <= generous_bound,
            "non-monotonic checkpoint read too much WAL: {} bytes in {} calls, bound {generous_bound}",
            wal_read_bytes.load(Ordering::Relaxed),
            wal_reads.load(Ordering::Relaxed)
        );
        for page_id in commit_order.iter().copied() {
            assert_eq!(
                db.read_page(page_id).expect("read copied page").as_ref(),
                vec![page_id as u8; page_size].as_slice()
            );
        }
    }

    #[test]
    fn checkpoint_read_ahead_does_not_overread_backward_wal_offsets() {
        let commit_order: Vec<u32> = (3..99).rev().collect();
        assert_nonmonotonic_checkpoint_read_volume(
            Path::new(":checkpoint-backward-read-ahead:"),
            &commit_order,
        );
    }

    #[test]
    fn checkpoint_read_ahead_does_not_overread_shuffled_wal_offsets() {
        let commit_order: Vec<u32> = (0..96).map(|index| 3 + (index * 37) % 96).collect();
        assert_nonmonotonic_checkpoint_read_volume(
            Path::new(":checkpoint-shuffled-read-ahead:"),
            &commit_order,
        );
    }

    #[test]
    fn checkpoint_read_ahead_rejects_corrupt_full_page_frame_type() {
        let max_database_write = Arc::new(AtomicUsize::new(0));
        let wal_reads = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(MaxWriteVfs {
            inner: Arc::new(MemVfs::default()),
            max_database_write,
            wal_reads,
            wal_read_bytes: Arc::new(AtomicUsize::new(0)),
            db_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_data: Arc::new(AtomicUsize::new(0)),
            wal_sync_metadata: Arc::new(AtomicUsize::new(0)),
            sync_sequence: Arc::new(AtomicUsize::new(0)),
            db_sync_order: Arc::new(AtomicUsize::new(0)),
            wal_sync_order: Arc::new(AtomicUsize::new(0)),
        }));
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            release_freed_memory_after_checkpoint: false,
            ..DbConfig::default()
        };
        let path = Path::new(":checkpoint-corrupt-read-ahead:");
        let db = Db::create_with_vfs(path, config, vfs.clone()).expect("create database");
        let page_id = 3;
        db.begin_write().expect("begin write");
        db.write_page(page_id, &vec![0x5A; db.config().page_size as usize])
            .expect("stage full page");
        db.commit().expect("commit full page");

        let mut wal_path = path.as_os_str().to_os_string();
        wal_path.push(".wal");
        let wal_file = vfs
            .open(Path::new(&wal_path), OpenMode::OpenExisting, FileKind::Wal)
            .expect("open WAL for corruption");
        assert_eq!(
            wal_file
                .write_at(WAL_HEADER_SIZE, &[FrameType::PageDelta as u8])
                .expect("corrupt frame type"),
            1
        );

        let error = db
            .checkpoint_wal()
            .expect_err("checkpoint must reject the corrupt indexed full-page frame");
        assert!(matches!(error, crate::DbError::Corruption { .. }));
        assert!(error.to_string().contains("expected a full Page frame"));
    }

    /// Checkpoint copyback coalesces dirty pages sorted by page id into a
    /// small number of contiguous `pwrite`s. This exercises the path with
    /// enough rows to span many btree leaf/interior pages, then verifies every
    /// committed row survives a checkpoint + reopen (which reads straight from
    /// the main db file, exercising the copyback output).
    #[test]
    fn checkpoint_copyback_preserves_all_rows_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-copyback-reopen.ddb");
        let db = Db::open_or_create(
            &path,
            DbConfig {
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
        )
        .expect("open db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .expect("create table");
        {
            let mut txn = db.transaction().expect("begin");
            let insert = txn
                .prepare("INSERT INTO t (id, v) VALUES ($1, $2)")
                .expect("prepare");
            let mut batch = txn.prepared_batch(&insert, 2).expect("batch");
            for i in 1..=20_000i64 {
                batch
                    .execute_mut(&mut [Value::Int64(i), Value::Int64(i.wrapping_mul(3))])
                    .expect("insert");
            }
            txn.commit().expect("commit");
        }
        db.checkpoint_wal().expect("checkpoint");
        let storage = db.storage_info().expect("storage info");
        assert_eq!(
            storage.wal_end_lsn, 0,
            "WAL should be truncated after checkpoint"
        );
        drop(db);

        // Reopen reads directly from the main db file (no WAL), so any
        // copyback ordering or batching bug would surface as missing/wrong rows.
        let db = Db::open(&path, DbConfig::default()).expect("reopen");
        let result = db.execute("SELECT COUNT(*), SUM(v) FROM t").expect("count");
        let row = result.rows().first().expect("row");
        assert_eq!(row.values()[0], Value::Int64(20_000));
        // sum_{i=1..20000} 3i = 3 * 20000*20001/2 = 600_030_000
        assert_eq!(row.values()[1], Value::Int64(600_030_000));
        // Spot-check boundaries and a middle row.
        let r = db
            .execute("SELECT v FROM t WHERE id = 20000")
            .expect("last");
        assert_eq!(r.rows()[0].values(), &[Value::Int64(60_000)]);
        let r = db.execute("SELECT v FROM t WHERE id = 1").expect("first");
        assert_eq!(r.rows()[0].values(), &[Value::Int64(3)]);
        let r = db
            .execute("SELECT v FROM t WHERE id = 10000")
            .expect("middle");
        assert_eq!(r.rows()[0].values(), &[Value::Int64(30_000)]);
    }
}
