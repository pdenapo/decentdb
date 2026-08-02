//! WAL recovery and index rebuild logic.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::delta::apply_page_delta_in_place;
use super::format::{
    FrameEncoding, FrameType, WalFrame, WalHeader, WAL_HEADER_SIZE, WAL_HEADER_SIZE_USIZE,
};
use super::index::{WalIndex, WalVersion};
use super::index_sidecar::WalIndexSidecar;

const MAX_PENDING_RECOVERY_FRAMES: usize = 1_000_000;
const RECOVERY_PENDING_OVERFLOW_MESSAGE: &str =
    "WAL recovery aborted: more than 1,000,000 uncommitted page frames before commit";

#[derive(Debug)]
struct PendingRecoveryPage {
    data: Vec<u8>,
    lsn: u64,
    wal_offset: u64,
    frame_len: u32,
    encoding: FrameEncoding,
}

pub(crate) fn initialize_or_recover(
    file: &Arc<dyn VfsFile>,
    pager: &PagerHandle,
    page_size: u32,
    hot_set_pages: u32,
    sidecar: Option<&mut WalIndexSidecar>,
) -> Result<(WalIndex, u64, u32)> {
    let size = file.file_size()?;
    if size == 0 {
        let header = WalHeader::new(page_size, 0);
        write_all_at(file.as_ref(), 0, &header.encode())?;
        file.set_len(WAL_HEADER_SIZE)?;
        return Ok((WalIndex::default(), 0, 0));
    }

    if size < WAL_HEADER_SIZE {
        return Err(DbError::corruption(format!(
            "WAL file {} is shorter than the fixed header",
            file.path().display()
        )));
    }

    let mut header_bytes = [0_u8; WAL_HEADER_SIZE_USIZE];
    read_exact_at(file.as_ref(), 0, &mut header_bytes)?;
    let header = WalHeader::decode(&header_bytes)?;
    if header.page_size != page_size {
        return Err(DbError::corruption(format!(
            "WAL page size {} does not match database page size {}",
            header.page_size, page_size
        )));
    }
    if header.wal_end_offset > size {
        return Err(DbError::corruption(format!(
            "WAL logical end offset {} exceeds file size {}",
            header.wal_end_offset, size
        )));
    }

    let mut index = WalIndex::default();
    let mut max_page_id = 0;
    let mut offset = WAL_HEADER_SIZE;
    let mut pending = Vec::<PageId>::new();
    let mut pending_pages = HashMap::<PageId, PendingRecoveryPage>::new();
    while offset < header.wal_end_offset {
        let Some(frame) =
            WalFrame::decode_from_file(file.as_ref(), offset, page_size, header.wal_end_offset)?
        else {
            break;
        };
        let frame_len = frame.encoded_len(page_size) as u32;
        let next_offset = offset + frame_len as u64;
        match frame.frame_type {
            FrameType::Page => {
                max_page_id = max_page_id.max(frame.page_id);
                push_pending_frame(&mut pending, frame.page_id)?;
                store_pending_page(
                    &mut pending_pages,
                    frame.page_id,
                    frame.payload,
                    next_offset,
                    offset,
                    frame_len,
                    FrameEncoding::Page,
                )?;
            }
            FrameType::PageDelta => {
                max_page_id = max_page_id.max(frame.page_id);
                push_pending_frame(&mut pending, frame.page_id)?;
                if let Some(pending_page) = pending_pages.get_mut(&frame.page_id) {
                    apply_page_delta_in_place(&mut pending_page.data, &frame.payload)?;
                    pending_page.lsn = next_offset;
                    pending_page.wal_offset = offset;
                    pending_page.frame_len = frame_len;
                    pending_page.encoding = FrameEncoding::PageDelta;
                } else {
                    let mut data =
                        if let Some(version) = index.latest_visible(frame.page_id, u64::MAX) {
                            version.payload.as_slice().to_vec()
                        } else {
                            pager.read_page(frame.page_id)?.to_vec()
                        };
                    apply_page_delta_in_place(&mut data, &frame.payload)?;
                    store_pending_page(
                        &mut pending_pages,
                        frame.page_id,
                        data,
                        next_offset,
                        offset,
                        frame_len,
                        FrameEncoding::PageDelta,
                    )?;
                }
            }
            FrameType::Commit => {
                pending.clear();
                for (page_id, pending_page) in pending_pages.drain() {
                    // Recovery runs before any readers can hold snapshots, so we only need
                    // the latest recovered version per page. Retaining full historical
                    // versions from a large WAL can consume substantial memory on open.
                    index.add_version(
                        page_id,
                        WalVersion::resident(
                            pending_page.lsn,
                            pending_page.wal_offset,
                            pending_page.frame_len,
                            pending_page.encoding,
                            Arc::from(pending_page.data),
                        ),
                        false,
                    );
                }
            }
            FrameType::Checkpoint => {
                pending.clear();
                pending_pages.clear();
            }
        }
        offset = next_offset;
    }

    // Keep every latest recovered page resident until replay is complete.
    // A later PageDelta may use a page from an earlier committed transaction
    // as its base; spilling that page mid-replay would make recovery fall back
    // to a stale main-database image. The sidecar is only a post-replay hot-set
    // projection, never part of delta reconstruction.
    spill_recovered_latest_versions(&mut index, hot_set_pages, sidecar)?;

    Ok((index, header.wal_end_offset, max_page_id))
}

fn spill_recovered_latest_versions(
    index: &mut WalIndex,
    hot_set_pages: u32,
    mut sidecar: Option<&mut WalIndexSidecar>,
) -> Result<()> {
    let Some(sidecar) = sidecar.as_mut() else {
        return Ok(());
    };
    let hot_set_pages = usize::try_from(hot_set_pages).unwrap_or(usize::MAX);
    while let Some((page_id, version)) = index.spill_one_cold_latest(hot_set_pages) {
        sidecar.write_latest(page_id, &version)?;
    }
    Ok(())
}

pub(crate) fn persist_header(
    file: &Arc<dyn VfsFile>,
    page_size: u32,
    wal_end_offset: u64,
) -> Result<()> {
    let header = WalHeader::new(page_size, wal_end_offset);
    write_all_at(file.as_ref(), 0, &header.encode())
}

fn push_pending_frame(pending: &mut Vec<PageId>, page_id: PageId) -> Result<()> {
    if pending.len() >= MAX_PENDING_RECOVERY_FRAMES {
        return Err(recovery_pending_overflow_error());
    }
    pending.push(page_id);
    Ok(())
}

fn store_pending_page(
    pending_pages: &mut HashMap<PageId, PendingRecoveryPage>,
    page_id: PageId,
    data: Vec<u8>,
    lsn: u64,
    wal_offset: u64,
    frame_len: u32,
    encoding: FrameEncoding,
) -> Result<()> {
    let at_capacity = pending_pages.len() >= MAX_PENDING_RECOVERY_FRAMES;
    match pending_pages.entry(page_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let pending_page = entry.get_mut();
            pending_page.data = data;
            pending_page.lsn = lsn;
            pending_page.wal_offset = wal_offset;
            pending_page.frame_len = frame_len;
            pending_page.encoding = encoding;
            Ok(())
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            if at_capacity {
                return Err(recovery_pending_overflow_error());
            }
            entry.insert(PendingRecoveryPage {
                data,
                lsn,
                wal_offset,
                frame_len,
                encoding,
            });
            Ok(())
        }
    }
}

fn recovery_pending_overflow_error() -> DbError {
    DbError::corruption(RECOVERY_PENDING_OVERFLOW_MESSAGE)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use crate::config::{DbConfig, ProcessCoordinationMode, WalSyncMode};
    use crate::storage::page;
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::{faulty, write_all_at, FileKind, OpenMode, Vfs, VfsHandle};
    use crate::{Db, DbError, Value};

    use super::{initialize_or_recover, persist_header, MAX_PENDING_RECOVERY_FRAMES};
    use crate::wal::format::{WalHeader, WAL_HEADER_SIZE};
    use crate::wal::WalHandle;

    fn test_pager(vfs: &crate::vfs::mem::MemVfs, path: &Path) -> PagerHandle {
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap db");
        PagerHandle::open(Arc::clone(&file), header, 1).expect("open pager")
    }

    fn wait_for_test_path(path: &Path, label: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            assert!(Instant::now() < deadline, "timed out waiting for {label}");
            std::thread::yield_now();
        }
    }

    #[test]
    fn corrupt_wal_header_is_reported() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        let bad_header = [0_u8; 32];
        write_all_at(file.as_ref(), 0, &bad_header).expect("write corrupt header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
            .expect_err("header is corrupt");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn empty_wal_is_initialized_header_only() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        let (index, end, max_page_id) =
            initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
                .expect("initialize wal");

        assert_eq!(index.version_count(), 0);
        assert_eq!(end, 0);
        assert_eq!(max_page_id, 0);

        let mut header_bytes = [0_u8; 32];
        crate::vfs::read_exact_at(file.as_ref(), 0, &mut header_bytes).expect("read header");
        let header = WalHeader::decode(&header_bytes).expect("decode wal header");
        assert_eq!(header.page_size, page::DEFAULT_PAGE_SIZE);
    }

    struct FailpointCleanup;

    impl Drop for FailpointCleanup {
        fn drop(&mut self) {
            let _ = faulty::clear_failpoints();
        }
    }

    #[test]
    fn checkpoint_no_longer_emits_transient_checkpoint_frame() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("no-checkpoint-frame.ddb");
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config).expect("create db");
        db.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'durable')")
            .expect("insert row");

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.write_checkpoint".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install checkpoint-frame sentinel");
        db.checkpoint_wal()
            .expect("checkpoint should not emit a checkpoint WAL frame");
        let logs = faulty::failpoint_logs().expect("failpoint log");
        assert!(
            logs.iter()
                .all(|entry| entry.label != "wal.write_checkpoint"),
            "normal destructive checkpoint emitted a legacy checkpoint frame: {logs:?}"
        );
    }

    #[test]
    fn checkpoint_truncate_write_failure_leaves_wal_recoverable() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("truncate-write-failure.ddb");
        let config = DbConfig {
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'recoverable')")
            .expect("insert row");

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.write_header".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install WAL truncate-header failure");
        let error = db
            .checkpoint_wal()
            .expect_err("truncate header write failure must fail checkpoint");
        assert!(matches!(error, DbError::Io { .. }));

        faulty::clear_failpoints().expect("clear failpoints before reopen");
        drop(db);
        let reopened = Db::open(&path, config).expect("recover from retained WAL");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("read recovered row");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("recoverable".into())]
        );
    }

    #[test]
    fn checkpoint_truncate_sync_failure_remains_reopenable() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("truncate-sync-failure.ddb");
        let config = DbConfig {
            wal_sync_mode: WalSyncMode::Full,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'main-db-copy')")
            .expect("insert row");

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install WAL truncate sync failure");
        let error = db
            .checkpoint_wal()
            .expect_err("truncate data-and-length sync failure must fail checkpoint");
        assert!(matches!(error, DbError::Io { .. }));

        faulty::clear_failpoints().expect("clear failpoints before reopen");
        drop(db);
        let reopened = Db::open(&path, config).expect("database remains reopenable");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("read checkpointed row");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("main-db-copy".into())]
        );
    }

    #[test]
    fn checkpoint_truncate_sync_failure_same_handle_can_append_after_error() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("truncate-sync-same-handle.ddb");
        let config = DbConfig {
            wal_sync_mode: WalSyncMode::Full,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'checkpointed')")
            .expect("insert first row");

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install WAL truncate sync failure");
        let error = db
            .checkpoint_wal()
            .expect_err("late truncate sync failure must be surfaced");
        assert!(matches!(error, DbError::Io { .. }));

        faulty::clear_failpoints().expect("clear failpoints before append");
        db.execute("INSERT INTO t VALUES (2, 'post-error')")
            .expect("same handle appends after reconciled truncate state");
        let result = db
            .execute("SELECT COUNT(*) FROM t")
            .expect("count rows after append");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

        drop(db);
        let reopened = Db::open(&path, config).expect("reopen after same-handle append");
        let result = reopened
            .execute("SELECT COUNT(*) FROM t")
            .expect("count reopened rows");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    }

    #[test]
    fn checkpoint_truncate_sync_failure_refreshes_already_open_independent_handle() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("truncate-sync-independent-handle.ddb");
        let config = DbConfig {
            wal_sync_mode: WalSyncMode::Full,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let db1 = Db::create(&path, config.clone()).expect("create db1");
        db1.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        db1.execute("INSERT INTO t VALUES (1, 'checkpointed')")
            .expect("insert first row");

        crate::evict_shared_wal(&path).expect("evict shared WAL to simulate independent opener");
        let db2 = Db::open(&path, config.clone()).expect("open independent db2");
        let result = db2
            .execute("SELECT COUNT(*) FROM t")
            .expect("db2 sees pre-checkpoint WAL");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);

        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install WAL truncate sync failure");
        let error = db1
            .checkpoint_wal()
            .expect_err("late truncate sync failure must be surfaced");
        assert!(matches!(error, DbError::Io { .. }));

        faulty::clear_failpoints().expect("clear failpoints before db2 refresh");
        let result = db2
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("db2 refreshes to checkpoint generation");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("checkpointed".into())]
        );
        db2.execute("INSERT INTO t VALUES (2, 'post-error')")
            .expect("independent handle appends after refresh");

        drop(db1);
        drop(db2);
        let reopened = Db::open(&path, config).expect("reopen after independent append");
        let result = reopened
            .execute("SELECT COUNT(*) FROM t")
            .expect("count reopened rows");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    }

    #[test]
    fn independent_checkpoint_syncs_externally_acknowledged_async_tail_before_copyback() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("external-async-tail-checkpoint.ddb");
        let writer_config = DbConfig {
            wal_sync_mode: WalSyncMode::AsyncCommit {
                interval_ms: 60_000,
            },
            paged_row_storage: false,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let checkpoint_config = DbConfig {
            wal_sync_mode: WalSyncMode::Full,
            process_coordination: ProcessCoordinationMode::Required,
            paged_row_storage: false,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let writer = Db::create(&path, writer_config).expect("create async writer");
        writer
            .execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("create table");
        writer
            .execute("INSERT INTO t VALUES (1, 'async-tail')")
            .expect("ack async insert without waiting for flusher");

        crate::evict_shared_wal(&path).expect("evict shared WAL to simulate external opener");
        let checkpointer = Db::open(&path, checkpoint_config).expect("open independent checkpoint");
        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install forced external-tail sync failure");
        faulty::install_failpoint(faulty::Failpoint {
            label: "db.write_page".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install database-copyback sentinel");
        let error = checkpointer
            .checkpoint_wal()
            .expect_err("external async WAL tail must be synced before copyback");
        assert!(matches!(error, DbError::Io { .. }));
        let logs = faulty::failpoint_logs().expect("failpoint logs");
        assert!(
            logs.iter()
                .any(|entry| entry.label == "wal.fsync" && entry.outcome == "error"),
            "checkpoint did not force-sync the externally recovered async WAL tail: {logs:?}"
        );
        assert!(
            logs.iter().all(|entry| entry.label != "db.write_page"),
            "checkpoint copied database pages before external WAL tail sync completed: {logs:?}"
        );
    }

    #[test]
    fn child_process_checkpoint_syncs_externally_acknowledged_async_tail_before_copyback() {
        let _failpoint_guard = faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        faulty::clear_failpoints().expect("clear failpoints");
        let _cleanup = FailpointCleanup;
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("child-external-async-tail.ddb");
        let ready_path = tempdir.path().join("child-ready");
        let release_path = tempdir.path().join("child-release");
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("wal::recovery::tests::external_async_writer_child_helper")
            .arg("--nocapture")
            .env("DDB_EXTERNAL_ASYNC_CHILD_DB", &path)
            .env("DDB_EXTERNAL_ASYNC_CHILD_READY", &ready_path)
            .env("DDB_EXTERNAL_ASYNC_CHILD_RELEASE", &release_path)
            .spawn()
            .expect("spawn external async writer child");
        wait_for_test_path(&ready_path, "external async writer child");

        let checkpoint_config = DbConfig {
            wal_sync_mode: WalSyncMode::Full,
            process_coordination: ProcessCoordinationMode::Required,
            paged_row_storage: false,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let checkpointer = Db::open(&path, checkpoint_config).expect("open parent checkpoint db");
        let (latest_lsn, has_coordination, needs_tail_sync, locally_synced) =
            checkpointer.wal_checkpoint_tail_state_for_tests();
        assert!(
            latest_lsn > 0,
            "parent checkpoint handle recovered no WAL tail"
        );
        assert!(
            has_coordination,
            "parent checkpoint handle opened without process coordination"
        );
        assert!(
            needs_tail_sync,
            "parent checkpoint handle did not mark recovered WAL tail uncertain: latest_lsn={latest_lsn} has_coordination={has_coordination} needs_tail_sync={needs_tail_sync} locally_synced={locally_synced}"
        );
        assert!(
            !locally_synced,
            "parent checkpoint handle incorrectly treated recovered WAL tail as locally synced: latest_lsn={latest_lsn} has_coordination={has_coordination} needs_tail_sync={needs_tail_sync} locally_synced={locally_synced}"
        );
        faulty::install_failpoint(faulty::Failpoint {
            label: "wal.fsync".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install forced external-tail sync failure");
        faulty::install_failpoint(faulty::Failpoint {
            label: "db.write_page".to_string(),
            trigger_on: 1,
            action: faulty::FailAction::Error,
        })
        .expect("install database-copyback sentinel");
        let error = checkpointer
            .checkpoint_wal()
            .expect_err("child process async WAL tail must be synced before copyback");
        assert!(matches!(error, DbError::Io { .. }));
        let logs = faulty::failpoint_logs().expect("failpoint logs");
        assert!(
            logs.iter()
                .any(|entry| entry.label == "wal.fsync" && entry.outcome == "error"),
            "checkpoint did not force-sync the child process async WAL tail: {logs:?}"
        );
        assert!(
            logs.iter().all(|entry| entry.label != "db.write_page"),
            "checkpoint copied database pages before child WAL tail sync completed: {logs:?}"
        );

        std::fs::write(&release_path, []).expect("release external async writer child");
        let status = child.wait().expect("wait for child");
        assert!(
            status.success(),
            "external async writer child failed: {status}"
        );
    }

    #[test]
    fn external_async_writer_child_helper() {
        let Some(path) = std::env::var_os("DDB_EXTERNAL_ASYNC_CHILD_DB").map(PathBuf::from) else {
            return;
        };
        let ready_path = PathBuf::from(
            std::env::var_os("DDB_EXTERNAL_ASYNC_CHILD_READY")
                .expect("external async child ready path"),
        );
        let release_path = PathBuf::from(
            std::env::var_os("DDB_EXTERNAL_ASYNC_CHILD_RELEASE")
                .expect("external async child release path"),
        );
        let db = Db::create(
            &path,
            DbConfig {
                wal_sync_mode: WalSyncMode::AsyncCommit {
                    interval_ms: 60_000,
                },
                process_coordination: ProcessCoordinationMode::Required,
                paged_row_storage: false,
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
        )
        .expect("create child async db");
        db.execute("CREATE TABLE t(id INT64, val TEXT)")
            .expect("child create table");
        db.execute("INSERT INTO t VALUES (1, 'child-async-tail')")
            .expect("child ack async insert");
        std::fs::write(&ready_path, []).expect("publish child ready");
        wait_for_test_path(&release_path, "external async child release");
        drop(db);
    }

    #[test]
    fn wal_header_page_size_mismatch() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        // Write a header with a different page size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE * 2, 0);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
            .expect_err("mismatch");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn wal_header_end_offset_exceeds_file_size() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        // Set wal_end_offset to be larger than the actual file size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, WAL_HEADER_SIZE + 100);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
            .expect_err("end_exceeds");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn replay_committed_frames_populates_index() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let frames = vec![
            crate::wal::format::WalFrame::page(3, vec![0xAA; ps as usize]),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for f in &frames {
            data.extend_from_slice(&f.encode(ps).unwrap());
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, end, max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(index.version_count(), 1);
        assert_eq!(end, logical_end);
        assert_eq!(max_page_id, 3);
    }

    #[test]
    fn recovery_retains_only_latest_version_per_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let first = vec![0x11; ps as usize];
        let second = vec![0x22; ps as usize];
        let frames = vec![
            crate::wal::format::WalFrame::page(7, first),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(7, second.clone()),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(index.version_count(), 1);
        let latest = index
            .latest_visible(7, u64::MAX)
            .expect("latest version for page 7");
        assert_eq!(latest.payload.as_slice(), second.as_slice());
    }

    #[test]
    fn recovery_large_wal_replay_keeps_single_version_per_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let update_count = 1024_u32;
        let page_id = 42_u32;
        let mut frames = Vec::with_capacity((update_count as usize) * 2);
        for update in 0..update_count {
            frames.push(crate::wal::format::WalFrame::page(
                page_id,
                vec![(update % 251) as u8; ps as usize],
            ));
            frames.push(crate::wal::format::WalFrame::commit());
        }

        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(
            index.version_count(),
            1,
            "recovery should not retain full historical page versions",
        );
        let latest = index
            .latest_visible(page_id, u64::MAX)
            .expect("latest version for page");
        assert_eq!(
            latest.payload.as_slice()[0],
            ((update_count - 1) % 251) as u8
        );
    }

    #[test]
    fn recovery_spills_excess_full_page_versions_into_sidecar() {
        let mem_vfs = Arc::new(crate::vfs::mem::MemVfs::default());
        let vfs: Arc<dyn Vfs> = mem_vfs.clone();
        let handle = VfsHandle::from_vfs(vfs);
        let db_path = Path::new("spill-recovery.ddb");
        let wal_path = Path::new("spill-recovery.ddb.wal");
        let file = mem_vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&mem_vfs, db_path);

        let ps = page::DEFAULT_PAGE_SIZE;
        let frames = vec![
            crate::wal::format::WalFrame::page(7, vec![0x11; ps as usize]),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(8, vec![0x22; ps as usize]),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let mut sidecar = crate::wal::index_sidecar::WalIndexSidecar::open(&handle, db_path)
            .expect("open sidecar");
        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 1, Some(&mut sidecar)).expect("recover");

        assert_eq!(index.version_count(), 1);
        assert_eq!(sidecar.version_count(), 1);
        assert!(index.latest_visible(8, u64::MAX).is_some());
        let spilled = sidecar
            .read_latest(7)
            .expect("read spilled latest")
            .expect("page 7 should spill during recovery");
        assert!(matches!(
            spilled.payload,
            crate::wal::index::WalVersionPayload::OnDisk {
                encoding: crate::wal::format::FrameEncoding::Page,
                ..
            }
        ));
    }

    #[test]
    fn recovery_spills_excess_delta_versions_into_sidecar() {
        let mem_vfs = Arc::new(crate::vfs::mem::MemVfs::default());
        let vfs: Arc<dyn Vfs> = mem_vfs.clone();
        let handle = VfsHandle::from_vfs(vfs);
        let db_path = Path::new("spill-recovery-delta.ddb");
        let wal_path = Path::new("spill-recovery-delta.ddb.wal");
        let file = mem_vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&mem_vfs, db_path);

        let ps = page::DEFAULT_PAGE_SIZE;
        let base_page = vec![0x21; ps as usize];
        pager
            .write_page_direct(7, &base_page)
            .expect("seed base page for delta recovery");
        let mut updated_page = base_page.clone();
        updated_page[0] = 0x99;
        updated_page[11] = 0x42;
        let delta_payload =
            crate::wal::delta::encode_page_delta(&base_page, &updated_page).expect("delta payload");
        let frames = vec![
            crate::wal::format::WalFrame::page_delta(7, delta_payload),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(8, vec![0x22; ps as usize]),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let mut sidecar = crate::wal::index_sidecar::WalIndexSidecar::open(&handle, db_path)
            .expect("open sidecar");
        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 1, Some(&mut sidecar)).expect("recover");

        assert_eq!(index.version_count(), 1);
        assert_eq!(sidecar.version_count(), 1);
        assert!(index.latest_visible(8, u64::MAX).is_some());
        let spilled = sidecar
            .read_latest(7)
            .expect("read spilled latest")
            .expect("page 7 should spill during recovery");
        assert!(matches!(
            spilled.payload,
            crate::wal::index::WalVersionPayload::OnDisk {
                encoding: crate::wal::format::FrameEncoding::PageDelta,
                ..
            }
        ));
    }

    #[test]
    fn recovery_defers_hot_set_spill_until_later_delta_bases_are_applied() {
        let mem_vfs = Arc::new(crate::vfs::mem::MemVfs::default());
        let vfs: Arc<dyn Vfs> = mem_vfs.clone();
        let handle = VfsHandle::from_vfs(vfs);
        let db_path = Path::new("recovery-late-delta.ddb");
        let wal_path = Path::new("recovery-late-delta.ddb.wal");
        let file = mem_vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create WAL file");
        let pager = test_pager(&mem_vfs, db_path);
        let ps = page::DEFAULT_PAGE_SIZE;
        let page_a = vec![0x31; ps as usize];
        let page_b = vec![0x42; ps as usize];
        let mut page_c = page_a.clone();
        page_c[17] = 0x53;
        page_c[29] = 0x64;
        let delta_c = crate::wal::delta::encode_page_delta(&page_a, &page_c)
            .expect("encode late page-7 delta");
        let frames = vec![
            crate::wal::format::WalFrame::page(7, page_a),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(8, page_b.clone()),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page_delta(7, delta_c),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode recovery frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write WAL header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write WAL frames");
        file.set_len(logical_end).expect("set WAL length");

        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            wal_index_hot_set_pages: 1,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&handle, db_path, &cfg, &pager, None)
            .expect("recover with one-page hot set");
        assert_eq!(wal.latest_snapshot(), logical_end);
        assert_eq!(wal.version_count().expect("recovered versions"), 2);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, 7, logical_end)
                .expect("read recovered late delta")
                .expect("page 7 recovered")
                .as_ref(),
            page_c.as_slice()
        );

        wal.checkpoint(&pager, 0)
            .expect("checkpoint recovered late delta");
        assert_eq!(
            pager
                .read_page(7)
                .expect("read checkpointed late delta")
                .as_ref(),
            page_c.as_slice()
        );
        assert_eq!(
            pager
                .read_page(8)
                .expect("read checkpointed page 8")
                .as_ref(),
            page_b.as_slice()
        );
        drop(wal);

        let reopened = WalHandle::acquire(&handle, db_path, &cfg, &pager, None)
            .expect("reopen checkpointed recovery chain");
        assert_eq!(reopened.latest_snapshot(), 0);
        assert_eq!(reopened.version_count().expect("reopened versions"), 0);
        assert_eq!(
            pager
                .read_page(7)
                .expect("read reopened checkpointed delta")
                .as_ref(),
            page_c.as_slice()
        );
    }

    #[test]
    fn partial_frames_at_end_are_ignored() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = crate::wal::format::WalFrame::page(1, vec![0xBB; ps as usize]);
        let mut data = frame.encode(ps).unwrap();
        // Truncate the last byte to simulate a torn/partial write
        data.truncate(data.len() - 1);

        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write partial frame");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        // No committed frames -> index should be empty
        assert_eq!(index.version_count(), 0);
    }

    #[test]
    fn replay_page_delta_frames_populates_index_with_rebuilt_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let mut base = vec![0_u8; ps as usize];
        base[24..28].copy_from_slice(&11_u32.to_le_bytes());
        pager.write_page_direct(3, &base).expect("write base page");

        let mut updated = base.clone();
        updated[24..28].copy_from_slice(&27_u32.to_le_bytes());
        updated[96..104].copy_from_slice(b"manifest");
        let delta = crate::wal::delta::encode_page_delta(&base, &updated).expect("encode delta");

        let frames = vec![
            crate::wal::format::WalFrame::page_delta(3, delta),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        let version = index
            .latest_visible(3, u64::MAX)
            .expect("recovered page version");
        assert_eq!(version.payload.as_slice(), updated.as_slice());
    }

    #[test]
    fn legacy_checkpoint_frame_recovery_still_replays_later_full_and_delta_frames() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let base_page_id = 3;
        let full_page_id = 4;
        let base = vec![0x31; ps as usize];
        pager
            .write_page_direct(base_page_id, &base)
            .expect("write delta base page");
        let mut updated = base.clone();
        updated[16] = 0x42;
        updated[97] = 0x53;
        let delta = crate::wal::delta::encode_page_delta(&base, &updated).expect("encode delta");
        let full = vec![0x64; ps as usize];
        let ignored = vec![0xA5; ps as usize];

        let frames = vec![
            crate::wal::format::WalFrame::page(full_page_id, ignored),
            crate::wal::format::WalFrame::checkpoint(123),
            crate::wal::format::WalFrame::page(full_page_id, full.clone()),
            crate::wal::format::WalFrame::page_delta(base_page_id, delta),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, end, max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover legacy WAL");
        assert_eq!(end, logical_end);
        assert_eq!(max_page_id, full_page_id);
        assert_eq!(index.version_count(), 2);
        assert_eq!(
            index
                .latest_visible(full_page_id, u64::MAX)
                .expect("full page recovered")
                .payload
                .as_slice(),
            full.as_slice()
        );
        assert_eq!(
            index
                .latest_visible(base_page_id, u64::MAX)
                .expect("delta page recovered")
                .payload
                .as_slice(),
            updated.as_slice()
        );
    }

    #[test]
    fn recovery_many_deltas_to_same_page_does_not_clone_base_per_delta() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let page_id = 9_u32;
        let mut current = vec![0_u8; ps as usize];
        current[0..4].copy_from_slice(&1_u32.to_le_bytes());

        let mut frames = Vec::with_capacity(1026);
        frames.push(crate::wal::format::WalFrame::page(page_id, current.clone()));
        for update in 0_u32..1024_u32 {
            let mut next = current.clone();
            let byte_index = ((update as usize) * 31) % next.len();
            next[byte_index] = next[byte_index]
                .wrapping_add((update % 251) as u8)
                .wrapping_add(1);
            let delta =
                crate::wal::delta::encode_page_delta(&current, &next).expect("encode delta");
            frames.push(crate::wal::format::WalFrame::page_delta(page_id, delta));
            current = next;
        }
        let expected = current;
        frames.push(crate::wal::format::WalFrame::commit());

        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(index.version_count(), 1);
        let version = index
            .latest_visible(page_id, u64::MAX)
            .expect("latest version for page");
        assert_eq!(version.payload.as_slice(), expected.as_slice());
    }

    #[test]
    #[ignore = "requires generating a large synthetic WAL to exercise the hard overflow bound"]
    fn recovery_rejects_pending_overflow() {
        let tempdir = TempDir::new().expect("tempdir");
        let wal_path = tempdir.path().join("overflow.wal");
        let vfs = crate::vfs::VfsHandle::for_path(&wal_path);
        let file = vfs
            .open(&wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager_vfs = crate::vfs::mem::MemVfs::default();
        let pager = test_pager(&pager_vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let page_id = 13_u32;
        let base = vec![0_u8; ps as usize];
        let mut updated = base.clone();
        updated[0] = 1;
        let delta = crate::wal::delta::encode_page_delta(&base, &updated).expect("encode delta");
        let page_bytes = crate::wal::format::WalFrame::page(page_id, base)
            .encode(ps)
            .expect("encode page frame");
        let delta_bytes = crate::wal::format::WalFrame::page_delta(page_id, delta)
            .encode(ps)
            .expect("encode delta frame");

        write_all_at(file.as_ref(), 0, &WalHeader::new(ps, 0).encode()).expect("write header");
        let mut offset = WAL_HEADER_SIZE;
        write_all_at(file.as_ref(), offset, &page_bytes).expect("write page frame");
        offset += page_bytes.len() as u64;
        for _ in 0..MAX_PENDING_RECOVERY_FRAMES {
            write_all_at(file.as_ref(), offset, &delta_bytes).expect("write delta frame");
            offset += delta_bytes.len() as u64;
        }
        persist_header(&file, ps, offset).expect("persist header");
        file.set_len(offset).expect("set wal len");

        let error =
            initialize_or_recover(&file, &pager, ps, 0, None).expect_err("pending overflow");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
        assert!(error
            .to_string()
            .contains("more than 1,000,000 uncommitted page frames"));
    }
}
