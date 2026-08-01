#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::config::DbConfig;
    use crate::vfs::stats;
    use crate::{Db, Value};

    /// Disables global VFS accounting on drop so a failing assert cannot leak
    /// the enabled flag into other tests sharing the process. Nextest runs
    /// each test in its own process, but the guard keeps `cargo test` safe
    /// too. Mirrors `benchmark::VfsStatsScope`.
    struct StatsGuard;

    impl StatsGuard {
        fn begin() -> Self {
            stats::set_enabled(true);
            Self
        }
    }

    impl Drop for StatsGuard {
        fn drop(&mut self) {
            stats::set_enabled(false);
        }
    }

    /// Checkpoint copyback must fsync the main database file before the WAL —
    /// the only other copy of the committed pages — is truncated (ADR 0004).
    #[test]
    fn checkpoint_syncs_db_file_before_wal_truncation() {
        let _stats_guard = StatsGuard::begin();
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-syncs-db-file.ddb");
        let db = Db::open_or_create(
            &path,
            DbConfig {
                // Disable threshold-based and background checkpoints so the
                // explicit checkpoint below is the only one that runs.
                wal_checkpoint_threshold_pages: 0,
                wal_checkpoint_threshold_bytes: 0,
                background_checkpoint_worker: false,
                ..DbConfig::default()
            },
        )
        .expect("open db");
        db.execute("CREATE TABLE synced (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create table");
        db.execute("INSERT INTO synced VALUES (1, 'durable')")
            .expect("insert row");

        // Ignore open/bootstrap noise; only the checkpoint may sync the db file.
        stats::reset();
        db.checkpoint_wal().expect("checkpoint");

        let snapshot = stats::snapshot();
        assert!(
            snapshot.db.sync_metadata_calls + snapshot.db.sync_data_calls >= 1,
            "checkpoint must sync the database file before discarding the WAL: {snapshot:?}"
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
}
