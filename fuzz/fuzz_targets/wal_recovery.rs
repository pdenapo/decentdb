//! Fuzz target: WAL recovery must never panic on malformed input.
//!
//! Builds a valid template database once per process, then for each fuzz
//! input materializes the template database file alongside a fuzz-derived
//! WAL and reopens it. Recovery may succeed or return a typed corruption
//! error — it must never panic (libFuzzer reports panics as crashes).
//!
//! Run with: `cargo +nightly fuzz run wal_recovery`

#![no_main]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use decentdb::{Db, DbConfig, WalSyncMode};
use libfuzzer_sys::fuzz_target;

/// Snapshot of a valid, checkpointed database used as the per-iteration base.
struct Template {
    db_bytes: Vec<u8>,
}

static TEMPLATE: OnceLock<Template> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

const MAX_WAL_BYTES: usize = 256 * 1024;

fn wal_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

fn build_template() -> Template {
    let dir = std::env::temp_dir().join(format!("decentdb-fuzz-template-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    if fs::create_dir_all(&dir).is_err() {
        return Template { db_bytes: vec![] };
    }
    let db_path = dir.join("template.ddb");

    let config = DbConfig {
        wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
        ..DbConfig::default()
    };
    let result = (|| -> Option<Vec<u8>> {
        let db = Db::create(&db_path, config).ok()?;
        db.execute("CREATE TABLE t(id INT64, val TEXT, amount FLOAT64)")
            .ok()?;
        for i in 0..25 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'row-{i}', {i}.5)"))
                .ok()?;
        }
        db.checkpoint_wal().ok()?;
        drop(db);
        fs::read(&db_path).ok()
    })();

    let _ = fs::remove_dir_all(&dir);
    Template {
        db_bytes: result.unwrap_or_default(),
    }
}

/// Derive this iteration's WAL bytes from the fuzz input.
fn fuzz_wal_bytes(data: &[u8]) -> &[u8] {
    if data.len() > MAX_WAL_BYTES {
        &data[..MAX_WAL_BYTES]
    } else {
        data
    }
}

fuzz_target!(|data: &[u8]| {
    let template = TEMPLATE.get_or_init(build_template);
    // Skip iteration if template setup failed (e.g. transient filesystem errors).
    if template.db_bytes.is_empty() {
        return;
    }

    let ordinal = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let db_path = std::env::temp_dir().join(format!(
        "decentdb-fuzz-wal-{}-{ordinal}.ddb",
        std::process::id()
    ));

    if fs::write(&db_path, &template.db_bytes).is_err() {
        return;
    }
    if fs::write(wal_path(&db_path), fuzz_wal_bytes(data)).is_err() {
        let _ = fs::remove_file(&db_path);
        return;
    }

    // Recovery must succeed cleanly or return a typed error — never panic.
    if let Ok(db) = Db::open(&db_path, DbConfig::default()) {
        // If recovery accepted the WAL, basic reads must also not panic.
        let _ = db.execute("SELECT COUNT(*) FROM t");
    }

    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(wal_path(&db_path));
    // Best-effort cleanup of any coordination sidecar.
    let mut coord = db_path.as_os_str().to_os_string();
    coord.push(".coord");
    let _ = fs::remove_file(PathBuf::from(coord));
});
