# WAL Checkpoint Strategy
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Implement WAL size-based checkpointing with configurable thresholds and forced checkpoint timeout.

### Rationale
- Prevents unbounded WAL growth
- Configurable threshold allows tuning for different workloads
- Timeout prevents indefinite blocking if readers are long-lived
- Forced checkpoint with readers active ensures progress

### Alternatives Considered
- Checkpoint only when no readers: Can block indefinitely
- Time-based checkpointing: Doesn't account for WAL size
- Manual checkpoint only: Too much operational burden

### Trade-offs
- **Pros**: Bounded WAL size, configurable, ensures progress
- **Cons**: Forced checkpoint may be slower, requires careful implementation

### References
- SPEC.md §4.3 (Checkpointing)

### Implementation notes (2026-08)

Checkpoint copyback must make the main database file durable before the WAL
(the only other committed-page copy) is truncated. The implementation uses
the built-in VFS `sync_data` durability contract for this barrier: both file
contents and the file length must be stable before the call succeeds. Linux
implements this with `fdatasync`, which persists size changes required for
subsequent data retrieval. Rust's macOS implementation uses `F_FULLFSYNC`,
Windows uses `FlushFileBuffers`, and the OPFS implementation uses the same
synchronous access-handle flush for data and metadata syncs. This preserves
the invariant (data + size durable before WAL discard) while allowing
platforms such as Linux to omit recovery-irrelevant timestamp metadata.
The same contract is used for the final zero-end WAL header and WAL-length
truncate barrier: both header content and the shortened length are durable,
without requiring unrelated inode/timestamp metadata. This remains ordered
strictly after the main-database `sync_data` barrier.

Copyback sorts dirty pages by page id and coalesces them into contiguous
positional writes capped at 8 MiB each. The bound prevents memory usage from
scaling with database size and also bounds the temporary encrypted copy made
by the TDE VFS. Copyback skips the per-page page-cache refresh because
`refresh_from_disk` clears the cache immediately afterward. These are pure
performance refinements within the existing checkpoint contract; they do not
alter durability, reader-safety, or retention semantics.

Because the checkpoint owns the cross-process gate and each copyback write
updates the pager's cached file length, local post-copyback cache invalidation
also avoids a redundant header read and file-size stats.

Under `WalSyncMode::AsyncCommit`, a reader-free destructive checkpoint first
forces the stable WAL tail durable before any main-database copyback write. This
foreground barrier is taken after the writer/checkpoint locks and no-reader
decision make `current_lsn` stable, and it surfaces sync errors before modifying
the database file. Normal destructive checkpoints no longer append transient WAL
checkpoint frames; recovery keeps legacy checkpoint-frame decode support for old
WAL files. See ADR 0209.
