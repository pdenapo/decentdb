# ADR 0209: Checkpoint Tail Async Durability Barrier

**Date:** 2026-08-01
**Status:** Accepted

## Context

ADR 0004 requires a reader-free destructive checkpoint to copy every committed
WAL page into the main database, make the database durable, and only then
discard the WAL tail. ADR 0135 allows `WalSyncMode::AsyncCommit` to acknowledge
commits before the background WAL flusher has synced them.

Those contracts interact at the checkpoint tail. Once a checkpoint has acquired
the writer/checkpoint locks and proven there are no retaining readers, the
current WAL end is stable. But under `AsyncCommit`, some frames up to that stable
end may still be dirty in the OS page cache. Copying those pages into the main
database before the WAL tail is durable creates an unsafe crash interval: a
database sync failure or later WAL truncate failure must still leave at least
one durable source of truth for the committed pages.

Historically, the destructive checkpoint appended and force-synced a transient
`FrameType::Checkpoint` frame before truncation. That made async tails durable,
but it added a WAL write, a WAL logical-end publication, and an extra sync to
the hottest successful checkpoint path.

## Decision

Reader-free destructive checkpoint now performs a foreground `AsyncCommit`
durability barrier after:

1. the process checkpoint lock and local WAL write lock make `current_lsn`
   stable; and
2. local, named-snapshot, and process reader retention prove copyback is safe;

but before the first main-database copyback write.

For `WalSyncMode::AsyncCommit`, the barrier synchronously flushes the WAL file to
the dirty LSN observed at barrier entry. It is serialized with the background
flusher by a per-WAL flush mutex and uses the VFS `sync_data` contract, which
includes any file-length growth required to address those bytes. The durable
LSN watermark advances only after the physical sync succeeds, and sync errors
are returned to the checkpoint caller before any database page is overwritten.

For all non-`AsyncCommit` modes the barrier is a no-op. Synchronous modes have
already performed their commit durability barrier, and `TestingOnlyUnsafeNoSync`
keeps its explicit test-only durability relaxation.

When a handle opens or refreshes a non-empty WAL tail published through process
coordination, it cannot prove whether the publishing process used synchronous or
asynchronous commit. Until coordination carries a durable watermark, a
destructive checkpoint of such an externally recovered tail performs a
conservative `sync_data` on the stable WAL end before any main-database
copyback. A successful forced sync clears the local “checkpoint tail sync
needed” marker; a failed sync returns before copyback.

Normal destructive checkpoint no longer appends or syncs a transient
`FrameType::Checkpoint` frame. Legacy WAL recovery continues to decode and honor
old checkpoint frames by clearing uncommitted pending pages before replaying
later frames. The WAL byte format, database format, checkpoint LSN in the main
database header, and coordination sidecar format are unchanged.

## Ordering

Successful reader-free checkpoint order is:

1. acquire process checkpoint lock when process coordination is enabled;
2. refresh from coordination;
3. acquire local WAL write lock and set `checkpoint_pending`;
4. fence local reader registration through the WAL index lock;
5. compute `current_lsn` and reader-retention blockers;
6. if no readers block destructive checkpoint, flush the local async WAL tail to
   durability and, for externally recovered coordinated tails, force-sync the
   physical WAL tail;
7. materialize latest WAL versions and copy them into the main database;
8. refresh/truncate main database state, set `last_checkpoint_lsn`, and
   `sync_data` the main database;
9. publish a process checkpoint-generation barrier with `wal_end_lsn` still set
   to the old stable WAL end;
10. rebase `AsyncCommit` dirty/durable watermarks to zero before exposing a
    lower logical end;
11. write a zero-end WAL header, reconcile the local logical end, and shrink
    the WAL file to its fixed header;
12. perform the single final WAL `sync_data` for the replacement header and
    shorter file length, overlapping local index/counter cleanup when the
    bounded copyback worker exists;
13. publish the final process checkpoint metadata with `wal_end_lsn = 0`;
14. return any late WAL-reset, worker, publication, or cache-maintenance error
    after local reconciliation.

Failure ordering is intentionally conservative:

- Async preflush failure happens before any main-database copyback write; the
  existing WAL remains the recovery source.
- Main-database sync failure leaves the published WAL tail intact.
- Failure to publish the checkpoint-generation barrier aborts before any WAL
  mutation; the old WAL tail remains intact.
- WAL truncate-header write failure happens after the barrier but before local
  WAL state is reconciled; the old WAL tail remains intact and peers refresh
  through the barrier when the gate opens.
- WAL shrink or final `sync_data` failure happens after the live zero-end
  header is written. The checkpoint reconciles local WAL state, clears
  indexes, skips final `wal_end_lsn = 0` publication if the durability barrier
  failed, and returns the late I/O error. The earlier checkpoint-generation
  barrier still forces already-open handles to refresh before their next
  reader or writer operation.
- Final process checkpoint publication failure leaves the local state and WAL
  file reconciled; the earlier barrier remains available to force peer refresh,
  and the publication error is returned.

## Consequences

- Successful destructive checkpoints remove one transient WAL frame write and
  one WAL sync from the normal path, improving checkpoint latency and reducing
  cold-read/report workloads that checkpoint between benchmark phases.
- The final WAL truncate barrier uses the built-in VFS `sync_data` contract to
  durably persist both the zero logical end and shorter file length. Linux
  therefore uses `fdatasync` and may omit recovery-irrelevant inode/timestamp
  metadata; platforms with one durable flush primitive retain their existing
  full flush. General commit and uncertain-tail sync policy remains unchanged.
- `Db::sync()` and checkpoint share the same foreground async flush machinery, so
  sync errors are surfaced instead of hidden behind background polling.
- Background and foreground flushers serialize the dirty-LSN decision and
  physical sync under the flush mutex.
- Successful logical truncation resets `AsyncCommit` dirty and durable
  watermarks to zero so later low-offset WAL tails are not hidden behind the
  pre-checkpoint high-water mark.
- Refreshing from an external checkpoint that lowers the observed WAL end also
  rebases local `AsyncCommit` watermarks to the refreshed clean LSN.
- Old WAL files containing checkpoint frames remain readable; new successful
  destructive checkpoints simply do not emit them.

## Validation

- AsyncCommit tests cover long background intervals where checkpoint must flush
  the dirty tail itself and verify that post-checkpoint low-offset commits are
  still flushed by `Db::sync()`, including after an external checkpoint refresh.
- Fault-injection tests verify async preflush errors return before any database
  copyback write, including an independent opener checkpointing an externally
  acknowledged async tail.
- Recovery tests verify normal destructive checkpoint does not emit
  `wal.write_checkpoint`, legacy checkpoint frames still replay later full and
  delta frames, WAL truncate write/sync failures leave the database reopenable,
  and late reset sync failure does not strand same-handle or already-open
  independent handles on stale WAL state.
- A final-publication fault fails the coordination-header write after durable
  zero and physical shrink. The stale pre-reset coordination barrier is safely
  repaired by an independent opener; the original handle then refreshes,
  appends at low offsets, and reopens with both transactions intact.
- Existing database-sync-failure checkpoint tests continue to verify that a
  failed main-database durability barrier leaves the WAL recoverable.

## References

- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0135-async-commit-wal-group-commit.md`
- `design/adr/0202-wal-frame-integrity-without-checksums.md`
- `design/adr/0208-cross-process-reader-admission-checkpoint-gate.md`
