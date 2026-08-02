# Paged On-Disk WAL Index with Bounded In-Memory Hot Set
**Date:** 2026-04-22
**Status:** Accepted (2026-04-23); generation-safe rebuildable sidecar implemented.

### Decision (proposed)

Persist the WAL page-version index to a dedicated on-disk structure with a
bounded in-memory hot set, replacing the unbounded
`HashMap<PageId, Vec<WalVersion>>` currently held in `SharedWalInner::index`.

This ADR is explicitly **Deferred** until ADR 0140 (`WalVersion`
discriminated payload) has shipped and been measured. The expected benefit
of ADR 0140 is large enough that this ADR may not be needed; if it is, the
post-0140 measurements will inform the on-disk index design.

### Rationale

After ADR 0140 the per-version *payload* cost is paid only for the hot K
versions per page plus active-reader retention. The remaining cost is the
**index entry itself** — `(PageId, lsn, payload-discriminator)` — which is
~24 bytes per dirty page kept in a `HashMap`. For workloads that touch
many millions of distinct pages between checkpoints (large bulk loads with
small commit cadence, long-lived reader sessions per ADR 0019), this
in-memory cost can still reach hundreds of MB.

A paged on-disk index would:

1. cap in-memory state at a configurable hot-set size;
2. survive process restart faster than re-scanning the WAL frames;
3. enable the background checkpoint worker (ADR 0058) to process WAL
   regions independently of the writer thread.

### Why Deferred

- Slices M1, M2, M4 (ADRs 0137, 0138, 0140) collectively reduce the
  observed memory footprint by an estimated 20×. Deferring this ADR until
  those land lets us measure whether a further reduction is needed, and
  what the realistic upper bound on dirty-page count between checkpoints
  actually is in production workloads.
- An on-disk WAL index is a non-trivial format change (new file, new
  recovery path, new corruption surface). Doing it before establishing
  the empirical need would be premature.
- Coordinated change with ADR 0058 (background checkpoint worker)
  preferred — both touch the same boundary.

### Open Questions

- **File layout.** Sidecar file (e.g. `*.wal-idx`) or interleaved with the
  WAL?
- **Recovery semantics.** Rebuild from WAL on missing/corrupt index? (Yes,
  almost certainly — keeps the index a pure cache.)
- **Hot-set eviction policy.** LRU keyed on page id, or recency-weighted?
- **Interaction with ADR 0067 mmap reads.** If the index is sidecar, can
  it also be `mmap`-backed?

### Implemented cache and failure protocol

The delivered sidecar remains a rebuildable cache rather than a recovery
authority. Its in-memory handle treats physical records as belonging to one
logical WAL generation. A clear invalidates that generation before any
fallible header rewrite or truncate. If physical cleanup fails partway, reads,
checkpoint enumeration, promotion, and version accounting ignore every old
record. The first subsequent spill must complete a fresh clear before writing
new metadata, and each open clears before WAL recovery. Thus WAL-offset reuse
after checkpoint cannot reinterpret a pre-reset record as a current frame.

This adds no persistent format field or migration: validity is deliberately
conservative and process-local, while every reopen rebuilds the cache from the
authoritative current WAL.

Individual records use one publication byte. An overwrite first publishes
`EMPTY`, writes all remaining metadata, and publishes `PRESENT` last. A failed
write can therefore expose only the previous complete record, no record, or the
new complete record; it cannot expose `PRESENT` with partially replaced WAL
metadata. Clearing a record changes only that publication byte. When promotion
or spill reports an I/O error, the authoritative version is kept or restored in
the in-memory index before the error leaves the cache-maintenance operation.

Sidecar I/O is excluded from the ambiguous commit interval. Every sidecar read,
promotion, and duplicate-record clear needed by a commit occurs while preparing
a bounded page group, before the final group can publish the new logical end in
the WAL header. Failure leaves the preceding logical WAL end and in-memory
index authoritative; an already-written unpublished frame group may be
overwritten by the retry. Once the WAL is durable and locally published,
demotion/spill is best-effort cache maintenance. A failed spill restores the
popped version and cannot turn an acknowledged transaction into an error.
Auto-checkpoint is likewise post-commit maintenance and is retried by a later
write or explicit checkpoint rather than creating a committed-but-reported-
failed result.

The implemented sidecar has process-local validity and locking. A database
using cross-process coordination therefore selects the in-memory WAL index even
when `wal_index_hot_set_pages` is non-zero. Enabling spill for coordinated
processes requires a separately specified shared generation and locking
protocol; one process must never truncate or invalidate another process's
cache while it is consulting it.

### References

- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0033-wal-frame-format.md
- design/adr/0056-wal-index-pruning-on-checkpoint.md
- design/adr/0058-background-incremental-checkpoint-worker.md
- design/adr/0067-wal-mmap-write-path.md
- design/adr/0137-size-based-auto-checkpoint-trigger.md
- design/adr/0138-post-checkpoint-heap-release.md
- design/adr/0140-walversion-discriminated-payload.md
- design/adr/0210-bounded-pipelined-checkpoint-copyback.md
