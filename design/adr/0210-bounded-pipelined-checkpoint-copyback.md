# ADR 0210: Bounded Pipelined Checkpoint I/O

**Date:** 2026-08-02
**Status:** Accepted

## Context

A reader-free destructive checkpoint materializes the latest committed image of
each dirty page from the WAL and copies those images into the main database.
ADR 0004 requires the database copy to become durable before the WAL can be
reset. ADR 0209 additionally requires an uncertain or asynchronous WAL tail to
be durable before the first database page is overwritten.

Copyback previously performed these two CPU/I/O stages serially: materialize up
to 8 MiB of WAL pages, issue one positional database write, wait for that write,
then begin materializing the next batch. The bounded batches avoided
database-sized scratch allocation, but large contiguous checkpoints left either
WAL decoding or database-file writeback idle at each stage boundary. The
rust-baseline medium and full checkpoints are dominated by this repeated work.

Small checkpoints do not have enough copyback work to amortize a thread launch,
and sparse dirty-page sets would require frequent handoffs for short runs.

## Decision

For a sorted checkpoint set that is both:

1. one contiguous page-id run; and
2. larger than one 8 MiB copyback batch;

checkpoint uses a per-call I/O worker and two reusable, bounded 8 MiB
buffers. The checkpoint thread materializes the next batch from the WAL while
the worker writes the preceding batch to the main database. At most one batch
is outstanding. Before dispatching another batch, the checkpoint thread
receives the completed buffer and reuses it.

Small and non-contiguous checkpoint sets retain the original sequential
copyback path. If the operating system cannot create the worker, the
checkpoint also falls back to sequential copyback before writing any database
page.

After copyback, an eligible checkpoint keeps the now-idle worker until final
WAL truncation. Once all database pages and the database header are durable and
the zero-end WAL header has been written and the WAL shortened, the same worker
performs the final WAL `sync_data` while the checkpoint thread clears
local/sidecar indexes, resets counters, and returns freed allocator arenas. The
checkpoint thread receives the sync result and joins the worker before final
coordination publication or return.

The worker is created and owned inside one checkpoint call. An explicit
`JoinHandle` plus a Drop fallback guarantee clean shutdown on every exit path.
It does not outlive the checkpoint, own a persistent WAL lifecycle, or change
the single-writer/many-reader model. The existing process checkpoint gate, WAL
write lock, and reader-retention checks remain held for the complete operation.

## Durability and failure ordering

Pipelining overlaps independent work without weakening a durability or
publication boundary:

1. the stable WAL tail is made durable when required;
2. all pipelined database writes complete;
3. local cache/header/freelist checkpoint updates complete;
4. the database file is synced;
5. checkpoint coordination is published and the WAL is logically reset;
6. WAL shrink occurs, then the final WAL sync overlaps only local index/counter
   cleanup and allocator trimming;
7. the worker is joined and its result reconciled before final zero-end
   coordination publication and successful return.

A materialization error, database-write error, channel failure, or worker panic
is returned before the database sync and before any WAL logical or physical
reset. Some main-database pages may already contain newer committed images, but
the complete published WAL remains the recovery authority. Worker panic is
converted to a typed internal error after the worker is joined. Normal
RAII cleanup releases the checkpoint-pending flag and locks, so the same handle
can retry safely.

A normal final WAL sync error follows ADR 0209's late-error reconciliation: the
local logical reset and index cleanup remain complete, final zero-end process
publication is skipped, and the I/O error is returned. If the worker or result
channel fails while final sync is uncertain, checkpoint repeats the idempotent
`sync_data` on the caller thread before considering final publication, still
joins the failed worker, and returns the typed worker error.

The paged WAL-index sidecar is a generation-scoped cache. `clear()` marks its
current generation unreadable in memory before rewriting its header or
truncating physical records. Consequently, a partial sidecar clear after the
logical WAL reset cannot leave pre-reset offsets available to same-process
reads or later low-offset commits. The first later spill must successfully
clear the physical sidecar before it can publish a current-generation record;
if that clear also fails, the committed version is restored in memory and the
post-commit cache-maintenance error is deferred for a later retry while the
stale generation remains unreadable. A fresh open likewise clears the sidecar
before WAL recovery and fails closed if it cannot. This makes final zero-end
checkpoint publication safe even when sidecar cleanup reports a delayed
physical error.

Async-commit durability watermarks are rebased before the zero-end WAL header
or logical end can be published. A rebase failure therefore leaves the old WAL
tail and in-memory index live and retryable. After logical zero is published,
index and sidecar invalidation is unconditional; no later fallible bookkeeping
step can strand stale versions as current.

## Memory bounds

The pipelined path owns at most two 8 MiB copyback buffers, one 256 KiB WAL
read-ahead window, and the existing per-write encryption scratch when TDE is
enabled. Memory is therefore independent of database size. The sequential path
continues to own one 8 MiB copyback buffer.

The buffers and WAL-version references are dropped before the database sync and
post-checkpoint heap-release step. Benchmark RSS sampling showed no material
increase: median peak RSS changed by approximately +0.08 MiB at medium scale
and +0.22 MiB at full scale, both far below one page-batch allocation and within
run-to-run noise.

## Consequences

- WAL materialization and database positional writes overlap for large
  contiguous checkpoints.
- Fresh release A/B medians improved from 46.632 ms to 43.601 ms at
  rust-baseline medium scale (6.5%) and from 320.369 ms to 283.685 ms at full
  scale (11.5%) from copyback overlap. Reusing the same worker for final WAL
  sync reduced a subsequent 14-run medium median to approximately 40.88 ms;
  the eight-run full median remained effectively neutral at approximately
  282.87 ms. Smoke does not enter the pipeline.
- Copyback write size remains capped at 8 MiB.
- There is one owned-and-joined thread launch per eligible checkpoint. A persistent
  checkpoint worker is intentionally not introduced by this decision; it would
  require a broader lifecycle, shutdown, and panic-recovery contract.
- There is no database, WAL, coordination-sidecar, or C ABI format change.

## Validation

- A deterministic VFS probe blocks the first database batch write and verifies
  that the checkpoint thread reads the next WAL batch concurrently.
- Existing bounded-write and checkpoint/reopen tests cover batch sizing and
  main-database integrity.
- Fault-injection tests fail the second worker database write and verify the WAL
  remains published and can recover the final page after reopen.
- A WAL materialization-read failure while the first database write is
  outstanding verifies joined shutdown and retained-WAL recovery.
- A worker-panic test verifies the panic becomes a typed error, the WAL remains
  published, and the same database handle can retry checkpoint successfully.
- A forced worker-spawn failure verifies sequential fallback occurs before
  copyback and all writes stay on the checkpoint caller.
- TDE coverage verifies encrypted batch copyback, worker reuse for the final
  WAL sync, and successful reopen.
- Final-sync error and panic tests verify late logical reconciliation, skipped
  publication on ordinary sync failure, caller-thread resync after uncertain
  worker failure, joined shutdown, and reopen integrity.
- Final coordination-publication failure after durable zero/shrink leaves the
  earlier barrier readable; an independent peer repairs it from authoritative
  storage, after which the original handle appends and both rows survive reopen.
- A hotset-one partial-sidecar-clear test fails truncation after the replacement
  sidecar header is written, verifies the stale generation immediately reports
  zero readable versions, reuses its old WAL offsets with different pages,
  repeats first-spill clear failures while preserving committed versions in
  memory, forces a fresh sidecar spill, fails a partial sidecar-header rewrite
  closed during reopen, then retries reopen and rebuilds only the non-empty
  post-reset WAL generation.
- An injected async-watermark rebase failure verifies checkpoint leaves the old
  logical WAL end and index readable, and that the same handle can retry the
  checkpoint successfully.
- Release rust-baseline smoke, medium, and full trials record checkpoint time,
  checkpoint RSS, and process peak RSS for the sequential and pipelined builds.

## References

- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0138-post-checkpoint-heap-release.md`
- `design/adr/0204-bounded-wal-commit-preparation.md`
- `design/adr/0208-cross-process-reader-admission-checkpoint-gate.md`
- `design/adr/0209-checkpoint-tail-async-durability-barrier.md`
