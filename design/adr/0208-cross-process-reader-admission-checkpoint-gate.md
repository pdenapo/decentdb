# ADR 0208: Cross-Process Reader Admission And Checkpoint Gate

**Date:** 2026-08-01
**Status:** Accepted

## Context

ADR 0178 requires a conservative process reader slot before a reader depends
on its snapshot and requires checkpoints to retain WAL needed by every live
process reader. A slot lock and an `INITIALIZING` state make a visible slot
conservative, but they do not close the interval before the slot exists:

1. a reader refreshes its local WAL and prepares to register;
2. a checkpoint scans all slots and observes none;
3. the reader captures and publishes its snapshot slot; and
4. the checkpoint copies back, durably resets the WAL, and publishes the new
   checkpoint generation.

The reader can then depend on WAL frames that the checkpoint was permitted to
discard. Rechecking coordination generations around registration detects
publication that has already occurred, but cannot make the scan-to-publication
interval atomic with reader admission.

The coordination sidecar already provides a stable OS byte-range lock
namespace. Its bytes 0, 1, and 2 serialize initialization, writer/checkpoint
ownership, and metadata publication; reader slot locks start at byte 4096.

## Decision

Reserve sidecar lock byte 3 as the reader-admission/checkpoint gate. This byte
is a lock range only; it stores no record.

A process-coordinated reader takes the gate shared and holds it across the
complete admission sequence:

1. refresh local pager/WAL state from coordination;
2. read the before-registration WAL and checkpoint generations;
3. capture the local snapshot while registering an `INITIALIZING` slot and
   then its `ACTIVE` snapshot LSN under that slot's ownership lock;
4. read and validate the after-registration generations; and
5. either return the registered reader or release both the slot and admission
   gate before bounded retry/backoff.

The existing eight fast exponential retries are retained. After those retries,
registration polls at the capped delay until the configured process-lock busy
timeout. A zero timeout reports `Busy`; an expired nonzero timeout reports
`Timeout`. It must never return an unvalidated or unregistered stale snapshot.

A checkpoint takes the same gate exclusively after acquiring the existing
process writer/checkpoint lock. It holds the exclusive gate from before WAL
refresh and the cross-process retention scan through:

- process and local reader-retention decisions;
- copyback and main-database durability synchronization;
- checkpoint-generation barrier publication while the old WAL end is still
  visible;
- WAL logical reset or truncation and WAL index cleanup; and
- final coordination-sidecar checkpoint publication.

The exclusive gate therefore makes “no reader slot exists” stable for the
entire destructive checkpoint interval. Readers admitted before it are visible
to the scan; readers arriving after it cannot capture a dependent snapshot
until reset and publication are complete.

## Lock Order And Local Arbitration

The required acquisition order is:

1. coordinator initialization lock, used only while opening or rebuilding;
2. process writer/checkpoint lock byte 1;
3. exclusive reader-admission gate byte 3;
4. in-process WAL write lock and WAL-index fence; and
5. metadata publication while those checkpoint locks remain held.

Readers take shared admission byte 3 and then a reader-slot ownership byte at
4096 or above. They never take the writer lock. Slot release does not acquire
the writer or admission gate. This order lets an admitted reader finish while
a checkpoint waits and prevents a writer/checkpoint/reader cycle.

POSIX `fcntl` record locks are associated with a process, so two file handles
in one process do not provide independent shared ownership and one local
unlock can release a coalesced process lock. Coordinators for the same
canonical sidecar path therefore share a process-local readers/writer arbiter.
The arbiter owns exactly one OS shared lock while one or more local readers are
admitted, or exactly one OS exclusive lock for a local checkpoint. Waiting
writers prevent new local readers from starving a checkpoint. Windows retains
the same local arbitration and uses the VFS range-lock implementation for
cross-process exclusion. The path-local registry also gives every local
coordinator for the canonical database path the same coordination-file
descriptor. This is required on classic POSIX record-lock implementations,
where closing any descriptor for the inode can release the process's locks,
including locks originally acquired through a different descriptor.

The same path-local state serializes writer byte 1 across local threads and
owns its single OS writer guard. Same-thread nested acquisition increments a
central recursion depth; a different local thread waits without issuing
another `fcntl` call. The local wait and subsequent OS wait consume one busy
timeout budget, preserve writer/checkpoint timeout metrics and callbacks, and
release the OS guard only after the last nested guard and owner-diagnostic
cleanup. Guards retain the acquiring owner identity, so moving or dropping a
guard from another thread does not transfer recursion ownership.

Neither local arbiter holds its state mutex while waiting on the OS range
lock. Exactly one local waiter is published as the pending OS-attempt leader;
the other waiters remain on the local condition variable and independently
enforce their own total timeout budgets. The leader clears its pending state
and wakes them after success or failure. A zero-timeout waiter can therefore
return `Busy`, and a short waiter can return `Timeout`, while a longer local
leader is still blocked by another process.

The physical sidecar is derived from the canonical database path and opened
there. Symlink and canonical spellings consequently share both one sidecar and
one path-local arbiter regardless of which spelling opens first. Distinct hard
links cannot be coalesced portably by pathname canonicalization and concurrent
opens through multiple hard-link names are not supported.

## Crash And Corruption Behavior

OS range locks are released when a process exits, including after a crash. An
`INITIALIZING` slot remains a conservative retention blocker while its owner
lock is live. It may be cleared only after the engine proves its ownership lock
is available. Invalid slot checksums or states remain conservative truncation
blockers. A checkpoint cannot misinterpret the pre-slot “locked but empty”
window because the reader holds the shared admission gate during that window.
Because classic POSIX locks do not conflict within one process, the path-local
slot registry atomically reserves a slot before attempting its OS lock and
keeps that reservation through record clearing and explicit OS unlock. A local
retention scan treats even a reserved-but-still-empty slot conservatively.
Every stale-slot ownership probe uses the same ordering: reserve the slot
locally first, then try its OS lock. Failure to reserve is treated as live or
otherwise retention-blocking; a reclaimable probe retains both the reservation
and OS guard until the stale record is cleared and the OS guard is explicitly
released. Diagnostic slot scans use the same atomic probe, so they cannot
temporarily unlock or misclassify a newly registered same-process reader.

An error while acquiring either local or OS admission ownership removes the
waiter from local counters and preserves the previously held OS mode. Failed
reader validation releases the slot and shared gate before backoff. Failed
checkpoint work releases the gate through guard unwinding without publishing a
successful reset. Reader-slot OS-lock and record-write failures release their
local reservation in the same safe order. Writer owner-publication failures
clear central ownership and the OS guard before returning the error.

All cooperating processes must run a binary that implements this gate. Mixing
an older process that does not participate in byte 3 with a newer process is
not a supported concurrent-open configuration.

## Compatibility And Scope

- Database, WAL, WAL-index, catalog, coordination-header, and reader-slot byte
  layouts are unchanged.
- The coordination sidecar version and database format version are unchanged;
  byte 3 was previously unused lock namespace and contains no persistent data.
- In-memory databases, unsupported WASM environments, and explicit
  `SingleProcessUnsafe` operation have no process coordinator and pay no gate
  cost.
- Read paths that already prove an observed-current resident snapshot may keep
  their readerless shortcut. Every storage-backed or deferred fallback with a
  pager must use the coordinated admission path.
- This ADR does not change WAL durability, checkpoint copyback ordering,
  transaction isolation, or the one-writer/many-readers model.

## Alternatives Considered

1. **Rely on before/after generation validation alone.** Rejected because a
   checkpoint may scan before registration and publish only after validation.
2. **Treat every locked slot as active even when its record is empty.** Rejected
   because a checkpoint would have to lock-probe all slots and still could race
   before the reader acquires one.
3. **Take the writer lock for every reader admission.** Rejected because it
   serializes read start with all writes, not only destructive checkpoints.
4. **Disable WAL truncation whenever multiple processes are open.** Rejected
   because it is safe but causes unbounded WAL retention under ordinary use.
5. **Use only an in-process readers/writer lock.** Rejected because unrelated
   OS processes would remain uncoordinated.

## Validation

- Deterministic two-handle tests pause a reader in the locked-empty interval
  and prove checkpoint exclusion. A subprocess test holds the exclusive gate
  across an empty retention scan and publication, proves an external reader
  cannot register in that interval, then observes its live slot after reopening
  admission.
- Tests cover zero-timeout `Busy`, nonzero `Timeout`, active
  `INITIALIZING` conservatism, abandoned initializer reclamation, and corrupt
  slot retention blocking. Fault injection verifies reader-slot reservation
  reuse and writer-owner publication cleanup.
- Multithread and subprocess tests cover distinct local reader slots plus an
  external slot probe, same-thread nested writers dropped out of order, local
  writer waiters plus external exclusion, local writer timeout metrics and
  callbacks, and canonical/symlink open order in both directions.
- Deterministic stale-decode race tests pause retention and diagnostic scans,
  register a same-process reader in the decoded slot, and prove the scan cannot
  clear or unlock the replacement. Fault injection proves a failed stale-slot
  clear releases both the probe OS guard and its local reservation.
- Subprocesses hold admission byte 3 and writer byte 1 while long local leaders
  wait in the OS. Concurrent zero- and short-timeout local waiters prove their
  deadlines are independent of the leader's OS wait.
- Existing independent-handle refresh, external-reader checkpoint retention,
  checkpoint durability/failpoint, recovery, and deferred storage-backed read
  tests must remain green.
- WAL tests and strict core clippy must pass on native targets. Platform VFS
  lock tests remain responsible for the underlying POSIX and Windows
  cross-process range-lock contract.

## References

- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0018-checkpointing-reader-count-mechanism.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0177-cross-process-coordination-sidecar-and-locking.md`
- `design/adr/0178-cross-process-reader-retention-and-wal-refresh.md`
- `design/adr/0180-database-identity-for-coordination-sidecars.md`
