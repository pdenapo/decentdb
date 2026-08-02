# ADR 0211: Fresh-Create Bootstrap Sync Overlap

**Date:** 2026-08-02
**Status:** Accepted

## Context

Creating a native DecentDB file writes two known pages: the database header and
the reserved empty catalog root. The create contract then calls the VFS
`sync_data` barrier so both bytes and the file length needed to address them are
durable before `Db::create` returns. Only after that barrier did the engine
construct the pager, process coordinator, empty WAL state, runtime, and public
`Db` handle.

The initialization is independent of the physical bootstrap flush when the
main file is known fresh, but its fixed setup latency was entirely serialized
behind storage latency. Any overlap must preserve the durable-create boundary,
must not race another main-file operation with the flush, and must remain safe
for custom VFS implementations, fault injection, encryption, and browser
targets.

## Decision

The VFS has a private, opt-in concurrent-bootstrap-sync reservation. The
default is unavailable. Native `OsVfs` opts in; `StatsVfs` delegates the
reservation, and `EncryptedVfs` delegates only when its wrapped VFS can provide
one. `MemVfs`, OPFS, and custom VFS implementations therefore remain inline
unless they explicitly implement the capability.

After the two bootstrap pages and their final length have been written, a
capability-enabled native create:

1. resolves and caches the canonical main path before the overlap begins;
2. starts one scoped, named worker with a 64 KiB stack;
3. has that worker call exactly the bootstrap file's `sync_data` method;
4. constructs the known-fresh pager and process coordinator, then attempts to
   acquire the WAL with an atomic `CreateNew` open;
5. carries the exact newly opened WAL file into initialization, avoiding a
   separate existence-check/open gap;
6. constructs the empty runtime and `Db` on the caller while the barrier runs;
7. joins the worker and requires its sync result before returning the `Db`.

If atomic WAL acquisition finds an existing sidecar or same-process shared WAL
handle, initialization unwinds without opening that WAL for recovery. Create
first joins and validates the bootstrap sync, then retries the ordinary
recovery-aware open path. This ordering closes the race where another actor
could create an orphan WAL after an absence check but before acquisition and
cause recovery to read the not-yet-durable main file.

The known-fresh pager constructor seeds its cached page count to exactly two,
matching `write_database_bootstrap_vfs`, rather than reading or stating the main
file. The pre-resolved canonical-path hint is reused by the open lock,
coordination setup, and WAL registry. Thus no main-file read, write, stat,
resize, metadata sync, lock, or canonicalization occurs in the initialization
side of the overlap.

If scoped worker creation fails, create performs the original inline
`sync_data` barrier before database initialization. A worker panic becomes a
typed internal error. A worker sync error takes precedence over a concurrent
initialization error, and no `Db` is returned unless the worker joined and the
durability barrier succeeded. The API never returns pending durability and
does not move the observable successful-return boundary.

`FaultyVfs` retains owner-thread-only failpoint decisions. It returns no
concurrent reservation whenever failpoints are active, keeping the bootstrap
sync and its hit ordering inline. Its reservation also holds the shared side of
a gate whose exclusive side covers failpoint installation and clearing. This
closes the check/use race: a failpoint cannot become active between reservation
selection and worker completion and then be silently bypassed on the worker.

The database format, WAL format, coordination format, C ABI, and public
configuration are unchanged.

## Consequences

- Native fresh creates can overlap the one mandatory main-file durability
  barrier with independent fixed-cost initialization.
- Fresh WAL selection is atomic. An existing or concurrently created sidecar
  cannot enter recovery until the main bootstrap barrier has joined.
- Durable create semantics remain unchanged: callers receive either a fully
  joined, durably bootstrapped `Db` or an error.
- Fresh pager construction avoids one redundant main-file size query; existing
  opens still validate the stored header and stat the main file once.
- VFS safety is conservative and compositional. A wrapper may delegate the
  reservation only when its behavior remains safe across the worker boundary.
- One short-lived native thread is created per capability-enabled fresh create.
  Thread creation failure is non-fatal because the exact inline barrier path is
  retained.
- Concurrent failpoint installation can wait briefly for an already-reserved
  bootstrap sync; active failpoints never move off their owner thread.

## Validation

- A blocking capability-enabled VFS proves WAL/coordination initialization
  advances while bootstrap sync is blocked, all other main-file operation
  counts remain zero, and create cannot return until sync is released and
  joined.
- Focused tests cover sync error precedence, worker panic conversion, forced
  spawn fallback, custom-VFS inline behavior, and same-path `CreateNew` failure
  without deadlock.
- A deterministic race test proves failpoint installation serializes with an
  outstanding FaultyVfs reservation instead of bypassing injection.
- Existing create failpoint tests cover `Error` and `DropSync` exactly once on
  the owner thread, with no WAL initialization after the error.
- Open-path counters retain one main-file open, zero fresh header reads, zero
  fresh file-size calls, one create data sync, and the normal existing-open
  header/stat behavior.
- Orphan-WAL and deterministic check/acquire-race regressions prove that normal
  recovery starts only after the main bootstrap barrier has joined.
- TDE create/reopen tests exercise atomic encrypted-WAL creation during the
  delegated native overlap, while memory/custom VFS tests retain inline
  behavior.

## Alternatives Considered

- **Return before sync completes:** rejected because it weakens the durable
  create contract and exposes pending durability.
- **Enable every `Send + Sync` VFS automatically:** rejected because thread
  safety alone does not establish bootstrap-sync, wrapper, or failpoint
  semantics.
- **Run pager stat/header validation during sync:** rejected because it would
  access the file concurrently and discard knowledge already established by
  the bootstrap writer.
- **Move failpoint decisions onto the worker:** rejected because the harness
  intentionally binds deterministic decisions and ordering to the installer
  thread.
- **Use a persistent worker pool:** rejected because it adds lifecycle and
  shutdown complexity to a single create-only barrier.

## References

- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0105-in-memory-vfs.md`
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
- `design/adr/0177-cross-process-coordination-sidecar-and-locking.md`
- `design/adr/0209-checkpoint-tail-async-durability-barrier.md`
