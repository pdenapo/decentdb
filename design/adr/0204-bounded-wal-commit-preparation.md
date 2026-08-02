# ADR 0204: Bounded WAL Commit Preparation

**Date:** 2026-08-01
**Status:** Accepted

## Context

The WAL writer previously prepared every page in a transaction before issuing
any write. It retained all staged page payloads, materialized a base-page
`Arc<[u8]>` for every staged page, and encoded every WAL frame into one
contiguous `Vec<u8>`. A large rust-baseline seed commit therefore held three
page-sized representations at once: the transaction's staged pages, roughly
138 MiB of base pages, and roughly 138 MiB of encoded frames at full scale.
The base-page and encoded-frame copies scaled linearly with transaction size;
at huge scale they accounted for roughly 1.38 GiB of avoidable transient
memory.

The WAL format already has the recovery boundary needed to avoid this
whole-transaction preparation. The WAL header publishes a logical end offset,
and recovery only applies page frames followed by a Commit frame within that
logical end. Bytes written after the old logical end remain invisible.

This change affects WAL commit and failure behavior and therefore requires an
ADR under the repository's architecture policy. It does not require a file
format version or migration because the persisted bytes are unchanged.

## Decision

Prepare and write page frames in bounded groups whose maximum full-frame
payload is 4 MiB. With the default 4 KiB database page this is approximately
1,020 pages per group. Base-page materialization uses the same page count, so
encoded frames and base pages consume approximately 8 MiB combined. Smaller
transactions reserve only what their page count needs.

For each group, the writer:

1. acquires the WAL index lock once and resolves all pre-commit base pages for
   that group at the transaction's snapshot LSN, consulting and promoting any
   latest version spilled into the bounded-hot-set sidecar before falling back
   to the main database;
2. encodes each page using the existing full-page/delta choice and records its
   frame offset, encoded length, and encoding for later index publication;
3. writes the group sequentially at the current physical tail without
   changing the WAL header's logical end.

After every page group succeeds, the writer appends the existing 13-byte
Commit frame and updates header bytes 16..24 to the new logical end in one VFS
batch. It then uses the existing sync-mode barrier. Only after that barrier
succeeds does it publish versions into the WAL index, advance `wal_end_lsn`,
update checkpoint counters, and publish process coordination state. ADR 0206
refines the publication representation for reader-free self-contained full Page
frames by publishing them as on-disk WAL versions instead of resident page
payloads; the bounded preparation and logical-end invariants here remain
unchanged.

Duplicate page IDs are tracked across the complete transaction, not merely
within one preparation group. The first occurrence may use the existing delta
choice. Every later occurrence is a full Page frame, preserving the recovery
rule that a transaction-local repeat must not delta-encode against a
pre-commit base.

The reusable encoded-frame buffer is bounded in two stages. During preparation
it is shrunk to the current bounded group size after a formerly larger
transaction. Once all encoded bytes have been written, capacities up to 256
KiB remain available for ordinary commit reuse, while larger allocations are
released completely. The same idle-retention cleanup runs after preparation
or sync failure. Swapping a large buffer with an empty `Vec` avoids a new
cleanup-time allocation; a later large commit grows the buffer through the
existing fallible reservation path. Base-page payloads are released after each
group. The staged page payloads and compact per-page publication metadata
remain live until durability and index publication unless a later refinement
releases self-contained full Page payloads before publication. In either case,
the prepared payloads are not another page-sized copy.

Every preparation allocation uses `try_reserve` or `try_reserve_exact`,
including the transaction-wide duplicate set and prepared-page metadata, the
bounded page-group/base-page metadata, encoded-frame scratch, and delta
scratch. Reservation failure returns a contextual `DbError::Internal`, clears
all prepared publication state, and occurs before the first group write when
the requested capacity is known up front.

## Required Invariants

1. **Format identity.** Page, PageDelta, Commit, and header bytes retain their
   existing encodings and ordering. WAL format v8 is unchanged.
2. **Logical-end publication.** Page groups written beyond the old logical end
   are invisible until every page frame and the Commit frame are present and
   the header end-offset write completes.
3. **Durability before index publication.** `Full` and `Normal` retain their
   existing sync-before-index-publication behavior. `AsyncCommit`, deferred
   group commit, and testing-only no-sync modes retain their documented
   barriers and acknowledgement rules.
4. **Atomic reader view.** `wal_end_lsn` changes while holding the WAL index
   lock and only after all versions for the commit are installed, so a reader
   observes either the previous index/end pair or the complete new pair.
5. **Stable delta base.** Every base lookup uses the pre-commit index and the
   caller's snapshot LSN. A latest version spilled into the bounded-hot-set
   sidecar is promoted before main-database fallback and retains on-disk-WAL
   provenance. Prepared groups are not added to that index early.
6. **Duplicate-page recovery.** A repeated page ID within one commit is always
   encoded as a full page, including when the repeat crosses a group boundary.
7. **Failure recovery.** A preparation or group-write failure leaves the old
   logical end and in-memory index unchanged. Its unpublished physical tail is
   ignored on reopen and overwritten by a later commit. A failure after header
   publication retains the pre-existing ambiguous-commit behavior: recovery
   may observe the commit even though the caller received an I/O or sync error.
8. **Single writer.** The process writer guard and WAL writer-state lock remain
   held across all groups, the final publication write, the durability barrier,
   and index publication. Concurrent writers cannot interleave physical tails.
9. **Fallible preparation growth.** Capacity overflow or allocator refusal in
   WAL-owned preparation buffers returns an engine error rather than invoking
   an infallible `reserve` path. Prepared pages, base references, and encoded
   bytes are cleared before the error reaches the caller.
10. **Bounded idle frame scratch.** After encoded frame bytes have been written
    or preparation fails, an empty writer retains at most 256 KiB of encoded
    frame capacity. Releasing excess capacity does not change the WAL logical
    end, durability barrier, or index-publication order.

## Consequences

### Positive

- Base-page and encoded-frame preparation memory is bounded independently of
  transaction size: approximately 8 MiB combined at the default page size.
- A full rust-baseline seed should reduce these two transient buffers from
  roughly 276 MiB to roughly 8 MiB, a net reduction near 268 MiB. Huge scale
  should save roughly 1.37 GiB net.
- TDE no longer creates a transaction-sized encrypted copy of one giant frame
  batch; encryption scratch is bounded by the same group size.
- Duplicate detection uses a transaction-wide page-ID set rather than a
  quadratic scan of all previously prepared pages.
- A database handle does not retain a multi-megabyte encoded-frame allocation
  after a bulk commit. Heap profiling of the rust-baseline smoke seed found
  approximately 1.26 MiB retained at length zero before this idle cap.
- Readers can acquire the WAL index lock between preparation groups rather
  than waiting for every base page in a large transaction to materialize.

### Negative

- A large commit issues one positional write per preparation group instead of
  one positional write for all page frames. The 4 MiB group size balances
  bounded memory with syscall amortization.
- File preallocation may advance through multiple 16 MiB extents during a
  large commit instead of computing the final encoded length up front.
- The first commit larger than the 256 KiB idle-retention bound after a prior
  large commit must reserve encoded-frame scratch again.
- An unsuccessful large commit can leave unpublished bytes in the preallocated
  physical tail. This is safe because the header logical end remains old, but
  forensic tools must continue to treat the logical end as authoritative.
- Duplicate tracking adds transaction-proportional page-ID metadata. It is
  small relative to page payloads and replaces the previous quadratic scan.

## Alternatives Considered

1. **Keep whole-transaction preparation.** Rejected because two avoidable
   page-sized copies dominate peak memory in large durable commits.
2. **Encode every page twice.** A first pass could calculate the exact final
   length and a second pass could write bounded groups. Rejected because it
   repeats base lookup and delta encoding on the commit hot path.
3. **Publish each group as an independent commit.** Rejected because it would
   break transaction atomicity and expose partial commits to readers and
   recovery.
4. **Publish page versions to the in-memory index before durability.** Rejected
   because readers could observe data that recovery cannot guarantee and it
   violates ADR 0003.
5. **Change the WAL format to add group records.** Rejected because the current
   logical-end and Commit-frame protocol already provides the required atomic
   boundary; a new format would add migration and compatibility cost without
   benefit.
6. **Acquire the index lock once per page.** Rejected because it adds lock
   churn and reader/writer contention. One acquisition per approximately
   4 MiB group provides bounded bases while amortizing synchronization.

## Validation

- A commit spanning more than two preparation groups verifies that reusable
  encoded and base-page buffers remain bounded, every page is visible, and all
  pages survive WAL recovery.
- A duplicate page whose second occurrence crosses a group boundary verifies
  that the later frame remains a full Page frame and its payload wins.
- A deterministic write failure in a later group verifies that the logical end
  and index remain unchanged, recovery ignores the unpublished tail, and a
  subsequent commit overwrites it successfully.
- Unit tests verify that ordinary encoded-frame capacity remains reusable,
  over-bound capacity is released, and successful and failed large commits
  retain no more than the 256 KiB idle bound.
- Existing WAL delta, snapshot, checkpoint, coordination, async-commit,
  recovery, and fault-injection tests must retain their behavior.
- The repository has no deterministic process-allocation failure seam: its
  `FaultyVfs` intercepts storage operations only, and replacing the global
  allocator would affect the parallel test process rather than this WAL path.
  Consequently no synthetic allocation-failure test is added. The fallible
  reservations are covered by compile/clippy checks, while the shared cleanup
  path is exercised by the deterministic later-group VFS failure test.
- `cargo fmt --check`, `cargo check -p decentdb`, and clippy with warnings as
  errors must pass.

## References

- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0004-wal-checkpoint-strategy.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0068-wal-header-end-offset.md`
- `design/adr/0135-async-commit-wal-group-commit.md`
- `design/adr/0202-wal-frame-integrity-without-checksums.md`
- `crates/decentdb/src/wal/writer.rs`
- `crates/decentdb/src/wal/recovery.rs`
