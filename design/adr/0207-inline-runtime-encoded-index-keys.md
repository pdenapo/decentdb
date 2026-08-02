# ADR 0207: Inline Runtime Encoded Index Keys

**Date:** 2026-08-01
**Status:** Accepted

## Context

Runtime B-tree indexes use the same sortable byte encoding as persistent index
keys. The runtime representation was an owned `Vec<u8>`, and the fast path for
a single indexed column first cloned the source `Value` and then allocated the
encoded vector. Common text indexes therefore performed two heap allocations
per inserted row. The rust-baseline artist-name and album-title indexes contain
millions of short keys whose complete encoding is at most 16 bytes on the
64-bit benchmark targets.

Non-unique encoded indexes also traversed every posting during commit-time
compaction, even when every posting remained a singleton and could not release
any capacity. That made a no-op maintenance pass proportional to the number of
distinct encoded keys.

These structures are derived runtime state. They are not stored in the
database, WAL, catalog, or migration formats.

## Decision

Use an internal `RuntimeEncodedKey` backed by a pointer-width-specific,
union-layout `SmallVec` for runtime encoded B-tree keys:

- 64-bit targets use `SmallVec<[u8; 16]>`;
- supported 32-bit targets, including `wasm32-unknown-unknown`, use
  `SmallVec<[u8; 8]>`; and
- compile-time assertions require the selected representation to remain
  exactly the size of `Vec<u8>` on every compiled target.

The canonical key encoder can write directly into that representation:

- encodings no larger than the target's inline capacity remain inline;
- longer encodings spill to owned heap storage;
- byte content and lexicographic ordering remain identical to the existing
  persistent `Vec<u8>` encoding; and
- map lookup and range operations borrow key bytes rather than allocating a
  temporary owned key when ownership is unnecessary.

The single-column runtime-index fast path borrows the source `Value` through
encoding instead of cloning it. Composite keys retain their existing
`Row::encode()` allocation. Converting that `Vec` into `RuntimeEncodedKey` may
copy it into inline storage when both its length and source capacity fit the
target's inline capacity; it is not required to remain spilled. Persistent-key
paths retain their existing representation and semantics.

Non-unique encoded postings use an inline-singleton enum on 64-bit targets.
On 32-bit targets an `i64`-carrying enum would grow beyond the former
three-word `Vec<i64>` footprint because of alignment, so postings remain
Vec-backed there. A second compile-time assertion requires the selected
posting representation to remain exactly `Vec<i64>`-sized on every target.

Track whether a non-unique encoded index has ever promoted a singleton posting
to a multi-row posting since its last compaction. Commit-time compaction visits
encoded postings only while that state says a multi-row allocation may be
reclaimable. Insert, update, delete, resurrection, rebuild, and copy-on-write
transitions maintain the state conservatively: a false value may skip the
traversal only when no posting can own spare multi-row capacity; false
positives are permitted and merely retain the old scan.

## Compatibility and Invariants

- Persistent key bytes and comparison order are unchanged.
- Database, WAL, catalog, index, and migration formats are unchanged.
- SQL uniqueness, lookup, range, update, delete, rollback, and snapshot
  behavior are unchanged.
- Long keys retain an owned representation and cannot borrow caller storage.
- Runtime keys and non-unique posting owners remain the same size as their
  former `Vec` representations on both supported 64-bit and 32-bit targets;
  inline storage must not increase every index entry's footprint.
- Derived runtime indexes remain copy-on-write through their existing `Arc`
  ownership boundaries.

## Alternatives Considered

1. **Keep `Vec<u8>`.** Rejected because short-key indexes pay a heap allocation
   for bytes that fit inside the former vector object's footprint.
2. **Intern strings or key bytes.** Rejected because an interner adds hashing,
   lifetime management, and another table while most benchmark keys are
   distinct.
3. **Use a transaction-sized arena.** Rejected because runtime index keys must
   outlive the transaction and snapshots retain copy-on-write index state.
4. **Use 16 inline bytes on every target.** Rejected because it would enlarge
   every wasm32 map node from 12 to 20 bytes. Sixteen bytes covers the common
   64-bit benchmark names; eight bytes is the largest union-layout capacity
   that preserves the 12-byte `Vec` owner size on supported 32-bit targets.
5. **Always skip posting compaction.** Rejected because genuine multi-row
   postings can retain significant excess capacity after deletes.

## Consequences

### Positive

- Common 64-bit single-column text inserts avoid both the source-value clone
  and the encoded-key heap allocation. Shorter keys receive the same benefit
  on supported 32-bit targets.
- Borrowed lookup and range probes avoid temporary key allocation.
- All-singleton non-unique encoded indexes avoid a commit-time O(number of
  keys) no-op traversal.
- No format migration or durability change is required.

### Negative

- The runtime and persistent encoded-key owner types differ internally.
- The `smallvec` union feature and pointer-width-specific capacities become
  part of the core crate's layout assumptions and require compile-time size
  assertions plus target checks.
- Non-unique singleton postings remain heap-backed on 32-bit targets to avoid
  growing every posting owner.
- Dirty-state maintenance adds conservative bookkeeping to encoded posting
  mutations.

## Validation

- Frozen golden byte vectors and ordering tests cover every supported `Value`
  kind independently of either encoder path, including the inline/spilled
  boundary and existing spatial-key errors.
- Production compile-time assertions and target checks prove that
  `RuntimeEncodedKey` and `RuntimeEncodedRowIds` remain `Vec`-sized on 64-bit
  and wasm32. Structural tests cover borrowed B-tree lookup, removal, and
  ranges across inline and spilled keys.
- Runtime-index tests cover unique and non-unique inserts, singleton-to-many
  promotion, many-to-one/empty reduction, remove-everywhere recomputation,
  post-shrink re-promotion, update/delete/resurrection, copy-on-write shrink
  state, borrowed ranges, and checkpoint/close/reopen SQL queries for short and
  long unique and non-unique keys.
- Formatting, core checks, strict clippy, focused executor tests, and a
  `wasm32-unknown-unknown` core check must pass.
- Repeated rust-baseline trials must confirm seed-time and peak-RSS effects
  without changing workload, durability, or result semantics.

## References

- `design/adr/0036-integer-primary-key.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0203-compact-dense-runtime-int64-index.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
