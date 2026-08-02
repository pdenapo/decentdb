# ADR 0203: Compact Dense Runtime INT64 Indexes

**Date:** 2026-08-01
**Status:** Accepted

## Context

DecentDB maintains in-memory runtime B-tree indexes for constraint enforcement
and query execution. A unique, non-null, single-column `INT64` index currently
uses an identity-hashed `HashMap<i64, i64>` from index key to row ID.

ADR 0036 makes a single `INTEGER PRIMARY KEY` value the row ID. Common append
workloads therefore populate their primary-key runtime index with mappings such
as `1 -> 1, 2 -> 2, ...`. The rust-baseline music workload has this shape for
artists, albums, and songs. At huge scale those three indexes contain roughly
27.75 million redundant key/value pairs. They consume hundreds of megabytes and
perform a hash-table insertion for every seeded row even though the complete
mapping can be described by a start value and a length.

This runtime state is derived from table rows and catalog metadata. It is not
part of the database or WAL format.

## Decision

Represent unique typed `INT64` runtime keys through an internal
`UniqueInt64Keys` abstraction with two representations:

1. `DenseIdentity { start, len }` represents one contiguous range in which
   every key maps to the identical row ID.
2. `Sparse(Int64Map<i64>)` preserves the existing identity-hashed map for all
   other mappings.

An empty unique `INT64` index starts dense. An insert extends the dense range
when the new mapping is an adjacent identity mapping. A duplicate inside the
range reports the existing row ID with the same semantics as `HashMap::insert`.
An out-of-order, gapped, or non-identity mapping converts the range to the
sparse representation before applying the insert. Rebuilds use the same
abstraction so a checkpointed database reopens into the compact form when its
rows still form a dense identity range.

Logical delete visibility remains in the existing per-index deleted-row-ID set.
Reinserting the same identity mapping may clear that tombstone without
expanding the dense range. A mutation that must remove or replace a mapping
inside the dense range falls back to the sparse representation before changing
the mapping.

The abstraction provides lookup, iteration, count, mutation, and compaction
operations so every existing runtime-index consumer observes the same logical
key-to-row-ID relation. Dense iteration yields owned `(i64, i64)` pairs rather
than references because its entries are computed rather than allocated.

## Scope and Compatibility

- This is an in-memory representation change only.
- Database, WAL, catalog, index, and migration formats are unchanged.
- SQL uniqueness, lookup, ordering, update, delete, rollback, and snapshot
  semantics are unchanged.
- Sparse and non-identity unique `INT64` indexes retain the existing hash-map
  behavior.
- Encoded, UUID, and non-unique runtime index representations are unchanged.
- Durability and the one-writer/many-readers model are unchanged.

## Alternatives Considered

1. **Keep the identity-hashed map.** Rejected because it stores two integers
   plus hash-table control/capacity overhead for a mapping already implied by
   ADR 0036 and spends CPU hashing every append.
2. **Add a new `RuntimeBtreeKeys` variant.** Rejected because it would expand
   every exhaustive match across the executor. Hiding the representation
   behind the existing `UniqueInt64` variant keeps the semantic type stable and
   narrows the correctness audit.
3. **Store a dense `Vec<i64>`.** Rejected because identity values remain
   redundant and still require eight bytes per entry.
4. **Use multiple compressed ranges plus overlays immediately.** Deferred. It
   avoids a potentially expensive dense-to-sparse conversion after a very
   large range, but adds mutation and iteration complexity not required for the
   common sequential identity workload. The sparse fallback is deliberately
   conservative and preserves all behavior.
5. **Remove the runtime primary-key index entirely.** Rejected for this phase.
   Runtime indexes also enforce constraints and serve executor paths; removing
   one based on catalog role would require a broader planner and constraint
   audit.

## Consequences

### Positive

- Sequential integer primary-key inserts avoid one hash-table insertion and
  allocation-growth path per row.
- Dense unique integer indexes use constant payload space instead of space
  proportional to row count.
- Reopen/rebuild can recover the compact representation without a format or
  migration change.
- Sparse indexes keep their existing general semantics.

### Negative

- The abstraction and owned iterator add internal executor complexity.
- The first incompatible mutation after a large dense range must materialize a
  sparse map and is therefore proportional to the range length.
- Callers must not rely on hash-map iteration returning borrowed pairs.

## Validation

- Unit tests cover empty, append, prepend, duplicate, sparse-gap,
  non-identity, delete/reinsert, and mapping-replacement behavior.
- Existing runtime index, DML, constraint, update/delete, and query tests must
  pass unchanged semantically.
- `cargo fmt --check`, `cargo check -p decentdb`, and clippy with warnings as
  errors must pass.
- The rust-baseline medium/full/huge runs must confirm seed-time and peak-RSS
  effects without changing workload, durability, or result semantics.

## References

- `design/adr/0036-integer-primary-key.md`
- `design/adr/0092-integer-pk-auto-increment.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0200-resident-table-delete-tombstones-and-format-14.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
