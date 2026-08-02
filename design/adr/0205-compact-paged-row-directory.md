# ADR 0205: Compact Paged-Row Directory

**Date:** 2026-08-01
**Status:** Accepted

## Context

Paged resident tables currently keep one `TablePageEntry` for every visible
row. Each entry repeats the row ID and chunk index beside an eight-byte
`RowLocatorV1`. Alignment makes the entry substantially larger than the
locator itself. The huge rust-baseline dataset therefore spends hundreds of
megabytes describing rows whose IDs are the contiguous sequence `1..=N` and
whose chunks already retain physical row grouping.

Deferred paged tables independently build an identity-hashed
`Int64Map<CachedPagedRowLocator>`. For the same contiguous tables this repeats
the row ID, chunk pointer, checksum, and locator for every row even though only
the eight-byte locator varies within a chunk. Building that map also hashes and
inserts every row while reopening or preparing a deferred runtime index.

Both structures are runtime-derived accelerators. Row IDs and chunk payloads
remain authoritative in the existing table payload and paged-manifest formats.

## Decision

Introduce a shared runtime-only dense paged-row directory for the case where:

- every visible row ID is contiguous and increasing in physical chunk order;
- chunks have no tombstones or overlay payloads; and
- every row boundary can be parsed into the existing bounded locator fields.

The dense directory stores:

1. one starting row ID;
2. one eight-byte `RowLocatorV1` per visible base row; and
3. cumulative per-chunk row ends used to resolve a row position to its chunk.

Row ID lookup subtracts the starting row ID, validates the resulting position,
uses the locator at that position, and selects the owning chunk from the
cumulative ends. Iteration derives row IDs from the start and position. This
removes the per-row row ID, chunk index, and overlay flag.

`TablePageManifest` uses an internal directory enum with dense and sparse
representations. Construction and reopen attempt the dense representation
without first allocating sparse entries. A gap, out-of-order row ID, physical
tombstone, overlay, update, delete, resurrection, or incompatible append uses
the existing sparse `TablePageEntry` representation. A contiguous append can
extend the dense locator vector and its final chunk range directly.

The deferred locator cache reuses the same dense directory. It stores chunk
pointer/checksum metadata once per chunk and derives a
`CachedPagedRowLocator` on lookup. It does not build an `Int64Map` for a dense
table. Sparse, overlay, and tombstoned deferred tables retain the existing map
fallback and verified-payload cache.

Large directory and sparse-map reservations use fallible reservation and
return a database error rather than relying on an infallible transaction-sized
allocation.

## Compatibility and Invariants

- Table payload, paged-manifest, WAL, catalog, and migration formats do not
  change.
- Stored row IDs remain authoritative and are validated while constructing a
  dense directory; density is never inferred from catalog counts alone.
- Release construction parses and bounds-checks the row stream without eagerly
  decoding every value. Row payloads retain the existing decode-on-access
  validation; debug builds also decode eagerly while constructing a directory.
- `row_by_id`, positional iteration, projection scans, range lookup, updates,
  deletes, overlay replacement, resurrection, and chunk restoration observe
  the same visible row ordering and corruption checks.
- Dense and sparse directories remain copy-on-write through the enclosing
  runtime manifest `Arc`; reader snapshots never observe in-place mutation.
- Existing persistent primary-key locators remain independent and unchanged.

## Alternatives Considered

1. **Pack the existing entry struct.** Rejected because unaligned fields would
   complicate safe access and still repeat row IDs and chunk indexes.
2. **Use a dense vector of complete cached locators.** Rejected because chunk
   pointers and checksums would remain duplicated per row.
3. **Infer byte offsets by rescanning chunks.** Rejected because variable-width
   rows would turn point lookup and projection into repeated linear scans.
4. **Require row IDs to begin at one.** Rejected because any contiguous signed
   range can use direct indexing safely with an explicit start value.
5. **Keep dense storage through tombstone and overlay deltas.** Deferred. A
   base-plus-exception design could compress more mutation-heavy tables, but a
   conservative sparse fallback makes the first implementation easier to
   audit and preserves all existing mutation behavior.

## Consequences

### Positive

- Dense resident manifests use eight bytes per row plus small per-chunk
  metadata instead of a full `TablePageEntry` per row.
- Dense deferred caches avoid one hash-map entry and hash insertion per row.
- Manifest construction, reopen, and deferred index preparation share one
  density-validation scan and lookup contract.
- No migration or durability compatibility work is required.

### Negative

- Directory consumers must resolve owned logical entries rather than borrowing
  a concrete per-row struct.
- The first incompatible mutation materializes the sparse directory and is
  proportional to the table's visible row count.
- Mutation-heavy or naturally gapped tables receive no directory compression.

## Validation

- Structural tests assert the eight-byte locator and dense/sparse
  representation selection, plus corruption-safe locator bounds handling.
- Runtime tests cover contiguous construction and append, gapped/out-of-order
  fallback, ranges and positional scans, tombstone/overlay update and
  resurrection, and copy-on-write behavior.
- File-backed SQL tests cover checkpoint, reopen/rebuild, point lookup,
  projection, update, delete, and sparse fallback.
- Deferred tests assert that contiguous tables use no per-row locator map and
  that sparse tables retain correct lookup behavior.
- Formatting, strict clippy, focused tests, and the full core suite must pass.

## References

- `design/adr/0036-integer-primary-key.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0200-resident-table-delete-tombstones-and-format-14.md`
- `design/adr/0203-compact-dense-runtime-int64-index.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
