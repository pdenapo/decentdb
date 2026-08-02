# ADR 0206: On-Disk Publication for Self-Contained WAL Pages

**Date:** 2026-08-01
**Status:** Accepted

## Context

ADR 0204 bounded base-page and encoded-frame preparation, but a large commit
still retained every input page vector until the durability barrier and then
moved every vector into the in-memory WAL index. The index therefore kept one
page-sized `Arc<[u8]>` per latest WAL version even when no reader needed old
history. The rust-baseline seed path can leave hundreds of MiB to more than a
GiB of full page images resident until checkpoint.

The existing WAL index already supports `OnDisk` versions. Recovery and the
read/checkpoint paths can materialize a full Page frame directly from its WAL
offset. A full Page frame is self-contained, unlike a PageDelta frame, and
does not require an earlier index version or main-database base to decode.

This decision changes the runtime WAL representation and commit hot path, so
it requires an ADR. It does not change WAL bytes, the database file format, or
checkpoint/durability semantics and therefore needs no format-version bump or
migration parser.

## Decision

After each bounded preparation group is encoded, release the input vector for
every full Page frame when no active reader or retained snapshot was observed.
Retain only compact publication metadata: page ID, frame offset, encoded
length, and the fact that the frame is a self-contained full page. After the
existing sync-mode barrier succeeds, publish that version directly as
`WalVersionPayload::OnDisk`.

PageDelta frames continue to retain their complete materialized page image and
are published as `Resident`. If an active reader or retained snapshot is
observed during preparation, newly encoded full pages also remain resident.
If a reader registers after an earlier group's full payload was released, the
writer retains the old versions under the index lock and safely publishes the
new self-contained version as `OnDisk`; the reader's snapshot never depends on
the new payload being resident.

Record delta-base provenance as one of:

- main database;
- resident WAL version; or
- on-disk WAL version.

A main-database page remains a valid delta base under the existing checkpoint
gating. A resident WAL version may remain a delta base under the existing
resident-version and hot-set policy. An on-disk WAL version never becomes the
sole base of a newly encoded delta, because clearing or spilling index history
could otherwise leave that delta without its required predecessor. Such a
write falls back to a self-contained full Page frame. Base lookup must consult
the bounded-hot-set sidecar before the main database; a promoted sidecar
version keeps on-disk-WAL provenance and therefore takes this full-frame path.

Fold the unchanged Commit marker into the final bounded page group. The final
group and header logical-end update remain one `write_all_at_many` publication
operation, so a small commit again performs two positional writes: one for
frames plus marker and one for the header end offset. Earlier groups remain
unpublished physical tail bytes. The preparation scratch bound increases by
only the fixed 13-byte Commit frame.

## Required Invariants

1. **Durability boundary.** The index and in-process `wal_end_lsn` change only
   after the existing `Full`, `Normal`, async, deferred-group, or testing-mode
   barrier completes according to that mode's contract.
2. **Logical atomicity.** Earlier groups remain beyond the old header logical
   end. The final frames, Commit marker, and header update preserve the same
   all-or-nothing recovery boundary as ADR 0204.
3. **Self-contained on-disk publication.** The writer directly publishes only
   full Page frames as `OnDisk`. PageDelta frames retain a materialized image.
4. **Reader history.** The retain-history decision remains inside the WAL
   index lock. A reader sees either the complete old index/end pair or the
   complete new pair, and its old visible versions are not cleared.
5. **Stable delta base.** A newly emitted PageDelta may not rely solely on an
   on-disk WAL version whose predecessor can leave the index.
6. **Delta demotion.** Explicit resident-byte demotion may demote only
   self-contained full Page frames. A resident PageDelta remains resident
   unless a future representation records and preserves a stable base.
7. **Recovery replay order.** Recovery reconstructs every committed latest
   page before applying the configured sidecar/hot-set spill. A later delta
   therefore always sees an earlier recovered page in the in-memory index.
8. **Failure state.** Preparation, frame-write, or sync failure does not modify
   the live index or in-process logical end. As before, a failure after the
   persisted header publication is an ambiguous commit on reopen; a retry from
   the old live end safely overwrites that tail.
9. **Format identity.** Page, PageDelta, Commit, header, checkpoint, and WAL
   version encodings are unchanged.

## Consequences

### Positive

- Reader-free large commits no longer turn every full staged page into a
  page-sized resident WAL-index allocation.
- Full-page input vectors are released group by group before sync rather than
  surviving for the complete transaction.
- The expected rust-baseline saving is approximately one page image per full
  WAL version: roughly 135 MiB at full scale and substantially more at huge
  scale before allocator effects.
- Small commits recover the pre-streaming two-write shape without weakening
  the logical publication or durability order.
- The runtime/index change requires no persistent-format migration.

### Negative

- Reading a newly committed full page through the WAL may require a positional
  read and allocation instead of cloning a resident `Arc`. Workloads that need
  repeated WAL reads should checkpoint or rely on higher-level resident row
  sources/page caches.
- A small update whose latest base is only an on-disk WAL version is encoded as
  a full Page frame. This trades WAL bytes for an explicit, safe base chain.
- Active readers can still require transaction-proportional resident history;
  that memory is necessary for snapshot correctness.
- Resident delta images are ineligible for explicit byte-target demotion until
  stable-base provenance is represented. Delta-heavy workloads may therefore
  retain more WAL-index memory than full-page workloads.
- Recovery applies the sidecar hot-set bound after complete replay rather than
  after each Commit, so its transient latest-page index can exceed the final
  configured hot set while opening a large WAL.
- The final bounded frame buffer permits 13 bytes beyond the ADR 0204 page
  payload bound.

## Alternatives Considered

1. **Demote only after index publication.** Rejected because it creates every
   page-sized `Arc` first and requires another index scan; allocator high-water
   behavior can retain much of the peak.
2. **Publish every frame type on disk.** Rejected because a PageDelta needs a
   stable predecessor chain and checkpoint can change its main-database base.
3. **Delta-encode against any materialized on-disk WAL version.** Rejected
   because materialization proves current readability, not that its predecessor
   remains indexed after latest-version replacement or hot-set spilling.
4. **Keep a transaction-wide full-page vector until sync.** Rejected because
   the encoded WAL batches already hold the bytes needed for the write and the
   full frame can be rematerialized after publication.
5. **Write the Commit marker separately.** Rejected for the final group because
   it adds a syscall to every small commit without improving the header-based
   atomicity boundary.

## Validation

- A commit spanning more than two preparation groups has zero resident full
  page payloads before reopen, bounded frame/base scratch, and recovers every
  page.
- A checkpoint materializes a mixed resident-delta/on-disk-full index and
  copies both page images back correctly.
- Subsequent writes use resident WAL deltas safely and fall back to full pages
  for on-disk WAL bases.
- With a one-page hot set, committing page A, spilling it by committing page B,
  and then rewriting page A emits a self-contained frame and preserves the
  latest image after reopen, even when that rewrite would be a tiny delta
  against the stale main-database image.
- Two updates to one page produce a delta against a resident WAL base; explicit
  byte-target demotion crosses that page without demoting it, and read,
  checkpoint, and reopen preserve both edits.
- Recovery of Page(7), Page(8), then a later PageDelta(7) with a one-page hot
  set reconstructs the delta before sidecar spill and preserves it through
  read, checkpoint, and reopen.
- An active old reader observes its original page while the latest snapshot
  observes the new page.
- Final-group write failure and WAL sync failure leave the old live logical
  state intact; ignored/ambiguous tails can be overwritten and reopened.
- A single-page commit performs exactly two WAL writes and survives reopen.
- Focused WAL tests, formatting, `cargo check`, and strict clippy must pass.

## References

- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0068-wal-header-end-offset.md`
- `design/adr/0135-async-commit-wal-group-commit.md`
- `design/adr/0204-bounded-wal-commit-preparation.md`
- `crates/decentdb/src/wal/index.rs`
- `crates/decentdb/src/wal/mod.rs`
- `crates/decentdb/src/wal/writer.rs`
