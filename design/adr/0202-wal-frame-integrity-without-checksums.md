## WAL Frame Integrity Without Per-Frame Checksums
**Date:** 2026-07-31
**Status:** Accepted

### Decision

Reaffirm ADR 0064's decision to omit per-frame CRC32C checksums and keep the
WAL format (v8) unchanged: the 8-byte frame trailer remains reserved and is
written as zeros (`crates/decentdb/src/wal/format.rs:20`, `:193`;
`crates/decentdb/src/wal/writer.rs:540`, `:713`). There is no format version
bump and no migration.

This ADR supersedes the now-stale portions of ADR 0064's rationale — its cited
mitigations "payload-size validation ... and LSN sanity checks" no longer exist
— and restates the integrity posture that actually remains, the residual risks
the project accepts, and the triggers that would require revisiting the
decision.

### Rationale

**History.**

- ADR 0064 (format v5) removed per-frame CRC32C because commit latency was
  dominated by checksumming full 4KB payloads, citing SQLite's lower commit
  latency without per-frame checksums. Its rationale leaned on three
  mitigations: payload-size validation, frame-type validation, and LSN sanity
  checks.
- ADR 0065 (format v6) then removed the per-frame LSN field; LSNs are now
  derived from WAL byte offsets, so no per-frame LSN sanity check survives.
- ADR 0066 (format v7) then removed the `payload_length` header field; payload
  sizes are derived from frame type and page size, so payload-length validation
  reduced to fixed sizes by type (`format.rs:47-55`).
- ADR 0068 (format v8) added the WAL header carrying the logical end offset.

Of 0064's three mitigations, only frame-type validation survives intact. The
performance motivation — no per-frame CRC over 4KB payloads on the commit hot
path — is unchanged and remains the governing constraint.

**Integrity mechanisms that exist today** (verified against current code):

1. WAL header validation: magic, header version 1 or 2, page-size match with
   the database, and logical-end sanity (0 or >= header size, and not beyond
   the physical file size) (`format.rs:98-119`,
   `crates/decentdb/src/wal/recovery.rs:53-67`).
2. Partial-tail tolerance: `decode_from_file` returns `None` when a frame
   header or body would cross the published logical end, and the recovery scan
   stops there (`format.rs:208-223`, `recovery.rs:74-79`; test
   `partial_frames_at_end_are_ignored`, `recovery.rs:547`).
3. Commit-boundary atomicity: recovery buffers page frames per transaction and
   publishes them to the WAL index only when a Commit frame is seen; a tail
   that ends mid-transaction is dropped wholesale (`recovery.rs:124-147`).
4. DoS bound: recovery rejects more than 1,000,000 uncommitted pending frames
   (`recovery.rs:18-20`, `:184-226`).
5. Structural validation: only 4 of 256 type-byte values decode
   (`format.rs:57-71`); page frames must carry a non-zero page id and non-page
   frames a zero page id (`format.rs:229-236`).
6. Bounds-checked delta decode: fixed 512-byte payload length, patch-header and
   patch-bytes overrun checks, and no writes past page end
   (`crates/decentdb/src/wal/delta.rs:87-141`).
7. Positional ordering: LSN = byte offset (ADR 0065), so replay order is the
   sequential scan order; the only surviving end-to-end sanity check is the
   header end offset versus file size (`recovery.rs:62-67`).
8. Durability boundary: `commit_pages` syncs the WAL before acknowledging in
   `Full` (`sync_data`/`sync_metadata`) and `Normal` (`sync_data`) modes
   (`writer.rs:188`, `:464-493`; `crates/decentdb/src/config.rs:36-60`);
   `AsyncCommit` defers sync to a background flusher with a documented
   durability window and an explicit `Db::sync` barrier (ADR 0135). Checkpoint
   frames are force-synced via `sync_durably` even under `AsyncCommit`
   (`writer.rs:441-447`, `:497-503`).
9. Never-panic recovery under corruption: `crates/decentdb/src/bin/wal_fuzz.rs`
   exercises six corruption strategies (truncate, flip_bits, bad_magic,
   random_bytes, zero_wal, oversized) and fails on any panic.

**Why the project accepts the residual risk for now.**

- Committed-data durability is bounded by sync-before-ack: in `Full`/`Normal`
  modes, everything up to the last acknowledged commit was fsynced, so the
  exposure is limited to the unflushed tail after the last sync.
- Mis-application of garbage requires a specific conjunction: a crash inside
  the post-write/pre-sync window, writeback reordering that persists the header
  end offset without all frame bytes, and garbage bytes that then decode as
  structurally valid frames (including delta bounds checks) and, to affect the
  index, a following decodable Commit frame.
- Recovery must never panic, and `wal_fuzz` covers that contract; a wrong
  decode surfaces as a bounded corruption error or ignored tail, not a crash.
- Structural guards (type byte, page-id rules, fixed per-type lengths, delta
  bounds, 1M-frame cap) bound the blast radius of any false accept; the pager
  tolerates out-of-range page ids by materializing zeroed pages
  (`crates/decentdb/src/storage/pager.rs:322-338`).
- The performance motivation that drove ADR 0064 still holds; no field
  evidence yet justifies paying the per-frame checksum cost.

### Alternatives Considered

- Reinstate per-frame CRC32C (format v9): restores payload-corruption
  detection but reintroduces the hot-path cost ADR 0064 removed and forces a
  format bump plus a `decentdb-migrate` reader. Deferred absent field evidence.
- WAL header salt / per-commit cumulative checksum (already deferred in 0064):
  catches garbage tails at commit granularity with far less per-frame cost, but
  still requires a format change; remains the preferred option if revisiting.
- Sync between the frame batch and the end-offset publish: closes the
  reorder window but adds a per-commit fsync, defeating the batched-append
  design (ADR 0068) and the group-commit work (ADR 0135).
- Publish the end offset before writing frames: strictly worse; recovery would
  scan unwritten preallocated space with no checksum to stop it.

### Trade-offs

Residual accepted risks, stated plainly:

- **End-offset/frame writeback-reorder window.** The new logical end offset
  (header bytes 16..24) is written in the same unsynced `write_all_at_many`
  batch as the frame bytes (`writer.rs:181-184`, `:350-353`, `:436-439`). The
  in-code ordering comment (`writer.rs:185-186`) guarantees syscall order, not
  durable order; a crash can persist the new end offset without all frame
  bytes, and recovery has no checksum to reject the resulting garbage.
- **Garbage-tail false accepts.** A garbage type byte has a 4/256 (~1.6%)
  chance of decoding as a valid frame type per trailing frame position, and
  the page-id check accepts any non-zero `u32` for page frames; a false Page
  frame followed by a decodable Commit frame would be indexed and could extend
  `max_page_count` (`recovery.rs:84-98`, `crates/decentdb/src/wal/mod.rs:448`).
- **Undetectable committed-payload corruption.** Corruption inside a
  fully-written, synced, committed frame — whether from media errors or
  misdirected writes — is applied silently; nothing in the format can detect
  it. This was the known cost recorded in 0064 and is unchanged.

Explicit triggers that require revisiting this decision (with reinstating a
trailer checksum or header salt in a new format version as the expected
remedy):

- any field report of silent WAL mis-replay or post-crash corruption that
  survives `Full` sync mode;
- a fuzzer- or test-discovered path where garbage frames decode and are
  applied as committed data;
- new storage-media or power-fail evidence that writeback reordering within a
  batched `pwrite` is observable on supported platforms;
- any future WAL format change, which must fold integrity metadata into the
  new version per the ADR-required-decision policy.

### References

- `design/adr/0064-wal-frame-checksum-removal.md` (decision reaffirmed here;
  mitigations rationale partially superseded)
- `design/adr/0065-wal-frame-lsn-removal.md`
- `design/adr/0066-wal-frame-payload-length-removal.md`
- `design/adr/0068-wal-header-end-offset.md`
- `design/adr/0135-async-commit-wal-group-commit.md`
- `crates/decentdb/src/wal/format.rs`, `wal/writer.rs`, `wal/recovery.rs`,
  `wal/delta.rs`, `crates/decentdb/src/bin/wal_fuzz.rs`
- `design/SPEC.md` §4.1 (WAL frame format)
