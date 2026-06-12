# ADR-0030 — Segment manifest & retirement: the checkpoint record names the live segment set

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** Project owner + systems design
- **Related:** [02 §3](../02-architecture.md#3-storage-engine-internals) · [02 §12](../02-architecture.md#12-cross-cutting-architectural-invariants) (invariants 1 & 2) · [ADR-0002](0002-on-disk-storage-format.md) · [ADR-0023](0023-append-only-record-model-validity-index.md) · STL-231 / STL-177 / STL-186

## Context

History-preserving compaction (STL-231, [roadmap v0.3](../03-roadmap.md#v03--historization-indexing--operability))
merges N small sealed segments into consolidated, read-optimized output
segment(s) and then **atomically swaps** the live segment set — inputs are
retired, never mutated (invariant 1). That swap needs a durable answer to "which
segments are live?" that can change in one atomic step.

Today that answer is implicit. The checkpoint file (`stele.checkpoint`,
STL-177) records `{replay_floor, durable_fence, segment_count}`: the live set is
*by definition* the contiguous prefix `seg-0 … seg-{count-1}`, recovery scans
the data dir, and any `seg-*` file at a higher index is an **orphan** (a torn
flush) it removes. A count can only grow the set at the tail — it cannot express
"segments 0–4 were replaced by segment 5", so compaction is unrepresentable.

Constraints: the swap must be a **single atomic, durable step** (a crash leaves
either the inputs live or the outputs live, never half); flush must keep its
existing atomic commit point (one fsync'd record vouches the new segment *and*
advances the replay floor — splitting those across two files would un-atomize
flush); sealed segments stay immutable (invariant 1); the WAL remains the only
source of truth, with the manifest derived/auxiliary in the same sense as the
checkpoint today ([ADR-0023]); and a v0.2 data directory must keep booting.

Options considered:

- **(a) A contiguous live *range* `[first, last)`** — fixed-size record, but it
  can only express compacting a *prefix* of the set into a fresh tail segment.
  Partial compaction (merge the K oldest of N, keep the rest) and the planned
  time-era compaction ([ADR-0021]) both produce non-contiguous live sets; the
  model would be outgrown within the same milestone.
- **(b) A separate manifest file** beside the checkpoint. Flush must then update
  two files (the floor in one, the set in the other) — there is no atomic
  two-file commit on the [`Disk` contract](../02-architecture.md#37-pluggable-storage-backends),
  so flush loses its single commit point.
- **(c) Generalize the checkpoint record to carry the live set explicitly.**
  The checkpoint *already is* the segment manifest in all but name — one
  append-only, CRC'd, fsync'd record per transition, last-valid-record-wins,
  torn-tail tolerant. Naming the set explicitly subsumes the count.

## Decision

**The checkpoint record grows into the segment manifest: it carries the explicit
list of live segment indexes, and one fsync'd record append is the single atomic
commit point for *every* segment-set transition — flush adds, compaction swaps.**

- **Record format.** A new variable-length record (magic `STMF`) in the same
  `stele.checkpoint` file: `magic | replay_floor | durable_fence |
  live_count(u32) | live_indexes(u64 × count) | crc32c`. The prior fixed-length
  `STCK` record remains decodable — its count means the contiguous prefix
  `[0, count)` — so a v0.2 data dir boots unchanged and is upgraded by its next
  flush/compaction appending an `STMF` record. The load scan dispatches on the
  per-record magic and keeps the existing torn-tail semantics: the last record
  that decodes wins; a torn or corrupt tail falls back to the prior good record.
- **Dead = not in the live set.** Recovery removes (best-effort) any `seg-*`
  file whose index is not in the live list. This single rule subsumes both
  populations: an **orphan** (written by a flush/compaction whose manifest
  record never became durable — STL-177's crash-during-flush safety) and a
  **retired** input (superseded by a committed compaction whose post-commit
  cleanup was interrupted). Neither is ever opened or trusted.
- **Compaction protocol** (mirrors flush's ordering): write the consolidated
  output segment(s) at fresh indexes and fsync them; `sync_dir` so the entries
  are durable before anything vouches for them ([STL-232]); append + fsync one
  manifest record whose live list names the outputs **instead of** the inputs —
  *this is the swap*; only then remove the retired input files, best-effort.
  A crash before the manifest append leaves the inputs live and the outputs
  orphaned; a crash after leaves the outputs live and the inputs retired —
  never half.
- **Index allocation is monotone.** Outputs (flush or compaction) always take
  indexes strictly above every index ever committed; on boot the next index is
  `max(live) + 1` (0 when empty). A retired index is never reused, so a
  lingering retired file can never be confused with a live one of the same name.
- **Retirement is not mutation.** Invariant 1 ("no in-place mutation of a sealed
  segment") is untouched: a live segment's bytes never change; compaction
  *rewrites into new segments* and retires inputs whole. The immutability oracle
  (STL-186) extends to compaction as: every segment name present before and
  after an operation has byte-identical content — disappearance via retirement
  is legal, mutation is not.

## Consequences

### Positive
- Compaction's atomic swap reuses the proven flush commit machinery — one
  record, one fsync, last-valid-wins recovery — rather than introducing a second
  durability protocol.
- Arbitrary set transitions (partial compaction, time-era clustering,
  multi-output splits) are expressible without another format change.
- The orphan/retired distinction collapses into one recovery rule, simpler than
  the count-threshold special case it replaces.

### Negative / costs
- Variable-length records: the manifest scan loses its fixed stride and must
  bound the decoded list length before allocating (a corrupt count must not
  drive a huge allocation; the CRC then rejects the record).
- The record grows with the live-set size (8 bytes per live segment).
  Compaction itself keeps the set small; the file still grows one record per
  transition and trimming it remains the follow-up noted since STL-177.
- Two record formats to decode for the foreseeable future (`STCK` legacy,
  `STMF` current).

### Neutral / follow-ups
- Background/scheduled compaction (this ADR covers only the durable model and
  the manual admin trigger); segment-set *selection* policy (size-tiered,
  time-era per [ADR-0021]) is deliberately out of scope.
- The manifest stays per-table (per-namespace), like the checkpoint it extends;
  a session-level manifest is a v0.5+ object-store concern
  ([02 §10](../02-architecture.md#10-deployment--topology-evolution-by-roadmap-phase)).

[ADR-0021]: 0021-storage-lifecycle-tiered-archival.md
[ADR-0023]: 0023-append-only-record-model-validity-index.md
[STL-232]: https://allegromusic.atlassian.net/browse/STL-232
