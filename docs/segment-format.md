# Sealed segment format — on-disk spec

> **Status:** Living spec, current as of **format version 14**. Unlike the
> [hash-key spec](hash-key-v1.md), the on-disk segment format is **not frozen**:
> it is a *pre-1.0* surface that may break between minor versions, each break
> documented and signalled by a header version bump ([roadmap §versioning](03-roadmap.md#versioning),
> [ADR-0014](adr/0014-release-channels-and-versioning-policy.md)). It
> **freezes forward at v1.0**. This document is the human-readable companion to
> the canonical, exhaustively-commented source:
> [`crates/stele-storage/src/segment/`](../crates/stele-storage/src/segment/)
> (`format.rs` for the constants, `writer.rs` for the byte layout, `reader.rs`
> for the parse). When this doc and the code disagree, the code wins — and that
> is a bug in this doc.
>
> **Related:** [ADR-0002](adr/0002-on-disk-storage-format.md) (the decision to
> own a custom format) · [ADR-0023](adr/0023-append-only-record-model-validity-index.md)
> (no stored `sys_to`) · [ADR-0024](adr/0024-time-representation.md) (µs/`+∞`
> time) · [ADR-0025](adr/0025-valid-time-indexing.md) (valid-time index) ·
> [architecture §3.2](02-architecture.md#32-on-disk-segment-format) ·
> [assumptions A8, O2](assumptions.md).

## Why this exists

A **sealed segment** is the immutable, columnar bulk-storage half of the storage
engine: the row-oriented WAL + delta tier flushes recent writes into one of these
files, and from then on the file is never mutated in place
([architecture §3.1–3.2](02-architecture.md#3-storage-engine-internals),
[ADR-0002](adr/0002-on-disk-storage-format.md)). The format is the **least
reversible** artifact in the system — once real data exists in it, every future
change is a migration — so it is specified here, byte for byte, rather than left
to be read out of the encoder.

The format is designed *around the bitemporal, append-only record model*: time
axes and provenance are first-class columns, a version carries only **birth
state** (there is no stored period end), and logical deletes persist as durable
tombstones so a from-scratch index rebuild can never resurrect a deleted row.
Parquet/Arrow are interop formats, not this — see
[ADR-0002](adr/0002-on-disk-storage-format.md) for that decision.

## Conventions

- **Endianness.** Every multi-byte integer is **little-endian on disk**,
  independent of the host that wrote it (the writer always emits `to_le_bytes`,
  the reader always decodes LE), so a data directory is portable across
  architectures. The two magics are ASCII byte strings, not integers. (This is
  the opposite choice from the portable [hash-key spec](hash-key-v1.md), which is
  big-endian *because* an external client must reproduce it; the segment format
  is the engine's own on-disk artifact.)
- **Time.** All timestamps are `i64` **microseconds since the Unix epoch, UTC**;
  a valid-time period end of `i64::MAX` denotes an open ("until changed")
  interval ([ADR-0024](adr/0024-time-representation.md),
  [`stele_common::time::VALID_TIME_OPEN`](../crates/stele-common/src/time.rs)).
- **`u64` columns.** `txn_id` and `seq` are logically `u64`; they are stored in
  the `i64` column layout by a lossless bit reinterpretation (`u64 as i64` on
  write, `i64 as u64` on read). Only zone-map ordering would differ for values
  ≥ 2⁶³, unreachable in practice.
- **Checksums** are **CRC32C** (Castagnoli).

## File layout

```text
+=====================================================================+
| HEADER                16 bytes                                      |
+---------------------------------------------------------------------+
| BODY  (column data)                                                 |
|   ROW-GROUP 0         column chunks, in schema order                |
|   ROW-GROUP 1         (one row-group by default; the writer may     |
|   ...                  bound rows/group so a wide segment splits)   |
|   RETRACTION CHUNKS   tombstone columns (only if ≥1 logical delete) |
+---------------------------------------------------------------------+
| FOOTER  (variable)                                                  |
|   schema_id, flags, row-group metadata, retraction metadata,        |
|   then the optional trailing sections the flags announce:           |
|   [ bloom ] [ valid-interval summary ] [ per-row-group summaries ]  |
+---------------------------------------------------------------------+
| TRAILER               16 bytes                                      |
+=====================================================================+
```

The **column data** (version row-groups, then the retraction section) lives in
the body. The **column metadata** (offsets, lengths, codecs, zone-map stats) and
the advisory pruning structures (bloom, valid-interval summaries) live in the
footer. A reader seeks to the trailer first, reads `footer_len`, parses the
footer, then random-accesses exactly the column chunks it needs.

### Header (16 bytes)

| offset | size | field | value |
|---|---|---|---|
| 0 | 8 | magic | `"STLSEG\0\0"` (`HEADER_MAGIC`) |
| 8 | 2 | `format_version` | `u16` LE — **14** (`FORMAT_VERSION`) |
| 10 | 2 | flags | `u16` LE — reserved, currently always `0` |
| 12 | 4 | reserved | `u32` LE — `0` |

A reader checks the magic, then rejects any `format_version` that is not exactly
its own with `SegmentError::UnsupportedVersion` — a clean header-level reject, so
an older reader never half-parses a newer footer (see
[Version history](#version-history)). The first bytes of every segment are
therefore `53 54 4C 53 45 47 00 00  0E 00 00 00  00 00 00 00`.

### Body: column chunks

The body is a flat concatenation of **column chunks**, in this order:

1. each version **row-group**, in row order; within a row-group, one chunk per
   column in [schema order](#columns--the-implicit-schema);
2. the **retraction section** chunks (only when the segment holds ≥1 logical
   delete).

Each chunk is a 16-byte header followed by its payload:

**Chunk header (16 bytes, `CHUNK_HEADER_LEN`)**

| offset | size | field |
|---|---|---|
| 0 | 4 | `payload_len` (`u32` LE) |
| 4 | 4 | `value_count` (`u32` LE) |
| 8 | 1 | `codec` (`0` = Plain, `1` = Dict) |
| 9 | 3 | reserved (`0`) |
| 12 | 4 | `crc32c` (`u32` LE) over `chunk_header[0..12] ‖ payload` |

The CRC covers the first 12 header bytes plus the payload, so a flip anywhere
except the CRC field itself is detected, and a flip in the CRC field is detected
as a mismatch. `value_count` is the number of logical values in this chunk — the
row-group's row count for a version column, or the retraction count for a
tombstone column.

### Column payload encodings

A column's `ColumnType` is fixed by its id (below). Two physical types:

**`I64` — fixed width.** `value_count` × 8-byte little-endian `i64`. Always
stored `Plain`.

**`Bytes` — variable width.** Two codecs; the writer picks per chunk:

- **`Plain` (codec 0).** For each value, `[u32 len LE][len bytes]`. A SQL `NULL`
  cell is encoded as the reserved length sentinel `BYTES_NULL_SENTINEL`
  (`0xFFFF_FFFF`) and **no** value bytes. A present value can never reach that
  length — the version-frame ceiling `MAX_VERSION_FRAME_LEN` (16 MiB) bounds it
  far below — so NULL and present are always distinguishable. Only the `payload`
  column is ever NULL ([STL-154]).
- **`Dict` (codec 1).** A version-chain-aware dictionary ([STL-250]):
  `[u8 code_width][u32 dict_count LE][(u32 len LE, bytes) × dict_count][code × value_count]`.
  Each `code` is a `code_width`-byte LE index into the dictionary; a dictionary
  entry `len` of `0xFFFF_FFFF` marks a NULL entry. `code_width` is **1** byte for
  ≤256 distinct values, **2** for ≤65536, else **4** (`code_width_for`). The
  writer keeps a dictionary only when dictionary encoding is enabled *and* the
  result is **strictly smaller** than `Plain`, so an all-distinct column never
  grows — the "chosen by the writer from column statistics" rule of
  [architecture §3.2](02-architecture.md#32-on-disk-segment-format). A value
  repeated across a key's version chain (the *identical* `business_key`, a
  repeated `principal`/`payload`) is then stored once. Decoding is transparent
  behind the column-read API, so late materialization is unaffected and the
  zone-map stats below are identical for either codec.

The codec is self-describing (its tag rides in both the chunk header and the
footer entry), but adding a *new* codec still bumps `FORMAT_VERSION` so an older
reader rejects at the header rather than choking on an unknown codec byte
mid-footer.

### Footer

The footer is CRC-protected as a whole (see the trailer) and laid out as:

```text
schema_id            u32 LE   — implicit Version schema = 0 (SCHEMA_ID_IMPLICIT_VERSION)
flags                u32 LE   — optional-section bits (below); 0 = none
row_group_count      u32 LE
  per row-group:
    row_count        u32 LE
    column_count     u32 LE
    column meta × column_count        (entry layout below)
retraction_count     u32 LE   — shared value_count of the tombstone columns (0 if none)
retraction_col_count u32 LE
  retraction column meta × retraction_col_count
[ bloom section ]            — iff flags & FOOTER_FLAG_BLOOM (0x01)
[ valid-interval summary ]   — iff flags & FOOTER_FLAG_VALID_INTERVALS (0x02)
[ per-row-group summaries ]  — iff flags & FOOTER_FLAG_RG_VALID_INTERVALS (0x04)
```

The retraction section is **always present**, just empty (`0`/`0`) when the
segment holds no deletes, so the parse is unconditional. The three trailing
sections are present only when their flag bit is set, in the fixed order shown; a
footer with `flags == 0` is byte-identical to a pre-v11 footer.

**Column meta entry**

| size | field |
|---|---|
| 2 | `column_id` (`u16` LE) |
| 1 | `codec` |
| 1 | `stat_flags` (zone-map presence bits, below) |
| 8 | `offset` (`u64` LE) — absolute file offset of the chunk |
| 8 | `length` (`u64` LE) — chunk header + payload |
| 4 | `value_count` (`u32` LE) |
| 4 | reserved (`0`) |
| 4 | `min_len` (`u32` LE) + `min_len` bytes (zone-map min) |
| 4 | `max_len` (`u32` LE) + `max_len` bytes (zone-map max) |

### Zone maps (per-column min/max)

Each column meta carries a min and a max for **block skipping**: a scan that can
prove its predicate falls outside `[min, max]` skips the chunk without reading it
([architecture §3.5](02-architecture.md), [ADR-0025](adr/0025-valid-time-indexing.md)).
The bound is computed over the column's **logical** values (so it is codec-independent):

- **`I64`** — the 8-byte LE min and max. Always exact (an `i64` bound is always
  representable).
- **`Bytes`** — a **bounded lexicographic prefix**, capped at
  `MAX_BYTES_STAT_PREFIX_LEN` (**64** bytes), because a value can be up to 16 MiB
  and inlining a full bound could blow the footer's `u32` length ceiling. The
  **min** prefix is truncated *down* (a prefix is lex-`≤` its source, a sound
  lower bound); the **max** prefix is rounded *up* (so it stays `≥` every value
  sharing it). This keeps `might_contain`'s no-false-negatives contract for
  worst-case blobs.

**`stat_flags`** (`u8`, [STL-120]) disambiguates a *present-but-open* bound from
an *absent* one — both encode as a zero-length stat field:

| bit | name | meaning |
|---|---|---|
| `0x01` | `STAT_MIN_UNBOUNDED` | min is open below (−∞) |
| `0x02` | `STAT_MAX_UNBOUNDED` | max is open above (+∞) |

A bit set + zero-length field = an open end (arises only for a degenerate bytes
prefix: an empty lex-min, or an all-`0xFF` max with no shorter upper bound). A
bit clear + zero-length field = no stats (the column had no non-NULL values). A
bit clear + a non-empty field = a concrete bound. `i64` columns never set these
bits. An older reader that ignores `stat_flags` still reads a zero-length field
as "no stats" and merely forgoes the recovered pruning — never a wrong result —
which is why this change needed **no** version bump.

### Optional footer sections (advisory)

All three are **advisory**: they gate reads only and are **never** consulted to
produce a result, so toggling them changes scan *speed*, never answers. Each
rides the immutable segment it summarizes, so it survives flush, compaction, and
recovery with no separate structure to rebuild.

- **Bloom (`FOOTER_FLAG_BLOOM`, [STL-238]).** A per-segment business-key
  membership filter, so a point lookup or `MERGE` probe skips a whole segment
  whose bloom proves the key absent — the random/hash-key case zone maps cannot
  prune. Layout: `[u8 probe_count][u32 word_count LE][u64 word × word_count LE]`.
  `probe_count` is `BLOOM_HASHES` (**4**, double FNV-1a); the bit count is
  `word_count × 64`, a power of two. Sized at `DEFAULT_BITS_PER_KEY` (**12**)
  bits/key by default. Absent for an empty segment or when disabled.
- **Per-segment valid-interval summary (`FOOTER_FLAG_VALID_INTERVALS`,
  [STL-241]).** The coalesced union of the segment's `[valid_from, valid_to)`
  windows (at most `DEFAULT_VALID_INTERVAL_CAP` = **256** disjoint intervals,
  smallest gaps merged — a sound widening), so a `FOR VALID_TIME AS OF v` read
  skips a segment whose coverage has a *gap* at `v` — the backdated-write scatter
  case zone-map min/max cannot prune. Layout:
  `[u32 interval_count LE][(i64 from LE, i64 to LE) × interval_count]`. Only on a
  valid-time table's segments.
- **Per-row-group valid summaries (`FOOTER_FLAG_RG_VALID_INTERVALS`,
  [STL-316]).** One summary per row-group — the same refinement [STL-173] made
  for the system-axis zone maps. Layout: `[u32 row_group_count LE]` then, per
  row-group, either a present summary (`interval_count ≥ 1` then the pairs) or an
  **admit-all** marker (a bare `u32` `0`). Set exactly when the per-segment
  summary is.

### Trailer (16 bytes)

| offset | size | field |
|---|---|---|
| 0 | 4 | `footer_crc` (`u32` LE) — CRC32C over the whole footer |
| 4 | 4 | `footer_len` (`u32` LE) |
| 8 | 8 | magic `"STLSEGFT"` (`TRAILER_MAGIC`) |

The trailer sits at the very end so a reader can find the footer without first
decoding it: read the last 16 bytes, check the magic, take `footer_len`, then
read and CRC-check the footer immediately preceding the trailer.

## Columns — the implicit schema

v0.1 has exactly **one** schema, `schema_id = 0`: the always-on **Version**
columns, optionally extended with the valid-time pair for a valid-time table.
Real, catalog-resolved schema ids ride on a later ticket
([STL-98](https://allegromusic.atlassian.net/browse/STL-98)). Column ids are
**frozen** — they live in every footer — so additions take the next free value
and existing ids never renumber (hence the non-contiguous write order: `seq` and
`retract_seq` were appended at ids 14/15 but written in their logical position).

| id | name | type | group | notes |
|---|---|---|---|---|
| 0 | `business_key` | bytes | version | opaque key bytes |
| 1 | `sys_from` | i64 µs | version | system-time start; **no `sys_to`** is stored ([ADR-0023]) |
| 2 | `payload` | bytes | version | opaque payload; **the only nullable column** ([STL-154]) |
| 3 | `txn_id` | i64 (u64 bits) | version | provenance: writing transaction |
| 4 | `committed_at` | i64 µs | version | provenance: commit timestamp |
| 5 | `principal` | bytes | version | provenance: auth principal |
| 6 | `valid_from` | i64 µs | version | valid-time start — **valid-time tables only** ([STL-117]) |
| 7 | `valid_to` | i64 µs | version | valid-time end; `i64::MAX` = open — valid-time tables only |
| 8 | `retract_key` | bytes | retraction | deleted version's business key |
| 9 | `retract_sys_from` | i64 | retraction | deleted version's `sys_from` |
| 10 | `retract_closed_at` | i64 µs | retraction | when the period was closed |
| 11 | `retract_closed_by_txn` | i64 (u64 bits) | retraction | delete provenance: who |
| 12 | `retract_closed_by_committed_at` | i64 µs | retraction | delete provenance: when |
| 13 | `retract_closed_by_principal` | bytes | retraction | delete provenance: by whom |
| 14 | `seq` | i64 (u64 bits) | version | per-commit monotonic tiebreak for same-µs `sys_from` ([ADR-0024]) |
| 15 | `retract_seq` | i64 (u64 bits) | retraction | deleted version's `seq` |

**Version row-group write order** (`ColumnId::ALL`): `business_key`, `sys_from`,
`seq`, `payload`, `txn_id`, `committed_at`, `principal` — then `valid_from`,
`valid_to` for a valid-time table. **Retraction section write order**
(`ColumnId::RETRACTION`): `retract_key`, `retract_sys_from`, `retract_seq`,
`retract_closed_at`, `retract_closed_by_txn`, `retract_closed_by_committed_at`,
`retract_closed_by_principal`. The footer records each column's id and offset, so
write order is just convention and reader/writer cannot drift.

### Why there is no `sys_to`, and why retractions are stored

A version's system-time **end** is *not* a column. It is the `sys_from` of the
next version that supersedes the same `(key, valid region)`, materialized once
into the derived, rebuildable **validity index** ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)).
Storing only birth state is what makes the append-only/tamper-evidence claim hold
under scrutiny: nothing on the durable record can be rewritten to say a version's
period ended.

A **logical delete** ("close with no successor") cannot be reconstructed from
version adjacency — an adjacency-only rebuild would resurrect the row across the
deletion gap ([16 §12](16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap)).
So a delete persists as a payload-less **tombstone** in the retraction section
(the [`Close`](../crates/stele-storage/src/validity/index.rs) fields above), and the
segment store is **self-contained for a from-scratch index rebuild** even after
WAL truncation — the required oracle in [16 §12](16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap).

### Valid-time payload framing

On a valid-time table the interval is **not** stored twice. The delta tier frames
each payload with a 16-byte (`VALID_TIME_PREFIX_LEN`) valid-time prefix; at flush
the writer lifts that prefix into the first-class `valid_from`/`valid_to` columns
and stores only the **bare user payload** in the `payload` column ([STL-117],
[STL-119]). A reader re-frames the payload from those columns on read. A
system-only segment stores the payload verbatim.

## Integrity & immutability

- **Two-level checksums.** Every column chunk has its own CRC32C (over
  `header[0..12] ‖ payload`); the footer has its own CRC32C (in the trailer). A
  single-byte flip anywhere in a chunk payload or the footer is caught at read
  time as `SegmentError::Corrupt`. A torn write (a crash mid-seal) leaves a
  malformed segment the reader rejects — and the WAL drives re-flush, since
  sealed segments are downstream of the only durability point.
- **Immutability is enforced by the absence of a write path**, not a runtime
  check ([architecture §12, invariant 1](02-architecture.md#12-cross-cutting-architectural-invariants)).
  `SegmentWriter::create` routes through `Disk::create`, which fails with
  `AlreadyExists` on an existing name; there is no `open`-for-append surface, and
  `SegmentReader` never calls `append`/`sync`. A sealed segment cannot be
  reopened for mutation through this API.

## Version history

Each bump is a backwards-incompatible layout change rejected cleanly at the
header by an older reader (`SegmentError::UnsupportedVersion`). There is no
read-compat shim and no rewrite tool pre-1.0: no released data exists to migrate,
and the validity index is rebuildable from the log, so the "migration" for a
pre-1.0 bump is the clean reject ([segment `mod.rs` migration note](../crates/stele-storage/src/segment/mod.rs)).

| version | change | ticket |
|---|---|---|
| v1 | four-column implicit `Version` schema (`business_key`, `sys_from`, `sys_to`, `payload`) | — |
| v2 | + always-on provenance columns (`txn_id`, `committed_at`, `principal`) | STL-93 |
| v3 | + per-table opt-in valid-time pair (`valid_from`, `valid_to`) | STL-117 |
| v4 | + close-provenance columns (later removed) | STL-118 |
| v5 | stop duplicating the valid-time interval (bare payload + reframe on read) | STL-119 |
| v6 | **drop stored `sys_to` and close-provenance** → derived validity index | STL-133, [ADR-0023] |
| v7 | persist retractions as payload-less tombstone rows (retraction section) | STL-143 |
| v8 | + always-on per-commit `seq` column | STL-141 |
| v9 | + `seq` on the retraction tombstone | STL-145 |
| v10 | let `payload` carry SQL `NULL` (`BYTES_NULL_SENTINEL`) | STL-154 |
| v11 | + per-segment business-key bloom section | STL-238 |
| v12 | + per-segment valid-time interval summary section | STL-241 |
| v13 | version-chain-aware **dictionary** codec (`Codec::Dict`) | STL-250 |
| **v14** | + per-row-group valid-time interval summary section | STL-316 |

> **Graceful (no-bump) change.** The `stat_flags` byte ([STL-120]) repurposed a
> reserved, always-zero byte additively, so old and new readers interoperate
> without a version bump — the only format change to date that did not bump the
> generation. See [zone maps](#zone-maps-per-column-minmax).

## Constants (quick reference)

| constant | value |
|---|---|
| `HEADER_MAGIC` | `"STLSEG\0\0"` |
| `TRAILER_MAGIC` | `"STLSEGFT"` |
| `FORMAT_VERSION` | `14` |
| `HEADER_LEN` / `TRAILER_LEN` / `CHUNK_HEADER_LEN` | `16` each |
| `BYTES_NULL_SENTINEL` | `0xFFFF_FFFF` |
| `STAT_MIN_UNBOUNDED` / `STAT_MAX_UNBOUNDED` | `0x01` / `0x02` |
| `FOOTER_FLAG_BLOOM` / `_VALID_INTERVALS` / `_RG_VALID_INTERVALS` | `0x01` / `0x02` / `0x04` |
| `MAX_BYTES_STAT_PREFIX_LEN` | `64` |
| `SCHEMA_ID_IMPLICIT_VERSION` | `0` |
| `BLOOM_HASHES` / `DEFAULT_BITS_PER_KEY` | `4` / `12` |
| `DEFAULT_VALID_INTERVAL_CAP` | `256` |
| `VALID_TIME_PREFIX_LEN` | `16` |
| `MAX_VERSION_FRAME_LEN` | `16 MiB` |

## Not yet in the format (planned)

- **Further codecs.** RLE, delta, and FOR (frame-of-reference) — especially for
  the monotonic `sys_from`/`seq` axes — slot in as new `Codec` variants the same
  way `Dict` did (each bumps the version). ([feature plan §A.6](01-feature-plan.md#a6--columnar-core-with-adequate-point-access).)
- **Compression codecs** (block-level LZ4/Zstd) are not yet specified.
- **Real schema evolution.** One implicit `schema_id` today; catalog-resolved
  schema ids ride on [STL-98](https://allegromusic.atlassian.net/browse/STL-98).

[STL-117]: https://allegromusic.atlassian.net/browse/STL-117
[STL-119]: https://allegromusic.atlassian.net/browse/STL-119
[STL-120]: https://allegromusic.atlassian.net/browse/STL-120
[STL-154]: https://allegromusic.atlassian.net/browse/STL-154
[STL-173]: https://allegromusic.atlassian.net/browse/STL-173
[STL-238]: https://allegromusic.atlassian.net/browse/STL-238
[STL-241]: https://allegromusic.atlassian.net/browse/STL-241
[STL-250]: https://allegromusic.atlassian.net/browse/STL-250
[STL-316]: https://allegromusic.atlassian.net/browse/STL-316
[ADR-0023]: adr/0023-append-only-record-model-validity-index.md
[ADR-0024]: adr/0024-time-representation.md
