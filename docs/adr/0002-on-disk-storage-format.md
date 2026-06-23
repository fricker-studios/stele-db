# ADR-0002 — Custom append-only columnar on-disk format

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [02 — Architecture §3](../02-architecture.md#3-storage-engine-internals) · [ADR-0008](0008-mvcc-on-append-only.md) · [assumptions A7, A8, O2](../assumptions.md)

## Context

The on-disk format is the **least reversible** thing in a database — once real data exists in it, changing it requires migrations forever. Stele's format must serve an append-only, bitemporal, columnar engine with adequate point access. It must store both time axes efficiently, retain history through compaction, support zone-map pruning (including by time range), and be self-describing across schema evolution so [time-travel survives DDL](../02-architecture.md#5-catalog--metadata).

Alternatives: **adopt Parquet/ORC as the primary write format** (great ecosystem, but row-group immutability and a schema model not built around bitemporal records or our delta/compaction lifecycle; awkward for the version-chain locality we want), or **a key-value store like RocksDB underneath** (proven, but it pushes us toward a row/LSM model and away from native columnar scans, and hides the layout we specifically want to control).

## Decision

**We will design Stele's own open-spec, versioned on-disk format**: immutable, self-describing **columnar sealed segments** (conceptually Parquet/ORC-like — row-groups, per-column chunks, footer with statistics) **designed around the bitemporal record model**, fronted by a **row-oriented WAL + delta tier** for recent writes ([02 §3.1–3.2](../02-architecture.md#3-storage-engine-internals)). System-time/valid-time/provenance are **first-class columns**, segments are sorted/clustered by `(business_key, sys_from)` for version-chain locality, and every segment carries zone maps, optional bloom filters, a schema id, and checksums.

Parquet/Arrow remain **interop** formats (import/export, foreign tables) — not the core write path. The in-memory/execution representation is **Arrow-shaped** ([assumption A7](../assumptions.md)) for SIMD and ecosystem interop.

## Consequences

### Positive
- Full control to optimize for the bitemporal/append-only access patterns and time-range pruning that define Stele.
- Immutability of sealed segments yields trivial cache/replica coherence ([ADR-0007](0007-storage-compute-separation.md)) and history preservation through compaction.
- Self-describing + versioned footer enables time-travel across schema changes.
- Checksums + a fuzzed reader ([06 §3](../06-testing-strategy.md#3-fuzzing)) make corruption detectable, never exploitable.

### Negative / costs
- We own a hard, high-stakes artifact: the format spec, its encoders/decoders, and every future migration. Significant engineering and a large share of the [testing budget](../06-testing-strategy.md).
- No free ride on Parquet's existing readers for the core format (mitigated by Parquet export).
- Format mistakes are expensive post-data — hence the pre-1.0 freedom to break it, freezing forward only at [v1.0](../03-roadmap.md#versioning).

### Neutral / follow-ups
- The detailed segment spec (encodings, codecs, footer layout) — [open question O2](../assumptions.md), once deferred to "its own design doc + an amendment to this ADR" — **is now written: [segment-format.md](../segment-format.md)** (the canonical byte-level spec), with this ADR's [Amendments](#amendments) carrying each format change. O2 is resolved (STL-261).
- Format version is embedded in the header from day one so migrations are always possible.

## Amendments

- **Dictionary column encoding (STL-250, v0.3) — format v13.** The first per-column codec beyond `Plain`: a bytes column whose values repeat across a key's version chain (the *identical* `business_key`, a repeated `principal` / `payload`) is stored as a small dictionary of its distinct values plus a narrow code per row, so an unchanged column is stored once rather than re-stored wholesale per version ([feature plan §A.2](../01-feature-plan.md), "Efficient historization"). The codec is chosen **per chunk by the writer from column statistics** — dictionary only when it is strictly smaller than plain, so an all-distinct column never grows ([architecture §3.2](../02-architecture.md#32-on-disk-segment-format)). The per-chunk codec tag (already in the chunk header and footer entry) is the dispatch point and the reader decodes it transparently, so late materialization is unaffected; min/max zone stats are computed over the logical values, so pruning is unchanged. Applied today by compaction (the natural place to spend CPU consolidating history); the monotonic-axis codecs (delta / FOR for `sys_from` / `seq`) drop in the same way as a follow-up. Bumping the header version keeps an older reader's reject clean at the header rather than mid-footer on the unknown codec byte — the pre-1.0 freedom-to-break policy above.
