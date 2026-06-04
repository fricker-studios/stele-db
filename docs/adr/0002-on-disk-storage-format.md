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
- The detailed segment spec (encodings, codecs, footer layout) is [open question O2](../assumptions.md) and gets its own design doc + an amendment to this ADR before v0.1 storage work.
- Format version is embedded in the header from day one so migrations are always possible.
