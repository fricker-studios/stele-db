# ADR-0025 — Valid-time indexing & the scatter problem

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Project owner + systems design
- **Related:** [02 §3](../02-architecture.md#3-storage-engine-internals) · [ADR-0002](0002-on-disk-storage-format.md) · [ADR-0021](0021-storage-lifecycle-tiered-archival.md) · [01 §A.5](../01-feature-plan.md#a5--hash-keys--mergeupsert) · [14 — Performance](../14-performance-and-benchmarking.md)

## Context

Stele relies on **zone maps** (per-block min/max) to prune work during scans. This works beautifully for **system-time**, which is monotonic — new versions append with ever-increasing `sys_from`, so each block's system-time min/max is tight and "as-of S" pruning skips most blocks.

**Valid-time is different.** Stele's signature workload is *late-arriving, backdated* data (corrections, restatements, retroactive postings). Backdated writes land in *today's* block but carry *old* valid-times, so a block's valid-time min/max **spans almost the entire timeline** → zone maps prune nothing on valid-time predicates → full scans. This is the dark side of our "efficient backdated writes" differentiator, and it must be designed for, not discovered in production.

## Decision

**Prune the two axes with different mechanisms:**

- **System-time → zone maps** (monotonic; tight min/max; great skipping). Unchanged.
- **Valid-time → a dedicated index, not zone maps.** A secondary/validity index over valid-time intervals (e.g., an interval/range index, or per-segment valid-time interval summaries) answers "valid at V / overlapping [V1,V2)" without relying on block min/max. The [validity index](0023-append-only-record-model-validity-index.md) is the natural home for this.
- **Optional valid-time clustering** for backfill-heavy tables: a table may opt to cluster/sort segments by valid-time so blocks regain tight valid-time min/max, trading some ingest locality for scan pruning. Off by default; a per-table knob.
- **Temporal column compression is measured per column:** delta-encoding crushes monotonic `sys_*`; scattered `valid_*` compress better with dictionary/RLE. The writer selects codecs per column ([ADR-0002](0002-on-disk-storage-format.md)) and we **report compression ratios per temporal column** ([14](../14-performance-and-benchmarking.md)).

A dedicated benchmark builds heavy-backdated data and measures **scan amplification on valid-time filters** with and without the index/clustering.

## Consequences

### Positive
- Backdated-heavy workloads stay fast on valid-time predicates — turning a latent trap into a measured, defended edge.
- Keeps the columnar scan path clean: zone maps for system-time, index for valid-time; neither compromises the other.

### Negative / costs
- A valid-time index adds build/maintenance cost and storage (mitigated: it's derived/rebuildable like the validity index).
- Valid-time clustering trades ingest write-locality for read pruning — a per-table tuning decision, not free.

### Neutral / follow-ups
- Exact index structure (interval tree / segment-interval summaries / R-tree-like) is decided during the indexing work (v0.3–v0.5). **v0.3 (STL-241)** lands the per-segment-summary form: each sealed segment's footer carries the *coalesced union* of its rows' `[valid_from, valid_to)` windows (format v12, gated by `FOOTER_FLAG_VALID_INTERVALS`), so a `FOR VALID_TIME AS OF v` read skips a whole segment whose coverage has a gap at `v` — the scatter case the `valid_from` / `valid_to` zone-map min/max cannot prune. It is advisory and derived/rebuildable like the validity index (it rides the immutable segment, so flush / compaction / recovery need no separate rebuild). A cross-segment interval/range tree and the optional per-table valid-time clustering remain open for backfill-heavy workloads.
- Interacts with [partition hotspotting](0006-distribution-later-shared-storage.md) in the distributed phase (sharding by system-time vs valid-time) — addressed there.
