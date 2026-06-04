# ADR-0021 — Storage lifecycle: system-time-driven tiered archival

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [02 §4](../02-architecture.md#storage-lifecycle-tiered-archival-controlling-append-only-growth) · [01 §A.7](../01-feature-plan.md#a7--object-storage-tiering--storage-lifecycle-storagecompute-separation) · [ADR-0007](0007-storage-compute-separation.md) · [ADR-0002](0002-on-disk-storage-format.md) · [assumption A32](../assumptions.md)

## Context

Append-only storage ([ADR-0002](0002-on-disk-storage-format.md)) means total data volume only ever grows. On object storage this is a real cost problem: at S3 Standard rates (~$23/TB·month) an audit-native engine accumulating decades of history becomes expensive, even though the vast majority of that history is old and rarely queried. We need to control cost **without deleting data** — deletion (retention/expiry) is a separate, opt-in lever ([01 §A.2](../01-feature-plan.md#a2--append-only--immutable-storage--historization)); here the requirement is to *keep everything* but make cold history cheap, with a way to pull it back when needed.

Object stores offer a cost ladder — S3 Standard → Standard-IA / Glacier Instant (cheaper, still millisecond reads) → Glacier Deep Archive (~$1/TB·month, ~23× cheaper, but 12–48h retrieval). The questions: what decides when data tiers down, how do queries behave when they hit slow-retrieval tiers, and does the engine or the object store drive it.

## Decision

**We will add engine-native, system-time-driven tiered archival**, distinct from retention/expiry and preserving append-only + audit guarantees.

- **Staleness signal = system-time age.** The bitemporal model already knows which versions are *superseded history* vs *current*; superseded versions older than a (configurable, per-namespace/table) threshold tier down. Current versions stay hot. No access-pattern guessing required.
- **Tier ladder:** hot (local NVMe cache) → warm (S3 Standard) → cold (S3-IA / Glacier Instant, transparent ms reads) → frozen (Glacier Deep Archive). Segment-granular.
- **Time-era compaction** clusters segments so a cold segment is *purely* old history and ages together — no live row dragged into archive.
- **Resident metadata.** Catalog, segment index, and **zone maps are never archived**, so the planner prunes *before* rehydrating and only thaws the segments a query actually needs.
- **Explicit async restore for frozen data.** The **tier-aware planner** detects a Glacier-class hit and returns `restore required` + a handle with a cost/latency estimate (rather than hanging for hours); the user calls `RESTORE` (SQL or admin API) to rehydrate, then re-queries. Cold/instant tiers are read transparently.
- **Engine-native and pluggable.** Stele decides per-segment placement and sets the storage class on write/migration, across any S3-compatible backend; delegating to S3 Intelligent-Tiering is an optional backend mode. Conservative defaults — no surprise archival.

Phasing: lands at **v0.7**, building on the [object-store cold tier](0007-storage-compute-separation.md) (v0.5).

## Consequences

### Positive
- Makes unbounded append-only growth economically sustainable — old history lives at ~$1/TB·month while staying fully queryable (after restore).
- Exploits the bitemporal identity: system-time is a *correct* hot/cold signal, not a heuristic — a real advantage over generic engines.
- Resident zone maps + segment-granular archival keep retrieval cost bounded (prune before thaw).
- **Data is never lost** — archival changes cost/latency only; durability and auditability are untouched ([Charter](../00-charter.md)).

### Negative / costs
- Adds a tier-placement policy engine, tier-aware planning, and a restore/rehydration workflow — real implementation surface.
- Querying frozen data is slow (hours) and incurs retrieval fees; the explicit-restore UX manages but cannot eliminate this.
- Time-era compaction adds a clustering dimension to the compactor.

### Neutral / follow-ups
- Default thresholds, per-namespace policy schema, and the `RESTORE` surface are detailed in the storage-lifecycle design doc when implemented.
- Tiering interacts with [per-namespace encryption](0019-encryption-at-rest-kms.md) and [backups](../01-feature-plan.md#b6--backup-restore--snapshots) — archived segments stay encrypted; restores re-warm transparently.
- Non-AWS object stores expose different class/retrieval semantics; the pluggable backend abstracts them, with capabilities advertised to the planner.
