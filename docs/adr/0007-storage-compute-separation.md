# ADR-0007 — Separation of storage and compute via object storage

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [02 — Architecture §4](../02-architecture.md#4-object-storage-tiering--storagecompute-separation) · [ADR-0002](0002-on-disk-storage-format.md) · [ADR-0006](0006-distribution-later-shared-storage.md) · [assumption A11](../assumptions.md)

## Context

Separation of storage and compute (with object-storage tiering) is a named differentiating primitive ([Charter §4](../00-charter.md#4-differentiating-primitives-the-identity)). It enables elastic compute, cheap cold storage, and — later — multiple stateless readers over one dataset ([ADR-0006](0006-distribution-later-shared-storage.md)). The question is *how* to structure storage so this is clean rather than a bolt-on. The key enabler is already in place: **sealed segments are immutable** ([ADR-0002](0002-on-disk-storage-format.md)), which makes cached/remote data trivially coherent.

## Decision

**We will architect storage behind a pluggable backend trait** (`local`, `memory`, `s3`) with **S3-compatible object storage as the cold tier** and a **local-NVMe hot cache** ([02 §4](../02-architecture.md#4-object-storage-tiering--storagecompute-separation), [assumption A11](../assumptions.md)). Resident metadata (catalog, segment index, zone maps) stays hot; sealed segments tier to object storage and are pulled into the local cache on demand. Because segments never mutate, a cached or remotely-read segment **can never be stale** — no coherence protocol is needed. The WAL remains on durable local/log storage as the commit-durability point.

Phasing ([03](../03-roadmap.md)): pluggable backends in v0.3, cold tier + hot cache in v0.5, full storage/compute separation in v0.7.

## Consequences

### Positive
- Cheap, durable cold storage; elastic compute; the foundation for nearly-free read scale-out later.
- Immutability dividend: trivial cache/replica coherence — a direct payoff of the append-only design.
- Backups become almost free in the separated model (the object store *is* the durable copy).
- Backend trait keeps dev/test fast (in-memory/local) while production uses S3.

### Negative / costs
- Object-store latency/throughput characteristics must be hidden by caching and prefetch; naive access patterns are slow.
- Cache sizing, eviction, and warm-up become real operational concerns ([05 config](../05-dev-environment.md#configuration)).
- Consistency of the *manifest* (which segments are current) is the hard part — deferred to the [distribution ADR](0006-distribution-later-shared-storage.md) where Raft handles it.

### Neutral / follow-ups
- Specific S3 client/`object_store` crate choice and cache policy (LRU/foyer-style) are implementation decisions made at v0.3/v0.5.
- GCS/Azure support comes via the S3-compat or additional backends behind the same trait.
