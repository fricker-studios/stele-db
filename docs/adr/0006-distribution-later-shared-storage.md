# ADR-0006 — Distribution later: Raft control plane + shared object storage

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [02 — Architecture §10](../02-architecture.md#10-distribution--consensus-later-phase) · [03 — Roadmap](../03-roadmap.md#v20--distribution-era) · [ADR-0007](0007-storage-compute-separation.md) · [assumptions A5, A12](../assumptions.md)

## Context

Distribution is in Stele's identity ([Charter §4](../00-charter.md#4-differentiating-primitives-the-identity)) but explicitly a **later phase**. The [Charter §3 guardrail](../00-charter.md#3-the-guardrail--lead-with-the-non-goal) and the [roadmap](../03-roadmap.md) demand that the single-node temporal core be rock-solid *first*; distributed databases that distribute before their storage engine is correct ship correctness bugs at N× the blast radius. We must also choose a distribution *shape* now — even though we build it later — because it constrains foundational decisions (immutability, shared storage).

Alternatives: a **shared-nothing, sharded, replicated-state-machine** design (à la classic NewSQL — powerful but heavy, and it would push complexity into the storage layer early), or a **Spanner-style TrueTime** approach (requires special clock infrastructure), or **Paxos-from-scratch** (needless risk when Raft is well-understood).

## Decision

**We will defer distribution to the v2.0+ era and, when we build it, use a Raft-based control plane over a shared-object-storage data plane** ([02 §10](../02-architecture.md#10-distribution--consensus-later-phase)). Compute nodes are largely **stateless over shared, immutable segments in object storage** ([ADR-0007](0007-storage-compute-separation.md)); **Raft** provides consensus for the **control plane** (segment manifest, schema, commit coordination), not for bulk data movement. Consistency for this phase is validated by **Jepsen-style testing before any multi-node production claim** ([06 §7](../06-testing-strategy.md#7-jepsen-style-consistency-testing-distributed-phase)).

This is recorded as a **direction**, not a frozen commitment to a specific Raft library or manifest protocol — those are decided in their own ADRs when the work begins.

## Consequences

### Positive
- Read scale-out is nearly free: immutable segments need no cache-coherence protocol across readers ([ADR-0007](0007-storage-compute-separation.md)).
- Raft is well-understood, testable, and avoids bespoke-consensus risk; it's scoped to metadata, where the data volume is small.
- Building single-node-first means distribution rests on a *correct* foundation, not a moving one.

### Negative / costs
- Shared-storage latency (object store) shapes the design; needs aggressive caching and careful commit-path engineering.
- Deferring distribution means some "big data" use cases wait until v2.0+.
- The control-plane/data-plane split adds operational moving parts that must be Jepsen-validated.

### Neutral / follow-ups
- Specific Raft implementation, manifest format, and distributed-commit protocol get their own ADRs at v2.0 design time.
- [Assumption A12](../assumptions.md) records this as a direction subject to revision.
