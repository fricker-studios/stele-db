# ADR-0023 — Append-only record model: a derived validity index (no stored `sys_to`)

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Project owner + systems design
- **Related:** [02 §2–3](../02-architecture.md#2-the-bitemporal-record-model) · [ADR-0002](0002-on-disk-storage-format.md) · [ADR-0008](0008-mvcc-on-append-only.md) · [ADR-0026](0026-verifiable-audit-log.md) · [16 — Bitemporal Semantics](../16-bitemporal-semantics.md)

## Context

The original architecture diagram showed a **stored** `sys_to` per version. That is the SQL:2011 model: superseding a version *mutates* its end column — which contradicts Stele's core claim that a sealed segment is never mutated and history is append-only and tamper-evident. A privileged user able to update `sys_to` could rewrite "what we believed when." We must close a version's system-time interval **without mutating any committed record.**

Options: (a) store-and-update `sys_to` (rejected — not append-only); (b) store **no** `sys_to` and infer each version's end at read time from the next version's `sys_from` (pure inference); (c) store no `sys_to` on the record, but **materialize it once** into a derived index when the superseding assertion commits.

## Decision

**The append-only log is the source of truth; `sys_to` is not stored on the version record but is materialized once into a derived, rebuildable validity index.**

- A version record carries `sys_from` (and the valid-time period); it is **never mutated**. Supersession and logical delete are **new appended records** (assertions / retractions).
- When a superseding assertion (or a delete) commits, the prior version's `sys_to` is written **once** into a **validity index** — written once, never updated. `sys_to` is genuinely write-once (the past, once recorded, never changes; valid-time corrections are new assertions, not edits).
- The validity index is **derived state, fully rebuildable from the log** — an *accelerator*, not an authority. Losing or corrupting it cannot corrupt history; it is reconstructed by replaying the log. This keeps the [verifiable log](0026-verifiable-audit-log.md) authoritative.
- **Atomicity:** the close-of-prior and open-of-new are committed in the **same atomic transaction** — readers never observe a transient gap or overlap.
- **Per-key serialization:** supersession of a key takes a per-key serialization point at commit, so two concurrent supersessions cannot both open a new current version (the loser retries) — this closes the "concurrent supersede race."

Why materialize rather than purely infer: pure read-time inference must locate the *next* version to bound each interval → read-amplification on deep/hot version chains (the "one claim revised 10,000 times" case), weaker "active-at-S" block pruning, and slower range/diff queries. Materializing once removes all of that for one extra append-only, rebuildable write.

## Consequences

### Positive
- True append-only: no committed record is ever mutated → the tamper-evidence and as-of-reproducibility claims hold under scrutiny.
- Fast reads: the validity index answers "active at S" as a direct range-containment lookup, avoiding version-chain walks.
- The index being derived means index bugs degrade performance, never correctness — and recovery just rebuilds it.

### Negative / costs
- Two coupled writes per supersession (assert + close), which must be atomic — adds commit-path care.
- A second structure (the validity index) to build, recover, and (later) replicate — though always rebuildable from the log.
- The per-key serialization point is a write-side contention point on hot keys (measured in [06](../06-testing-strategy.md)).

### Neutral / follow-ups
- The validity index's physical form (per-segment vs global; in-memory + spill) is an implementation detail decided during v0.1.
- Formal close/inference semantics live in the [bitemporal semantics spec](../16-bitemporal-semantics.md).
