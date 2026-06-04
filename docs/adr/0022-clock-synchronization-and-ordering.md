# ADR-0022 — Clock synchronization & cross-node time ordering

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (raised in follow-up) + systems design
- **Related:** [02 §10](../02-architecture.md#10-distribution--consensus-later-phase) · [ADR-0006](0006-distribution-later-shared-storage.md) · [02 §2 bitemporal model](../02-architecture.md#2-the-bitemporal-record-model) · [11 — Operations](../11-operations-and-runbooks.md) · [assumption A33](../assumptions.md)

## Context

Stele's spine is **system-time** — every version is stamped with when the database recorded it, and as-of/bitemporal correctness depends on that ordering ([02 §2](../02-architecture.md#2-the-bitemporal-record-model)). On a single node this is trivial (one monotonic clock). In a **distributed** deployment ([ADR-0006](0006-distribution-later-shared-storage.md)), multiple nodes assign system-time, so **cross-node clock agreement becomes correctness-critical**: skew could let one node record a "later" version with an earlier timestamp than another, corrupting history ordering and as-of reads.

Plain **NTP is necessary but not sufficient.** Real-world NTP drift is often tens to hundreds of milliseconds — fine for logs, not for ordering a time-native database. The options: rely on NTP alone (unsafe), use **Hybrid Logical Clocks (HLC)** to bound the impact of skew (CockroachDB/YugabyteDB/Mongo approach), use **TrueTime-style** GPS/atomic clocks + commit-wait (Spanner; needs special infra), or exploit modern **PTP / cloud time-sync** (e.g. Amazon Time Sync now offers microsecond accuracy) to tighten the bound.

## Decision

**We require NTP as an operational baseline and layer a skew-tolerant clock model on top; we do not trust wall clocks for correctness.** (Single-node is unaffected.)

1. **NTP is a hard requirement** on every node in a distributed deployment (chrony, or PTP/PHC where available), enforced by the [operator](../09-ecosystem-and-products.md#5-kubernetes--openshift-operator) as a **preflight check + continuous monitoring**. A node without healthy time sync does not join the cluster.
2. **Hybrid Logical Clocks (HLC)** assign system-time across nodes — physical time plus a logical counter — so **causal ordering survives bounded skew** without per-event coordination.
3. **A configurable max-skew bound** is enforced; a node that detects drift beyond the bound **fences itself** (stops serving) rather than risk mis-ordering — **fail-safe, not fail-wrong**.
4. **Tighter sync narrows the bound:** PTP / cloud time-sync (Amazon Time Sync microsecond, etc.) shrink the uncertainty window and improve latency; a **TrueTime-style commit-wait** path is left optional where high-precision time is available.

This is recorded as the **direction**; the exact HLC integration, default max-skew, and fencing mechanics are pinned when distributed work begins ([roadmap v2.0+](../03-roadmap.md#v20--distribution-era)).

## Consequences

### Positive
- Protects the engine's defining property (correct system-time ordering) under real-world clock conditions.
- HLC gives causal consistency cheaply, without TrueTime's special hardware; PTP/cloud time-sync is an *optional* accelerator, not a dependency.
- Fail-safe fencing means a drifting node degrades availability, never correctness — the right trade for an audit-native engine.

### Negative / costs
- Operational burden: NTP/PTP must be set up and monitored; the operator owns preflight + alerting ([11](../11-operations-and-runbooks.md)).
- Max-skew fencing can reduce availability under bad time sync (a node drops out) — acceptable, and surfaced as a clear operational signal.
- HLC adds a small amount of per-event state and reasoning to the distributed commit path.

### Neutral / follow-ups
- Single-node and read-replica deployments don't need the cross-node machinery; it activates with multi-writer distribution (v2.0+).
- Clock-skew scenarios are first-class **fault-injection inputs** in the [simulation harness](../06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece) and [Jepsen testing](../06-testing-strategy.md#7-jepsen-style-consistency-testing-distributed-phase).
- Whether to offer commit-wait (lower-latency-at-higher-precision) is decided when high-precision time is a deployment reality.
