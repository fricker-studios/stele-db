# ADR-0008 — MVCC layered on the append-only store

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [02 — Architecture §9](../02-architecture.md#9-transaction--concurrency-model) · [01 §B.4](../01-feature-plan.md#b4--transactions-concurrency--mvcc) · [ADR-0002](0002-on-disk-storage-format.md) · [assumption A10](../assumptions.md)

## Context

Stele needs a concurrency-control model that gives ACID transactions and consistent reads. Crucially, the storage engine is **already multi-version**: append-only + bitemporal means every key naturally has a chain of system-time-stamped versions ([02 §2](../02-architecture.md#2-the-bitemporal-record-model)). The question is what concurrency model best fits — locking, OCC, or MVCC — and which isolation level to default to.

Lock-based concurrency would fight the append-only grain and hurt the analytical read path. A from-scratch serializable-only design adds risk and cost before it's needed. MVCC, by contrast, is almost *already built* — a snapshot read is "the latest system-time version ≤ my snapshot."

## Decision

**We will implement MVCC directly on top of the append-only store**, with **snapshot isolation as the v1 default** isolation level ([assumption A10](../assumptions.md)). A transaction reads a snapshot (a system-time point) and sees, per key, the latest version whose `sys` interval contains the snapshot; writes append new versions with `sys_from = commit_time`; write-write conflicts on overlapping snapshots are detected and the loser retries. **Read-committed** is a selectable lower level (v0.3); **serializable (SSI)** is a later opt-in (v0.7). History is **not** garbage-collected by default (append-only) — space is managed via tiering and explicit, audited retention policies only ([01 §A.2](../01-feature-plan.md#a2--append-only--immutable-storage--historization)).

## Consequences

### Positive
- The concurrency model falls out of the storage model — minimal extra machinery, strong conceptual coherence.
- Readers never block writers and vice-versa; ideal for the analytical + temporal read workload.
- Snapshot reads and `AS OF` reads are the *same mechanism* — time-travel and MVCC unify.
- No GC of versions by default means the audit/history guarantees are never quietly eroded by vacuum.

### Negative / costs
- Snapshot isolation permits write-skew anomalies; users needing strict serializability wait for the SSI opt-in (v0.7).
- Long-lived snapshots pin history (intended here, but a space consideration handled by tiering, not vacuum).
- Conflict detection + retry semantics must be carefully specified and tested (a [correctness oracle](../06-testing-strategy.md#4-correctness-oracles-the-temporal-heart) covers isolation).

### Neutral / follow-ups
- Whether SSI becomes default at some later major version is left open.
- The interaction of MVCC snapshots with the distributed manifest is deferred to [ADR-0006](0006-distribution-later-shared-storage.md).

## Amendment — selectable READ COMMITTED + explicit conflict contract (STL-248, 2026-06-19)

The original decision left two things informal: the precise write-write conflict rule ("the loser retries") and how the selectable lower level behaves. This amendment makes both commitments, without changing the overall design.

- **First-committer-wins, surfaced as a retryable `40001`.** A transaction's `COMMIT` is refused iff it wrote a key whose current version was committed by another transaction *strictly after* this transaction's snapshot. The refusal is SQLSTATE **`40001` (`serialization_failure`)** — a *retryable* signal, applied atomically (nothing of the losing transaction lands) — and the first committer's write stands. This is the existing snapshot-isolation commitment made precise; clients retry the whole transaction on `40001` (see [12 §9](../12-data-migration-and-interop.md#9-driver-notes-isolation--retryable-errors)). A [correctness oracle](../06-testing-strategy.md#4-correctness-oracles-the-temporal-heart) re-derives every commit's Ok/Conflict decision from an independent first-committer-wins rule, so the rule is tested, not just asserted.

- **`READ COMMITTED` is selectable; it re-pins per statement.** `BEGIN ISOLATION LEVEL READ COMMITTED` (and `SET TRANSACTION ISOLATION LEVEL …`) selects per-statement snapshots: each statement reads a fresh snapshot, so it observes transactions committed before it began. `REPEATABLE READ`/`SNAPSHOT` remain the default snapshot-isolation level (one snapshot per transaction). The weaker level's commit check naturally raises `40001` in fewer cases (its snapshot is fresher), which is the expected weaker guarantee — not a different conflict rule.

- **`SERIALIZABLE` is rejected, not downgraded.** Until SSI ships (v0.7), `SERIALIZABLE` returns `0A000` (feature not supported) rather than silently behaving as snapshot isolation — aliasing it would be a false serializability promise, which the audit-honesty charter forbids.

- **No deadlock detection — by construction.** Writers detect conflicts at commit and the loser retries; no writer ever *waits* on a lock held by another. With no lock-wait edges there is no wait-for cycle to deadlock, so there is no deadlock detector and clients never see `40P01`. This is a property of commit-time (OCC-style) conflict detection on the append-only store, not deferred work.
