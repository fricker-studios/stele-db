# ADR-0029 — Cross-table commit atomicity: a transaction commit-marker over the per-table WALs

- **Status:** Accepted
- **Date:** 2026-06-11
- **Deciders:** Project owner + systems design
- **Related:** [02 §9](../02-architecture.md#9-transaction--concurrency-model) · [02 §12](../02-architecture.md#12-cross-cutting-architectural-invariants) (invariants 2 & 10) · [ADR-0026](0026-verifiable-audit-log.md) · [ADR-0028](0028-durable-catalog-log.md) · STL-215 / STL-192 / STL-178

## Context

STL-192 made a multi-statement `COMMIT` crash-atomic **per table**: each table a
transaction touches owns its WAL, so its writes land as one group-committed redo
record + one fsync, and that table recovers all-or-none (the record boundary *is*
the transaction boundary). But each table owns a *separate* WAL — created under
its own `t{idx:020}-` namespace ([ADR-0028](0028-durable-catalog-log.md),
STL-148) — so a transaction spanning **several** tables writes one record + one
fsync *per table*. A crash *between* two tables' group commits leaves some
tables' writes durable and others not: a partial commit across tables, violating
atomicity.

Constraints: the durability point must stay a log fsync (invariant 2); sealed
segments are never mutated (invariant 1); the storage/txn core stays
runtime-agnostic and deterministic (ADR-0010); the **single-table** path — by far
the common case — must keep exactly one fsync per `COMMIT` (no regression on the
STL-192 fast path); and the audit/commit-ordering story (invariant 10,
[ADR-0026](0026-verifiable-audit-log.md)) must not be contradicted.

Options considered:

- **(a) One shared WAL for the whole engine.** Atomicity falls out trivially (a
  transaction is one record in one log). But it discards the per-table WAL
  architecture the rest of the engine is built on — per-table recovery, flush,
  replay-floor, and namespace isolation ([ADR-0028](0028-durable-catalog-log.md))
  — a large, invasive rewrite, and it serializes unrelated tables' writes behind
  one log. Out of proportion to the gap.
- **(b) Fold cross-table commit into the `stele-txn` hash-chained commit log**
  ([ADR-0026](0026-verifiable-audit-log.md), STL-178). The right *long-term*
  home — it already records "transaction T committed" tamper-evidently — but
  `SessionEngine` does not use `TxnManager` at all yet, so this means wiring an
  entire commit-manager (its own WAL, the chain, conflict-index rebuild) into the
  session path: a much larger change than the atomicity gap requires, and the
  hash chain (a ~v0.5 Merkle-proof concern) is not needed to *order* recovery.
- **(c) A dedicated append-only commit-marker log** at the session level — the
  classic redo + commit-marker (2PC-over-shared-log) protocol — mirroring the
  catalog log of [ADR-0028](0028-durable-catalog-log.md).

## Decision

**We will make a multi-table `COMMIT` atomic with a dedicated append-only
commit-marker log — `stele.commits` on the session's shared disk, owned by
`stele-engine` — gating two-phase per-table redo records on recovery.**

- **Two-phase per-table records.** A multi-table commit writes each table's
  writes as a **two-phase** WAL redo record: the pre-STL-192 record framing
  prefixed with a one-byte tag (`0xFF`, disjoint from every redo tag) and the
  committing `txn_id`. The record is durable but **inert** — it carries no force
  of its own until vouched. A single-table (and auto-commit) record is written
  with the unchanged plain framing and applies unconditionally, so the common
  path is byte-for-byte and fsync-for-fsync what it was.
- **One marker, fsynced last.** After **every** per-table leg is durable, one
  marker — "transaction `txn_id` committed", framed `magic | length | payload |
  CRC32C` like the catalog log — is appended to `stele.commits` and **fsynced**.
  That fsync is the transaction's commit point; the `COMMIT` is acknowledged only
  after it returns. A single-table commit writes **no** marker — its one record's
  boundary is already its atomic commit point — so the fast path stays one fsync.
- **Recovery gates legs on the marker.** `SessionEngine::recover` replays
  `stele.commits` into the set of committed transaction ids and threads it through
  every table's `Engine::recover_with_commits` →
  `dml::recover_replay`: a two-phase record is replayed **iff** its `txn_id` is in
  the set; otherwise the marker never became durable (a crash between the
  per-table commits and the marker) and the leg is discarded. So the transaction
  recovers all-or-none across **every** table it wrote. A bare per-table
  `Engine::recover` (the storage-level sims/tests, which never write two-phase
  records) passes a sentinel "apply all" and is unchanged.
- **Write-ahead ordering = the protocol's correctness.** Each per-table leg is
  appended **and fsynced** before the marker is written, so "marker durable ⇒
  every leg durable." A crash before the marker fsync ⇒ no marker ⇒ all legs
  discarded (presumed abort); a crash after ⇒ marker durable ⇒ all legs (already
  durable) applied. A mid-sequence leg failure writes no marker, so the
  transaction is durably uncommitted.
- **Torn-tail contract, fail-closed on corruption** — identical to the catalog
  log: a partial trailing marker frame (or a tail not beginning with the magic)
  is the debris of a crashed append whose fsync never returned, so the
  transaction recovers as uncommitted and replay stops; a *complete* marker frame
  whose CRC fails is an acknowledged commit gone bad and is a hard recovery error.

## Consequences

### Positive
- A multi-table `COMMIT` is crash-atomic across every table — the STL-192
  guarantee extended from per-table to per-transaction — with no change to any
  storage-tier format and no change to the single-table fast path's fsync count.
- Reuses the per-table WAL architecture and the catalog log's framing/torn-tail
  machinery wholesale; the new mechanism is one small file and a one-byte record
  prefix, not a rewrite.
- The marker is fsynced like every other durability point (invariant 2), and it
  is a faithful, if minimal, "transaction T committed" record — a stepping stone
  toward folding into the tamper-evident commit log of option (b).

### Negative / costs
- A third durable-log mechanism at the session level (row WAL + catalog log +
  commit-marker log). Accepted: it is tiny (one marker per *multi-table* commit),
  and the alternatives either rewrite the storage core (a) or pull in a whole
  commit manager (b).
- The marker log grows one record per multi-table commit; like the catalog log it
  wants snapshot/compaction eventually, folded into option (b).
- An fsync that *fails after a successful append* (the leg's or the marker's)
  leaves durability indeterminate — a later `tick` may still flush the staged
  bytes — so it must be treated as a crash, not a clean abort. Enforcing that
  (poisoning the engine on fsync failure) is STL-217, unchanged by this ADR.
- In-memory rollback of a failed multi-table commit's already-applied (non-durable)
  writes is still deferred (STL-216); durability is correct (all-or-none = none),
  but the live process keeps the applied state until recovery drops it.

### Neutral / follow-ups
- Folding the marker into the `stele-txn` hash-chained commit log (option b)
  supersedes this log when `SessionEngine` adopts `TxnManager`; the recovery
  semantics fixed here (gate a leg on its transaction's marker) carry over.
- Cross-table coordination lives in `stele-engine`, which `stele-sim` cannot
  depend on, so the seed-reproducible crash-atomicity coverage is an in-process
  FaultDisk/MemDisk sweep in `stele-engine` (the pattern STL-210 set for
  session-level kill coverage), not a `stele-sim` scenario.
