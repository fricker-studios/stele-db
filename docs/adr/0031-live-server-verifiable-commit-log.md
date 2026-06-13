# ADR-0031 — Live-server verifiable commit log: hash-chain `SessionEngine`'s commit log

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** Project owner + systems design
- **Related:** [02 §12](../02-architecture.md#12-cross-cutting-architectural-invariants) (invariant 10) · [ADR-0026](0026-verifiable-audit-log.md) (verifiable-audit pillar) · [ADR-0029](0029-cross-table-commit-marker.md) (cross-table commit marker — refined here) · [ADR-0023](0023-append-only-record-model-validity-index.md) (log is source of truth) · STL-178 / STL-199 / STL-302

## Context

Invariant 10 — *the commit log is hash-chained and verifiable; tampering with any
historical record is detectable* ([ADR-0026](0026-verifiable-audit-log.md), the
headline differentiator) — is implemented in `stele-txn`'s `TxnManager`: a
SHA-256 [`CommitRecord`](../../crates/stele-txn/src/commit_record.rs) chain,
re-verified on recovery, oracled in the simulator (STL-178). But **the live
server does not use `TxnManager`**. `SessionEngine` ([stele-engine](../../crates/stele-engine/src/lib.rs))
runs its own commit path — a per-session `next_txn` counter and per-table WALs —
so the hash chain is never written, and nothing tamper-evident is reachable from
a running server.

STL-199 surfaced this concretely while lighting up the shell's temporal
meta-commands: `\audit`'s scope item — "per-version commit-chain hashes +
verification verdict" — had **no data source in the running engine**, so `\audit`
(and `\lineage`'s `hash ← prevHash` line) were split out to STL-302.

[ADR-0029](0029-cross-table-commit-marker.md) already weighed wiring the
`stele-txn` chain into the session path (its option **b**) and deferred it: for
*cross-table atomicity* the hash chain is not needed to order recovery, and
adopting a whole commit-manager was disproportionate to that gap. It instead
added a session-owned **commit-marker log** — `stele.commits`, one CRC-framed,
fsynced marker per *multi-table* commit naming the committing `txn_id` — and
explicitly called it "a faithful, if minimal, *transaction T committed* record —
a stepping stone toward folding into the tamper-evident commit log of option (b)."

STL-302 is where the audit chain *is* needed. We close the gap now, on the
mechanism ADR-0029 set up, without adopting `TxnManager`.

Constraints carried forward: the durability point stays a log fsync (invariant
2); sealed segments are never mutated (invariant 1); the storage/txn core stays
deterministic and runtime-agnostic (ADR-0010); the audit chain must be
genuinely tamper-evident, i.e. a **durable witness independent of the data** —
a chain recomputed from the same (possibly-mutated) data is not evidence (it is
the "consistent rewrite" `verify_chain` documents, and the demo FNV chain the CLI
prototype draws).

## Decision

**Make `SessionEngine`'s commit-marker log a hash-chained commit log, reusing
STL-178's `CommitRecord` / `verify_chain`, and surface it over pg-wire for the
shell's `\audit` and `\lineage`.**

- **One `CommitRecord` per data commit.** Each auto-commit DML statement and each
  multi-statement `COMMIT` that writes rows appends a
  [`CommitRecord`](../../crates/stele-txn/src/commit_record.rs)
  `{txn_id, commit_ts, seq, prev_hash}` to `stele.commits` — the same 56-byte
  frame STL-178 chains — fsynced after the write's own data is durable. The
  engine carries the running chain `head` (the next record's `prev_hash`) and a
  monotonic commit `seq` in memory. This is the `stele-txn` chain primitive
  reused verbatim; only the *writer* differs (the session engine, not
  `TxnManager`).
- **The marker log becomes the commit log.** A record's `txn_id` field is exactly
  the marker ADR-0029 gated two-phase legs on, so cross-table recovery is
  unchanged: recovery replays `stele.commits`, collects the committed `txn_id`s,
  and a two-phase leg is still applied iff its transaction is in that set. The
  single-table fast path's plain WAL record still applies unconditionally; its
  commit record is **additive** (it does not gate that record's application).
- **Verify on recovery, fail closed.** Recovery runs `verify_chain_recover` over
  the replayed records: a broken link (a tampered historical record) refuses
  recovery rather than serving forged history, and the verified tail seeds the
  in-memory `head`/`seq`. This extends STL-178's recovery verification to the
  live server.
- **Introspection surface.** A Stele-native `SELECT * FROM stele_audit('t'[, key])`
  is recognized structurally in `SessionEngine::execute` — exactly like STL-199's
  `stele_history` — and answered as an ordinary row set: per version `(txid, op,
  hash, prev_hash)` plus the chain verdict `(chain_ok, chain_len, chain_head)`.
  The verdict is `verify_chain_to(durable records, in-memory head)`: it reads the
  **durable** `stele.commits` (so on-disk tampering is detected) and anchors
  against the live head (so a wholesale tail rewrite is caught too, per
  [`chain`](../../crates/stele-txn/src/chain.rs)). No pg-wire routing change.
- **Scope: data commits, not catalog/DDL.** The chain covers row-producing
  commits (`INSERT`/`UPDATE`/`DELETE`/`MERGE`) — what `\audit` and `\lineage`
  surface. `CREATE`/`DROP` remain in the durable catalog log
  ([ADR-0028](0028-durable-catalog-log.md), CRC-protected); a `DROP`'s bulk row
  closes are recovery-re-derivable from the catalog drop record (STL-220), so
  they are deliberately **not** chained — chaining a commit recovery reconstructs
  elsewhere would desynchronize the chain from the data. Hash-chaining the
  catalog log, and a single unified log, are follow-ups.

## Consequences

### Positive
- Invariant 10 holds for the **live server**, not just the simulator: a running
  Stele can prove its own history is untampered, the verifiable-audit pillar's
  whole point ([ADR-0026](0026-verifiable-audit-log.md)).
- `\audit` and `\lineage`'s `hash ← prevHash` are real, backed by the STL-178
  chain rather than the prototype's demo FNV construct.
- Reuses STL-178's `CommitRecord`/`verify_chain` and ADR-0029's `stele.commits`
  framing/torn-tail/CRC machinery wholesale — the new code is a writer hook, a
  recovery verify, and an introspection call, not a new subsystem.
- Recovery now fails closed on a tampered live commit log, where before it had no
  chain to check.

### Negative / costs
- **The single-table fast path now fsyncs twice per commit** (the data record,
  then the commit record), where ADR-0029 deliberately kept it at one. This
  refines ADR-0029's fast-path optimization: the audit chain is the headline
  differentiator ([ADR-0026](0026-verifiable-audit-log.md), which anticipates
  "hashing/Merkle maintenance on the commit path (modest, batched per commit)"),
  and ADR-0029 kept the chain out only because atomicity did not need it. A later
  batched/group commit-record fsync can amortize this; not needed for STL-302.
- `stele.commits` now grows one record per *data* commit, not per multi-table
  commit — it wants snapshot/compaction eventually (the same follow-up
  ADR-0029 noted for the marker log).
- The commit-record `commit_ts`/`seq` are the session engine's, sourced from its
  commit clock and a session counter — not `TxnManager`'s. When the engine
  eventually adopts `TxnManager`, the two converge into one chain (ADR-0029's
  option b endgame); this ADR is the stepping stone, not that convergence.

### Neutral / follow-ups
- Hash-chaining the catalog log (DDL verifiability) and unifying the row WAL /
  catalog log / commit log under one verifiable format.
- Merkle inclusion/consistency proofs over this chain (~v0.5,
  [ADR-0026](0026-verifiable-audit-log.md)).
- A seed-reproducible tamper sweep lives in `stele-engine` (in-process
  MemDisk/FaultDisk), not `stele-sim`, since the simulator cannot depend on
  `stele-engine` (the pattern ADR-0029 set for session-level coverage).
