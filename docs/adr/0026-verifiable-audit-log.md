# ADR-0026 — Verifiable audit log: hash-chain + Merkle inclusion/consistency proofs

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Project owner (raised via considerations review) + systems design
- **Related:** [ADR-0018](0018-security-auditability-pillar.md) (security pillar) · [ADR-0020](0020-crypto-shredding-erasure.md) (erasure) · [ADR-0023](0023-append-only-record-model-validity-index.md) (log is source of truth) · [10 §3](../10-security-and-compliance.md#3-identity-driven-security-the-differentiator) · [Charter §4](../00-charter.md#4-differentiating-primitives-the-identity)

## Context

The strongest answer to "why not just Postgres + history tables" is **tamper-evidence you can verify yourself**: a Postgres history table can be silently rewritten by a privileged user; an auditor must *trust the operator*. The considerations review argues this should be Stele's **headline** differentiator, not a late "should." The roadmap previously placed cryptographic verifiability at v0.7. We pull it forward and make it core.

## Decision

**The append-only commit log is cryptographically verifiable, and this is a headline pillar.**

- **Hash-chained commit log from ~v0.2:** each commit carries the hash of the prior commit, so altering any historical record breaks the chain detectably. Cheap, early, and aligned with the [log-is-source-of-truth](0023-append-only-record-model-validity-index.md) model.
- **Merkle inclusion & consistency proofs by ~v0.5:** a Merkle tree over commits lets an external party verify (a) a given record **is included** in the log and (b) the log is an **append-only extension** of a previously-seen state (consistency) — *without trusting the operator*. This is the Certificate-Transparency-style verifiable-log pattern applied to a database.
- **Third-party verification tooling:** publish the proof format and a verifier so auditors check independently. Pursue an external audit of the cryptographic claims before any production-trust marketing.
- **Crypto-shred must preserve proofs:** field-/subject-level [crypto-shredding](0020-crypto-shredding-erasure.md) removes *plaintext* by destroying keys, but the hashed leaves and chain remain — so erasing one subject **does not break inclusion/consistency proofs for everyone else**. The proof is over ciphertext/commitments, not cleartext.

## Consequences

### Positive
- The defensible "verify it yourself" claim — the empty-quadrant differentiator vs Postgres, SQL:2011 history tables, and lakehouse time-travel.
- Pulling it early shapes the commit/log format correctly from the start (retrofitting a hash-chain later is painful).
- Composes with reproducibility ([06](../06-testing-strategy.md)) and erasure ([ADR-0020](0020-crypto-shredding-erasure.md)) instead of fighting them.

### Negative / costs
- Hashing/Merkle maintenance on the commit path (modest, batched per commit).
- Proof generation/storage and a published, stable proof format become a long-term compatibility commitment.
- Security-minded buyers will probe hard — demands a published threat model and ideally an external crypto audit ([10](../10-security-and-compliance.md)).

### Neutral / follow-ups
- Exact hash function, tree shape (e.g. RFC 6962-style), and checkpoint/witness strategy decided during the v0.2–v0.5 work.
- Interaction with distribution: the log/Merkle root must be agreed by consensus ([ADR-0006](0006-distribution-later-shared-storage.md)); proofs must hold across nodes and compaction ([cross-version reproducibility](../06-testing-strategy.md)).
