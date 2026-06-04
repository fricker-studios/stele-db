# ADR-0020 — Right-to-erasure & data-lifecycle: a layered strategy (namespace-drop + crypto-shredding)

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [10 §10](../10-security-and-compliance.md#the-append-only-vs-right-to-erasure-tension-handled-not-hand-waved) · [ADR-0018](0018-security-auditability-pillar.md) · [ADR-0019](0019-encryption-at-rest-kms.md) (key hierarchy) · [ADR-0009](0009-data-vault-conceptual-seam.md) (engine=mechanism, app=policy) · [ADR-0002](0002-on-disk-storage-format.md) (immutability) · [assumption A31](../assumptions.md)

## Context

Stele's defining property is **append-only immutability** ([ADR-0002](0002-on-disk-storage-format.md)) — history is never destructively rewritten. This appears to conflict with **right-to-erasure** obligations (GDPR and similar) that a financial/healthcare-adjacent engine must be able to honor. We cannot resolve this by quietly mutating sealed segments — that destroys the tamper-evidence and forensic guarantees that are the whole point ([ADR-0018](0018-security-auditability-pillar.md)).

Crypto-shredding (destroy the key, not the bytes) is one answer, but **not the only one**, and a single mechanism is the wrong shape: erasure needs vary in *granularity* (a whole tenant offboarding vs. one data subject vs. just PII fields) and in whether **physical** deletion is legally required. Notably, in the [target market](../00-charter.md#1-what-stele-is) the dominant real case is **whole-tenant offboarding**, and per-individual erasure is often *overridden* by legal retention obligations (HIPAA/financial). So the right design is a **layered set of composable mechanisms** with the engine supplying mechanism and the controller (app/Solvia) supplying policy — the same seam discipline as [ADR-0009](0009-data-vault-conceptual-seam.md).

## Decision

**We will support a layered erasure & data-lifecycle strategy**, built on the [envelope-encryption key hierarchy](0019-encryption-at-rest-kms.md) (KMS KEK → per-namespace NEK → per-segment DEK):

1. **Namespace-drop — the primary mechanism (tenant offboarding).** Namespaces/schemas are a first-class isolation **and lifecycle** unit, each with its own **NEK**. An **audited "drop namespace"** *destroys the NEK*: the whole tenant's data becomes unreadable instantly, everywhere including backups; physical space is reclaimed lazily by compaction. This is crypto-shredding **at namespace granularity** — a clean break that **preserves append-only immutability** (segments are orphaned, never rewritten).
2. **Per-subject crypto-shredding — the fine-grained backstop.** For erasing a single data subject inside a live namespace, encrypt that subject's data under a per-subject key and destroy it.
3. **PII sidecar / pseudonymization.** Keep erasable PII out of the immutable core where possible: store a token; the token→PII mapping lives in a small mutable side-store; erase = drop the mapping.
4. **Scoped, audited physical retention/expiry** ([01 §A.2](../01-feature-plan.md#a2--append-only--immutable-storage--historization)) for cases that legally require true physical deletion of bytes.

**The conceptual line:** history *within* a dataset is immutable (never rewritten); the *lifecycle of a whole namespace* — create, decommission — is a legitimate, audited, coarse operation ([architecture invariant 8](../02-architecture.md#12-cross-cutting-architectural-invariants)). The **engine provides mechanism, not policy**: financial/healthcare records often carry legal retention obligations that lawfully override erasure; the controller (app/Solvia) decides what/when. Phasing: namespace isolation + drop at **v0.5**, per-subject crypto-shred + the full set at **v0.7**, with the erasure paths **proven by test** in the [security trust gate](../10-security-and-compliance.md#12-security--the-trust-gate).

## Consequences

### Positive
- Reconciles append-only immutability with erasure — Stele is both audit-native *and* erasure-capable, at the right granularity for each case.
- **Namespace-drop is the dominant real case (tenant offboarding) and the cleanest** — one key destruction, no segment mutation, works across backups/replicas.
- Namespaces pay off well beyond erasure: per-tenant keys/BYOK, residency, blast-radius containment, per-tenant backup/restore, performance isolation.
- Reuses the [envelope-encryption](0019-encryption-at-rest-kms.md) machinery; subject/namespace keys are just layers of the same hierarchy.

### Negative / costs
- A namespace/subject **key hierarchy** adds key-management complexity (which key decrypts what; destroy it *everywhere*, including backups/replicas).
- "Erased" ciphertext lingers until compaction reclaims it — unreadable, but worth documenting.
- Correctness is critical and subtle: a bug that fails to destroy (or mis-scopes) a key is a compliance breach — these paths need strong tests and oracles ([06](../06-testing-strategy.md)).
- Per-individual erasure within a live namespace is finer-grained and less clean than namespace-drop — acceptable, since it's the rarer case here.

### Neutral / follow-ups
- Key-hierarchy granularity (namespace vs subject overlay) and the "drop namespace" reconcile/GC state machine are detailed in the encryption/erasure design doc when implemented.
- Legal review of crypto-shredding-as-erasure per jurisdiction is a pre-production task (widely accepted; confirm for target markets).
- Tenant→namespace mapping is an **app/Solvia** concern; the engine exposes the namespace + key + drop primitives only ([ADR-0009](0009-data-vault-conceptual-seam.md)).
