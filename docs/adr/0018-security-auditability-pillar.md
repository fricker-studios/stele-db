# ADR-0018 — Security & Auditability as a first-class pillar

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up session)
- **Related:** [10 — Security & Compliance](../10-security-and-compliance.md) · [Charter §4, §6](../00-charter.md#4-differentiating-primitives-the-identity) · [01 §B.8](../01-feature-plan.md#b8--security-authz--data-protection-pillar) · [ADR-0001](0001-implementation-language-rust.md) · [assumptions A28–A29](../assumptions.md)

## Context

Stele targets financial and heavily-audited systems. Originally, security sat in the "general DBMS substrate" — built "to the standard the differentiators require, no more, no less." For the target market that bar is too low: in regulated finance/healthcare, security and compliance are not a backplane, they're a buying requirement.

There's a real risk in elevating security, though: the [Charter's central guardrail](../00-charter.md#3-the-guardrail--lead-with-the-non-goal) is *focus* — don't try to be world-class at everything. A naive "add a security pillar" could become an open-ended enterprise-checkbox sprawl that pulls the project off its temporal/audit identity.

The resolving insight: **for an audit-native engine, security and auditability are the same axis.** Immutability is tamper-resistance; provenance is non-repudiation; time-travel is forensics; hash-chained commits are cryptographic verifiability. So security can be elevated *without* competing for the novelty budget — it's mostly already there.

## Decision

**We elevate "Security & Auditability" to a first-class, unified pillar**, framed so it reinforces focus rather than diluting it ([Charter §4](../00-charter.md#4-differentiating-primitives-the-identity); detailed in [10](../10-security-and-compliance.md)).

The discipline, in three tiers:
1. **World-class** at the security that flows from the audit-native identity — tamper-evidence, provenance/non-repudiation, forensic time-travel, cryptographic verifiability, immutable audit log.
2. **Excellent** at the table-stakes controls regulated buyers require — encryption in transit + at rest (KMS/BYOK), strong authN, RBAC + row/column security + masking, access auditing, memory-safety/exploit resistance, secure defaults, supply-chain integrity, and a compliance roadmap (**SOC 2, HIPAA, PCI-DSS, GDPR** — [assumption A29](../assumptions.md)).
3. **Out of scope** — generic security features unrelated to the identity, same as any non-identity feature.

Concretely this re-tiers the [feature plan §B.8](../01-feature-plan.md#b8--security-authz--data-protection-pillar) higher and earlier (encryption at rest, RBAC, access auditing → v0.5; cryptographic verifiability bumped to v0.7), adds a [security trust-gate](../10-security-and-compliance.md#12-security--the-trust-gate), and spawns ADRs [0019](0019-encryption-at-rest-kms.md) (encryption/KMS) and [0020](0020-crypto-shredding-erasure.md) (erasure).

## Consequences

### Positive
- Matches the target market; security becomes a reason to choose Stele, not a gap.
- Coherent with the identity — the pillar is mostly *naming* and *hardening* what the temporal/audit primitives already provide.
- Forces security to be designed in from v0.1 (memory safety, secure defaults) rather than retrofitted.

### Negative / costs
- More surface to build and maintain (encryption, KMS, RLS/CLS, compliance mappings) — mitigated by tiering and by leaning on the identity primitives.
- Compliance certifications are org/process work (a commercial-entity responsibility), not just engine features; the engine can only make them *achievable*.
- Requires ongoing discipline (tier 3) to avoid security-checkbox sprawl.

### Neutral / follow-ups
- Specific deep decisions get their own ADRs: encryption at rest ([0019](0019-encryption-at-rest-kms.md)), crypto-shredding erasure ([0020](0020-crypto-shredding-erasure.md)); an authZ-model ADR and an incident-response runbook are follow-ups.
- Formal certification timing is tied to the commercial/cloud phase and the [trust gate](../10-security-and-compliance.md#12-security--the-trust-gate).
