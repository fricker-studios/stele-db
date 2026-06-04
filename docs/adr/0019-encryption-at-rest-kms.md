# ADR-0019 — Encryption at rest: envelope encryption + external KMS / BYOK

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [10 §4](../10-security-and-compliance.md#4-data-protection--encryption) · [ADR-0018](0018-security-auditability-pillar.md) · [ADR-0002](0002-on-disk-storage-format.md) (segments) · [ADR-0007](0007-storage-compute-separation.md) (object store) · [assumption A30](../assumptions.md)

## Context

Regulated workloads (SOC 2 / HIPAA / PCI-DSS) require encryption at rest, and ideally **customer control of keys** (BYOK), so that even the storage/cloud provider — or Stele's own managed service — cannot read the data. We need an approach that fits the [immutable segment format](0002-on-disk-storage-format.md) and the [object-storage tier](0007-storage-compute-separation.md), supports key rotation without rewriting everything, and underpins [crypto-shredding erasure](0020-crypto-shredding-erasure.md).

Options: full-disk/volume encryption only (coarse, no app-level key control, no BYOK story), single-key application encryption (simple but no rotation/granularity), or **envelope encryption** (per-object data keys wrapped by a KMS-held key — the industry-standard pattern).

## Decision

**We will use envelope encryption with an external KMS and BYOK/HYOK support.**

- Each **segment** is encrypted with a per-segment **data key (DEK)**; DEKs are wrapped by a **per-namespace key (NEK)**; NEKs are wrapped by a root **key-encryption key (KEK)** held in a **KMS** (AWS KMS, GCP KMS, Azure Key Vault, HashiCorp Vault — pluggable). Immutable segments make this clean: a sealed segment's key is fixed for life. The per-namespace layer enables per-tenant BYOK and underpins [namespace-drop erasure](0020-crypto-shredding-erasure.md) (destroying a NEK shreds a whole tenant).
- **BYOK / HYOK:** customers may bring or hold their own KEK; revoking it instantly renders all wrapped DEKs — and therefore all data — unreadable.
- **Rotation:** rotate the KEK by re-wrapping DEKs (no bulk re-encryption); rotate DEKs opportunistically during compaction.
- **End to end:** the object-store cold tier and backups inherit encryption because it's a property of the segment, wherever it lives ([10 §4](../10-security-and-compliance.md#4-data-protection--encryption)).
- Encryption is **transparent** to queries; keys are fetched/cached under the engine's trust zone, never exposed to clients.
- Phasing: at-rest encryption at **v0.5**; external KMS + BYOK at **v0.7** ([10 §13](../10-security-and-compliance.md#13-security-by-milestone)).

## Consequences

### Positive
- Meets regulatory encryption-at-rest requirements; BYOK gives customers (and auditors) provable control.
- Envelope design enables cheap key rotation and is the substrate for [crypto-shredding](0020-crypto-shredding-erasure.md).
- Immutability makes per-segment keying simple and coherent (no key churn on a sealed segment).

### Negative / costs
- KMS dependency and key-caching add operational complexity and a failure mode (KMS unavailability → no decrypt); needs careful caching + availability design.
- Per-segment keys add metadata and crypto overhead on the read path (mitigated by caching unwrapped DEKs in-memory within the trust zone).
- BYOK shifts a hard operational responsibility (key custody) to customers — must be documented clearly.

### Neutral / follow-ups
- Exact cipher suite, key hierarchy granularity (per-segment vs per-table vs per-subject overlay for [erasure](0020-crypto-shredding-erasure.md)), and KMS abstraction are detailed in the encryption design doc when implemented.
- Hardware HSM support and FIPS-validated modules are later considerations for the most regulated deployments.
