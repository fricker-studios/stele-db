# Stele — Documentation

> **Stele** is a from-scratch, **append-only, bitemporal, audit-native analytical database engine**, written in Rust. It competes on **correctness, time-travel, and auditability** — not on simultaneously out-benchmarking ClickHouse and Postgres (an [explicit non-goal](00-charter.md#3-the-guardrail--lead-with-the-non-goal)).
>
> The name evokes an inscribed stone slab that preserves a record permanently — and, in botany, the central column of a plant (the columnar core). Reserved home: `steledb.com`.

This `/docs` set is the **single source of truth** for the project's vision, architecture, and operations. **Definition of done:** a newcomer — or future-you with no memory of the founding session — can read these and understand the vision, the full feature set, the architecture, how to build/test/run locally, how CI/CD and releases work, and the 1/5/10-year plan, *with zero additional context.*

## Read in order

| # | Document | What it answers |
|---|---|---|
| 00 | [**Charter**](00-charter.md) | Why Stele exists; scope; the non-goals; principles; success criteria. **Start here.** |
| 01 | [**Feature Plan**](01-feature-plan.md) | The full feature inventory (differentiators + general DBMS), tiered must/should/later and mapped to milestones. |
| 02 | [**Architecture**](02-architecture.md) | System design with Mermaid diagrams: storage internals, bitemporal model, query layer, pg-wire, lineage, txn/concurrency, distribution. |
| 03 | [**Roadmap**](03-roadmap.md) | What v0.1/v0.5/v1.0 mean; the ordered (undated) milestone sequence; the 1/5/10-year vision. |
| 04 | [**CI/CD**](04-cicd.md) | GitHub Actions: build/lint/test, sanitizers, fuzzing, MSRV, benchmark-regression gate, signed releases, branch strategy. |
| 05 | [**Dev Environment**](05-dev-environment.md) | Toolchain, build/test/run, the canonical Docker image, the `stele` CLI, devcontainer/Nix/devbox, the five-minute path. |
| 06 | [**Testing Strategy**](06-testing-strategy.md) | Unit, property, **deterministic simulation testing**, fuzzing, crash/recovery, bitemporal correctness oracles, Jepsen, benchmarks, the trust gate. |
| 07 | [**Licensing & OSS**](07-licensing-and-oss.md) | BSL 1.1 → Apache-2.0; contribution model; governance; docs-site; community; trademark. |
| 08 | [**Packaging, Distribution & Releases**](08-packaging-distribution-and-releases.md) | The artifact catalog; tagged Docker images; binaries; package registries; release channels; **cross-artifact versioning & compatibility policy**; supply-chain signing/SBOM/provenance; docs-per-release. |
| 09 | [**Ecosystem & Products**](09-ecosystem-and-products.md) | The CLI/REPL; the **Tauri desktop app** (Stele Studio); the admin/control-plane API + client SDKs; the **K8s/OpenShift operator** + Helm; docs/marketing site; WASM playground; telemetry/privacy. |
| 10 | [**Security & Compliance**](10-security-and-compliance.md) | Security as a **first-class pillar**: threat model; identity-driven security; encryption (TLS/at-rest/KMS/BYOK); authN/authZ (RBAC/RLS/CLS/ABAC); access auditing; memory-safety/exploit protection; compliance roadmap (SOC 2/HIPAA/PCI-DSS/GDPR); crypto-shredding erasure. |

## Supporting material

- [**Architecture Decision Records**](adr/README.md) — one record per significant decision (Context / Decision / Status / Consequences). Twenty ADRs to date.
- [**Assumptions log**](assumptions.md) — the running ledger of what was decided on faith, and the open questions.

## The thesis in four SQL statements

```sql
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;
--   → 100   (time-travel: the value *before* the update — history is never destroyed)
```

## The three load-bearing commitments (don't break these without amending the [Charter](00-charter.md))

1. **No dual-benchmark heroics.** World-class at analytical + temporal/audit; *adequate* at transactional point ops; never both-at-once. ([§3](00-charter.md#3-the-guardrail--lead-with-the-non-goal))
2. **Append-only by default; correctness over speed.** No production data until the [trust gate](06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized) is met.
3. **Data Vault / Solvia stay out of the engine — but the groundwork is ready.** The engine contains no hub/link/satellite/claim; it *does* provide general primitives (stable hash keys, hash distribution + co-location, hash-keyed MERGE, lineage) that make integration seamless. Bright line: [ADR-0009](adr/0009-data-vault-conceptual-seam.md); groundwork: [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md).

## Decisions at a glance (confirmed in the founding session)

| Decision | Choice | ADR |
|---|---|---|
| Implementation language | **Rust** (edition 2024) | [0001](adr/0001-implementation-language-rust.md) |
| On-disk format | **Custom append-only columnar** segments + row WAL/delta | [0002](adr/0002-on-disk-storage-format.md) |
| Postgres wire protocol | **Early & incremental** (simple query in v0.1) | [0003](adr/0003-postgres-wire-protocol-early.md) |
| Licensing | **BSL 1.1 → Apache-2.0, 4-year** Change Date | [0004](adr/0004-licensing-bsl.md) |
| Distribution | **Later**: Raft control plane + shared object storage | [0006](adr/0006-distribution-later-shared-storage.md) |
| Concurrency | **MVCC on append-only**, snapshot isolation default | [0008](adr/0008-mvcc-on-append-only.md) |
| Core test method | **Deterministic Simulation Testing** | [0010](adr/0010-deterministic-simulation-testing.md) |
| DV/Solvia integration | **Groundwork via general primitives** (hash distribution, stable hashes); no DV concepts in-engine | [0009](adr/0009-data-vault-conceptual-seam.md) · [0011](adr/0011-hash-distribution-integration-groundwork.md) |
| Desktop app | **Tauri**, BSL, free community tool ("Stele Studio") | [0012](adr/0012-desktop-app-tauri.md) |
| Kubernetes | **Operator + Helm**, OperatorHub + OpenShift-certified | [0013](adr/0013-kubernetes-openshift-operator.md) |
| Releases & versioning | **3 channels + cross-artifact SemVer/compat policy** | [0014](adr/0014-release-channels-and-versioning-policy.md) |
| Telemetry | **Off by default, explicit opt-in** | [0015](adr/0015-telemetry-opt-in.md) |
| Admin surface | **Dedicated control-plane API**; PG drivers for SQL | [0016](adr/0016-admin-control-plane-api.md) |
| Default port | **5454** (pg-wire), configurable — not 5432, for identity + no local-PG clash | [0017](adr/0017-default-network-port-5454.md) |
| Security | **First-class pillar** (audit-native = security-native); world-class identity-driven security + excellent table-stakes controls | [0018](adr/0018-security-auditability-pillar.md) |
| Encryption at rest | **Envelope encryption + external KMS + BYOK/HYOK** | [0019](adr/0019-encryption-at-rest-kms.md) |
| Right-to-erasure | **Layered**: namespace-drop (per-namespace key destruction) + per-subject crypto-shredding; preserves append-only | [0020](adr/0020-crypto-shredding-erasure.md) |
| Tenancy | **Namespaces** as first-class isolation + lifecycle units (per-namespace keys, audited drop); app maps tenant→namespace | [0020](adr/0020-crypto-shredding-erasure.md) · [0009](adr/0009-data-vault-conceptual-seam.md) |

---

*These documents are planning artifacts for a deliberately long-horizon, no-deadline craft project. The order of work is committed; the pace is whatever correctness demands.*
