# Assumptions Log

> A running log of assumptions made while planning Stele. Each entry is **labeled**, dated, and given a status. When an assumption is confirmed, revised, or killed, update its row — don't delete the history. Assumptions that harden into decisions graduate to an [ADR](adr/README.md).

**Legend — Status:** `CONFIRMED` (decided by the project owner) · `ASSUMED` (working assumption, proceed unless overturned) · `OPEN` (needs a decision before it blocks something) · `REVISED` (changed; see note).

---

## Confirmed in the founding session

These four were answered directly by the project owner and are treated as decided.

| # | Topic | Decision | Status | ADR |
|---|---|---|---|---|
| A1 | **Implementation language** | **Rust.** Memory safety without GC, strong concurrency, mature crate ecosystem suited to a from-scratch storage engine. | `CONFIRMED` | [ADR-0001](adr/0001-implementation-language-rust.md) |
| A2 | **Postgres wire-protocol timing** | **Early & incremental.** A minimal pg-wire shim from v0.1/v0.5 to validate with real clients (psql, drivers); coverage expands over time. | `CONFIRMED` | [ADR-0003](adr/0003-postgres-wire-protocol-early.md) |
| A3 | **Minimum-interesting v0.1** | **Bitemporal core + as-of.** Single-node, append-only storage; historizing INSERT/UPDATE; and as-of SELECT across *both* system-time and valid-time. Demonstrates the project's identity first. | `CONFIRMED` | — (see [03](03-roadmap.md)) |
| A4 | **Licensing** | **BSL 1.1 → Apache-2.0, 4-year Change Date**, with an Additional Use Grant permitting non-competing production use and an anti-managed-service clause. | `CONFIRMED` | [ADR-0004](adr/0004-licensing-bsl.md) |
| A20 | **Distribution + cloud storage in scope** | **Confirmed kept.** Full distribution and plug-and-play, pluggable S3-compatible cloud storage with storage/compute separation remain firmly in scope; "later phase" is *sequencing* (single-node correctness first), not deprioritization or a scope cut. | `CONFIRMED` | [ADR-0006](adr/0006-distribution-later-shared-storage.md), [ADR-0007](adr/0007-storage-compute-separation.md) |
| A21 | **DV/Solvia integration groundwork** | **Confirmed.** Build general-primitive groundwork (stable/portable hash functions, hash distribution + co-location, hash-keyed bitemporal MERGE, inline lineage) so DV/Solvia integration is seamless — while keeping all DV/RCM concepts out of the engine (the bright line). | `CONFIRMED` | [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md), [ADR-0009](adr/0009-data-vault-conceptual-seam.md) |
| A22 | **Desktop app** | **Tauri** (Rust core + native webview), licensed **BSL 1.1**, a free source-available community tool ("Stele Studio"); pgAdmin-style admin/query tool first, analytics workflow later. | `CONFIRMED` | [ADR-0012](adr/0012-desktop-app-tauri.md) |
| A23 | **Kubernetes/OpenShift** | Ship **both a Helm chart and a full operator**; list on **OperatorHub** and pursue **Red Hat OpenShift certification**. | `CONFIRMED` | [ADR-0013](adr/0013-kubernetes-openshift-operator.md) |
| A24 | **Releases & versioning** | **Three channels** (edge/beta/stable) + **coordinated-but-independent SemVer** with explicit per-surface compatibility contracts (on-disk format the most conservative). | `CONFIRMED` | [ADR-0014](adr/0014-release-channels-and-versioning-policy.md) |
| A25 | **Telemetry** | **Off by default, explicit opt-in**, anonymous aggregate only; across engine, CLI, and desktop app. | `CONFIRMED` | [ADR-0015](adr/0015-telemetry-opt-in.md) |
| A26 | **Admin surface & SDKs** | A dedicated **admin/control-plane API** (gRPC + HTTP gateway) for ops; **rely on existing PG drivers** for SQL; ship `stele-client` (Rust) + thin language SDKs later. | `CONFIRMED` | [ADR-0016](adr/0016-admin-control-plane-api.md) |
| A27 | **Default network port** | **5454** for pg-wire by default, **configurable** (consumers can override). Distinct from Postgres 5432 for identity and to avoid clashing with a local Postgres; IANA-unassigned, in the safe band (1024–32767). | `CONFIRMED` | [ADR-0017](adr/0017-default-network-port-5454.md) |
| A28 | **Security is a pillar** | **Security & Auditability elevated to a first-class, unified pillar** (audit-native = security-native): world-class at identity-driven security, excellent at table-stakes controls, plus concrete technical security (encryption, exploit protection). | `CONFIRMED` | [ADR-0018](adr/0018-security-auditability-pillar.md) |
| A29 | **Compliance targets** | Roadmap targets **SOC 2 Type II, HIPAA, PCI-DSS, GDPR / data residency** (targets, not current claims; tied to the trust gate). | `CONFIRMED` | [10 §10](10-security-and-compliance.md#10-compliance-roadmap) |
| A30 | **Encryption at rest** | **Envelope encryption** (per-segment DEK wrapped by KMS-held KEK) with **external KMS + BYOK/HYOK**; transparent, rotatable, inherited by backups/object tier. | `CONFIRMED` | [ADR-0019](adr/0019-encryption-at-rest-kms.md) |
| A31 | **Right-to-erasure & data lifecycle** | **Layered strategy:** namespace-drop (destroy the per-namespace key) primary for tenant offboarding; per-subject crypto-shredding as fine-grained backstop; PII sidecar + scoped physical expiry. All preserve append-only immutability. Namespaces are a first-class engine tenancy primitive (per-namespace keys + audited drop); app maps tenant→namespace. Engine=mechanism, controller=policy. | `CONFIRMED` | [ADR-0020](adr/0020-crypto-shredding-erasure.md), [ADR-0019](adr/0019-encryption-at-rest-kms.md) |

---

## Working assumptions (proceed unless overturned)

| # | Area | Assumption | Status | Notes / trigger to revisit |
|---|---|---|---|---|
| A5 | Topology ordering | **Single-node first; distribution is a later phase.** The bitemporal core must be rock-solid on one node before any multi-node work. | `ASSUMED` | Strongly implied by the brief's "distribution (later phase)" and the 1/5/10-yr arc. Revisit only if a single-node ceiling appears early. [ADR-0006](adr/0006-distribution-later-shared-storage.md) |
| A6 | Target platforms | **Linux x86-64 is the primary tier-1 target.** macOS (x86-64 + Apple Silicon/arm64) is tier-1 for *development*. Linux arm64 is tier-2. Windows is tier-3 (CI-built, not a priority). | `ASSUMED` | DB servers run on Linux; contributors run macOS. Reassess if a Windows-heavy contributor base appears. |
| A7 | In-memory format | **Apache Arrow-shaped columnar batches** for the in-memory/execution representation, to inherit interoperability and avoid reinventing a vector format. On-disk format is Stele's own (Arrow is not a storage format). | `ASSUMED` | If Arrow's churn or dependency weight becomes a problem, fall back to a bespoke columnar batch. [ADR-0002](adr/0002-on-disk-storage-format.md) |
| A8 | On-disk format | **A custom Stele columnar segment format** (open-spec, versioned), *inspired by* Parquet/ORC but designed around the bitemporal record model and append-only segments + a row-oriented WAL/delta tier. We do **not** adopt Parquet as the primary write format. | `ASSUMED` | Parquet export/import is a compatibility feature, not the core format. [ADR-0002](adr/0002-on-disk-storage-format.md) |
| A9 | SQL dialect | **SQL with SQL:2011 temporal predicates** (`FOR SYSTEM_TIME AS OF`, `AS OF`, period predicates) as the north star, surfaced through Postgres-compatible syntax wherever the two agree. | `ASSUMED` | Where SQL:2011 and Postgres conventions conflict, document the choice; lean Postgres-compatible for ecosystem reasons. |
| A10 | Concurrency model | **MVCC layered on the append-only store**, with snapshot isolation as the default isolation level for v1; serializable as a later option. | `ASSUMED` | The append-only core makes MVCC natural. [ADR-0008](adr/0008-mvcc-on-append-only.md) |
| A11 | Object storage | **S3-compatible API** (AWS S3, MinIO, R2, GCS via compat layer) as the tiering target; local-disk and in-memory backends for dev/test. | `ASSUMED` | Pluggable object-store trait so backends are swappable. [ADR-0007](adr/0007-storage-compute-separation.md) |
| A12 | Distribution mechanism | When distribution arrives, **Raft for metadata/control-plane consensus** and **shared object storage for data** (compute nodes are largely stateless over S3), rather than a Paxos-from-scratch or a Spanner-style TrueTime approach. | `ASSUMED` | Decision deferred; recorded as a *direction*, not a commitment. [ADR-0006](adr/0006-distribution-later-shared-storage.md) |
| A13 | Async runtime | **Tokio** as the async runtime; **but** the storage engine's deterministic core is written runtime-agnostic so it can run under the simulation harness's virtual clock/scheduler. | `ASSUMED` | DST requirement ([06](06-testing-strategy.md)) constrains how tightly we may couple to Tokio. |
| A14 | Rust edition / MSRV | **Rust 2024 edition** (stable since 1.85, Feb 2025). MSRV pinned and bumped deliberately; toolchain pinned via `rust-toolchain.toml`. | `ASSUMED` | Current as of June 2026. [05](05-dev-environment.md), [04](04-cicd.md). |
| A15 | Licensing — Change License legality | **Apache-2.0 is a valid BSL Change License.** BSL 1.1 requires a Change License "compatible with GPL v2.0 or later"; Apache-2.0 is GPLv3-compatible (a "later" version), and CockroachDB used Apache-2.0 as its BSL change license as precedent. | `ASSUMED` | If a lawyer disputes this, fall back to MPL-2.0 or GPLv2+ as Change License. [ADR-0004](adr/0004-licensing-bsl.md) |
| A16 | Governance | **BDFL/maintainer-led** open governance initially (single steward + contributors), evolving toward a small maintainer council as the community grows. No foundation donation in the foreseeable term. | `ASSUMED` | [07](07-licensing-and-oss.md) |
| A17 | Provenance scope (v1) | First-class lineage means **per-row transaction provenance** (who/what/when wrote this version) in v1; *derivation* lineage (this row computed from those inputs) is a later, opt-in feature. | `ASSUMED` | Full derivation lineage is expensive; phase it. [02](02-architecture.md) §lineage. |
| A18 | Benchmark identity | Benchmarks are **correctness-gated and asymmetric**: analytical (ClickBench/TPC-H-derived) + a bespoke temporal/as-of suite are first-class; transactional (TPC-C-like) is a *floor*, not a target to win. | `ASSUMED` | Reinforces the §3 guardrail in the charter. [06](06-testing-strategy.md) |
| A19 | Docs/site | Docs are **Markdown-first in-repo** (this set), published later via a static site (Docusaurus or mdBook) at `steledb.com`. | `ASSUMED` | [07](07-licensing-and-oss.md) |

---

## Open questions (decide before they block work)

| # | Question | Why it matters | Blocks |
|---|---|---|---|
| O1 | Exact MSRV floor and bump cadence. | Affects which crate features we can use and the CI matrix. | Not blocking now; decide before v0.1 tag. |
| O2 | Final on-disk segment format spec (encodings, compression codecs, footer layout). | The format is the hardest thing to change post-data. | Blocks v0.1 storage work; needs its own design doc + ADR amendment. |
| O3 | Whether valid-time is *required* on every table or *opt-in per table*. | Affects the catalog, SQL surface, and storage overhead. | Blocks the temporal DDL design. Leaning **opt-in per table** (system-time always-on, valid-time opt-in). |
| O4 | Trademark strategy for "Stele"/"steledb" and any logo. | Name protection for the OSS + future commercial split. | Not blocking; resolve before a public launch. [07](07-licensing-and-oss.md) |
| O5 | Whether to expose a native (non-pg) wire protocol at all, or pg-wire only. | Affects client-library surface area. | Leaning **pg-wire only** for external clients; native API is in-process/embedded. |

---

*How to use this file:* when you make a planning assumption anywhere in `/docs`, add a row here and link to it. When an assumption is settled, update its status and, if it's load-bearing, open or amend an [ADR](adr/README.md). This file is the project's "what we decided to take on faith, and why" ledger.
