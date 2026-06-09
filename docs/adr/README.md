# Architecture Decision Records

An **ADR** captures one significant, hard-to-reverse decision: its **Context**, the **Decision**, its **Status**, and the **Consequences** (good and bad). Per the [Charter §6](../00-charter.md#6-guiding-principles), *every significant architectural decision becomes an ADR.* ADRs are immutable once `Accepted` — to change one, write a new ADR that supersedes it (and update the old one's Status to `Superseded by ADR-NNNN`).

Use the [template](_template.md) for new records. Number sequentially.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-implementation-language-rust.md) | Implementation language: Rust | Accepted |
| [0002](0002-on-disk-storage-format.md) | Custom append-only columnar on-disk format | Accepted |
| [0003](0003-postgres-wire-protocol-early.md) | Postgres wire protocol, early & incremental | Accepted |
| [0004](0004-licensing-bsl.md) | Licensing: BSL 1.1 → Apache-2.0 (4-year) | Accepted |
| [0005](0005-reproducible-builds-pinned-toolchain.md) | Reproducible builds & pinned toolchain | Accepted |
| [0006](0006-distribution-later-shared-storage.md) | Distribution later: Raft control plane + shared object storage | Accepted |
| [0007](0007-storage-compute-separation.md) | Separation of storage and compute via object storage | Accepted |
| [0008](0008-mvcc-on-append-only.md) | MVCC layered on the append-only store | Accepted |
| [0009](0009-data-vault-conceptual-seam.md) | Data Vault / Solvia kept out of the engine (conceptual seam only) | Accepted |
| [0010](0010-deterministic-simulation-testing.md) | Deterministic Simulation Testing as the core test method | Accepted |
| [0011](0011-hash-distribution-integration-groundwork.md) | Integration-groundwork primitives: hash distribution, DV-ready but DV-agnostic | Accepted |
| [0012](0012-desktop-app-tauri.md) | Desktop analytics app: Tauri, BSL, free community tool | Accepted |
| [0013](0013-kubernetes-openshift-operator.md) | Kubernetes/OpenShift operator + Helm, OperatorHub & certified | Accepted |
| [0014](0014-release-channels-and-versioning-policy.md) | Release channels & cross-artifact versioning/compatibility policy | Accepted |
| [0015](0015-telemetry-opt-in.md) | Telemetry: off by default, explicit opt-in | Accepted |
| [0016](0016-admin-control-plane-api.md) | Admin/control-plane API + client SDK strategy | Accepted |
| [0017](0017-default-network-port-5454.md) | Default network port: 5454 (pg-wire), configurable | Accepted |
| [0018](0018-security-auditability-pillar.md) | Security & Auditability as a first-class pillar | Accepted |
| [0019](0019-encryption-at-rest-kms.md) | Encryption at rest: envelope encryption + KMS / BYOK | Accepted |
| [0020](0020-crypto-shredding-erasure.md) | Right-to-erasure & data-lifecycle: layered (namespace-drop + crypto-shredding) | Accepted |
| [0021](0021-storage-lifecycle-tiered-archival.md) | Storage lifecycle: system-time-driven tiered archival | Accepted |
| [0022](0022-clock-synchronization-and-ordering.md) | Clock synchronization & cross-node time ordering (NTP + HLC + fencing) | Accepted |
| [0023](0023-append-only-record-model-validity-index.md) | Append-only record model: derived validity index (no stored `sys_to`) | Accepted |
| [0024](0024-time-representation.md) | Time representation: µs / int64 / +∞ sentinel / sequence | Accepted |
| [0025](0025-valid-time-indexing.md) | Valid-time indexing & the scatter problem | Accepted |
| [0026](0026-verifiable-audit-log.md) | Verifiable audit log (hash-chain + Merkle proofs) | Accepted |
| [0027](0027-vectorized-execution-model.md) | Vectorized execution: batch-at-a-time Volcano pull over Arrow-shaped batches | Accepted |

## Conventions

- **Filename:** `NNNN-kebab-title.md`, zero-padded sequence number.
- **Status values:** `Proposed` · `Accepted` · `Deprecated` · `Superseded by ADR-NNNN`.
- **One decision per ADR.** If a record is arguing two things, split it.
- ADRs link *down* to the docs they justify ([01–07](../README.md)) and *up* to the [Charter](../00-charter.md).
