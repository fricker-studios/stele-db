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

## Conventions

- **Filename:** `NNNN-kebab-title.md`, zero-padded sequence number.
- **Status values:** `Proposed` · `Accepted` · `Deprecated` · `Superseded by ADR-NNNN`.
- **One decision per ADR.** If a record is arguing two things, split it.
- ADRs link *down* to the docs they justify ([01–07](../README.md)) and *up* to the [Charter](../00-charter.md).
