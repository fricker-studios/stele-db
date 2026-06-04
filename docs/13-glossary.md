# 13 — Glossary

> Plain-language definitions of the terms used across these docs. Links point to where each is treated in depth. New to Stele? Read this alongside the [Charter](00-charter.md).

### ABAC (Attribute-Based Access Control)
Authorization by attributes/policies (purpose, data classification, clearance) rather than only static roles. Later than RBAC. See [10 §6](10-security-and-compliance.md#6-authorization).

### Append-only storage
The core discipline: writes *append* new versions; old versions are retained and never destructively overwritten. "Update" and "delete" are logical. See [01 §A.2](01-feature-plan.md#a2--append-only--immutable-storage--historization), [Charter §6](00-charter.md#6-guiding-principles).

### As-of query (time-travel)
A query that reads the data *as of* a point in time — `SELECT … FOR SYSTEM_TIME AS OF …` (and the valid-time equivalent). Time-travel is a query, not a backup restore. See [01 §A.3](01-feature-plan.md#a3--as-of--time-travel-query-surface).

### Bitemporality
Storing two independent time axes per row: **system-time** (when the DB recorded it) and **valid-time** (when the fact holds in the modeled world). The engine's defining capability. See [02 §2](02-architecture.md#2-the-bitemporal-record-model).

### BYOK / HYOK (Bring/Hold Your Own Key)
Customer controls the encryption key; revoking it renders data unreadable. Enables per-tenant key control. See [ADR-0019](adr/0019-encryption-at-rest-kms.md).

### Catalog
The versioned metadata store (schemas, tables, namespaces). Itself versioned so time-travel survives schema changes. See [02 §5](02-architecture.md#5-catalog--metadata).

### Columnar storage
Data laid out by column (not row) for fast scans/aggregation — Stele's primary structure. See [02 §3](02-architecture.md#3-storage-engine-internals).

### Compaction
Background merging of delta + segments into read-optimized layouts — **history-preserving** (never discards versions). **Time-era compaction** clusters segments by system-time era for clean tiering. See [02 §3.1](02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving), [ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md).

### Crypto-shredding
Erasure by destroying a key rather than mutating data: the ciphertext stays in the immutable store but becomes permanently unreadable. Used at **namespace** granularity (primary) and **per-subject** (backstop). See [ADR-0020](adr/0020-crypto-shredding-erasure.md).

### Data Vault
A logical data-modeling pattern (hubs/links/satellites). **Not in the engine** — Stele provides primitives (hash keys, MERGE, lineage) that make it cheap; the pattern lives in apps like Solvia. See [ADR-0009](adr/0009-data-vault-conceptual-seam.md).

### DEK / KEK / NEK (Data / Key-Encryption / Namespace key)
The envelope-encryption hierarchy: a per-segment **DEK** is wrapped by a per-namespace **NEK**, wrapped by a root **KEK** in a KMS. Destroying a NEK shreds a whole namespace. See [ADR-0019](adr/0019-encryption-at-rest-kms.md).

### Delta tier
The recent-writes layer (row-oriented, in-memory + spill) in front of the immutable columnar segments; flushed to segments by compaction. See [02 §3.1](02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving).

### Deterministic Simulation Testing (DST)
Running the whole engine inside a simulator that controls all non-determinism (time, disk, network, RNG), so rare bugs are found and **any failure replays from a seed**. The FoundationDB/TigerBeetle method; Stele's core test approach. See [06 §5](06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece), [ADR-0010](adr/0010-deterministic-simulation-testing.md).

### Envelope encryption
Encrypting data with a data key, then encrypting (wrapping) that key with another key. Enables rotation and BYOK without re-encrypting data. See [ADR-0019](adr/0019-encryption-at-rest-kms.md).

### Hash key
A deterministic, stable, portable hash of a business key — the integration-groundwork primitive for hash-keyed models and hash distribution. See [01 §A.5](01-feature-plan.md#a5--hash-keys--mergeupsert), [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md).

### HLC (Hybrid Logical Clock)
A clock combining physical time with a logical counter, giving causally-consistent ordering across nodes under bounded clock skew. How Stele orders system-time when distributed. See [ADR-0022](adr/0022-clock-synchronization-and-ordering.md).

### Lineage / Provenance
First-class metadata answering "who/what/when wrote this version" (and, later, "derived from what inputs"). Stored inline, captured at commit. See [02 §8](02-architecture.md#8-lineage--provenance-subsystem), [01 §A.4](01-feature-plan.md#a4--lineage--provenance-first-class).

### MERGE / upsert
A hash-keyed insert-or-update that, in Stele, correctly closes prior valid-time periods and opens new ones — the historization workhorse. See [01 §A.5](01-feature-plan.md#a5--hash-keys--mergeupsert).

### MVCC (Multi-Version Concurrency Control)
Concurrency via versioned reads against a snapshot. In Stele it falls out of the append-only store: a snapshot read is "the latest system-time version ≤ my snapshot." Default isolation: snapshot isolation. See [ADR-0008](adr/0008-mvcc-on-append-only.md).

### Namespace (schema)
A first-class isolation **and** lifecycle unit: each can carry its own encryption key, residency, and policy, and supports an **audited drop** (tenant offboarding / clean-break erasure). Apps map tenant→namespace. See [02 §5](02-architecture.md#5-catalog--metadata), [ADR-0020](adr/0020-crypto-shredding-erasure.md).

### NTP / PTP
Network/Precision Time Protocols for clock synchronization. **Required** on distributed nodes (the engine is time-native) — but a baseline, paired with [HLC](#hlc-hybrid-logical-clock) + skew fencing for correctness. PTP/cloud time-sync (microsecond) tightens the bound. See [ADR-0022](adr/0022-clock-synchronization-and-ordering.md).

### Object-storage tiering
Storing cold sealed segments in S3-compatible object storage with a local hot cache; the basis for storage/compute separation. See [02 §4](02-architecture.md#4-object-storage-tiering--storagecompute-separation), [ADR-0007](adr/0007-storage-compute-separation.md).

### pg-wire (Postgres wire protocol)
The protocol Stele speaks so existing Postgres drivers/ORMs/BI tools work for free. Stele implements the *protocol*, not Postgres's internals. Default port **5454** ([ADR-0017](adr/0017-default-network-port-5454.md)). See [ADR-0003](adr/0003-postgres-wire-protocol-early.md).

### RBAC / RLS / CLS
Role-Based Access Control; Row-Level Security; Column-Level Security (with masking). Layered authorization. See [10 §6](10-security-and-compliance.md#6-authorization).

### Retention / expiry
The *deletion* lever (off by default): explicit, audited physical removal of history past a horizon. Distinct from **tiering** (which keeps data). See [01 §A.2](01-feature-plan.md#a2--append-only--immutable-storage--historization).

### Segment
An immutable, self-describing, columnar on-disk file (the sealed storage unit), with zone maps, bloom filters, checksums, and provenance columns. See [02 §3.2](02-architecture.md#32-on-disk-segment-format), [ADR-0002](adr/0002-on-disk-storage-format.md).

### Snapshot isolation
The default isolation level: a transaction reads a consistent snapshot (a system-time point) and never sees writes outside it. See [ADR-0008](adr/0008-mvcc-on-append-only.md).

### Solvia
A separate lab-RCM SaaS that Stele may *eventually* host. Fully decoupled until Stele earns trust; never coupled into the engine. See [Charter §7](00-charter.md#7-the-solvia-seam-designed-for-decoupled).

### Stele / Stele Studio
**Stele** — the engine (this project). **Stele Studio** — the Tauri desktop analytics app built on it ([ADR-0012](adr/0012-desktop-app-tauri.md)).

### System-time
The time axis recording *when the database held a version* (set by the committing transaction). Always present; the engine's ordering spine. See [02 §2](02-architecture.md#2-the-bitemporal-record-model).

### Tamper-evidence
The property that history can't be silently altered — from immutability + checksums + (later) hash-chained/Merkle commits, verifiable by an external auditor. See [10 §3](10-security-and-compliance.md#3-identity-driven-security-the-differentiator).

### Tier (hot / warm / cold / frozen)
Storage cost/latency levels: hot (local cache) → warm (S3 Standard) → cold (S3-IA/Glacier Instant) → frozen (Glacier Deep Archive, hours to restore). Driven by system-time age. See [ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md).

### Trust gate
The defined, tested bar (sim/oracles/recovery/security/community) that must be green before Stele holds **any production data**. See [Charter §8](00-charter.md#8-the-trust-gate-no-production-data-stated-plainly), [06 §9](06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized).

### Valid-time
The time axis recording *when a fact is true in the modeled world* (user/app-supplied). Per-table opt-in. Pairs with system-time for full bitemporality. See [02 §2](02-architecture.md#2-the-bitemporal-record-model).

### WAL (Write-Ahead Log)
The durable, append-only commit log; the **fsync at commit is the only durability point**, and crash recovery replays it. See [02 §3.4](02-architecture.md#34-write-path-sequence), [01 §B.5](01-feature-plan.md#b5--durability-wal--crash-recovery).

### Zone map
Per-segment min/max (and other) statistics that let the planner **skip** segments during a scan — including by time range — without reading them. Stays resident even when data is archived. See [02 §3.1](02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving), [ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md).
