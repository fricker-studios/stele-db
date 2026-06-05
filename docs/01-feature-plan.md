# 01 — Feature Plan

> **Status:** Living inventory. Reprioritize freely; keep the tiering honest.
> **Read with:** [00 — Charter](00-charter.md) (the *why*) · [02 — Architecture](02-architecture.md) (the *how*) · [03 — Roadmap](03-roadmap.md) (the *when/in-what-order*).

This document is the complete feature inventory for Stele, split into **(A) the temporal/audit differentiators** and **(B) the general DBMS substrate** beneath them. Every feature carries a **tier** and a **target milestone**.

## How to read the tiers

- **Must** — Stele is not *Stele* without it. These define the identity or are load-bearing substrate that the identity needs.
- **Should** — strongly wanted; expected by serious users; can trail the Must set by a milestone or two.
- **Later** — real and planned, but explicitly deferred so the core stays sharp. Includes everything distribution-related.

> **The guardrail, restated:** features are prioritized to make Stele **world-class at analytical + temporal/audit** workloads and **adequate at transactional point operations**. Any feature whose only justification is "to beat Postgres at OLTP" or "to beat ClickHouse at a benchmark" is deprioritized on sight. See [Charter §3](00-charter.md#3-the-guardrail--lead-with-the-non-goal).

## Milestone shorthand

Milestones are *ordered*, not dated (this is a no-deadline track — see [03](03-roadmap.md)). The labels: **v0.1**, **v0.2**, **v0.3**, **v0.4**, **v0.5**, **v0.6**, **v0.7**, **v0.8**, **v0.9**, **v1.0**, **v2.0+**. Their meaning is defined in the [roadmap](03-roadmap.md#milestone-sequence-ordered-undated).

> The in-between versions (v0.4, v0.6, v0.8, v0.9) are **checkpoints** that subdivide the v0.5 / v0.7 / v1.0 anchors; the [roadmap](03-roadmap.md#milestone-sequence-ordered-undated) is authoritative for what each contains. A few feature rows below still carry their original anchor tag (e.g. the object-store tier as v0.5) where the roadmap has since pulled them into a checkpoint — retagging those rows is a tracked follow-up, not a contradiction.

---

# A. Differentiator features (the identity)

These are the reason Stele exists. They get the novelty budget.

## A.1 — Bitemporality

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **System-time versioning** | Every row carries a `system_time` period (`[valid_from, valid_to)`) set by the transaction that wrote it. Always-on. Never destructively overwritten. | Must | **v0.1** |
| **Valid-time versioning** | Per-table opt-in `valid_time` period describing when the fact holds in the modeled world; user- or app-supplied. | Must | **v0.1** |
| **Bitemporal tables** | A table can carry *both* axes simultaneously — the full 2D (system × valid) history. | Must | **v0.2** |
| **`FOR SYSTEM_TIME AS OF`** | SQL:2011-style point-in-time read on the system axis. | Must | **v0.1** |
| **Valid-time `AS OF` / period predicates** | `FOR VALID_TIME AS OF`, `CONTAINS`, `OVERLAPS`, `PRECEDES`, etc. | Must | **v0.2** |
| **Bitemporal `AS OF (sys, valid)`** | Joint point-in-time across both axes ("as we believed at T1, about the world at T2"). | Must | **v0.3** |
| **Temporal `BETWEEN`/range scans** | Return all versions over a system or valid interval. | Should | **v0.3** |
| **Temporal DDL** | `CREATE TABLE … WITH SYSTEM VERSIONING`, add/drop valid-time period, period constraints. | Must | **v0.2** |
| **Valid-time integrity** | Temporal primary keys and temporal foreign keys (no overlapping valid-time for the same key). | Should | **v0.5** |
| **Temporal `MERGE` semantics** | Upsert that correctly closes prior valid-time periods and opens new ones (the historization workhorse). | Must | **v0.3** |
| **Retroactive & post-dated changes** | Insert facts effective in the past or future on the valid-time axis without rewriting system history. | Should | **v0.5** |
| **Time-zone / calendar correctness** | UTC-internal, well-defined boundary semantics (half-open intervals), leap handling documented. | Must | **v0.2** |

## A.2 — Append-only / immutable storage & historization

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Append-only segment store** | Immutable columnar segments; writes append, never overwrite in place. | Must | **v0.1** |
| **Logical delete/update** | "Delete" closes a system-time period; "update" appends a new version. Physical bytes are retained. | Must | **v0.1** |
| **Efficient historization** | Version chains compressed so unchanged columns aren't re-stored wholesale (delta/dictionary encoding across versions). | Should | **v0.3** |
| **Compaction / merge** | Background compaction merges deltas into segments *without* losing history; produces read-optimized layouts. | Must | **v0.3** |
| **Retention & history policies** | Optional, explicit, audited policies to *physically* expire history older than a horizon (off by default; append-only is the default posture). | Should | **v0.7** |
| **Immutable-by-default guarantee** | A documented, test-enforced invariant: no code path mutates a sealed segment. | Must | **v0.2** |

## A.3 — As-of / time-travel query surface

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Point-in-time `SELECT`** | The core time-travel read (system axis). | Must | **v0.1** |
| **Session/statement time context** | `SET system_time = …` so a whole session reads "as of" a timestamp; default is "now/latest." | Should | **v0.3** |
| **Temporal joins** | Joins that respect as-of context across both tables (consistent snapshot across the query). | Must | **v0.3** |
| **Change-feed / "diff between two times"** | "What changed between T1 and T2?" as a query, not a CDC pipeline. | Should | **v0.5** |
| **Time-travel over schema changes** | As-of reads remain correct across DDL/schema evolution (versioned catalog). | Should | **v0.7** |

## A.4 — Lineage & provenance (first-class)

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Per-row transaction provenance** | Each version records the writing transaction id, commit time, and an auth principal (who/what/when). | Must | **v0.2** |
| **Provenance query surface** | Pseudo-columns / system functions to read a row's provenance inline (`_stele_txn_id`, `_stele_committed_at`, `_stele_principal`). | Must | **v0.3** |
| **Immutable audit log** | The WAL/commit log is itself an auditable, append-only record; tamper-evident hashing optional. | Should | **v0.5** |
| **Derivation lineage (opt-in)** | "This row was computed from those input rows by that statement." Column/row-level lineage graph. Expensive; opt-in. | Later | **v0.7+** |
| **Cryptographic verifiability** | Merkle/hash-chained commits so an external auditor can verify history wasn't altered — a [security pillar](#b8--security-authz--data-protection-pillar) feature, bumped earlier accordingly. | Should | **v0.7** |

## A.5 — Hash keys & MERGE/upsert

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Deterministic, portable hash keys** | Built-in hash functions for business keys, **stable across engine versions, platforms, and client languages** (published spec) so external models compute identical keys — the integration-groundwork primitive ([ADR-0011](adr/0011-hash-distribution-integration-groundwork.md)) any hash-keyed pattern (Data Vault included) needs, without implementing Data Vault. | Must | **v0.2** |
| **Fast `MERGE`/upsert** | High-throughput merge keyed on hash/PK, integrated with temporal close/open semantics. | Must | **v0.3** |
| **Bulk ingest path** | Batched, append-optimized load (`COPY`-style) that feeds the columnar writer directly. | Must | **v0.3** |
| **Idempotent ingest** | Hash-key + system-time make re-ingesting the same batch a no-op (exactly-once-ish loading). | Should | **v0.5** |

## A.6 — Columnar core with adequate point access

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Columnar storage + encodings** | Dictionary, RLE, bit-packing, FOR (frame-of-reference), delta; per-column codec selection. | Must | **v0.1** |
| **Vectorized scan/aggregation** | Batch-at-a-time (Arrow-shaped) execution; SIMD-friendly. | Must | **v0.2** |
| **Zone maps / min-max + zone skipping** | Per-segment statistics to skip blocks during scans (incl. time-range skipping). | Must | **v0.2** |
| **B-tree / point-lookup index** | A secondary access path giving *adequate* point lookups and small range reads. | Should | **v0.3** |
| **Bloom filters / hash index** | Accelerate hash-key point lookups and MERGE probes. | Should | **v0.3** |
| **Late materialization** | Defer column fetches until after predicate filtering. | Should | **v0.5** |

## A.7 — Object-storage tiering & storage lifecycle (storage/compute separation)

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Pluggable storage backends** | Trait-based: local disk, in-memory (test), S3-compatible. | Should | **v0.3** |
| **S3-compatible cold tier** | Sealed segments tier to object storage; metadata stays hot. | Should | **v0.5** |
| **Local hot cache** | LRU/“foyer”-style cache of hot segments/pages on local NVMe. | Should | **v0.5** |
| **Tiered archival (system-time-driven)** | Multi-tier ladder (Standard → IA/Glacier Instant → Deep Archive) driven by **system-time age**; *keeps every byte* — distinct from retention/expiry. Controls append-only cost growth ([ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md)). | Should | **v0.7** |
| **Time-era compaction** | Cluster segments by system-time era so cold segments are purely historical and age together cleanly. | Should | **v0.7** |
| **Async restore / rehydration** | Explicit `RESTORE` (admin API / SQL) for frozen (Glacier-class) data; planner returns "restore required" + estimate rather than hanging for hours. Data always still exists. | Should | **v0.7** |
| **Separation of storage & compute** | Compute nodes are (largely) stateless over shared object storage. | Later | **v0.7** |
| **Tier-aware planner** | The optimizer knows each segment's tier (hot/warm/cold/frozen), prunes via resident zone maps *before* rehydrating, and gates frozen-data access. | Should | **v0.7** |

---

# B. General DBMS substrate

The unglamorous foundation. Built to the standard the differentiators need — **adequate** on the transactional side, **excellent** where scans/temporal depend on it.

## B.1 — SQL surface

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Core DML** (`SELECT/INSERT/UPDATE/DELETE`) | With temporal semantics baked into U/D (logical, not physical). | Must | **v0.1** |
| **Core DDL** (`CREATE/ALTER/DROP TABLE`, schemas) | Including temporal table options. | Must | **v0.1** |
| **Joins** (inner/outer/semi/anti), **GROUP BY**, **aggregates** | The analytical bread-and-butter. | Must | **v0.2** |
| **Subqueries / CTEs** | Including correlated subqueries. | Must | **v0.3** |
| **Window functions** | Analytical staple. | Should | **v0.5** |
| **`MERGE` statement** | First-class (see A.5). | Must | **v0.3** |
| **`COPY` / bulk load** | (See A.5.) | Must | **v0.3** |
| **Views / materialized views** | MVs with temporal-aware refresh are a later item. | Should / Later | **v0.5 / v0.7** |
| **Prepared statements / parameter binding** | Required for pg-wire extended query protocol. | Must | **v0.2** |
| **`EXPLAIN` / `EXPLAIN ANALYZE`** | Plan + execution introspection. | Must | **v0.3** |
| **Recursive CTEs, `LATERAL`, set ops** | Completeness. | Should | **v0.5** |
| **Stored procedures / UDFs** | Out of early scope; revisit via extensibility (B.10). | Later | **v1.0+** |

## B.2 — Type system

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Scalars** | bool, integers, floats, decimal/numeric, text/varchar, bytea. | Must | **v0.1** |
| **Temporal types** | date, time, timestamp, timestamptz (UTC-internal), interval. | Must | **v0.1** |
| **`PERIOD` / range types** | First-class period type backing system/valid time. | Must | **v0.2** |
| **UUID, hash digests** | For hash keys and provenance ids. | Must | **v0.2** |
| **JSON/JSONB** | Semi-structured column support. | Should | **v0.5** |
| **Arrays, structs/nested** | Arrow-native nested types. | Should | **v0.5** |
| **Custom/extension types** | Via the type-extension API (B.10). | Later | **v1.0+** |
| **Null semantics & three-valued logic** | Correct SQL null behavior throughout. | Must | **v0.1** |

## B.3 — Indexing & access paths

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Zone maps / segment stats** | Primary scan-pruning mechanism (see A.6). | Must | **v0.2** |
| **B-tree secondary index** | Point/range access for the transactional minority. | Should | **v0.3** |
| **Hash index** | Hash-key lookups, MERGE probes. | Should | **v0.3** |
| **Bloom filters** | Negative-lookup pruning. | Should | **v0.3** |
| **Time-partitioned indexing** | Index structures aware of the time axes. | Should | **v0.5** |
| **Min/max + dictionary pushdown** | Predicate pushdown into encoded columns. | Should | **v0.5** |

## B.4 — Transactions, concurrency & MVCC

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **ACID single-node transactions** | Atomic commit, durable via WAL. | Must | **v0.1** |
| **MVCC over the append-only store** | Versions are *already* the storage model; MVCC reads pick a snapshot. | Must | **v0.2** |
| **Snapshot isolation (default)** | The v1 default isolation level. | Must | **v0.2** |
| **Read-committed** | Selectable lower level. | Should | **v0.3** |
| **Serializable (SSI)** | Serializable snapshot isolation as an opt-in. | Later | **v0.7** |
| **Multi-statement transactions** | `BEGIN/COMMIT/ROLLBACK`, savepoints. | Must | **v0.2** |
| **Deadlock / conflict handling** | Conflict detection + retry semantics. | Should | **v0.3** |

> **Design note:** because storage is append-only and bitemporal, MVCC is a *natural fit*, not a bolt-on — a reader's snapshot is "the latest system-time version ≤ my snapshot time." See [ADR-0008](adr/0008-mvcc-on-append-only.md).

## B.5 — Durability, WAL & crash recovery

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Write-ahead log** | Durable, append-only commit log; group commit. | Must | **v0.1** |
| **Crash recovery** | Deterministic replay to a consistent state; idempotent. | Must | **v0.1** |
| **Checkpoints / flushing** | Bound recovery time; flush delta tier to sealed segments. | Must | **v0.2** |
| **Torn-write / fsync correctness** | Verified against power-loss models in the sim harness ([06](06-testing-strategy.md)). | Must | **v0.2** |
| **Point-in-time recovery (PITR)** | Recover to any system-time — *trivial* given the model, but proven by test. | Should | **v0.5** |

## B.6 — Backup, restore & snapshots

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Full backup/restore** | Consistent, online backup of segments + WAL + catalog. | Must | **v0.3** |
| **Incremental backup** | Append-only makes incrementals natural (ship new segments + WAL). | Should | **v0.5** |
| **Snapshot/clone** | Cheap logical snapshot (a system-time pin). | Should | **v0.5** |
| **Object-store-native backup** | Backups *are* the object-store tier in the separated architecture. | Later | **v0.7** |

## B.7 — Replication & high availability

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **WAL streaming replica (read)** | Async log shipping to a read replica. | Later | **v0.7** |
| **Synchronous replication** | Quorum durability. | Later | **v1.0** |
| **Automatic failover** | Leader election (ties into distribution/consensus). | Later | **v1.0+** |
| **Distributed consensus** | Raft for control-plane metadata; data over shared object storage. | Later | **v2.0+** |
| **Clock sync requirement (NTP/PTP)** | Hard operational requirement on all nodes (the engine is time-native); operator-enforced preflight + monitoring ([ADR-0022](adr/0022-clock-synchronization-and-ordering.md)). | Later | **v2.0+** |
| **Hybrid Logical Clocks + skew fencing** | HLC for causally-consistent cross-node system-time under bounded skew; a node beyond the max-skew bound fences itself (fail-safe, never mis-orders history). | Later | **v2.0+** |
| **Hash distribution / partitioning** | Partition rows across nodes by a declared distribution (hash) key — the general scale-out primitive ([ADR-0011](adr/0011-hash-distribution-integration-groundwork.md)). | Later | **v2.0+** |
| **Key co-location / co-partitioning** | Co-partition tables joined on the same key so those joins stay node-local (no shuffle); generic groundwork for hub↔satellite-shaped access, without naming it. | Later | **v2.0+** |

> All of B.7 is gated behind the single-node core being rock-solid (Charter §3, [ADR-0006](adr/0006-distribution-later-shared-storage.md)). Hash distribution and co-location are *general* sharded-analytics primitives that also make hash-keyed models (Data Vault included) distribute cleanly — see [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md).

## B.8 — Security, authZ & data protection (pillar)

> **This is a [first-class pillar](00-charter.md#4-differentiating-primitives-the-identity), not substrate** — Stele targets financial and heavily-audited systems. The *identity-driven* half (tamper-evidence, provenance-as-audit, forensic time-travel, cryptographic verifiability) lives in [A.2](#a2--append-only--immutable-storage--historization) and [A.4](#a4--lineage--provenance-first-class); the table-stakes controls below are tiered higher and earlier than a typical engine. Full treatment, threat model, and the compliance roadmap (SOC 2 / HIPAA / PCI-DSS / GDPR) are in [10 — Security & Compliance](10-security-and-compliance.md) ([ADR-0018](adr/0018-security-auditability-pillar.md)).

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Memory-safety / exploit resistance** | Rust ownership eliminates whole exploit classes; `unsafe` minimized and gated by Miri/ASan/TSan/UBSan + fuzzing ([06](06-testing-strategy.md)). Foundational from day one. | Must | **v0.1** |
| **Secure defaults** | TLS on, telemetry off ([ADR-0015](adr/0015-telemetry-opt-in.md)), least-privilege, no default passwords, minimal network exposure. | Must | **v0.3** |
| **TLS in transit (+ mTLS)** | Wire encryption for pg-wire and the admin API; mutual-TLS option. | Must | **v0.3** |
| **Authentication** | SCRAM-SHA-256 (pg-compatible), mTLS, admin-API tokens. | Must | **v0.3** |
| **Roles & privileges (RBAC)** | `GRANT/REVOKE` on objects; least-privilege roles. | Must | **v0.5** |
| **Encryption at rest** | Envelope encryption: per-segment data keys wrapped by a key-encryption key; transparent on read/write ([ADR-0019](adr/0019-encryption-at-rest-kms.md)). | Must | **v0.5** |
| **External KMS + BYOK** | Pluggable KMS (AWS KMS, Vault, etc.); bring-your-own-key / hold-your-own-key. | Should | **v0.7** |
| **Encrypted backups** | Backups/object-store tier inherit at-rest encryption end to end. | Must | **v0.5** |
| **Access auditing** | Every read/write logged with principal/time via the provenance infra; tamper-evident; SIEM export. | Must | **v0.5** |
| **Row-level security** | Policy-based row visibility; pairs naturally with audit/lineage. | Should | **v0.5** |
| **Column-level security & masking** | Column grants + dynamic masking/redaction of sensitive fields (PII/PHI/PAN). | Should | **v0.7** |
| **ABAC / policy engine** | Attribute/policy-based access beyond roles (e.g., purpose, classification). | Later | **v0.7+** |
| **Namespace / tenant isolation + lifecycle** | Namespaces/schemas as a first-class isolation **and** lifecycle unit: per-namespace keys, residency, and policy; an **audited "drop namespace"** offboards a whole tenant as a clean break. General tenancy primitive — app maps tenant→namespace ([ADR-0020](adr/0020-crypto-shredding-erasure.md), [ADR-0009](adr/0009-data-vault-conceptual-seam.md)). | Should | **v0.5** |
| **Layered right-to-erasure** | Namespace-drop (destroy the namespace key) for tenant offboarding; per-subject crypto-shredding as the fine-grained backstop; PII sidecar + scoped physical expiry. All preserve append-only immutability ([ADR-0020](adr/0020-crypto-shredding-erasure.md)). | Should | **v0.7** |
| **Secrets management** | No plaintext secrets at rest; integration with external secret stores. | Should | **v0.5** |
| **Rate limiting / DoS resistance** | Connection/query quotas, resource limits, query-cost guards. | Should | **v0.5** |
| **Federated identity (OIDC/SSO/SAML)** | Enterprise SSO for the admin surfaces. | Later | **v1.0+** |

## B.9 — Observability & operability

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Structured logging** | `tracing`-based, leveled, structured. | Must | **v0.1** |
| **Metrics** | Prometheus/OpenMetrics endpoint (latency, throughput, compaction, cache hit rates). | Must | **v0.3** |
| **Distributed tracing** | OpenTelemetry spans through the query path. | Should | **v0.5** |
| **`EXPLAIN ANALYZE` + query stats** | Per-operator timing/rows. | Must | **v0.3** |
| **System catalogs / `pg_catalog` shims** | So pg admin tools work. | Should | **v0.5** |
| **Health/readiness endpoints** | For container orchestration. | Should | **v0.3** |
| **Slow-query log** | Operability. | Should | **v0.5** |

## B.10 — Extensibility

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Storage-backend trait** | Pluggable object stores (in scope early, see A.7). | Should | **v0.3** |
| **Scalar/aggregate UDFs** | Register functions (Rust first; scripting later). | Later | **v1.0** |
| **Foreign data / external tables** | Read Parquet/CSV/Iceberg in place. | Later | **v1.0+** |
| **Pluggable auth** | LDAP/OIDC hooks. | Later | **v1.0+** |
| **Extension API stability** | Versioned, semver'd extension surface. | Later | **v1.0+** |

## B.11 — Client interface & ecosystem

| Feature | Description | Tier | Milestone |
|---|---|---|---|
| **Postgres wire protocol — simple query** | Minimal pg-wire so `psql` connects and runs queries. | Must | **v0.1** |
| **pg-wire — extended query (prepared/bind)** | Drivers/ORMs need this. | Must | **v0.2** |
| **pg-wire — `COPY` protocol** | Bulk load over the wire. | Should | **v0.3** |
| **Driver/ORM compatibility matrix** | Verified against real clients (psql, JDBC, psycopg, pgx, SQLAlchemy). | Should | **v0.5** |
| **BI/admin tool compatibility** | DBeaver, Grafana, Metabase, etc. via pg-wire + catalog shims. | Should | **v0.7** |
| **Native CLI (`stele`)** | Admin + local query shell. | Must | **v0.2** |
| **Admin / control-plane API** | Dedicated gRPC + HTTP/JSON surface for ops (backup/restore/PITR, introspection, users/roles, metrics) — the shared substrate for the CLI, desktop app, operator, and SDKs ([ADR-0016](adr/0016-admin-control-plane-api.md)). | Should | **v0.3** |
| **`stele-client` SDK + thin language SDKs** | Rust client crate (crates.io) wrapping the admin API; Python/TS/Go wrappers later. SQL stays on existing PG drivers. | Should / Later | **v0.3 / v1.0+** |
| **Desktop app (Stele Studio)** | Tauri pgAdmin-style admin/query tool with temporal-native UI; analytics workflow later ([ADR-0012](adr/0012-desktop-app-tauri.md), [09](09-ecosystem-and-products.md)). | Later | **v0.7** |
| **Kubernetes/OpenShift operator + Helm** | Declarative install (Helm) + lifecycle automation (operator); OperatorHub + OpenShift-certified ([ADR-0013](adr/0013-kubernetes-openshift-operator.md)). | Later | **v0.5 / v0.7** |
| **Embedded/library mode** | Use the engine in-process (the eventual Solvia integration path — *capability only, no coupling*). | Later | **v1.0+** |

---

## C. Feature-to-milestone summary (at a glance)

| Milestone | Headline capability set |
|---|---|
| **v0.1** | Single-node append-only columnar store · system-time + valid-time storage · `AS OF` (system) reads · core DML/DDL · WAL + crash recovery · minimal pg-wire (psql connects) · `stele` CLI seed. |
| **v0.2** | Full bitemporal tables + temporal DDL · vectorized executor · zone maps · MVCC + snapshot isolation · multi-statement txns · per-row provenance · hash keys · pg-wire extended protocol. |
| **v0.3** | Bitemporal `AS OF` · temporal `MERGE`/upsert · bulk ingest · joins/CTEs · B-tree/hash/bloom indexes · compaction · backup/restore · metrics + `EXPLAIN ANALYZE` · pluggable storage backends · **authN + TLS/mTLS + secure defaults**. |
| **v0.5** | Object-store cold tier + hot cache · change-feed/diff · window functions · **RBAC · row-level security · encryption at rest (+ encrypted backups) · access auditing** · incremental backup/PITR · idempotent ingest · driver compat matrix · temporal integrity constraints. |
| **v0.7** | Storage/compute separation · read replicas (WAL streaming) · serializable isolation · derivation lineage (opt-in) · **column security + masking · external KMS/BYOK · cryptographic audit verifiability · crypto-shredding erasure** · BI-tool compatibility. |
| **v1.0** | Hardened, documented, semver-stable single-node (+ read-replica) engine · **completed security baseline + compliance posture (SOC 2 path) · federated SSO** · synchronous replication groundwork · extension API v1 · trust-gate met for first production use. |
| **v2.0+** | Distribution: Raft control plane + shared-object-storage data plane · distributed query · Jepsen-validated consistency · managed/cloud offering · the path to hosting Solvia. |

> Tiers and milestones are intentionally revisable. What is **not** revisable without amending the [Charter](00-charter.md): the asymmetric performance bar, append-only-by-default, and keeping Data Vault/Solvia concepts out of the engine.
