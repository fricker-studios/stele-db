# 00 — Stele Charter

> **Status:** Founding charter. Authoritative.
> **Audience:** Anyone — a new contributor, a future maintainer, or future-you with no memory of the founding session — who needs to understand *why Stele exists and what it refuses to become.*
> **Read next:** [01 — Feature Plan](01-feature-plan.md) · [02 — Architecture](02-architecture.md) · [03 — Roadmap](03-roadmap.md) · [ADR index](adr/README.md)

---

## 1. What Stele is

**Stele is a from-scratch, append-only, bitemporal, audit-native analytical database engine.**

The name carries the whole thesis. A *stele* is an inscribed stone slab raised to preserve a record permanently — nothing is erased, only added. In botany, the *stele* is the central column of a plant: its structural and conductive core. Both meanings are the design: **a permanent, append-only record built around a columnar core.** The reserved home is `steledb.com`.

Stele is written in **Rust** ([ADR-0001](adr/0001-implementation-language-rust.md)). It is a long-horizon craft project on the systems/hobby track: there is no delivery clock, no customer deadline, and no production data until the engine has *earned* it. Correctness, time-travel, and auditability are the product. Raw throughput is a constraint to satisfy, never the headline.

## 2. The thesis (one paragraph)

Most databases treat history as a side-effect you bolt on with triggers, audit tables, CDC pipelines, and slowly-changing-dimension gymnastics. Stele treats history as the **primary key of reality**: every fact is stored with *when it was true in the world* (valid time) and *when the system learned it* (system/transaction time), and nothing is ever destructively overwritten. On that foundation, "what did this table look like last Tuesday, as we understood it at month-end close?" is a first-class query, not an archaeology project. Stele competes on **correctness, time-travel, and auditability** for analytical and temporal workloads — a space where Postgres is awkward and ClickHouse is indifferent.

## 3. The guardrail — lead with the non-goal

> **Stele will not try to out-benchmark ClickHouse on analytics *and* Postgres on transactions at the same time. That dual-benchmark goal is an explicit, permanent non-goal and a known graveyard.**

Every credible from-scratch database that died chasing "OLTP and OLAP, both world-class, in one box" died because the two workloads pull the storage engine in opposite directions (row-vs-column layout, point-write latency vs scan throughput, small hot working sets vs cold sequential sweeps). Stele picks a side and stays there:

- **World-class** at analytical + temporal/audit workloads.
- **Adequate** at transactional point operations (point lookups, single-row upserts, small reads).
- **Never the reverse, and never both-at-once heroics.**

When a design decision forces a trade between "faster point-write" and "cleaner temporal/scan semantics," the temporal/scan side wins by default. This guardrail is load-bearing. It is repeated in [01](01-feature-plan.md), [02](02-architecture.md), and [03](03-roadmap.md) on purpose.

## 4. Differentiating primitives (the identity)

These are non-negotiable. They are *what Stele is*; everything else is plumbing in service of them.

| Primitive | Plain-English meaning |
|---|---|
| **Bitemporality** | Every row carries both **system time** (when the DB recorded it) and **valid time** (when the fact holds in the modeled world). Both are queryable. |
| **Append-only / immutable storage** | Writes append new versions; old versions are retained and efficiently historized. "Delete" and "update" are logical, not physical. |
| **As-of / point-in-time queries** | `SELECT … FOR SYSTEM_TIME AS OF …` and the valid-time equivalent. Time-travel is a query, not a backup restore. |
| **Lineage & provenance as first-class** | Every record can answer "where did I come from, by what transaction, derived from what inputs?" Provenance metadata lives in the engine, not in an app-side audit table. |
| **Hash-key support + fast `MERGE`/upsert** | Deterministic hash keys and a high-throughput merge/upsert path — the ingestion primitive audit and historization lean on. |
| **Columnar core with adequate point-lookup** | Columnstore for scans and aggregation; a B-tree/point-access path that is *good enough* for the transactional minority. |
| **Object-storage tiering (S3-compatible)** | Separation of storage and compute; cold data lives in object storage, hot data is cached locally. |
| **Distribution** | A later-phase capability, designed-for but not built first. |

If a proposed feature does not strengthen one of these or the general-DBMS substrate beneath them, it is out of scope until proven otherwise.

## 5. Scope

### In scope
- A single-node engine first: bitemporal storage, append-only columnar format, as-of queries, MERGE/upsert, a working SQL surface, WAL + crash recovery, and a **minimal-but-incremental Postgres wire-protocol front end from early on** ([ADR-0003](adr/0003-postgres-wire-protocol-early.md)).
- Object-storage tiering and storage/compute separation as the engine matures.
- Distribution, consensus, and a managed/cloud offering as **later** phases.
- The general DBMS substrate (types, indexing, transactions/MVCC, durability, backup/restore, security, observability, extensibility) — built to the standard the differentiators require, no more, no less.

### Out of scope (now and possibly forever)
- **Beating ClickHouse and Postgres simultaneously.** (See §3.)
- **Data Vault inside the engine.** Hubs, links, and satellites are a *logical modeling pattern*, not a storage feature. They live in a separate product (**Solvia**, a lab-RCM SaaS), never in Stele's storage layer. Stele provides the primitives that make Data Vault and audit *cheap and fast*; it does not implement them. See §7 and [ADR-0009](adr/0009-data-vault-conceptual-seam.md).
- **Any coupling to Solvia or to revenue-cycle-management (RCM) specifics.** Stele is designed *aware* of the eventual fit but *coupled* to nothing.
- **Premature production customer data.** Non-negotiable. See §8.

## 6. Guiding principles

1. **Correctness and auditability over speed — always.** A benchmark win bought with a correctness compromise is a loss. Every temporal/as-of behavior has a written oracle ([06](06-testing-strategy.md)).
2. **Append-only is a discipline, not a feature flag.** The storage engine is immutable at its core; mutation is modeled as new truth, not erased truth.
3. **Pick one workload identity and defend it.** Analytical + temporal first; transactional adequacy second; both-at-once never.
4. **Earn trust before taking data.** Open-source usage and a deep test apparatus come *before* any production deployment, and long before Solvia.
5. **Every significant decision is an ADR.** If it's architecturally load-bearing and reversible only at cost, it gets a Context/Decision/Status/Consequences record in [`/docs/adr`](adr/README.md).
6. **Reproducibility is a feature.** Pinned toolchains, deterministic builds where feasible, deterministic simulation testing at the core ([04](04-cicd.md), [06](06-testing-strategy.md)).
7. **Inherit ecosystems; don't reinvent them.** Postgres wire compatibility buys the entire driver/ORM/BI/admin tooling world. Apache Arrow-shaped in-memory representation buys interoperability. We spend novelty budget only on the differentiators.
8. **Slow churn is a strategy, not an apology.** No deadline means we get to do it right. The cost of that freedom is ruthless prioritization, documented here.

## 7. The Solvia seam (designed-for, decoupled)

Stele is intended to *eventually* become the storage engine beneath **Solvia** once it has earned trust through real open-source usage. RCM (revenue-cycle management) is a near-perfect fit for a bitemporal/audit engine: claims, adjustments, and corrections are inherently a "what did we believe, and when" problem.

But until that day, **the two are fully decoupled.** The discipline:

- Stele exposes *general* primitives (bitemporality, lineage, MERGE). It never contains a `claim`, a `hub`, a `satellite`, or any RCM/Data-Vault concept.
- Solvia (separately) implements Data Vault and RCM logic *on top of* those primitives.
- We keep a **clean conceptual seam** *and* lay reasonable **general-primitive groundwork** so the eventual integration is seamless: stable/portable hash functions, hash-based distribution and co-location, hash-keyed bitemporal MERGE, and inline lineage. These are generic sharded-analytics/audit primitives that *happen* to make Data Vault map on cleanly — the engine still contains **no** hub, link, satellite, or claim. The seam (what we keep **out**) is [ADR-0009](adr/0009-data-vault-conceptual-seam.md); the groundwork (what we **build**) is [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md).

## 8. The trust gate (no production data, stated plainly)

Stele **must not** hold production customer data until it has genuinely earned it. "Earned it" is defined, not vibed:

- A deterministic simulation testing harness ([06](06-testing-strategy.md)) running continuously, including crash/recovery and fault injection, with seed-replayable failures.
- Bitemporal and as-of correctness oracles passing across a large generated workload corpus.
- Backup/restore and point-in-time recovery proven by test, not by hope.
- A real (even if small) open-source user base exercising the engine on non-critical data.
- For the distributed phase: Jepsen-style consistency testing in place before any multi-node production claim.

Until those gates are met, Stele runs synthetic data, contributors' throwaway data, and benchmarks — nothing a human would miss.

## 9. Success criteria

Stele is succeeding if, in order:

1. **It is correct first.** As-of and bitemporal queries return provably right answers under fault injection; recovery is exact. (Measured by the oracle/sim suites, not by feel.)
2. **It is coherent to a newcomer.** A contributor can clone, build, test, and run the engine *in minutes* ([05](05-dev-environment.md)) and understand the design from `/docs` *with zero additional context* — the definition of done for this very document set.
3. **It is adopted, even modestly.** Real people run it because the time-travel/audit story solves a problem they actually have — not because it benchmarks well.
4. **It is world-class where it claims to be.** Competitive analytical + temporal performance (correctness-gated benchmarks, [06](06-testing-strategy.md)), and *adequate* transactional point behavior — measured against that asymmetric bar, never against the dual-graveyard bar.
5. **It earns the long game.** It accrues enough trust and track record to *eventually* host Solvia — on Stele's timeline, not a forced one.

## 10. What would make this fail (anti-charter)

So future-us can smell the failure modes early:

- Chasing the dual ClickHouse+Postgres benchmark "just to prove we can."
- Letting RCM/Data-Vault/Solvia concepts leak into the engine "to save time later."
- Taking real production data before the trust gate (§8) is met.
- Trading a correctness oracle for a throughput number.
- Building distribution before the single-node temporal core is rock-solid.
- Reinventing drivers, BI integrations, or in-memory formats we could have inherited.

If you catch the project doing any of these, this charter is the document you cite to stop it.

---

*Assumptions made in the founding session are logged in [assumptions.md](assumptions.md). Decisions are recorded as ADRs in [`/docs/adr`](adr/README.md).*
