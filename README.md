# Stele

[![ci](https://github.com/fricker-studios/stele-db/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fricker-studios/stele-db/actions/workflows/ci.yml)
[![nightly](https://github.com/fricker-studios/stele-db/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/fricker-studios/stele-db/actions/workflows/nightly.yml)
[![release](https://img.shields.io/github/v/release/fricker-studios/stele-db?include_prereleases&label=release)](https://github.com/fricker-studios/stele-db/releases)
<!-- shields.io's docker/v badge chokes on the cosign .sig tags in this repo (badges/shields#7583),
     so this queries the Docker Hub tags API directly for the newest v0* tag. -->
[![docker hub](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fhub.docker.com%2Fv2%2Frepositories%2Ffrickerstudios%2Fstele%2Ftags%3Fname%3Dv0%26ordering%3Dlast_updated%26page_size%3D1&query=%24.results%5B0%5D.name&label=docker%20hub&logo=docker&color=2496ED)](https://hub.docker.com/r/frickerstudios/stele)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-orange?logo=rust)](rust-toolchain.toml)
[![license](https://img.shields.io/badge/license-BSL%201.1-blue)](docs/07-licensing-and-oss.md)

**A from-scratch, append-only, bitemporal, audit-native analytical database engine.**

> A *stele* is an inscribed stone slab that preserves a record permanently — and, in botany, the central column of a plant. Both meanings are the design: **a permanent, append-only record built around a columnar core.**

Stele treats history as the primary key of reality. Every fact is stored with *when it was true in the world* (valid time) and *when the system learned it* (system time), and nothing is ever destructively overwritten. On that foundation, "what did this table look like last Tuesday, as we understood it at month-end close?" is a first-class query — not an archaeology project.

It competes on **correctness, time-travel, and auditability** for analytical and temporal/audit workloads.

## The thesis in four SQL statements

```sql
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;
--   → 100   (time-travel: the value *before* the update — history is never destroyed)
```

## Try it

```bash
docker run --rm -p 5454:5454 frickerstudios/stele:latest --dev   # or ghcr.io/fricker-studios/stele
psql -h localhost -p 5454 -d stele
```

Then paste the four statements above — give the engine a beat between the `UPDATE` and the time-travel `SELECT`, so there is a second of history to travel back to. Prebuilt binaries (Linux/macOS/Windows) are on the [releases page](https://github.com/fricker-studios/stele-db/releases), every artifact cosign-signed with SLSA provenance; to build from source, `just build` ([05 — Dev Environment](docs/05-dev-environment.md)).

## Status

> ⚠️ **Pre-1.0 · working engine · API stabilizing · holds no production data.**
> Stele is a deliberately long-horizon, no-deadline craft project. Correctness and auditability come before speed, and before any real data — see the [trust gate](docs/06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized). v0.1.0 and v0.2.0 have shipped: `stele-server` speaking the PostgreSQL wire protocol, the `stele` CLI/REPL, bitemporal `AS OF` on both time axes, multi-statement transactions with snapshot isolation, joins and aggregates, and signed multi-arch Docker images — see the [CHANGELOG](CHANGELOG.md) and [releases](https://github.com/fricker-studios/stele-db/releases).

## At a glance

| | |
|---|---|
| **Language** | Rust (edition 2024) |
| **Wire protocol** | PostgreSQL-compatible (default port **5454**) — bring your existing drivers/ORMs/BI tools |
| **Storage** | Append-only columnar segments · object-storage tiering · system-time-driven archival |
| **Differentiators** | Bitemporality · as-of/time-travel · lineage & provenance · tamper-evident audit · hash-keyed MERGE |
| **Security** | A [first-class pillar](docs/10-security-and-compliance.md): encryption + KMS/BYOK, RBAC/RLS/CLS, crypto-shredding erasure |
| **License** | [BSL 1.1 → Apache-2.0](docs/07-licensing-and-oss.md) (rolling 4-year), source-available |

## Documentation

The complete vision, architecture, and plan live in [`/docs`](docs/README.md) — **start with the [Charter](docs/00-charter.md).**

- [00 — Charter](docs/00-charter.md) — vision, scope, non-goals, principles
- [01 — Feature Plan](docs/01-feature-plan.md) · [02 — Architecture](docs/02-architecture.md) · [03 — Roadmap](docs/03-roadmap.md)
- [04 — CI/CD](docs/04-cicd.md) · [05 — Dev Environment](docs/05-dev-environment.md) · [06 — Testing Strategy](docs/06-testing-strategy.md)
- [07 — Licensing & OSS](docs/07-licensing-and-oss.md) · [08 — Packaging & Distribution](docs/08-packaging-distribution-and-releases.md) · [09 — Ecosystem & Products](docs/09-ecosystem-and-products.md)
- [10 — Security & Compliance](docs/10-security-and-compliance.md) · [11 — Operations & Runbooks](docs/11-operations-and-runbooks.md) · [12 — Data Migration & Interop](docs/12-data-migration-and-interop.md)
- [13 — Glossary](docs/13-glossary.md) · [14 — Performance & Benchmarking](docs/14-performance-and-benchmarking.md) · [15 — Commercialization & Sustainability](docs/15-commercialization-and-sustainability.md)
- [Architecture Decision Records](docs/adr/README.md) · [Assumptions log](docs/assumptions.md)

## License

Business Source License 1.1, converting to Apache License 2.0 four years after each release. Source-available and self-hostable; see [07 — Licensing & OSS](docs/07-licensing-and-oss.md). Stele is **source-available**, not OSI open-source — we say so plainly.
