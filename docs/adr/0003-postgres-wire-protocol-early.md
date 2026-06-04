# ADR-0003 — Postgres wire protocol, early & incremental

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in founding session)
- **Related:** [02 — Architecture §7](../02-architecture.md#7-postgres-wire-protocol-front-end) · [01 §B.11](../01-feature-plan.md#b11--client-interface--ecosystem) · [assumptions A2, A9, O5](../assumptions.md)

## Context

Adoption cost is the silent killer of new databases: even an excellent engine fails if nobody can connect their tools to it. The Postgres wire protocol is a de-facto standard with an enormous ecosystem — drivers (JDBC, psycopg, pgx), ORMs (SQLAlchemy, Prisma, ActiveRecord), BI tools (Grafana, Metabase), and admin tools (DBeaver, psql) all speak it. Implementing it means Stele **inherits that entire ecosystem for free**.

The question confirmed in the founding session was *timing*: build the engine first and add pg-wire late, gate it to 1.0, or do it early and incrementally. Early validation against real clients also pressure-tests the SQL surface and catalog far better than a bespoke protocol would.

## Decision

**We will implement the Postgres wire protocol early and incrementally**, starting in **v0.1** with the *simple query* protocol (so `psql` connects and runs `CREATE`/`INSERT`/`SELECT`), adding the *extended query* protocol (prepared statements / parameter binding) in **v0.2** (drivers/ORMs require it), and the `COPY` protocol in **v0.3** ([03](../03-roadmap.md)).

We implement the **protocol and introspection compatibility** (including `pg_catalog`/`information_schema` shims), **not** Postgres's planner/MVCC semantics wholesale. Stele is wire- and tooling-compatible, not a Postgres clone. Where SQL:2011 temporal syntax and Postgres conventions conflict, the choice is documented ([assumption A9](../assumptions.md)). Externally, pg-wire is the **only** client protocol ([assumption O5](../assumptions.md)); embedded/in-process use is a separate, later capability.

## Consequences

### Positive
- Massive reduction in adoption friction — users connect with tools they already have.
- Early dogfooding: real clients exercise the SQL surface and catalog from v0.1, surfacing gaps fast.
- The `stele` CLI shell can itself be a pg-wire client, so the protocol is continuously exercised.

### Negative / costs
- The protocol is broad; full driver/BI compatibility is a long tail (handled via the [compatibility matrix](../01-feature-plan.md#b11--client-interface--ecosystem) through v0.5–v0.7).
- Risk of users **expecting full Postgres semantics** we don't provide; mitigated by clear docs on the compatibility boundary.
- Maintaining `pg_catalog` shims as tools probe new corners is ongoing work.

### Neutral / follow-ups
- Driver/ORM/BI compatibility is validated milestone-by-milestone ([03](../03-roadmap.md)).
- Temporal SQL extensions ride on standard pg syntax where they don't conflict; conflicts are documented decisions.
