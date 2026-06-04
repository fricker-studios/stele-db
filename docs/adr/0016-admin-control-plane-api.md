# ADR-0016 — Admin/control-plane API + client SDK strategy

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [09 §2–3](../09-ecosystem-and-products.md#2-the-admin--control-plane-api-the-shared-substrate) · [ADR-0003](0003-postgres-wire-protocol-early.md) (pg-wire) · [ADR-0012](0012-desktop-app-tauri.md) (desktop) · [ADR-0013](0013-kubernetes-openshift-operator.md) (operator) · [assumption A26](../assumptions.md)

## Context

pg-wire ([ADR-0003](0003-postgres-wire-protocol-early.md)) gives Stele the entire SQL client ecosystem for free. But several first-party products — the [desktop app](0012-desktop-app-tauri.md), the [operator](0013-kubernetes-openshift-operator.md), the CLI, and automation SDKs — need **operations that aren't SQL**: health/status, backup/restore/PITR, snapshotting, segment/zone-map introspection, lineage queries, user/role management, metrics, and (later) cluster lifecycle. Two questions: do we overload pg-wire with this, or build a dedicated surface? And do we ship our own SQL client drivers or rely on Postgres's?

## Decision

**We will expose a dedicated admin/control-plane API, and rely on existing Postgres drivers for SQL** ([assumption A26](../assumptions.md)).

- **SQL access:** no first-party driver. Users use existing PG drivers (psycopg, pgx, JDBC, …) via pg-wire; we maintain a **compatibility matrix**, not a driver ([01 §B.11](../01-feature-plan.md#b11--client-interface--ecosystem)).
- **Admin/control-plane API:** a dedicated surface, **gRPC** (typed; for the operator and programmatic clients) with an **HTTP/JSON gateway** (for the desktop app, scripts, curl). It covers lifecycle, data ops (backup/restore/PITR/snapshot), introspection (catalog, segments, zone maps, lineage), security (users/roles/grants), and observability (metrics, slow queries, EXPLAIN).
- **Versioned** `v1alpha1`→`v1beta1`→`v1`, Kubernetes-style, with deprecation windows ([ADR-0014](0014-release-channels-and-versioning-policy.md)). **Auth/RBAC and TLS** are shared with the SQL surface.
- **`stele-client` (Rust)** wraps this API + temporal niceties and is the shared substrate for the CLI, Studio, and operator; **thin language SDKs** (Python/TS/Go) wrap the HTTP/gRPC surface as demand appears (v1.0+).
- **Phasing:** the admin API grows alongside the features it exposes — minimal at v0.3 (health, backup), broadening through v0.7 (lifecycle for the operator) and v2.0+ (cluster ops).

## Consequences

### Positive
- Keeps pg-wire focused on SQL and the engine's surface clean; operations live behind one coherent, versioned contract.
- A single seam that the CLI, desktop app, operator, and SDKs all share — build once, reuse everywhere.
- Not shipping SQL drivers saves enormous maintenance; the PG ecosystem does that work.

### Negative / costs
- A second protocol surface (gRPC/HTTP) to design, secure, version, and document.
- The HTTP gateway + gRPC duality adds some build/maintenance overhead (mitigated by codegen from one protobuf definition).

### Neutral / follow-ups
- The full admin API schema (protobuf/OpenAPI) is its own design artifact, defined as the exposed features land.
- Reference docs for the API are generated from the protobuf/OpenAPI so they can't drift ([09 §6](../09-ecosystem-and-products.md#6-docs--marketing-site)).
