# ADR-0014 — Release channels & cross-artifact versioning/compatibility policy

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + release engineering
- **Related:** [08](../08-packaging-distribution-and-releases.md) · [04 — CI/CD](../04-cicd.md) · [ADR-0002](0002-on-disk-storage-format.md) (on-disk format) · [ADR-0005](0005-reproducible-builds-pinned-toolchain.md) · [assumption A24](../assumptions.md)

## Context

Stele ships *many* artifacts — engine, CLI, Docker images, client SDK, Helm chart, operator/CRDs, desktop app, docs — and they evolve at different rates against a shared, slow-churn, no-deadline cadence ([03](../03-roadmap.md)). Without an explicit policy, version drift and compatibility surprises become inevitable (the on-disk format in particular is the least-reversible surface in the project, [ADR-0002](0002-on-disk-storage-format.md)). We need one coherent policy for channels, versioning, and compatibility across the whole catalog.

## Decision

**We adopt coordinated-but-independent SemVer with explicit per-surface compatibility contracts, and three release channels.**

**Channels** ([08 §6](../08-packaging-distribution-and-releases.md#6-release-channels--cadence)): **edge/nightly** (every `main` merge, no promise), **beta/RC** (tagged candidate, full deep CI gate + soak), **stable** (promoted from a soaked RC; what `latest`/version tags/OS repos point at). Releases are **tag-driven** and fully automated ([04 §release](../04-cicd.md#release-automation)). No calendar cadence.

**Versioning & compatibility contracts** ([08 §7](../08-packaging-distribution-and-releases.md#7-versioning--compatibility-policy-the-important-part)):

- **Engine/server** — SemVer; pre-1.0 minors may break; from 1.0 no breaking change without a major.
- **On-disk format** — integer `format vN`; **forward-compatible from v1.0** (a newer engine always reads older data; migrations explicit + tested). The most conservative surface.
- **Wire protocol** — documented pg-wire subset per release; never silently drops a supported message.
- **Client SDK** — tracks engine minor; works against its own minor and one back.
- **Admin API & operator CRDs** — Kubernetes-style `vNalphaM`→`vN` graduation with deprecation windows and conversion webhooks ([ADR-0016](0016-admin-control-plane-api.md), [ADR-0013](0013-kubernetes-openshift-operator.md)).
- **Helm chart / desktop app / docs** — SemVer tracking the engine minor; docs versioned per minor with a switcher.

**Cross-cutting rules:** MSRV is a documented, deliberately-bumped floor ([ADR-0005](0005-reproducible-builds-pinned-toolchain.md)); from 1.0 a **one-minor deprecation window** precedes any removal; **no LTS** promised pre-1.0; a **compatibility matrix** (engine ↔ SDK ↔ operator ↔ app ↔ format) ships with each release.

## Consequences

### Positive
- One predictable mental model for users and maintainers; compatibility is a contract, not a surprise.
- The on-disk format's conservative contract protects real data forever (post-1.0) while letting everything else move faster.
- Channels give contributors a fast lane (edge) and adopters a safe lane (stable) without conflating them.

### Negative / costs
- Maintaining independent version lines + a compatibility matrix is ongoing release-engineering work.
- Conversion webhooks and deprecation windows constrain how fast we can remove things post-1.0 (by design).

### Neutral / follow-ups
- Exact MSRV floor/cadence is [open question O1](../assumptions.md).
- Whether to offer LTS lines is revisited at 1.0 based on adopter needs.
- The compatibility-matrix format/tooling is decided when the second versioned artifact ships.
