# ADR-0004 — Licensing: BSL 1.1 → Apache-2.0 (4-year)

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in founding session)
- **Related:** [07 — Licensing & OSS](../07-licensing-and-oss.md) · [Charter §7](../00-charter.md#7-the-solvia-seam-designed-for-decoupled) · [assumptions A4, A15](../assumptions.md)

## Context

Stele must simultaneously **earn trust** (genuinely source-available and self-hostable), **protect a future commercial path** (a managed service, and eventually hosting [Solvia](0009-data-vault-conceptual-seam.md)), and **not lock anything away forever**. A permissive license (MIT/Apache) maximizes adoption but lets a hyperscaler out-host the project; pure proprietary fails the trust requirement; AGPL deters resale only partially and scares some adopters.

The Business Source License 1.1 is purpose-built for this: source-available with a parameterized **Additional Use Grant** for production use, a **Change Date** (≤ 4 years) at which each version converts to an open-source **Change License** that must be "compatible with GPL v2.0 or later."

## Decision

**We will license Stele's core engine under BSL 1.1**, with:
- **Additional Use Grant:** production use permitted *except* offering Stele as a competing managed/hosted "Database Service."
- **Change Date:** four years after each version's publication.
- **Change License:** **Apache License 2.0** (a valid GPL-compatible Change License; CockroachDB precedent — [assumption A15](../assumptions.md); fallback MPL-2.0 / GPLv2+ if counsel disputes).

Cloud/enterprise features are a separate **proprietary** tier, monetized later. The engine itself is complete and open under BSL ([07 — open-core boundary](../07-licensing-and-oss.md#open-core-boundary-bsl-core-vs-commercial-cloud)). Repo hygiene: `LICENSE` (BSL), `LICENSE-APACHE`, SPDX `BUSL-1.1` headers, dependency-license gating via `cargo-deny` ([04](../04-cicd.md)).

## Consequences

### Positive
- Full source visibility + self-host rights → the trust the project is *about*.
- Protection against managed-service resale before Stele can build its own offering.
- Guaranteed eventual open source: every release becomes Apache-2.0 within four years (a rolling open corpus).
- Precedent and tooling exist (MariaDB/CockroachDB/Couchbase) — a well-trodden path.

### Negative / costs
- BSL is **source-available, not OSI open-source** — some users/distros avoid non-OSI licenses; we must communicate this honestly and never mislabel it ([07 — community](../07-licensing-and-oss.md#community-strategy)).
- The Additional Use Grant must be worded narrowly enough not to chill ordinary adoption — needs legal review.
- A CLA (to enable relicensing into the commercial tier) adds contributor friction vs. a bare DCO.

### Neutral / follow-ups
- The Licensor entity is **Alex Fricker Studios, LLC**. CLA-vs-DCO and exact Additional Use Grant wording remain tracked as [open licensing questions](../07-licensing-and-oss.md#open-licensing-and-legal-questions).
- Trademark protection ([07 — trademark](../07-licensing-and-oss.md#trademark-notes)) is what keeps the *brand* protected even after code converts to Apache.
