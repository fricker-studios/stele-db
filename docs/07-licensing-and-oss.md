# 07 — Licensing & Open-Source Strategy

> **Status:** Decided posture (confirmed in the founding session). Legal review still recommended before any public release.
> **Read with:** [00 — Charter](00-charter.md) (the long game) · [ADR-0004](adr/0004-licensing-bsl.md) (the decision record).
> **Disclaimer:** This document is planning, not legal advice. Before publishing under any license, have a lawyer review the exact terms — especially the BSL parameters and the trademark posture.

## The decision, in one paragraph

Stele is released under the **Business Source License 1.1 (BSL 1.1)**, source-available and self-hostable, with an **Additional Use Grant** permitting production use *except* offering Stele as a competing managed/hosted database service, and a **Change Date of four years** after each version's release, at which point that version converts to **Apache License 2.0**. Cloud/enterprise features are monetized separately and later. This gives users full source visibility and the right to self-host and modify, protects against a hyperscaler reselling Stele as-a-service before the project can build its own commercial offering, and guarantees that every release eventually becomes true open source.

---

## Why BSL (and why not the alternatives)

The [Charter](00-charter.md#7-the-solvia-seam-designed-for-decoupled) needs a license that does three things at once: **(1)** earns trust through genuine source-availability and self-hostability, **(2)** protects the eventual commercial path (a managed service, and one day hosting Solvia), and **(3)** doesn't lock anything away forever. BSL is the cleanest fit.

| Option | Source visible? | Self-host? | Protects against managed-service resale? | Eventually open? | Verdict |
|---|---|---|---|---|---|
| **BSL 1.1 → Apache-2.0** | ✅ fully | ✅ | ✅ (via Additional Use Grant) | ✅ (at Change Date) | **Chosen.** Balances trust, protection, and an open future. |
| **Apache-2.0 (+ closed cloud / open-core)** | ✅ | ✅ | ❌ (anyone can host it) | already open | Maximum adoption, **no resale protection** — a hyperscaler could out-host us. |
| **AGPL-3.0** | ✅ | ✅ | ⚠️ partial (copyleft deters, doesn't forbid) | already open | Strong copyleft, but scares some enterprise users and OEMs; weaker than BSL for the specific managed-service threat. |
| **MIT / permissive** | ✅ | ✅ | ❌ | already open | No protection at all; wrong tool for the commercial long game. |
| **Fully proprietary** | ❌ | ❌ | ✅ | ❌ | Fails the trust requirement entirely — antithetical to the Charter. |

BSL is the license used by databases facing exactly this situation (MariaDB authored it; CockroachDB, Couchbase, and others adopted it). It is **source-available**, not OSI "open source," and we will say so plainly to avoid confusion.

## How BSL 1.1 works (the mechanics)

BSL 1.1 grants the right to **copy, modify, create derivative works, redistribute, and make non-production use** of the code, plus whatever **Additional Use Grant** the licensor specifies for production use. Two parameters define each release:

- **Change Date** — when the license converts. The licensor chooses any date **up to four years** after that version's first distribution. Stele uses the full **four years**.
- **Change License** — what it converts *to*. BSL requires a license **compatible with GPL v2.0 or later**. **Apache-2.0 qualifies** (it is GPLv3-compatible — a "later" GPL version), and CockroachDB's BSL used Apache-2.0 as its change license as direct precedent ([assumption A15](assumptions.md)). If counsel disputes this, the fallback Change License is **MPL-2.0** or **GPLv2-or-later**.

On the Change Date (or the four-year anniversary, whichever comes first), **that specific version** becomes Apache-2.0 — permanently and irrevocably. Newer versions remain under BSL until *their* clocks run out. The result: a continuously rolling open-source corpus trailing the latest release by at most four years.

### Stele's exact BSL parameters

```
Licensor:             [legal entity — TBD before launch]
Licensed Work:        Stele (the version stated in each release)
Additional Use Grant: You may make production use of the Licensed Work, provided
                      you do not offer a commercial product or service that, for a
                      fee, provides third parties a managed or hosted version of the
                      Licensed Work (a "Database Service"). Internal production use,
                      and use embedded in your own products that are not a Database
                      Service, are permitted.
Change Date:          Four years from the publication date of each released version.
Change License:       Apache License, Version 2.0
```

> The Additional Use Grant is the load-bearing clause: **self-host and use Stele in production freely; just don't resell it as a competing hosted database.** This is deliberately narrow so it protects the commercial path without chilling ordinary adoption. Final wording is subject to legal review ([open question O-legal in assumptions](assumptions.md)).

## Repository licensing hygiene

- **`LICENSE`** — the full BSL 1.1 text with Stele's parameters filled in.
- **`LICENSE-APACHE`** — the Apache-2.0 text referenced as the Change License.
- Per-file **SPDX headers**: `SPDX-License-Identifier: BUSL-1.1`.
- A **`licensing/` directory** documenting the Change Date schedule per release (so anyone can compute when a version goes Apache).
- **`THIRD-PARTY.md`** / SBOM: every dependency's license, gated by `cargo-deny` in CI ([04](04-cicd.md)) — we must not ship a dependency whose license is incompatible with redistributing under BSL. (Permissive deps — MIT/Apache/BSD — only; copyleft deps avoided in the engine.)

## Open-core boundary (BSL core vs commercial cloud)

| Tier | Examples | License |
|---|---|---|
| **Core engine** (everything in [01](01-feature-plan.md)) | storage, bitemporality, SQL, pg-wire, lineage, single-node + replicas, object-store tiering | **BSL 1.1 → Apache-2.0** |
| **Cloud / enterprise** (later, [03](03-roadmap.md#v20--distribution-era)) | managed control plane, autoscaling, fleet management, advanced enterprise security/compliance add-ons, SLAs/support | **Proprietary** |

The principle: **the engine is open and complete on its own.** Commercial value is in *operating* it at scale (the managed service), not in withholding core capability. A self-hoster gets a genuinely full database, not a crippled teaser.

---

## Contribution model

- **License inbound = license outbound** with a **CLA or DCO**. Given the future commercial relicensing/cloud plan, a lightweight **CLA** (granting the project the right to relicense contributions, e.g., into the commercial tier and the eventual Apache conversion) is the safer choice over a bare DCO. Decide and document before accepting the first external PR ([assumption A16](assumptions.md)).
- **`CONTRIBUTING.md`** specifies: the [five-minute dev path](05-dev-environment.md), the `just check` local gate, Conventional Commits, the ADR requirement for significant decisions, and the test bar ("no temporal feature without an oracle," [06](06-testing-strategy.md)).
- **`CODE_OF_CONDUCT.md`**: Contributor Covenant.
- **Issue/PR templates**, `good-first-issue`/`help-wanted` labels, and an ADR-first culture for anything architectural.
- **Security policy** (`SECURITY.md`): private disclosure channel, response expectations; advisories via GitHub Security Advisories.

## Governance

Start small and honest:

- **Maintainer-led (BDFL-style) governance** initially — a single steward (the founder) plus contributors. No pretense of a foundation or committee that doesn't exist yet ([assumption A16](assumptions.md)).
- **Evolution path:** as contributor mass grows, move to a **small maintainer council** with documented decision rules (lazy consensus for routine changes; ADR + maintainer sign-off for architectural ones).
- **Decisions are transparent:** every significant call is an [ADR](adr/README.md) in the open repo. Disagreements are resolved in the ADR's discussion, not in private.
- **No foundation donation** is planned in the foreseeable term; revisit only if/when neutrality becomes important to large adopters.

## Documentation & site plan

- **In-repo Markdown is the source of truth** (this `/docs` set), so docs version with the code and never drift ([assumption A19](assumptions.md)).
- **Published site at `steledb.com`** via a static generator — **mdBook** (Rust-native, simple) or **Docusaurus** (richer, versioned docs + blog). Lean mdBook early; reconsider Docusaurus when a versioned doc-set and a blog/community section are warranted. The **per-release versioned docs site, marketing front end, and WASM playground** are detailed in [09 §6–7](09-ecosystem-and-products.md#6-docs--marketing-site); release-time docs versioning is in [08 §10](08-packaging-distribution-and-releases.md#10-docs-per-release).
- **Content tiers:** a landing page (the one-paragraph thesis + the four-statement identity demo), a "Concepts" section (bitemporality, as-of, lineage explained for newcomers), a "Guides" section (install, connect, model temporal data), API/SQL reference (generated where possible), and the architecture/ADR docs surfaced from this repo.
- **Docs build is CI-gated** (broken links, `cargo doc` warnings) so the site can't ship broken.

## Community strategy

A slow-churn project grows a slow, real community — quality over virality:

- **Lead with the differentiator, not the benchmark.** The pitch is "time-travel and audit are free," demonstrated in four SQL statements ([05](05-dev-environment.md)). That's a story people remember.
- **Honesty about status.** Public README states plainly: pre-1.0, **no production data yet**, here's the [trust gate](06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized). Under-promising builds the trust the project is *about*.
- **Be source-available, say "source-available."** Never call BSL "open source." The Apache conversion clock is part of the pitch: "everything becomes true open source within four years."
- **Channels:** GitHub Discussions first (low overhead, searchable); a chat (Discord/Zulip) once there's enough activity to sustain it. Write-ups of the interesting engineering (DST, the bitemporal model, the segment format) are the best top-of-funnel for a systems project.
- **Target the niche, not the world.** People with genuine temporal/audit pain (regulated data, financial/clinical history, anyone fighting SCD-2 tables) are the early adopters — not "everyone who has a database."

## Trademark notes

- **"Stele" and "steledb"** (and any logo) should be protected so the *name* stays trustworthy even as the *code* becomes open: BSL→Apache opens the **code**, not the **brand**. This mirrors how Rust, Postgres, etc. keep trademark control while the code is freely licensed ([open question O4](assumptions.md)).
- Reserve a **`TRADEMARK.md`** / brand-usage policy: forks may use the code (per license) but may not call themselves "Stele" or imply official status. Permit nominative/fair use ("compatible with Stele," "built on Stele").
- Register the wordmark in the relevant jurisdiction before a public launch; hold `steledb.com` (already reserved) and key social handles.
- The trademark is also what makes the BSL Additional Use Grant enforceable in practice: even after a version goes Apache-2.0, a competitor can host the code but cannot brand it "Stele."

---

## Open licensing and legal questions

These are tracked in the [assumptions log](assumptions.md):


- Exact legal entity that will be the **Licensor** (formation before launch).
- Final **CLA vs DCO** decision and text.
- Counsel sign-off on **Apache-2.0 as Change License** (the GPL-compatibility reading, [A15](assumptions.md)).
- Precise **Additional Use Grant** wording (the managed-service carve-out) — narrow enough to not chill adoption, broad enough to protect the commercial path.
- Trademark registration jurisdiction(s) and timing.

---

### Sources (BSL mechanics)

- [Business Source License 1.1 — MariaDB (canonical text)](https://mariadb.com/bsl11/)
- [Business Source License (BSL 1.1): Requirements, Provisions, History — FOSSA](https://fossa.com/blog/business-source-license-requirements-provisions-history/)
- [Business Source License 1.1 — HashiCorp](https://www.hashicorp.com/en/bsl)
- [BUSL-1.1 — SPDX license list](https://spdx.org/licenses/BUSL-1.1.html)
