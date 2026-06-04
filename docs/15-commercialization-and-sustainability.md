# 15 — Commercialization & Sustainability

> **Status:** Directional and deliberately light. This is a **no-deadline craft / open-source project first**; commercialization is a *long-game possibility*, not a near-term plan. Recorded so the intent is honest and on the record — not because revenue is imminent.
> **Read with:** [07 — Licensing & OSS](07-licensing-and-oss.md) (the mechanics) · [Charter §7–8](00-charter.md#7-the-solvia-seam-designed-for-decoupled) (the long game) · [09 — Ecosystem](09-ecosystem-and-products.md) (the products).

## 1. The posture, stated plainly

Stele exists to be **correct and trusted** before it exists to make money. There is no funding clock and no growth target. Sustainability here means **low burn, high quality, and trust earned in the open** — not revenue ramp. Any commercial layer comes *after* the engine has earned its place, and never at the expense of the self-host story.

## 2. The model: open-core ([recap](07-licensing-and-oss.md#open-core-boundary-bsl-core-vs-commercial-cloud))

- **Core engine — BSL 1.1, free, source-available, self-hostable**, converting to Apache-2.0 on a rolling 4-year clock ([ADR-0004](adr/0004-licensing-bsl.md)). The engine is *complete on its own* — not a crippled teaser.
- **Commercial value is in *operating* it at scale**, not in withholding capability.

## 3. What is always free

The entire self-host experience, forever:

- The **engine**, the **`stele` CLI**, the **[Stele Studio](09-ecosystem-and-products.md#4-desktop-analytics-app-stele-studio) desktop app** (BSL, free), the **[Helm chart + operator](09-ecosystem-and-products.md#5-kubernetes--openshift-operator)**, client SDKs, and all documentation.
- A self-hoster gets a genuinely full, audit-native database — bitemporality, security pillar, tiering, the lot.

## 4. Possible revenue avenues (directional, uncommitted)

If and when commercialization makes sense, the candidates — all **additive**, none subtractive from the open core:

| Avenue | What it is |
|---|---|
| **Managed cloud service** | Run-it-for-you: the [admin/control-plane API](adr/0016-admin-control-plane-api.md) + operator productized into a hosted offering. The BSL anti-managed-service grant protects this path. |
| **Enterprise support / SLA** | Paid support, response guarantees, long-support lines. |
| **Enterprise features** | Advanced compliance/security add-ons, fleet management, [cloud marketplace images](09-ecosystem-and-products.md#8-cloud-marketplace-images) — the proprietary tier ([07](07-licensing-and-oss.md#open-core-boundary-bsl-core-vs-commercial-cloud)). |

These are **possibilities documented for coherence**, not a roadmap commitment. The [roadmap](03-roadmap.md) gates an "early cloud offering" to ~5 years, and only after the [trust gate](06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized).

## 5. The Solvia long-game

The eventual high-value outcome ([Charter §7](00-charter.md#7-the-solvia-seam-designed-for-decoupled)): once Stele has earned trust through real open-source usage, it can become the **storage engine under Solvia** (a lab-RCM SaaS), and a managed product in its own right. Until then the two are **fully decoupled** ([ADR-0009](adr/0009-data-vault-conceptual-seam.md)). Trust first, then the high-stakes workload — never the reverse.

## 6. Sustainability of a craft project

- **Low burn by design** — a slow-churn project doesn't need to raise or ramp; it needs to stay correct and keep shipping quality.
- **Community contribution** ([07](07-licensing-and-oss.md#contribution-model)) spreads the load as adoption grows; governance evolves from steward-led toward a maintainer council.
- **The brand/trademark is the durable commercial asset** ([07 §trademark](07-licensing-and-oss.md#trademark-notes)): BSL→Apache opens the *code*, not the *name* — so even after conversion, the commercial path (and quality signal) is protected.

## 7. Commitments (anti-rug-pull)

So adopters can trust the project's intentions:

1. **Every release converts to Apache-2.0** within four years — irrevocably ([ADR-0004](adr/0004-licensing-bsl.md)). The open future is contractual, not promised.
2. **We call BSL "source-available," never "open source."** No mislabeling ([07 §community](07-licensing-and-oss.md#community-strategy)).
3. **The open core stays complete** — commercial features are *additive* (operate-at-scale), never capability removed from self-host.
4. **No bait-and-switch.** Honest status (pre-1.0, no production data yet), honest benchmarks ([14](14-performance-and-benchmarking.md)), honest licensing.

---

*This document is intentionally short and will be revisited if and when commercialization becomes concrete. For now it exists so the business intent is transparent and consistent with the project's trust-first values — nothing here overrides [the Charter](00-charter.md).*
