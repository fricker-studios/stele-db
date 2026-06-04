# ADR-0012 — Desktop analytics app: Tauri, BSL, free community tool

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up session)
- **Related:** [09 §4](../09-ecosystem-and-products.md#4-desktop-analytics-app-stele-studio) · [ADR-0016](0016-admin-control-plane-api.md) (the API it uses) · [ADR-0003](0003-postgres-wire-protocol-early.md) (pg-wire) · [ADR-0004](0004-licensing-bsl.md) (licensing) · [assumption A22](../assumptions.md)

## Context

Stele will ship a standalone, pgAdmin-style **desktop application** ("Stele Studio") that is also the intended future home of the **analytics workflow**. Two decisions matter most: the **UI framework** and the **licensing/positioning**. The engine is Rust ([ADR-0001](0001-implementation-language-rust.md)) and exposes pg-wire ([ADR-0003](0003-postgres-wire-protocol-early.md)) plus an admin API ([ADR-0016](0016-admin-control-plane-api.md)); the app should reuse that, ship small signed binaries, and feel native.

Alternatives: **Electron** (mature ecosystem, but heavy Chromium/Node runtime and no code sharing with the Rust engine), **native per-OS** (best UX, ~3× the work, no shared codebase), **web-console-only** (simplest distribution, weaker offline/native experience).

## Decision

**We will build the desktop app with [Tauri](https://tauri.app/)** (Rust backend + native OS webview) and license it **BSL 1.1** — a **free, source-available community tool**, monetized indirectly via the cloud/operator/enterprise tiers, not by charging for the app ([ADR-0004](0004-licensing-bsl.md), [assumption A22](../assumptions.md)).

- The Tauri Rust core reuses **`stele-client`** and engine types ([09 §3](../09-ecosystem-and-products.md#3-client-sdks)); the UI is a webview talking to that core.
- SQL flows over **pg-wire**; operations over the **admin API** ([ADR-0016](0016-admin-control-plane-api.md)).
- It leads with what generic pg tools can't do: **temporal-native UI** (`AS OF` time-slider, system-vs-valid-time diffs, lineage explorer), then grows into the analytics workflow post-1.0.
- Cross-platform (mac/win/linux) with **OS code-signing + notarization**, **Tauri auto-update** over a signed feed, and **opt-in/off-by-default** telemetry/crash reporting ([ADR-0015](0015-telemetry-opt-in.md)).
- Phasing: admin & query tool at **v0.7 preview → v1.0**; analytics workflow **post-1.0** ([roadmap](../03-roadmap.md#artifact--product-roadmap)).

## Consequences

### Positive
- Code/type sharing with the engine (one language, `stele-client` reuse); tiny binaries; native feel; strong per-OS signing story.
- BSL/free keeps it consistent with the engine and removes adoption friction — the app becomes a showcase for the temporal features rather than a paywall.
- A polished temporal UI is a genuine differentiator no off-the-shelf Postgres tool offers.

### Negative / costs
- Tauri's ecosystem is younger/smaller than Electron's; some UI components may need building.
- Webview rendering differences across OSes require testing (mitigated by Tauri's consistent API).
- Building a real analytics workflow is a large, open-ended effort — deliberately deferred to post-1.0.

### Neutral / follow-ups
- The "Stele Studio" name is provisional, pending the [trademark check](../07-licensing-and-oss.md#trademark-notes).
- The app must stay **decoupled**: the engine never depends on it ([09 overview](../09-ecosystem-and-products.md)).
- Whether deeper analytics features ever become a paid tier is left open ([ADR-0004](0004-licensing-bsl.md) governs any such split).
