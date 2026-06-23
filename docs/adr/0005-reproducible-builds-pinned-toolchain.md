# ADR-0005 — Reproducible builds & pinned toolchain

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + build/infra
- **Related:** [04 — CI/CD](../04-cicd.md) · [05 — Dev Environment](../05-dev-environment.md) · [Charter §6](../00-charter.md#6-guiding-principles) · [assumption A14](../assumptions.md)

## Context

The [Charter](../00-charter.md#6-guiding-principles) makes reproducibility a first-class principle: "pinned toolchains; deterministic builds where feasible." For a correctness-critical, long-horizon project, *"works on my machine"* is unacceptable — a [deterministic simulation](0010-deterministic-simulation-testing.md) failure must reproduce identically for every contributor and in CI, and a published binary should be traceable to its exact source and toolchain.

## Decision

**We will pin the toolchain and dependencies and pursue reproducible builds:**
- **`rust-toolchain.toml`** pins the exact Rust compiler version (edition 2024) — the single source of truth honored by native builds, devcontainer, Nix, and CI alike. This pinned version is also Stele's **MSRV**, bumped only deliberately ([assumption A14](../assumptions.md)).
- **`Cargo.lock` committed**; CI builds `--locked`.
- **GitHub Actions pinned by commit SHA**; updates arrive as reviewable Dependabot/Renovate PRs.
- **Hermetic dev shells** (Nix flake / devbox / devcontainer) all resolve to the same pinned toolchain ([05](../05-dev-environment.md#hermetic-shells-pick-your-poison)).
- **Long-term goal:** bit-for-bit reproducible release artifacts where the toolchain allows, plus **SLSA provenance** and **cosign** signing so third parties can verify a binary matches the tagged source ([04 — release](../04-cicd.md#release-automation)).

## Consequences

### Positive
- DST and benchmark results reproduce identically everywhere — the foundation of seed-replayable debugging.
- Supply-chain integrity: signed, provenance-attested artifacts; license/advisory gating via `cargo-deny`/`cargo-audit`.
- New contributors get an identical environment in minutes ([05](../05-dev-environment.md#the-five-minute-path-the-headline-promise)).

### Negative / costs
- Toolchain bumps are manual events (a small, deliberate tax) rather than automatic — by design.
- Full bit-for-bit reproducibility can be fiddly (build-path/timestamp normalization); treated as a goal, not a v0.1 gate.

### Neutral / follow-ups
- MSRV floor and bump cadence — [open question O1](../assumptions.md) — are now settled: MSRV is pinned at **1.89.0** and bumped only deliberately; the 1.85→1.89 bump (STL-225) exercised the policy. O1 is resolved (STL-261).
- A `beta`/`nightly` early-warning CI job runs against newer compilers to catch breakage ahead of pinned bumps.
