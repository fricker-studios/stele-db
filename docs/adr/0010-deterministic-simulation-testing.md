# ADR-0010 — Deterministic Simulation Testing as the core test method

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner + systems design
- **Related:** [06 — Testing Strategy §5](../06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece) · [02 — Architecture §11](../02-architecture.md#11-crate--module-decomposition-intended) · [Charter §6, §8](../00-charter.md#6-guiding-principles) · [ADR-0001](0001-implementation-language-rust.md) · [assumption A13](../assumptions.md)

## Context

The highest-value bugs in a database are emergent: rare concurrency interleavings, crash-timing windows, fault interactions (torn writes, lost fsyncs, partitions). Ordinary unit/integration tests almost never find them, and when they do, the failures are non-reproducible heisenbugs. The [Charter](../00-charter.md#8-the-trust-gate-no-production-data-stated-plainly) gates production data on *proven* correctness under faults — which requires a method that can explore these scenarios systematically and **reproduce any failure exactly**.

**Deterministic Simulation Testing (DST)** — pioneered by **FoundationDB**, refined by **TigerBeetle** — does exactly this: make all non-determinism (time, disk, network, RNG, scheduling) injectable, run the system inside a simulator driven by a single seed, and replay any failure from that seed. TigerBeetle and FoundationDB demonstrate the payoff (compressing millennia of runtime into hours of CPU; seed-replayable bugs). The catch: it only works if the system is **designed deterministic from the start** — retrofitting it later is enormously harder.

## Decision

**We will make Deterministic Simulation Testing the centerpiece of Stele's testing strategy, and build the storage/transaction core to be deterministic and runtime-agnostic from day one.** A dedicated **`stele-sim`** crate provides a virtual clock, deterministic RNG, simulated disk (latency, reordering, torn writes, fsync loss, corruption, full-disk), simulated network (delay, drop, reorder, partition), a deterministic scheduler, and **seed-based replay** ([06 §5](../06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece)). The core depends on **traits** for clock/disk/network with a real implementation for production and a simulated one for tests ([02 §11](../02-architecture.md#11-crate--module-decomposition-intended)); the core does not read the wall clock, spawn ungoverned threads, or touch `std::fs`/`tokio::net` directly. An invariant checker asserts durability, isolation, and no-lost-history at every simulated step. Failures print the seed; `just sim-seed <N>` replays them exactly.

## Consequences

### Positive
- Reproducible failures (a bug is a *number*) — no more heisenbugs; debugging becomes tractable.
- Systematic exploration of crash-timing and fault interleavings that conventional tests can't reach.
- Operationalizes the [trust gate](../06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized): "tested enough to hold data" becomes a concrete, CI-visible bar.
- Pairs with Rust's safety ([ADR-0001](0001-implementation-language-rust.md)) and the [correctness oracles](../06-testing-strategy.md#4-correctness-oracles-the-temporal-heart) for defense in depth.

### Negative / costs
- A real architectural constraint: the core must be runtime-agnostic and deterministic — no leaking `tokio`/`std` time/IO into it ([assumption A13](../assumptions.md)). This shapes module boundaries from v0.1.
- Building and maintaining `stele-sim` is significant engineering investment, started early even though payoff compounds later.
- Some third-party crates that assume their own runtime/IO can't be used in the deterministic core (only at the edges).

### Neutral / follow-ups
- DST covers the single binary; the distributed phase adds [Jepsen-style](../06-testing-strategy.md#7-jepsen-style-consistency-testing-distributed-phase) black-box testing ([ADR-0006](0006-distribution-later-shared-storage.md)).
- The simulator's fidelity (which fault models it includes) grows over time; new fault models are added as the engine gains surface area.
