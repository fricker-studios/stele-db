# ADR-0001 — Implementation language: Rust

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in founding session)
- **Related:** [Charter](../00-charter.md) · [05 — Dev Environment](../05-dev-environment.md) · [06 — Testing Strategy](../06-testing-strategy.md) · [assumption A1](../assumptions.md)

## Context

Stele is a from-scratch storage engine whose entire value proposition is **correctness and auditability** ([Charter §6](../00-charter.md#6-guiding-principles)). It needs precise control over memory layout (columnar encodings, page formats), predictable performance (no GC pauses during scans or compaction), strong concurrency, and — critically for [Deterministic Simulation Testing](0010-deterministic-simulation-testing.md) — the ability to build a runtime-agnostic, deterministic core. This is a decade-long project, so language maturity and ecosystem longevity matter as much as raw capability.

Alternatives considered: **C++** (deepest DB ecosystem, but manual memory safety is a perpetual correctness tax — directly at odds with the thesis), **Zig** (excellent control and `comptime`, but pre-1.0 language churn and a young ecosystem are risky over a 10-year horizon), **Go** (fast iteration and great tooling, but GC pauses and weaker low-level control are a poor fit for a columnar/storage engine).

## Decision

**We will implement Stele in Rust** (edition 2024, MSRV pinned — see [ADR-0005](0005-reproducible-builds-pinned-toolchain.md)).

Rust gives memory and thread safety *without* a garbage collector, an ownership model that catches whole classes of storage-engine bugs at compile time, a mature async/concurrency story, zero-cost abstractions for hot paths, and an ecosystem (Arrow, tokio, sqlparser, object_store, etc.) that lets us inherit rather than reinvent. The strong type system and `unsafe`-isolation also pair naturally with the correctness-first culture and the sanitizer/Miri/DST regime in [06](../06-testing-strategy.md).

## Consequences

### Positive
- Memory/thread safety by construction — fewer of exactly the bugs that corrupt a database.
- No GC; predictable latency for scans, compaction, and recovery.
- Rich, relevant crate ecosystem (Arrow-shaped batches, S3 backends, pg-wire building blocks) reduces novelty spend to the actual differentiators.
- Trait-based design supports the runtime-agnostic deterministic core that DST requires.
- Excellent tooling (cargo, clippy, nextest, fuzz, Miri) underpins the CI regime in [04](../04-cicd.md).

### Negative / costs
- Steeper learning curve; the borrow checker can slow early iteration on complex data structures (e.g., intrusive version chains) — sometimes requiring `unsafe` islands that must be carefully tested (Miri, sanitizers).
- Compile times on a large workspace can be significant (mitigated by caching/`sccache`, [04](../04-cicd.md)).
- Some advanced determinism work (controlling all async scheduling) requires discipline to avoid leaking `tokio`/`std` time into the core ([06 §5](../06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece)).

### Neutral / follow-ups
- MSRV and edition policy are set in [ADR-0005](0005-reproducible-builds-pinned-toolchain.md).
- Revisit only on a fundamental ecosystem shift; this is among the least reversible decisions in the project.
