# ADR-0015 — Telemetry: off by default, explicit opt-in

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up session)
- **Related:** [09 §9](../09-ecosystem-and-products.md#9-telemetry--privacy) · [Charter §6](../00-charter.md#6-guiding-principles) · [07](../07-licensing-and-oss.md) · [assumption A25](../assumptions.md)

## Context

Usage telemetry helps prioritize development, but it's a trust-sensitive decision — especially for a **trust-first, audit-native** engine whose likely adopters handle regulated data ([Charter](../00-charter.md)). The spectrum: on-by-default/opt-out (most data, least trust), anonymous-aggregate opt-in, explicit opt-in/off-by-default, or none at all.

## Decision

**Telemetry is off by default and requires explicit opt-in**, across every shipped artifact — engine, CLI, and desktop app ([assumption A25](../assumptions.md)).

- **Default:** nothing leaves the user's machine. No phone-home, no beacons, no "anonymous ping."
- **If opted in:** strictly **anonymous aggregate** signal only — version, OS/arch, coarse feature-usage counters, anonymized crash reports. **Never** query text, schema, table/column names, data values, identifiers, IPs, or connection details.
- **Transparent & in the open:** exactly what would be collected is documented and printed at opt-in time; the collecting code is source-available (BSL).
- **Revocable:** a single flag/setting disables it at any time.

This is treated as a **feature**, not a limitation — it's part of why a regulated/security-conscious shop can trust Stele.

## Consequences

### Positive
- Strong alignment with the project's trust-first ethos; removes a common adoption blocker for regulated buyers.
- No risk of leaking sensitive data through telemetry — the engine's whole premise is taking data seriously.
- Being source-available makes the privacy claims verifiable, not just asserted.

### Negative / costs
- **Much less usage data** to guide prioritization (most users won't opt in). We lean harder on issues, discussions, and direct user contact for feedback.
- Opt-in flows must be built carefully so they're discoverable without being naggy.

### Neutral / follow-ups
- The precise opt-in schema (exact counters) is defined when telemetry is implemented; it must be reviewable and minimal.
- If, far later, a managed/cloud offering needs operational metrics, that is a *different* context (the operator runs the user's own infrastructure) and gets its own decision — it does not change this default for the self-hosted artifacts.
