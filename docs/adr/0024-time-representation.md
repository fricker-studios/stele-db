# ADR-0024 — Time representation: microsecond, int64, +∞ sentinel, per-commit sequence

- **Status:** Accepted
- **Date:** 2026-06-05
- **Deciders:** Project owner + systems design
- **Related:** [02 §2](../02-architecture.md#2-the-bitemporal-record-model) · [16 — Bitemporal Semantics](../16-bitemporal-semantics.md) · [ADR-0023](0023-append-only-record-model-validity-index.md) · [ADR-0022](0022-clock-synchronization-and-ordering.md)

## Context

The engine is time-native, so the physical representation of timestamps is load-bearing and hard to change post-data. Two traps flagged in review: **int64 nanoseconds overflows on 2262-04-11** (a far-future medical/financial effective date — or a `+∞` "until changed" sentinel — won't fit), and **same-instant writes** need a deterministic total order or results are non-reproducible.

## Decision

- **Resolution: microseconds (µs).** Stored as **int64** µs since the Unix epoch, which reaches ~year **294247** — no 2262 cliff, ample for long-dated instruments and lifelong medical records, while keeping a single 8-byte column.
- **UTC-internal**; time zone / business-date interpretation is a presentation concern (a fact's valid-time may be a business date — handled in the [semantics spec](../16-bitemporal-semantics.md)).
- **Half-open intervals `[start, end)`** everywhere.
- **Explicit `+∞` sentinel** for open-ended ("until changed") intervals — a reserved max value distinct from any real timestamp, with defined comparison/arithmetic.
- **Per-commit monotonic sequence number** gives a total order to writes sharing the same µs tick (and same writer), so reproduction is deterministic. In the distributed phase this is the logical counter of the [Hybrid Logical Clock](0022-clock-synchronization-and-ordering.md).
- Reject **reversed** intervals (`end < start`); define **zero-length** (`start == end`) handling in the spec.

## Consequences

### Positive
- No overflow cliff; far-future and far-past dates representable.
- Same-tick determinism via the sequence number — required for byte-identical as-of reproducibility.
- Compact (int64) temporal columns; four per row, so the 8-byte choice matters for storage and compression ([ADR-0025](0025-valid-time-indexing.md)).

### Negative / costs
- µs (not ns) loses sub-microsecond resolution — acceptable for financial/clinical events; revisit only if a workload genuinely needs ns (would imply 128-bit time).
- The `+∞` sentinel must be handled consistently in encodings, zone maps, and arithmetic (tested at boundaries in [06](../06-testing-strategy.md)).

### Neutral / follow-ups
- DST / leap-second / fiscal-calendar handling for *valid-time business dates* is specified in [docs/16](../16-bitemporal-semantics.md), not baked into the physical type.
- If a 128-bit ns representation is ever required, it is a format-version migration, not a silent change.
