# ADR-0027 — Vectorized execution model: batch-at-a-time Volcano pull over Arrow-shaped batches

- **Status:** Accepted
- **Date:** 2026-06-09
- **Deciders:** Project owner + systems design
- **Related:** [02 §6 (query layer)](../02-architecture.md#6-query-layer) · [02 §11 (crate decomposition)](../02-architecture.md#11-crate--module-decomposition-intended) · [assumption A7](../assumptions.md) · [ADR-0002](0002-on-disk-storage-format.md) · [ADR-0010](0010-deterministic-simulation-testing.md) · [06 — Testing Strategy](../06-testing-strategy.md) · STL-77 [C9] (STL-169)

## Context

v0.2 turns the query engine from a single `SnapshotScan` that returns one fully materialized result ([STL-100]) into a pipeline of composable physical operators — filter, project, aggregate, join (STL-77 [C10]–[C13]). Those operators are about to harden around an execution model, and the choice of model is load-bearing and hard to reverse: every operator's `next`/`execute` contract, its memory profile, and its determinism story all follow from it. We record the decision now, *before* the operators calcify, so reality and `/docs` do not drift — the operator framework itself already shipped under this model in [STL-169], and this ADR ratifies what it assumed.

The architecture already commits to the broad strokes ([02 §6](../02-architecture.md#6-query-layer), [§11](../02-architecture.md#11-crate--module-decomposition-intended): "vectorized operators", "Arrow batches"). What is *un*-pinned, and what this ADR pins:

1. **Iteration model** — row-at-a-time vs **batch-at-a-time** (vectorized); and **pull** (Volcano/iterator) vs **push** (data-driven / operator-emits-to-consumer).
2. **Batch representation** — bespoke vs **Arrow-shaped** columnar arrays ([assumption A7](../assumptions.md)).
3. **Batch sizing** — fixed default and whether it is configurable.
4. **Where `SnapshotScan` fits** — it predates the operator trait and must become a *source* operator without changing its result.

The constraints that bound the choice:

- **Determinism is non-negotiable** ([ADR-0010](0010-deterministic-simulation-testing.md), [02 §12 invariant 7](../02-architecture.md#12-cross-cutting-architectural-invariants)): the execution core in `stele-exec` runs under the simulation scheduler, so the model may not introduce a runtime, threads, or wall-clock reads of its own.
- **No-deadline posture** ([Charter §6](../00-charter.md#6-guiding-principles)): we optimize for a correct, legible core over peak throughput now. SIMD-friendliness and parallelism should be *reachable later* without re-architecting, but need not be built today.
- **Ecosystem interop** ([assumption A7](../assumptions.md)): an Arrow-shaped in-memory format inherits the Arrow ecosystem (compute kernels, IPC, BI tools) and avoids reinventing a vector format. Arrow is an *in-memory/execution* representation only — the on-disk format stays Stele's own ([ADR-0002](0002-on-disk-storage-format.md)).

Alternatives considered:

- **Push / data-centric (HyPer/DuckDB-style morsels, or codegen):** excellent for cache locality and parallel scaling, but the control-flow inversion and (for codegen) a compilation step add complexity that buys throughput we are explicitly deferring, and complicate stepping the pipeline deterministically under the sim scheduler. Rejected *for now* — see follow-ups.
- **Row-at-a-time Volcano (classic):** simplest, but per-row virtual-call overhead defeats the columnar/SIMD story and assumption A7. Rejected.
- **Bespoke (non-Arrow) batch type:** fewer dependencies, but forfeits the interop that A7 exists to capture. Held only as A7's documented fallback if Arrow's churn/weight becomes a problem.

## Decision

**Stele executes queries with a batch-at-a-time, pull-based (Volcano) operator pipeline over Arrow-shaped columnar batches.**

- **One operator trait, pull model.** Every physical operator implements a single trait whose one method pulls the next batch from upstream or reports end-of-stream (`Operator::next() -> Result<Option<Batch>, _>`). A plan is a chain of operators; pulling the top operator drives the whole pipeline one batch at a time. The trait is **object-safe** (`&mut self`, no generic methods) so a plan whose shape is only known at runtime can be erased to `Box<dyn Operator>`.
- **End-of-stream is `None`, never an empty batch.** Operators never emit a `rows == 0` batch; a consumer loops `while let Some(b) = op.next()? {}` without special-casing empties.
- **Batches are Arrow-shaped columnar arrays** ([assumption A7](../assumptions.md)): a `Batch` is a set of `(ColumnId, Column)` in projection order, every column holding exactly `rows` values, aligned row-wise. A `Column` is one typed, contiguous array (variable-length bytes with per-cell nullability; fixed-width `i64`). This is the executor's sole inter-operator currency.
- **Batch size is a fixed default, overridable per source.** The default chunk size is **1024 rows** (`DEFAULT_BATCH_SIZE`) — large enough to amortize the per-pull cost of the iterator model, small enough to bound peak per-batch memory. A source operator takes an explicit `batch_rows`; `0` is clamped to `1` so the pipeline always makes progress.
- **`SnapshotScan` becomes a *source* operator** via `into_source(batch_rows)`, which wraps it in a `ScanSource`. The scan keeps its existing result, late materialization, and validity-index pruning ([STL-146]); the source adapter is *eager at the source* — on its first pull it runs `SnapshotScan::execute` once, then hands out `batch_rows`-sized windows. The concatenation of the emitted batches is **byte-for-byte** the single batch `execute` returns today, which is exactly the result-equivalence the operators are verified against.
- **The model adds no runtime or wall-clock dependency** over `SnapshotScan`, so the whole pipeline runs under the deterministic simulation scheduler like the rest of the storage/txn core ([02 §12 invariant 7](../02-architecture.md#12-cross-cutting-architectural-invariants)).

## Consequences

### Positive
- **Composability:** filter / project / aggregate / join (STL-77 [C10]–[C13]) all build on the one `next`-returns-a-`Batch` contract; an operator is testable in isolation against a batch stream.
- **Bounded memory along the pipeline:** shaping operators (e.g. `Project`) stream one batch at a time, so a deep plan over a wide scan does not materialize the whole intermediate result per stage.
- **Interop & SIMD-readiness:** Arrow-shaped, columnar batches keep arrays contiguous and SIMD-friendly and let us reach for Arrow compute kernels / IPC later without reshaping data ([assumption A7](../assumptions.md)).
- **Legibility:** the pull model is the most widely understood execution model; a new contributor reads a plan top-down and the data flows bottom-up — no control-flow inversion to reason about.
- **Determinism preserved:** stepping the pipeline is a deterministic sequence of `next` calls, so it slots straight into the sim scheduler with no new oracle burden (this ADR adds no temporal/as-of semantics — it ratifies the *mechanism* that carries `SnapshotScan`'s already-oracle-verified results).

### Negative / costs
- **Throughput left on the table (deliberately):** pull + `Box<dyn>` dynamic dispatch per batch is slower than a push/codegen engine. We accept this per the no-deadline posture; batch-at-a-time keeps the per-row cost negligible, and the dispatch cost is per *batch*, not per row.
- **Eager source today:** `ScanSource` materializes the whole scan on first pull rather than streaming batches out of the segment reader. True streaming needs chunk-level row addressing the segment reader does not yet expose — a tracked v0.2 refinement (see follow-ups), invisible to the operator *interface*.
- **Not-yet-zero-copy slicing:** `Column::slice` currently deep-copies each window because a `Column` owns its cells rather than a shared buffer; an Arrow-style zero-copy slice awaits a shared-buffer `Column` ([STL-170] / PR #77 follow-up).
- **Committing to Arrow's shape** ties us to Arrow's churn and dependency weight; A7 documents the bespoke-batch fallback if that becomes a problem.

### Neutral / follow-ups
- **Streaming source** (batches produced without first materializing the full scan) and **shared-buffer / zero-copy `Column`** are tracked v0.2 refinements off [STL-170], orthogonal to the operator interface fixed here.
- **Push / morsel-parallel or codegen execution** is *not foreclosed*: this is the v0.2 single-thread model. Revisit when throughput, not correctness, is the binding constraint — a future ADR would supersede this one for the parallel/push model rather than amend it.
- Operator-level concurrency, exchange operators, and a cost model that sizes batches adaptively are out of scope here and gated on the optimizer maturing ([02 §6](../02-architecture.md#6-query-layer)).
- Revisit if Arrow's interop dividend fails to materialize, per [assumption A7](../assumptions.md)'s fallback.
