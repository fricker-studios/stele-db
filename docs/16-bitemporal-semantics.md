# 16 — Bitemporal Semantics (formal spec)

> **Status:** The normative spec. Every correctness test and oracle ([06](06-testing-strategy.md)) is checked against *this*. Written before the engine so behavior is defined, not discovered.
> **Read with:** [02 §2 record model](02-architecture.md#2-the-bitemporal-record-model) · [ADR-0023 record model](adr/0023-append-only-record-model-validity-index.md) · [ADR-0024 time](adr/0024-time-representation.md).

This document defines exactly what Stele returns for a bitemporal query, at the boundaries, so there is no ambiguity for implementers or auditors. Where a rule is a *choice*, it is stated as a choice with rationale.

## 1. Model

A **fact** about a **key** is an immutable, appended **assertion**:

```
assertion = (key, sys_from, valid_from, valid_to, payload, txn, seq, principal)
retraction = (key, sys_from, valid_from, valid_to, txn, seq, principal)   # a logical delete
```

- Records are never mutated ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)). A version's **system-time end** (`sys_to`) is *not stored*; it is the `sys_from` of the next assertion that supersedes it for the same `(key, valid-time region)`, materialized once into the derived **validity index**.
- All timestamps are **µs, int64, UTC**, with a reserved **`+∞`** sentinel ([ADR-0024](adr/0024-time-representation.md)).
- `seq` is the per-commit monotonic sequence number giving a total order to same-µs writes.

## 2. Intervals

- **Half-open** `[start, end)` on **both** axes. A point `t` is in `[a, b)` iff `a ≤ t < b`.
- **`end = +∞`** denotes "until changed / currently true."
- **Reversed** intervals (`end < start`) are **rejected** at write time.
- **Zero-length** intervals (`start == end`) are **rejected** by default (they cover no point); a future option may admit them as event markers — until then, rejected.

## 3. The visibility function (the definition of "as of")

For a key `k`, system-time `S`, and valid-time `V`:

```
v(k, S, V) = the payload of the unique assertion A for k such that
    A.sys_from ≤ S
    and A is not superseded as of S   (no later-or-equal-sys assertion for the same
                                       valid region exists with sys_from ≤ S)
    and V ∈ [A.valid_from, A.valid_to)
    and A is not retracted as of S
  → if no such A: the key is ABSENT at (S, V).
```

- **Uniqueness:** at any `(S, V)` there is **at most one** active version per key (the [2D-tiling invariant](#5-the-2d-tiling-invariant)). If two assertions tie on `sys_from`, `seq` breaks the tie (higher `seq` wins / supersedes).
- **`S = now`** means the snapshot system-time taken **at query start** (not re-evaluated per row) — see [§6](#6-snapshots).
- **`V = now`** is resolved the same way against the valid axis.

## 4. The four as-of classes

| Query | Meaning |
|---|---|
| `v(k, now, now)` | current state |
| `v(k, S_past, now)` | what we believed *as of `S_past`* about the present |
| `v(k, now, V_past)` | our *current* understanding of the past at `V_past` |
| `v(k, S_past, V_past)` | full bitemporal point reconstruction |

Point lookup, range scan, **aggregation, and join must all apply the *same* `v`** — the scan path and the aggregate/join path may not resolve as-of differently (a top source of silent bugs; tested in [06](06-testing-strategy.md)).

## 5. The 2D-tiling invariant

Each key occupies a set of **rectangles** in (system-time × valid-time) space. Invariant:

> For any key, at any point `(S, V)`, **at most one** version is active; within the asserted coverage the rectangles **tile with no unintended gaps or overlaps**.

A correction must **clip** the prior rectangle (close it on the system axis, and on the valid axis open the corrected region) — failure to clip leaves overlaps (double-counting) or gaps (data vanishes at some as-of points). Property-based and differential tests assert this over millions of random `(S, V)` probes ([06](06-testing-strategy.md)).

## 6. Snapshots

A query fixes one `(S, V)` pair **at query start** and uses it for every row — a 10-minute scan sees a single coherent system-time slice even as ingestion continues. "As of now" is *now-at-query-start*, never now-at-each-row.

## 7. Monotonicity (the bedrock audit property)

> Adding a later-system-time fact **never** changes any `v(k, S, V)` result for `S` earlier than that fact's `sys_from`.

The past is immutable. This is asserted everywhere; a violation is a critical bug.

## 8. Temporal joins

Joining two bitemporal relations **intersects both axes**: the result tuple's validity is the **intersection** of the inputs' system- and valid-time regions; rows whose regions don't overlap on *both* axes do not join.

## 9. Coalescing (a documented choice)

Stele **does not auto-coalesce on write** ([Part E default](../README.md)): adjacent intervals with identical values are stored **as asserted**, preserving the exact provenance an auditor expects. Coalesced output is available on demand as a **view/option**. The required invariant: splitting one interval into two adjacent identical-value intervals (or coalescing them) **must not change any query result** — tested via metamorphic tests ([06](06-testing-strategy.md)).

## 10. Valid-time as a business date

Valid-time is often a **business date**, not an instant ("effective March 2024"). The spec defines: business dates resolve to half-open µs ranges in UTC at documented boundaries; DST gaps/overlaps, leap seconds, Feb 29, end-of-month, and fiscal calendars (e.g. 4-4-5) are handled at this resolution layer, not in the physical type ([ADR-0024](adr/0024-time-representation.md)). Cross-zone reporting stores UTC + originating zone.

**`TIMESTAMPTZ` is stored UTC-internal** ([STL-189], [ADR-0024](adr/0024-time-representation.md)). A `timestamptz` literal's zone offset is normalized away to the engine's single µs/UTC scale on input — `2024-01-15 12:00:00+05` and `2024-01-15 02:00:00-05` store the *same* instant — and the value renders back with a `+00` offset (the engine carries no session time zone to localize into). Two literals naming one instant in different zones are therefore indistinguishable once stored, on both axes; the half-open boundary tests in [§2](#2-intervals) hold identically whether the instant arrived as a bare `timestamp` or a zoned `timestamptz`. Leap seconds (`:60`) are not representable — the physical type is leap-second-free UTC microseconds. Preserving an originating zone alongside the instant is the separate cross-zone-reporting concern above, not part of the scalar type.

[STL-189]: https://allegromusic.atlassian.net/browse/STL-189

## 11. What the engine enforces vs. punts (stated honestly)

| Concern | Engine | Notes |
|---|---|---|
| Half-open boundaries, `+∞`, reversed/zero-length rejection | **Enforces** | §2 |
| At-most-one-active-version (2D tiling) per key | **Enforces** | §5; via the validity index + per-key serialization ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)) |
| Temporal primary key (no overlapping valid-time per key per system slice) | **Enforces** | [feature A.1](01-feature-plan.md#a1--bitemporality) (v0.5) |
| Temporal foreign keys (child validity within parent) | **Optional / later** | [feature A.1](01-feature-plan.md#a1--bitemporality) |
| Sequenced vs. non-sequenced constraints (Snodgrass) | **Documented per constraint** | which mode each supports is stated |
| **Cascading correction** of derived aggregates | **Punts** (app's job) | engine makes staleness **detectable** via the change-feed + derivation lineage ([12](12-data-migration-and-interop.md#6-change-feed-out)) — it does not recompute |

This honesty *is* the product: in a trust-led domain, a precisely-stated guarantee beats an over-claimed one.

## 12. Deletes, retractions & the deletion gap

A **delete is "close, don't reopen":** a retraction closes the current version's system-time period and opens nothing. The key is then **ABSENT** for all `S ≥ closed_at` until (if ever) a later assertion re-opens it. Nothing is physically removed; the deleted version remains queryable as-of the past (physical erasure is the separate [ADR-0020](adr/0020-crypto-shredding-erasure.md) path).

**Retractions are first-class durable records — never inferences.** Version adjacency ("a version's `sys_to` is the next version's `sys_from`") is only valid for *supersessions*, where close and successor share one atomic commit. A delete is a **close with no successor**; adjacency inference cannot represent it. The canonical failure:

```
INSERT@t0 (V1) → UPDATE@t1 (V2) → UPDATE@t2 (V3) → DELETE@t3 → re-INSERT@t4 (V4)
adjacency-only rebuild infers V3 = [t2, t4)   ✗  (resurrects the row across [t3, t4))
with the retraction record:  V3 = [t2, t3), gap [t3, t4), V4 = [t4, +∞)   ✓
```

**Where retractions durably live** (so a validity-index rebuild can never lose a deletion gap):

1. **WAL** — every retraction is a redo record; the fsync is the durability point.
2. **Segments** — at delta flush, retractions are persisted as **payload-less tombstone rows** (`key`, target `sys_from`, `closed_at`, `seq`, closing provenance); compaction preserves them like any version. The segment store is therefore **self-contained for a from-scratch rebuild** (versions + retractions fully determine the index), even after WAL truncation.
3. **Validity-index checkpoints** — routine recovery is *checkpoint + WAL tail*; the full replay always remains possible.

The retraction record is also where **delete provenance** lives ("who deleted this, when, by what statement" is a queryable fact), and the [hash-chained commit log](adr/0026-verifiable-audit-log.md) keeps the delete event tamper-evident.

**Required oracle test:** the delete-then-reinsert gap must survive a **full index rebuild** — as-of results across `[t3, t4)` are byte-identical before and after rebuilding the validity index from segments. An adjacency-inference implementation must fail this test.

