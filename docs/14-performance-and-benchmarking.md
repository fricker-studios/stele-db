# 14 — Performance & Benchmarking Methodology

> **Status:** Founding methodology. Defines *how we measure and report* — so that when there are numbers, they're honest.
> **Read with:** [00 — Charter §3](00-charter.md#3-the-guardrail--lead-with-the-non-goal) (the asymmetric bar) · [06 — Testing §8](06-testing-strategy.md#8-benchmark-suite-correctness-first-asymmetric) (the suites) · [04 — CI/CD](04-cicd.md#benchmark-regression-gate-the-do-not-regress-rule) (the gate).

Benchmarks for Stele exist to **prevent regressions and prove the asymmetric contract** — never to manufacture marketing wins. The [Charter](00-charter.md) puts correctness above speed; this document keeps performance work honest enough to deserve that ordering. The guiding rule: **a number we can't reproduce, and wouldn't publish the config for, is not a number we cite.**

## 1. The asymmetric performance contract

Restating the [Charter guardrail](00-charter.md#3-the-guardrail--lead-with-the-non-goal) as a measurable bar:

| Workload | Bar | How we talk about it |
|---|---|---|
| **Temporal / as-of / audit** | **World-class** | The identity — we lead with it. |
| **Analytical (scan/aggregate/join)** | **World-class / competitive** | Reported head-to-head, same hardware. |
| **Ingest / MERGE / historization** | **Competitive** | It's an audit engine; loading matters. |
| **Transactional point ops** | **Adequate floor** | Stated as "adequate," never as a win. |

We never frame Stele as beating ClickHouse *and* Postgres at once ([the graveyard](00-charter.md#3-the-guardrail--lead-with-the-non-goal)). Honesty about what we're *merely adequate* at is part of the brand.

## 2. What we benchmark

| Suite | Measures | Basis |
|---|---|---|
| **Temporal/as-of (bespoke)** | as-of point/range, bitemporal joins, MERGE historization, time-range pruning | Stele-authored, open |
| **Analytical** | scan, filter, aggregate, join throughput | ClickBench- / TPC-H-derived |
| **Transactional (floor)** | single-row upsert/lookup latency | TPC-C-like |
| **Ingest** | bulk load + MERGE rates | bespoke |
| **Recovery / compaction** | recovery time, compaction throughput, write amplification | bespoke |
| **Cost-performance** | $/query and $/TB across tiers ([§6](#6-cost-performance)) | bespoke |

Detail on suite intent is in [06 §8](06-testing-strategy.md#8-benchmark-suite-correctness-first-asymmetric).

## 3. Correctness gates every benchmark

Before any timing is recorded, a benchmark **asserts its result is correct** (against an [oracle](06-testing-strategy.md#4-correctness-oracles-the-temporal-heart) or known answer). **A fast wrong answer fails the benchmark.** We will never publish a throughput number bought with a correctness compromise — that would invert the entire thesis.

## 4. Methodology principles

1. **Reproducible.** Pinned hardware spec, pinned engine version ([ADR-0005](adr/0005-reproducible-builds-pinned-toolchain.md)), and the **full config published** alongside every result. Anyone can re-run it.
2. **Representative data — skewed, not uniform.** Real-shaped datasets with temporal depth: **Zipfian** key + version-depth skew (a few keys with thousands of revisions), the **entities×versions** axis (1M keys × 1000 versions ≠ 1B × 1), month-end bursts, and **steady-state runs long enough that compaction kicks in mid-benchmark**. Benchmark on the **substrate you deploy on** (cloud object storage ≠ local NVMe). Adapt **TPC-H/TPC-DS** with as-of predicates and use **TPC-BiH** bitemporal patterns.
3. **Distributions, measured right.** Report **p50 / p90 / p99 / p99.9 / max** via **HdrHistogram**, plus throughput and cross-run variance. Use **open-loop** load generation (wrk2 / Gatling open model / YCSB intended-rate) to avoid **coordinated omission**, which understates tail latency 10–100× under saturation. Report **visibility lag** (submit→queryable) as an SLA. A mean alone is marketing.
4. **Warm vs cold disclosed.** Cache state, tier placement (hot/cold/[frozen](adr/0021-storage-lifecycle-tiered-archival.md)), and whether a [restore](adr/0021-storage-lifecycle-tiered-archival.md) was involved are always stated.
5. **Apples-to-apples.** Comparisons run competitors on the **same hardware** with **reasonable, documented** tuning — never a strawman config for the other system.
6. **Open harness.** The benchmark code and configs are source-available ([BSL](07-licensing-and-oss.md)) so results are auditable, not asserted.
7. **No cherry-picking.** We publish the suite's results as a whole, including where we lose, not a curated highlight reel.

## 5. The do-not-regress gate

Performance is protected continuously, not measured once ([04](04-cicd.md#benchmark-regression-gate-the-do-not-regress-rule)):

- `criterion` benchmarks produce a baseline per `main` commit; a PR that regresses a tracked path beyond threshold (e.g. **>5%** wall-clock or **>3%** allocations) **fails** unless explicitly re-baselined with a written rationale.
- Baselines are versioned and traceable to the commit that moved them, so a regression always points at its cause.
- On a slow-churn project, the real risk is *quietly getting slower while working on something else* — this gate is the guard against it.

## 6. Cost-performance

For an object-store-tiered engine, **cost is a first-class performance axis**: $/query and $/TB-month matter as much as latency. We report:

- **Storage cost by tier mix** ([ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md)) — append-only growth is only sustainable because cold history is cheap.
- **Retrieval cost** of cold/frozen reads (and how zone-map pruning bounds it).
- **Compute efficiency** (work per core), since [storage and compute scale independently](adr/0007-storage-compute-separation.md).

A benchmark that's fast but ruinously expensive is reported as such — cost is not hidden behind latency.

## 7. Performance-claims policy

- **No public performance claim without a reproducible harness + published methodology.** Ever.
- Claims are **correctness-gated** and **asymmetric** — we state what we're world-class at, competitive at, and merely adequate at, in the same breath.
- We **disclose hardware and cost** with any headline number.
- We **report regressions** as openly as improvements; a perf postmortem is a normal artifact.

## 8. Continuous tracking & phasing

- **Nightly benchmark runs** with dashboards and regression alerts ([04](04-cicd.md#nightlyyml--sanitizers-fuzzing-sim-benchmarks)); optionally a `bencher`-style service for history.
- **Phasing:** microbenchmarks + the do-not-regress gate from **v0.2**; the temporal and analytical suites mature through **v0.5–v1.0**; published, competitor-comparative results are a **post-trust-gate** activity — we earn the right to make claims by first being correct.

---

*This methodology is itself part of the trust story: an audit-native engine that fudged its own benchmarks would be self-refuting. We measure the way we ask our users to trust us — verifiably.*
