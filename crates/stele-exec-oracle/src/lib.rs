//! Test-only home for the DuckDB differential oracles (STL-144, STL-167).
//!
//! This crate carries no code. It exists solely so the DuckDB-backed bitemporal
//! `AS OF` oracles in `tests/` can keep `duckdb` (the `bundled` C++
//! amalgamation) as a dependency *without* dragging that multi-minute compile
//! onto every per-PR CI job (STL-158). The crate is excluded from the per-PR
//! `--workspace` runs and from `default-members`; the oracles run in the nightly
//! gate via `cargo … -p stele-exec-oracle`.
//!
//! Two layers of the same bitemporal truth are diffed against an independent SQL
//! implementation inside an in-memory DuckDB:
//!
//! * `tests/duckdb_differential.rs` (STL-144) drives Stele's `SnapshotScan`
//!   executor directly and resolves the valid axis with a hand-coded membership
//!   test — the read path a query *lowers to*.
//! * `tests/sql_path_differential.rs` (STL-167) drives the **whole SQL
//!   bind→exec pipeline** — the binder lifting `FOR SYSTEM_TIME AS OF s FOR
//!   VALID_TIME AS OF v` and period predicates, the engine resolving both axes —
//!   so the differential covers the query surface end to end. This is the v0.2
//!   bitemporal-correctness exit gate.
//!
//! DuckDB stays a dev-only dependency that never links into the runtime-agnostic
//! storage/txn core (ADR-0010).
//!
//! (The type and crate names above are plain code spans, not intra-doc links:
//! `stele-exec` / `stele-engine` are dev-dependencies, invisible to this crate's
//! library target that `cargo doc` builds — a resolvable link would break the
//! `-D warnings` doc gate.)
