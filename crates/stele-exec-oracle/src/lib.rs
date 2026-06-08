//! Test-only home for the DuckDB differential oracle (STL-144).
//!
//! This crate carries no code. It exists solely so the DuckDB-backed bitemporal
//! `AS OF` oracle in `tests/duckdb_differential.rs` can keep `duckdb` (the
//! `bundled` C++ amalgamation) as a dependency *without* dragging that
//! multi-minute compile onto every per-PR CI job (STL-158). The crate is
//! excluded from the per-PR `--workspace` runs and from `default-members`; the
//! oracle runs in the nightly gate via `cargo … -p stele-exec-oracle`.
//!
//! It diffs the `stele-exec` executor against an independent SQL implementation
//! of the same bitemporal truth. DuckDB stays a dev-only dependency that never
//! links into the runtime-agnostic storage/txn core (ADR-0010).
