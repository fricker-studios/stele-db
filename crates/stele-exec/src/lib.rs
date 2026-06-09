//! Vectorized executor — Arrow-shaped batches, pull/push hybrid.
//!
//! Runs against an MVCC snapshot from [`stele-txn`] and reads from the storage
//! engine's tiered layout
//! ([`docs/02-architecture.md` §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow)).
//!
//! Constraint: the executor core is written to run under the deterministic
//! scheduler ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md))
//! — no `tokio::spawn`, no wall-clock reads.
//!
//! v0.1 lands the read-path glue: [`SnapshotScan`], which merges the delta tier
//! and the sealed segments into one snapshot-resolved, projected batch
//! ([STL-100]).
//!
//! v0.2 adds the [`Operator`] framework: a Volcano-style, batch-at-a-time pull
//! pipeline over Arrow-shaped batches, with [`SnapshotScan`] re-expressed as a
//! source operator ([`ScanSource`]) and a [`Project`] shaping operator. The
//! aggregate / join / filter operators (STL-77 C10–C13) build on this trait.

mod operator;
mod period;
mod snapshot_scan;

pub use operator::{DEFAULT_BATCH_SIZE, Operator, Project, ScanSource};
pub use period::evaluate;
pub use snapshot_scan::{Batch, Column, ScanError, ScanOutput, ScanStats, SnapshotScan};
// Re-exported so consumers (the binder's bound predicate, the oracle) name the
// same period vocabulary the evaluator works in ([STL-165]).
pub use stele_common::period::{Interval, IntervalError, PeriodPredicate};
