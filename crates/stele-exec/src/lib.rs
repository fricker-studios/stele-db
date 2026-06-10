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
//! source operator ([`ScanSource`]) and a [`Project`] shaping operator. On top
//! of it sits the vectorized scalar expression evaluator ([`eval_expr`], with
//! its [`Expr`] / [`Vector`] vocabulary) and the [`Filter`] operator it powers
//! ([STL-170]) — comparisons, integer arithmetic, boolean connectives, and SQL
//! three-valued NULL logic over a whole batch at a time. The [`ExplodePayload`]
//! operator slices the row-codec payload blob into first-class value columns so
//! that filter runs over arbitrary columns on the live query path ([STL-206]).
//! The aggregate / join operators (STL-77 C11–C13) build on the same trait.
//!
//! [STL-170]: https://allegromusic.atlassian.net/browse/STL-170
//! [STL-206]: https://allegromusic.atlassian.net/browse/STL-206

mod expr;
mod operator;
mod period;
mod snapshot_scan;

pub use expr::{ArithOp, CmpOp, Expr, ExprError, LogicOp, Vector, eval_expr};
pub use operator::{DEFAULT_BATCH_SIZE, ExplodePayload, Filter, Operator, Project, ScanSource};
pub use period::evaluate;
pub use snapshot_scan::{Batch, Column, ScanError, ScanOutput, ScanStats, SnapshotScan};
// Re-exported so consumers (the binder's bound predicate, the oracle) name the
// same period vocabulary the evaluator works in ([STL-165]).
pub use stele_common::period::{Interval, IntervalError, PeriodPredicate};
