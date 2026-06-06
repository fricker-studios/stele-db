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

mod snapshot_scan;

pub use snapshot_scan::{Batch, Column, ScanError, ScanOutput, ScanStats, SnapshotScan};
