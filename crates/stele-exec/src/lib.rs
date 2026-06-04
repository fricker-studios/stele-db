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
//! Scaffold only at v0.1.

#![allow(dead_code)]
