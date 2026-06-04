//! Transaction manager — MVCC layered on the append-only store.
//!
//! Snapshot isolation is the v1 default; serializable (SSI) is a later opt-in
//! ([`docs/02-architecture.md` §9](../../../docs/02-architecture.md#9-transaction--concurrency-model),
//! [ADR-0008](../../../docs/adr/0008-mvcc-on-append-only.md)).
//!
//! Scaffold only at v0.1; the snapshot / conflict-detection paths land with
//! the executor.

#![allow(dead_code)]
