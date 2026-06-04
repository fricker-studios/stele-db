//! Deterministic simulation harness — Stele's correctness substrate.
//!
//! The FoundationDB / TigerBeetle pattern: a virtual clock, virtual disk,
//! virtual network, and a deterministic scheduler. Every test seed is a movie
//! that plays back the same way every time — bugs reproduce instead of haunt
//! ([`docs/06-testing-strategy.md`](../../../docs/06-testing-strategy.md),
//! [ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).
//!
//! Scaffold only at v0.1; the harness fills out across milestones as the core
//! crystallizes.

#![allow(dead_code)]

// pub mod virtual_clock;
// pub mod virtual_disk;
// pub mod scheduler;
// pub mod fault_injection;
