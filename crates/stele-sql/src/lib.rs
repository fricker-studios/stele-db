//! SQL frontend — parse, bind, plan, optimize.
//!
//! Starts from `sqlparser-rs` (cheap dependency, mature) and grows
//! Stele-specific temporal grammar on top
//! ([`docs/02-architecture.md` §6](../../../docs/02-architecture.md#6-query-layer)).
//!
//! The interesting optimizer rules are **temporal-aware**: pushing an `AS OF`
//! predicate into segment-level `sys_time` zone-map pruning is what makes
//! time-travel cheap.
//!
//! Scaffold only at v0.1.

#![allow(dead_code)]
