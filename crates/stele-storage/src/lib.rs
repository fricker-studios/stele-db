//! Storage engine — append-only columnar segments, row-oriented delta tier, and WAL.
//!
//! This crate is the **deterministic core** of Stele
//! (see [`docs/02-architecture.md` §3](../../../docs/02-architecture.md#3-storage-engine-internals)
//! and [ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).
//!
//! It must remain **runtime-agnostic**: no `tokio`, no global state, no direct
//! reads of wall-clock time. All I/O and time enter through traits (`Clock`,
//! later `Disk`, `Wal`) so the engine can be driven by either real OS resources
//! or the deterministic substitutes in [`stele-sim`].
//!
//! ## Invariants ([architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//!
//! 1. **No in-place mutation of a sealed segment.** Ever.
//! 2. **The WAL fsync is the only durability point.**
//! 3. **Immutability ⇒ trivial cache/replica coherence.**
//! 7. **The storage/txn core is deterministic** and runnable under the simulation scheduler.

#![allow(dead_code)] // scaffold — real impls land per [STL-76] roadmap

pub mod backend;
pub(crate) mod checksum;
pub mod delta;
pub mod dml;
pub mod segment;
pub mod systime;
pub mod validtime;
pub mod wal;

// Submodule placeholders — each becomes its own ticket under STL-76.
// pub mod compaction;
