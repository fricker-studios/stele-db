//! Shared primitives for the Stele workspace.
//!
//! This crate is the dependency root of the workspace graph
//! (see [`docs/02-architecture.md` §11](../../../docs/02-architecture.md#11-crate--module-decomposition-intended)).
//! It is deliberately small: types, error scaffolding, and clock abstractions
//! that the storage / txn core can depend on without dragging in an async runtime.
//!
//! The runtime-agnostic constraint is an architectural invariant
//! ([ADR-0010 — Deterministic simulation testing](../../../docs/adr/0010-deterministic-simulation-testing.md)),
//! enforced here by keeping this crate `no_std`-friendly in spirit (we still link
//! `std`, but avoid `tokio`, file I/O, and global state).

pub mod datetime;
pub mod hash;
pub mod hashkey;
pub mod metrics;
pub mod period;
pub mod provenance;
pub mod row_codec;
pub mod scram;
pub mod time;
pub mod types;

/// Stele's default Postgres-wire listen port ([ADR-0017](../../../docs/adr/0017-default-network-port-5454.md)).
pub const DEFAULT_PG_PORT: u16 = 5454;

/// Errors that cross crate boundaries.
///
/// Each subsystem layers its own typed errors on top; this enum is the lingua
/// franca used at boundaries where a more specific type would leak internals.
#[derive(Debug, thiserror::Error)]
pub enum SteleError {
    /// A precondition or invariant was violated. Indicates a bug, not bad input.
    #[error("internal invariant violated: {0}")]
    Internal(String),

    /// I/O failure surfaced from a runtime boundary.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias for crate-local results.
pub type Result<T, E = SteleError> = std::result::Result<T, E>;
