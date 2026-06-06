//! Clock abstraction.
//!
//! The Stele core never reads wall-clock time directly. Instead it depends on
//! the [`Clock`] trait, which is implemented by:
//!
//! * `SystemClock` — production: the OS clock.
//! * `stele_sim::VirtualClock` — deterministic, advances on demand.
//!
//! Keeping the core off of `SystemTime::now()` is what lets every test seed
//! reproduce bit-for-bit ([ADR-0010](../../../../docs/adr/0010-deterministic-simulation-testing.md)).

use std::time::{SystemTime, UNIX_EPOCH};

/// Stele's system-time epoch is the Unix epoch, expressed in microseconds.
///
/// Microseconds give ~292,000 years of `i64` range and align with the precision
/// the bitemporal model uses on disk
/// (see [`docs/02-architecture.md` §2](../../../../docs/02-architecture.md#2-the-bitemporal-record-model)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SystemTimeMicros(pub i64);

/// Sentinel for "open" (current) system-time intervals — `sys_to = +∞`.
pub const SYSTEM_TIME_OPEN: SystemTimeMicros = SystemTimeMicros(i64::MAX);

/// A point on the **valid-time** axis — when a fact is true in the modeled
/// world.
///
/// As opposed to [`SystemTimeMicros`], which records when the *database* held
/// the version (see [`docs/02-architecture.md` §2](../../../../docs/02-architecture.md#2-the-bitemporal-record-model)).
/// Same epoch (Unix) and precision (microseconds) as system-time, but a
/// **distinct type on purpose**: the two axes are independent, and silently
/// comparing a system-time point to a valid-time point — or swapping one for
/// the other at a call site — is a classic bitemporal bug. Keeping them
/// un-interchangeable makes that a compile error.
///
/// Unlike system-time, valid-time is **per-table opt-in and supplied by the
/// writer** (invariant 4 of [architecture §12](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)),
/// so it is never stamped from a [`Clock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValidTimeMicros(pub i64);

/// Sentinel for an **open** valid-time interval — `valid_to = +∞`.
///
/// Read as "true from `valid_from` until further notice." Mirrors
/// [`SYSTEM_TIME_OPEN`] on the system axis; a row whose fact has no known end
/// date carries it.
pub const VALID_TIME_OPEN: ValidTimeMicros = ValidTimeMicros(i64::MAX);

/// Injectable monotonic-ish clock. Implementations decide reality (OS clock) vs
/// virtual time (sim).
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTimeMicros;
}

/// The OS clock. Production default.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTimeMicros {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        // Saturating cast: we will not survive past year ~294,247 AD anyway.
        SystemTimeMicros(i64::try_from(dur.as_micros()).unwrap_or(i64::MAX))
    }
}
