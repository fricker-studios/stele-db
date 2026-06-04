//! Clock abstraction.
//!
//! The Stele core never reads wall-clock time directly. Instead it depends on
//! the [`Clock`] trait, which is implemented by:
//!
//! * `SystemClock` — production: the OS clock.
//! * `stele_sim::VirtualClock` (later) — deterministic, advances on demand.
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
