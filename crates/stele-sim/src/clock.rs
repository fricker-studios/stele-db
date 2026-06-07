//! `VirtualClock` — deterministic simulated time ([STL-108]).
//!
//! The promised sim implementation of [`stele_common::time::Clock`]: it never
//! reads `SystemTime::now()`, so the system-time axis a seeded run stamps is a
//! pure function of the seed. Time does not flow on its own — it advances *only*
//! when the [scheduler](crate::scheduler) moves it, which happens when every task
//! is blocked and the only way forward is to jump to the next sleeper's deadline.
//! That is the whole point: logical time is compressed, and identical for every
//! replay of a seed ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::time::{Clock, SystemTimeMicros};

/// A monotonic, on-demand-advancing clock for deterministic simulation.
///
/// Cloning shares the same underlying time (an `Arc`), so the scheduler can hand
/// a clone to the system under test while keeping one to advance — both observe
/// the same virtual `now`.
#[derive(Debug, Clone, Default)]
pub struct VirtualClock {
    micros: Arc<AtomicI64>,
}

impl VirtualClock {
    /// A clock starting at `start` microseconds since the Unix epoch.
    #[must_use]
    pub fn new(start: i64) -> Self {
        Self {
            micros: Arc::new(AtomicI64::new(start)),
        }
    }

    /// The current virtual time, in microseconds since the Unix epoch.
    #[must_use]
    pub fn now_micros(&self) -> i64 {
        self.micros.load(Ordering::Acquire)
    }

    /// Advance time to at least `target`. Never moves backward: a `target` at or
    /// before `now` is a no-op, preserving monotonicity.
    pub fn advance_to(&self, target: i64) {
        let mut cur = self.micros.load(Ordering::Acquire);
        while target > cur {
            match self.micros.compare_exchange_weak(
                cur,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Advance time by `delta` microseconds (saturating).
    pub fn advance_by(&self, delta: i64) {
        self.advance_to(self.now_micros().saturating_add(delta));
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.now_micros())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_where_told_and_reads_through_clock() {
        let clock = VirtualClock::new(100);
        assert_eq!(clock.now_micros(), 100);
        assert_eq!(clock.now(), SystemTimeMicros(100));
    }

    #[test]
    fn advance_is_monotonic() {
        let clock = VirtualClock::new(0);
        clock.advance_to(50);
        assert_eq!(clock.now_micros(), 50);
        // Backward target is ignored.
        clock.advance_to(10);
        assert_eq!(clock.now_micros(), 50);
        clock.advance_by(5);
        assert_eq!(clock.now_micros(), 55);
    }

    #[test]
    #[allow(clippy::redundant_clone)] // The clone is the point: prove handles share state.
    fn clones_share_time() {
        let a = VirtualClock::new(0);
        let b = a.clone();
        // Advancing through either handle is observed by the other — they share
        // the same underlying time.
        a.advance_to(123);
        assert_eq!(b.now_micros(), 123);
        b.advance_to(200);
        assert_eq!(a.now_micros(), 200);
    }
}
