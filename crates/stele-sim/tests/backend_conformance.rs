//! The seeded `FaultDisk` is a conformant [`Disk`] backend ([STL-232]).
//!
//! `stele-storage`'s shared conformance suite
//! ([`stele_storage::backend::conformance`]) runs **unchanged** against the
//! fault-injecting disk, completing the DoD triangle: `local` and `memory`
//! pass it in `stele-storage/tests/backend.rs`, and the disk every fault sweep
//! is built on passes it here — so a fault scenario's findings are about the
//! *engine*, never about the test disk deviating from the contract.
//!
//! Fault *shapes* (torn prefix, short read, bit flip, …) are pinned by
//! `fault_disk`'s own unit tests; the engine-level crash semantics (rotation
//! fence poison, flush abort before the vouch) live with the wired call sites
//! in `stele-storage`. What this file adds on top of the quiet-contract run is
//! the caller's-eye determinism claim: the same seed produces the same
//! *observable outcome sequence* through the public `Disk` API, not merely the
//! same internal event log.

use std::io;

use stele_sim::{FaultDisk, FaultProfile};
use stele_storage::backend::conformance;
use stele_storage::backend::{Disk, DiskFile};

/// A quiet (no fault class enabled) `FaultDisk` is contract-transparent: the
/// full shared suite passes exactly as it does on the inner `MemDisk`.
#[test]
fn a_quiet_fault_disk_passes_the_shared_backend_contract() {
    let mut seed = 0xC0FF_EE00u64;
    conformance::run_all(|| {
        seed += 1;
        FaultDisk::new(seed, FaultProfile::none())
    });
}

/// One caller-visible step of the probe workload: which call ran, whether it
/// succeeded, and what the caller observed (error kind / bytes read).
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Err(io::ErrorKind),
    Read(Vec<u8>),
}

/// Record one unit-result outcome.
fn tally(outcomes: &mut Vec<Outcome>, r: io::Result<()>) {
    match r {
        Ok(()) => outcomes.push(Outcome::Ok),
        Err(e) => outcomes.push(Outcome::Err(e.kind())),
    }
}

/// Drive a fixed workload through the public `Disk` API and record every
/// caller-visible outcome in order.
fn probe(disk: &FaultDisk) -> Vec<Outcome> {
    let mut outcomes = Vec::new();

    for i in 0..8u8 {
        tally(&mut outcomes, disk.create(&format!("f-{i}")).map(|_| ()));
    }
    for i in 0..8u8 {
        let name = format!("f-{i}");
        match disk.open(&name) {
            Err(e) => outcomes.push(Outcome::Err(e.kind())),
            Ok(mut f) => {
                tally(&mut outcomes, f.append(&vec![i; usize::from(i) + 3]));
                tally(&mut outcomes, f.sync());
                let mut buf = [0u8; 16];
                match f.read_at(0, &mut buf) {
                    Ok(n) => outcomes.push(Outcome::Read(buf[..n].to_vec())),
                    Err(e) => outcomes.push(Outcome::Err(e.kind())),
                }
            }
        }
    }
    tally(&mut outcomes, disk.sync_dir());
    outcomes
}

/// A moderately hostile mixed profile, so the probe workload actually trips
/// several classes.
fn mixed_profile() -> FaultProfile {
    FaultProfile::none()
        .with_torn_write(0.3)
        .with_short_read(0.3)
        .with_bit_flip(0.2)
        .with_fail_sync(0.3)
        .with_full_disk(0.1)
}

/// The seed-replay claim, from the caller's side of the trait: the same seed
/// and profile replay the byte-identical sequence of successes, error kinds,
/// and read contents — and the internal fault logs agree too.
#[test]
fn the_same_seed_replays_identical_observable_outcomes() {
    let run = |seed: u64| {
        let disk = FaultDisk::new(seed, mixed_profile());
        let outcomes = probe(&disk);
        (outcomes, disk.events())
    };

    let (outcomes_a, events_a) = run(42);
    let (outcomes_b, events_b) = run(42);
    assert!(
        events_a.iter().any(|_| true),
        "the mixed profile must actually inject something for this test to bite"
    );
    assert_eq!(outcomes_a, outcomes_b, "same seed ⇒ same observable run");
    assert_eq!(events_a, events_b, "same seed ⇒ same fault log");

    // And a different seed produces a different run (for these constants —
    // deterministically checked, no flake).
    let (outcomes_c, _) = run(43);
    assert_ne!(outcomes_a, outcomes_c, "distinct seeds diverge");
}
