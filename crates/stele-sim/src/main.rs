//! `stele-sim` driver — replays seeds against the deterministic harness.
//!
//! Walking-skeleton CLI: argument parsing and a "no scenarios yet" message.
//! Real seed replay lands as the storage/txn core does.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "stele-sim",
    about = "Stele deterministic simulation harness ([docs/06-testing-strategy.md])."
)]
struct Args {
    /// Number of distinct seeds to run when sweeping.
    #[arg(long, default_value_t = 0)]
    seeds: u64,

    /// Replay one specific seed (overrides --seeds).
    #[arg(long)]
    seed: Option<u64>,

    /// Toggle fault injection (disk, network, clock skew).
    #[arg(long, default_value = "off")]
    fault_injection: String,
}

fn main() {
    let args = Args::parse();
    if let Some(seed) = args.seed {
        let digest = stele_sim::run_storage_seed(seed);
        let vt_digest = stele_sim::run_validtime_seed(seed);
        let del_digest = stele_sim::run_delete_seed(seed);
        let dml_digest = stele_sim::run_dml_seed(seed);
        println!(
            "stele-sim: seed {seed} → storage digest {digest:#018x} · valid-time digest {vt_digest:#018x} · delete digest {del_digest:#018x} · dml digest {dml_digest:#018x}"
        );
    } else if args.seeds == 0 {
        println!("stele-sim: no seeds requested (pass --seeds N or --seed S)");
    } else {
        // Sweep: each seed is independent and reproducible. Fold the per-seed
        // digests with an order-dependent FNV-style mix (not XOR, which would
        // cancel matching digests) so the sweep stays a sharp regression signal.
        let mut sweep = 0xCBF2_9CE4_8422_2325u64;
        for seed in 0..args.seeds {
            // Mix both scenarios per seed so the sweep regresses on either the
            // sealed-segment path or the valid-time ingestion path.
            sweep = (sweep ^ stele_sim::run_storage_seed(seed)).wrapping_mul(0x0000_0100_0000_01B3);
            sweep =
                (sweep ^ stele_sim::run_validtime_seed(seed)).wrapping_mul(0x0000_0100_0000_01B3);
            sweep = (sweep ^ stele_sim::run_delete_seed(seed)).wrapping_mul(0x0000_0100_0000_01B3);
            // The full DML write path: WAL redo records replayed back into a delta.
            sweep = (sweep ^ stele_sim::run_dml_seed(seed)).wrapping_mul(0x0000_0100_0000_01B3);
        }
        println!(
            "stele-sim: swept {} seed(s) over the in-memory backend → sweep digest {sweep:#018x}",
            args.seeds
        );
        if args.fault_injection != "off" {
            // The flag is accepted (the justfile passes it), but the seeded
            // storage workload does not yet inject disk faults — that lands with
            // the seeded-fault virtual disk in STL-109. Say so rather than imply
            // toggling it changed the digest above.
            println!(
                "stele-sim: note: --fault-injection={} is not yet wired into the storage workload (STL-109)",
                args.fault_injection
            );
        }
    }
}
