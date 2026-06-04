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
        println!("stele-sim: seed {seed} → storage digest {digest:#018x}");
    } else if args.seeds == 0 {
        println!("stele-sim: no seeds requested (pass --seeds N or --seed S)");
    } else {
        // Sweep: each seed is independent and reproducible. We fold the
        // per-seed digests so the sweep itself has a single comparable result.
        let mut sweep = 0u64;
        for seed in 0..args.seeds {
            sweep ^= stele_sim::run_storage_seed(seed);
        }
        println!(
            "stele-sim: swept {} seed(s) over the in-memory backend, fault_injection={} → sweep digest {sweep:#018x}",
            args.seeds, args.fault_injection
        );
    }
}
