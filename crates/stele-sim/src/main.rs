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
        println!("stele-sim: replay seed {seed} — scaffold, no scenarios registered yet");
    } else {
        println!(
            "stele-sim: sweep {} seed(s), fault_injection={} — scaffold, no scenarios registered yet",
            args.seeds, args.fault_injection
        );
    }
}
