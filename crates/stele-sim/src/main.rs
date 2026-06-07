//! `stele-sim` driver — drives the [`stele_sim`] scenario registry (STL-110).
//!
//! `--seeds N` sweeps every registered scenario across N distinct seeds;
//! `--seed K` replays one seed across all of them. A scenario that violates an
//! invariant panics with the seed in its message; [`install_failure_reporter`]
//! prints a prominent `scenario + seed` banner so a contributor can reproduce
//! it locally with `just sim-seed K`.
//!
//! [`install_failure_reporter`]: stele_sim::install_failure_reporter

use clap::{Parser, ValueEnum};

/// A strict on/off toggle — clap rejects anything but `on` or `off`, so a typo
/// like `--fault-injection Off` fails fast instead of silently enabling faults.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Toggle {
    On,
    Off,
}

#[derive(Parser, Debug)]
#[command(
    name = "stele-sim",
    about = "Stele deterministic simulation harness ([docs/06-testing-strategy.md])."
)]
struct Args {
    /// Number of distinct seeds to sweep across all registered scenarios.
    #[arg(long, default_value_t = 0)]
    seeds: u64,

    /// Replay one specific seed across all scenarios (overrides --seeds).
    #[arg(long)]
    seed: Option<u64>,

    /// Toggle fault injection (gates the seeded-fault virtual-disk scenario).
    #[arg(long, value_enum, default_value = "off")]
    fault_injection: Toggle,
}

fn main() {
    let args = Args::parse();

    // Any scenario panic becomes a prominent, reproducible `scenario + seed`
    // banner. Installed for both paths so replay failures are named too.
    stele_sim::install_failure_reporter();

    if let Some(seed) = args.seed {
        // Reproduction path: run every scenario for this one seed and print each
        // digest. A failing scenario surfaces its full assertion (with the seed).
        println!("stele-sim: replaying seed {seed} across all scenarios");
        for (name, digest) in stele_sim::replay(seed) {
            println!("  {name:<18} digest {digest:#018x}");
        }
    } else if args.seeds == 0 {
        println!("stele-sim: no seeds requested (pass --seeds N or --seed S)");
    } else {
        let faults_on = args.fault_injection == Toggle::On;
        // Sweep: only returns once every active scenario passes every seed. A
        // failure prints the banner via the panic hook and exits non-zero.
        let report = stele_sim::sweep(args.seeds, faults_on);
        println!(
            "stele-sim: swept {} seed(s) across {} scenario(s) → sweep digest {:#018x}",
            report.seeds, report.scenarios, report.digest
        );
        // The scheduler's DoD statistic (STL-108): how many *distinct*
        // interleavings the seeds explored. The schedule scenario already runs
        // inside the sweep for its digest; replay the demo trace here purely to
        // count distinct schedules for the human-facing report.
        let schedules: std::collections::HashSet<Vec<u8>> =
            (0..args.seeds).map(stele_sim::run_schedule_seed).collect();
        println!(
            "stele-sim: scheduler explored {} distinct interleaving(s) across {} seed(s)",
            schedules.len(),
            args.seeds
        );
        if faults_on {
            println!(
                "stele-sim: fault injection on → seeded-fault virtual-disk scenario included (STL-109)"
            );
        }
    }
}
