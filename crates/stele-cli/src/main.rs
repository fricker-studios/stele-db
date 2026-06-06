//! The `stele` CLI binary.
//!
//! v0.1 surface is intentionally tiny: `stele server` starts the daemon (so the
//! single binary covers the "five-minute path" in [`docs/05-dev-environment.md`](../../../docs/05-dev-environment.md)),
//! `stele version` reports the build, and every other subcommand is a polite
//! "not yet" with a doc link.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "stele",
    version,
    about = "The Stele CLI — engine daemon, shell, and admin tooling."
)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Start the engine daemon (alias for `stele-server`).
    Server(ServerArgs),
    /// Interactive SQL shell. Not implemented in v0.1.
    Shell,
    /// One-shot query. Not implemented in v0.1.
    Query { sql: String },
    /// Print version + build metadata.
    Version,
}

#[derive(clap::Args, Debug)]
struct ServerArgs {
    #[arg(long)]
    listen: Option<std::net::SocketAddr>,
    /// Dev mode: verbose tracing, no auth, scratch storage.
    /// Ignored when `--config` is given — a config file always runs in non-dev mode.
    #[arg(long, default_value_t = true)]
    dev: bool,
    /// Path to a `stele.toml`. When set, config (including `[storage] backend`)
    /// comes from the file instead of dev defaults.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmd::Server(s) => {
            let cfg = match s.config {
                // The file owns configuration; `--listen` still overrides the
                // full listen address (host + port). `--dev` has no effect here.
                Some(path) => {
                    let mut cfg = stele_server::Config::load(path)?;
                    if let Some(addr) = s.listen {
                        cfg.listen = addr;
                    }
                    cfg
                }
                None => {
                    let mut cfg = stele_server::Config::dev();
                    if let Some(addr) = s.listen {
                        cfg.listen = addr;
                    }
                    cfg.dev = s.dev;
                    cfg
                }
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(stele_server::run(cfg))?;
            Ok(())
        }
        Cmd::Shell | Cmd::Query { .. } => {
            anyhow::bail!(
                "not implemented in v0.1 — see docs/03-roadmap.md. Use `psql -h localhost -p 5454 -d stele` for now."
            )
        }
        Cmd::Version => {
            println!("{}", version_line());
            Ok(())
        }
    }
}

/// The line `stele version` prints: the crate version plus the git commit the
/// binary was built from (captured in `build.rs`, or `unknown` when built
/// outside a git checkout).
fn version_line() -> String {
    format!(
        "stele {} (commit {})",
        env!("CARGO_PKG_VERSION"),
        env!("STELE_GIT_COMMIT")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse exactly like `main`, but surface clap errors as a test failure
    /// instead of `std::process::exit` (which `parse_from` would do, taking the
    /// whole test binary down).
    fn parse(argv: &[&str]) -> Cmd {
        Args::try_parse_from(argv).expect("argv should parse").cmd
    }

    #[test]
    fn version_line_reports_crate_version_and_commit() {
        let line = version_line();
        assert!(line.contains(env!("CARGO_PKG_VERSION")), "{line}");
        // The rendered line carries the *actual* commit, not just the label —
        // build.rs always sets the env var (never empty, even off a checkout).
        assert!(!env!("STELE_GIT_COMMIT").is_empty());
        assert!(line.contains(env!("STELE_GIT_COMMIT")), "{line}");
    }

    #[test]
    fn documented_v0_1_surface_parses() {
        assert!(matches!(parse(&["stele", "version"]), Cmd::Version));
        assert!(matches!(parse(&["stele", "shell"]), Cmd::Shell));
        assert!(matches!(
            parse(&["stele", "query", "SELECT 1"]),
            Cmd::Query { .. }
        ));
        assert!(matches!(parse(&["stele", "server"]), Cmd::Server(_)));
    }

    #[test]
    fn server_accepts_listen_and_dev_flags() {
        let Cmd::Server(s) = parse(&["stele", "server", "--listen", "127.0.0.1:6000", "--dev"])
        else {
            panic!("expected server subcommand");
        };
        assert_eq!(s.listen, Some("127.0.0.1:6000".parse().unwrap()));
        assert!(s.dev);
    }
}
