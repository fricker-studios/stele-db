//! The `stele` CLI binary.
//!
//! `stele server` starts the daemon (so the single binary covers the
//! "five-minute path" in [`docs/05-dev-environment.md`](../../../docs/05-dev-environment.md)),
//! `stele shell` opens the interactive query shell over pg-wire (STL-185),
//! `stele version` reports the build, and every other subcommand is a polite
//! "not yet" with a doc link.

use clap::{Parser, Subcommand};

mod client;
mod shell;

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
    /// Interactive SQL shell over pg-wire.
    Shell(ShellArgs),
    /// One-shot query. Not implemented in v0.1.
    Query { sql: String },
    /// Print version + build metadata.
    Version,
}

#[derive(clap::Args, Debug)]
struct ShellArgs {
    /// Engine host to connect to.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Engine pg-wire port.
    #[arg(long, default_value_t = stele_common::DEFAULT_PG_PORT)]
    port: u16,
    /// User name sent in the startup message (the dev server runs trust auth).
    #[arg(long, default_value = "stele")]
    user: String,
    /// Database name sent in the startup message.
    #[arg(long, default_value = "stele")]
    dbname: String,
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
        Cmd::Shell(s) => shell::run(&shell::Opts {
            host: s.host,
            port: s.port,
            user: s.user,
            dbname: s.dbname,
        }),
        Cmd::Query { .. } => {
            anyhow::bail!(
                "not implemented yet — see docs/03-roadmap.md. Use `stele shell` or `psql -h localhost -p 5454 -d stele` for now."
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
    fn documented_surface_parses() {
        assert!(matches!(parse(&["stele", "version"]), Cmd::Version));
        assert!(matches!(parse(&["stele", "shell"]), Cmd::Shell(_)));
        assert!(matches!(
            parse(&["stele", "query", "SELECT 1"]),
            Cmd::Query { .. }
        ));
        assert!(matches!(parse(&["stele", "server"]), Cmd::Server(_)));
    }

    #[test]
    fn shell_defaults_target_the_local_dev_server() {
        let Cmd::Shell(s) = parse(&["stele", "shell"]) else {
            panic!("expected shell subcommand");
        };
        assert_eq!(s.host, "127.0.0.1");
        assert_eq!(s.port, stele_common::DEFAULT_PG_PORT);
        assert_eq!(s.user, "stele");
        assert_eq!(s.dbname, "stele");
    }

    #[test]
    fn shell_accepts_explicit_connection_flags() {
        let Cmd::Shell(s) = parse(&[
            "stele", "shell", "--host", "10.0.0.7", "--port", "6000", "--user", "ops", "--dbname",
            "audit",
        ]) else {
            panic!("expected shell subcommand");
        };
        assert_eq!(s.host, "10.0.0.7");
        assert_eq!(s.port, 6000);
        assert_eq!(s.user, "ops");
        assert_eq!(s.dbname, "audit");
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
