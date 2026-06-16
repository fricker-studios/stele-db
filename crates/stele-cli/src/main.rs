//! The `stele` CLI binary.
//!
//! `stele server` starts the daemon (so the single binary covers the
//! "five-minute path" in [`docs/05-dev-environment.md`](../../../docs/05-dev-environment.md)),
//! `stele shell` opens the interactive query shell over pg-wire (STL-185),
//! `stele restore` rebuilds a data directory from a backup (STL-249),
//! `stele version` reports the build, and every other subcommand is a polite
//! "not yet" with a doc link.

use anyhow::Context as _;
use clap::{Parser, Subcommand};

mod admin;
mod client;
mod highlight;
mod render;
mod shell;
mod theme;

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
    /// Rebuild a data directory from a backup, then verify it (STL-249).
    Restore(RestoreArgs),
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
    /// TLS, libpq sslmode-style: `prefer` (try TLS, fall back to plaintext —
    /// the default), `disable`, `require` (TLS or fail; encrypts but does not
    /// verify the server), or `verify-full` (verify against --tls-ca + host).
    #[arg(long, value_enum, default_value_t = client::SslMode::Prefer)]
    tls: client::SslMode,
    /// PEM CA bundle that `--tls verify-full` verifies the server against.
    #[arg(long)]
    tls_ca: Option<std::path::PathBuf>,
    /// Result-table border style.
    #[arg(long, value_enum, default_value_t = render::BorderStyle::Psql)]
    border: render::BorderStyle,
    /// Prepend a 1-based row-number column to result tables.
    #[arg(long)]
    row_numbers: bool,
    /// Disable ANSI color even on a terminal (NO_COLOR is also honored).
    #[arg(long)]
    no_color: bool,
    /// Admin / control-plane host for the `\status`/`\backup`/`\restore`/`\pitr`/
    /// `\inspect-segment` tier (STL-200). Defaults to `--host`.
    #[arg(long)]
    admin_host: Option<String>,
    /// Admin / control-plane (ops listener) port — where the HTTP/JSON gateway
    /// answers (STL-254). Defaults to the documented ops port `9090`.
    #[arg(long, default_value_t = 9090)]
    admin_port: u16,
    /// Bearer token for the admin / control-plane API. The server enables the API
    /// by configuring `[admin] tokens` in `stele.toml`; without a token here the
    /// admin tier is refused (the surface rejects every unauthenticated request).
    /// Falls back to `STELE_ADMIN_TOKEN` when the flag is omitted.
    #[arg(long)]
    admin_token: Option<String>,
}

#[derive(clap::Args, Debug)]
struct RestoreArgs {
    /// The backup directory to restore from — a directory produced by
    /// `BACKUP TO '<path>'`, containing a `MANIFEST` and the backed-up files.
    #[arg(long)]
    from: std::path::PathBuf,
    /// The data directory to materialize into. Must be empty (or not yet exist);
    /// restore refuses to merge into a directory that already holds data.
    #[arg(long)]
    to: std::path::PathBuf,
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
            let cfg = if let Some(path) = s.config {
                // The file owns configuration; `--listen` still overrides the
                // full listen address (host + port). `--dev` has no effect here.
                let mut cfg = stele_server::Config::load(path)?;
                if let Some(addr) = s.listen {
                    cfg.listen = addr;
                }
                cfg
            } else {
                let mut cfg = stele_server::Config::dev();
                if let Some(addr) = s.listen {
                    cfg.listen = addr;
                }
                cfg.dev = s.dev;
                cfg
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(stele_server::run(cfg))?;
            Ok(())
        }
        Cmd::Shell(s) => {
            // The admin tier defaults its host to the pg-wire host (the common
            // single-node case) and its port to the ops listener's `9090`. The
            // token falls back to STELE_ADMIN_TOKEN so it need not appear in
            // shell history or `ps` output.
            let admin_host = s.admin_host.unwrap_or_else(|| s.host.clone());
            let admin_token = s.admin_token.or_else(|| {
                std::env::var("STELE_ADMIN_TOKEN")
                    .ok()
                    .filter(|t| !t.is_empty())
            });
            shell::run(&shell::Opts {
                host: s.host,
                port: s.port,
                user: s.user,
                dbname: s.dbname,
                tls: client::TlsOpts {
                    mode: s.tls,
                    ca: s.tls_ca,
                },
                border: s.border,
                row_nums: s.row_numbers,
                no_color: s.no_color,
                admin: admin::AdminConfig {
                    host: admin_host,
                    port: s.admin_port,
                    token: admin_token,
                },
            })
        }
        Cmd::Restore(r) => run_restore(&r),
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

/// Rebuild a data directory from a backup, then verify it by running normal
/// recovery ([STL-249]).
///
/// This is the **offline** half of backup/restore (the online half is the
/// `BACKUP TO '<path>'` admin command). It [`restore_disk`](stele_engine::backup::restore_disk)s
/// the backup — checking the manifest's self-digest and every file's SHA-256
/// before writing it, so a single flipped byte is refused — then boots
/// [`SessionEngine::recover`](stele_engine::SessionEngine::recover) against the
/// materialized directory, which re-verifies segment checksums and the commit-log
/// hash chain (STL-178). On success the data directory is ready for
/// `stele server --config …` to point at.
fn run_restore(args: &RestoreArgs) -> anyhow::Result<()> {
    use stele_storage::backend::LocalDisk;

    anyhow::ensure!(
        args.from.is_dir(),
        "backup directory {} does not exist or is not a directory",
        args.from.display()
    );
    let src = LocalDisk::open(&args.from)
        .with_context(|| format!("opening backup directory {}", args.from.display()))?;
    let dst = LocalDisk::open(&args.to)
        .with_context(|| format!("opening target data directory {}", args.to.display()))?;

    let manifest = stele_engine::backup::restore_disk(&src, &dst).with_context(|| {
        format!(
            "restoring backup {} into {}",
            args.from.display(),
            args.to.display()
        )
    })?;

    // Validate the materialized directory by running normal recovery: segment
    // checksums and the STL-178 commit-log hash chain re-verify, and every table
    // reopens. A corrupt-but-checksum-matching backup is caught here.
    let engine = stele_engine::SessionEngine::recover(dst, stele_common::time::SystemClock)
        .with_context(|| {
            format!(
                "recovering the restored data directory {}",
                args.to.display()
            )
        })?;

    println!(
        "restored {} file(s) into {} (backup fence {}µs); recovered {} table(s)",
        manifest.files.len(),
        args.to.display(),
        manifest.fence_micros,
        engine.describe_live_tables().len(),
    );
    Ok(())
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
        assert!(matches!(
            parse(&["stele", "restore", "--from", "/b", "--to", "/d"]),
            Cmd::Restore(_)
        ));
    }

    #[test]
    fn restore_parses_from_and_to() {
        let Cmd::Restore(r) = parse(&[
            "stele",
            "restore",
            "--from",
            "/srv/backup",
            "--to",
            "/var/lib/stele",
        ]) else {
            panic!("expected restore subcommand");
        };
        assert_eq!(r.from, std::path::PathBuf::from("/srv/backup"));
        assert_eq!(r.to, std::path::PathBuf::from("/var/lib/stele"));
    }

    #[test]
    fn restore_requires_both_from_and_to() {
        assert!(Args::try_parse_from(["stele", "restore", "--from", "/b"]).is_err());
        assert!(Args::try_parse_from(["stele", "restore", "--to", "/d"]).is_err());
    }

    #[test]
    fn restore_errors_when_the_backup_directory_is_missing() {
        let dirs = Scratch::new("restore-missing");
        let err = run_restore(&RestoreArgs {
            from: dirs.path().join("nonexistent-backup"),
            to: dirs.path().join("data"),
        })
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("does not exist"),
            "expected a clear missing-backup error, got: {err:#}"
        );
    }

    #[test]
    fn restore_round_trips_a_real_backup_and_recovers() {
        use stele_common::time::SystemClock;
        use stele_engine::SessionEngine;
        use stele_storage::backend::LocalDisk;

        let dirs = Scratch::new("restore-round-trip");
        let data = dirs.path().join("data");
        let backup = dirs.path().join("backup");
        let restored = dirs.path().join("restored");

        // Produce a real backup from a live engine on local disk.
        let mut engine =
            SessionEngine::open(LocalDisk::open(&data).expect("data disk"), SystemClock);
        let target = LocalDisk::open(&backup).expect("backup disk");
        let manifest = engine.backup(&target).expect("backup");
        drop(engine);
        // The backup directory carries a manifest the CLI will read.
        assert!(backup.join("MANIFEST").is_file(), "manifest was written");

        // The CLI restore verb materializes + verifies + recovers without error.
        run_restore(&RestoreArgs {
            from: backup.clone(),
            to: restored.clone(),
        })
        .expect("restore");

        // Every file the manifest lists is materialized byte-for-byte; the manifest
        // itself is a backup artifact and is not copied into the data dir.
        assert!(!restored.join("MANIFEST").exists());
        for entry in &manifest.files {
            let original = std::fs::read(backup.join(&entry.name)).expect("read backup file");
            let copy = std::fs::read(restored.join(&entry.name)).expect("read restored file");
            assert_eq!(copy, original, "{} restored byte-for-byte", entry.name);
        }
    }

    #[test]
    fn restore_refuses_a_tampered_backup() {
        use stele_common::time::SystemClock;
        use stele_engine::SessionEngine;
        use stele_storage::backend::LocalDisk;

        let dirs = Scratch::new("restore-tamper");
        let data = dirs.path().join("data");
        let backup = dirs.path().join("backup");

        let mut engine =
            SessionEngine::open(LocalDisk::open(&data).expect("data disk"), SystemClock);
        engine
            .backup(&LocalDisk::open(&backup).expect("backup disk"))
            .expect("backup");
        drop(engine);

        // Flip a byte in the manifest: restore must refuse before touching the target.
        let manifest_path = backup.join("MANIFEST");
        let mut bytes = std::fs::read(&manifest_path).expect("read manifest");
        let last = bytes.len() - 2; // a hex digit inside the trailing digest line
        bytes[last] ^= 0x01;
        std::fs::write(&manifest_path, &bytes).expect("rewrite manifest");

        let err = run_restore(&RestoreArgs {
            from: backup,
            to: dirs.path().join("restored"),
        })
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("tamper") || format!("{err:#}").contains("digest"),
            "a tampered backup must be refused, got: {err:#}"
        );
    }

    /// A unique scratch directory under the OS temp dir, removed on drop — the
    /// CLI restore tests need real local directories but must leave nothing behind.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("stele-cli-{label}-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create scratch dir");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
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
        // libpq's default: try TLS, fall back to plaintext (STL-251).
        assert_eq!(s.tls, client::SslMode::Prefer);
        assert!(s.tls_ca.is_none());
    }

    #[test]
    fn shell_accepts_tls_flags() {
        let Cmd::Shell(s) = parse(&[
            "stele",
            "shell",
            "--tls",
            "verify-full",
            "--tls-ca",
            "/etc/stele/ca.pem",
        ]) else {
            panic!("expected shell subcommand");
        };
        assert_eq!(s.tls, client::SslMode::VerifyFull);
        assert_eq!(
            s.tls_ca.as_deref(),
            Some(std::path::Path::new("/etc/stele/ca.pem"))
        );
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
