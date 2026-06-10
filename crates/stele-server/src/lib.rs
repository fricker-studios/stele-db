//! Library surface for the engine daemon.
//!
//! Kept thin: the `main` binary parses args and invokes [`run`].
//! `stele-cli` depends on this crate so that `stele server …` can dispatch the
//! same code path as `stele-server` directly.
//!
//! ## Configuration
//!
//! Operators run with a `stele.toml` ([05 — configuration](../../../docs/05-dev-environment.md#configuration));
//! [`Config::from_toml_str`] / [`Config::load`] parse it. The five-minute path
//! needs no file: [`Config::dev`] supplies safe defaults (a local backend under a
//! scratch dir). [STL-116] wires the `[storage] backend` selection through to the
//! [`AnyDisk`] the engine constructs at boot.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use serde::Deserialize;
use stele_common::DEFAULT_PG_PORT;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server as PgServer, SharedSession};
use stele_storage::backend::{AnyDisk, BackendKind};
use tokio::signal;
use tracing::info;

/// Default data directory for non-dev runs that omit `[server] data_dir`
/// (matches the `stele.toml` example in [05 — configuration](../../../docs/05-dev-environment.md#configuration)).
const DEFAULT_DATA_DIR: &str = "/var/lib/stele";

/// Resolved engine configuration — the single point everyone reads from.
///
/// Built either from defaults ([`Config::dev`]) or a parsed `stele.toml`
/// ([`Config::from_toml_str`] / [`Config::load`]). Plenty more knobs will land.
#[derive(Debug, Clone)]
pub struct Config {
    /// pg-wire listen address.
    pub listen: SocketAddr,
    /// Dev mode: verbose tracing, no auth, scratch storage.
    pub dev: bool,
    /// Which storage backend the engine boots on.
    pub backend: BackendKind,
    /// Data directory the `local` backend roots itself at (ignored by `memory`).
    pub data_dir: PathBuf,
}

impl Config {
    /// Dev defaults — the five-minute path, no config file required: local
    /// backend in a per-process scratch dir, verbose tracing
    /// ([05](../../../docs/05-dev-environment.md#configuration)).
    #[must_use]
    pub fn dev() -> Self {
        Self {
            listen: default_listen(),
            dev: true,
            backend: BackendKind::Local,
            data_dir: dev_scratch_dir(),
        }
    }

    /// Parse a `stele.toml` document into a `Config` (a non-dev, operator run).
    ///
    /// # Errors
    /// Returns an error if the TOML is malformed or `[storage] backend` is not a
    /// recognized backend name.
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        let file: FileConfig = toml::from_str(s).context("parsing stele.toml")?;
        file.resolve()
    }

    /// Read and parse `stele.toml` from `path`.
    ///
    /// # Errors
    /// Returns an error if the file cannot be read, the TOML is malformed, or
    /// `[storage] backend` is invalid.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        Self::from_toml_str(&text)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::dev()
    }
}

/// The raw `stele.toml` shape this ticket models. Unknown sections (`[wal]`,
/// `[telemetry]`, `[storage.cache]`, …) are ignored by serde, so a richer config
/// file still parses — those knobs land in later tickets.
#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    storage: StorageSection,
}

#[derive(Debug, Default, Deserialize)]
struct ServerSection {
    listen: Option<SocketAddr>,
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct StorageSection {
    /// Kept as a raw string so the parse error (and its clear message) is owned
    /// by [`BackendKind`]'s [`FromStr`](std::str::FromStr), not serde.
    backend: Option<String>,
}

impl FileConfig {
    fn resolve(self) -> anyhow::Result<Config> {
        let backend = match self.storage.backend.as_deref() {
            Some(s) => s
                .parse::<BackendKind>()
                .map_err(|e| anyhow::anyhow!("[storage] {e}"))?,
            None => BackendKind::Local,
        };
        Ok(Config {
            listen: self.server.listen.unwrap_or_else(default_listen),
            dev: false,
            backend,
            data_dir: self
                .server
                .data_dir
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR)),
        })
    }
}

/// Boot the engine: install tracing, construct the configured storage backend,
/// start the pgwire listener, wait for SIGINT.
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    init_tracing(cfg.dev);
    info!(?cfg, "stele-server: starting");

    // Construct the backend the operator selected, then stand up the per-session
    // engine on it — the Catalog + commit clock + per-table storage tiers that
    // hold state across statements (STL-148).
    let disk = AnyDisk::open(cfg.backend, &cfg.data_dir)
        .with_context(|| format!("opening {} storage backend", cfg.backend))?;
    info!(
        backend = %cfg.backend,
        data_dir = %cfg.data_dir.display(),
        "storage backend ready"
    );
    // Boot through the cold-start recovery path (STL-210, ADR-0028): replay the
    // durable catalog log and reopen every table's tiers. On an empty data dir
    // this is exactly a fresh session, so it is unconditional — a restarted
    // server resumes the tables (and their history) a prior run created.
    let engine = SessionEngine::recover(disk, SystemClock)
        .context("recovering session engine from on-disk state")?;
    info!(
        tables = engine.describe_live_tables().len(),
        "session engine ready"
    );

    // One engine shared across every connection, behind a mutex (STL-131): a
    // CREATE TABLE on any connection is visible to the next statement, and the
    // pgwire loop now routes DDL through `engine.execute` (table reads / DML
    // follow in STL-147). The lock is held only per synchronous statement,
    // never across wire I/O.
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let pg = PgServer::new(cfg.listen, session);

    tokio::select! {
        res = pg.run() => res.context("pgwire listener exited")?,
        _ = signal::ctrl_c() => {
            info!("received SIGINT, shutting down");
        }
    }

    Ok(())
}

const fn default_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PG_PORT)
}

/// A per-process scratch directory for `--dev`, under the OS temp dir so a fresh
/// clone needs no setup and leaves nothing behind in the source tree.
fn dev_scratch_dir() -> PathBuf {
    std::env::temp_dir().join(format!("stele-dev-{}", std::process::id()))
}

/// The environment variable that selects the log output format.
const LOG_FORMAT_ENV: &str = "STELE_LOG_FORMAT";

/// The verbosity filter applied when `RUST_LOG`
/// ([`EnvFilter::try_from_default_env`](tracing_subscriber::EnvFilter::try_from_default_env))
/// is unset: chatty for dev, quiet for an operator run.
const fn default_filter(dev: bool) -> &'static str {
    if dev { "info,stele=debug" } else { "info" }
}

/// How the subscriber renders each event — selected by `STELE_LOG_FORMAT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    /// Human-readable text, the dev default.
    Text,
    /// One JSON object per line, for production log shippers
    /// (`STELE_LOG_FORMAT=json`).
    Json,
}

impl LogFormat {
    /// Resolve the format from a raw `STELE_LOG_FORMAT` value. The match is
    /// trimmed and case-insensitive; an unset, empty, or unrecognized value
    /// falls back to [`LogFormat::Text`] so a typo degrades to readable logs
    /// rather than silently changing format or dropping output.
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("json") => Self::Json,
            _ => Self::Text,
        }
    }

    /// Read [`LOG_FORMAT_ENV`] from the process environment.
    fn from_env() -> Self {
        Self::parse(std::env::var(LOG_FORMAT_ENV).ok().as_deref())
    }
}

/// Install the global tracing subscriber.
///
/// Verbosity honors `RUST_LOG` (an [`EnvFilter`](tracing_subscriber::EnvFilter)
/// directive string such as `stele_pgwire=trace`); when it is unset the
/// [`default_filter`] for the mode
/// applies. Output is human-readable text unless `STELE_LOG_FORMAT=json`
/// selects the line-delimited JSON formatter for production
/// ([05 — logging](../../../docs/05-dev-environment.md#logging)).
///
/// Uses `try_init`, so a second call (e.g. a test that already installed a
/// subscriber) is a no-op rather than a panic.
fn init_tracing(dev: bool) {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter(dev)));

    let builder = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true);

    match LogFormat::from_env() {
        LogFormat::Json => {
            let _ = builder.json().try_init();
        }
        LogFormat::Text => {
            let _ = builder.try_init();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_backend_parses_from_storage_section() {
        let cfg = Config::from_toml_str("[storage]\nbackend = \"memory\"\n").unwrap();
        assert_eq!(cfg.backend, BackendKind::Memory);
        assert!(!cfg.dev);
    }

    #[test]
    fn local_backend_uses_configured_data_dir() {
        let toml = "[server]\ndata_dir = \"/srv/stele\"\n[storage]\nbackend = \"local\"\n";
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.backend, BackendKind::Local);
        assert_eq!(cfg.data_dir, PathBuf::from("/srv/stele"));
    }

    #[test]
    fn listen_is_parsed_from_server_section() {
        let cfg = Config::from_toml_str("[server]\nlisten = \"127.0.0.1:6000\"\n").unwrap();
        assert_eq!(cfg.listen, "127.0.0.1:6000".parse().unwrap());
    }

    #[test]
    fn missing_storage_section_defaults_to_local() {
        let cfg = Config::from_toml_str("[server]\ndata_dir = \"/srv/stele\"\n").unwrap();
        assert_eq!(cfg.backend, BackendKind::Local);
    }

    #[test]
    fn empty_config_defaults_to_local_and_default_data_dir() {
        let cfg = Config::from_toml_str("").unwrap();
        assert_eq!(cfg.backend, BackendKind::Local);
        assert_eq!(cfg.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(cfg.listen, default_listen());
    }

    #[test]
    fn invalid_backend_is_a_clear_config_error() {
        let err = Config::from_toml_str("[storage]\nbackend = \"s3\"\n").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("[storage]"), "{msg}");
        assert!(msg.contains("\"s3\""), "{msg}");
        assert!(msg.contains("local") && msg.contains("memory"), "{msg}");
    }

    #[test]
    fn unknown_sections_are_ignored() {
        // A forward-compatible file with knobs this ticket doesn't model yet.
        let toml = "[storage]\nbackend = \"memory\"\n\n[wal]\nfsync = \"on_commit\"\n\n[telemetry]\ntracing = \"info\"\n";
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.backend, BackendKind::Memory);
    }

    #[test]
    fn the_committed_sample_config_parses_to_the_documented_defaults() {
        // The shipped `stele.example.toml` must always load — this guards it from
        // silently drifting out of sync with the parser (STL-208). It uses the
        // documented defaults, so a config-file run resolves to exactly them
        // (non-dev, local backend, default listen + data_dir).
        let cfg = Config::from_toml_str(include_str!("../../../stele.example.toml"))
            .expect("stele.example.toml must parse");
        assert!(!cfg.dev, "a config-file run is never dev mode");
        assert_eq!(cfg.backend, BackendKind::Local);
        assert_eq!(cfg.listen, default_listen());
        assert_eq!(cfg.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
    }

    #[test]
    fn dev_defaults_to_local_in_scratch() {
        let cfg = Config::dev();
        assert!(cfg.dev);
        assert_eq!(cfg.backend, BackendKind::Local);
    }

    #[test]
    fn log_format_selects_json_only_for_the_json_value() {
        // The one value that opts into production JSON, including casing/padding
        // a shell or orchestrator might introduce.
        assert_eq!(LogFormat::parse(Some("json")), LogFormat::Json);
        assert_eq!(LogFormat::parse(Some("JSON")), LogFormat::Json);
        assert_eq!(LogFormat::parse(Some("  json  ")), LogFormat::Json);
    }

    #[test]
    fn log_format_falls_back_to_text() {
        // Unset, empty, and unrecognized all degrade to readable text rather
        // than silently swallowing logs.
        assert_eq!(LogFormat::parse(None), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("text")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("pretty")), LogFormat::Text);
    }

    #[test]
    fn default_filter_is_verbose_in_dev_quiet_otherwise() {
        assert_eq!(default_filter(true), "info,stele=debug");
        assert_eq!(default_filter(false), "info");
    }
}
