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

pub mod ops;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::Context as _;
use serde::Deserialize;
use stele_common::DEFAULT_PG_PORT;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{AuthMode, Server as PgServer, ServerTls, SharedSession, TlsMode, TlsSettings};
use stele_storage::backend::{AnyDisk, BackendKind};
use tokio::signal;
use tracing::{info, warn};

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
    /// Ops HTTP listen address — `/metrics`, `/healthz`, `/readyz` ([STL-253]),
    /// the listener the admin HTTP gateway will share ([ADR-0016]). Configured
    /// by `[telemetry] metrics` (the shape docs/05 reserved); defaults to
    /// `0.0.0.0:9090`.
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    /// [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md
    pub metrics_listen: SocketAddr,
    /// TLS on pg-wire ([STL-251]): certificate material + plaintext policy from
    /// the `[tls]` section. `None` = TLS not configured — the secure-defaults
    /// posture then decides at boot whether the server may start at all
    /// (plaintext is loopback-only for non-dev runs).
    ///
    /// [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
    pub tls: Option<TlsSettings>,
    /// pg-wire authentication ([STL-252]): the `[auth]` section's mode.
    /// [`AuthMode::Trust`] when the section is absent (and always in dev);
    /// an `[auth]` section defaults to `"scram"` — configuring authentication
    /// means wanting it, the same secure-defaults posture as `[tls]`.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    pub auth: AuthMode,
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
            metrics_listen: default_metrics_listen(),
            tls: None,
            auth: AuthMode::Trust,
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
    #[serde(default)]
    telemetry: TelemetrySection,
    tls: Option<TlsSection>,
    auth: Option<AuthSection>,
}

#[derive(Debug, Default, Deserialize)]
struct ServerSection {
    listen: Option<SocketAddr>,
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct TelemetrySection {
    /// The ops HTTP listen address (`/metrics`, `/healthz`, `/readyz`) — the
    /// `[telemetry] metrics` key from docs/05, active since [STL-253].
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    metrics: Option<SocketAddr>,
}

#[derive(Debug, Default, Deserialize)]
struct StorageSection {
    /// Kept as a raw string so the parse error (and its clear message) is owned
    /// by [`BackendKind`]'s [`FromStr`](std::str::FromStr), not serde.
    backend: Option<String>,
}

/// The `[tls]` section ([STL-251]): certificate material plus the plaintext
/// policy. `mode` defaults to `"required"` — configuring TLS means wanting it,
/// so the lenient posture (`"optional"`) is the explicit opt-in, not the default.
///
/// [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
#[derive(Debug, Default, Deserialize)]
struct TlsSection {
    /// `"required"` (default) | `"optional"` | `"disabled"`.
    mode: Option<String>,
    /// PEM server certificate chain.
    cert: Option<PathBuf>,
    /// PEM private key.
    key: Option<PathBuf>,
    /// PEM client CA — setting it switches on **mTLS** (every client must
    /// present a certificate chaining to it).
    client_ca: Option<PathBuf>,
}

impl TlsSection {
    fn resolve(self) -> anyhow::Result<Option<TlsSettings>> {
        let mode = match self.mode.as_deref().unwrap_or("required") {
            "required" => TlsMode::Required,
            "optional" => TlsMode::Optional,
            // The whole section is inert — handy for keeping the cert paths in
            // the file while temporarily turning TLS off.
            "disabled" => return Ok(None),
            other => anyhow::bail!(
                "[tls] mode {other:?} is not recognized (expected \"required\", \
                 \"optional\", or \"disabled\")"
            ),
        };
        let (Some(cert), Some(key)) = (self.cert, self.key) else {
            anyhow::bail!("[tls] requires both `cert` and `key` (PEM file paths)");
        };
        Ok(Some(TlsSettings {
            cert,
            key,
            client_ca: self.client_ca,
            mode,
        }))
    }
}

/// The `[auth]` section ([STL-252]): pg-wire authentication. `mode` defaults
/// to `"scram"` — configuring authentication means wanting it, the same
/// posture as `[tls]` defaulting to `"required"`.
///
/// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
#[derive(Debug, Default, Deserialize)]
struct AuthSection {
    /// `"scram"` (default) | `"trust"`.
    mode: Option<String>,
}

impl AuthSection {
    fn resolve(self) -> anyhow::Result<AuthMode> {
        match self.mode.as_deref().unwrap_or("scram") {
            "scram" => Ok(AuthMode::Scram),
            // Explicitly configured trust — handy for keeping the section in
            // the file while bootstrapping the first user.
            "trust" => Ok(AuthMode::Trust),
            other => anyhow::bail!(
                "[auth] mode {other:?} is not recognized (expected \"scram\" or \"trust\")"
            ),
        }
    }
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
            metrics_listen: self
                .telemetry
                .metrics
                .unwrap_or_else(default_metrics_listen),
            tls: self.tls.map(TlsSection::resolve).transpose()?.flatten(),
            auth: self
                .auth
                .map_or(Ok(AuthMode::Trust), AuthSection::resolve)?,
        })
    }
}

/// What boot does about plaintext, per the secure-defaults posture
/// ([docs/10 §4](../../../docs/10-security-and-compliance.md#4-data-protection--encryption), STL-251).
#[derive(Debug, PartialEq, Eq)]
enum PlaintextPosture {
    /// Start silently.
    Proceed,
    /// Start, but say loudly what is unencrypted and how to fix it.
    Warn(String),
    /// Don't start: this configuration would silently serve plaintext beyond
    /// the local machine.
    Refuse(String),
}

/// Decide the plaintext posture for `cfg`.
///
/// * `--dev` is friction-free: plaintext is the documented five-minute path.
/// * Non-dev with `tls = "required"`: nothing is plaintext — proceed.
/// * Non-dev with `tls = "optional"`: warn — plaintext clients are accepted.
/// * Non-dev **without TLS**: plaintext on loopback warns; a non-loopback bind
///   is refused outright. There is no silent-plaintext production posture —
///   the operator either configures `[tls]`, binds loopback, or runs `--dev`.
fn plaintext_posture(cfg: &Config) -> PlaintextPosture {
    if cfg.dev {
        return PlaintextPosture::Proceed;
    }
    match &cfg.tls {
        Some(tls) if tls.mode == TlsMode::Required => PlaintextPosture::Proceed,
        Some(_) => PlaintextPosture::Warn(
            "[tls] mode = \"optional\": plaintext connections are still accepted; \
             set mode = \"required\" once clients are migrated"
                .to_owned(),
        ),
        None if cfg.listen.ip().is_loopback() => PlaintextPosture::Warn(format!(
            "no [tls] configured: pg-wire on {} is PLAINTEXT (loopback-only); \
             configure [tls] before exposing this server",
            cfg.listen
        )),
        None => PlaintextPosture::Refuse(format!(
            "refusing to listen on non-loopback {} without TLS: a production \
             server must not silently serve plaintext (docs/10 §4). Configure \
             a [tls] section in stele.toml, bind a loopback address, or run \
             --dev for local development",
            cfg.listen
        )),
    }
}

/// The boot-time authentication warning for `cfg`, if any ([STL-252]).
///
/// A non-dev server running `trust` accepts any startup as any identity, so it
/// warns — unless mTLS is on, where the client certificate *is* the
/// authentication story. Dev stays friction-free, exactly like the plaintext
/// posture.
fn auth_posture(cfg: &Config) -> Option<String> {
    let mtls = cfg.tls.as_ref().is_some_and(|tls| tls.client_ca.is_some());
    (!cfg.dev && cfg.auth == AuthMode::Trust && !mtls).then(|| {
        "no [auth] configured: pg-wire connections are UNAUTHENTICATED (trust); \
         configure [auth] mode = \"scram\" (and CREATE USER) before exposing this server"
            .to_owned()
    })
}

/// Boot the engine.
///
/// Install tracing, apply the plaintext posture, start the ops HTTP listener,
/// construct the configured storage backend, recover the session engine,
/// start the pgwire listener, wait for SIGINT.
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    init_tracing(cfg.dev);
    info!(?cfg, "stele-server: starting");

    // Secure defaults (STL-251, docs/10 §4): decide up front what this
    // configuration means for plaintext — and refuse outright rather than
    // silently serve unencrypted traffic beyond the local machine. Checked
    // before any port is bound, so a refused config holds nothing.
    match plaintext_posture(&cfg) {
        PlaintextPosture::Proceed => {}
        PlaintextPosture::Warn(msg) => warn!("{msg}"),
        PlaintextPosture::Refuse(msg) => anyhow::bail!(msg),
    }
    // The authentication half of the same posture (STL-252).
    if let Some(msg) = auth_posture(&cfg) {
        warn!("{msg}");
    }

    // Stand the ops HTTP listener up FIRST, before recovery runs (STL-253):
    // `/healthz` answers as soon as the process holds the port, while
    // `/readyz` reports 503 until recovery completes — the flip orchestrators
    // key their traffic-routing on.
    let ops_state = Arc::new(ops::OpsState::new());
    let ops = ops::OpsServer::new(cfg.metrics_listen, Arc::clone(&ops_state))
        .bind()
        .await
        .with_context(|| format!("binding ops listener on {}", cfg.metrics_listen))?;
    let mut ops_task = tokio::spawn(ops.serve());

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
        users = engine.user_count(),
        "session engine ready"
    );
    // SCRAM with an empty user store refuses every connection (STL-252) —
    // boot anyway (the config is coherent), but say loudly how to get in.
    if cfg.auth == AuthMode::Scram && engine.user_count() == 0 {
        warn!(
            "[auth] mode = \"scram\" but no users exist: every connection will be \
             refused. Bootstrap: boot once without [auth] (trust, loopback or TLS), \
             run CREATE USER <name> PASSWORD '<password>', then re-enable [auth] — \
             verifiers are durable and survive the restart"
        );
    }
    // Give the registry real durations (STL-253). Only the production server
    // installs a time source — see [`uptime_micros`].
    engine.metrics().install_time_source(uptime_micros);

    // One engine shared across every connection, behind a mutex (STL-131): a
    // CREATE TABLE on any connection is visible to the next statement, and the
    // pgwire loop now routes DDL through `engine.execute` (table reads / DML
    // follow in STL-147). The lock is held only per synchronous statement,
    // never across wire I/O.
    let session: SharedSession = Arc::new(Mutex::new(engine));
    // Recovery is complete: flip `/readyz` (STL-253). From here it tracks the
    // engine's WAL-poison state live.
    ops_state.set_ready(Arc::clone(&session));
    let mut pg = PgServer::new(cfg.listen, session).with_auth(cfg.auth);
    if cfg.auth == AuthMode::Scram {
        info!("SCRAM-SHA-256 authentication enabled on pg-wire");
    }
    if let Some(settings) = &cfg.tls {
        // Load certificate material now so a bad path / non-PEM file is a boot
        // error with context, not a per-connection surprise.
        let tls = ServerTls::load(settings).context("loading [tls] certificate material")?;
        info!(
            mode = ?settings.mode,
            mtls = settings.client_ca.is_some(),
            "TLS enabled on pg-wire"
        );
        pg = pg.with_tls(tls);
    }

    tokio::select! {
        res = pg.run() => res.context("pgwire listener exited")?,
        // The ops listener only exits on a bind/served I/O failure or a panic —
        // either way the operator surface is gone, so treat it as fatal rather
        // than serving on half a contract.
        res = &mut ops_task => res.context("ops listener task aborted")?.context("ops listener exited")?,
        _ = signal::ctrl_c() => {
            info!("received SIGINT, shutting down");
        }
    }

    Ok(())
}

const fn default_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PG_PORT)
}

/// Stele's default ops/metrics HTTP port — `9090`, the conventional
/// Prometheus-ecosystem scrape port documented in
/// [05 — configuration](../../../docs/05-dev-environment.md#configuration).
const DEFAULT_METRICS_PORT: u16 = 9090;

const fn default_metrics_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_METRICS_PORT)
}

/// Microseconds since the first call — the monotonic time source the server
/// installs on the engine's metric registry
/// ([`Metrics::install_time_source`](stele_common::metrics::Metrics::install_time_source)).
/// Only the production server installs one; tests and the simulator leave the
/// registry sourceless (all durations zero), which is what keeps the
/// deterministic core clock-free ([ADR-0010]).
///
/// [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
fn uptime_micros() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(Instant::now);
    u64::try_from(epoch.elapsed().as_micros()).unwrap_or(u64::MAX)
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
    fn telemetry_metrics_listen_is_parsed() {
        let cfg = Config::from_toml_str("[telemetry]\nmetrics = \"127.0.0.1:9988\"\n").unwrap();
        assert_eq!(cfg.metrics_listen, "127.0.0.1:9988".parse().unwrap());
    }

    #[test]
    fn metrics_listen_defaults_to_the_documented_port() {
        // Both an empty operator config and dev mode land on 0.0.0.0:9090
        // (docs/05 — configuration).
        let cfg = Config::from_toml_str("").unwrap();
        assert_eq!(cfg.metrics_listen, default_metrics_listen());
        assert_eq!(Config::dev().metrics_listen, default_metrics_listen());
        assert_eq!(default_metrics_listen().port(), 9090);
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
        // silently drifting out of sync with the parser (STL-208). It binds
        // loopback (the [tls] section is commented out, and the secure-defaults
        // posture refuses a plaintext non-loopback bind — STL-251), so a
        // config-file run of the example boots out of the box.
        let cfg = Config::from_toml_str(include_str!("../../../stele.example.toml"))
            .expect("stele.example.toml must parse");
        assert!(!cfg.dev, "a config-file run is never dev mode");
        assert_eq!(cfg.backend, BackendKind::Local);
        assert_eq!(cfg.listen, "127.0.0.1:5454".parse().unwrap());
        assert_eq!(cfg.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(cfg.metrics_listen, default_metrics_listen());
        assert!(
            cfg.tls.is_none(),
            "the example ships with [tls] commented out"
        );
        assert_eq!(
            cfg.auth,
            AuthMode::Trust,
            "the example ships with [auth] commented out"
        );
        assert!(
            !matches!(plaintext_posture(&cfg), PlaintextPosture::Refuse(_)),
            "the committed example must boot"
        );
    }

    #[test]
    fn dev_defaults_to_local_in_scratch() {
        let cfg = Config::dev();
        assert!(cfg.dev);
        assert_eq!(cfg.backend, BackendKind::Local);
        assert!(cfg.tls.is_none(), "dev mode does not configure TLS");
    }

    // --- [tls] section (STL-251) -------------------------------------------

    #[test]
    fn tls_section_parses_paths_and_defaults_to_required() {
        let toml = "[tls]\ncert = \"/etc/stele/server.crt\"\nkey = \"/etc/stele/server.key\"\n";
        let tls = Config::from_toml_str(toml)
            .unwrap()
            .tls
            .expect("tls configured");
        assert_eq!(tls.cert, PathBuf::from("/etc/stele/server.crt"));
        assert_eq!(tls.key, PathBuf::from("/etc/stele/server.key"));
        assert_eq!(
            tls.mode,
            TlsMode::Required,
            "configuring TLS means wanting it"
        );
        assert!(tls.client_ca.is_none(), "mTLS is opt-in");
    }

    #[test]
    fn tls_optional_mode_and_client_ca_parse() {
        let toml = "[tls]\nmode = \"optional\"\ncert = \"c.pem\"\nkey = \"k.pem\"\nclient_ca = \"ca.pem\"\n";
        let tls = Config::from_toml_str(toml)
            .unwrap()
            .tls
            .expect("tls configured");
        assert_eq!(tls.mode, TlsMode::Optional);
        assert_eq!(tls.client_ca, Some(PathBuf::from("ca.pem")));
    }

    #[test]
    fn tls_disabled_mode_turns_the_section_inert() {
        // The operator keeps the cert paths in the file but switches TLS off.
        let toml = "[tls]\nmode = \"disabled\"\ncert = \"c.pem\"\nkey = \"k.pem\"\n";
        assert!(Config::from_toml_str(toml).unwrap().tls.is_none());
    }

    #[test]
    fn tls_section_without_cert_or_key_is_a_clear_config_error() {
        let err = Config::from_toml_str("[tls]\ncert = \"c.pem\"\n").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("[tls]"), "{msg}");
        assert!(msg.contains("cert") && msg.contains("key"), "{msg}");
    }

    #[test]
    fn tls_unknown_mode_is_a_clear_config_error() {
        let err = Config::from_toml_str("[tls]\nmode = \"prefer\"\ncert = \"c\"\nkey = \"k\"\n")
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("\"prefer\""), "{msg}");
        assert!(
            msg.contains("required") && msg.contains("optional"),
            "{msg}"
        );
    }

    #[test]
    fn no_tls_section_means_no_tls() {
        assert!(Config::from_toml_str("").unwrap().tls.is_none());
    }

    // --- [auth] section (STL-252) -------------------------------------------

    #[test]
    fn no_auth_section_means_trust() {
        assert_eq!(Config::from_toml_str("").unwrap().auth, AuthMode::Trust);
        assert_eq!(Config::dev().auth, AuthMode::Trust, "dev stays auth-free");
    }

    #[test]
    fn auth_section_defaults_to_scram() {
        // Configuring authentication means wanting it — the bare section is
        // the production posture, like [tls] defaulting to "required".
        assert_eq!(
            Config::from_toml_str("[auth]\n").unwrap().auth,
            AuthMode::Scram
        );
        assert_eq!(
            Config::from_toml_str("[auth]\nmode = \"scram\"\n")
                .unwrap()
                .auth,
            AuthMode::Scram
        );
    }

    #[test]
    fn auth_trust_mode_is_the_explicit_opt_out() {
        assert_eq!(
            Config::from_toml_str("[auth]\nmode = \"trust\"\n")
                .unwrap()
                .auth,
            AuthMode::Trust
        );
    }

    #[test]
    fn auth_unknown_mode_is_a_clear_config_error() {
        let err = Config::from_toml_str("[auth]\nmode = \"md5\"\n").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("\"md5\""), "{msg}");
        assert!(msg.contains("scram") && msg.contains("trust"), "{msg}");
    }

    #[test]
    fn auth_posture_warns_on_non_dev_trust_without_mtls() {
        // Non-dev trust is a warning…
        let cfg = non_dev("127.0.0.1:5454", None);
        let msg = auth_posture(&cfg).expect("trust warns");
        assert!(msg.contains("UNAUTHENTICATED"), "{msg}");
        assert!(msg.contains("scram"), "{msg}");

        // …dev is friction-free, scram is silent…
        assert_eq!(auth_posture(&Config::dev()), None);
        let mut scram = non_dev("127.0.0.1:5454", None);
        scram.auth = AuthMode::Scram;
        assert_eq!(auth_posture(&scram), None);

        // …and mTLS counts as authentication (the client cert is the identity).
        let mut mtls = non_dev("0.0.0.0:5454", Some(TlsMode::Required));
        if let Some(tls) = mtls.tls.as_mut() {
            tls.client_ca = Some(PathBuf::from("ca.pem"));
        }
        assert_eq!(auth_posture(&mtls), None);
    }

    // --- secure-defaults posture (STL-251, docs/10 §4) -----------------------

    fn non_dev(listen: &str, tls: Option<TlsMode>) -> Config {
        let mut cfg = Config::from_toml_str("").unwrap();
        cfg.listen = listen.parse().unwrap();
        cfg.tls = tls.map(|mode| TlsSettings {
            cert: PathBuf::from("c.pem"),
            key: PathBuf::from("k.pem"),
            client_ca: None,
            mode,
        });
        cfg
    }

    #[test]
    fn dev_mode_is_friction_free_even_off_loopback() {
        let mut cfg = Config::dev();
        cfg.listen = "0.0.0.0:5454".parse().unwrap();
        assert_eq!(plaintext_posture(&cfg), PlaintextPosture::Proceed);
    }

    #[test]
    fn non_dev_without_tls_refuses_a_non_loopback_bind() {
        let posture = plaintext_posture(&non_dev("0.0.0.0:5454", None));
        let PlaintextPosture::Refuse(msg) = posture else {
            panic!("expected refusal, got {posture:?}");
        };
        // The message must hand the operator every way out.
        assert!(msg.contains("[tls]"), "{msg}");
        assert!(msg.contains("loopback"), "{msg}");
        assert!(msg.contains("--dev"), "{msg}");
    }

    #[test]
    fn non_dev_without_tls_warns_on_loopback() {
        let posture = plaintext_posture(&non_dev("127.0.0.1:5454", None));
        let PlaintextPosture::Warn(msg) = posture else {
            panic!("expected warning, got {posture:?}");
        };
        assert!(msg.contains("PLAINTEXT"), "{msg}");
    }

    #[test]
    fn non_dev_with_required_tls_proceeds_silently() {
        let cfg = non_dev("0.0.0.0:5454", Some(TlsMode::Required));
        assert_eq!(plaintext_posture(&cfg), PlaintextPosture::Proceed);
    }

    #[test]
    fn non_dev_with_optional_tls_warns_about_plaintext_clients() {
        let posture = plaintext_posture(&non_dev("0.0.0.0:5454", Some(TlsMode::Optional)));
        let PlaintextPosture::Warn(msg) = posture else {
            panic!("expected warning, got {posture:?}");
        };
        assert!(msg.contains("optional"), "{msg}");
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
