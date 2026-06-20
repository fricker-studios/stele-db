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

pub mod admin;
pub mod ops;
pub mod tls;

use std::fmt;
use std::future::{self, Future};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::Context as _;
use serde::Deserialize;
use stele_common::DEFAULT_PG_PORT;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{
    AuthMode, Server as PgServer, ServerTls, SharedSession, TlsMode, TlsReloader, TlsSettings,
};
use stele_storage::backend::{AnyDisk, BackendKind};
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, warn};

use crate::admin::{AdminAuth, AdminService};
use crate::tls::AcceptorSource;

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
    /// posture then decides at boot what plaintext means for this bind: loopback
    /// serves plaintext (with a warning), and a non-loopback bind gets an
    /// ephemeral self-signed certificate ([STL-304]) so the listener is
    /// encrypted rather than plaintext-beyond-loopback.
    ///
    /// [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
    /// [STL-304]: https://allegromusic.atlassian.net/browse/STL-304
    pub tls: Option<TlsSettings>,
    /// pg-wire authentication ([STL-252]): the `[auth]` section's mode.
    /// [`AuthMode::Trust`] when the section is absent (and always in dev);
    /// an `[auth]` section defaults to `"scram"` — configuring authentication
    /// means wanting it, the same secure-defaults posture as `[tls]`.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    pub auth: AuthMode,
    /// Admin / control-plane API bearer tokens ([STL-254], ADR-0016): the
    /// `[admin] tokens` list. **Secure default** — empty means the admin API
    /// authorizes nothing (the gRPC listener is not bound and the HTTP gateway
    /// rejects every request), so configuring a token is what turns the surface
    /// on. Never logged.
    ///
    /// [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
    pub admin_tokens: AdminTokens,
    /// The admin gRPC listen address ([STL-254]): `[admin] grpc_listen`. Only
    /// bound when at least one token is configured. The HTTP/JSON gateway shares
    /// the ops listener ([`metrics_listen`](Self::metrics_listen)) instead of
    /// taking a port of its own. Default `0.0.0.0:5455`.
    pub admin_grpc_listen: SocketAddr,
}

/// Admin bearer tokens, with a [`Debug`] that prints only their count — the
/// secrets must never reach a log line (the `info!(?cfg)` at boot, [STL-254]).
#[derive(Clone, Default)]
pub struct AdminTokens(Vec<String>);

impl AdminTokens {
    /// The configured tokens.
    #[must_use]
    pub fn tokens(&self) -> &[String] {
        &self.0
    }
}

impl fmt::Debug for AdminTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AdminTokens(<{} configured>)", self.0.len())
    }
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
            // Dev is auth-free for pg-wire, but the admin API stays token-gated
            // even in dev: no tokens means the surface is off, never open.
            admin_tokens: AdminTokens::default(),
            admin_grpc_listen: default_admin_grpc_listen(),
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
    admin: Option<AdminSection>,
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

/// The `[admin]` section ([STL-254], ADR-0016): the admin / control-plane API's
/// bearer tokens and gRPC listen address. Configuring a token **enables** the
/// surface; with none, the API rejects every request (secure default), like
/// `[auth]`/`[tls]` defaulting to their strict modes.
///
/// [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
#[derive(Debug, Default, Deserialize)]
struct AdminSection {
    /// Static bearer tokens accepted on the admin surface. Never logged.
    tokens: Option<Vec<String>>,
    /// The gRPC listen address (default `0.0.0.0:5455`); only bound when a token
    /// is configured.
    grpc_listen: Option<SocketAddr>,
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
        let admin = self.admin.unwrap_or_default();
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
            admin_tokens: AdminTokens(admin.tokens.unwrap_or_default()),
            admin_grpc_listen: admin.grpc_listen.unwrap_or_else(default_admin_grpc_listen),
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
    /// This configuration would otherwise serve plaintext beyond the local
    /// machine. Rather than refuse to boot, generate an ephemeral self-signed
    /// certificate so the listener is encrypted (TLS `required`) and warn — the
    /// cert is unauthenticated and should be replaced ([STL-304]). The string
    /// is the warning the operator must see.
    ///
    /// [STL-304]: https://allegromusic.atlassian.net/browse/STL-304
    GenerateSelfSigned(String),
}

/// Decide the plaintext posture for `cfg`.
///
/// * `--dev` is friction-free: plaintext is the documented five-minute path.
/// * Non-dev with `tls = "required"`: nothing is plaintext — proceed.
/// * Non-dev with `tls = "optional"`: warn — plaintext clients are accepted.
/// * Non-dev **without TLS**: plaintext on loopback warns; a non-loopback bind
///   generates an ephemeral self-signed certificate (encryption without
///   authentication) and warns, rather than refusing to boot ([STL-304]). There
///   is still no *silent*-plaintext production posture — the listener is either
///   encrypted or loopback-only.
///
/// [STL-304]: https://allegromusic.atlassian.net/browse/STL-304
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
        None => PlaintextPosture::GenerateSelfSigned(format!(
            "no [tls] configured: generated an ephemeral self-signed certificate \
             for the non-loopback bind {}. Connections are ENCRYPTED but NOT \
             authenticated — clients cannot verify this server, and the \
             certificate is regenerated on every restart. Configure a [tls] \
             section with a CA-issued cert before production (docs/10 §4)",
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

/// The TLS material the daemon serves on, shared by pg-wire and the admin gRPC +
/// ops HTTP transports ([STL-311]). Two shapes, both turned into per-connection
/// acceptors the listeners read live:
///
/// * [`Reloading`](Self::Reloading) — operator `[tls]` material behind a
///   [`TlsReloader`], so SIGHUP rotation ([STL-293]) reaches every surface.
/// * [`SelfSigned`](Self::SelfSigned) — the ephemeral certificate the
///   secure-defaults fallback mints ([STL-304]); fixed for the process lifetime.
///
/// [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
enum TlsContext {
    /// Hot-reloadable operator-supplied material from `[tls]`.
    Reloading(TlsReloader),
    /// An ephemeral self-signed certificate (encryption without authentication).
    SelfSigned(ServerTls),
}

impl TlsContext {
    /// An [`AcceptorSource`] over this context for the admin gRPC + ops HTTP
    /// accept loops. Reading the acceptor per connection means the reloading
    /// variant picks up rotations there too.
    fn acceptor_source(&self) -> AcceptorSource {
        match self {
            Self::Reloading(reloader) => AcceptorSource::reloading(reloader),
            Self::SelfSigned(server_tls) => AcceptorSource::fixed(server_tls),
        }
    }
}

/// Resolve the single TLS context for the whole daemon ([STL-311]).
///
/// * `[tls]` configured → load it behind a [`TlsReloader`] (errors as a boot
///   failure with context, never a per-connection surprise).
/// * No `[tls]`, non-dev, and a secret-carrying surface bound beyond loopback →
///   mint an ephemeral self-signed certificate ([STL-304]). pg-wire's own
///   decision is `pg_posture`; [`admin_wants_self_signed`] adds the admin surface.
/// * Otherwise → `None` (plaintext: dev, or a loopback-only bind).
fn resolve_tls(cfg: &Config, pg_posture: &PlaintextPosture) -> anyhow::Result<Option<TlsContext>> {
    if let Some(settings) = &cfg.tls {
        let reloader =
            TlsReloader::load(settings.clone()).context("loading [tls] certificate material")?;
        info!(
            mode = ?settings.mode,
            mtls = settings.client_ca.is_some(),
            "TLS enabled on pg-wire and the admin surface"
        );
        return Ok(Some(TlsContext::Reloading(reloader)));
    }
    let pg_wants = matches!(pg_posture, PlaintextPosture::GenerateSelfSigned(_));
    if !cfg.dev && (pg_wants || admin_wants_self_signed(cfg)) {
        let server_tls = ServerTls::self_signed(TlsMode::Required)
            .context("generating a self-signed TLS certificate")?;
        info!(
            mode = ?TlsMode::Required,
            "self-signed TLS enabled on pg-wire and the admin surface (ephemeral certificate)"
        );
        return Ok(Some(TlsContext::SelfSigned(server_tls)));
    }
    Ok(None)
}

/// Whether the admin surface, were it left plaintext, would carry bearer tokens
/// beyond loopback — the trigger for extending the self-signed fallback to it
/// ([STL-311]). Only when the API is actually enabled (a token configured): a
/// disabled surface carries no secret, and the ops listener then serves only
/// metrics/probes, which need no certificate. Caller guarantees non-dev + no
/// `[tls]`.
fn admin_wants_self_signed(cfg: &Config) -> bool {
    if cfg.admin_tokens.tokens().is_empty() {
        return false;
    }
    // Tokens ride the admin gRPC listener and the `/v1alpha1` gateway on the ops
    // listener; either one bound off-loopback exposes them.
    !cfg.admin_grpc_listen.ip().is_loopback() || !cfg.metrics_listen.ip().is_loopback()
}

/// Boot the engine.
///
/// Install tracing, apply the plaintext posture, start the ops HTTP listener,
/// construct the configured storage backend, recover the session engine,
/// start the pgwire + admin listeners, wait for SIGINT.
// Boot orchestration is inherently a long sequential wiring of independent
// subsystems (tracing, secure-defaults posture, ops/pg/admin listeners,
// recovery); the slim per-subsystem helpers are already extracted, so the
// residual branchiness is irreducible "compose the daemon" glue.
#[allow(clippy::cognitive_complexity)]
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    init_tracing(cfg.dev);
    info!(?cfg, "stele-server: starting");

    // Secure defaults (STL-251, STL-304, docs/10 §4): decide up front what this
    // configuration means for plaintext. A non-dev server without [tls] on a
    // non-loopback bind mints an ephemeral self-signed certificate (encryption
    // without authentication) rather than refusing to boot or silently serving
    // unencrypted traffic.
    let pg_posture = plaintext_posture(&cfg);
    match &pg_posture {
        PlaintextPosture::Proceed => {}
        PlaintextPosture::Warn(msg) | PlaintextPosture::GenerateSelfSigned(msg) => warn!("{msg}"),
    }
    // The authentication half of the same posture (STL-252).
    if let Some(msg) = auth_posture(&cfg) {
        warn!("{msg}");
    }

    // One TLS context, shared by pg-wire and the admin gRPC + ops HTTP transports
    // (STL-311): the admin surface reuses the pg-wire `[tls]` certificate material
    // — and the self-signed fallback — rather than a second cert config. Resolved
    // (and any ephemeral certificate minted) before any port is bound, so a
    // generation failure holds nothing open.
    let tls = resolve_tls(&cfg, &pg_posture)?;
    if let Some(TlsContext::Reloading(reloader)) = &tls {
        // Arm SIGHUP hot-reload once; pg-wire, the admin gRPC listener, and the ops
        // listener all read the same reloader cell, so one rotation reaches them all.
        spawn_tls_sighup_reload(reloader.clone());
    }
    let admin_tls = tls.as_ref().map(TlsContext::acceptor_source);

    // Stand the ops HTTP listener up FIRST, before recovery runs (STL-253):
    // `/healthz` answers as soon as the process holds the port, while
    // `/readyz` reports 503 until recovery completes — the flip orchestrators
    // key their traffic-routing on.
    let ops_state = Arc::new(ops::OpsState::new());
    let ops = ops::OpsServer::new(cfg.metrics_listen, Arc::clone(&ops_state))
        // Encrypt the ops listener (metrics, probes, and the /v1alpha1 admin
        // gateway) with the shared material when TLS is active (STL-311).
        .with_tls(admin_tls.clone())
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
    // never across wire I/O. The admin / control-plane API ([STL-254]) shares
    // this very engine: a typed handle for the admin core, the same handle
    // coerced to the `SharedSession` trait object pg-wire and ops use.
    let engine: Arc<Mutex<SessionEngine<SystemClock, AnyDisk>>> = Arc::new(Mutex::new(engine));
    let admin_auth = Arc::new(AdminAuth::new(cfg.admin_tokens.tokens().to_vec()));
    let admin_core = AdminService::new(Arc::clone(&engine));
    let session: SharedSession = engine;
    // Recovery is complete: flip `/readyz` (STL-253). From here it tracks the
    // engine's WAL-poison state live.
    ops_state.set_ready(Arc::clone(&session));
    // Mount the admin HTTP/JSON gateway on the ops listener — always present
    // (it authenticates every request, rejecting all when no token is set).
    ops_state.set_admin(Arc::new(admin::http::AdminHttp::new(
        admin_core.clone(),
        Arc::clone(&admin_auth),
    )));
    let mut pg = PgServer::new(cfg.listen, session).with_auth(cfg.auth);
    if cfg.auth == AuthMode::Scram {
        info!("SCRAM-SHA-256 authentication enabled on pg-wire");
    }
    // Apply the shared TLS context to pg-wire. The reloader path keeps SIGHUP
    // hot-reload (STL-293); the self-signed path is the STL-304 fallback.
    match &tls {
        Some(TlsContext::Reloading(reloader)) => pg = pg.with_tls_reloader(reloader),
        Some(TlsContext::SelfSigned(server_tls)) => pg = pg.with_tls(server_tls.clone()),
        None => {}
    }

    // The admin / control-plane gRPC listener ([STL-254], ADR-0016) — bound only
    // when a token is configured (secure default: no token ⇒ no surface). The
    // HTTP/JSON gateway is already live on the ops listener regardless. Both reuse
    // `admin_tls`, the same material pg-wire just took (STL-311).
    let admin_grpc = build_admin_grpc(&cfg, admin_core, admin_auth, admin_tls).await?;

    tokio::select! {
        res = pg.run() => res.context("pgwire listener exited")?,
        // The ops listener only exits on a bind/served I/O failure or a panic —
        // either way the operator surface is gone, so treat it as fatal rather
        // than serving on half a contract.
        res = &mut ops_task => res.context("ops listener task aborted")?.context("ops listener exited")?,
        // The admin gRPC listener (when enabled) is the same kind of operator
        // surface; its exit is fatal too. When disabled this never resolves.
        res = admin_grpc => res?,
        _ = signal::ctrl_c() => {
            info!("received SIGINT, shutting down");
        }
    }

    Ok(())
}

/// Arm TLS certificate hot-reload on SIGHUP ([STL-293]): spawn a task that, on
/// each `SIGHUP`, re-reads the `[tls]` cert/key paths and atomically swaps the
/// acceptor the pg-wire listener reads per connection. A failed reload (torn
/// write, non-PEM file, cert/key mismatch) keeps the previously loaded certificate
/// and is logged loudly — it never takes the listener down.
///
/// SIGHUP is the conventional "reload config without restart" signal (nginx /
/// PostgreSQL `pg_ctl reload`); cert-manager and similar rotators can be wired to
/// send it. The task lives for the lifetime of the process and is dropped on
/// shutdown.
///
/// [STL-293]: https://allegromusic.atlassian.net/browse/STL-293
#[cfg(unix)]
fn spawn_tls_sighup_reload(reloader: TlsReloader) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut hup = match signal(SignalKind::hangup()) {
        Ok(stream) => stream,
        Err(error) => {
            warn!(
                %error,
                "could not install the SIGHUP handler; TLS hot-reload is disabled \
                 (rotate the certificate with a restart)"
            );
            return;
        }
    };
    tokio::spawn(async move {
        info!(
            "TLS hot-reload armed: send SIGHUP (e.g. `kill -HUP <pid>`) to pick up a \
             rotated [tls] cert/key without a restart"
        );
        // `recv` yields `Some` per delivered signal and `None` only if the stream is
        // dropped; the reloader logs the success / failure of each attempt itself.
        while hup.recv().await.is_some() {
            info!("received SIGHUP: reloading [tls] certificate material");
            // `reload()` does blocking file I/O + rustls config building, so run it
            // on the blocking pool rather than the async worker — SIGHUP is
            // out-of-band, so the detour is free. A `JoinError` (the blocking task
            // panicking) is ignored: `reload()` returns its errors, it never panics.
            let reloader = reloader.clone();
            let _ = tokio::task::spawn_blocking(move || reloader.reload()).await;
        }
    });
}

/// On a platform without SIGHUP, hot-reload is unavailable: the listener keeps
/// serving the certificate loaded at boot, and rotation needs a restart.
#[cfg(not(unix))]
fn spawn_tls_sighup_reload(_reloader: TlsReloader) {
    warn!(
        "TLS hot-reload via SIGHUP is not available on this platform; rotate the \
         certificate with a restart"
    );
}

/// Build the admin / control-plane gRPC listener future ([STL-254], ADR-0016).
///
/// Bound and serving when a token is configured; an always-pending no-op
/// otherwise — the secure default: no token, no surface. The listener is bound
/// eagerly so a port clash is a boot error. When `tls` is set the listener
/// serves over TLS ([STL-311], sharing the pg-wire material); otherwise a
/// non-loopback bind warns that tokens travel plaintext.
async fn build_admin_grpc(
    cfg: &Config,
    admin_core: AdminService<SystemClock, AnyDisk>,
    admin_auth: Arc<AdminAuth>,
    tls: Option<AcceptorSource>,
) -> anyhow::Result<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>> {
    if !admin_auth.is_enabled() {
        info!(
            "admin API disabled: no [admin] tokens configured (the surface rejects every request)"
        );
        return Ok(Box::pin(future::pending()));
    }
    let listener = TcpListener::bind(cfg.admin_grpc_listen)
        .await
        .with_context(|| format!("binding admin gRPC listener on {}", cfg.admin_grpc_listen))?;
    let addr = listener.local_addr().unwrap_or(cfg.admin_grpc_listen);
    if tls.is_some() {
        info!(
            %addr,
            "admin API enabled: gRPC (v1alpha1, TLS) + HTTP/JSON gateway on the ops listener"
        );
    } else {
        info!(%addr, "admin API enabled: gRPC (v1alpha1) + HTTP/JSON gateway on the ops listener");
        if !addr.ip().is_loopback() {
            // Reachable only in dev: a non-dev off-loopback admin bind without
            // [tls] takes the self-signed fallback above (admin_wants_self_signed).
            warn!(
                "admin API tokens travel in PLAINTEXT on {addr}: bind loopback or configure \
                 [tls] to encrypt the admin surface"
            );
        }
    }
    let service = admin::grpc::GrpcAdmin::new(admin_core, admin_auth);
    Ok(Box::pin(async move {
        match tls {
            Some(tls) => admin::grpc::serve_tls(listener, service, tls).await,
            None => admin::grpc::serve(listener, service).await,
        }
        .context("admin gRPC listener exited")
    }))
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

/// Stele's default admin gRPC port — `5455`, the port the CLI design prototype
/// uses for the control-plane surface ([STL-254]; one past the pg-wire `5454`).
const DEFAULT_ADMIN_GRPC_PORT: u16 = 5455;

const fn default_admin_grpc_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_ADMIN_GRPC_PORT)
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
        // loopback with the [tls] section commented out: the secure-defaults
        // posture self-signs a plaintext non-loopback bind (STL-304) but merely
        // warns on loopback, so a config-file run of the example boots out of
        // the box on plaintext.
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
            matches!(plaintext_posture(&cfg), PlaintextPosture::Warn(_)),
            "the committed example binds loopback: plaintext warns, never self-signs"
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

    // --- [admin] section (STL-254) ------------------------------------------

    #[test]
    fn no_admin_section_means_the_api_is_disabled() {
        // Secure default: no tokens ⇒ the surface authorizes nothing, on a default
        // gRPC port that is never actually bound.
        let cfg = Config::from_toml_str("").unwrap();
        assert!(cfg.admin_tokens.tokens().is_empty());
        assert_eq!(cfg.admin_grpc_listen, default_admin_grpc_listen());
        assert_eq!(default_admin_grpc_listen().port(), 5455);
        assert!(Config::dev().admin_tokens.tokens().is_empty(), "dev too");
    }

    #[test]
    fn admin_section_parses_tokens_and_grpc_listen() {
        let toml = "[admin]\ntokens = [\"alpha\", \"beta\"]\ngrpc_listen = \"127.0.0.1:6455\"\n";
        let cfg = Config::from_toml_str(toml).unwrap();
        assert_eq!(cfg.admin_tokens.tokens(), ["alpha", "beta"]);
        assert_eq!(cfg.admin_grpc_listen, "127.0.0.1:6455".parse().unwrap());
    }

    #[test]
    fn admin_tokens_are_redacted_in_debug() {
        // The boot-time `info!(?cfg)` must never spill a token (STL-254).
        let cfg = Config::from_toml_str("[admin]\ntokens = [\"super-secret\"]\n").unwrap();
        let debug = format!("{:?}", cfg.admin_tokens);
        assert!(!debug.contains("super-secret"), "token leaked: {debug}");
        assert!(debug.contains("1 configured"), "{debug}");
        // …and not via the whole-Config Debug either.
        assert!(!format!("{cfg:?}").contains("super-secret"));
    }

    #[test]
    fn admin_section_with_only_grpc_listen_stays_disabled() {
        // A port but no token: still off (no token ⇒ no surface).
        let cfg = Config::from_toml_str("[admin]\ngrpc_listen = \"0.0.0.0:7000\"\n").unwrap();
        assert!(cfg.admin_tokens.tokens().is_empty());
        assert_eq!(cfg.admin_grpc_listen, "0.0.0.0:7000".parse().unwrap());
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
    fn non_dev_without_tls_self_signs_off_loopback() {
        // STL-304: rather than refuse to boot, a non-loopback bind without [tls]
        // generates an ephemeral self-signed certificate and warns loudly.
        let posture = plaintext_posture(&non_dev("0.0.0.0:5454", None));
        let PlaintextPosture::GenerateSelfSigned(msg) = posture else {
            panic!("expected self-signed generation, got {posture:?}");
        };
        // The warning must name the trade-off (encryption, not authentication)
        // and the path to a real cert.
        assert!(msg.contains("self-signed"), "{msg}");
        assert!(
            msg.contains("NOT") && msg.contains("authenticated"),
            "{msg}"
        );
        assert!(msg.contains("[tls]"), "{msg}");
        // That the daemon can actually mint the promised acceptor is proved,
        // for both modes plus a real handshake, in stele-pgwire's tls /
        // tls_wire tests — the call is identical regardless of caller crate.
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

    // --- admin-surface TLS posture (STL-311) --------------------------------

    /// A non-dev config with the admin API enabled, binding pg-wire / gRPC / ops
    /// at the given addresses and no `[tls]` section.
    fn non_dev_admin(pg: &str, grpc: &str, metrics: &str) -> Config {
        let mut cfg = Config::from_toml_str("").unwrap();
        cfg.listen = pg.parse().unwrap();
        cfg.admin_grpc_listen = grpc.parse().unwrap();
        cfg.metrics_listen = metrics.parse().unwrap();
        cfg.admin_tokens = AdminTokens(vec!["secret".to_owned()]);
        cfg
    }

    #[test]
    fn admin_wants_self_signed_only_when_enabled_and_off_loopback() {
        // Disabled admin never wants a cert, even bound wide open.
        let mut disabled = non_dev_admin("127.0.0.1:5454", "0.0.0.0:5455", "0.0.0.0:9090");
        disabled.admin_tokens = AdminTokens::default();
        assert!(!admin_wants_self_signed(&disabled));

        // Enabled + both admin surfaces loopback: tokens stay local, no cert.
        let loop_only = non_dev_admin("127.0.0.1:5454", "127.0.0.1:5455", "127.0.0.1:9090");
        assert!(!admin_wants_self_signed(&loop_only));

        // Enabled + a non-loopback gRPC bind → wants a cert.
        let public_grpc = non_dev_admin("127.0.0.1:5454", "0.0.0.0:5455", "127.0.0.1:9090");
        assert!(admin_wants_self_signed(&public_grpc));

        // Enabled + a non-loopback ops bind (the /v1alpha1 gateway) → wants a cert.
        let public_ops = non_dev_admin("127.0.0.1:5454", "127.0.0.1:5455", "0.0.0.0:9090");
        assert!(admin_wants_self_signed(&public_ops));
    }

    #[test]
    fn resolve_tls_self_signs_for_a_public_admin_on_a_loopback_pg_wire() {
        // pg-wire loopback (would not self-sign on its own) but the admin API is
        // exposed off-loopback: the shared context still mints an ephemeral cert
        // so the tokens are encrypted (STL-311 extends STL-304 to the admin API).
        let cfg = non_dev_admin("127.0.0.1:5454", "0.0.0.0:5455", "127.0.0.1:9090");
        let posture = plaintext_posture(&cfg);
        assert!(
            matches!(posture, PlaintextPosture::Warn(_)),
            "pg-wire alone only warns on loopback"
        );
        assert!(matches!(
            resolve_tls(&cfg, &posture).unwrap(),
            Some(TlsContext::SelfSigned(_))
        ));
    }

    #[test]
    fn resolve_tls_self_signs_for_a_public_pg_wire() {
        let cfg = non_dev("0.0.0.0:5454", None);
        assert!(matches!(
            resolve_tls(&cfg, &plaintext_posture(&cfg)).unwrap(),
            Some(TlsContext::SelfSigned(_))
        ));
    }

    #[test]
    fn resolve_tls_is_none_for_dev_and_for_a_quiet_loopback_bind() {
        // Dev is friction-free even off-loopback.
        let mut dev = Config::dev();
        dev.listen = "0.0.0.0:5454".parse().unwrap();
        assert!(
            resolve_tls(&dev, &plaintext_posture(&dev))
                .unwrap()
                .is_none()
        );

        // Non-dev, loopback pg-wire, admin disabled: nothing secret leaves the box.
        let quiet = non_dev("127.0.0.1:5454", None);
        assert!(
            resolve_tls(&quiet, &plaintext_posture(&quiet))
                .unwrap()
                .is_none()
        );
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
