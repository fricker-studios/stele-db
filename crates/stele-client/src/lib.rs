// SPDX-License-Identifier: BUSL-1.1
//! `stele-client` — the Rust SDK for the Stele **admin / control-plane API**
//! ([ADR-0016], [assumption A26]).
//!
//! Stele exposes two surfaces. **SQL** rides the PostgreSQL wire protocol, so any
//! existing Postgres driver (`psycopg`, `pgx`, JDBC, …) speaks it — Stele ships no
//! SQL driver of its own. Everything that is *not* SQL — health and status,
//! backup and restore-plan, and catalog / segment / version / commit-chain
//! introspection — lives behind a dedicated, versioned admin API. This crate is a
//! typed client for that surface, and the shared substrate the `stele` CLI's
//! admin tier, Studio, and the operator build on.
//!
//! # Transport
//!
//! The admin API offers two transports from one contract: typed **gRPC** (for the
//! operator and programmatic clients) and an **HTTP/JSON gateway** (for curl,
//! scripts, and the desktop app). This crate speaks the HTTP/JSON gateway — the
//! lighter dependency footprint (ADR-0016 makes gRPC optional in v0): one blocking
//! request per call over a plain [`std::net::TcpStream`], no async runtime and no
//! HTTP framework pulled into your dependency tree. When [TLS](Tls) is configured
//! the same call rides `https://` through `rustls`' blocking
//! [`StreamOwned`](rustls::StreamOwned) adapter — the very `rustls` the SQL surface
//! already pins ([ADR-0016]: TLS is shared with pg-wire), so still no async client
//! crate enters your tree.
//!
//! # Versioning
//!
//! This crate tracks the admin API's `v1alpha1` surface explicitly and follows
//! `0.x` SemVer ([ADR-0014]): every route it calls is under `/v1alpha1/…`, and a
//! `v1beta1`/`v1` graduation of the API is a new client minor. Pre-1.0, minor
//! releases may break.
//!
//! # Authentication
//!
//! Every call carries a static bearer token ([`Config::token`]) — the admin API's
//! `[admin] tokens` (`stele.toml`). With no token configured the server rejects
//! every request, so a missing token is refused locally ([`Error::NoToken`])
//! rather than spent on a round-trip.
//!
//! # TLS
//!
//! Set [`Config::tls`] to dial the admin gateway over `https://`, so a bearer token
//! never travels in cleartext off-loopback. A [`Tls`] with no [`ca`](Tls::ca)
//! **encrypts without verifying the server's identity** (libpq's `require` —
//! defeats eavesdropping, not an active man-in-the-middle); supplying a PEM CA
//! bundle verifies the server certificate against it and against the host name
//! (libpq's `verify-full`). Leave `tls` `None` — the default — for the loopback /
//! TLS-terminating-proxy deployments the gateway has always served in plaintext.
//!
//! # Example
//!
//! ```no_run
//! use stele_client::{Client, Config, Tls};
//!
//! # fn main() -> Result<(), stele_client::Error> {
//! let client = Client::new(
//!     Config::new(
//!         "stele.internal",
//!         9090, // the ops listener the HTTP/JSON gateway shares
//!         // A missing or empty env var becomes `None`, so an unconfigured token is
//!         // refused locally (`Error::NoToken`) rather than spent on a 401 round-trip.
//!         std::env::var("STELE_ADMIN_TOKEN").ok().filter(|t| !t.is_empty()),
//!     )
//!     // Encrypted and authenticated: verify the gateway against this CA bundle.
//!     .with_tls(Tls::verify("/etc/stele/ca.pem")),
//! );
//!
//! // Liveness, then engine state.
//! assert!(client.health()?.is_serving());
//! let status = client.status()?;
//! println!("stele {} · {} tables", status.server_version, status.table_count);
//!
//! // Trigger a consistent online backup and validate it without applying.
//! let manifest = client.backup("/var/lib/stele/backups/snap1")?;
//! let plan = client.restore_plan("/var/lib/stele/backups/snap1")?;
//! assert!(plan.valid);
//! # let _ = manifest;
//! # Ok(())
//! # }
//! ```
//!
//! [ADR-0016]: https://github.com/fricker-studios/stele-db/blob/main/docs/adr/0016-admin-control-plane-api.md
//! [ADR-0014]: https://github.com/fricker-studios/stele-db/blob/main/docs/adr/0014-release-channels-and-versioning-policy.md
//! [assumption A26]: https://github.com/fricker-studios/stele-db/blob/main/docs/assumptions.md

use std::fmt;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde::Deserialize;
use serde_json::{Value, json};

/// The `v1alpha1` admin-API version this client speaks. Every route is mounted
/// under `/{API_VERSION}/…`.
pub const API_VERSION: &str = "v1alpha1";

/// The default per-call timeout — a single admin request must not stall a caller
/// indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection settings for the admin HTTP/JSON gateway.
///
/// The gateway shares the server's ops listener (default port `9090`), not a port
/// of its own. `token` is the static bearer credential; leave it `None` only to
/// exercise the local-refusal path ([`Error::NoToken`]).
///
/// Construct via [`Config::new`] (plus [`with_tls`](Self::with_tls)); the struct is
/// `#[non_exhaustive]` so a later field addition stays backward-compatible for a
/// published-crate consumer.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// The ops-listener host serving the admin HTTP/JSON gateway.
    pub host: String,
    /// The ops-listener port (the gateway shares it). The server default is
    /// `9090`.
    pub port: u16,
    /// The bearer token, or `None` when none is configured (every call then
    /// short-circuits with [`Error::NoToken`]).
    pub token: Option<String>,
    /// TLS transport ([STL-320]). `None` (the default) dials plaintext HTTP — the
    /// loopback / proxy-fronted posture; `Some` dials `https://` so the bearer
    /// token is never exposed in cleartext off-loopback.
    ///
    /// [STL-320]: https://allegromusic.atlassian.net/browse/STL-320
    pub tls: Option<Tls>,
}

impl Config {
    /// Plaintext connection settings for `host:port` with an optional bearer
    /// `token`. Add [`with_tls`](Self::with_tls) to encrypt the transport.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16, token: Option<String>) -> Self {
        Self {
            host: host.into(),
            port,
            token,
            tls: None,
        }
    }

    /// Encrypt the transport with `tls` (dial `https://`).
    #[must_use]
    pub fn with_tls(mut self, tls: Tls) -> Self {
        self.tls = Some(tls);
        self
    }
}

/// TLS options for the admin gateway ([STL-320]).
///
/// Constructing a `Tls` (and attaching it via [`Config::with_tls`]) switches the
/// transport to `https://`. Two postures, matching libpq's `sslmode`:
///
/// * [`Tls::encrypt`] — no trust anchor: the server certificate is accepted
///   without a chain or host-name check (libpq's `require`). Defeats passive
///   eavesdropping, **not** an active man-in-the-middle.
/// * [`Tls::verify`] — verify the certificate against a PEM CA bundle and the host
///   name (libpq's `verify-full`). The authenticated posture.
///
/// [`server_name`](Self::server_name) overrides the name verified / sent via SNI;
/// it defaults to [`Config::host`], so set it only when you connect to an IP but
/// the certificate carries a DNS SAN (connect `127.0.0.1`, verify `localhost`).
///
/// [STL-320]: https://allegromusic.atlassian.net/browse/STL-320
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Tls {
    /// PEM CA bundle the server certificate must chain to. `None` accepts any
    /// certificate (encrypt-only); `Some` is the verified posture.
    pub ca: Option<PathBuf>,
    /// The name to verify the certificate against and send via SNI. `None`
    /// defaults to [`Config::host`].
    pub server_name: Option<String>,
}

impl Tls {
    /// Encrypt without verifying the server's identity (libpq's `require`). Use
    /// [`verify`](Self::verify) when you can pin a CA.
    #[must_use]
    pub fn encrypt() -> Self {
        Self::default()
    }

    /// Verify the server certificate against the PEM CA bundle at `ca` and against
    /// the host name (libpq's `verify-full`).
    #[must_use]
    pub fn verify(ca: impl Into<PathBuf>) -> Self {
        Self {
            ca: Some(ca.into()),
            server_name: None,
        }
    }

    /// Override the name verified against and sent via SNI (defaults to
    /// [`Config::host`]).
    #[must_use]
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }
}

/// A failure talking to the admin / control-plane API.
#[derive(Debug)]
pub enum Error {
    /// No bearer token was configured, so every call would be rejected. Refused
    /// locally — with an actionable message — rather than as a wasted round-trip.
    NoToken,
    /// The TCP connect, or the request/response I/O, failed.
    Transport(String),
    /// The gateway answered a non-2xx status; carries the HTTP status code and the
    /// parsed `error` message (or the raw body when it was not JSON).
    Status {
        /// The HTTP status code (e.g. `401`, `404`, `500`).
        code: String,
        /// The human-readable failure.
        message: String,
    },
    /// A 2xx body did not parse into the expected JSON shape.
    Decode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoToken => {
                f.write_str("the admin control-plane API requires a bearer token (none configured)")
            }
            Self::Transport(detail) => write!(f, "admin API transport error: {detail}"),
            Self::Status { code, message } => {
                write!(f, "admin API returned HTTP {code}: {message}")
            }
            Self::Decode(detail) => write!(f, "admin API reply did not decode: {detail}"),
        }
    }
}

impl std::error::Error for Error {}

/// The liveness reply from [`Client::health`].
#[derive(Debug, Clone, Deserialize)]
pub struct Health {
    /// The serving status, `"SERVING"` when the process can answer.
    pub status: String,
}

impl Health {
    /// Whether the server reported `SERVING`.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.status == "SERVING"
    }
}

/// Engine state ([`Client::status`]).
#[derive(Debug, Clone, Deserialize)]
pub struct StatusReport {
    /// Recovery complete and no WAL poisoned.
    pub ready: bool,
    /// A failed fsync has poisoned a table's WAL.
    pub wal_poisoned: bool,
    /// The server version serving the API.
    pub server_version: String,
    /// Number of live tables.
    pub table_count: u64,
    /// Number of users in the catalog user store.
    pub user_count: u64,
    /// Per-table summaries.
    pub tables: Vec<TableStatus>,
}

/// One live table's summary within a [`StatusReport`].
#[derive(Debug, Clone, Deserialize)]
pub struct TableStatus {
    /// The table name.
    pub name: String,
    /// Column count.
    pub column_count: u64,
    /// Resident sealed segments plus the hot delta tier.
    pub segment_count: u64,
}

/// A backup manifest summary ([`Client::backup`]).
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestSummary {
    /// The manifest format version.
    pub manifest_version: u32,
    /// The `stele-engine` version that produced the backup.
    pub stele_version: String,
    /// The fence instant (commit-clock high-water mark at backup time).
    pub fence_micros: i64,
    /// The hash-chained commit log's head the backup vouches for (lowercase hex).
    pub commit_head: String,
    /// How many files the backup contains.
    pub file_count: u64,
    /// Their total size in bytes.
    pub total_bytes: u64,
}

/// The verdict of validating a backup ([`Client::restore_plan`]).
#[derive(Debug, Clone, Deserialize)]
pub struct RestorePlan {
    /// The manifest decoded and every file matched its recorded checksum.
    pub valid: bool,
    /// Why the backup did not validate (when `valid` is false).
    pub error: Option<String>,
    /// The manifest summary, when it decoded.
    pub manifest: Option<ManifestSummary>,
}

/// One column header of a tabular introspection reply.
#[derive(Debug, Clone, Deserialize)]
pub struct Column {
    /// The column name.
    pub name: String,
    /// The column's rendered type name (e.g. `int8`, `text`).
    #[serde(rename = "type")]
    pub type_name: String,
}

/// A tabular introspection reply ([`Client::segments`], [`Client::versions`],
/// [`Client::audit_chain`]).
///
/// Each cell is its rendered text, or `None` for a SQL `NULL` — exactly as the
/// SQL wire renders it.
#[derive(Debug, Clone, Deserialize)]
pub struct TableData {
    /// The column headers, in output order.
    #[serde(default)]
    pub columns: Vec<Column>,
    /// One row of optional rendered cells, aligned to [`columns`](Self::columns).
    pub rows: Vec<Vec<Option<String>>>,
}

/// A blocking client for the admin HTTP/JSON gateway.
///
/// Cheap to construct (no socket is opened until a call is made) and cheap to
/// clone. Each method is one connect → request → read-to-EOF round trip.
#[derive(Debug, Clone)]
pub struct Client {
    config: Config,
    timeout: Duration,
    /// The TLS client config, built once on first use from [`Config::tls`] and
    /// reused across calls — so the CA bundle is read and parsed off disk a single
    /// time, not on every `round_trip`. Empty for a plaintext client.
    tls_config: OnceLock<Arc<rustls::ClientConfig>>,
}

impl Client {
    /// Wrap the connection settings with the default per-call timeout.
    #[must_use]
    pub const fn new(config: Config) -> Self {
        Self {
            config,
            timeout: DEFAULT_TIMEOUT,
            tls_config: OnceLock::new(),
        }
    }

    /// Override the per-call read/write timeout (default 30s).
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The shared [`rustls::ClientConfig`] for this client's [`Tls`] settings,
    /// built once and memoized. The CA bundle (when one is pinned) is read off disk
    /// only on this first build; later calls reuse the `Arc`. A build error (a
    /// missing/invalid CA) is not cached — it re-attempts next call, which is the
    /// rare path anyway.
    fn shared_tls_config(&self, tls: &Tls) -> Result<Arc<rustls::ClientConfig>, Error> {
        if let Some(config) = self.tls_config.get() {
            return Ok(Arc::clone(config));
        }
        let built = Arc::new(tls_client_config(tls)?);
        // A concurrent call may have installed one first; `set` then no-ops and we
        // return the installed winner so every caller shares the same config.
        let _ = self.tls_config.set(Arc::clone(&built));
        Ok(self.tls_config.get().map_or(built, Arc::clone))
    }

    /// `GET /v1alpha1/health` — liveness. `SERVING` whenever the process can
    /// answer; says nothing about engine readiness ([`status`](Self::status)'s
    /// job).
    ///
    /// # Errors
    /// [`Error`] on a missing token, transport failure, non-2xx status, or a reply
    /// that does not decode.
    pub fn health(&self) -> Result<Health, Error> {
        let value = self.request("GET", "/v1alpha1/health", None)?;
        decode(value)
    }

    /// `GET /v1alpha1/status` — engine state: readiness, WAL-poison, version, and
    /// table/user/segment counts.
    ///
    /// # Errors
    /// As [`health`](Self::health).
    pub fn status(&self) -> Result<StatusReport, Error> {
        let value = self.request("GET", "/v1alpha1/status", None)?;
        decode(value)
    }

    /// `POST /v1alpha1/backup` — trigger a consistent online backup into the
    /// server-side directory `path`, returning its manifest summary. `path` is
    /// created if absent and must be empty.
    ///
    /// # Errors
    /// As [`health`](Self::health); also a `400` when the target is non-empty.
    pub fn backup(&self, path: &str) -> Result<ManifestSummary, Error> {
        let value = self.request("POST", "/v1alpha1/backup", Some(&json!({ "path": path })))?;
        // The route wraps the manifest: `{"manifest": {…}}`.
        let manifest = value
            .get("manifest")
            .cloned()
            .ok_or_else(|| Error::Decode("backup reply missing `manifest`".to_owned()))?;
        decode(manifest)
    }

    /// `POST /v1alpha1/restore-plan` — validate a backup directory without applying
    /// it.
    ///
    /// # Errors
    /// As [`health`](Self::health). A missing directory or a failed checksum is a
    /// *valid* reply with `valid = false`, not an error.
    pub fn restore_plan(&self, path: &str) -> Result<RestorePlan, Error> {
        let value = self.request(
            "POST",
            "/v1alpha1/restore-plan",
            Some(&json!({ "path": path })),
        )?;
        decode(value)
    }

    /// `POST /v1alpha1/segments` — per-table columnar segment + zone-map metadata.
    ///
    /// # Errors
    /// As [`health`](Self::health); a `404` for an unknown table.
    pub fn segments(&self, table: &str) -> Result<TableData, Error> {
        let value = self.request(
            "POST",
            "/v1alpha1/segments",
            Some(&json!({ "table": table })),
        )?;
        decode(value)
    }

    /// `POST /v1alpha1/versions` — per-key (or whole-table, when `key` is `None`)
    /// version history. `key` is a SQL literal folded to the key column's type.
    ///
    /// # Errors
    /// As [`health`](Self::health); a `404` for an unknown table or a `400` for a
    /// `key` that is not a literal of the key type.
    pub fn versions(&self, table: &str, key: Option<&str>) -> Result<TableData, Error> {
        let value = self.request("POST", "/v1alpha1/versions", Some(&table_body(table, key)))?;
        decode(value)
    }

    /// `POST /v1alpha1/audit-chain` — per-version commit hash-chain links plus an
    /// intact/broken verdict. `key` is a SQL literal folded to the key column's
    /// type.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub fn audit_chain(&self, table: &str, key: Option<&str>) -> Result<TableData, Error> {
        let value = self.request(
            "POST",
            "/v1alpha1/audit-chain",
            Some(&table_body(table, key)),
        )?;
        decode(value)
    }

    /// `POST /v1alpha1/reload-tls` — reload the server's TLS certificate/key from
    /// the configured `[tls]` paths without a restart ([STL-326]), the
    /// cross-platform / programmatic counterpart to the Unix-only SIGHUP trigger.
    /// Returns the reloaded certificate path the server confirms.
    ///
    /// # Errors
    /// As [`health`](Self::health); a `409` when the server has no reloadable
    /// `[tls]` material (plaintext, loopback, or the self-signed fallback), or a
    /// `500` when the new pair is unreadable / mismatched — in both cases the live
    /// certificate keeps serving.
    ///
    /// [STL-326]: https://allegromusic.atlassian.net/browse/STL-326
    pub fn reload_tls(&self) -> Result<String, Error> {
        let value = self.request("POST", "/v1alpha1/reload-tls", None)?;
        value
            .get("cert_path")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::Decode("reload-tls reply missing `cert_path`".to_owned()))
    }

    /// One request/response round-trip. Returns the parsed 2xx JSON body, or an
    /// [`Error`] carrying the gateway's failure.
    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, Error> {
        let token = self.config.token.as_deref().ok_or(Error::NoToken)?;
        let body = body.map(ToString::to_string).unwrap_or_default();
        let raw = self.round_trip(method, path, &body, token)?;
        let (code, payload) = parse_http_response(&raw)?;
        if code.starts_with('2') {
            serde_json::from_str(&payload)
                .map_err(|e| Error::Decode(format!("invalid JSON reply: {e}")))
        } else {
            // Error bodies are the gateway's `{"error":…}` JSON, or — for the ops
            // listener's own 503/404 — plain text. Prefer the JSON message.
            let message = serde_json::from_str::<Value>(&payload)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| payload.trim().to_owned());
            Err(Error::Status { code, message })
        }
    }

    /// Open a connection, send one HTTP/1.1 request, and read the whole reply.
    fn round_trip(
        &self,
        method: &str,
        path: &str,
        body: &str,
        token: &str,
    ) -> Result<String, Error> {
        let host = &self.config.host;
        let port = self.config.port;
        // Header-injection guard: `host` and `token` are interpolated into the raw
        // request head below, so a `\r`/`\n` (or any control char) in either would
        // let a caller smuggle extra header lines or a second request. Tokens
        // routinely come from environment variables, so refuse a malformed value up
        // front — before opening a socket — rather than emit a corrupt request.
        reject_control_chars("admin host", host)?;
        reject_control_chars("admin token", token)?;
        let tcp = TcpStream::connect((host.as_str(), port)).map_err(|e| {
            Error::Transport(format!("connecting to the admin API at {host}:{port}: {e}"))
        })?;
        tcp.set_read_timeout(Some(self.timeout)).ok();
        tcp.set_write_timeout(Some(self.timeout)).ok();
        // Plaintext or — when `[tls]` is configured — the rustls-wrapped stream.
        // Either way the framing below is identical: the TLS handshake runs lazily
        // on the first read/write of the `StreamOwned` adapter (a verification
        // failure surfaces here as a transport error), and the encrypted record
        // layer is transparent to the HTTP/1.1 we speak over it.
        let mut stream: Box<dyn ReadWrite> = match &self.config.tls {
            None => Box::new(tcp),
            Some(tls) => {
                let config = self.shared_tls_config(tls)?;
                Box::new(tls_stream(config, tls.server_name.as_deref(), host, tcp)?)
            }
        };
        // Build the request head from explicit header lines joined by CRLF — no
        // continuation/indentation trickery, so there is no chance of leading
        // whitespace folding a header. `Connection: close` lets the read below run
        // to EOF (the gateway serves one request per connection).
        let head = [
            format!("{method} {path} HTTP/1.1"),
            format!("Host: {host}:{port}"),
            format!("Authorization: Bearer {token}"),
            "Accept: application/json".to_owned(),
            "Content-Type: application/json".to_owned(),
            format!("Content-Length: {}", body.len()),
            "Connection: close".to_owned(),
        ]
        .join("\r\n");
        let request = format!("{head}\r\n\r\n{body}");
        stream
            .write_all(request.as_bytes())
            .map_err(|e| Error::Transport(format!("sending the admin request: {e}")))?;
        stream.flush().ok();
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|e| Error::Transport(format!("reading the admin reply: {e}")))?;
        String::from_utf8(raw)
            .map_err(|_| Error::Transport("admin reply was not valid UTF-8".to_owned()))
    }
}

// ---------------------------------------------------------------------------
// TLS transport (STL-320)
// ---------------------------------------------------------------------------

/// The blocking duplex transport the round-trip runs over — a plain
/// [`TcpStream`] or the rustls-wrapped stream. Blanket-implemented over anything
/// that is both [`Read`](std::io::Read) and [`Write`](std::io::Write), so the two
/// transports box into one `dyn` object the HTTP framing is agnostic to.
trait ReadWrite: std::io::Read + std::io::Write {}
impl<T: std::io::Read + std::io::Write> ReadWrite for T {}

/// Wrap `tcp` in a blocking rustls client stream over the (already-built, shared)
/// `config`. The handshake itself is deferred to the first read/write — so an
/// untrusted certificate surfaces as the round-trip's transport error, not here.
fn tls_stream(
    config: Arc<rustls::ClientConfig>,
    server_name_override: Option<&str>,
    host: &str,
    tcp: TcpStream,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, Error> {
    // The name to verify against / send via SNI: the override, else the connect
    // host. An IP literal yields an `IpAddress` server name (no SNI extension),
    // which the encrypt-only verifier ignores and a CA-verified handshake checks
    // against the certificate's IP SANs — exactly libpq's behaviour.
    let name = server_name_override.unwrap_or(host);
    let server_name = ServerName::try_from(name.to_owned())
        .map_err(|e| Error::Transport(format!("invalid TLS server name {name:?}: {e}")))?;
    let conn = rustls::ClientConnection::new(config, server_name)
        .map_err(|e| Error::Transport(format!("initializing TLS: {e}")))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

/// Build the rustls client config for `tls`: verify against the pinned CA bundle
/// when one is given (chain + host name, libpq's `verify-full`), otherwise accept
/// any server certificate (encrypt-only, libpq's `require`). No client
/// certificate is presented — the admin SDK authenticates with the bearer token.
fn tls_client_config(tls: &Tls) -> Result<rustls::ClientConfig, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .map_err(|e| Error::Transport(format!("selecting TLS protocol versions: {e}")))?;
    let config = match tls.ca.as_deref() {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(ca)
                .map_err(|e| Error::Transport(format!("reading CA bundle {}: {e}", ca.display())))?
            {
                let cert =
                    cert.map_err(|e| Error::Transport(format!("parsing a CA certificate: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| Error::Transport(format!("adding a CA certificate: {e}")))?;
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(
                provider.signature_verification_algorithms,
            )))
            .with_no_client_auth(),
    };
    Ok(config)
}

/// The encrypt-only verifier ([`Tls::encrypt`]): accepts any server certificate
/// (no chain or host-name check) while still verifying the handshake *signatures*,
/// so the peer must hold the key for the certificate it presents. The standard
/// rustls "danger" pattern, matching libpq `sslmode=require`; it is the SDK twin
/// of the `stele shell`'s verifier ([STL-251]).
///
/// [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
#[derive(Debug)]
struct AcceptAnyServerCert(rustls::crypto::WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// The `{"table":…,"key":…?}` request body shared by the introspection routes.
/// `key` is omitted entirely when `None` (a whole-table read).
fn table_body(table: &str, key: Option<&str>) -> Value {
    key.map_or_else(
        || json!({ "table": table }),
        |key| json!({ "table": table, "key": key }),
    )
}

/// Reject a value bound for a raw HTTP header line if it carries an ASCII control
/// character (notably CR/LF) — the header-injection / request-smuggling guard for
/// the `host` and `token` interpolated into the request head. A hostname or bearer
/// token never legitimately contains one, so this only ever fires on an attack or
/// a corrupt config.
fn reject_control_chars(field: &str, value: &str) -> Result<(), Error> {
    if let Some(pos) = value.bytes().position(|b| b.is_ascii_control()) {
        return Err(Error::Transport(format!(
            "{field} contains an illegal control character at byte {pos}: \
             refusing to send a request that could be header-injected"
        )));
    }
    Ok(())
}

/// Decode a JSON value into a typed reply, mapping a shape mismatch to
/// [`Error::Decode`].
fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value)
        .map_err(|e| Error::Decode(format!("unexpected admin reply shape: {e}")))
}

/// Split an HTTP response into its status code (the 3-digit token of the status
/// line) and its body (everything past the blank line).
fn parse_http_response(raw: &str) -> Result<(String, String), Error> {
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(|| {
        Error::Transport("malformed admin reply: no header terminator".to_owned())
    })?;
    let status_line = head.lines().next().unwrap_or_default();
    // "HTTP/1.1 200 OK" → "200".
    let code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| Error::Transport(format!("malformed admin status line: {status_line:?}")))?;
    Ok((code.to_owned(), body.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_response_splits_status_and_body() {
        let raw = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"ready\":true}\n";
        let (code, body) = parse_http_response(raw).expect("parse");
        assert_eq!(code, "200");
        assert_eq!(body, "{\"ready\":true}\n");
    }

    #[test]
    fn parse_http_response_rejects_a_bodyless_frame() {
        // No CRLFCRLF terminator — a desynchronized stream, not a valid reply.
        assert!(matches!(
            parse_http_response("HTTP/1.1 200 OK"),
            Err(Error::Transport(_))
        ));
    }

    #[test]
    fn health_decodes_and_reports_serving() {
        let h: Health = serde_json::from_str(r#"{"status":"SERVING"}"#).expect("decode");
        assert!(h.is_serving());
        let h: Health = serde_json::from_str(r#"{"status":"NOT_SERVING"}"#).expect("decode");
        assert!(!h.is_serving());
    }

    #[test]
    fn status_report_decodes_the_gateway_shape() {
        let body = r#"{
            "ready": true, "wal_poisoned": false, "server_version": "0.3.0",
            "table_count": 2, "user_count": 1,
            "tables": [{"name":"account","column_count":2,"segment_count":3}]
        }"#;
        let report: StatusReport = serde_json::from_str(body).expect("decode");
        assert!(report.ready);
        assert_eq!(report.table_count, 2);
        assert_eq!(report.tables[0].name, "account");
        assert_eq!(report.tables[0].segment_count, 3);
    }

    #[test]
    fn restore_plan_decodes_an_invalid_verdict() {
        let body = r#"{"valid": false, "error": "backup directory \"/x\" does not exist", "manifest": null}"#;
        let plan: RestorePlan = serde_json::from_str(body).expect("decode");
        assert!(!plan.valid);
        assert!(plan.error.unwrap().contains("does not exist"));
        assert!(plan.manifest.is_none());
    }

    #[test]
    fn table_data_decodes_columns_rows_and_null_cells() {
        let body = r#"{
            "columns": [{"name":"segment","type":"text"},{"name":"bytes","type":"int8"}],
            "rows": [["seg-0001", "4096"], ["seg-0002", null]]
        }"#;
        let data: TableData = serde_json::from_str(body).expect("decode");
        assert_eq!(data.columns[0].name, "segment");
        assert_eq!(data.columns[1].type_name, "int8");
        assert_eq!(data.rows[0][0].as_deref(), Some("seg-0001"));
        assert!(data.rows[1][1].is_none(), "SQL NULL → None");
    }

    #[test]
    fn table_body_omits_key_when_absent() {
        assert_eq!(table_body("t", None), json!({ "table": "t" }));
        assert_eq!(
            table_body("t", Some("42")),
            json!({ "table": "t", "key": "42" })
        );
    }

    #[test]
    fn missing_token_is_refused_without_a_socket() {
        // An unused port — the call must short-circuit on the missing token before
        // any connect is attempted.
        let client = Client::new(Config::new("127.0.0.1", 1, None));
        assert!(matches!(client.status(), Err(Error::NoToken)));
        assert!(matches!(client.health(), Err(Error::NoToken)));
    }

    #[test]
    fn crlf_in_token_or_host_is_refused_without_a_socket() {
        // A CR/LF in the token would smuggle a header line; refuse before any
        // connect (port 1 would fail anyway, but the guard fires first).
        let evil_token = Client::new(Config::new(
            "127.0.0.1",
            1,
            Some("tok\r\nX-Evil: 1".to_owned()),
        ));
        assert!(matches!(evil_token.status(), Err(Error::Transport(_))));
        // Same for a CR/LF in the host.
        let evil_host = Client::new(Config::new(
            "127.0.0.1\r\nHost: evil",
            1,
            Some("tok".to_owned()),
        ));
        assert!(matches!(evil_host.status(), Err(Error::Transport(_))));
    }

    #[test]
    fn error_display_is_actionable() {
        assert!(Error::NoToken.to_string().contains("bearer token"));
        assert!(
            Error::Status {
                code: "404".to_owned(),
                message: "no such admin endpoint".to_owned(),
            }
            .to_string()
            .contains("404")
        );
    }

    // -- TLS transport (STL-320) -------------------------------------------

    #[test]
    fn config_is_plaintext_until_tls_is_attached() {
        let plain = Config::new("h", 9090, None);
        assert!(plain.tls.is_none(), "the default transport is plaintext");
        let secured = plain.with_tls(Tls::encrypt());
        assert!(secured.tls.is_some(), "with_tls switches to https");
    }

    #[test]
    fn tls_constructors_set_the_expected_posture() {
        // Encrypt-only: no CA, no host override.
        let encrypt = Tls::encrypt();
        assert!(encrypt.ca.is_none());
        assert!(encrypt.server_name.is_none());
        // Verified: pins the CA bundle.
        let verify = Tls::verify("/etc/stele/ca.pem");
        assert_eq!(
            verify.ca.as_deref(),
            Some(std::path::Path::new("/etc/stele/ca.pem"))
        );
        // The SNI / verification-name override is independent of the trust anchor.
        let named = Tls::encrypt().with_server_name("localhost");
        assert_eq!(named.server_name.as_deref(), Some("localhost"));
    }

    #[test]
    fn encrypt_only_config_builds_without_a_ca() {
        // The encrypt-only posture needs no trust anchor on disk, so the rustls
        // config builds offline (the handshake itself is exercised end-to-end in
        // the live-server TLS integration test).
        assert!(tls_client_config(&Tls::encrypt()).is_ok());
    }

    #[test]
    fn a_missing_ca_bundle_is_a_transport_error() {
        // `verify` against a path that does not exist fails when the config is
        // built — before a socket is opened — with an actionable transport error.
        let err = tls_client_config(&Tls::verify("/no/such/ca.pem")).unwrap_err();
        assert!(
            matches!(&err, Error::Transport(detail) if detail.contains("CA bundle")),
            "{err:?}"
        );
    }

    #[test]
    fn tls_config_is_built_once_and_shared() {
        // The rustls config is memoized: two resolutions hand back the same `Arc`,
        // so a reused client does not rebuild it (or re-read a CA) per call.
        let client = Client::new(Config::new("h", 9090, None));
        let tls = Tls::encrypt();
        let first = client.shared_tls_config(&tls).expect("build");
        let second = client.shared_tls_config(&tls).expect("cached");
        assert!(
            Arc::ptr_eq(&first, &second),
            "the TLS config must be built once and reused"
        );
    }
}
