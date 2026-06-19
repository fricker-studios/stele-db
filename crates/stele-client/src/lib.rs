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
//! HTTP framework pulled into your dependency tree.
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
//! rather than spent on a round-trip. TLS for the admin surface is not yet
//! available on the gateway; until it lands, bind the ops listener to loopback or
//! front it with a TLS-terminating proxy.
//!
//! # Example
//!
//! ```no_run
//! use stele_client::{Client, Config};
//!
//! # fn main() -> Result<(), stele_client::Error> {
//! let client = Client::new(Config {
//!     host: "127.0.0.1".to_owned(),
//!     port: 9090, // the ops listener the HTTP/JSON gateway shares
//!     token: Some(std::env::var("STELE_ADMIN_TOKEN").unwrap_or_default()),
//! });
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
use std::time::Duration;

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
#[derive(Debug, Clone)]
pub struct Config {
    /// The ops-listener host serving the admin HTTP/JSON gateway.
    pub host: String,
    /// The ops-listener port (the gateway shares it). The server default is
    /// `9090`.
    pub port: u16,
    /// The bearer token, or `None` when none is configured (every call then
    /// short-circuits with [`Error::NoToken`]).
    pub token: Option<String>,
}

impl Config {
    /// Connection settings for `host:port` with an optional bearer `token`.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16, token: Option<String>) -> Self {
        Self {
            host: host.into(),
            port,
            token,
        }
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
}

impl Client {
    /// Wrap the connection settings with the default per-call timeout.
    #[must_use]
    pub const fn new(config: Config) -> Self {
        Self {
            config,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Override the per-call read/write timeout (default 30s).
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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
        let mut stream = TcpStream::connect((host.as_str(), port)).map_err(|e| {
            Error::Transport(format!("connecting to the admin API at {host}:{port}: {e}"))
        })?;
        stream.set_read_timeout(Some(self.timeout)).ok();
        stream.set_write_timeout(Some(self.timeout)).ok();
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

/// The `{"table":…,"key":…?}` request body shared by the introspection routes.
/// `key` is omitted entirely when `None` (a whole-table read).
fn table_body(table: &str, key: Option<&str>) -> Value {
    key.map_or_else(
        || json!({ "table": table }),
        |key| json!({ "table": table, "key": key }),
    )
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
        let client = Client::new(Config {
            host: "127.0.0.1".to_owned(),
            // An unused port — the call must short-circuit on the missing token
            // before any connect is attempted.
            port: 1,
            token: None,
        });
        assert!(matches!(client.status(), Err(Error::NoToken)));
        assert!(matches!(client.health(), Err(Error::NoToken)));
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
}
