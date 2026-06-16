// SPDX-License-Identifier: BUSL-1.1
//! The `stele shell` admin / control-plane client ([STL-200]).
//!
//! A small **blocking HTTP/1.1 + JSON** client for the admin API's HTTP/JSON
//! gateway ([STL-254], [ADR-0016]) — the `/v1alpha1/…` routes the ops listener
//! serves ([STL-253]). The shell's admin tier (`\status` / `\backup` /
//! `\restore` / `\inspect-segment`) rides this surface; SQL and the temporal
//! tier (`\history` …) stay on pg-wire.
//!
//! gRPC — the admin API's *other* transport — would force an async `tonic` stack
//! onto a deliberately blocking shell ([STL-185]); the HTTP/JSON gateway is the
//! curl/script face built for exactly this, and the shell already hand-rolls its
//! pg-wire transport, so a one-request-per-call HTTP client is in pattern. The
//! gateway answers `Connection: close` with a `Content-Length` body, so each call
//! is: connect, write the request, read to EOF.
//!
//! Authentication is the gateway's static bearer token (`Authorization: Bearer …`).
//! With no token configured the surface rejects every request (`401`), so a
//! missing token is refused locally ([`AdminError::NoToken`]) rather than spent on
//! a round-trip.
//!
//! [STL-200]: https://allegromusic.atlassian.net/browse/STL-200
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [STL-185]: https://allegromusic.atlassian.net/browse/STL-185
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

/// A single admin request must not stall the shell indefinitely.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection settings for the admin HTTP/JSON gateway: the ops-listener host
/// and port, and the bearer token (absent until the operator supplies one via
/// `--admin-token` / `STELE_ADMIN_TOKEN`).
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// The ops-listener host — by default the same host the shell dials for
    /// pg-wire.
    pub host: String,
    /// The ops-listener port (the admin HTTP/JSON gateway shares it). Default
    /// `9090`.
    pub port: u16,
    /// The bearer token, or `None` when none was supplied.
    pub token: Option<String>,
}

/// A failure talking to the admin / control-plane API.
#[derive(Debug)]
pub enum AdminError {
    /// No bearer token was configured, so every call would `401`. Refused locally
    /// with an actionable message rather than a wasted round-trip.
    NoToken,
    /// The TCP connect or the request/response I/O failed.
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

/// Engine state ([`AdminClient::status`]) — the JSON shape `/v1alpha1/status`
/// returns (mirrors the server's `StatusReport`).
#[derive(Debug, Clone, Deserialize)]
pub struct StatusReport {
    /// Recovery complete and no WAL poisoned.
    pub ready: bool,
    /// A failed fsync has poisoned a table's WAL ([STL-217]).
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
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
    /// Resident sealed segments + the hot delta tier.
    pub segment_count: u64,
}

/// A backup manifest summary ([`AdminClient::backup`]).
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

/// The verdict of validating a backup ([`AdminClient::restore_plan`]).
#[derive(Debug, Clone, Deserialize)]
pub struct RestorePlan {
    /// The manifest decoded and every file matched its recorded checksum.
    pub valid: bool,
    /// Why the backup did not validate (when `valid` is false).
    pub error: Option<String>,
    /// The manifest summary, when it decoded.
    pub manifest: Option<ManifestSummary>,
}

/// A tabular introspection reply ([`AdminClient::segments`]).
///
/// Only the `rows` are decoded — `\inspect-segment` reads the fixed
/// segment-metadata columns positionally ([STL-301]), so the reply's `columns`
/// header is ignored (serde drops unknown fields).
#[derive(Debug, Clone, Deserialize)]
pub struct TableData {
    /// One row of optional rendered cells (`None` = SQL NULL).
    pub rows: Vec<Vec<Option<String>>>,
}

/// A blocking client for the admin HTTP/JSON gateway.
pub struct AdminClient {
    config: AdminConfig,
}

impl AdminClient {
    /// Wrap the connection settings (no socket is opened until a call is made).
    #[must_use]
    pub const fn new(config: AdminConfig) -> Self {
        Self { config }
    }

    /// `GET /v1alpha1/status` — engine state.
    ///
    /// # Errors
    /// [`AdminError`] on a missing token, transport failure, non-2xx status, or a
    /// reply that does not decode.
    pub fn status(&self) -> Result<StatusReport, AdminError> {
        let value = self.request("GET", "/v1alpha1/status", None)?;
        decode(value)
    }

    /// `POST /v1alpha1/backup` — trigger a consistent online backup into the
    /// server-side directory `path`, returning its manifest summary.
    ///
    /// # Errors
    /// As [`status`](Self::status); also a `400` when the target is non-empty.
    pub fn backup(&self, path: &str) -> Result<ManifestSummary, AdminError> {
        let value = self.request("POST", "/v1alpha1/backup", Some(&json!({ "path": path })))?;
        // The route wraps the manifest: `{"manifest": {…}}`.
        let manifest = value
            .get("manifest")
            .cloned()
            .ok_or_else(|| AdminError::Decode("backup reply missing `manifest`".to_owned()))?;
        decode(manifest)
    }

    /// `POST /v1alpha1/restore-plan` — validate a backup directory without
    /// applying it.
    ///
    /// # Errors
    /// As [`status`](Self::status). A missing directory or a failed checksum is a
    /// *valid* reply with `valid = false`, not an error.
    pub fn restore_plan(&self, path: &str) -> Result<RestorePlan, AdminError> {
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
    /// As [`status`](Self::status); a `404` for an unknown table.
    pub fn segments(&self, table: &str) -> Result<TableData, AdminError> {
        let value = self.request(
            "POST",
            "/v1alpha1/segments",
            Some(&json!({ "table": table })),
        )?;
        decode(value)
    }

    /// One request/response round-trip. Returns the parsed 2xx JSON body, or an
    /// [`AdminError`] carrying the gateway's failure.
    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, AdminError> {
        let token = self.config.token.as_deref().ok_or(AdminError::NoToken)?;
        let body = body.map(ToString::to_string).unwrap_or_default();
        let raw = self.round_trip(method, path, &body, token)?;
        let (code, payload) = parse_http_response(&raw)?;
        if code.starts_with('2') {
            serde_json::from_str(&payload)
                .map_err(|e| AdminError::Decode(format!("invalid JSON reply: {e}")))
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
            Err(AdminError::Status { code, message })
        }
    }

    /// Open a connection, send one HTTP/1.1 request, and read the whole reply.
    fn round_trip(
        &self,
        method: &str,
        path: &str,
        body: &str,
        token: &str,
    ) -> Result<String, AdminError> {
        let host = &self.config.host;
        let port = self.config.port;
        let mut stream = TcpStream::connect((host.as_str(), port)).map_err(|e| {
            AdminError::Transport(format!("connecting to the admin API at {host}:{port}: {e}"))
        })?;
        stream.set_read_timeout(Some(ADMIN_TIMEOUT)).ok();
        stream.set_write_timeout(Some(ADMIN_TIMEOUT)).ok();
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
            .map_err(|e| AdminError::Transport(format!("sending the admin request: {e}")))?;
        stream.flush().ok();
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|e| AdminError::Transport(format!("reading the admin reply: {e}")))?;
        String::from_utf8(raw)
            .map_err(|_| AdminError::Transport("admin reply was not valid UTF-8".to_owned()))
    }
}

/// Decode a JSON value into a typed reply, mapping a shape mismatch to
/// [`AdminError::Decode`].
fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, AdminError> {
    serde_json::from_value(value)
        .map_err(|e| AdminError::Decode(format!("unexpected admin reply shape: {e}")))
}

/// Split an HTTP response into its status code (the 3-digit token of the status
/// line) and its body (everything past the blank line).
fn parse_http_response(raw: &str) -> Result<(String, String), AdminError> {
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(|| {
        AdminError::Transport("malformed admin reply: no header terminator".to_owned())
    })?;
    let status_line = head.lines().next().unwrap_or_default();
    // "HTTP/1.1 200 OK" → "200".
    let code = status_line.split_whitespace().nth(1).ok_or_else(|| {
        AdminError::Transport(format!("malformed admin status line: {status_line:?}"))
    })?;
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
            Err(AdminError::Transport(_))
        ));
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
    fn table_data_decodes_rows_and_null_cells() {
        // The reply's `columns` header is present on the wire but ignored — only
        // the rows are decoded (read positionally by `\inspect-segment`).
        let body = r#"{
            "columns": [{"name":"segment","type":"text"},{"name":"bytes","type":"int8"}],
            "rows": [["seg-0001", "4096"], ["seg-0002", null]]
        }"#;
        let data: TableData = serde_json::from_str(body).expect("decode");
        assert_eq!(data.rows[0][0].as_deref(), Some("seg-0001"));
        assert!(data.rows[1][1].is_none(), "SQL NULL → None");
    }

    #[test]
    fn missing_token_is_refused_without_a_socket() {
        let client = AdminClient::new(AdminConfig {
            host: "127.0.0.1".to_owned(),
            // An unused port — the call must short-circuit on the missing token
            // before any connect is attempted.
            port: 1,
            token: None,
        });
        assert!(matches!(client.status(), Err(AdminError::NoToken)));
    }
}
