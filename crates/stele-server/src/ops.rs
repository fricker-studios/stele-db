//! The **ops HTTP listener**: `/metrics`, `/healthz`, `/readyz` ([STL-253]).
//!
//! A dedicated plain-HTTP/1.1 port, distinct from pg-wire, for the operator
//! surface — the listener the admin/control-plane HTTP gateway will share
//! ([ADR-0016]). Three endpoints:
//!
//! * **`GET /healthz`** — liveness: `200` whenever the process can accept a
//!   TCP connection and answer. Says nothing about the engine.
//! * **`GET /readyz`** — readiness: `200` only once recovery has completed
//!   **and** no table's WAL is poisoned
//!   ([`SessionHandle::is_poisoned`](stele_pgwire::SessionHandle::is_poisoned),
//!   [STL-217]). The listener binds *before* recovery runs, so an orchestrator
//!   watches this flip `503 → 200` across a (re)start — and back to `503` if a
//!   failed fsync poisons the engine, which per the WAL contract must be
//!   resolved by a restart into recovery.
//! * **`GET /metrics`** — the session's metric registry in the Prometheus text
//!   exposition format ([`Metrics::render`](stele_common::metrics::Metrics::render)).
//!   `503` until recovery completes (there is no registry to render yet).
//!
//! The server is hand-rolled over `tokio` — a request-line parse and a fixed
//! response, far below the threshold where an HTTP framework would pay for
//! its dependency tree (the facade ticket explicitly avoids heavyweight deps).
//!
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use stele_common::metrics::SharedMetrics;
use stele_pgwire::SharedSession;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, info, instrument, warn};

use crate::admin::http::{AdminHttpResponder, HttpRequest, MAX_BODY_BYTES};
use crate::tls::AcceptorSource;

/// The path prefix the admin / control-plane HTTP gateway owns ([STL-254]).
const ADMIN_PREFIX: &str = "/v1alpha1/";

/// How long a client may dribble its request head before the connection is
/// dropped — scrapers send their `GET` in one segment, so this only bounds
/// misbehaving peers.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The most request-head bytes accepted before the connection is dropped.
const MAX_HEAD_BYTES: usize = 8 * 1024;

/// The Prometheus text exposition content type ([`Metrics::render`]).
///
/// [`Metrics::render`]: stele_common::metrics::Metrics::render
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Shared readiness state behind the probe endpoints.
///
/// Constructed **before** recovery (not ready), then flipped once by
/// [`set_ready`](Self::set_ready) when the recovered engine is in hand. From
/// then on `/readyz` tracks the engine's poison state live.
#[derive(Default)]
pub struct OpsState {
    /// `None` until recovery completes; then the serving session and its
    /// metric registry (cached here so a scrape never takes the engine lock
    /// just to reach the registry).
    ready: Mutex<Option<Ready>>,
    /// `None` until recovery completes; then the admin / control-plane HTTP
    /// gateway ([ADR-0016], [STL-254]) that handles `/v1alpha1/…`. Installed
    /// alongside [`set_ready`](Self::set_ready) because it needs the recovered
    /// engine. Always present in a running server (it authenticates every
    /// request, rejecting all when no token is configured).
    admin: Mutex<Option<Arc<dyn AdminHttpResponder>>>,
}

struct Ready {
    session: SharedSession,
    metrics: SharedMetrics,
}

impl OpsState {
    /// A fresh, not-ready state: `/readyz` and `/metrics` answer `503`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the admin / control-plane HTTP gateway that answers `/v1alpha1/…`
    /// ([STL-254]). Done once the recovered engine is in hand.
    pub fn set_admin(&self, admin: Arc<dyn AdminHttpResponder>) {
        *self.admin.lock().unwrap_or_else(PoisonError::into_inner) = Some(admin);
    }

    /// The installed admin gateway, if any.
    fn admin(&self) -> Option<Arc<dyn AdminHttpResponder>> {
        self.admin
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Mark recovery complete: `/readyz` flips to `200` (as long as `session`
    /// stays unpoisoned) and `/metrics` starts rendering its registry.
    pub fn set_ready(&self, session: SharedSession) {
        let metrics = session
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .metrics();
        *self.ready.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(Ready { session, metrics });
    }

    /// `Ok` when the server should report ready; `Err(reason)` otherwise.
    fn readiness(&self) -> Result<(), &'static str> {
        // Clone the session handle out so the engine lock below is never taken
        // while the readiness lock is held.
        let session = self
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|ready| Arc::clone(&ready.session));
        let Some(session) = session else {
            return Err("starting: recovery has not completed\n");
        };
        let poisoned = session
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_poisoned();
        if poisoned {
            // A failed fsync is a crash, not a clean abort ([STL-217]): the
            // engine refuses writes until restarted into recovery, so this
            // instance must be rotated out.
            Err("wal poisoned: restart the server to recover\n")
        } else {
            Ok(())
        }
    }

    /// The metric exposition, or `None` before recovery completes.
    fn render_metrics(&self) -> Option<String> {
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|ready| ready.metrics.render())
    }
}

/// Ops listener entry point: bind, accept, answer.
pub struct OpsServer {
    listen_addr: SocketAddr,
    state: Arc<OpsState>,
    /// TLS for the listener ([STL-311]): when set, every connection (the metrics
    /// scrape, the probes, and the `/v1alpha1` admin gateway) is encrypted with
    /// the shared pg-wire certificate material; `None` serves plaintext (dev /
    /// loopback, the secure-defaults posture [`crate::run`] decides).
    tls: Option<AcceptorSource>,
}

impl OpsServer {
    #[must_use]
    pub const fn new(listen_addr: SocketAddr, state: Arc<OpsState>) -> Self {
        Self {
            listen_addr,
            state,
            tls: None,
        }
    }

    /// Encrypt the listener with the shared TLS material ([STL-311]). `None`
    /// keeps it plaintext (the default), so callers can pass the daemon's
    /// resolved posture through unconditionally.
    #[must_use]
    pub fn with_tls(mut self, tls: Option<AcceptorSource>) -> Self {
        self.tls = tls;
        self
    }

    /// Bind the listen socket now, returning a [`BoundOpsServer`] whose
    /// [`local_addr`](BoundOpsServer::local_addr) reports the address actually
    /// bound — the same no-race ephemeral-port shape as the pg-wire listener
    /// (STL-152).
    pub async fn bind(self) -> io::Result<BoundOpsServer> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(BoundOpsServer {
            listener,
            local_addr,
            state: self.state,
            tls: self.tls,
        })
    }
}

/// An [`OpsServer`] that has already bound its listen socket.
pub struct BoundOpsServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    state: Arc<OpsState>,
    tls: Option<AcceptorSource>,
}

impl BoundOpsServer {
    /// The address the listen socket is actually bound to — the resolved port
    /// when the caller asked for an ephemeral `:0`.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and answer connections until cancelled by the caller. When TLS is
    /// configured ([STL-311]) every connection is handshaked before its request
    /// is read; the handshake runs inside the per-connection task, so a stalled
    /// peer never blocks accepting the next one.
    #[instrument(skip_all, fields(addr = %self.local_addr, tls = self.tls.is_some()))]
    pub async fn serve(self) -> io::Result<()> {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        info!(addr = %self.local_addr, %scheme, "stele-server: ops listener up (/metrics /healthz /readyz)");
        loop {
            let (stream, peer) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "ops accept failed");
                    continue;
                }
            };
            let state = Arc::clone(&self.state);
            let tls = self.tls.clone();
            tokio::spawn(async move {
                match tls {
                    // Plaintext: serve the raw TCP stream directly.
                    None => {
                        if let Err(e) = handle_connection(stream, state).await {
                            debug!(%peer, error = %e, "ops connection closed with error");
                        }
                    }
                    // TLS: handshake first, then serve the encrypted stream. A
                    // failed handshake (a plaintext client, a rejected mTLS cert)
                    // is logged and the connection dropped.
                    Some(tls) => match crate::tls::handshake(tls.acceptor(), stream).await {
                        Ok(stream) => {
                            if let Err(e) = handle_connection(stream, state).await {
                                debug!(%peer, error = %e, "ops connection closed with error");
                            }
                        }
                        Err(e) => debug!(%peer, error = %e, "ops TLS handshake failed"),
                    },
                }
            });
        }
    }
}

/// Serve exactly one request on `stream`, then close (`Connection: close` —
/// scrapers reconnect per scrape, so keep-alive buys nothing here). Generic over
/// the stream so the same path serves a raw [`tokio::net::TcpStream`] or a
/// TLS-wrapped one ([STL-311]).
async fn handle_connection<S>(mut stream: S, state: Arc<OpsState>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(()); // peer closed (or dribbled past a cap) before a full request
    };
    let response = route(&request, &state);
    write_response(&mut stream, &response).await?;
    stream.shutdown().await
}

/// A raw request: the head (everything up to and including the blank line) and
/// the body bytes (empty unless the request carried a `Content-Length`).
struct RawRequest {
    head: String,
    body: Vec<u8>,
}

/// Read one request: the head, then — if it declares a `Content-Length` — the
/// body. `None` if the peer closes first, exceeds [`MAX_HEAD_BYTES`] /
/// [`MAX_BODY_BYTES`], or stalls past [`READ_TIMEOUT`]. Body-less probes (the
/// metrics/health scrapes) read exactly the head and return an empty body.
async fn read_request<S>(stream: &mut S) -> io::Result<Option<RawRequest>>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    // Read until the CRLFCRLF that ends the head.
    let head_end = loop {
        let read = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk)).await;
        let n = match read {
            Ok(result) => result?,
            Err(_elapsed) => return Ok(None),
        };
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(None);
        }
    };

    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut body = buf[head_end..].to_vec();
    // Read the rest of the body, if any, per the declared length.
    if let Some(len) = content_length(&head) {
        if len > MAX_BODY_BYTES {
            return Ok(None);
        }
        while body.len() < len {
            let read = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk)).await;
            let n = match read {
                Ok(result) => result?,
                Err(_elapsed) => return Ok(None),
            };
            if n == 0 {
                break; // peer closed early; route on what we have
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(len);
    }
    Ok(Some(RawRequest { head, body }))
}

/// Parse the `Content-Length` header (case-insensitive) from `head`.
fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
}

/// The `(method, path)` of a request head, with any query string stripped from
/// the path.
fn request_line(head: &str) -> (&str, &str) {
    let line = head.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let path = target.split('?').next().unwrap_or_default();
    (method, path)
}

/// Route a request to either the admin gateway (`/v1alpha1/…`) or the built-in
/// metrics/probe endpoints.
fn route(request: &RawRequest, state: &OpsState) -> Response {
    let (method, path) = request_line(&request.head);
    // `/v1alpha1/…` (with a non-empty tail) belongs to the admin gateway when one
    // is installed; a bare `/v1alpha1/` and everything else fall through to the
    // built-in metrics/probe routes.
    if path
        .strip_prefix(ADMIN_PREFIX)
        .is_some_and(|rest| !rest.is_empty())
    {
        return state.admin().map_or_else(
            || Response::text("503 Service Unavailable", "admin API not ready\n"),
            |admin| {
                let request = HttpRequest {
                    method: method.to_owned(),
                    path: path.to_owned(),
                    authorization: header(&request.head, "authorization").map(str::to_owned),
                    body: request.body.clone(),
                };
                // An admin handler may do slow, blocking engine work (backup, disk
                // reads). Run it with `block_in_place` so it does not starve the
                // shared ops runtime (metrics scrapes, probes, other admin calls) —
                // the gRPC transport offloads the same work with `spawn_blocking`.
                let reply = tokio::task::block_in_place(|| admin.respond(&request));
                Response {
                    status: reply.status,
                    content_type: reply.content_type,
                    allow: reply.allow,
                    body: reply.body,
                }
            },
        );
    }
    respond(method, path, state)
}

/// The first value of header `name` (case-insensitive) in `head`.
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

/// A fully-formed response: status line tail, content type, optional `Allow`
/// (for `405`), and body.
struct Response {
    status: &'static str,
    content_type: &'static str,
    allow: &'static str,
    body: String,
}

impl Response {
    /// A `text/plain` response with no `Allow` header.
    fn text(status: &'static str, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            allow: "",
            body: body.to_owned(),
        }
    }
}

/// Route a built-in metrics/probe request to its response. Pure (no I/O), so the
/// routing table is unit-testable without sockets.
fn respond(method: &str, path: &str, state: &OpsState) -> Response {
    if method != "GET" {
        return Response {
            allow: "GET",
            ..Response::text("405 Method Not Allowed", "method not allowed\n")
        };
    }
    match path {
        "/healthz" => Response::text("200 OK", "ok\n"),
        "/readyz" => match state.readiness() {
            Ok(()) => Response::text("200 OK", "ready\n"),
            Err(reason) => Response::text("503 Service Unavailable", reason),
        },
        "/metrics" => state.render_metrics().map_or_else(
            || {
                Response::text(
                    "503 Service Unavailable",
                    "starting: recovery has not completed\n",
                )
            },
            |body| Response {
                status: "200 OK",
                content_type: METRICS_CONTENT_TYPE,
                allow: "",
                body,
            },
        ),
        _ => Response::text("404 Not Found", "not found\n"),
    }
}

async fn write_response<S>(stream: &mut S, response: &Response) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // `Allow` is mandatory on 405 (RFC 9110 §15.5.6); each route supplies the
    // methods it accepts.
    let allow = if response.allow.is_empty() {
        String::new()
    } else {
        format!("Allow: {}\r\n", response.allow)
    };
    let head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
        response.status,
        response.content_type,
        response.body.len(),
        allow,
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(response.body.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")
    }

    /// Parse a request head and route it the way [`handle_connection`] does for a
    /// built-in (non-admin) request — exercising [`request_line`] + [`respond`].
    fn respond_head(head: &str, state: &OpsState) -> Response {
        let (method, path) = request_line(head);
        respond(method, path, state)
    }

    #[test]
    fn healthz_is_ok_even_before_ready() {
        let state = OpsState::new();
        let r = respond_head(&get("/healthz"), &state);
        assert_eq!(r.status, "200 OK");
        assert_eq!(r.body, "ok\n");
    }

    #[test]
    fn readyz_and_metrics_are_503_before_recovery() {
        let state = OpsState::new();
        let r = respond_head(&get("/readyz"), &state);
        assert_eq!(r.status, "503 Service Unavailable");
        assert!(r.body.contains("recovery"), "{}", r.body);
        let m = respond_head(&get("/metrics"), &state);
        assert_eq!(m.status, "503 Service Unavailable");
    }

    #[test]
    fn unknown_path_is_404_and_non_get_is_405() {
        let state = OpsState::new();
        assert_eq!(respond_head(&get("/nope"), &state).status, "404 Not Found");
        let r = respond_head("POST /metrics HTTP/1.1\r\n\r\n", &state);
        assert_eq!(r.status, "405 Method Not Allowed");
        assert_eq!(r.allow, "GET");
    }

    #[test]
    fn query_strings_are_stripped_from_the_route() {
        let state = OpsState::new();
        let r = respond_head(&get("/healthz?probe=1"), &state);
        assert_eq!(r.status, "200 OK");
    }

    #[test]
    fn garbage_request_line_is_refused_not_a_panic() {
        let state = OpsState::new();
        assert_eq!(respond_head("", &state).status, "405 Method Not Allowed");
        assert_eq!(respond_head("GET\r\n\r\n", &state).status, "404 Not Found");
    }

    #[test]
    fn content_length_is_parsed_case_insensitively() {
        assert_eq!(
            content_length("POST /x HTTP/1.1\r\nContent-Length: 12\r\n\r\n"),
            Some(12)
        );
        assert_eq!(
            content_length("POST /x HTTP/1.1\r\ncontent-length:  7 \r\n\r\n"),
            Some(7)
        );
        assert_eq!(content_length("GET /x HTTP/1.1\r\n\r\n"), None);
    }

    #[test]
    fn admin_prefix_without_a_gateway_is_503() {
        // `/v1alpha1/…` with no admin installed (the default OpsState) is a 503,
        // distinct from a built-in 404 — the surface exists but is not ready.
        let state = OpsState::new();
        let req = RawRequest {
            head: get("/v1alpha1/health"),
            body: Vec::new(),
        };
        assert_eq!(route(&req, &state).status, "503 Service Unavailable");
    }
}
