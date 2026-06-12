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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, instrument, warn};

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
}

impl OpsServer {
    #[must_use]
    pub const fn new(listen_addr: SocketAddr, state: Arc<OpsState>) -> Self {
        Self { listen_addr, state }
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
        })
    }
}

/// An [`OpsServer`] that has already bound its listen socket.
pub struct BoundOpsServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    state: Arc<OpsState>,
}

impl BoundOpsServer {
    /// The address the listen socket is actually bound to — the resolved port
    /// when the caller asked for an ephemeral `:0`.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and answer connections until cancelled by the caller.
    #[instrument(skip_all, fields(addr = %self.local_addr))]
    pub async fn serve(self) -> io::Result<()> {
        info!(addr = %self.local_addr, "stele-server: ops listener up (/metrics /healthz /readyz)");
        loop {
            let (stream, peer) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "ops accept failed");
                    continue;
                }
            };
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state).await {
                    debug!(%peer, error = %e, "ops connection closed with error");
                }
            });
        }
    }
}

/// Serve exactly one request on `stream`, then close (`Connection: close` —
/// scrapers reconnect per scrape, so keep-alive buys nothing here).
async fn handle_connection(mut stream: TcpStream, state: Arc<OpsState>) -> io::Result<()> {
    let Some(head) = read_request_head(&mut stream).await? else {
        return Ok(()); // peer closed (or dribbled past the cap) before a full head
    };
    let response = respond(&head, &state);
    write_response(&mut stream, &response).await?;
    stream.shutdown().await
}

/// Read until the blank line that ends the request head, returning the raw
/// head bytes as a lossy string. `None` if the peer closes first, exceeds
/// [`MAX_HEAD_BYTES`], or stalls past [`READ_TIMEOUT`].
async fn read_request_head(stream: &mut TcpStream) -> io::Result<Option<String>> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        let read = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk)).await;
        let n = match read {
            Ok(result) => result?,
            Err(_elapsed) => return Ok(None),
        };
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Ok(None);
        }
    }
}

/// A fully-formed response: status line tail, content type, body.
struct Response {
    status: &'static str,
    content_type: &'static str,
    body: String,
}

/// Route one request head to its response. Pure (no I/O), so the routing
/// table is unit-testable without sockets.
fn respond(head: &str, state: &OpsState) -> Response {
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    // Probes sometimes append cache-busting queries; route on the path alone.
    let path = target.split('?').next().unwrap_or_default();

    if method != "GET" {
        return Response {
            status: "405 Method Not Allowed",
            content_type: "text/plain; charset=utf-8",
            body: "method not allowed\n".to_owned(),
        };
    }
    match path {
        "/healthz" => Response {
            status: "200 OK",
            content_type: "text/plain; charset=utf-8",
            body: "ok\n".to_owned(),
        },
        "/readyz" => match state.readiness() {
            Ok(()) => Response {
                status: "200 OK",
                content_type: "text/plain; charset=utf-8",
                body: "ready\n".to_owned(),
            },
            Err(reason) => Response {
                status: "503 Service Unavailable",
                content_type: "text/plain; charset=utf-8",
                body: reason.to_owned(),
            },
        },
        "/metrics" => state.render_metrics().map_or_else(
            || Response {
                status: "503 Service Unavailable",
                content_type: "text/plain; charset=utf-8",
                body: "starting: recovery has not completed\n".to_owned(),
            },
            |body| Response {
                status: "200 OK",
                content_type: METRICS_CONTENT_TYPE,
                body,
            },
        ),
        _ => Response {
            status: "404 Not Found",
            content_type: "text/plain; charset=utf-8",
            body: "not found\n".to_owned(),
        },
    }
}

async fn write_response(stream: &mut TcpStream, response: &Response) -> io::Result<()> {
    // `Allow` is mandatory on 405 (RFC 9110 §15.5.6); harmless to scope it there.
    let allow = if response.status.starts_with("405") {
        "Allow: GET\r\n"
    } else {
        ""
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

    #[test]
    fn healthz_is_ok_even_before_ready() {
        let state = OpsState::new();
        let r = respond(&get("/healthz"), &state);
        assert_eq!(r.status, "200 OK");
        assert_eq!(r.body, "ok\n");
    }

    #[test]
    fn readyz_and_metrics_are_503_before_recovery() {
        let state = OpsState::new();
        let r = respond(&get("/readyz"), &state);
        assert_eq!(r.status, "503 Service Unavailable");
        assert!(r.body.contains("recovery"), "{}", r.body);
        let m = respond(&get("/metrics"), &state);
        assert_eq!(m.status, "503 Service Unavailable");
    }

    #[test]
    fn unknown_path_is_404_and_non_get_is_405() {
        let state = OpsState::new();
        assert_eq!(respond(&get("/nope"), &state).status, "404 Not Found");
        let r = respond("POST /metrics HTTP/1.1\r\n\r\n", &state);
        assert_eq!(r.status, "405 Method Not Allowed");
    }

    #[test]
    fn query_strings_are_stripped_from_the_route() {
        let state = OpsState::new();
        let r = respond(&get("/healthz?probe=1"), &state);
        assert_eq!(r.status, "200 OK");
    }

    #[test]
    fn garbage_request_line_is_refused_not_a_panic() {
        let state = OpsState::new();
        assert_eq!(respond("", &state).status, "405 Method Not Allowed");
        assert_eq!(respond("GET\r\n\r\n", &state).status, "404 Not Found");
    }
}
