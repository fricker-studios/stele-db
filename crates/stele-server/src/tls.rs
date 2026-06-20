//! Shared TLS plumbing for the admin / control-plane transports ([STL-311]).
//!
//! The admin surface ([STL-254], [ADR-0016]) is two listeners — a tonic gRPC
//! service and the ops HTTP listener that hosts the `/v1alpha1` JSON gateway.
//! Both reuse the **same** certificate material pg-wire loads from the `[tls]`
//! section ([STL-251]) rather than a second cert config: this module wraps that
//! material in an [`AcceptorSource`] the two accept loops read per connection,
//! so a SIGHUP-driven rotation ([STL-293]) and the optional mTLS client-CA reach
//! the admin surface exactly as they reach pg-wire.
//!
//! Unlike pg-wire — which negotiates encryption in-band with `SSLRequest` — gRPC
//! and HTTP speak TLS from the first byte, so each transport runs the rustls
//! handshake itself ([`handshake`]) on the [`TlsAcceptor`] this source hands out.
//!
//! [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
//! [STL-293]: https://allegromusic.atlassian.net/browse/STL-293
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use std::io;
use std::time::Duration;

use stele_pgwire::{ServerTls, TlsReloader};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// How long a peer may take to complete the TLS handshake before the connection
/// is dropped. A bound on the handshake keeps a stalled or non-TLS client from
/// holding a per-connection task open indefinitely; well-behaved clients finish
/// in a single round trip well inside this.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// ALPN for the admin gRPC listener: gRPC is HTTP/2, which tonic's client expects
/// negotiated by ALPN ([RFC 7540 §3.3]). pg-wire (no ALPN) and the HTTP/1.1 ops
/// listener (no ALPN; clients that offer `h2` fall back to HTTP/1.1) take the
/// plain acceptor instead.
///
/// [RFC 7540 §3.3]: https://www.rfc-editor.org/rfc/rfc7540#section-3.3
const GRPC_ALPN: &[&[u8]] = &[b"h2"];

/// Where an admin-surface accept loop reads its live TLS acceptor from.
///
/// Two shapes, mirroring how pg-wire is wired ([`crate::run`]):
///
/// * [`AcceptorSource::reloading`] — backed by the [`TlsReloader`] the `[tls]`
///   section loads. Each [`acceptor`](Self::acceptor) call reads the **live**
///   acceptor, so a SIGHUP rotation ([STL-293]) reaches the admin surface too.
/// * [`AcceptorSource::fixed`] — backed by a single [`ServerTls`], for the
///   ephemeral self-signed fallback a non-loopback bind without `[tls]` mints
///   ([STL-304]); there is nothing to reload.
///
/// Cloning is cheap (an `Arc` bump) — every clone hands out the same live
/// acceptor, so the copy each listener holds stays in lock-step with rotations.
///
/// [STL-293]: https://allegromusic.atlassian.net/browse/STL-293
/// [STL-304]: https://allegromusic.atlassian.net/browse/STL-304
#[derive(Clone)]
pub enum AcceptorSource {
    /// A fixed context — the ephemeral self-signed fallback ([STL-304]).
    Fixed(ServerTls),
    /// The live context behind a hot-reloadable [`TlsReloader`] ([STL-293]).
    Reloading(TlsReloader),
}

impl AcceptorSource {
    /// A source over a single fixed [`ServerTls`] — the self-signed fallback.
    #[must_use]
    pub fn fixed(tls: &ServerTls) -> Self {
        Self::Fixed(tls.clone())
    }

    /// A source over a [`TlsReloader`], so admin connections pick up a rotated
    /// certificate without a restart.
    #[must_use]
    pub fn reloading(reloader: &TlsReloader) -> Self {
        Self::Reloading(reloader.clone())
    }

    /// The acceptor for the **ops HTTP** listener (no ALPN — HTTP/1.1). Read once
    /// per connection so the [`Reloading`](Self::Reloading) variant always
    /// handshakes with the currently installed certificate.
    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        match self {
            Self::Fixed(tls) => tls.acceptor(),
            Self::Reloading(reloader) => reloader.acceptor(),
        }
    }

    /// The acceptor for the **admin gRPC** listener — the same certificate
    /// material, tagged with HTTP/2 ALPN (`h2`) so tonic's client negotiates the
    /// protocol. Also read per connection for hot-reload.
    #[must_use]
    pub fn grpc_acceptor(&self) -> TlsAcceptor {
        match self {
            Self::Fixed(tls) => tls.acceptor_with_alpn(GRPC_ALPN),
            Self::Reloading(reloader) => reloader.acceptor_with_alpn(GRPC_ALPN),
        }
    }
}

/// Run the rustls server handshake on a freshly accepted `stream`, bounded by a
/// fixed handshake timeout.
///
/// Returns the encrypted stream, or an [`io::Error`] when the handshake fails (a
/// non-TLS client, a rejected mTLS certificate) or times out — the caller logs
/// and drops the connection. The caller picks the `acceptor` for its transport
/// ([`AcceptorSource::acceptor`] for HTTP, [`AcceptorSource::grpc_acceptor`] for
/// gRPC).
pub async fn handshake(
    acceptor: TlsAcceptor,
    stream: TcpStream,
) -> io::Result<TlsStream<TcpStream>> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
        Ok(result) => result,
        Err(_elapsed) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "TLS handshake did not complete within the timeout",
        )),
    }
}
