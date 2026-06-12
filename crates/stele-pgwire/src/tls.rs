//! TLS on the pg-wire startup path ([STL-251]).
//!
//! Postgres clients negotiate encryption with an `SSLRequest` before the
//! `StartupMessage`: the server answers `S` (proceed with a TLS handshake) or
//! `N` (no TLS — the client may fall back to plaintext or hang up, per its
//! `sslmode`). This module owns the server half of that decision: loading the
//! certificate/key (and the optional client CA for **mTLS**) into a
//! [`rustls`] acceptor, and the policy for what happens to clients that try
//! to stay plaintext.
//!
//! rustls ships only modern, safe cipher suites — there is no way to configure
//! a weak one — which is exactly the "modern ciphers only; secure defaults"
//! posture [docs/10 §4](../../../docs/10-security-and-compliance.md#4-data-protection--encryption)
//! requires. The *posture* decision (when a plaintext listener is even allowed
//! to start) lives with the daemon config in `stele-server`; this module only
//! enforces the per-connection half.
//!
//! [STL-251]: https://allegromusic.atlassian.net/browse/STL-251

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

/// What happens to a client that skips the `SSLRequest` and opens with a
/// plaintext `StartupMessage` while the server has TLS configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// TLS is offered (an `SSLRequest` gets `S`) but plaintext startup is
    /// still accepted — the migration posture.
    Optional,
    /// Every connection must negotiate TLS; a plaintext `StartupMessage` is
    /// refused with a `FATAL` error (SQLSTATE `28000`, the same class Postgres
    /// uses for a `pg_hba.conf` "SSL off" reject).
    Required,
}

/// The operator-supplied TLS material and policy, as paths — parsed out of
/// `stele.toml` by `stele-server` and turned into a live acceptor by
/// [`ServerTls::load`].
#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// PEM server certificate chain (leaf first).
    pub cert: PathBuf,
    /// PEM private key (PKCS#8, PKCS#1, or SEC1).
    pub key: PathBuf,
    /// PEM CA bundle for **mTLS**: when set, every client must present a
    /// certificate that chains to this CA or the handshake is rejected.
    pub client_ca: Option<PathBuf>,
    /// Plaintext policy once TLS is configured.
    pub mode: TlsMode,
}

/// Errors loading TLS material into an acceptor. These are configuration
/// errors: they happen once at boot, never per connection.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("reading {what} from {path}: {source}")]
    Pem {
        what: &'static str,
        path: PathBuf,
        source: rustls_pki_types::pem::Error,
    },

    #[error("{what} file {path} contains no PEM-encoded entries")]
    Empty { what: &'static str, path: PathBuf },

    #[error("building TLS server config: {0}")]
    Config(#[from] rustls::Error),

    #[error("building mTLS client verifier: {0}")]
    ClientVerifier(#[from] rustls::server::VerifierBuilderError),
}

/// A loaded, ready-to-accept TLS context: the rustls acceptor plus the
/// plaintext policy the connection handler enforces.
#[derive(Clone)]
pub struct ServerTls {
    pub(crate) acceptor: TlsAcceptor,
    pub(crate) mode: TlsMode,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TlsAcceptor holds key material; print the policy only.
        f.debug_struct("ServerTls")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl ServerTls {
    /// Load certificate, key, and (optionally) the mTLS client CA from disk
    /// and build the acceptor.
    ///
    /// # Errors
    /// Returns a [`TlsError`] when a file is unreadable / not PEM, the
    /// key doesn't match a supported format, or rustls rejects the pair.
    pub fn load(settings: &TlsSettings) -> Result<Self, TlsError> {
        let certs = load_certs("certificate", &settings.cert)?;
        let key = PrivateKeyDer::from_pem_file(&settings.key).map_err(|source| TlsError::Pem {
            what: "private key",
            path: settings.key.clone(),
            source,
        })?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)?;

        let config = match &settings.client_ca {
            // mTLS: the client must present a certificate chaining to this CA.
            Some(ca_path) => {
                let mut roots = rustls::RootCertStore::empty();
                for ca in load_certs("client CA", ca_path)? {
                    roots.add(ca)?;
                }
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()?;
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        }
        .with_single_cert(certs, key)?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            mode: settings.mode,
        })
    }
}

/// Every PEM certificate in `path`, in order (a chain file works as-is).
fn load_certs(what: &'static str, path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|source| TlsError::Pem {
            what,
            path: path.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Pem {
            what,
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsError::Empty {
            what,
            path: path.to_path_buf(),
        });
    }
    Ok(certs)
}
