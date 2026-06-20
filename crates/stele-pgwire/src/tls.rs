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
//! Once an mTLS handshake completes, [`peer_identity`] turns the verified client
//! certificate's subject CN/SAN into a [`CertIdentity`] — the authenticated
//! principal the session stamps into provenance ([STL-291]).
//!
//! [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
//! [STL-291]: https://allegromusic.atlassian.net/browse/STL-291

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;
use x509_parser::prelude::{FromDer as _, GeneralName, X509Certificate};

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

    #[error("generating a self-signed certificate: {0}")]
    SelfSigned(#[from] rcgen::Error),
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
        let client_ca = settings
            .client_ca
            .as_deref()
            .map(|path| load_certs("client CA", path))
            .transpose()?;
        Self::from_material(certs, key, client_ca, settings.mode)
    }

    /// Build an acceptor from a freshly generated, ephemeral self-signed
    /// certificate — the daemon's fallback when a non-dev server is started
    /// without operator-supplied `[tls]` material on a non-loopback bind
    /// ([STL-304]).
    ///
    /// The listener is then **encrypted** (the rustls handshake runs) rather
    /// than refusing to boot or silently serving plaintext, but the certificate
    /// is **unauthenticated** — there is no CA chain, so a client cannot verify
    /// it is talking to the right server, and a restart mints a fresh
    /// certificate. The caller is expected to warn the operator loudly and to
    /// replace it with a CA-issued cert before production. The private key never
    /// touches disk.
    ///
    /// # Errors
    /// Returns a [`TlsError`] if certificate generation or the rustls config
    /// build fails.
    ///
    /// [STL-304]: https://allegromusic.atlassian.net/browse/STL-304
    pub fn self_signed(mode: TlsMode) -> Result<Self, TlsError> {
        let (certs, key) = generate_self_signed()?;
        Self::from_material(certs, key, None, mode)
    }

    /// Assemble the rustls acceptor from already-parsed certificate material:
    /// the server chain + key, the optional mTLS client-CA roots, and the
    /// plaintext policy. Shared by [`Self::load`] (PEM from disk) and
    /// [`Self::self_signed`] (generated in memory).
    fn from_material(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        client_ca: Option<Vec<CertificateDer<'static>>>,
        mode: TlsMode,
    ) -> Result<Self, TlsError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)?;

        let config = match client_ca {
            // mTLS: the client must present a certificate chaining to this CA.
            Some(ca_certs) => {
                let mut roots = rustls::RootCertStore::empty();
                for ca in ca_certs {
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
            mode,
        })
    }
}

/// The verified identity of a connection's peer, extracted from the client
/// certificate it presented during an **mTLS** handshake ([STL-291]).
///
/// rustls has already checked that the certificate chains to the configured
/// `client_ca` before this is read — a peer that fails verification never
/// reaches the startup message (`negotiate_startup`). So this name is
/// *authenticated*: it states who is on the other end of the connection, not
/// merely who the client claims to be. The session adopts it as the write
/// principal so provenance records the verified identity (precedence and the
/// `trust`/`scram` interplay live in `run_session`).
///
/// The `name` is the certificate's subject **Common Name** when present — the
/// field Postgres `cert` authentication maps onto a user — and otherwise the
/// first **Subject Alternative Name** (a DNS name, e-mail, or URI). A
/// certificate carrying neither a CN nor a usable SAN names no principal, so it
/// yields `None`.
///
/// [STL-291]: https://allegromusic.atlassian.net/browse/STL-291
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CertIdentity {
    /// The canonical principal name: the subject CN, else the first SAN.
    pub(crate) name: String,
}

/// The verified [`CertIdentity`] of `conn`'s peer, if it presented a client
/// certificate during the (already-verified) mTLS handshake.
///
/// `None` for a plain-TLS connection — no `client_ca` was configured, so no
/// certificate was requested — or for a certificate whose subject names nothing
/// usable. rustls returns the peer chain end-entity-first, so the leaf is the
/// first element.
pub(crate) fn peer_identity(conn: &rustls::ServerConnection) -> Option<CertIdentity> {
    let end_entity = conn.peer_certificates()?.first()?;
    identity_from_cert_der(end_entity.as_ref())
}

/// Parse one end-entity certificate's DER into a [`CertIdentity`]: the subject
/// Common Name if present, else the first usable Subject Alternative Name.
///
/// Split out from [`peer_identity`] so the CN-vs-SAN precedence is unit-testable
/// against rcgen-minted certificates without standing up a TLS handshake.
fn identity_from_cert_der(der: &[u8]) -> Option<CertIdentity> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let cn = cert
        .subject()
        .iter_common_name()
        .filter_map(|attr| attr.as_str().ok())
        .find(|cn| !cn.is_empty())
        .map(str::to_owned);
    let name = cn.or_else(|| first_subject_alt_name(&cert))?;
    Some(CertIdentity { name })
}

/// The first usable Subject Alternative Name — a DNS name, e-mail, or URI — in
/// document order. The other SAN kinds (IP address, directory name, …) are not
/// meaningful session principals, so they are skipped.
fn first_subject_alt_name(cert: &X509Certificate) -> Option<String> {
    let san = cert.subject_alternative_name().ok()??;
    san.value.general_names.iter().find_map(|gn| match gn {
        GeneralName::DNSName(s) | GeneralName::RFC822Name(s) | GeneralName::URI(s)
            if !s.is_empty() =>
        {
            Some((*s).to_owned())
        }
        _ => None,
    })
}

/// Generate an ephemeral self-signed server certificate plus its key, for the
/// `localhost` SAN — the encryption material behind [`ServerTls::self_signed`].
/// The certificate is intentionally unauthenticated (no CA), so it only ever
/// backs the "encrypt rather than refuse to boot" fallback.
fn generate_self_signed() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError>
{
    let key = rcgen::KeyPair::generate()?;
    let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()])?;
    let cert = params.self_signed(&key)?;
    let cert_der = cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
    Ok((vec![cert_der], key_der))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_signed_builds_a_usable_acceptor_for_each_mode() {
        // The ephemeral fallback (STL-304) must yield a servable acceptor:
        // `with_single_cert` inside `from_material` validates that the generated
        // key matches its certificate, so an `Ok` return means a real handshake
        // can use it. The end-to-end handshake is exercised in tests/tls_wire.rs.
        for mode in [TlsMode::Required, TlsMode::Optional] {
            let tls = ServerTls::self_signed(mode).expect("generate self-signed acceptor");
            assert_eq!(tls.mode, mode);
        }
    }

    /// A self-signed leaf certificate (DER) with the given subject CN and DNS
    /// SANs — the input to the identity parser. Self-signed is fine: the parser
    /// reads the subject, it does not verify the chain (rustls already did).
    fn leaf_der(common_name: Option<&str>, dns_sans: &[&str]) -> Vec<u8> {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let sans: Vec<String> = dns_sans.iter().map(|s| (*s).to_owned()).collect();
        let mut params = rcgen::CertificateParams::new(sans).expect("cert params");
        // rcgen seeds a default CN ("rcgen self signed cert"); start from an empty
        // subject so the test controls exactly which CN (if any) the cert carries.
        params.distinguished_name = rcgen::DistinguishedName::new();
        if let Some(cn) = common_name {
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, cn);
        }
        let cert = params.self_signed(&key).expect("self-sign leaf");
        cert.der().as_ref().to_vec()
    }

    #[test]
    fn identity_prefers_the_subject_common_name() {
        // CN present alongside a SAN: the CN is the principal (Postgres `cert`
        // auth semantics).
        let der = leaf_der(Some("svc-billing"), &["billing.svc.local"]);
        let id = identity_from_cert_der(&der).expect("an identity");
        assert_eq!(id.name, "svc-billing");
    }

    #[test]
    fn identity_falls_back_to_the_first_san_without_a_cn() {
        // No CN: the first SAN, in document order, becomes the principal.
        let der = leaf_der(None, &["primary.example", "secondary.example"]);
        let id = identity_from_cert_der(&der).expect("an identity");
        assert_eq!(id.name, "primary.example");
    }

    #[test]
    fn identity_is_none_without_a_cn_or_san() {
        // A subjectless certificate names no principal — the session then falls
        // back to the startup `user`, exactly as a plain-TLS connection does.
        let der = leaf_der(None, &[]);
        assert!(identity_from_cert_der(&der).is_none());
    }

    #[test]
    fn identity_is_none_for_non_certificate_bytes() {
        assert!(identity_from_cert_der(b"this is not a certificate").is_none());
    }
}
