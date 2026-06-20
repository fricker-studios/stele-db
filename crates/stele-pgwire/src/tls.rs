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
//! On a TLS connection the server also advertises **SCRAM-SHA-256-PLUS** channel
//! binding ([STL-297]): [`ServerTls::endpoint_cbind`] holds the `tls-server-end-point`
//! binding data (RFC 5929) for the configured certificate, computed once at load,
//! which the SASL exchange folds into the client's `c=` check to bind the proof
//! to this TLS channel.
//!
//! [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
//! [STL-291]: https://allegromusic.atlassian.net/browse/STL-291
//! [STL-297]: https://allegromusic.atlassian.net/browse/STL-297

use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};

use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use stele_common::hash::sha256;
use tokio_rustls::TlsAcceptor;
use x509_parser::oid_registry::{OID_PKCS1_SHA256WITHRSA, OID_SIG_ECDSA_WITH_SHA256};
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
    /// The rustls config the [`acceptor`](Self::acceptor) wraps, retained so
    /// [`acceptor_with_alpn`](Self::acceptor_with_alpn) can mint an ALPN-tagged
    /// variant from the same certificate material (the admin gRPC listener needs
    /// HTTP/2 negotiated; STL-311) without re-reading the cert.
    config: std::sync::Arc<ServerConfig>,
    pub(crate) mode: TlsMode,
    /// The RFC 5929 `tls-server-end-point` channel-binding data for the server's
    /// end-entity certificate — the certificate DER hashed with SHA-256 — or
    /// `None` when that certificate's signature hash is not one we bind with
    /// SHA-256. It is computed once at load (the single configured certificate is
    /// what every handshake presents), and gates the **SCRAM-SHA-256-PLUS** offer:
    /// `Some` ⇒ the SASL exchange advertises PLUS and folds this into the `c=`
    /// check; `None` ⇒ PLUS is simply not advertised and plain SCRAM still runs
    /// over the encrypted channel (STL-297).
    pub(crate) endpoint_cbind: Option<Vec<u8>>,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TlsAcceptor holds key material; print the policy only.
        f.debug_struct("ServerTls")
            .field("mode", &self.mode)
            .field("channel_binding", &self.endpoint_cbind.is_some())
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
        // The `tls-server-end-point` binding for the leaf is fixed for this
        // server (the one configured certificate is presented on every
        // handshake), so compute it once here rather than per connection.
        let endpoint_cbind = certs
            .first()
            .and_then(|leaf| endpoint_channel_binding(leaf.as_ref()));

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

        let config = Arc::new(config);
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::clone(&config)),
            config,
            mode,
            endpoint_cbind,
        })
    }

    /// The rustls acceptor this context drives, for callers that run the TLS
    /// handshake on their own accept loop rather than through pg-wire's
    /// `SSLRequest` negotiation. The admin / control-plane transports reuse the
    /// pg-wire certificate material this way ([STL-311]) — gRPC and the HTTP/JSON
    /// gateway speak TLS from the first byte, so they take the acceptor directly.
    /// Cheap to clone (an `Arc` bump).
    ///
    /// [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        self.acceptor.clone()
    }

    /// An acceptor over this same certificate material but advertising the ALPN
    /// `protocols` (e.g. `[b"h2"]`). The admin gRPC listener needs HTTP/2
    /// negotiated by ALPN ([STL-311]); pg-wire and the HTTP/1.1 ops listener take
    /// the plain [`acceptor`](Self::acceptor) (no ALPN), so the protocol list
    /// lives with the caller that needs it rather than baked into every context.
    ///
    /// [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
    #[must_use]
    pub fn acceptor_with_alpn(&self, protocols: &[&[u8]]) -> TlsAcceptor {
        let mut config = (*self.config).clone();
        config.alpn_protocols = protocols.iter().map(|p| p.to_vec()).collect();
        TlsAcceptor::from(Arc::new(config))
    }
}

/// The RFC 5929 `tls-server-end-point` channel-binding data for an end-entity
/// certificate: the DER-encoded certificate hashed with the digest of its
/// signature algorithm.
///
/// RFC 5929 §4.1 derives the hash from the certificate's `signatureAlgorithm`
/// (using SHA-256 where the signature itself uses MD5 or SHA-1). This
/// implementation binds **only** the SHA-256 case directly: a leaf signed
/// RSA-SHA-256 or ECDSA-SHA-256 — every modern certificate, including the
/// `rcgen` certs we mint for the self-signed fallback and the tests. Every other
/// signature hash returns `None`: the stronger SHA-384/512 (a filed follow-up,
/// STL-330) and the legacy MD5/SHA-1 (deprecated, not worth a binding path).
/// `None` means PLUS is *not advertised* (plain SCRAM still runs over TLS),
/// which is the safe degrade — better than advertising PLUS and computing a hash
/// the client would compute differently, which would fail the `c=` check.
fn endpoint_channel_binding(cert_der: &[u8]) -> Option<Vec<u8>> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    let sig_alg = &cert.signature_algorithm.algorithm;
    (*sig_alg == OID_PKCS1_SHA256WITHRSA || *sig_alg == OID_SIG_ECDSA_WITH_SHA256)
        .then(|| sha256(cert_der).as_bytes().to_vec())
}

/// A hot-swappable holder for the active [`ServerTls`]. The accept loop reads the
/// current acceptor out of this cell once per connection (see the listener loop in
/// `lib.rs`); a [`TlsReloader::reload`] swaps the inner `Arc` atomically. Reads are
/// uncontended in the steady state and never held across `await`, so the `RwLock`
/// is effectively a cheap read-mostly pointer.
pub(crate) type SharedServerTls = Arc<RwLock<Arc<ServerTls>>>;

/// Wrap an already-loaded [`ServerTls`] in a [`SharedServerTls`] cell — the
/// static (non-reloadable) path behind [`Server::with_tls`](crate::Server::with_tls).
pub(crate) fn shared_tls(tls: ServerTls) -> SharedServerTls {
    Arc::new(RwLock::new(Arc::new(tls)))
}

/// Owns the live TLS material and the policy to re-read it from disk, so a running
/// server can pick up a rotated certificate/key **without a restart** ([STL-293]).
///
/// STL-251 loaded the `[tls]` cert/key once into a fixed acceptor; operators with
/// short-lived certificates (cert-manager / Let's Encrypt) had to restart to
/// rotate. A reloader instead holds a shared, hot-swappable acceptor cell that the
/// accept loop reads per connection, plus the [`TlsSettings`] paths needed to
/// rebuild the acceptor on demand. The trigger (a SIGHUP handler in `stele-server`)
/// lives with the daemon; this type is the runtime-agnostic mechanism it drives,
/// and the unit + wire tests exercise it directly.
///
/// Cloning is cheap — every clone shares the same cell, so the copy the listener
/// holds and the copy the signal handler calls [`reload`](Self::reload) on stay in
/// lock-step.
///
/// # Failure posture
/// A failed [`reload`](Self::reload) — a torn write, a non-PEM file, or a
/// cert/key mismatch — leaves the previously loaded acceptor in place and returns
/// the error. A bad rotation never takes the listener down.
///
/// [STL-293]: https://allegromusic.atlassian.net/browse/STL-293
#[derive(Clone)]
pub struct TlsReloader {
    active: SharedServerTls,
    settings: Arc<TlsSettings>,
}

impl TlsReloader {
    /// Load the initial certificate material and build the reloader. The boot-time
    /// error posture matches [`ServerTls::load`]: a bad pair *at startup* still
    /// fails fast (only *subsequent* reloads keep the old material on failure).
    ///
    /// # Errors
    /// Returns a [`TlsError`] when the initial load fails — see [`ServerTls::load`].
    pub fn load(settings: TlsSettings) -> Result<Self, TlsError> {
        let initial = ServerTls::load(&settings)?;
        Ok(Self {
            active: shared_tls(initial),
            settings: Arc::new(settings),
        })
    }

    /// The cell the [`Server`](crate::Server) reads the live acceptor out of.
    pub(crate) fn cell(&self) -> SharedServerTls {
        Arc::clone(&self.active)
    }

    /// The TLS acceptor currently installed — the one a [`reload`](Self::reload)
    /// most recently swapped in. Callers that run their own accept loop (the
    /// admin / control-plane gRPC and ops HTTP transports, [STL-311]) read this
    /// per connection, so a SIGHUP-driven rotation reaches them exactly as it
    /// reaches the pg-wire listener — new connections present the rotated cert,
    /// in-flight ones keep theirs. Cheap to clone (an `Arc` bump).
    ///
    /// [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        self.active
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .acceptor
            .clone()
    }

    /// The live acceptor, tagged with the ALPN `protocols` — the hot-reloadable
    /// counterpart of [`ServerTls::acceptor_with_alpn`], so the admin gRPC
    /// listener picks up a rotated certificate while still negotiating HTTP/2
    /// ([STL-311]).
    ///
    /// [STL-311]: https://allegromusic.atlassian.net/browse/STL-311
    #[must_use]
    pub fn acceptor_with_alpn(&self, protocols: &[&[u8]]) -> TlsAcceptor {
        self.active
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .acceptor_with_alpn(protocols)
    }

    /// The acceptor currently installed — a snapshot, as the accept loop sees it.
    /// Test-only: the accept loop reads the cell directly, and the wire tests
    /// observe the live cert from the client side of a real handshake.
    #[cfg(test)]
    pub(crate) fn current(&self) -> Arc<ServerTls> {
        Arc::clone(&self.active.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Re-read the cert/key (and any mTLS client CA) from the configured paths and
    /// swap the active acceptor atomically. New connections handshake with the new
    /// certificate; connections already established keep the acceptor they used.
    ///
    /// On failure the active acceptor is **left untouched** — the listener keeps
    /// serving the previously loaded material — and the error is both logged and
    /// returned so the caller can surface it.
    ///
    /// # Errors
    /// Returns a [`TlsError`] when the rotated material is unreadable / not PEM or
    /// rustls rejects the pair. The previously loaded acceptor stays live.
    pub fn reload(&self) -> Result<(), TlsError> {
        match ServerTls::load(&self.settings) {
            Ok(reloaded) => {
                *self.active.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(reloaded);
                tracing::info!(
                    cert = %self.settings.cert.display(),
                    "TLS certificate reloaded; new connections present the rotated cert"
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    cert = %self.settings.cert.display(),
                    "TLS reload failed; keeping the certificate already in use"
                );
                Err(error)
            }
        }
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
            // rcgen mints an ECDSA-P256/SHA-256 leaf, so channel binding is
            // available — PLUS is advertised even on the self-signed fallback.
            assert!(
                tls.endpoint_cbind.is_some(),
                "an ECDSA-SHA-256 leaf yields tls-server-end-point binding"
            );
        }
    }

    #[test]
    fn channel_binding_is_the_sha256_of_an_ecdsa_sha256_cert() {
        // `tls-server-end-point` for a SHA-256-signed certificate is the SHA-256
        // of the certificate DER (RFC 5929 §4.1). A client computes the identical
        // value from the same bytes it received in the handshake, which is what
        // makes the `c=` check bind the SASL proof to this TLS channel.
        let der = leaf_der(Some("svc"), &["svc.local"]);
        let cbind = endpoint_channel_binding(&der).expect("SHA-256 binding");
        assert_eq!(cbind, sha256(&der).as_bytes().to_vec());
        assert_eq!(cbind.len(), 32, "SHA-256 is 32 bytes");
    }

    #[test]
    fn channel_binding_is_none_for_non_certificate_bytes() {
        // A parse failure must degrade to "no binding" (PLUS unadvertised), never
        // a panic — the certificate is operator-supplied.
        assert!(endpoint_channel_binding(b"this is not a certificate").is_none());
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

    /// A self-signed server (cert, key) PEM pair for `localhost` — enough material
    /// for [`ServerTls::load`] (which builds the acceptor, it does not verify a
    /// chain). The end-to-end handshake across a rotation lives in tests/tls_wire.rs.
    fn self_signed_server_pem() -> (String, String) {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("params");
        let cert = params.self_signed(&key).expect("self-sign");
        (cert.pem(), key.serialize_pem())
    }

    #[test]
    fn reloader_swaps_on_success_and_keeps_the_old_acceptor_on_failure() {
        // STL-293: a good rotation installs a new acceptor; a broken one returns an
        // error and leaves the live acceptor untouched (the listener never drops).
        let dir = std::env::temp_dir().join(format!("stele-tls-reload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");

        let (cert_a, key_a) = self_signed_server_pem();
        std::fs::write(&cert_path, &cert_a).expect("write cert");
        std::fs::write(&key_path, &key_a).expect("write key");

        let reloader = TlsReloader::load(TlsSettings {
            cert: cert_path.clone(),
            key: key_path.clone(),
            client_ca: None,
            mode: TlsMode::Required,
        })
        .expect("initial load");
        let first = reloader.current();

        // A good rotation swaps the active acceptor for a freshly built one.
        let (cert_b, key_b) = self_signed_server_pem();
        std::fs::write(&cert_path, &cert_b).expect("rotate cert");
        std::fs::write(&key_path, &key_b).expect("rotate key");
        reloader.reload().expect("good reload");
        let second = reloader.current();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "a successful reload installs a new acceptor"
        );

        // A broken rotation (non-PEM cert, e.g. a torn write) fails and keeps the
        // acceptor that was already serving.
        std::fs::write(&cert_path, b"not a certificate").expect("corrupt cert");
        reloader
            .reload()
            .expect_err("a broken pair must not swap in");
        let third = reloader.current();
        assert!(
            Arc::ptr_eq(&second, &third),
            "a failed reload keeps the previously loaded acceptor"
        );
    }
}
