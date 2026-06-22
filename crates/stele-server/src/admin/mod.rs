//! The **admin / control-plane API** ([STL-254], [ADR-0016]).
//!
//! A dedicated ops surface for things that are not SQL — health, status,
//! backup/restore validation, and segment / version / commit-chain introspection
//! — exposed two ways from one transport-agnostic core ([`AdminService`]):
//!
//! * **gRPC** ([`grpc`]) on its own listener (typed, for programmatic clients and
//!   the operator), and
//! * an **HTTP/JSON gateway** ([`http`]) sharing the ops listener with the metrics
//!   endpoint ([STL-253]) under `/v1alpha1/…` (for curl, scripts, the desktop app).
//!
//! Both transports authenticate the same way — a static bearer token
//! ([`AdminAuth`]) — and call the same core, so the contract is the one
//! `v1alpha1` `.proto` ([`proto`], the repo-root `proto/` tree). SQL stays on
//! pg-wire (existing Postgres drivers); this surface never touches it.
//!
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

/// Generated gRPC service + message types for the `v1alpha1` admin API.
///
/// Produced by `build.rs` from `proto/stele/admin/v1alpha1/admin.proto`. The
/// generated code is exempt from the workspace's strict lints.
pub mod proto {
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        unsafe_code,
        missing_docs,
        unreachable_pub
    )]
    include!(concat!(env!("OUT_DIR"), "/stele.admin.v1alpha1.rs"));
}

pub mod grpc;
pub mod http;

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use stele_common::time::Clock;
use stele_engine::{SelectResult, SessionEngine, backup as engine_backup};
use stele_pgwire::TlsReloader;
use stele_storage::backend::{Disk, LocalDisk};

/// The `stele-server` crate version this API reports as the server version.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Static bearer-token authentication for the admin surface.
///
/// The ADR-0016 "admin-API tokens" (feature-plan B.8): one or more opaque secrets
/// from `[admin] tokens` in `stele.toml`. **Secure default** — with no tokens
/// configured the surface authorizes nothing, so every call is rejected. Tokens
/// are never logged.
#[derive(Clone, Default)]
pub struct AdminAuth {
    tokens: Vec<String>,
}

impl AdminAuth {
    /// Build an authenticator over the configured bearer tokens.
    #[must_use]
    pub const fn new(tokens: Vec<String>) -> Self {
        Self { tokens }
    }

    /// Whether any token is configured. When false the admin API rejects every
    /// request (the gRPC listener is not even bound — [`run`](crate::run)).
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        !self.tokens.is_empty()
    }

    /// Whether `presented` (a bearer-token value, or `None` when the request
    /// carried no credential) matches a configured token.
    ///
    /// Comparison is constant-time per candidate and never short-circuits across
    /// the configured set, so a valid token's position is not timing-observable.
    /// With no tokens configured this is always `false` (secure default).
    #[must_use]
    pub fn authorize(&self, presented: Option<&str>) -> bool {
        let Some(presented) = presented else {
            return false;
        };
        let presented = presented.as_bytes();
        let mut matched = false;
        for token in &self.tokens {
            matched |= ct_eq(token.as_bytes(), presented);
        }
        matched
    }
}

/// Extract the token from an `Authorization` value, accepting the `Bearer`
/// auth-scheme case-insensitively (RFC 9110 §11.1) with the conventional single
/// space. `None` if the header is not a bearer credential.
pub(crate) fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("Bearer")
        .then(|| token.trim())
        .filter(|t| !t.is_empty())
}

/// Constant-time byte-slice equality. Length inequality short-circuits (a token's
/// length is not a meaningful secret); equal-length contents are compared without
/// data-dependent branching.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A failure serving an admin request, mapped onto each transport's error model
/// (HTTP status / gRPC [`Status`](tonic::Status)).
#[derive(Debug)]
pub enum AdminError {
    /// The named entity (e.g. a table) does not exist → HTTP 404 / `NOT_FOUND`.
    NotFound(String),
    /// The request was malformed (e.g. an unparsable key literal, a bad path) →
    /// HTTP 400 / `INVALID_ARGUMENT`.
    InvalidArgument(String),
    /// The server is not in a state to serve this request (e.g. a TLS reload was
    /// asked of a server with no reloadable `[tls]` material) → HTTP 409 /
    /// `FAILED_PRECONDITION`. The request is well-formed; the server's
    /// configuration simply cannot satisfy it.
    FailedPrecondition(String),
    /// The engine or storage failed → HTTP 500 / `INTERNAL`.
    Internal(String),
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(m)
            | Self::InvalidArgument(m)
            | Self::FailedPrecondition(m)
            | Self::Internal(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for AdminError {}

/// Map an [`EngineError`](stele_engine::EngineError) from an introspection /
/// backup call onto an [`AdminError`].
fn map_engine_error(err: stele_engine::EngineError) -> AdminError {
    use stele_engine::EngineError;
    match err {
        EngineError::UnknownTable(t) => AdminError::NotFound(format!("unknown table {t:?}")),
        EngineError::IntrospectionKey(reason) => AdminError::InvalidArgument(reason),
        // A backup into a non-empty directory is the caller's mistake, not a
        // server fault.
        EngineError::Backup(engine_backup::BackupError::TargetNotEmpty) => {
            AdminError::InvalidArgument(
                "backup target directory is not empty: choose a fresh path".to_owned(),
            )
        }
        other => AdminError::Internal(other.to_string()),
    }
}

/// A backup manifest's summary — the transport-agnostic shape of
/// [`BackupManifest`](engine_backup::BackupManifest).
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl From<&engine_backup::BackupManifest> for ManifestSummary {
    fn from(m: &engine_backup::BackupManifest) -> Self {
        Self {
            manifest_version: m.manifest_version,
            stele_version: m.stele_version.clone(),
            fence_micros: m.fence_micros,
            commit_head: m.commit_head.to_hex(),
            file_count: m.files.len() as u64,
            total_bytes: m.files.iter().map(|f| f.len).sum(),
        }
    }
}

/// Engine state ([`AdminService::status`]).
#[derive(Debug, Clone)]
pub struct StatusReport {
    /// Recovery complete and no WAL poisoned.
    pub ready: bool,
    /// A failed fsync has poisoned a table's WAL ([STL-217]).
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    pub wal_poisoned: bool,
    /// The server version serving this API.
    pub server_version: String,
    /// Number of live tables.
    pub table_count: u64,
    /// Number of users in the catalog user store.
    pub user_count: u64,
    /// Per-table summaries.
    pub tables: Vec<TableStatus>,
}

/// One live table's summary within a [`StatusReport`].
#[derive(Debug, Clone)]
pub struct TableStatus {
    /// The table name.
    pub name: String,
    /// Column count.
    pub column_count: u64,
    /// Resident sealed segments + the hot delta tier.
    pub segment_count: u64,
}

/// The verdict of validating a backup ([`AdminService::restore_plan`]).
#[derive(Debug, Clone)]
pub struct RestorePlan {
    /// The manifest decoded and every file matched its recorded checksum.
    pub valid: bool,
    /// Why the backup did not validate (when `valid` is false).
    pub error: Option<String>,
    /// The manifest summary, when it decoded.
    pub manifest: Option<ManifestSummary>,
}

/// A tabular introspection reply — the transport-agnostic shape of a
/// [`SelectResult`], with every cell rendered to text by its column type.
#[derive(Debug, Clone)]
pub struct TableData {
    /// `(name, type-name)` per column, in output order.
    pub columns: Vec<(String, String)>,
    /// One row of optional rendered cells (`None` = SQL NULL), aligned to
    /// [`columns`](Self::columns).
    pub rows: Vec<Vec<Option<String>>>,
}

/// The transport-agnostic admin core.
///
/// Every method operates on the one shared [`SessionEngine`], behind the same
/// mutex the pg-wire front end uses. Both the gRPC service ([`grpc`]) and the
/// HTTP/JSON gateway ([`http`]) call into it, so the two surfaces behave
/// identically. Cloning is cheap (an `Arc` bump) — each transport holds its own
/// handle.
pub struct AdminService<C: Clock + Clone, D: Disk + Clone> {
    engine: Arc<Mutex<SessionEngine<C, D>>>,
}

impl<C: Clock + Clone, D: Disk + Clone> Clone for AdminService<C, D> {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
        }
    }
}

impl<C, D> AdminService<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    /// Wrap the shared engine handle.
    #[must_use]
    pub const fn new(engine: Arc<Mutex<SessionEngine<C, D>>>) -> Self {
        Self { engine }
    }

    /// Lock the engine, recovering a poisoned mutex (the same posture as the rest
    /// of the server: a panicked statement must not wedge the daemon).
    fn lock(&self) -> MutexGuard<'_, SessionEngine<C, D>> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Liveness: the process can answer. Always serving — engine state is
    /// [`status`](Self::status)'s job, not health's.
    #[must_use]
    pub const fn health(&self) -> bool {
        true
    }

    /// Engine state: readiness, WAL-poison, version, and table/user/segment counts.
    #[must_use]
    pub fn status(&self) -> StatusReport {
        let engine = self.lock();
        let descriptions = engine.describe_live_tables();
        let tables = descriptions
            .iter()
            .map(|t| TableStatus {
                name: t.name.clone(),
                column_count: t.columns.len() as u64,
                // Best-effort: a table that fails to enumerate its segments still
                // appears, with a 0 count, rather than failing the whole report.
                segment_count: engine
                    .segment_metadata(&t.name)
                    .map_or(0, |r| r.rows.len() as u64),
            })
            .collect();
        let poisoned = engine.is_poisoned();
        StatusReport {
            ready: !poisoned,
            wal_poisoned: poisoned,
            server_version: SERVER_VERSION.to_owned(),
            table_count: descriptions.len() as u64,
            user_count: engine.user_count() as u64,
            tables,
        }
    }

    /// Trigger a consistent online full backup into `path` ([STL-249]); return its
    /// manifest summary. `path` is created if absent and must be empty.
    ///
    /// # Errors
    ///
    /// [`AdminError::InvalidArgument`] if the target is non-empty;
    /// [`AdminError::Internal`] on an I/O failure opening or writing it.
    ///
    /// [STL-249]: https://allegromusic.atlassian.net/browse/STL-249
    pub fn backup(&self, path: &str) -> Result<ManifestSummary, AdminError> {
        // The backup target is always a local filesystem directory (object-store
        // targets are v0.4), regardless of the engine's own backend — mirroring
        // the wire `BACKUP TO` path ([STL-249]).
        let target = LocalDisk::open(path)
            .map_err(|e| AdminError::Internal(format!("opening backup target {path:?}: {e}")))?;
        let manifest = self.lock().backup(&target).map_err(map_engine_error)?;
        Ok(ManifestSummary::from(&manifest))
    }

    /// Validate a backup directory without applying it ([`engine_backup::inspect_backup`]).
    /// A missing directory or a failed check is a *valid response* with
    /// `valid = false`, not an error.
    ///
    /// # Errors
    ///
    /// [`AdminError::Internal`] only if the directory exists but cannot be opened.
    pub fn restore_plan(&self, path: &str) -> Result<RestorePlan, AdminError> {
        // Validation must not have side effects: `LocalDisk::open` would *create*
        // the directory, so a non-existent path is reported invalid up front
        // rather than silently materialized.
        if !std::path::Path::new(path).is_dir() {
            return Ok(RestorePlan {
                valid: false,
                error: Some(format!("backup directory {path:?} does not exist")),
                manifest: None,
            });
        }
        let src = LocalDisk::open(path)
            .map_err(|e| AdminError::Internal(format!("opening backup {path:?}: {e}")))?;
        // `inspect_backup` returns the decoded manifest whenever it was readable —
        // even on a per-file checksum failure — so the verdict carries what the
        // backup claims to be alongside any failure.
        let inspection = engine_backup::inspect_backup(&src);
        let manifest = inspection.manifest.as_ref().map(ManifestSummary::from);
        Ok(match inspection.result {
            Ok(()) => RestorePlan {
                valid: true,
                error: None,
                manifest,
            },
            Err(e) => RestorePlan {
                valid: false,
                error: Some(e.to_string()),
                manifest,
            },
        })
    }

    /// Per-table columnar segment + zone-map metadata ([STL-301]).
    ///
    /// # Errors
    ///
    /// [`AdminError::NotFound`] for an unknown table; [`AdminError::Internal`] on a
    /// storage error.
    ///
    /// [STL-301]: https://allegromusic.atlassian.net/browse/STL-301
    pub fn segments(&self, table: &str) -> Result<TableData, AdminError> {
        let result = self
            .lock()
            .segment_metadata(table)
            .map_err(map_engine_error)?;
        render_table(&result)
    }

    /// Per-key (or whole-table, when `key` is `None`) version history ([STL-199]).
    /// `key` is a SQL literal folded to the key column's type.
    ///
    /// # Errors
    ///
    /// [`AdminError::NotFound`] for an unknown table; [`AdminError::InvalidArgument`]
    /// if `key` is not a literal of the key type; [`AdminError::Internal`] on a
    /// storage error.
    ///
    /// [STL-199]: https://allegromusic.atlassian.net/browse/STL-199
    pub fn versions(&self, table: &str, key: Option<&str>) -> Result<TableData, AdminError> {
        let key_expr = key.map(parse_key_literal).transpose()?;
        let result = self
            .lock()
            .version_history(table, key_expr.as_ref())
            .map_err(map_engine_error)?;
        render_table(&result)
    }

    /// Per-version commit hash-chain links + an intact/broken verdict ([STL-302]).
    /// `key` is a SQL literal folded to the key column's type.
    ///
    /// # Errors
    ///
    /// As [`versions`](Self::versions), plus [`AdminError::Internal`] if the commit
    /// log cannot be read.
    ///
    /// [STL-302]: https://allegromusic.atlassian.net/browse/STL-302
    pub fn audit_chain(&self, table: &str, key: Option<&str>) -> Result<TableData, AdminError> {
        let key_expr = key.map(parse_key_literal).transpose()?;
        let result = self
            .lock()
            .audit_chain(table, key_expr.as_ref())
            .map_err(map_engine_error)?;
        render_table(&result)
    }
}

/// Reload the server's TLS certificate material on demand — the cross-platform,
/// token-authenticated counterpart to the Unix-only SIGHUP trigger ([STL-293]),
/// driven by the admin `ReloadTls` RPC / `POST /v1alpha1/reload-tls` ([STL-326]).
/// Both transports call this one core so they behave identically.
///
/// `reloader` is `Some` exactly when the server booted with operator `[tls]`
/// material behind a hot-reloadable acceptor; the ephemeral self-signed fallback
/// and the plaintext / loopback postures have nothing to reload.
///
/// On success returns the reloaded certificate path, so the operator can confirm
/// which material the rotation picked up. The failure posture is
/// [`TlsReloader::reload`]'s and is unchanged from SIGHUP: a bad pair (torn write,
/// non-PEM, cert/key mismatch) leaves the live certificate in place and the error
/// is logged and returned (here [`AdminError::Internal`]). A server with no
/// reloadable material answers [`AdminError::FailedPrecondition`] rather than
/// pretending to rotate.
///
/// [STL-293]: https://allegromusic.atlassian.net/browse/STL-293
/// [STL-326]: https://allegromusic.atlassian.net/browse/STL-326
pub(crate) fn reload_tls(reloader: Option<&TlsReloader>) -> Result<String, AdminError> {
    let Some(reloader) = reloader else {
        return Err(AdminError::FailedPrecondition(
            "TLS hot-reload is unavailable: the server has no reloadable [tls] \
             certificate configured (it is running plaintext, on loopback, or with \
             the ephemeral self-signed fallback)"
                .to_owned(),
        ));
    };
    reloader
        .reload()
        .map(|()| reloader.cert_path().display().to_string())
        .map_err(|e| AdminError::Internal(format!("TLS reload failed: {e}")))
}

/// Render a [`SelectResult`] into the transport-agnostic [`TableData`], decoding
/// each cell to text by its column type exactly as the SQL wire does
/// ([`stele_pgwire::render_cell`]).
fn render_table(result: &SelectResult) -> Result<TableData, AdminError> {
    let columns = result
        .columns
        .iter()
        .map(|(name, ty)| (name.clone(), ty.to_string()))
        .collect();
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let mut cells = Vec::with_capacity(row.len());
        for (cell, (_, ty)) in row.iter().zip(&result.columns) {
            let rendered = stele_pgwire::render_cell(*ty, cell.as_deref())
                .map_err(|e| AdminError::Internal(format!("rendering introspection cell: {e}")))?;
            cells.push(rendered);
        }
        rows.push(cells);
    }
    Ok(TableData { columns, rows })
}

/// Parse an introspection key, supplied as a SQL literal (`42`, `'alice'`,
/// `DATE '2020-01-01'`), into the AST literal the engine folds to the key column's
/// type — the same folding the SQL `\history` path uses, so the business key
/// matches byte-for-byte.
///
/// Strictly a single literal expression: anything else (a column reference, a
/// function call, multiple items) is rejected as an invalid argument. The parsed
/// expression is only ever handed to the engine's literal folder; nothing is
/// executed.
fn parse_key_literal(key: &str) -> Result<stele_sql::sqlparser::ast::Expr, AdminError> {
    use stele_sql::sqlparser::ast::{SelectItem, SetExpr, Statement as SqlStatement};

    let invalid =
        || AdminError::InvalidArgument(format!("key {key:?} must be a single SQL literal"));
    let statements = stele_sql::parse(&format!("SELECT {key}")).map_err(|e| {
        AdminError::InvalidArgument(format!("key {key:?} is not a valid literal: {e}"))
    })?;
    let [statement] = statements.as_slice() else {
        return Err(invalid());
    };
    let Some(SqlStatement::Query(query)) = statement.sql() else {
        return Err(invalid());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid());
    };
    let [SelectItem::UnnamedExpr(expr)] = select.projection.as_slice() else {
        return Err(invalid());
    };
    // A literal shape only — reject an identifier (`id`), a function call
    // (`now()`), or any computed expression at the boundary, rather than passing
    // it to the engine to reject. Mirrors the literal forms `fold` accepts.
    if !is_sql_literal(expr) {
        return Err(invalid());
    }
    Ok(expr.clone())
}

/// Whether `expr` is a SQL literal — the only shape an introspection key may be.
/// Matches the same forms `stele_sql`'s `fold` folds: a plain value, a typed
/// string (`DATE '…'`), or a signed number.
fn is_sql_literal(expr: &stele_sql::sqlparser::ast::Expr) -> bool {
    use stele_sql::sqlparser::ast::{Expr, UnaryOperator};
    match expr {
        Expr::Value(_) | Expr::TypedString(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => matches!(expr.as_ref(), Expr::Value(_)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_tokens_rejects_everything_and_reports_disabled() {
        let auth = AdminAuth::default();
        assert!(!auth.is_enabled());
        assert!(!auth.authorize(None));
        assert!(!auth.authorize(Some("anything")));
        assert!(!auth.authorize(Some("")));
    }

    #[test]
    fn authorize_matches_only_a_configured_token() {
        let auth = AdminAuth::new(vec!["s3cret".to_owned(), "other".to_owned()]);
        assert!(auth.is_enabled());
        assert!(auth.authorize(Some("s3cret")));
        assert!(auth.authorize(Some("other")));
        assert!(!auth.authorize(Some("S3CRET")), "case-sensitive");
        assert!(!auth.authorize(Some("s3cre")), "prefix is not a match");
        assert!(
            !auth.authorize(Some("s3cret ")),
            "trailing space is not a match"
        );
        assert!(!auth.authorize(None));
    }

    #[test]
    fn key_literal_accepts_int_text_and_date() {
        // Each parses to exactly one literal expression; the engine folds it to the
        // key column's type later.
        for key in ["42", "-7", "'alice'", "DATE '2020-01-01'"] {
            assert!(parse_key_literal(key).is_ok(), "{key} should parse");
        }
    }

    #[test]
    fn key_literal_rejects_non_literals_and_injection() {
        for key in ["id", "1, 2", "now()", "1; DROP TABLE t", ""] {
            assert!(
                matches!(parse_key_literal(key), Err(AdminError::InvalidArgument(_))),
                "{key:?} should be rejected"
            );
        }
    }

    #[test]
    fn reload_tls_without_a_reloader_is_a_precondition_failure() {
        // A server with no reloadable [tls] material (plaintext, loopback, or the
        // ephemeral self-signed fallback) cannot rotate — the request is well-formed
        // but the configuration cannot satisfy it. The success path needs real cert
        // files and is covered end-to-end in tests/admin_api.rs + tests/admin_tls.rs.
        let err = reload_tls(None).expect_err("no reloader must fail");
        assert!(
            matches!(err, AdminError::FailedPrecondition(_)),
            "expected FailedPrecondition, got {err:?}"
        );
        assert!(err.to_string().contains("no reloadable"), "{err}");
    }
}
