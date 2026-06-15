//! The **HTTP/JSON gateway** for the admin API ([STL-254], [ADR-0016]).
//!
//! The curl/script/desktop-app face of the same [`AdminService`] core the gRPC
//! transport serves, mounted under `/v1alpha1/…` on the **shared ops listener**
//! ([STL-253] — the metrics/probe port) rather than a port of its own. Requests
//! authenticate with the same bearer token (`Authorization: Bearer …`).
//!
//! Routing is synchronous and side-effect-light — the engine call runs inline in
//! the per-connection task, exactly as the pg-wire `BACKUP` path runs inline in
//! its statement. The ops listener owns the socket I/O; this module owns the
//! routing, auth, and JSON, exposed to the listener through [`AdminHttpResponder`].
//!
//! ## Routes (all under `/v1alpha1`)
//!
//! | method | path | body | reply |
//! |---|---|---|---|
//! | GET/POST | `/health` | — | `{"status":"SERVING"}` |
//! | GET/POST | `/status` | — | engine status |
//! | POST | `/backup` | `{"path":"…"}` | `{"manifest":{…}}` |
//! | POST | `/restore-plan` | `{"path":"…"}` | `{"valid":…,"error":…,"manifest":…}` |
//! | POST | `/segments` | `{"table":"…"}` | tabular |
//! | POST | `/versions` | `{"table":"…","key":"…"?}` | tabular |
//! | POST | `/audit-chain` | `{"table":"…","key":"…"?}` | tabular |
//!
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use serde::Deserialize;
use serde_json::{Value, json};

use stele_common::time::Clock;
use stele_storage::backend::Disk;

use super::{
    AdminAuth, AdminError, AdminService, ManifestSummary, RestorePlan, StatusReport, TableData,
};

/// JSON content type for every admin reply.
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";

/// The largest JSON request body the gateway accepts (admin bodies are tiny —
/// a path or a table/key pair).
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// A parsed admin HTTP request, handed in by the ops listener.
pub struct HttpRequest {
    /// The HTTP method (`GET`, `POST`, …).
    pub method: String,
    /// The request path (query string already stripped).
    pub path: String,
    /// The raw `Authorization` header value, if present.
    pub authorization: Option<String>,
    /// The request body bytes (empty for a body-less request).
    pub body: Vec<u8>,
}

/// A fully-formed admin HTTP response for the ops listener to write.
pub struct HttpResponse {
    /// The status line tail, e.g. `200 OK`.
    pub status: &'static str,
    /// The `Content-Type`.
    pub content_type: &'static str,
    /// The `Allow` header value for a `405` (empty otherwise) — admin endpoints
    /// vary (some accept `GET, POST`, the action endpoints only `POST`).
    pub allow: &'static str,
    /// The body.
    pub body: String,
}

/// The seam the ops listener routes `/v1alpha1/…` through, kept object-safe (a
/// plain synchronous call) so the listener need not be generic over the engine.
pub trait AdminHttpResponder: Send + Sync + 'static {
    /// Route, authenticate, and answer one admin request.
    fn respond(&self, request: &HttpRequest) -> HttpResponse;
}

/// The HTTP-facing wrapper: the core plus the bearer-token authenticator.
pub struct AdminHttp<C: Clock + Clone, D: Disk + Clone> {
    core: AdminService<C, D>,
    auth: std::sync::Arc<AdminAuth>,
}

impl<C: Clock + Clone, D: Disk + Clone> AdminHttp<C, D> {
    /// Wrap the shared core + authenticator.
    #[must_use]
    pub const fn new(core: AdminService<C, D>, auth: std::sync::Arc<AdminAuth>) -> Self {
        Self { core, auth }
    }
}

impl<C, D> AdminHttpResponder for AdminHttp<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    fn respond(&self, request: &HttpRequest) -> HttpResponse {
        // Authentication first — an unauthenticated caller learns nothing about
        // routing (a known path and an unknown one both answer 401).
        let token = request
            .authorization
            .as_deref()
            .and_then(super::bearer_token);
        if !self.auth.authorize(token) {
            return json(
                "401 Unauthorized",
                &json!({ "error": "admin API requires a valid bearer token" }),
            );
        }

        match request.path.as_str() {
            "/v1alpha1/health" => {
                guard_read(request, || json("200 OK", &json!({ "status": "SERVING" })))
            }
            "/v1alpha1/status" => guard_read(request, || {
                json("200 OK", &status_json(&self.core.status()))
            }),
            "/v1alpha1/backup" => self.guard_post(request, |core, body| {
                let PathBody { path } = parse_body(body)?;
                Ok(json!({ "manifest": manifest_json(&core.backup(&path)?) }))
            }),
            "/v1alpha1/restore-plan" => self.guard_post(request, |core, body| {
                let PathBody { path } = parse_body(body)?;
                Ok(restore_plan_json(&core.restore_plan(&path)?))
            }),
            "/v1alpha1/segments" => self.guard_post(request, |core, body| {
                let TableBody { table, .. } = parse_body(body)?;
                Ok(table_json(&core.segments(&table)?))
            }),
            "/v1alpha1/versions" => self.guard_post(request, |core, body| {
                let TableBody { table, key } = parse_body(body)?;
                Ok(table_json(&core.versions(&table, key.as_deref())?))
            }),
            "/v1alpha1/audit-chain" => self.guard_post(request, |core, body| {
                let TableBody { table, key } = parse_body(body)?;
                Ok(table_json(&core.audit_chain(&table, key.as_deref())?))
            }),
            _ => json(
                "404 Not Found",
                &json!({ "error": "no such admin endpoint" }),
            ),
        }
    }
}

impl<C, D> AdminHttp<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    /// Require `POST`, enforce the body cap, run `handler`, and JSON-encode its
    /// result (or its [`AdminError`]).
    fn guard_post(
        &self,
        request: &HttpRequest,
        handler: impl FnOnce(&AdminService<C, D>, &[u8]) -> Result<Value, AdminError>,
    ) -> HttpResponse {
        if request.method != "POST" {
            return method_not_allowed("POST");
        }
        if request.body.len() > MAX_BODY_BYTES {
            return json(
                "413 Payload Too Large",
                &json!({ "error": "request body exceeds the admin limit" }),
            );
        }
        match handler(&self.core, &request.body) {
            Ok(value) => json("200 OK", &value),
            Err(e) => error_response(&e),
        }
    }
}

/// Require a read method (`GET` or `POST`) for a no-argument endpoint.
fn guard_read(request: &HttpRequest, handler: impl FnOnce() -> HttpResponse) -> HttpResponse {
    if request.method == "GET" || request.method == "POST" {
        handler()
    } else {
        method_not_allowed("GET, POST")
    }
}

/// `{"path":"…"}` — backup / restore-plan request body.
#[derive(Deserialize)]
struct PathBody {
    path: String,
}

/// `{"table":"…","key":"…"?}` — introspection request body.
#[derive(Deserialize)]
struct TableBody {
    table: String,
    #[serde(default)]
    key: Option<String>,
}

/// Parse a JSON request body, turning a parse failure into an
/// [`AdminError::InvalidArgument`].
fn parse_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, AdminError> {
    serde_json::from_slice(body)
        .map_err(|e| AdminError::InvalidArgument(format!("invalid JSON body: {e}")))
}

fn status_json(s: &StatusReport) -> Value {
    json!({
        "ready": s.ready,
        "wal_poisoned": s.wal_poisoned,
        "server_version": s.server_version,
        "table_count": s.table_count,
        "user_count": s.user_count,
        "tables": s.tables.iter().map(|t| json!({
            "name": t.name,
            "column_count": t.column_count,
            "segment_count": t.segment_count,
        })).collect::<Vec<_>>(),
    })
}

fn manifest_json(m: &ManifestSummary) -> Value {
    json!({
        "manifest_version": m.manifest_version,
        "stele_version": m.stele_version,
        "fence_micros": m.fence_micros,
        "commit_head": m.commit_head,
        "file_count": m.file_count,
        "total_bytes": m.total_bytes,
    })
}

fn restore_plan_json(p: &RestorePlan) -> Value {
    json!({
        "valid": p.valid,
        "error": p.error,
        "manifest": p.manifest.as_ref().map(manifest_json),
    })
}

fn table_json(d: &TableData) -> Value {
    json!({
        "columns": d.columns.iter().map(|(name, ty)| json!({ "name": name, "type": ty })).collect::<Vec<_>>(),
        // A cell is its rendered text, or JSON null for SQL NULL.
        "rows": d.rows.iter().map(|row| row.iter().map(|c| json!(c)).collect::<Vec<_>>()).collect::<Vec<_>>(),
    })
}

/// Map an [`AdminError`] to its HTTP response.
fn error_response(err: &AdminError) -> HttpResponse {
    let status = match err {
        AdminError::NotFound(_) => "404 Not Found",
        AdminError::InvalidArgument(_) => "400 Bad Request",
        AdminError::Internal(_) => "500 Internal Server Error",
    };
    json(status, &json!({ "error": err.to_string() }))
}

/// A `405` whose `Allow` (the methods this endpoint accepts) the ops writer
/// surfaces.
fn method_not_allowed(allow: &'static str) -> HttpResponse {
    HttpResponse {
        allow,
        ..json(
            "405 Method Not Allowed",
            &json!({ "error": "method not allowed for this admin endpoint" }),
        )
    }
}

/// Build a JSON [`HttpResponse`] with `status` and a serialized `value`.
fn json(status: &'static str, value: &Value) -> HttpResponse {
    HttpResponse {
        status,
        content_type: JSON_CONTENT_TYPE,
        allow: "",
        body: format!("{value}\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_json_renders_cells_and_nulls() {
        let data = TableData {
            columns: vec![("id".to_owned(), "int8".to_owned())],
            rows: vec![vec![Some("1".to_owned())], vec![None]],
        };
        let v = table_json(&data);
        assert_eq!(v["columns"][0]["name"], "id");
        assert_eq!(v["columns"][0]["type"], "int8");
        assert_eq!(v["rows"][0][0], "1");
        assert!(v["rows"][1][0].is_null(), "SQL NULL renders as JSON null");
    }

    #[test]
    fn restore_plan_json_carries_verdict() {
        let plan = RestorePlan {
            valid: false,
            error: Some("backup directory \"/x\" does not exist".to_owned()),
            manifest: None,
        };
        let v = restore_plan_json(&plan);
        assert_eq!(v["valid"], false);
        assert!(v["error"].as_str().unwrap().contains("does not exist"));
        assert!(v["manifest"].is_null());
    }
}
