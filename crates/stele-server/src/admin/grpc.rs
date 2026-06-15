//! The **gRPC transport** for the admin API ([STL-254], [ADR-0016]).
//!
//! A thin [`tonic`] service that authenticates the bearer token, then hands every
//! call to the shared [`AdminService`] core. Engine work runs on a blocking thread
//! ([`tokio::task::spawn_blocking`]) so a slow backup never stalls the gRPC
//! reactor. The wire contract is the generated [`proto`](super::proto) — one
//! `.proto`, two transports.
//!
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use std::sync::Arc;

use stele_common::time::Clock;
use stele_storage::backend::Disk;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use super::proto::admin_service_server::{AdminService as AdminServiceRpc, AdminServiceServer};
use super::proto::{
    AuditChainRequest, BackupRequest, BackupResponse, Cell, Column, HealthRequest, HealthResponse,
    ManifestSummary as ProtoManifest, RestorePlanRequest, RestorePlanResponse, Row,
    SegmentsRequest, StatusRequest, StatusResponse, TableResponse, TableStatus as ProtoTableStatus,
    VersionsRequest, health_response::ServingStatus,
};
use super::{
    AdminAuth, AdminError, AdminService, ManifestSummary, RestorePlan, StatusReport, TableData,
};

/// The gRPC-facing wrapper: the core plus the bearer-token authenticator.
#[derive(Clone)]
pub struct GrpcAdmin<C: Clock + Clone, D: Disk + Clone> {
    core: AdminService<C, D>,
    auth: Arc<AdminAuth>,
}

impl<C, D> GrpcAdmin<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    /// Wrap the shared core + authenticator.
    #[must_use]
    pub const fn new(core: AdminService<C, D>, auth: Arc<AdminAuth>) -> Self {
        Self { core, auth }
    }

    /// Reject the request unless it carries a valid `authorization: Bearer …`
    /// metadata entry matching a configured token.
    fn authenticate<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(super::bearer_token);
        if self.auth.authorize(token) {
            Ok(())
        } else {
            Err(Status::unauthenticated(
                "admin API requires a valid bearer token",
            ))
        }
    }
}

/// Serve the gRPC admin API on an already-bound `listener`.
///
/// The caller binds the listener (so it can report the bound address), mirroring
/// the pg-wire / ops listeners ([STL-152]); the server runs until the future is
/// dropped.
///
/// # Errors
///
/// Propagates a fatal [`tonic::transport::Error`] from the server.
///
/// [STL-152]: https://allegromusic.atlassian.net/browse/STL-152
pub async fn serve<C, D>(
    listener: TcpListener,
    service: GrpcAdmin<C, D>,
) -> Result<(), tonic::transport::Error>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    tonic::transport::Server::builder()
        .add_service(AdminServiceServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
}

/// Map an [`AdminError`] onto a gRPC [`Status`].
fn to_status(err: AdminError) -> Status {
    match err {
        AdminError::NotFound(m) => Status::not_found(m),
        AdminError::InvalidArgument(m) => Status::invalid_argument(m),
        AdminError::Internal(m) => Status::internal(m),
    }
}

/// Run a blocking core call off the reactor, surfacing a panic as `INTERNAL`.
async fn blocking<C, D, T>(
    core: AdminService<C, D>,
    f: impl FnOnce(AdminService<C, D>) -> T + Send + 'static,
) -> Result<T, Status>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(core))
        .await
        .map_err(|e| Status::internal(format!("admin task failed: {e}")))
}

#[tonic::async_trait]
impl<C, D> AdminServiceRpc for GrpcAdmin<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.authenticate(&request)?;
        Ok(Response::new(HealthResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.authenticate(&request)?;
        let report = blocking(self.core.clone(), |core| core.status()).await?;
        Ok(Response::new(status_to_proto(report)))
    }

    async fn backup(
        &self,
        request: Request<BackupRequest>,
    ) -> Result<Response<BackupResponse>, Status> {
        self.authenticate(&request)?;
        let path = request.into_inner().path;
        let summary = blocking(self.core.clone(), move |core| core.backup(&path))
            .await?
            .map_err(to_status)?;
        Ok(Response::new(BackupResponse {
            manifest: Some(manifest_to_proto(summary)),
        }))
    }

    async fn restore_plan(
        &self,
        request: Request<RestorePlanRequest>,
    ) -> Result<Response<RestorePlanResponse>, Status> {
        self.authenticate(&request)?;
        let path = request.into_inner().path;
        let plan = blocking(self.core.clone(), move |core| core.restore_plan(&path))
            .await?
            .map_err(to_status)?;
        Ok(Response::new(restore_plan_to_proto(plan)))
    }

    async fn segments(
        &self,
        request: Request<SegmentsRequest>,
    ) -> Result<Response<TableResponse>, Status> {
        self.authenticate(&request)?;
        let table = request.into_inner().table;
        let data = blocking(self.core.clone(), move |core| core.segments(&table))
            .await?
            .map_err(to_status)?;
        Ok(Response::new(table_to_proto(data)))
    }

    async fn versions(
        &self,
        request: Request<VersionsRequest>,
    ) -> Result<Response<TableResponse>, Status> {
        self.authenticate(&request)?;
        let VersionsRequest { table, key } = request.into_inner();
        let data = blocking(self.core.clone(), move |core| {
            core.versions(&table, key.as_deref())
        })
        .await?
        .map_err(to_status)?;
        Ok(Response::new(table_to_proto(data)))
    }

    async fn audit_chain(
        &self,
        request: Request<AuditChainRequest>,
    ) -> Result<Response<TableResponse>, Status> {
        self.authenticate(&request)?;
        let AuditChainRequest { table, key } = request.into_inner();
        let data = blocking(self.core.clone(), move |core| {
            core.audit_chain(&table, key.as_deref())
        })
        .await?
        .map_err(to_status)?;
        Ok(Response::new(table_to_proto(data)))
    }
}

fn status_to_proto(report: StatusReport) -> StatusResponse {
    StatusResponse {
        ready: report.ready,
        wal_poisoned: report.wal_poisoned,
        server_version: report.server_version,
        table_count: report.table_count,
        user_count: report.user_count,
        tables: report
            .tables
            .into_iter()
            .map(|t| ProtoTableStatus {
                name: t.name,
                column_count: t.column_count,
                segment_count: t.segment_count,
            })
            .collect(),
    }
}

fn manifest_to_proto(summary: ManifestSummary) -> ProtoManifest {
    ProtoManifest {
        manifest_version: summary.manifest_version,
        stele_version: summary.stele_version,
        fence_micros: summary.fence_micros,
        commit_head: summary.commit_head,
        file_count: summary.file_count,
        total_bytes: summary.total_bytes,
    }
}

fn restore_plan_to_proto(plan: RestorePlan) -> RestorePlanResponse {
    RestorePlanResponse {
        valid: plan.valid,
        error: plan.error.unwrap_or_default(),
        manifest: plan.manifest.map(manifest_to_proto),
    }
}

fn table_to_proto(data: TableData) -> TableResponse {
    TableResponse {
        columns: data
            .columns
            .into_iter()
            .map(|(name, ty)| Column { name, r#type: ty })
            .collect(),
        rows: data
            .rows
            .into_iter()
            .map(|cells| Row {
                cells: cells
                    .into_iter()
                    .map(|cell| {
                        cell.map_or_else(
                            || Cell {
                                is_null: true,
                                value: String::new(),
                            },
                            |value| Cell {
                                is_null: false,
                                value,
                            },
                        )
                    })
                    .collect(),
            })
            .collect(),
    }
}
