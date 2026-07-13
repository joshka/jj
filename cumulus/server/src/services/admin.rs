use std::sync::Arc;

use cumulus_proto::PROTOCOL_VERSION;
use cumulus_proto::v1;
use cumulus_proto::v1::admin_service_server::AdminService;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::ServerState;

#[derive(Debug)]
pub(crate) struct AdminApi {
    // Held for future admin RPCs (Gc, §13.5); Health doesn't need it.
    _state: Arc<ServerState>,
}

impl AdminApi {
    pub(crate) fn new(state: Arc<ServerState>) -> Self {
        Self { _state: state }
    }
}

#[tonic::async_trait]
impl AdminService for AdminApi {
    async fn health(
        &self,
        _request: Request<v1::Empty>,
    ) -> Result<Response<v1::HealthResponse>, Status> {
        Ok(Response::new(v1::HealthResponse {
            status: "ok".to_owned(),
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn gc(
        &self,
        _request: Request<v1::GcRequest>,
    ) -> Result<Response<v1::GcResponse>, Status> {
        // SPEC-ONLY §13.5; needs an op-retention policy decision first.
        Err(Status::unimplemented("Gc is not implemented in v1"))
    }
}
