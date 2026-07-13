use cumulus_proto::v1;
use cumulus_proto::v1::index_service_server::IndexService;
use futures::stream::BoxStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

/// SPEC-ONLY §13.2: declared for wire stability, all UNIMPLEMENTED in v1.
#[derive(Debug)]
pub(crate) struct IndexApi;

#[tonic::async_trait]
impl IndexService for IndexApi {
    async fn get_index_manifest(
        &self,
        _request: Request<v1::GetIndexManifestRequest>,
    ) -> Result<Response<v1::IndexManifest>, Status> {
        Err(Status::unimplemented(
            "IndexService is not implemented in v1 (SPEC.md §13.2)",
        ))
    }

    type GetIndexSegmentsStream = BoxStream<'static, Result<v1::IndexSegment, Status>>;

    async fn get_index_segments(
        &self,
        _request: Request<v1::GetIndexSegmentsRequest>,
    ) -> Result<Response<Self::GetIndexSegmentsStream>, Status> {
        Err(Status::unimplemented(
            "IndexService is not implemented in v1 (SPEC.md §13.2)",
        ))
    }
}
