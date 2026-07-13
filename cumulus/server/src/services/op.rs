use std::sync::Arc;

use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_proto::v1::op_service_server::OpService;
use cumulus_store::OpIngest;
use futures::StreamExt as _;
use futures::stream::BoxStream;
use jj_lib::object_id::ObjectId as _;
use prost::Message as _;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::ServerState;
use crate::blocking;
use crate::status::convert_error_to_status;
use crate::status::decode_error_to_status;
use crate::status::store_error_to_status;

#[derive(Debug)]
pub(crate) struct OpApi {
    state: Arc<ServerState>,
}

impl OpApi {
    pub(crate) fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        write!(s, "{b:02x}").unwrap();
        s
    })
}

#[tonic::async_trait]
impl OpService for OpApi {
    async fn get_op_heads(
        &self,
        request: Request<v1::RepoRef>,
    ) -> Result<Response<v1::OpHeadsResponse>, Status> {
        let store = self.state.repos.get(&request.into_inner().repo).await?;
        let op_head_ids = blocking(move || store.op_heads().map_err(store_error_to_status)).await?;
        Ok(Response::new(v1::OpHeadsResponse { op_head_ids }))
    }

    async fn have_ops(
        &self,
        request: Request<v1::HaveOpsRequest>,
    ) -> Result<Response<v1::HaveOpsResponse>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let missing_op_ids = blocking(move || {
            store
                .missing_ops(&request.op_ids)
                .map_err(store_error_to_status)
        })
        .await?;
        Ok(Response::new(v1::HaveOpsResponse { missing_op_ids }))
    }

    type GetOpsStream = BoxStream<'static, Result<v1::OpWithView, Status>>;

    async fn get_ops(
        &self,
        request: Request<v1::GetOpsRequest>,
    ) -> Result<Response<Self::GetOpsStream>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let ops = blocking(move || {
            let mut ops = vec![];
            for op_id in request.op_ids {
                let (op_data, view_id) = store
                    .get_op(&op_id)
                    .map_err(store_error_to_status)?
                    .ok_or_else(|| {
                        Status::not_found(format!("operation {} not found", hex(&op_id)))
                    })?;
                let view_data = store
                    .get_view(&view_id)
                    .map_err(store_error_to_status)?
                    .ok_or_else(|| {
                        Status::internal(format!(
                            "operation {} references missing view {}",
                            hex(&op_id),
                            hex(&view_id)
                        ))
                    })?;
                ops.push(v1::OpWithView {
                    op_id,
                    op_data,
                    view_id,
                    view_data,
                });
            }
            Ok(ops)
        })
        .await?;
        Ok(Response::new(
            futures::stream::iter(ops.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn push_ops(
        &self,
        request: Request<v1::PushOpsRequest>,
    ) -> Result<Response<v1::PushOpsResponse>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let op_head_ids = blocking(move || {
            let mut heads = store.op_heads().map_err(store_error_to_status)?;
            // Oldest-first: each op's parents were pushed in an earlier
            // iteration or already stored (§8.1 step 6).
            for op in request.ops {
                let proto = v1::Operation::decode(&*op.op_data)
                    .map_err(|err| decode_error_to_status("operation", err))?;
                let parents = proto.parents.clone();
                let view_id_in_op = proto.view_id.clone();
                let operation =
                    convert::operation_from_proto(proto).map_err(convert_error_to_status)?;
                let actual_op_id = ids::operation_id(&operation).to_bytes();
                if actual_op_id != op.op_id {
                    return Err(Status::invalid_argument(format!(
                        "operation {} hashes to {}",
                        hex(&op.op_id),
                        hex(&actual_op_id)
                    )));
                }
                if view_id_in_op != op.view_id {
                    return Err(Status::invalid_argument(format!(
                        "operation {} references view {}, not {}",
                        hex(&op.op_id),
                        hex(&view_id_in_op),
                        hex(&op.view_id)
                    )));
                }
                // The §6.2 precondition needs the view's head commit ids;
                // decode the batch copy, or the stored view when omitted.
                let stored_view_data;
                let (view_data, view_bytes) = if op.view_data.is_empty() {
                    match store.get_view(&op.view_id).map_err(store_error_to_status)? {
                        Some(data) => {
                            stored_view_data = data;
                            (None, &*stored_view_data)
                        }
                        None => {
                            return Err(Status::failed_precondition(format!(
                                "view {} for operation {} is neither in the batch nor stored",
                                hex(&op.view_id),
                                hex(&op.op_id)
                            )));
                        }
                    }
                } else {
                    (Some(&*op.view_data), &*op.view_data)
                };
                let view_proto = v1::View::decode(view_bytes)
                    .map_err(|err| decode_error_to_status("view", err))?;
                if view_data.is_some() {
                    let view = convert::view_from_proto(view_proto.clone())
                        .map_err(convert_error_to_status)?;
                    let actual_view_id = ids::view_id(&view).to_bytes();
                    if actual_view_id != op.view_id {
                        return Err(Status::invalid_argument(format!(
                            "view {} hashes to {}",
                            hex(&op.view_id),
                            hex(&actual_view_id)
                        )));
                    }
                }
                let view_head_commits = convert::view_head_ids(&view_proto);
                heads = store
                    .push_op(&OpIngest {
                        op_id: &op.op_id,
                        op_data: &op.op_data,
                        parents: &parents,
                        view_id: &op.view_id,
                        view_data,
                        view_head_commits: &view_head_commits,
                    })
                    .map_err(store_error_to_status)?;
            }
            Ok(heads)
        })
        .await?;
        Ok(Response::new(v1::PushOpsResponse { op_head_ids }))
    }

    async fn update_op_heads_cas(
        &self,
        _request: Request<v1::UpdateOpHeadsCasRequest>,
    ) -> Result<Response<v1::UpdateOpHeadsCasResponse>, Status> {
        // SPEC-ONLY §8.2 (Mode B).
        Err(Status::unimplemented(
            "UpdateOpHeadsCas is not implemented in v1 (SPEC.md §8.2)",
        ))
    }

    type SubscribeOpHeadsStream = BoxStream<'static, Result<v1::OpHeadsUpdate, Status>>;

    async fn subscribe_op_heads(
        &self,
        _request: Request<v1::RepoRef>,
    ) -> Result<Response<Self::SubscribeOpHeadsStream>, Status> {
        // SPEC-ONLY §8.2 (Mode B).
        Err(Status::unimplemented(
            "SubscribeOpHeads is not implemented in v1 (SPEC.md §8.2)",
        ))
    }
}
