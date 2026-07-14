use std::sync::Arc;

use cumulus_proto::BLOB_FRAME_BYTES;
use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_proto::v1::object_service_server::ObjectService;
use cumulus_store::Blob;
use cumulus_store::ObjectKind;
use cumulus_store::Store;
use futures::StreamExt as _;
use futures::stream::BoxStream;
use jj_lib::object_id::ObjectId as _;
use prost::Message as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::ServerState;
use crate::blocking;
use crate::services::chunk_objects;
use crate::status::convert_error_to_status;
use crate::status::decode_error_to_status;
use crate::status::store_error_to_status;

#[derive(Debug)]
pub(crate) struct ObjectApi {
    state: Arc<ServerState>,
}

impl ObjectApi {
    pub(crate) fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

fn wire_kind(kind: i32) -> Result<ObjectKind, Status> {
    ObjectKind::from_i64(i64::from(kind))
        .ok_or_else(|| Status::invalid_argument(format!("invalid object kind {kind}")))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        write!(s, "{b:02x}").unwrap();
        s
    })
}

/// Verifies that client-supplied object bytes hash to the id they claim
/// (content-addressing: ids are permanent, §0.5) and ingests them.
fn ingest_object(store: &Store, object: &v1::Object) -> Result<(), Status> {
    let kind = wire_kind(object.kind)?;
    let actual_id = match kind {
        ObjectKind::Commit => {
            let proto = v1::Commit::decode(&*object.data)
                .map_err(|err| decode_error_to_status("commit", err))?;
            let relations = convert::commit_relations(&proto);
            let commit = convert::commit_from_proto(proto);
            let actual_id = ids::commit_id(&commit).to_bytes();
            if actual_id == object.id {
                store
                    .put_commit(&cumulus_store::CommitIngest {
                        id: &object.id,
                        data: &object.data,
                        parents: &relations.parents,
                        change_id: &relations.change_id,
                    })
                    .map_err(store_error_to_status)?;
            }
            actual_id
        }
        ObjectKind::Tree => {
            let proto = v1::Tree::decode(&*object.data)
                .map_err(|err| decode_error_to_status("tree", err))?;
            let tree = convert::tree_from_proto(proto).map_err(convert_error_to_status)?;
            let actual_id = ids::tree_id(&tree).to_bytes();
            if actual_id == object.id {
                store
                    .put_object(kind, &object.id, &object.data)
                    .map_err(store_error_to_status)?;
            }
            actual_id
        }
        ObjectKind::Symlink => {
            let actual_id = cumulus_store::hash_bytes(&object.data);
            if actual_id == object.id {
                store
                    .put_object(kind, &object.id, &object.data)
                    .map_err(store_error_to_status)?;
            }
            actual_id
        }
    };
    if actual_id != object.id {
        return Err(Status::invalid_argument(format!(
            "object {} hashes to {}",
            hex(&object.id),
            hex(&actual_id)
        )));
    }
    Ok(())
}

#[tonic::async_trait]
impl ObjectService for ObjectApi {
    async fn have_objects(
        &self,
        request: Request<v1::HaveObjectsRequest>,
    ) -> Result<Response<v1::HaveObjectsResponse>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let refs = request
            .refs
            .into_iter()
            .map(|object_ref| Ok((wire_kind(object_ref.kind)?, object_ref.id)))
            .collect::<Result<Vec<_>, Status>>()?;
        let missing =
            blocking(move || store.missing_objects(&refs).map_err(store_error_to_status)).await?;
        Ok(Response::new(v1::HaveObjectsResponse {
            missing: missing
                .into_iter()
                .map(|(kind, id)| v1::ObjectRef {
                    kind: kind as i32,
                    id,
                })
                .collect(),
        }))
    }

    type GetObjectsStream = BoxStream<'static, Result<v1::ObjectChunk, Status>>;

    async fn get_objects(
        &self,
        request: Request<v1::GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let refs = request
            .refs
            .into_iter()
            .map(|object_ref| Ok((wire_kind(object_ref.kind)?, object_ref.id)))
            .collect::<Result<Vec<_>, Status>>()?;
        let objects = blocking(move || {
            let mut objects = vec![];
            for (kind, id) in refs {
                let data = store
                    .get_object(kind, &id)
                    .map_err(store_error_to_status)?
                    .ok_or_else(|| {
                        Status::not_found(format!("object {} ({kind:?}) not found", hex(&id)))
                    })?;
                objects.push(v1::Object {
                    kind: kind as i32,
                    id,
                    data,
                });
            }
            Ok(objects)
        })
        .await?;
        let chunks = chunk_objects(objects);
        Ok(Response::new(
            futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn put_objects(
        &self,
        request: Request<Streaming<v1::ObjectChunk>>,
    ) -> Result<Response<v1::PutObjectsResponse>, Status> {
        let mut stream = request.into_inner();
        let mut store: Option<Arc<Store>> = None;
        let mut object_count: u32 = 0;
        while let Some(chunk) = stream.message().await? {
            let store = match &store {
                Some(store) => store.clone(),
                None => {
                    if chunk.repo.is_empty() {
                        return Err(Status::invalid_argument(
                            "first PutObjects chunk must set repo",
                        ));
                    }
                    let opened = self.state.repos.get(&chunk.repo).await?;
                    store = Some(opened.clone());
                    opened
                }
            };
            object_count += chunk.objects.len() as u32;
            blocking(move || {
                for object in &chunk.objects {
                    ingest_object(&store, object)?;
                }
                Ok(())
            })
            .await?;
        }
        Ok(Response::new(v1::PutObjectsResponse { object_count }))
    }

    type GetBlobStream = BoxStream<'static, Result<v1::BlobFrame, Status>>;

    async fn get_blob(
        &self,
        request: Request<v1::GetBlobRequest>,
    ) -> Result<Response<Self::GetBlobStream>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let id = request.id;
        let id_for_error = id.clone();
        let blob = blocking(move || store.get_blob(&id).map_err(store_error_to_status))
            .await?
            .ok_or_else(|| Status::not_found(format!("blob {} not found", hex(&id_for_error))))?;
        match blob {
            Blob::Inline(data) => {
                // Inline blobs are < 64 KiB, well under the 1 MiB frame size.
                let frame = v1::BlobFrame { offset: 0, data };
                Ok(Response::new(futures::stream::iter([Ok(frame)]).boxed()))
            }
            Blob::File { path, .. } => {
                let (sender, receiver) = tokio::sync::mpsc::channel(4);
                // A dedicated blocking task streams the CAS file in 1 MiB
                // frames; memory stays bounded by the channel depth.
                tokio::task::spawn_blocking(move || {
                    use std::io::Read as _;
                    let send = |frame: Result<v1::BlobFrame, Status>| {
                        // The receiver hanging up just means the client went
                        // away; stop reading.
                        sender.blocking_send(frame).is_ok()
                    };
                    let mut file = match std::fs::File::open(&path) {
                        Ok(file) => file,
                        Err(err) => {
                            send(Err(Status::internal(format!("opening CAS file: {err}"))));
                            return;
                        }
                    };
                    let mut offset = 0u64;
                    let mut buf = vec![0u8; BLOB_FRAME_BYTES];
                    loop {
                        match file.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                let frame = v1::BlobFrame {
                                    offset,
                                    data: buf[..n].to_vec(),
                                };
                                offset += n as u64;
                                if !send(Ok(frame)) {
                                    break;
                                }
                            }
                            Err(err) => {
                                send(Err(Status::internal(format!("reading CAS file: {err}"))));
                                break;
                            }
                        }
                    }
                });
                Ok(Response::new(ReceiverStream::new(receiver).boxed()))
            }
        }
    }

    async fn put_blob(
        &self,
        request: Request<Streaming<v1::PutBlobFrame>>,
    ) -> Result<Response<v1::PutBlobResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty PutBlob stream"))?;
        let Some(v1::put_blob_frame::Frame::Header(header)) = first.frame else {
            return Err(Status::invalid_argument(
                "first PutBlob frame must be the header",
            ));
        };
        let store = self.state.repos.get(&header.repo).await?;
        let mut writer = Some({
            let store = store.clone();
            blocking(move || store.blob_writer().map_err(store_error_to_status)).await?
        });
        let mut written: u64 = 0;
        while let Some(frame) = stream.message().await? {
            let data = match frame.frame {
                Some(v1::put_blob_frame::Frame::Data(data)) => data,
                Some(v1::put_blob_frame::Frame::Header(_)) => {
                    return Err(Status::invalid_argument("duplicate PutBlob header"));
                }
                None => continue,
            };
            written += data.len() as u64;
            let mut taken = writer.take().unwrap();
            writer = Some(
                blocking(move || {
                    taken.write(&data).map_err(store_error_to_status)?;
                    Ok(taken)
                })
                .await?,
            );
        }
        if written != header.size {
            return Err(Status::invalid_argument(format!(
                "blob size mismatch: header said {}, received {written}",
                header.size
            )));
        }
        let expected_id = header.id;
        let expected_for_finish = expected_id.clone();
        let writer = writer.take().unwrap();
        let (id, size) = blocking(move || {
            let expected = (!expected_for_finish.is_empty()).then_some(&*expected_for_finish);
            let finished = writer.finish(expected).map_err(store_error_to_status)?;
            let size = finished.size();
            let id = store
                .finish_blob(&finished)
                .map_err(store_error_to_status)?;
            Ok((id, size))
        })
        .await?;
        Ok(Response::new(v1::PutBlobResponse { id, size }))
    }

    type GetCommitsReachableFromStream = BoxStream<'static, Result<v1::ObjectChunk, Status>>;

    async fn get_commits_reachable_from(
        &self,
        request: Request<v1::ReachabilityRequest>,
    ) -> Result<Response<Self::GetCommitsReachableFromStream>, Status> {
        let request = request.into_inner();
        let store = self.state.repos.get(&request.repo).await?;
        let commits = blocking(move || {
            store
                .commits_reachable_from(&request.heads, &request.have)
                .map_err(store_error_to_status)
        })
        .await?;
        let objects = commits
            .into_iter()
            .map(|(id, data)| v1::Object {
                kind: v1::ObjectKind::Commit as i32,
                id,
                data,
            })
            .collect();
        let chunks = chunk_objects(objects);
        Ok(Response::new(
            futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn get_blob_manifest(
        &self,
        _request: Request<v1::GetBlobRequest>,
    ) -> Result<Response<v1::BlobManifest>, Status> {
        // SPEC-ONLY §13.1.
        Err(Status::unimplemented(
            "GetBlobManifest is not implemented in v1 (cumulus/docs/SPEC.md §13.1)",
        ))
    }

    type GetChunksStream = BoxStream<'static, Result<v1::ChunkData, Status>>;

    async fn get_chunks(
        &self,
        _request: Request<v1::GetChunksRequest>,
    ) -> Result<Response<Self::GetChunksStream>, Status> {
        // SPEC-ONLY §13.1.
        Err(Status::unimplemented(
            "GetChunks is not implemented in v1 (cumulus/docs/SPEC.md §13.1)",
        ))
    }
}
