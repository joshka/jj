use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;
use std::time::SystemTime;

use cumulus_proto::MAX_OBJECTS_PER_CHUNK;
use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_proto::v1::object_service_client::ObjectServiceClient;
use cumulus_proto::v1::op_service_client::OpServiceClient;
use cumulus_store::Blob;
use cumulus_store::CommitIngest;
use cumulus_store::ObjectKind;
use cumulus_store::Store;
use jj_lib::object_id::ObjectId as _;
use prost::Message as _;
use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::config::CumulusConfig;

const PUSH_LOCK_KEY: &str = "remote/origin/push_lock";
const LAST_HEADS_KEY: &str = "remote/origin/last_op_heads";
const LAST_PUSH_ERROR_KEY: &str = "remote/origin/last_push_error";

/// Result counts from one Mode A synchronization.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncReport {
    /// Operations uploaded.
    pub pushed_operations: usize,
    /// Commit metadata objects uploaded.
    pub pushed_commits: usize,
    /// Operations downloaded.
    pub pulled_operations: usize,
    /// Commit metadata objects downloaded.
    pub pulled_commits: usize,
    /// Whether another process already owned the push lock.
    pub push_already_running: bool,
}

/// Local queue and last-error information shown by `jj cumulus sync --status`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncStatus {
    /// Number of queued local outbox records.
    pub outbox_depth: usize,
    /// Last detached-push error, if any.
    pub last_push_error: Option<String>,
    /// Locally cached metadata-object count represented by the outbox.
    pub queued_metadata: usize,
    /// Locally cached blob count represented by the outbox.
    pub queued_blobs: usize,
}

/// Error from an explicit or detached Mode A synchronization.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SyncError {
    /// A local cache operation failed.
    #[error("Cumulus local cache operation failed")]
    Store(#[from] cumulus_store::StoreError),
    /// A wire object or operation was malformed or violated content addressing.
    #[error("invalid Cumulus wire data: {0}")]
    InvalidData(String),
    /// Connecting or making an RPC failed.
    #[error("Cumulus remote operation failed: {0}")]
    Remote(String),
    /// The owned runtime task failed.
    #[error("Cumulus runtime task failed: {0}")]
    Runtime(String),
}

/// Mode A push/pull engine over one named `origin` remote.
#[derive(Debug)]
pub struct SyncEngine {
    remote: RemoteSync,
    runtime: tokio::runtime::Runtime,
}

impl SyncEngine {
    /// Creates a sync engine for a local cache and its remote configuration.
    pub fn new(config: CumulusConfig, store: Arc<Store>) -> Result<Self, SyncError> {
        let token = config
            .token()
            .map_err(|error| SyncError::Remote(error.to_string()))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("cumulus-sync")
            .build()
            .map_err(|error| SyncError::Runtime(error.to_string()))?;
        Ok(Self {
            remote: RemoteSync {
                config,
                token,
                store,
            },
            runtime,
        })
    }

    /// Pushes local Mode A state, draining work that arrives during the push.
    ///
    /// A second process returns immediately when the logical push lock is held.
    pub async fn push(&self) -> Result<SyncReport, SyncError> {
        let remote = self.remote.clone();
        self.runtime
            .spawn(async move { remote.push_until_drained().await })
            .await
            .map_err(|error| SyncError::Runtime(error.to_string()))?
    }

    /// Pulls remote operations, views, and commit metadata without trees or blobs.
    pub async fn pull(&self) -> Result<SyncReport, SyncError> {
        let remote = self.remote.clone();
        self.runtime
            .spawn(async move { remote.pull_once().await })
            .await
            .map_err(|error| SyncError::Runtime(error.to_string()))?
    }

    /// Pushes then pulls, preserving Mode A's local-first ordering.
    pub async fn sync(&self) -> Result<SyncReport, SyncError> {
        let mut report = self.push().await?;
        let pull = self.pull().await?;
        report.pulled_operations = pull.pulled_operations;
        report.pulled_commits = pull.pulled_commits;
        Ok(report)
    }

    /// Reads queue depth and the durable last detached-push error.
    pub fn status(&self) -> Result<SyncStatus, SyncError> {
        let outbox = self.remote.store.outbox_list()?;
        let last_push_error = self
            .remote
            .store
            .sync_state_get(LAST_PUSH_ERROR_KEY)?
            .filter(|value| !value.is_empty())
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        let queued_blobs = outbox.iter().filter(|(_, kind, _)| *kind == 4).count();
        let outbox_depth = outbox.iter().filter(|(_, kind, _)| *kind == 6).count();
        Ok(SyncStatus {
            outbox_depth,
            last_push_error,
            queued_metadata: outbox.len() - queued_blobs,
            queued_blobs,
        })
    }
}

#[derive(Clone, Debug)]
struct RemoteSync {
    config: CumulusConfig,
    token: Option<String>,
    store: Arc<Store>,
}

#[derive(Debug)]
struct LocalOp {
    id: Vec<u8>,
    data: Vec<u8>,
    view_id: Vec<u8>,
    view_data: Vec<u8>,
    operation: v1::Operation,
    view: v1::View,
}

impl RemoteSync {
    async fn push_until_drained(self) -> Result<SyncReport, SyncError> {
        let owner = format!("{}:{}", std::process::id(), unix_timestamp());
        if !self
            .store
            .sync_lock_try_acquire(PUSH_LOCK_KEY, owner.as_bytes())?
        {
            return Ok(SyncReport {
                push_already_running: true,
                ..SyncReport::default()
            });
        }

        let result = async {
            let mut combined = SyncReport::default();
            loop {
                match self.push_once().await {
                    Ok(report) => {
                        combined.pushed_operations += report.pushed_operations;
                        combined.pushed_commits += report.pushed_commits;
                        if self.store.outbox_list()?.is_empty() {
                            self.store.sync_state_set(LAST_PUSH_ERROR_KEY, b"")?;
                            break Ok(combined);
                        }
                    }
                    Err(error) => break Err(error),
                }
            }
        }
        .await;
        if let Err(error) = &result {
            let message = format!("{}: {error}", unix_timestamp());
            self.store
                .sync_state_set(LAST_PUSH_ERROR_KEY, message.as_bytes())?;
        }
        self.store
            .sync_lock_release(PUSH_LOCK_KEY, owner.as_bytes())?;
        result
    }

    async fn push_once(&self) -> Result<SyncReport, SyncError> {
        let outbox = self.store.outbox_list()?;
        let covered_seq = outbox.last().map(|(seq, _, _)| *seq);
        let local_heads = self.store.op_heads()?;
        let _remote_heads = self.get_op_heads().await?;
        let ops = self.collect_unsent_ops(&local_heads).await?;
        let referenced_commits = ops
            .iter()
            .flat_map(|op| convert::view_head_ids(&op.view))
            .collect::<HashSet<_>>();
        let commits = self.collect_missing_commits(referenced_commits).await?;
        let (trees_and_symlinks, blobs) = self.collect_missing_tree_closure(&commits).await?;

        self.put_objects(trees_and_symlinks).await?;
        for blob_id in blobs {
            self.put_blob(blob_id).await?;
        }
        self.put_commits(&commits).await?;
        self.push_ops(&ops).await?;

        let heads = self.get_op_heads().await?;
        self.store.sync_state_set(
            LAST_HEADS_KEY,
            &v1::OpHeadsResponse { op_head_ids: heads }.encode_to_vec(),
        )?;
        if let Some(seq) = covered_seq {
            self.store.outbox_drain(seq)?;
        }
        Ok(SyncReport {
            pushed_operations: ops.len(),
            pushed_commits: commits.len(),
            ..SyncReport::default()
        })
    }

    async fn pull_once(&self) -> Result<SyncReport, SyncError> {
        let remote_heads = self.get_op_heads().await?;
        let (pulled_ops, views) = self.pull_operations(&remote_heads).await?;
        let commit_heads = views
            .iter()
            .flat_map(convert::view_head_ids)
            .collect::<HashSet<_>>();
        let mut have = vec![];
        for id in &commit_heads {
            if self.store.has_object(ObjectKind::Commit, id)? {
                have.push(id.clone());
            }
        }
        let pulled_commits = self
            .pull_commits(commit_heads.into_iter().collect(), have)
            .await?;
        for head in &remote_heads {
            self.store.merge_local_op_head(head)?;
        }
        self.store.sync_state_set(
            LAST_HEADS_KEY,
            &v1::OpHeadsResponse {
                op_head_ids: remote_heads,
            }
            .encode_to_vec(),
        )?;
        Ok(SyncReport {
            pulled_operations: pulled_ops,
            pulled_commits,
            ..SyncReport::default()
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<Vec<u8>>, SyncError> {
        let mut client = OpServiceClient::new(connect_lazy(&self.config.url)?);
        let request = v1::RepoRef {
            repo: self.config.repo.clone(),
        };
        let response = client
            .get_op_heads(authorize(request, self.token.as_deref())?)
            .await
            .map_err(remote_error)?;
        Ok(response.into_inner().op_head_ids)
    }

    async fn collect_unsent_ops(&self, heads: &[Vec<u8>]) -> Result<Vec<LocalOp>, SyncError> {
        let mut queue = VecDeque::from(heads.to_vec());
        let mut visited = HashSet::new();
        let mut collected = HashMap::new();
        while !queue.is_empty() {
            let mut candidates = vec![];
            while candidates.len() < MAX_OBJECTS_PER_CHUNK {
                let Some(id) = queue.pop_front() else {
                    break;
                };
                if is_root(&id) || !visited.insert(id.clone()) {
                    continue;
                }
                candidates.push(id);
            }
            if candidates.is_empty() {
                continue;
            }
            let missing = self.have_ops(candidates).await?;
            for id in missing {
                let (data, view_id) = self.store.get_op(&id)?.ok_or_else(|| {
                    SyncError::InvalidData(format!("local operation {} is missing", hex(&id)))
                })?;
                let view_data = self.store.get_view(&view_id)?.ok_or_else(|| {
                    SyncError::InvalidData(format!("local view {} is missing", hex(&view_id)))
                })?;
                let operation = v1::Operation::decode(&*data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                let view = v1::View::decode(&*view_data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                queue.extend(operation.parents.iter().cloned());
                collected.insert(
                    id.clone(),
                    LocalOp {
                        id,
                        data,
                        view_id,
                        view_data,
                        operation,
                        view,
                    },
                );
            }
        }
        topological_ops(collected)
    }

    async fn have_ops(&self, ids: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, SyncError> {
        let mut client = OpServiceClient::new(connect_lazy(&self.config.url)?);
        let request = v1::HaveOpsRequest {
            repo: self.config.repo.clone(),
            op_ids: ids,
        };
        let response = client
            .have_ops(authorize(request, self.token.as_deref())?)
            .await
            .map_err(remote_error)?;
        Ok(response.into_inner().missing_op_ids)
    }

    async fn collect_missing_commits(
        &self,
        heads: HashSet<Vec<u8>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, SyncError> {
        let mut queue = VecDeque::from_iter(heads);
        let mut visited = HashSet::new();
        let mut commits = HashMap::new();
        while !queue.is_empty() {
            let mut candidates = vec![];
            while candidates.len() < MAX_OBJECTS_PER_CHUNK {
                let Some(id) = queue.pop_front() else {
                    break;
                };
                if is_root(&id) || !visited.insert(id.clone()) {
                    continue;
                }
                candidates.push(id);
            }
            if candidates.is_empty() {
                continue;
            }
            let refs = candidates
                .into_iter()
                .map(|id| (ObjectKind::Commit, id))
                .collect();
            for (_, id) in self.have_objects(refs).await? {
                let data = self
                    .store
                    .get_object(ObjectKind::Commit, &id)?
                    .ok_or_else(|| {
                        SyncError::InvalidData(format!("local commit {} is missing", hex(&id)))
                    })?;
                let proto = v1::Commit::decode(&*data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                queue.extend(proto.parents.iter().cloned());
                commits.insert(id, data);
            }
        }
        Ok(commits)
    }

    async fn collect_missing_tree_closure(
        &self,
        commits: &HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(Vec<v1::Object>, HashSet<Vec<u8>>), SyncError> {
        let mut tree_queue = VecDeque::new();
        for data in commits.values() {
            let commit = v1::Commit::decode(&**data)
                .map_err(|error| SyncError::InvalidData(error.to_string()))?;
            tree_queue.extend(commit.root_tree);
        }
        let mut visited = HashSet::new();
        let mut metadata = vec![];
        let mut blobs = HashSet::new();
        let mut symlinks = HashSet::new();
        while !tree_queue.is_empty() {
            let mut candidates = vec![];
            while candidates.len() < MAX_OBJECTS_PER_CHUNK {
                let Some(id) = tree_queue.pop_front() else {
                    break;
                };
                if !visited.insert(id.clone()) {
                    continue;
                }
                candidates.push((ObjectKind::Tree, id));
            }
            for (_, id) in self.have_objects(candidates).await? {
                let data = self
                    .store
                    .get_object(ObjectKind::Tree, &id)?
                    .ok_or_else(|| {
                        SyncError::InvalidData(format!("local tree {} is missing", hex(&id)))
                    })?;
                let tree = v1::Tree::decode(&*data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                for entry in tree.entries {
                    match entry.value.and_then(|value| value.value) {
                        Some(v1::tree_value::Value::TreeId(id)) => tree_queue.push_back(id),
                        Some(v1::tree_value::Value::File(file)) => {
                            blobs.insert(file.id);
                        }
                        Some(v1::tree_value::Value::SymlinkId(id)) => {
                            symlinks.insert(id);
                        }
                        Some(v1::tree_value::Value::SubmoduleId(_)) | None => {}
                    }
                }
                metadata.push(v1::Object {
                    kind: ObjectKind::Tree as i32,
                    id,
                    data,
                });
            }
        }
        if !symlinks.is_empty() {
            let refs = symlinks
                .into_iter()
                .map(|id| (ObjectKind::Symlink, id))
                .collect();
            for (_, id) in self.have_objects(refs).await? {
                let data = self
                    .store
                    .get_object(ObjectKind::Symlink, &id)?
                    .ok_or_else(|| {
                        SyncError::InvalidData(format!("local symlink {} is missing", hex(&id)))
                    })?;
                metadata.push(v1::Object {
                    kind: ObjectKind::Symlink as i32,
                    id,
                    data,
                });
            }
        }
        Ok((metadata, blobs))
    }

    async fn have_objects(
        &self,
        refs: Vec<(ObjectKind, Vec<u8>)>,
    ) -> Result<Vec<(ObjectKind, Vec<u8>)>, SyncError> {
        if refs.is_empty() {
            return Ok(vec![]);
        }
        let mut client = ObjectServiceClient::new(connect_lazy(&self.config.url)?);
        let request = v1::HaveObjectsRequest {
            repo: self.config.repo.clone(),
            refs: refs
                .into_iter()
                .map(|(kind, id)| v1::ObjectRef {
                    kind: kind as i32,
                    id,
                })
                .collect(),
        };
        let response = client
            .have_objects(authorize(request, self.token.as_deref())?)
            .await
            .map_err(remote_error)?;
        response
            .into_inner()
            .missing
            .into_iter()
            .map(|reference| {
                let kind = ObjectKind::from_i64(i64::from(reference.kind)).ok_or_else(|| {
                    SyncError::InvalidData(format!("invalid object kind {}", reference.kind))
                })?;
                Ok((kind, reference.id))
            })
            .collect()
    }

    async fn put_objects(&self, objects: Vec<v1::Object>) -> Result<(), SyncError> {
        if objects.is_empty() {
            return Ok(());
        }
        let chunks = objects
            .chunks(MAX_OBJECTS_PER_CHUNK)
            .enumerate()
            .map(|(index, objects)| v1::ObjectChunk {
                repo: if index == 0 {
                    self.config.repo.clone()
                } else {
                    String::new()
                },
                objects: objects.to_vec(),
            })
            .collect::<Vec<_>>();
        let mut client = ObjectServiceClient::new(connect_lazy(&self.config.url)?);
        client
            .put_objects(authorize(
                tokio_stream::iter(chunks),
                self.token.as_deref(),
            )?)
            .await
            .map_err(remote_error)?;
        Ok(())
    }

    async fn put_commits(&self, commits: &HashMap<Vec<u8>, Vec<u8>>) -> Result<(), SyncError> {
        let mut ordered = commits
            .iter()
            .map(|(id, data)| {
                let generation = self.store.commit_generation(id)?.ok_or_else(|| {
                    SyncError::InvalidData(format!("commit {} has no generation", hex(id)))
                })?;
                Ok((generation, id.clone(), data.clone()))
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        ordered.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
        let objects = ordered
            .into_iter()
            .map(|(_, id, data)| v1::Object {
                kind: ObjectKind::Commit as i32,
                id,
                data,
            })
            .collect();
        self.put_objects(objects).await
    }

    async fn put_blob(&self, id: Vec<u8>) -> Result<(), SyncError> {
        let blob = self
            .store
            .get_blob(&id)?
            .ok_or_else(|| SyncError::InvalidData(format!("local blob {} is missing", hex(&id))))?;
        let size = blob.size();
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let header = v1::PutBlobFrame {
            frame: Some(v1::put_blob_frame::Frame::Header(
                v1::put_blob_frame::Header {
                    repo: self.config.repo.clone(),
                    id,
                    size,
                },
            )),
        };
        sender
            .send(header)
            .await
            .map_err(|_| SyncError::Remote("blob upload channel closed".into()))?;
        tokio::task::spawn_blocking(move || stream_blob_frames(blob, sender));
        let mut client = ObjectServiceClient::new(connect_lazy(&self.config.url)?);
        client
            .put_blob(authorize(
                ReceiverStream::new(receiver),
                self.token.as_deref(),
            )?)
            .await
            .map_err(remote_error)?;
        Ok(())
    }

    async fn push_ops(&self, ops: &[LocalOp]) -> Result<(), SyncError> {
        if ops.is_empty() {
            return Ok(());
        }
        let request = v1::PushOpsRequest {
            repo: self.config.repo.clone(),
            ops: ops
                .iter()
                .map(|op| v1::OpWithView {
                    op_id: op.id.clone(),
                    op_data: op.data.clone(),
                    view_id: op.view_id.clone(),
                    view_data: op.view_data.clone(),
                })
                .collect(),
        };
        let mut client = OpServiceClient::new(connect_lazy(&self.config.url)?);
        client
            .push_ops(authorize(request, self.token.as_deref())?)
            .await
            .map_err(remote_error)?;
        Ok(())
    }

    async fn pull_operations(
        &self,
        heads: &[Vec<u8>],
    ) -> Result<(usize, Vec<v1::View>), SyncError> {
        let mut queue = VecDeque::from(heads.to_vec());
        let mut visited = HashSet::new();
        let mut count = 0;
        let mut views = vec![];
        while !queue.is_empty() {
            let mut ids = vec![];
            while ids.len() < MAX_OBJECTS_PER_CHUNK {
                let Some(id) = queue.pop_front() else {
                    break;
                };
                if is_root(&id) || !visited.insert(id.clone()) || self.store.has_op(&id)? {
                    continue;
                }
                ids.push(id);
            }
            if ids.is_empty() {
                continue;
            }
            let mut client = OpServiceClient::new(connect_lazy(&self.config.url)?);
            let request = v1::GetOpsRequest {
                repo: self.config.repo.clone(),
                op_ids: ids,
            };
            let response = client
                .get_ops(authorize(request, self.token.as_deref())?)
                .await
                .map_err(remote_error)?;
            let mut stream = response.into_inner();
            while let Some(item) = stream.message().await.map_err(remote_error)? {
                let operation = v1::Operation::decode(&*item.op_data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                let view = v1::View::decode(&*item.view_data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                verify_op_with_view(&item, &operation, &view)?;
                queue.extend(operation.parents.iter().cloned());
                self.store.put_remote_op(
                    &item.op_id,
                    &item.op_data,
                    &operation.parents,
                    &item.view_id,
                    &item.view_data,
                )?;
                views.push(view);
                count += 1;
            }
        }
        Ok((count, views))
    }

    async fn pull_commits(
        &self,
        heads: Vec<Vec<u8>>,
        have: Vec<Vec<u8>>,
    ) -> Result<usize, SyncError> {
        if heads.is_empty() {
            return Ok(0);
        }
        let mut client = ObjectServiceClient::new(connect_lazy(&self.config.url)?);
        let request = v1::ReachabilityRequest {
            repo: self.config.repo.clone(),
            heads,
            have,
        };
        let response = client
            .get_commits_reachable_from(authorize(request, self.token.as_deref())?)
            .await
            .map_err(remote_error)?;
        let mut stream = response.into_inner();
        let mut count = 0;
        while let Some(chunk) = stream.message().await.map_err(remote_error)? {
            for object in chunk.objects {
                if object.kind != ObjectKind::Commit as i32 {
                    return Err(SyncError::InvalidData(
                        "reachability stream returned a non-commit object".into(),
                    ));
                }
                let proto = v1::Commit::decode(&*object.data)
                    .map_err(|error| SyncError::InvalidData(error.to_string()))?;
                let relations = convert::commit_relations(&proto);
                let commit = convert::commit_from_proto(proto);
                if ids::commit_id(&commit).as_bytes() != object.id {
                    return Err(SyncError::InvalidData(format!(
                        "commit {} failed content-hash verification",
                        hex(&object.id)
                    )));
                }
                self.store.put_commit(&CommitIngest {
                    id: &object.id,
                    data: &object.data,
                    parents: &relations.parents,
                    change_id: &relations.change_id,
                })?;
                count += 1;
            }
        }
        Ok(count)
    }
}

fn topological_ops(mut ops: HashMap<Vec<u8>, LocalOp>) -> Result<Vec<LocalOp>, SyncError> {
    let mut ordered = vec![];
    while !ops.is_empty() {
        let mut ready = ops
            .iter()
            .filter(|(_, op)| {
                op.operation
                    .parents
                    .iter()
                    .all(|parent| !ops.contains_key(parent))
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ready.sort();
        if ready.is_empty() {
            return Err(SyncError::InvalidData(
                "operation graph contains a cycle".into(),
            ));
        }
        for id in ready {
            ordered.push(ops.remove(&id).unwrap());
        }
    }
    Ok(ordered)
}

fn verify_op_with_view(
    item: &v1::OpWithView,
    operation: &v1::Operation,
    view: &v1::View,
) -> Result<(), SyncError> {
    let decoded_operation = convert::operation_from_proto(operation.clone())
        .map_err(|error| SyncError::InvalidData(error.to_string()))?;
    let decoded_view = convert::view_from_proto(view.clone())
        .map_err(|error| SyncError::InvalidData(error.to_string()))?;
    if operation.view_id != item.view_id
        || ids::operation_id(&decoded_operation).as_bytes() != item.op_id
        || ids::view_id(&decoded_view).as_bytes() != item.view_id
    {
        return Err(SyncError::InvalidData(
            "operation or view failed content-hash verification".into(),
        ));
    }
    Ok(())
}

fn stream_blob_frames(blob: Blob, sender: tokio::sync::mpsc::Sender<v1::PutBlobFrame>) {
    let mut reader: Box<dyn Read> = match blob {
        Blob::Inline(data) => Box::new(std::io::Cursor::new(data)),
        Blob::File { path, .. } => match std::fs::File::open(path) {
            Ok(file) => Box::new(file),
            Err(_) => return,
        },
    };
    let mut buffer = vec![0; cumulus_proto::BLOB_FRAME_BYTES];
    loop {
        let Ok(count) = reader.read(&mut buffer) else {
            return;
        };
        if count == 0 {
            return;
        }
        let frame = v1::PutBlobFrame {
            frame: Some(v1::put_blob_frame::Frame::Data(buffer[..count].to_vec())),
        };
        if sender.blocking_send(frame).is_err() {
            return;
        }
    }
}

fn connect_lazy(url: &str) -> Result<Channel, SyncError> {
    let endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|error| SyncError::Remote(error.to_string()))?;
    Ok(endpoint.connect_lazy())
}

fn authorize<T>(message: T, token: Option<&str>) -> Result<Request<T>, SyncError> {
    let mut request = Request::new(message);
    if let Some(token) = token {
        let value = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|error| SyncError::Remote(error.to_string()))?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(request)
}

fn remote_error(error: impl std::fmt::Display) -> SyncError {
    SyncError::Remote(error.to_string())
}

fn is_root(id: &[u8]) -> bool {
    id.iter().all(|byte| *byte == 0)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut output, byte| {
        write!(output, "{byte:02x}").unwrap();
        output
    })
}
