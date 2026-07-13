use std::fmt;
use std::fs::File;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::SystemTime;

use async_trait::async_trait;
use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_proto::v1::object_service_client::ObjectServiceClient;
use cumulus_store::Blob;
use cumulus_store::CommitIngest;
use cumulus_store::ObjectKind;
use cumulus_store::Store;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::AllowStdIo;
use futures::io::Cursor;
use futures::stream;
use futures::stream::BoxStream;
use jj_lib::backend::Backend;
use jj_lib::backend::BackendError;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::backend::BackendResult;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyHistory;
use jj_lib::backend::CopyId;
use jj_lib::backend::CopyRecord;
use jj_lib::backend::FileId;
use jj_lib::backend::RelatedCopy;
use jj_lib::backend::SecureSig;
use jj_lib::backend::SigningFn;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::make_root_commit;
use jj_lib::index::Index;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use prost::Message as _;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::config::CumulusConfig;

const CACHE_DIR: &str = "cumulus";
const OUTBOX_BLOB_KIND: i64 = 4;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Native commit backend backed by a local Cumulus cache and lazy remote reads.
///
/// Local writes are durable before this backend returns and never perform network
/// I/O. Cache misses execute tonic futures on an owned Tokio runtime, allowing
/// jj-lib to poll the `Backend` implementation from `pollster` or any other
/// executor.
pub struct CumulusBackend {
    config: CumulusConfig,
    token: Option<String>,
    store: Arc<Store>,
    runtime: OnceLock<Result<tokio::runtime::Runtime, String>>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl fmt::Debug for CumulusBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CumulusBackend")
            .field("repo", &self.config.repo)
            .field("url", &self.config.url)
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl CumulusBackend {
    /// Backend name written to `.jj/repo/store/type`.
    pub fn name() -> &'static str {
        "cumulus"
    }

    /// Initializes a new local cache and persists its remote metadata.
    pub fn init(store_path: &Path, config: CumulusConfig) -> Result<Self, BackendInitError> {
        config
            .save(store_path)
            .map_err(|error| BackendInitError(error.into()))?;
        let backend = Self::from_config(store_path, config).map_err(BackendInitError)?;
        let empty_tree = Tree::default();
        let data = convert::tree_to_proto(&empty_tree).encode_to_vec();
        backend
            .store
            .put_object(ObjectKind::Tree, backend.empty_tree_id.as_bytes(), &data)
            .map_err(|error| BackendInitError(error.into()))?;
        Ok(backend)
    }

    /// Loads a backend from its persisted remote metadata.
    pub fn load(store_path: &Path) -> Result<Self, BackendLoadError> {
        let config =
            CumulusConfig::load(store_path).map_err(|error| BackendLoadError(error.into()))?;
        Self::from_config(store_path, config).map_err(BackendLoadError)
    }

    fn from_config(store_path: &Path, config: CumulusConfig) -> Result<Self, BoxError> {
        let token = config.token()?;
        let root_commit_id = CommitId::new(config.root_commit_bytes()?);
        let root_change_id = ids::root_change_id();
        let empty_tree_id = TreeId::new(config.empty_tree_bytes()?);
        let store = Arc::new(Store::open(&store_path.join(CACHE_DIR))?);
        Ok(Self {
            config,
            token,
            store,
            runtime: OnceLock::new(),
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    /// Returns the persistent repository configuration.
    pub fn config(&self) -> &CumulusConfig {
        &self.config
    }

    /// Returns the local SQLite+CAS cache.
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    fn runtime(&self) -> BackendResult<&tokio::runtime::Runtime> {
        self.runtime
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name("cumulus-client")
                    .build()
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(|message| BackendError::Other(message.clone().into()))
    }

    async fn cached_object(&self, kind: ObjectKind, id: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        let store = self.store.clone();
        let id = id.to_vec();
        self.runtime()?
            .spawn_blocking(move || store.get_object(kind, &id))
            .await
            .map_err(to_other_error)?
            .map_err(to_other_error)
    }

    async fn cache_object(
        &self,
        kind: ObjectKind,
        id: Vec<u8>,
        data: Vec<u8>,
    ) -> BackendResult<()> {
        let store = self.store.clone();
        self.runtime()?
            .spawn_blocking(move || store.put_object(kind, &id, &data))
            .await
            .map_err(to_other_error)?
            .map_err(to_other_error)
    }

    async fn cache_commit(
        &self,
        id: Vec<u8>,
        data: Vec<u8>,
        relations: convert::CommitRelations,
    ) -> BackendResult<()> {
        let store = self.store.clone();
        self.runtime()?
            .spawn_blocking(move || {
                store.put_commit(&CommitIngest {
                    id: &id,
                    data: &data,
                    parents: &relations.parents,
                    change_id: &relations.change_id,
                })
            })
            .await
            .map_err(to_other_error)?
            .map_err(to_other_error)
    }

    async fn fetch_object(&self, kind: ObjectKind, id: &[u8]) -> Result<Vec<u8>, BoxError> {
        let url = self.config.url.clone();
        let repo = self.config.repo.clone();
        let token = self.token.clone();
        let id = id.to_vec();
        self.runtime()?
            .spawn(async move {
                let channel = connect_lazy(&url)?;
                let mut client = ObjectServiceClient::new(channel);
                let request = v1::GetObjectsRequest {
                    repo,
                    refs: vec![v1::ObjectRef {
                        kind: kind as i32,
                        id: id.clone(),
                    }],
                };
                let response = client
                    .get_objects(authorize(request, token.as_deref())?)
                    .await?;
                let mut stream = response.into_inner();
                let mut found = None;
                while let Some(chunk) = stream.message().await? {
                    for object in chunk.objects {
                        if object.kind == kind as i32 && object.id == id {
                            found = Some(object.data);
                        }
                    }
                }
                found.ok_or_else(|| "Cumulus server returned no requested object".into())
            })
            .await
            .map_err(|error| -> BoxError { error.into() })?
    }

    async fn fetch_blob(&self, id: &FileId) -> BackendResult<()> {
        let url = self.config.url.clone();
        let repo = self.config.repo.clone();
        let token = self.token.clone();
        let expected_id = id.to_bytes();
        let store = self.store.clone();
        let result: Result<(), BoxError> = self
            .runtime()?
            .spawn(async move {
                let channel = connect_lazy(&url)?;
                let mut client = ObjectServiceClient::new(channel);
                let request = v1::GetBlobRequest {
                    repo,
                    id: expected_id.clone(),
                };
                let response = client
                    .get_blob(authorize(request, token.as_deref())?)
                    .await?;
                let mut stream = response.into_inner();
                let mut writer = store.blob_writer()?;
                let mut expected_offset = 0_u64;
                while let Some(frame) = stream.message().await? {
                    if frame.offset != expected_offset {
                        return Err(format!(
                            "Cumulus blob stream offset {}, expected {expected_offset}",
                            frame.offset
                        )
                        .into());
                    }
                    expected_offset += frame.data.len() as u64;
                    writer.write(&frame.data)?;
                }
                let finished = writer.finish(Some(&expected_id))?;
                store.finish_blob(&finished)?;
                Ok(())
            })
            .await
            .map_err(to_other_error)?;
        result.map_err(|source| read_error(id, offline_hint(source)))
    }

    async fn open_blob(&self, id: &FileId) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let store = self.store.clone();
        let id_bytes = id.to_bytes();
        let blob = self
            .runtime()?
            .spawn_blocking(move || store.get_blob(&id_bytes))
            .await
            .map_err(to_other_error)?
            .map_err(to_other_error)?
            .ok_or_else(|| read_error(id, "blob disappeared from the local cache".into()))?;
        match blob {
            Blob::Inline(data) => Ok(Box::pin(Cursor::new(data))),
            Blob::File { path, .. } => {
                let file = File::open(path).map_err(|error| read_error(id, error.into()))?;
                Ok(Box::pin(AllowStdIo::new(file)))
            }
        }
    }

    fn append_outbox(&self, kind: i64, id: &[u8]) -> BackendResult<()> {
        self.store
            .outbox_append(kind, id)
            .map(|_| ())
            .map_err(to_other_error)
    }
}

#[async_trait]
impl Backend for CumulusBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        self.config.commit_id_length
    }

    fn change_id_length(&self) -> usize {
        self.config.change_id_length
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        16
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        if !self.store.has_blob(id.as_bytes()).map_err(to_other_error)? {
            self.fetch_blob(id).await?;
        }
        self.open_blob(id).await
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut writer = self.store.blob_writer().map_err(to_other_error)?;
        let mut buffer = vec![0; cumulus_proto::BLOB_FRAME_BYTES];
        loop {
            let count = contents.read(&mut buffer).await.map_err(to_other_error)?;
            if count == 0 {
                break;
            }
            writer.write(&buffer[..count]).map_err(to_other_error)?;
        }
        let finished = writer.finish(None).map_err(to_other_error)?;
        let id = FileId::new(finished.id().to_vec());
        self.store.finish_blob(&finished).map_err(to_other_error)?;
        self.append_outbox(OUTBOX_BLOB_KIND, id.as_bytes())?;
        Ok(id)
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let data = match self
            .cached_object(ObjectKind::Symlink, id.as_bytes())
            .await?
        {
            Some(data) => data,
            None => {
                let data = self
                    .fetch_object(ObjectKind::Symlink, id.as_bytes())
                    .await
                    .map_err(|source| read_error(id, offline_hint(source)))?;
                if ids::symlink_id(std::str::from_utf8(&data).map_err(|source| {
                    BackendError::InvalidUtf8 {
                        object_type: id.object_type(),
                        hash: id.hex(),
                        source,
                    }
                })?) != *id
                {
                    return Err(read_error(id, "symlink content hash mismatch".into()));
                }
                self.cache_object(ObjectKind::Symlink, id.to_bytes(), data.clone())
                    .await?;
                data
            }
        };
        String::from_utf8(data).map_err(|error| read_error(id, error.into()))
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let id = ids::symlink_id(target);
        self.store
            .put_object(ObjectKind::Symlink, id.as_bytes(), target.as_bytes())
            .map_err(to_other_error)?;
        self.append_outbox(ObjectKind::Symlink as i64, id.as_bytes())?;
        Ok(id)
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The Cumulus backend doesn't support copies".into(),
        ))
    }

    async fn write_copy(&self, _copy: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The Cumulus backend doesn't support copies".into(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "The Cumulus backend doesn't support copies".into(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let data = match self.cached_object(ObjectKind::Tree, id.as_bytes()).await? {
            Some(data) => data,
            None => {
                let data = self
                    .fetch_object(ObjectKind::Tree, id.as_bytes())
                    .await
                    .map_err(|source| read_error(id, offline_hint(source)))?;
                let proto =
                    v1::Tree::decode(&*data).map_err(|error| read_error(id, error.into()))?;
                let tree = convert::tree_from_proto(proto)
                    .map_err(|error| read_error(id, error.into()))?;
                if ids::tree_id(&tree) != *id {
                    return Err(read_error(id, "tree content hash mismatch".into()));
                }
                self.cache_object(ObjectKind::Tree, id.to_bytes(), data.clone())
                    .await?;
                data
            }
        };
        let proto = v1::Tree::decode(&*data).map_err(|error| read_error(id, error.into()))?;
        convert::tree_from_proto(proto).map_err(|error| read_error(id, error.into()))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let id = ids::tree_id(tree);
        let data = convert::tree_to_proto(tree).encode_to_vec();
        self.store
            .put_object(ObjectKind::Tree, id.as_bytes(), &data)
            .map_err(to_other_error)?;
        self.append_outbox(ObjectKind::Tree as i64, id.as_bytes())?;
        Ok(id)
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if id == &self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }
        let data = match self
            .cached_object(ObjectKind::Commit, id.as_bytes())
            .await?
        {
            Some(data) => data,
            None => {
                let data = self
                    .fetch_object(ObjectKind::Commit, id.as_bytes())
                    .await
                    .map_err(|source| read_error(id, offline_hint(source)))?;
                let proto =
                    v1::Commit::decode(&*data).map_err(|error| read_error(id, error.into()))?;
                let relations = convert::commit_relations(&proto);
                let commit = convert::commit_from_proto(proto);
                if ids::commit_id(&commit) != *id {
                    return Err(read_error(id, "commit content hash mismatch".into()));
                }
                self.cache_commit(id.to_bytes(), data.clone(), relations)
                    .await?;
                data
            }
        };
        let proto = v1::Commit::decode(&*data).map_err(|error| read_error(id, error.into()))?;
        Ok(convert::commit_from_proto(proto))
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        assert!(commit.secure_sig.is_none(), "commit.secure_sig was set");
        if commit.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }
        let mut proto = convert::commit_to_proto(&commit);
        if let Some(sign) = sign_with {
            let data = proto.encode_to_vec();
            let sig = sign(&data).map_err(to_other_error)?;
            proto.secure_sig = Some(sig.clone());
            commit.secure_sig = Some(SecureSig { data, sig });
        }
        let id = ids::commit_id(&commit);
        let data = proto.encode_to_vec();
        let relations = convert::commit_relations(&proto);
        self.store
            .put_commit(&CommitIngest {
                id: id.as_bytes(),
                data: &data,
                parents: &relations.parents,
                change_id: &relations.change_id,
            })
            .map_err(to_other_error)?;
        self.append_outbox(ObjectKind::Commit as i64, id.as_bytes())?;
        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

fn connect_lazy(url: &str) -> Result<Channel, BoxError> {
    Ok(Endpoint::from_shared(url.to_owned())?.connect_lazy())
}

fn authorize<T>(message: T, token: Option<&str>) -> Result<Request<T>, BoxError> {
    let mut request = Request::new(message);
    if let Some(token) = token {
        let value = MetadataValue::try_from(format!("Bearer {token}"))?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(request)
}

fn offline_hint(source: BoxError) -> BoxError {
    format!("{source}; object is not cached locally; run `jj cumulus sync`").into()
}

fn read_error(id: &impl ObjectId, source: BoxError) -> BackendError {
    BackendError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source,
    }
}

fn to_other_error(source: impl Into<BoxError>) -> BackendError {
    BackendError::Other(source.into())
}

#[cfg(test)]
mod tests {
    use pollster::FutureExt as _;

    use super::*;
    use jj_lib::merge::Merge;

    fn config() -> CumulusConfig {
        CumulusConfig {
            repo: "test".into(),
            url: "http://127.0.0.1:1".into(),
            token_file: None,
            auto_push: false,
            commit_id_length: ids::COMMIT_ID_LENGTH,
            change_id_length: ids::CHANGE_ID_LENGTH,
            root_commit_id: "00".repeat(ids::COMMIT_ID_LENGTH),
            empty_tree_id: ids::empty_tree_id().hex(),
            protocol_version: cumulus_proto::PROTOCOL_VERSION,
        }
    }

    #[test]
    fn local_writes_round_trip_without_a_server() -> Result<(), BoxError> {
        let temp_dir = tempfile::tempdir()?;
        let backend = CumulusBackend::init(temp_dir.path(), config())?;
        let tree = Tree::default();
        let tree_id = backend.write_tree(RepoPath::root(), &tree).block_on()?;
        assert_eq!(
            backend.read_tree(RepoPath::root(), &tree_id).block_on()?,
            tree
        );

        let root = backend.read_commit(backend.root_commit_id()).block_on()?;
        assert_eq!(root.root_tree, Merge::resolved(tree_id));
        assert_eq!(backend.store.outbox_list()?.len(), 1);
        Ok(())
    }

    #[test]
    fn uncached_read_explains_explicit_sync() -> Result<(), BoxError> {
        let temp_dir = tempfile::tempdir()?;
        let backend = CumulusBackend::init(temp_dir.path(), config())?;
        let id = TreeId::from_bytes(&[1; ids::COMMIT_ID_LENGTH]);
        let error = backend
            .read_tree(RepoPath::root(), &id)
            .block_on()
            .unwrap_err();
        let source = std::error::Error::source(&error).unwrap();
        assert!(source.to_string().contains("jj cumulus sync"));
        Ok(())
    }
}
