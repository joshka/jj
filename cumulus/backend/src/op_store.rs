use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_store::Store;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::OpStore;
use jj_lib::op_store::OpStoreError;
use jj_lib::op_store::OpStoreResult;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RootOperationData;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use prost::Message as _;

use crate::config::CumulusConfig;

const OUTBOX_VIEW_KIND: i64 = 5;
const OUTBOX_OPERATION_KIND: i64 = 6;

/// Operation store backed by the same SQLite database as Cumulus objects.
pub struct CumulusOpStore {
    store: Arc<Store>,
    root_data: RootOperationData,
    root_operation_id: OperationId,
    root_view_id: ViewId,
}

impl fmt::Debug for CumulusOpStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CumulusOpStore")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl CumulusOpStore {
    /// Operation-store name written to `.jj/repo/op_store/type`.
    pub fn name() -> &'static str {
        "cumulus"
    }

    /// Initializes an operation store using the backend's shared cache.
    pub fn init(
        op_store_path: &Path,
        root_data: RootOperationData,
    ) -> Result<Self, BackendInitError> {
        Self::open(op_store_path, root_data).map_err(BackendInitError)
    }

    /// Loads an operation store using the backend's shared cache.
    pub fn load(
        op_store_path: &Path,
        root_data: RootOperationData,
    ) -> Result<Self, BackendLoadError> {
        Self::open(op_store_path, root_data).map_err(BackendLoadError)
    }

    fn open(
        op_store_path: &Path,
        root_data: RootOperationData,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let backend_path = backend_path(op_store_path)?;
        CumulusConfig::load(&backend_path)?;
        let store = Arc::new(Store::open(&backend_path.join("cumulus"))?);
        Ok(Self {
            store,
            root_data,
            root_operation_id: OperationId::from_bytes(&[0; ids::OPERATION_ID_LENGTH]),
            root_view_id: ViewId::from_bytes(&[0; ids::VIEW_ID_LENGTH]),
        })
    }

    /// Returns the shared local cache.
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }
}

#[async_trait]
impl OpStore for CumulusOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if id == &self.root_view_id {
            return Ok(View::make_root(self.root_data.root_commit_id.clone()));
        }
        let data = self
            .store
            .get_view(id.as_bytes())
            .map_err(to_other_error)?
            .ok_or_else(|| not_found(id))?;
        let proto = v1::View::decode(&*data).map_err(|error| read_error(id, error.into()))?;
        convert::view_from_proto(proto).map_err(|error| read_error(id, error.into()))
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let id = ids::view_id(view);
        let data = convert::view_to_proto(view).encode_to_vec();
        self.store
            .put_local_view(id.as_bytes(), &data, OUTBOX_VIEW_KIND)
            .map_err(|error| write_error("view", error.into()))?;
        Ok(id)
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if id == &self.root_operation_id {
            return Ok(Operation::make_root(self.root_view_id.clone()));
        }
        let (data, _) = self
            .store
            .get_op(id.as_bytes())
            .map_err(to_other_error)?
            .ok_or_else(|| not_found(id))?;
        let proto = v1::Operation::decode(&*data).map_err(|error| read_error(id, error.into()))?;
        convert::operation_from_proto(proto).map_err(|error| read_error(id, error.into()))
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        assert!(!operation.parents.is_empty());
        let id = ids::operation_id(operation);
        let proto = convert::operation_to_proto(operation);
        let parents = operation
            .parents
            .iter()
            .map(|id| id.to_bytes())
            .collect::<Vec<_>>();
        self.store
            .put_local_op(
                id.as_bytes(),
                &proto.encode_to_vec(),
                &parents,
                operation.view_id.as_bytes(),
                OUTBOX_OPERATION_KIND,
            )
            .map_err(|error| write_error("operation", error.into()))?;
        Ok(id)
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let mut matches = self
            .store
            .operation_ids_with_prefix(&prefix.hex())
            .map_err(to_other_error)?
            .into_iter()
            .map(OperationId::new)
            .collect::<Vec<_>>();
        if prefix.matches(&self.root_operation_id) {
            matches.push(self.root_operation_id.clone());
        }
        matches.sort();
        matches.dedup();
        Ok(match &matches[..] {
            [] => PrefixResolution::NoMatch,
            [id] => PrefixResolution::SingleMatch(id.clone()),
            _ => PrefixResolution::AmbiguousMatch,
        })
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}

pub(crate) fn backend_path(
    component_path: &Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let repo_path = component_path
        .parent()
        .ok_or_else(|| format!("{} has no repository parent", component_path.display()))?;
    Ok(repo_path.join("store"))
}

fn not_found(id: &impl jj_lib::object_id::ObjectId) -> OpStoreError {
    OpStoreError::ObjectNotFound {
        object_type: id.object_type(),
        hash: id.hex(),
        source: "object is absent from the Cumulus cache".into(),
    }
}

fn read_error(
    id: &impl jj_lib::object_id::ObjectId,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> OpStoreError {
    OpStoreError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source,
    }
}

fn write_error(
    object_type: &'static str,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> OpStoreError {
    OpStoreError::WriteObject {
        object_type,
        source,
    }
}

fn to_other_error(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> OpStoreError {
    OpStoreError::Other(source.into())
}

#[cfg(test)]
mod tests {
    use pollster::FutureExt as _;

    use super::*;
    use jj_lib::backend::CommitId;
    use jj_lib::backend::MillisSinceEpoch;
    use jj_lib::backend::Timestamp;
    use jj_lib::op_store::OperationMetadata;
    use jj_lib::op_store::TimestampRange;

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

    fn create_store(temp_dir: &Path) -> Result<CumulusOpStore, Box<dyn std::error::Error>> {
        let backend_path = temp_dir.join("repo/store");
        std::fs::create_dir_all(&backend_path)?;
        config().save(&backend_path)?;
        let op_path = temp_dir.join("repo/op_store");
        std::fs::create_dir_all(&op_path)?;
        Ok(CumulusOpStore::init(
            &op_path,
            RootOperationData {
                root_commit_id: CommitId::from_bytes(&[0; ids::COMMIT_ID_LENGTH]),
            },
        )?)
    }

    #[test]
    fn views_and_operations_round_trip_atomically() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let store = create_store(temp_dir.path())?;
        let view = View::make_root(CommitId::from_bytes(&[0; ids::COMMIT_ID_LENGTH]));
        let view_id = store.write_view(&view).block_on()?;
        let operation = Operation {
            view_id,
            parents: vec![store.root_operation_id().clone()],
            metadata: OperationMetadata {
                time: TimestampRange {
                    start: Timestamp {
                        timestamp: MillisSinceEpoch(0),
                        tz_offset: 0,
                    },
                    end: Timestamp {
                        timestamp: MillisSinceEpoch(1),
                        tz_offset: 0,
                    },
                },
                description: "test".into(),
                hostname: "host".into(),
                username: "user".into(),
                is_snapshot: false,
                workspace_name: None,
                attributes: Default::default(),
            },
            commit_predecessors: Some(Default::default()),
        };
        let operation_id = store.write_operation(&operation).block_on()?;
        assert_eq!(store.read_operation(&operation_id).block_on()?, operation);
        assert_eq!(store.store.outbox_list()?.len(), 2);
        Ok(())
    }
}
