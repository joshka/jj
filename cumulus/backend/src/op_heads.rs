use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cumulus_store::Store;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_heads_store::OpHeadsStoreLock;
use jj_lib::op_store::OperationId;

use crate::config::CumulusConfig;
use crate::op_store::backend_path;

/// Local-first operation-head store with detached best-effort auto-push.
pub struct CumulusOpHeadsStore {
    store: Arc<Store>,
    auto_push: bool,
    workspace_root: PathBuf,
}

impl fmt::Debug for CumulusOpHeadsStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CumulusOpHeadsStore")
            .field("store", &self.store)
            .field("auto_push", &self.auto_push)
            .finish_non_exhaustive()
    }
}

impl CumulusOpHeadsStore {
    /// Op-heads-store name written to `.jj/repo/op_heads/type`.
    pub fn name() -> &'static str {
        "cumulus"
    }

    /// Initializes the local head set with the deterministic root operation.
    pub fn init(
        op_heads_path: &Path,
        root_operation_id: &OperationId,
    ) -> Result<Self, BackendInitError> {
        let store = Self::open(op_heads_path).map_err(BackendInitError)?;
        store
            .store
            .init_op_head(root_operation_id.as_bytes())
            .map_err(|error| BackendInitError(error.into()))?;
        Ok(store)
    }

    /// Loads an existing local head set.
    pub fn load(op_heads_path: &Path) -> Result<Self, BackendLoadError> {
        Self::open(op_heads_path).map_err(BackendLoadError)
    }

    fn open(op_heads_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let backend_path = backend_path(op_heads_path)?;
        let config = CumulusConfig::load(&backend_path)?;
        let store = Arc::new(Store::open(&backend_path.join("cumulus"))?);
        let repo_path = op_heads_path
            .parent()
            .ok_or_else(|| format!("{} has no repository parent", op_heads_path.display()))?;
        let workspace_root = repo_path
            .parent()
            .and_then(Path::parent)
            .unwrap_or(repo_path)
            .to_owned();
        Ok(Self {
            store,
            auto_push: config.auto_push,
            workspace_root,
        })
    }

    fn spawn_auto_push(&self) {
        if !self.auto_push || std::env::var_os("JJ_CUMULUS_PUSHER").is_some() {
            return;
        }
        let result = std::env::current_exe().and_then(|executable| {
            std::process::Command::new(executable)
                .args(["cumulus", "sync", "--push-only", "--quiet"])
                .current_dir(&self.workspace_root)
                .env("JJ_CUMULUS_PUSHER", "1")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map(|_| ())
        });
        if let Err(error) = result {
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let message = format!("{timestamp}: failed to spawn detached Cumulus pusher: {error}");
            self.store
                .sync_state_set("remote/origin/last_push_error", message.as_bytes())
                .ok();
        }
    }
}

#[derive(Debug)]
struct CumulusOpHeadsLock;

impl OpHeadsStoreLock for CumulusOpHeadsLock {}

#[async_trait]
impl OpHeadsStore for CumulusOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let old_ids = old_ids.iter().map(|id| id.to_bytes()).collect::<Vec<_>>();
        self.store
            .update_local_op_heads(&old_ids, new_id.as_bytes())
            .map_err(|source| OpHeadsStoreError::Write {
                new_op_id: new_id.clone(),
                source: source.into(),
            })?;
        self.spawn_auto_push();
        Ok(())
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let heads = self
            .store
            .op_heads()
            .map_err(|error| OpHeadsStoreError::Read(error.into()))?;
        if heads.is_empty() {
            return Err(OpHeadsStoreError::Read(
                "Corrupt Cumulus repository: no head operation".into(),
            ));
        }
        Ok(heads.into_iter().map(OperationId::new).collect())
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        // Each head replacement is a single BEGIN IMMEDIATE transaction. A
        // longer-lived write transaction here would deadlock the op/view writes
        // performed while jj resolves divergent heads.
        Ok(Box::new(CumulusOpHeadsLock))
    }
}
