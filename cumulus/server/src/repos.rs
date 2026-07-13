//! Per-repo store management (SPEC.md §6.3).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use cumulus_proto::PROTOCOL_VERSION;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_store::Store;
use jj_lib::object_id::ObjectId as _;
use prost::Message as _;
use tokio::sync::RwLock;
use tonic::Status;

use crate::blocking;
use crate::status::store_error_to_status;

const REPO_INFO_KEY: &str = "repo_info";

/// Open per-repo store handles, keyed by validated repo name.
#[derive(Debug)]
pub struct RepoManager {
    repos_dir: PathBuf,
    stores: RwLock<HashMap<String, Arc<Store>>>,
}

/// Rejects names that could escape the repos directory or surprise a
/// filesystem: one path component of `[A-Za-z0-9._-]`, not starting with a
/// dot or dash.
fn validate_repo_name(name: &str) -> Result<(), Status> {
    let valid = !name.is_empty()
        && name.len() <= 100
        && !name.starts_with(['.', '-'])
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "invalid repo name {name:?}"
        )))
    }
}

fn new_repo_info(name: &str) -> v1::RepoInfo {
    v1::RepoInfo {
        name: name.to_owned(),
        commit_id_length: ids::COMMIT_ID_LENGTH as u32,
        change_id_length: ids::CHANGE_ID_LENGTH as u32,
        root_commit_id: ids::root_commit_id().to_bytes(),
        empty_tree_id: ids::empty_tree_id().to_bytes(),
        protocol_version: PROTOCOL_VERSION,
        op_mode: v1::OpMode::LocalFirst as i32,
        // Reserved for §13.6; unused in v1.
        completeness: v1::Completeness::Unspecified as i32,
    }
}

impl RepoManager {
    /// Creates a manager rooted at `<data_dir>/repos`.
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self {
            repos_dir: data_dir.join("repos"),
            stores: RwLock::new(HashMap::new()),
        }
    }

    async fn open_store(&self, name: &str, create: bool) -> Result<Arc<Store>, Status> {
        validate_repo_name(name)?;
        if let Some(store) = self.stores.read().await.get(name) {
            return Ok(store.clone());
        }
        let dir = self.repos_dir.join(name);
        let name_owned = name.to_owned();
        let store = blocking(move || {
            if !create && !dir.join("meta.sqlite").is_file() {
                return Err(Status::not_found(format!("repo {name_owned:?} not found")));
            }
            let store = Store::open(&dir).map_err(store_error_to_status)?;
            Ok(Arc::new(store))
        })
        .await?;
        let mut stores = self.stores.write().await;
        // A concurrent open may have won; keep the first handle.
        let store = stores.entry(name.to_owned()).or_insert(store).clone();
        Ok(store)
    }

    /// Opens an existing repo. NOT_FOUND if it does not exist.
    pub async fn get(&self, name: &str) -> Result<Arc<Store>, Status> {
        self.open_store(name, false).await
    }

    /// Creates a repo (idempotent by name, spec §6.2) and returns its info.
    pub async fn create(&self, name: &str) -> Result<(Arc<Store>, v1::RepoInfo), Status> {
        let store = self.open_store(name, true).await?;
        let info = new_repo_info(name);
        let stored_info = {
            let store = store.clone();
            let info = info.clone();
            blocking(
                move || match store.kv_get(REPO_INFO_KEY).map_err(store_error_to_status)? {
                    Some(existing) => decode_repo_info(&existing),
                    None => {
                        store
                            .kv_set(REPO_INFO_KEY, &info.encode_to_vec())
                            .map_err(store_error_to_status)?;
                        Ok(info)
                    }
                },
            )
            .await?
        };
        Ok((store, stored_info))
    }

    /// Reads the repo info recorded at create time.
    pub async fn info(&self, name: &str) -> Result<v1::RepoInfo, Status> {
        let store = self.get(name).await?;
        blocking(move || {
            let bytes = store
                .kv_get(REPO_INFO_KEY)
                .map_err(store_error_to_status)?
                .ok_or_else(|| Status::internal("repo exists but has no repo info"))?;
            decode_repo_info(&bytes)
        })
        .await
    }

    /// Lists all repos under the data directory.
    pub async fn list(&self) -> Result<Vec<v1::RepoInfo>, Status> {
        let repos_dir = self.repos_dir.clone();
        let names = blocking(move || {
            let mut names = vec![];
            let entries = match std::fs::read_dir(&repos_dir) {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(names),
                Err(err) => return Err(Status::internal(format!("listing repos: {err}"))),
            };
            for entry in entries {
                let entry =
                    entry.map_err(|err| Status::internal(format!("listing repos: {err}")))?;
                let Ok(name) = entry.file_name().into_string() else {
                    continue;
                };
                if validate_repo_name(&name).is_ok() && entry.path().join("meta.sqlite").is_file() {
                    names.push(name);
                }
            }
            names.sort();
            Ok(names)
        })
        .await?;
        let mut infos = vec![];
        for name in names {
            infos.push(self.info(&name).await?);
        }
        Ok(infos)
    }
}

fn decode_repo_info(bytes: &[u8]) -> Result<v1::RepoInfo, Status> {
    v1::RepoInfo::decode(bytes).map_err(|err| Status::internal(format!("corrupt repo info: {err}")))
}
