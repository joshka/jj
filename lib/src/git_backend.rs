// Copyright 2020 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![expect(missing_docs)]

use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::Utf8Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::Cursor;
use futures::stream::BoxStream;
pub use girt::ObjectFormat;
use itertools::Itertools as _;
use once_cell::sync::OnceCell as OnceLock;
use pollster::FutureExt as _;
use prost::Message as _;
use smallvec::SmallVec;
use thiserror::Error;

use crate::backend::Backend;
use crate::backend::BackendError;
use crate::backend::BackendInitError;
use crate::backend::BackendLoadError;
use crate::backend::BackendResult;
use crate::backend::ChangeId;
use crate::backend::Commit;
use crate::backend::CommitId;
use crate::backend::CopyHistory;
use crate::backend::CopyId;
use crate::backend::CopyRecord;
use crate::backend::FileId;
use crate::backend::MillisSinceEpoch;
use crate::backend::RelatedCopy;
use crate::backend::SecureSig;
use crate::backend::Signature;
use crate::backend::SigningFn;
use crate::backend::SymlinkId;
use crate::backend::Timestamp;
use crate::backend::Tree;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::backend::make_root_commit;
use crate::config::ConfigGetError;
use crate::file_util;
use crate::file_util::BadPathEncoding;
use crate::file_util::IoResultExt as _;
use crate::file_util::PathError;
use crate::git::GitSettings;
use crate::index::Index;
use crate::lock::FileLock;
use crate::merge::Merge;
use crate::merge::MergeBuilder;
use crate::object_id::ObjectId;
use crate::repo_path::RepoPath;
use crate::repo_path::RepoPathBuf;
use crate::repo_path::RepoPathComponentBuf;
use crate::settings::UserSettings;
use crate::stacked_table::MutableTable;
use crate::stacked_table::ReadonlyTable;
use crate::stacked_table::TableSegment as _;
use crate::stacked_table::TableStore;
use crate::stacked_table::TableStoreError;

const CHANGE_ID_LENGTH: usize = 16;
/// Ref namespace used only for preventing GC.
const NO_GC_REF_NAMESPACE: &str = "refs/jj/keep/";

pub const JJ_CONFLICT_README_FILE_NAME: &str = "JJ-CONFLICT-README";

pub const JJ_TREES_COMMIT_HEADER: &str = "jj:trees";
pub const JJ_CONFLICT_LABELS_COMMIT_HEADER: &str = "jj:conflict-labels";
pub const CHANGE_ID_COMMIT_HEADER: &str = "change-id";

#[derive(Debug, Error)]
pub enum GitBackendInitError {
    #[error("Failed to initialize git repository")]
    InitRepository(#[source] girt::InitError),
    #[error("Failed to open git repository")]
    OpenRepository(#[source] girt::OpenError),
    #[error("Failed to encode git repository path")]
    EncodeRepositoryPath(#[source] BadPathEncoding),
    #[error(transparent)]
    Config(ConfigGetError),
    #[error(transparent)]
    Path(PathError),
}

impl From<Box<GitBackendInitError>> for BackendInitError {
    fn from(err: Box<GitBackendInitError>) -> Self {
        Self(err)
    }
}

#[derive(Debug, Error)]
pub enum GitBackendLoadError {
    #[error("Failed to open git repository")]
    OpenRepository(#[source] girt::OpenError),
    #[error("Failed to decode git repository path")]
    DecodeRepositoryPath(#[source] BadPathEncoding),
    #[error(transparent)]
    Config(ConfigGetError),
    #[error(transparent)]
    Path(PathError),
}

impl From<Box<GitBackendLoadError>> for BackendLoadError {
    fn from(err: Box<GitBackendLoadError>) -> Self {
        Self(err)
    }
}

/// `GitBackend`-specific error that may occur after the backend is loaded.
#[derive(Debug, Error)]
pub enum GitBackendError {
    #[error("Failed to read non-git metadata")]
    ReadMetadata(#[source] TableStoreError),
    #[error("Failed to write non-git metadata")]
    WriteMetadata(#[source] TableStoreError),
}

impl From<GitBackendError> for BackendError {
    fn from(err: GitBackendError) -> Self {
        Self::Other(err.into())
    }
}

#[derive(Debug, Error)]
pub enum GitRepoAtWorkdirError {
    #[error("No Git repository found at {path}")]
    NotFound {
        path: PathBuf,
        source: girt::OpenError,
    },
    #[error("Unrelated Git repository found at {path}")]
    Unrelated { path: PathBuf },
    #[error("Failed to open Git repository")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Error)]
pub enum GitGcError {
    #[error("Failed to plan retained Git objects")]
    Plan(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Refusing to remove Git objects because the reachability scan was incomplete")]
    IncompletePlan,
    #[error("Failed to repack or prune Git objects")]
    Maintenance(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Resource bounds for ordinary object reads. jj stores whole files in blobs,
/// so these are sized to accept any object Git itself would write.
/// Git configuration sources for repositories opened on behalf of the user.
///
/// This mirrors Git's selection of system, XDG, and home configuration files,
/// plus `GIT_CONFIG_{COUNT,KEY_n,VALUE_n}` overrides, from the process
/// environment.
pub(crate) fn git_config_inputs() -> girt::config::ConfigInputs {
    let system = cfg!(unix).then(|| PathBuf::from("/etc/gitconfig"));
    girt::config::ConfigInputs::from_environment(system, |name| std::env::var_os(name), 1024)
        .unwrap_or_default()
}

/// Options for creating a repository like `git init`, honoring the user's
/// `init.defaultBranch`.
fn init_options(
    kind: girt::InitKind,
    format: girt::ObjectFormat,
) -> Result<girt::InitOptions, Box<GitBackendInitError>> {
    let options = girt::InitOptions::new(kind).object_format(format);
    // An unreadable user configuration leaves Git's built-in defaults in place.
    match girt::Config::resolve(&git_config_inputs()) {
        Ok(config) => options
            .defaults_from(&config)
            .map_err(|err| Box::new(GitBackendInitError::InitRepository(err))),
        Err(_) => Ok(options),
    }
}

/// Opens the Git repository at `path` (a Git directory, a `.git` file, or a
/// working tree root) with user-level configuration.
pub(crate) fn open_git_repository(path: &Path) -> Result<girt::Repository, girt::OpenError> {
    girt::Repository::open_with_config(path, &git_config_inputs())
}

/// Converts a jj object id of valid length to a girt object id.
pub(crate) fn to_git_object_id(format: girt::ObjectFormat, id: &impl ObjectId) -> girt::ObjectId {
    girt::ObjectId::from_bytes(format, id.as_bytes()).expect("object id of repository length")
}

pub struct GitBackend {
    /// Repository opened at load time. Its configuration is a snapshot; use
    /// [`Self::git_repo()`] for a current view.
    repo: girt::Repository,
    /// Pack snapshot plus live loose-object reads. Refreshed when an object is
    /// not found, since other processes (and fetch) may install new packs.
    objects: RwLock<girt::Objects>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    shallow_root_ids: OnceLock<Vec<CommitId>>,
    extra_metadata_store: TableStore,
    cached_extra_metadata: Mutex<Option<Arc<ReadonlyTable>>>,
    write_change_id_header: bool,
    user_name: String,
    user_email: String,
}

impl GitBackend {
    pub const NAME: &str = "git";

    fn new(
        repo: girt::Repository,
        extra_metadata_store: TableStore,
        git_settings: GitSettings,
        settings: &UserSettings,
    ) -> Result<Self, girt::OpenError> {
        let format = repo.object_format();
        let objects = repo.objects(girt::PackLimits::trusted()).map_err(|err| {
            girt::OpenError::Malformed {
                path: repo.object_dir().to_owned(),
                reason: err.to_string(),
            }
        })?;
        let root_commit_id = CommitId::from_bytes(girt::ObjectId::null(format).as_bytes());
        let root_change_id = ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH]);
        let empty_tree_id =
            TreeId::from_bytes(format.hash_object(girt::ObjectKind::Tree, b"").as_bytes());
        Ok(Self {
            repo,
            objects: RwLock::new(objects),
            root_commit_id,
            root_change_id,
            empty_tree_id,
            shallow_root_ids: OnceLock::new(),
            extra_metadata_store,
            cached_extra_metadata: Mutex::new(None),
            write_change_id_header: git_settings.write_change_id_header,
            user_name: settings.user_name().to_owned(),
            user_email: settings.user_email().to_owned(),
        })
    }

    pub fn init_internal(
        settings: &UserSettings,
        store_path: &Path,
        object_hash: girt::ObjectFormat,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let git_repo_path = Path::new("git");
        girt::Repository::init_with_options(
            store_path.join(git_repo_path),
            &init_options(girt::InitKind::Bare, object_hash)?,
        )
        .map_err(GitBackendInitError::InitRepository)?;
        let git_repo = open_git_repository(&store_path.join(git_repo_path))
            .map_err(GitBackendInitError::OpenRepository)?;
        Self::init_with_repo(settings, store_path, git_repo_path, git_repo)
    }

    /// Initializes backend by creating a new Git repo at the specified
    /// workspace path. The workspace directory must exist.
    pub fn init_colocated(
        settings: &UserSettings,
        store_path: &Path,
        workspace_root: &Path,
        object_hash: girt::ObjectFormat,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let canonical_workspace_root = {
            let path = store_path.join(workspace_root);
            dunce::canonicalize(&path)
                .context(&path)
                .map_err(GitBackendInitError::Path)?
        };
        girt::Repository::init_with_options(
            &canonical_workspace_root,
            &init_options(girt::InitKind::Worktree, object_hash)?,
        )
        .map_err(GitBackendInitError::InitRepository)?;
        let git_repo = open_git_repository(&canonical_workspace_root.join(".git"))
            .map_err(GitBackendInitError::OpenRepository)?;
        let git_repo_path = workspace_root.join(".git");
        Self::init_with_repo(settings, store_path, &git_repo_path, git_repo)
    }

    /// Initializes backend with an existing Git repo at the specified path.
    pub fn init_external(
        settings: &UserSettings,
        store_path: &Path,
        git_repo_path: &Path,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let canonical_git_repo_path = {
            let path = store_path.join(git_repo_path);
            canonicalize_git_repo_path(&path)
                .context(&path)
                .map_err(GitBackendInitError::Path)?
        };
        let git_repo = open_git_repository(&canonical_git_repo_path)
            .map_err(GitBackendInitError::OpenRepository)?;
        Self::init_with_repo(settings, store_path, git_repo_path, git_repo)
    }

    fn init_with_repo(
        settings: &UserSettings,
        store_path: &Path,
        git_repo_path: &Path,
        repo: girt::Repository,
    ) -> Result<Self, Box<GitBackendInitError>> {
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendInitError::Config)?;
        let extra_path = store_path.join("extra");
        fs::create_dir(&extra_path)
            .context(&extra_path)
            .map_err(GitBackendInitError::Path)?;
        let target_path = store_path.join("git_target");
        let git_repo_path = if cfg!(windows) && git_repo_path.is_relative() {
            // When a repository is created in Windows, format the path with *forward
            // slashes* and not backwards slashes. This makes it possible to use the same
            // repository under Windows Subsystem for Linux.
            //
            // This only works for relative paths. If the path is absolute, there's not much
            // we can do, and it simply won't work inside and outside WSL at the same time.
            file_util::slash_path(git_repo_path)
        } else {
            git_repo_path.into()
        };
        let git_repo_path_bytes = file_util::path_to_bytes(&git_repo_path)
            .map_err(GitBackendInitError::EncodeRepositoryPath)?;
        fs::write(&target_path, git_repo_path_bytes)
            .context(&target_path)
            .map_err(GitBackendInitError::Path)?;
        let extra_metadata_store = TableStore::init(extra_path, repo.object_format().digest_len());
        Self::new(repo, extra_metadata_store, git_settings, settings)
            .map_err(|err| Box::new(GitBackendInitError::OpenRepository(err)))
    }

    pub fn load(
        settings: &UserSettings,
        store_path: &Path,
    ) -> Result<Self, Box<GitBackendLoadError>> {
        let git_repo_path = {
            let target_path = store_path.join("git_target");
            let git_repo_path_bytes = fs::read(&target_path)
                .context(&target_path)
                .map_err(GitBackendLoadError::Path)?;
            let git_repo_path = file_util::path_from_bytes(&git_repo_path_bytes)
                .map_err(GitBackendLoadError::DecodeRepositoryPath)?;
            let git_repo_path = store_path.join(git_repo_path);
            canonicalize_git_repo_path(&git_repo_path)
                .context(&git_repo_path)
                .map_err(GitBackendLoadError::Path)?
        };
        let repo =
            open_git_repository(&git_repo_path).map_err(GitBackendLoadError::OpenRepository)?;
        let extra_metadata_store =
            TableStore::load(store_path.join("extra"), repo.object_format().digest_len());
        let git_settings =
            GitSettings::from_settings(settings).map_err(GitBackendLoadError::Config)?;
        Self::new(repo, extra_metadata_store, git_settings, settings)
            .map_err(|err| Box::new(GitBackendLoadError::OpenRepository(err)))
    }

    /// Object format (hash function) of the underlying Git repository.
    pub fn object_format(&self) -> girt::ObjectFormat {
        self.repo.object_format()
    }

    /// Returns a freshly opened handle to the underlying Git repository.
    ///
    /// The handle reflects configuration changes made since the backend was
    /// loaded. Falls back to the load-time snapshot if reopening fails.
    ///
    /// Use [`Self::open_git_repo_at_workdir()`] for worktree operations.
    pub fn git_repo(&self) -> girt::Repository {
        open_git_repository(self.repo.git_dir()).unwrap_or_else(|err| {
            tracing::warn!(?err, "failed to reopen Git repository");
            self.repo.clone()
        })
    }

    /// Identity used for reflog entries written on behalf of the user.
    pub fn committer_signature(&self) -> girt::Signature {
        let now = crate::backend::Timestamp::now();
        let name = if self.user_name.is_empty() {
            EMPTY_STRING_PLACEHOLDER
        } else {
            &self.user_name
        };
        let email = if self.user_email.is_empty() {
            EMPTY_STRING_PLACEHOLDER
        } else {
            &self.user_email
        };
        girt::Signature {
            name: sanitize_identity(name),
            email: sanitize_identity(email),
            seconds: now.timestamp.0.div_euclid(1000).max(0),
            offset_minutes: now.tz_offset.try_into().unwrap_or(0),
        }
    }

    /// Reopens the repository at the given workspace path.
    pub fn open_git_repo_at_workdir(
        &self,
        path: &Path,
    ) -> Result<girt::Repository, GitRepoAtWorkdirError> {
        // Try the open repository first.
        let open_repo = self.git_repo();
        if let Some(workdir) = open_repo.worktree()
            && (workdir == path || dunce::canonicalize(path).is_ok_and(|path| workdir == path))
        {
            return Ok(open_repo);
        }

        // The input path doesn't include ".git".
        // A `.git` that isn't a valid repository means there's no repository here.
        let work_repo = open_git_repository(path).map_err(|err| match err {
            err @ (girt::OpenError::NotFound(_) | girt::OpenError::Malformed { .. }) => {
                GitRepoAtWorkdirError::NotFound {
                    path: path.to_owned(),
                    source: err,
                }
            }
            err => GitRepoAtWorkdirError::Other(err.into()),
        })?;
        let canonicalize = |path: &Path| {
            dunce::canonicalize(path).map_err(|err| GitRepoAtWorkdirError::Other(err.into()))
        };
        if open_repo.common_dir() == work_repo.common_dir()
            || canonicalize(open_repo.common_dir())? == canonicalize(work_repo.common_dir())?
        {
            Ok(work_repo)
        } else {
            let path = path.to_owned();
            Err(GitRepoAtWorkdirError::Unrelated { path })
        }
    }

    /// Path to the `.git` directory or the repository itself if it's bare.
    pub fn git_repo_path(&self) -> &Path {
        self.repo.git_dir()
    }

    /// Path to the working tree of the underlying Git repository, if any.
    pub fn git_workdir(&self) -> Option<&Path> {
        self.repo.worktree()
    }

    fn shallow_root_ids(&self) -> &[CommitId] {
        // Shallow roots are read once when the repository is opened. Refreshing
        // them on every read would be expensive and bad for consistency.
        self.shallow_root_ids.get_or_init(|| {
            self.repo
                .shallow_roots()
                .iter()
                .map(|oid| CommitId::from_bytes(oid.as_bytes()))
                .collect()
        })
    }

    fn cached_extra_metadata_table(&self) -> BackendResult<Arc<ReadonlyTable>> {
        let mut locked_head = self.cached_extra_metadata.lock().unwrap();
        match locked_head.as_ref() {
            Some(head) => Ok(head.clone()),
            None => {
                let table = self
                    .extra_metadata_store
                    .get_head()
                    .map_err(GitBackendError::ReadMetadata)?;
                *locked_head = Some(table.clone());
                Ok(table)
            }
        }
    }

    fn read_extra_metadata_table_locked(&self) -> BackendResult<(Arc<ReadonlyTable>, FileLock)> {
        let table = self
            .extra_metadata_store
            .get_head_locked()
            .map_err(GitBackendError::ReadMetadata)?;
        Ok(table)
    }

    fn save_extra_metadata_table(
        &self,
        mut_table: MutableTable,
        _table_lock: &FileLock,
    ) -> BackendResult<()> {
        let table = self
            .extra_metadata_store
            .save_table(mut_table)
            .map_err(GitBackendError::WriteMetadata)?;
        // Since the parent table was the head, saved table are likely to be new head.
        // If it's not, cache will be reloaded when entry can't be found.
        *self.cached_extra_metadata.lock().unwrap() = Some(table);
        Ok(())
    }

    /// Imports the given commits and ancestors from the backing Git repo.
    ///
    /// The `head_ids` may contain commits that have already been imported, but
    /// the caller should filter them out to eliminate redundant I/O processing.
    #[tracing::instrument(skip(self, head_ids))]
    pub fn import_head_commits<'a>(
        &self,
        head_ids: impl IntoIterator<Item = &'a CommitId>,
    ) -> BackendResult<()> {
        let head_ids: HashSet<&CommitId> = head_ids
            .into_iter()
            .filter(|&id| *id != self.root_commit_id)
            .collect();
        if head_ids.is_empty() {
            return Ok(());
        }

        for id in &head_ids {
            self.validate_git_object_id(*id)?;
        }
        // Create no-gc ref even if known to the extras table. Concurrent GC
        // process might have deleted the no-gc ref.
        let edits = head_ids
            .iter()
            .map(|id| self.to_no_gc_ref_update(id))
            .collect_vec();
        self.edit_references(&edits)?;

        // These commits are imported from Git. Make our change ids persist (otherwise
        // future write_commit() could reassign new change id.)
        tracing::debug!(
            heads_count = head_ids.len(),
            "import extra metadata entries"
        );
        let (table, table_lock) = self.read_extra_metadata_table_locked()?;
        let mut mut_table = table.start_mutation();
        self.import_extra_metadata_entries_from_heads(&mut mut_table, &table_lock, &head_ids)?;
        self.save_extra_metadata_table(mut_table, &table_lock)
    }

    fn import_extra_metadata_entries_from_heads(
        &self,
        mut_table: &mut MutableTable,
        _table_lock: &FileLock,
        head_ids: &HashSet<&CommitId>,
    ) -> BackendResult<()> {
        let shallow_roots = self.shallow_root_ids();
        let mut work_ids = head_ids
            .iter()
            .filter(|&id| mut_table.get_value(id.as_bytes()).is_none())
            .map(|&id| id.clone())
            .collect_vec();
        while let Some(id) = work_ids.pop() {
            let data = self.read_git_object(&id, girt::ObjectKind::Commit)?;
            let is_shallow = shallow_roots.contains(&id);
            // TODO(#1624): Should we read the root tree here and check if it has a
            // `.jjconflict-...` entries? That could happen if the user used `git` to e.g.
            // change the description of a commit with tree-level conflicts.
            let commit =
                commit_from_git_without_root_parent(&id, self.object_format(), &data, is_shallow)?;
            mut_table.add_entry(id.to_bytes(), serialize_extras(&commit));
            work_ids.extend(
                commit
                    .parents
                    .into_iter()
                    .filter(|id| mut_table.get_value(id.as_bytes()).is_none()),
            );
        }
        Ok(())
    }

    fn validate_git_object_id(&self, id: &impl ObjectId) -> BackendResult<girt::ObjectId> {
        let format = self.object_format();
        girt::ObjectId::from_bytes(format, id.as_bytes()).map_err(|_| {
            BackendError::InvalidHashLength {
                expected: format.digest_len(),
                actual: id.as_bytes().len(),
                object_type: id.object_type(),
                hash: id.hex(),
            }
        })
    }

    /// Reads a raw object of the expected kind, refreshing the pack snapshot
    /// once if it's not found.
    fn read_git_object(
        &self,
        id: &impl ObjectId,
        expected: girt::ObjectKind,
    ) -> BackendResult<Vec<u8>> {
        let git_id = self.validate_git_object_id(id)?;
        let read = |objects: &girt::Objects| {
            objects
                .read(git_id, girt::ReadLimits::trusted())
                .map_err(|err| to_read_object_err(err, id))
        };
        let mut object = read(&self.objects.read().unwrap())?;
        if object.is_none() {
            let mut objects = self.objects.write().unwrap();
            objects
                .refresh(
                    girt::PackLimits::trusted(),
                    girt::AlternateLimits::default(),
                )
                .map_err(|err| to_read_object_err(err, id))?;
            object = read(&objects)?;
        }
        let Some(object) = object else {
            return Err(BackendError::ObjectNotFound {
                object_type: id.object_type(),
                hash: id.hex(),
                source: format!("An object with id {} could not be found", id.hex()).into(),
            });
        };
        if object.kind() != expected {
            return Err(to_read_object_err(
                format!(
                    "expected {} but found {}",
                    expected.as_str(),
                    object.kind().as_str()
                ),
                id,
            ));
        }
        Ok(object.into_data())
    }

    /// Returns the (pack-snapshot) object store for bulk operations.
    pub(crate) fn objects(&self) -> std::sync::RwLockReadGuard<'_, girt::Objects> {
        self.objects.read().unwrap()
    }

    /// Refreshes the pack snapshot, e.g. after a fetch installed new packs.
    pub(crate) fn refresh_objects(&self) -> Result<(), girt::ObjectReadError> {
        self.objects.write().unwrap().refresh(
            girt::PackLimits::trusted(),
            girt::AlternateLimits::default(),
        )
    }

    fn read_file_sync(&self, id: &FileId) -> BackendResult<Vec<u8>> {
        self.read_git_object(id, girt::ObjectKind::Blob)
    }

    fn read_tree_for_commit(&self, id: &CommitId) -> BackendResult<girt::ObjectId> {
        let tree = self.read_commit(id).block_on()?.root_tree;
        // TODO(kfm): probably want to do something here if it is a merge
        let tree_id = tree.first().clone();
        self.validate_git_object_id(&tree_id)
    }

    fn write_blob(&self, bytes: &[u8], object_type: &'static str) -> BackendResult<girt::ObjectId> {
        self.repo
            .loose_objects()
            .write_blob(bytes)
            .map_err(|err| BackendError::WriteObject {
                object_type,
                source: Box::new(err),
            })
    }

    fn write_tree_entries(&self, entries: Vec<girt::TreeEntry>) -> BackendResult<girt::ObjectId> {
        let to_err = |err: Box<dyn std::error::Error + Send + Sync>| BackendError::WriteObject {
            object_type: "tree",
            source: err,
        };
        let tree =
            girt::Tree::new(self.object_format(), entries).map_err(|err| to_err(err.into()))?;
        self.repo
            .loose_objects()
            .write_tree(&tree)
            .map_err(|err| to_err(err.into()))
    }

    fn edit_references(&self, edits: &[girt::refs::RefEdit]) -> BackendResult<()> {
        if edits.is_empty() {
            return Ok(());
        }
        let refs = self
            .repo
            .references()
            .map_err(|err| BackendError::Other(err.into()))?;
        refs.transaction(edits)
            .map_err(|err| BackendError::Other(err.into()))?;
        Ok(())
    }

    /// Returns `RefEdit` that will create a ref in `refs/jj/keep` if not exist.
    /// Used for preventing GC of commits we create.
    fn to_no_gc_ref_update(&self, id: &CommitId) -> girt::refs::RefEdit {
        let name = girt::refs::RefName::new(format!("{NO_GC_REF_NAMESPACE}{id}")).unwrap();
        let target = girt::refs::Target::Direct(to_git_object_id(self.object_format(), id));
        girt::refs::RefEdit {
            name,
            dereference: false,
            target: Some(target.clone()),
            expected: girt::refs::Expected::AbsentOr(target),
            reflog: girt::refs::Reflog::Preserve,
        }
    }

    /// Write a tree conflict as a special tree with `.jjconflict-base-N` and
    /// `.jjconflict-side-N` subtrees. This ensure that the parts are not GC'd.
    /// Also includes a `JJ-CONFLICT-README` file explaining why these trees are
    /// present. The rest of the tree is copied from the first term of the
    /// conflict, which prevents editors with Git support from highlighting all
    /// files as new.
    fn write_tree_conflict(&self, conflict: &Merge<TreeId>) -> BackendResult<girt::ObjectId> {
        let format = self.object_format();
        let mut entries = itertools::chain(
            conflict
                .removes()
                .enumerate()
                .map(|(i, tree_id)| (format!(".jjconflict-base-{i}"), tree_id)),
            conflict
                .adds()
                .enumerate()
                .map(|(i, tree_id)| (format!(".jjconflict-side-{i}"), tree_id)),
        )
        .map(|(name, tree_id)| girt::TreeEntry {
            mode: girt::EntryMode::Tree,
            name: name.into_bytes(),
            id: to_git_object_id(format, tree_id),
        })
        .collect_vec();
        let readme_id = self
            .write_blob(CONFLICT_README.as_bytes(), "file")
            .map_err(|err| {
                BackendError::Other(
                    format!("Failed to write README for conflict tree: {err}").into(),
                )
            })?;
        entries.push(girt::TreeEntry {
            mode: girt::EntryMode::Blob,
            name: JJ_CONFLICT_README_FILE_NAME.into(),
            id: readme_id,
        });
        let first_tree_id = conflict.first();
        if *first_tree_id != self.empty_tree_id {
            let data = self.read_git_object(first_tree_id, girt::ObjectKind::Tree)?;
            let first_tree = girt::Tree::parse(format, &data)
                .map_err(|err| to_read_object_err(err, first_tree_id))?;
            for entry in first_tree.entries() {
                if !entry.name.starts_with(b".jjconflict")
                    && entry.name != JJ_CONFLICT_README_FILE_NAME.as_bytes()
                {
                    entries.push(entry.clone());
                }
            }
        }
        self.write_tree_entries(entries)
    }
}

const CONFLICT_README: &str = r#"This commit was made by jj, https://jj-vcs.dev/.
The commit contains file conflicts, and therefore looks wrong when used with
plain Git or other tools that are unfamiliar with jj.

The .jjconflict-* directories represent the different inputs to the conflict.
For details, see
https://docs.jj-vcs.dev/latest/git-compatibility/#format-mapping-details

If you see this file in your working copy, it probably means that you used a
regular `git` command to check out a conflicted commit. Use `jj abandon` to
recover.
"#;

/// Canonicalizes the given `path` except for the last `".git"` component.
///
/// The last path component matters when opening a Git repo without `core.bare`
/// config. This config is usually set, but the "repo" tool will set up such
/// repositories and symlinks. Opening such repo with fully-canonicalized path
/// would turn a colocated Git repo into a bare repo.
pub fn canonicalize_git_repo_path(path: &Path) -> io::Result<PathBuf> {
    if path.ends_with(".git") {
        let workdir = path.parent().unwrap();
        dunce::canonicalize(workdir).map(|dir| dir.join(".git"))
    } else {
        dunce::canonicalize(path)
    }
}

/// Extra (non-standard) headers of a Git commit, in order, with folded
/// continuation lines unfolded.
struct CommitHeaders<'a> {
    payload: girt::CommitPayload<'a>,
}

impl<'a> CommitHeaders<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            payload: girt::CommitPayload::from_bytes(data),
        }
    }

    /// Returns the first header value with the given name.
    fn find(&self, name: &str) -> Option<Vec<u8>> {
        self.payload
            .headers()
            .find(|header| header.name == name.as_bytes())
            .map(|header| header.unfolded_value())
    }

    /// Returns the index of the first header with the given name.
    fn position(&self, name: &str) -> Option<usize> {
        self.payload
            .headers()
            .position(|header| header.name == name.as_bytes())
    }
}

/// Parses the `jj:conflict-labels` header value if present.
fn extract_conflict_labels_from_commit(headers: &CommitHeaders) -> Merge<String> {
    let Some(mut value) = headers.find(JJ_CONFLICT_LABELS_COMMIT_HEADER) else {
        return Merge::resolved(String::new());
    };
    // As in Git, each line of the value ends in a newline.
    value.push(b'\n');

    str::from_utf8(&value)
        .expect("labels should be valid utf8")
        .split_terminator('\n')
        .map(str::to_owned)
        .collect::<MergeBuilder<_>>()
        .build()
}

/// Parses the `jj:trees` header value if present, otherwise returns the
/// resolved tree ID from Git.
fn extract_root_tree_from_commit(
    commit: &girt::Commit,
    headers: &CommitHeaders,
) -> Result<Merge<TreeId>, ()> {
    let Some(value) = headers.find(JJ_TREES_COMMIT_HEADER) else {
        let tree_id = TreeId::from_bytes(commit.tree().as_bytes());
        return Ok(Merge::resolved(tree_id));
    };

    let hash_len = commit.object_format().digest_len();
    let mut tree_ids = SmallVec::new();
    for hex in value.split(|b| *b == b' ') {
        let tree_id = TreeId::try_from_hex(hex).ok_or(())?;
        if tree_id.as_bytes().len() != hash_len {
            return Err(());
        }
        tree_ids.push(tree_id);
    }
    // It is invalid to use `jj:trees` with a non-conflicted tree. If this were
    // allowed, it would be possible to construct a commit which appears to have
    // different contents depending on whether it is viewed using `jj` or `git`.
    if tree_ids.len() == 1 || tree_ids.len() % 2 == 0 {
        return Err(());
    }
    Ok(Merge::from_vec(tree_ids))
}

/// Builds a commit header the way Git writes one: a value's final newline
/// terminates its last line rather than adding an empty continuation line.
fn git_header(name: &str, mut value: Vec<u8>) -> girt::CommitHeader {
    if value.ends_with(b"\n") {
        value.pop();
    }
    girt::CommitHeader {
        name: name.into(),
        value,
    }
}

/// Name of the commit header that carries a signature over the rest of the
/// commit, for the given object format.
fn signature_field_name(format: girt::ObjectFormat) -> &'static str {
    match format {
        girt::ObjectFormat::Sha1 => "gpgsig",
        girt::ObjectFormat::Sha256 => "gpgsig-sha256",
    }
}

fn commit_from_git_without_root_parent(
    id: &CommitId,
    format: girt::ObjectFormat,
    data: &[u8],
    is_shallow: bool,
) -> BackendResult<Commit> {
    let commit = girt::Commit::parse(format, data).map_err(|err| to_read_object_err(err, id))?;
    let headers = CommitHeaders::new(data);

    // If the git header has a change-id field, we attempt to convert that to a
    // valid JJ Change Id
    let change_id = extract_change_id_from_headers(&headers)
        .unwrap_or_else(|| synthetic_change_id_from_git_commit_id(id));

    // shallow commits don't have parents their parents actually fetched, so we
    // discard them here
    // TODO: This causes issues when a shallow repository is deepened/unshallowed
    let parents = if is_shallow {
        vec![]
    } else {
        commit
            .parents()
            .iter()
            .map(|oid| CommitId::from_bytes(oid.as_bytes()))
            .collect_vec()
    };
    // If the commit is a conflict, the conflict labels are stored in a commit
    // header separately from the trees.
    let conflict_labels = extract_conflict_labels_from_commit(&headers);
    // Conflicted commits written before we started using the `jj:trees` header
    // (~March 2024) may have the root trees stored in the extra metadata table
    // instead. For such commits, we'll update the root tree later when we read the
    // extra metadata.
    let root_tree = extract_root_tree_from_commit(&commit, &headers)
        .map_err(|()| to_read_object_err("Invalid jj:trees header", id))?;
    // Use lossy conversion as commit message with "mojibake" is still better than
    // nothing.
    // TODO: what should we do with commit.encoding?
    let description = String::from_utf8_lossy(commit.message()).into_owned();
    let author = commit
        .author()
        .map_err(|err| to_read_object_err(err, id))?
        .ok_or_else(|| to_read_object_err("missing author", id))?;
    let committer = commit
        .committer()
        .map_err(|err| to_read_object_err(err, id))?
        .ok_or_else(|| to_read_object_err("missing committer", id))?;
    let author = signature_from_git(author);
    let committer = signature_from_git(committer);

    // If the commit is signed, extract both the signature and the signed data
    // (which is the commit buffer with the signature header omitted).
    let secure_sig = headers
        .position(signature_field_name(format))
        .map(|index| {
            let header = headers.payload.headers().nth(index).unwrap();
            let data = headers
                .payload
                .without_headers(&[index])
                .ok_or_else(|| to_read_object_err("malformed signature header", id))?;
            // As in Git, the value's lines each end in a newline.
            let mut sig = header.unfolded_value();
            sig.push(b'\n');
            Ok::<_, BackendError>(SecureSig { data, sig })
        })
        .transpose()?;

    Ok(Commit {
        parents,
        predecessors: vec![],
        // If this commit has associated extra metadata, we may reset this later.
        root_tree,
        conflict_labels,
        change_id,
        description,
        author,
        committer,
        secure_sig,
    })
}

fn extract_change_id_from_headers(headers: &CommitHeaders) -> Option<ChangeId> {
    headers
        .find(CHANGE_ID_COMMIT_HEADER)
        .and_then(ChangeId::try_from_reverse_hex)
        .filter(|val| val.as_bytes().len() == CHANGE_ID_LENGTH)
}

/// Extracts change id from the headers of a raw Git commit object.
pub fn extract_change_id_from_commit(commit_data: &[u8]) -> Option<ChangeId> {
    extract_change_id_from_headers(&CommitHeaders::new(commit_data))
}

/// Deterministically creates a change id based on the commit id
///
/// Used when we get a commit without a change id. The exact algorithm for the
/// computation should not be relied upon.
pub fn synthetic_change_id_from_git_commit_id(id: &CommitId) -> ChangeId {
    // We reverse the bits of the commit id to create the change id. We don't
    // want to use the first bytes unmodified because then it would be ambiguous
    // if a given hash prefix refers to the commit id or the change id. It would
    // have been enough to pick the last 16 bytes instead of the leading 16
    // bytes to address that. We also reverse the bits to make it less likely
    // that users depend on any relationship between the two ids.
    let bytes = id.as_bytes()[id.as_bytes().len() - CHANGE_ID_LENGTH..]
        .iter()
        .rev()
        .map(|b| b.reverse_bits())
        .collect();
    ChangeId::new(bytes)
}

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

fn signature_from_git(signature: girt::IdentityRef) -> Signature {
    let name = signature.name;
    let name = if name != EMPTY_STRING_PLACEHOLDER.as_bytes() {
        String::from_utf8_lossy(name).into_owned()
    } else {
        "".to_string()
    };
    let email = signature.email;
    let email = if email != EMPTY_STRING_PLACEHOLDER.as_bytes() {
        String::from_utf8_lossy(email).into_owned()
    } else {
        "".to_string()
    };
    let time = signature.date().unwrap_or(girt::IdentityDate {
        seconds: 0,
        offset_minutes: 0,
    });
    let timestamp = MillisSinceEpoch(time.seconds.saturating_mul(1000));
    Signature {
        name,
        email,
        timestamp: Timestamp {
            timestamp,
            tz_offset: time.offset_minutes.into(),
        },
    }
}

/// Removes bytes that cannot be represented in a Git identity. Git itself
/// drops angle brackets and newlines, and trims surrounding whitespace.
fn sanitize_identity(value: &str) -> Vec<u8> {
    let value: Vec<u8> = value
        .bytes()
        .filter(|b| !matches!(b, b'<' | b'>' | b'\n' | b'\r' | 0))
        .collect();
    let value = value.trim_ascii().to_vec();
    if value.is_empty() {
        EMPTY_STRING_PLACEHOLDER.into()
    } else {
        value
    }
}

fn signature_to_git(signature: &Signature) -> girt::Signature {
    // git does not support empty names or emails
    let name = if !signature.name.is_empty() {
        &signature.name
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    let email = if !signature.email.is_empty() {
        &signature.email
    } else {
        EMPTY_STRING_PLACEHOLDER
    };
    girt::Signature {
        name: sanitize_identity(name),
        email: sanitize_identity(email),
        seconds: signature.timestamp.timestamp.0.div_euclid(1000),
        offset_minutes: signature.timestamp.tz_offset.clamp(-1439, 1439) as i16,
    }
}

fn serialize_extras(commit: &Commit) -> Vec<u8> {
    let mut proto = crate::protos::git_store::Commit {
        change_id: commit.change_id.to_bytes(),
        ..Default::default()
    };
    proto.uses_tree_conflict_format = true;
    for predecessor in &commit.predecessors {
        proto.predecessors.push(predecessor.to_bytes());
    }
    proto.encode_to_vec()
}

fn deserialize_extras(commit: &mut Commit, bytes: &[u8]) {
    let proto = crate::protos::git_store::Commit::decode(bytes).unwrap();
    if !proto.change_id.is_empty() {
        commit.change_id = ChangeId::new(proto.change_id);
    }
    if commit.root_tree.is_resolved()
        && proto.uses_tree_conflict_format
        && !proto.root_tree.is_empty()
    {
        let merge_builder: MergeBuilder<_> = proto
            .root_tree
            .iter()
            .map(|id_bytes| TreeId::from_bytes(id_bytes))
            .collect();
        commit.root_tree = merge_builder.build();
    }
    for predecessor in &proto.predecessors {
        commit.predecessors.push(CommitId::from_bytes(predecessor));
    }
}

fn to_read_object_err(
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    id: &impl ObjectId,
) -> BackendError {
    BackendError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source: err.into(),
    }
}

fn to_invalid_utf8_err(source: Utf8Error, id: &impl ObjectId) -> BackendError {
    BackendError::InvalidUtf8 {
        object_type: id.object_type(),
        hash: id.hex(),
        source,
    }
}

impl Debug for GitBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("GitBackend")
            .field("path", &self.git_repo_path())
            .finish()
    }
}

#[async_trait]
impl Backend for GitBackend {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn commit_id_length(&self) -> usize {
        self.object_format().digest_len()
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
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
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let data = self.read_file_sync(id)?;
        Ok(Box::pin(Cursor::new(data)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut bytes = Vec::new();
        contents.read_to_end(&mut bytes).await.unwrap();

        let oid = self.write_blob(&bytes, "file")?;
        Ok(FileId::new(oid.as_bytes().to_vec()))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let data = self.read_git_object(id, girt::ObjectKind::Blob)?;
        let target =
            String::from_utf8(data).map_err(|err| to_invalid_utf8_err(err.utf8_error(), id))?;
        Ok(target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let oid = self.write_blob(target.as_bytes(), "symlink")?;
        Ok(SymlinkId::new(oid.as_bytes().to_vec()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "The Git backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if id == &self.empty_tree_id {
            return Ok(Tree::default());
        }

        let data = self.read_git_object(id, girt::ObjectKind::Tree)?;
        let git_tree = girt::Tree::parse(self.object_format(), &data)
            .map_err(|err| to_read_object_err(err, id))?;
        let mut entries: Vec<_> = git_tree
            .entries()
            .iter()
            .map(|entry| -> BackendResult<_> {
                let name = RepoPathComponentBuf::new(
                    str::from_utf8(&entry.name).map_err(|err| to_invalid_utf8_err(err, id))?,
                )
                .unwrap();
                let oid = entry.id.as_bytes();
                let value = match entry.mode {
                    girt::EntryMode::Tree => TreeValue::Tree(TreeId::from_bytes(oid)),
                    girt::EntryMode::Blob => TreeValue::File {
                        id: FileId::from_bytes(oid),
                        executable: false,
                        copy_id: CopyId::placeholder(),
                    },
                    girt::EntryMode::Executable => TreeValue::File {
                        id: FileId::from_bytes(oid),
                        executable: true,
                        copy_id: CopyId::placeholder(),
                    },
                    girt::EntryMode::Symlink => TreeValue::Symlink(SymlinkId::from_bytes(oid)),
                    girt::EntryMode::Gitlink => TreeValue::GitSubmodule(CommitId::from_bytes(oid)),
                };
                Ok((name, value))
            })
            .try_collect()?;
        // While Git tree entries are sorted, the rule is slightly different.
        // Directory names are sorted as if they had trailing "/".
        if !entries.is_sorted_by_key(|(name, _)| name) {
            entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        }
        Ok(Tree::from_sorted_entries(entries))
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let format = self.object_format();
        let entries = contents
            .entries()
            .map(|entry| {
                let name = entry.name().as_internal_str().as_bytes().to_vec();
                let (mode, id) = match entry.value() {
                    TreeValue::File {
                        id,
                        executable,
                        copy_id: _, // TODO: Use the value
                    } => {
                        let mode = if *executable {
                            girt::EntryMode::Executable
                        } else {
                            girt::EntryMode::Blob
                        };
                        (mode, id.as_bytes())
                    }
                    TreeValue::Symlink(id) => (girt::EntryMode::Symlink, id.as_bytes()),
                    TreeValue::Tree(id) => (girt::EntryMode::Tree, id.as_bytes()),
                    TreeValue::GitSubmodule(id) => (girt::EntryMode::Gitlink, id.as_bytes()),
                };
                girt::TreeEntry {
                    mode,
                    name,
                    id: girt::ObjectId::from_bytes(format, id).expect("valid object id"),
                }
            })
            .collect();
        let oid = self.write_tree_entries(entries)?;
        Ok(TreeId::from_bytes(oid.as_bytes()))
    }

    #[tracing::instrument(skip(self))]
    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let mut commit = {
            let data = self.read_git_object(id, girt::ObjectKind::Commit)?;
            let is_shallow = self.shallow_root_ids().contains(id);
            commit_from_git_without_root_parent(id, self.object_format(), &data, is_shallow)?
        };
        if commit.parents.is_empty() {
            commit.parents.push(self.root_commit_id.clone());
        }

        let table = self.cached_extra_metadata_table()?;
        if let Some(extras) = table.get_value(id.as_bytes()) {
            deserialize_extras(&mut commit, extras);
        } else {
            // TODO: Remove this hack and map to ObjectNotFound error if we're sure that
            // there are no reachable ancestor commits without extras metadata. Git commits
            // imported by jj < 0.8.0 might not have extras (#924).
            // https://github.com/jj-vcs/jj/issues/2343
            tracing::info!("unimported Git commit found");
            self.import_head_commits([id])?;
            let table = self.cached_extra_metadata_table()?;
            let extras = table.get_value(id.as_bytes()).unwrap();
            deserialize_extras(&mut commit, extras);
        }
        Ok(commit)
    }

    async fn write_commit(
        &self,
        mut contents: Commit,
        mut sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        assert!(contents.secure_sig.is_none(), "commit.secure_sig was set");

        let format = self.object_format();
        let tree_ids = &contents.root_tree;
        let git_tree_id = match tree_ids.as_resolved() {
            Some(tree_id) => self.validate_git_object_id(tree_id)?,
            None => self.write_tree_conflict(tree_ids)?,
        };
        let author = signature_to_git(&contents.author);
        let mut committer = signature_to_git(&contents.committer);
        let message = &contents.description;
        if contents.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }
        let mut parents = vec![];
        for parent_id in &contents.parents {
            if *parent_id == self.root_commit_id {
                // Git doesn't have a root commit, so if the parent is the root commit, we don't
                // add it to the list of parents to write in the Git commit. We also check that
                // there are no other parents since Git cannot represent a merge between a root
                // commit and another commit.
                if contents.parents.len() > 1 {
                    return Err(BackendError::Unsupported(
                        "The Git backend does not support creating merge commits with the root \
                         commit as one of the parents."
                            .to_owned(),
                    ));
                }
            } else {
                parents.push(self.validate_git_object_id(parent_id)?);
            }
        }
        let mut extra_headers: Vec<girt::CommitHeader> = vec![];
        if !contents.conflict_labels.is_resolved() {
            // Labels cannot contain '\n' since we use it as a separator in the header.
            assert!(
                contents
                    .conflict_labels
                    .iter()
                    .all(|label| !label.contains('\n'))
            );
            let mut joined_with_newlines = contents.conflict_labels.iter().join("\n");
            joined_with_newlines.push('\n');
            extra_headers.push(git_header(
                JJ_CONFLICT_LABELS_COMMIT_HEADER,
                joined_with_newlines.into(),
            ));
        }
        if !tree_ids.is_resolved() {
            let value = tree_ids.iter().map(|id| id.hex()).join(" ");
            extra_headers.push(girt::CommitHeader {
                name: JJ_TREES_COMMIT_HEADER.into(),
                value: value.into(),
            });
        }
        if self.write_change_id_header {
            extra_headers.push(girt::CommitHeader {
                name: CHANGE_ID_COMMIT_HEADER.into(),
                value: contents.change_id.reverse_hex().into(),
            });
        }

        let loose = self.repo.loose_objects();
        if tree_ids.iter().any(|id| id == &self.empty_tree_id) {
            let tree = girt::Tree::new(format, vec![]).unwrap();
            loose
                .write_tree(&tree)
                .map_err(|err| BackendError::WriteObject {
                    object_type: "tree",
                    source: Box::new(err),
                })?;
        }

        let extras = serialize_extras(&contents);
        let to_write_err =
            |err: Box<dyn std::error::Error + Send + Sync>| BackendError::WriteObject {
                object_type: "commit",
                source: err,
            };

        // If two writers write commits of the same id with different metadata, they
        // will both succeed and the metadata entries will be "merged" later. Since
        // metadata entry is keyed by the commit id, one of the entries would be lost.
        // To prevent such race condition locally, we extend the scope covered by the
        // table lock. This is still racy if multiple machines are involved and the
        // repository is rsync-ed.
        let (table, table_lock) = self.read_extra_metadata_table_locked()?;
        let id = loop {
            let mut fields = girt::CommitFields {
                tree: git_tree_id,
                parents: parents.clone(),
                author: author.clone(),
                committer: committer.clone(),
                extra_headers: extra_headers.clone(),
                message: message.as_bytes().to_vec(),
            };

            if let Some(sign) = &mut sign_with {
                let unsigned =
                    girt::Commit::new(fields.clone()).map_err(|err| to_write_err(err.into()))?;
                let data = unsigned.as_bytes().to_vec();
                let sig = sign(&data).map_err(|err| to_write_err(err.into()))?;
                fields
                    .extra_headers
                    .push(git_header(signature_field_name(format), sig.clone()));
                contents.secure_sig = Some(SecureSig { data, sig });
            }

            let commit = girt::Commit::new(fields).map_err(|err| to_write_err(err.into()))?;
            let git_id = loose
                .write_commit(&commit)
                .map_err(|err| to_write_err(err.into()))?;

            match table.get_value(git_id.as_bytes()) {
                Some(existing_extras) if existing_extras != extras => {
                    // It's possible a commit already exists with the same
                    // commit id but different change id. Adjust the timestamp
                    // until this is no longer the case.
                    //
                    // For example, this can happen when rebasing duplicate
                    // commits, https://github.com/jj-vcs/jj/issues/694.
                    //
                    // `jj` resets the committer timestamp to the current
                    // timestamp whenever it rewrites a commit. So, it's
                    // unlikely for the timestamp to be 0 even if the original
                    // commit had its timestamp set to 0. Moreover, we test that
                    // a commit with a negative timestamp can still be written
                    // and read back by `jj`.
                    committer.seconds -= 1;
                }
                _ => break CommitId::from_bytes(git_id.as_bytes()),
            }
        };

        // Everything up to this point had no permanent effect on the repo except
        // GC-able objects
        self.edit_references(&[self.to_no_gc_ref_update(&id)])?;

        // Update the signature to match the one that was actually written to the object
        // store
        contents.committer.timestamp.timestamp = MillisSinceEpoch(committer.seconds * 1000);
        let mut mut_table = table.start_mutation();
        mut_table.add_entry(id.to_bytes(), extras);
        self.save_extra_metadata_table(mut_table, &table_lock)?;
        Ok((id, contents))
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root_id: &CommitId,
        head_id: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        let root_tree = self.read_tree_for_commit(root_id)?;
        let head_tree = self.read_tree_for_commit(head_id)?;
        let targets: Option<Vec<&[u8]>> = paths.map(|paths| {
            paths
                .iter()
                .map(|path| path.as_internal_file_string().as_bytes())
                .collect()
        });
        let options = girt::rewrites::Options {
            similarity: 50,
            copies: girt::rewrites::Copies::ModifiedPostimage,
            track_empty: false,
            candidate_limit: 1000,
            // Binary files are only matched exactly.
            approximate_binary: false,
        };
        let rewrites = self
            .objects()
            .detect_rewrites(
                Some(root_tree),
                Some(head_tree),
                options,
                girt::rewrites::Limits::default(),
                targets.as_deref(),
                &AtomicBool::new(false),
            )
            .map_err(|err| BackendError::Other(err.into()))?;

        let records = rewrites
            .into_iter()
            .map(|rewrite| -> BackendResult<Option<CopyRecord>> {
                let source = str::from_utf8(&rewrite.source)
                    .map_err(|err| to_invalid_utf8_err(err, root_id))?;
                let dest = str::from_utf8(&rewrite.target)
                    .map_err(|err| to_invalid_utf8_err(err, head_id))?;
                let target = RepoPathBuf::from_internal_string(dest).unwrap();
                if !paths.is_none_or(|paths| paths.contains(&target)) {
                    return Ok(None);
                }
                Ok(Some(CopyRecord {
                    target,
                    target_commit: head_id.clone(),
                    source: RepoPathBuf::from_internal_string(source).unwrap(),
                    source_file: FileId::from_bytes(rewrite.source_id.as_bytes()),
                    source_commit: root_id.clone(),
                }))
            })
            .filter_map(Result::transpose)
            .collect_vec();
        Ok(futures::stream::iter(records).boxed())
    }
}

impl GitBackend {
    /// Recreates `refs/jj/keep` refs for the `new_heads`, and removes the other
    /// unreachable and non-head refs.
    fn recreate_no_gc_refs(
        &self,
        new_heads: impl IntoIterator<Item = CommitId>,
        keep_newer: SystemTime,
    ) -> BackendResult<()> {
        // Calculate diff between existing no-gc refs and new heads.
        let new_heads: HashSet<CommitId> = new_heads.into_iter().collect();
        let mut no_gc_refs_to_keep_count: usize = 0;
        let mut ref_edits = Vec::new();
        let refs = self
            .repo
            .references()
            .map_err(|err| BackendError::Other(err.into()))?;
        let namespace =
            girt::refs::RefName::new(NO_GC_REF_NAMESPACE.trim_end_matches('/')).unwrap();
        let no_gc_refs = refs
            .list_namespace(&namespace)
            .map_err(|err| BackendError::Other(err.into()))?;
        for git_ref in no_gc_refs {
            let name_bytes = git_ref.name.as_bytes();
            let name = String::from_utf8_lossy(name_bytes);
            let girt::refs::Target::Direct(oid) = &git_ref.target else {
                return Err(BackendError::Other(
                    format!("Symbolic no-gc ref found: {name}").into(),
                ));
            };
            let id = CommitId::from_bytes(oid.as_bytes());
            let name_good = name_bytes[NO_GC_REF_NAMESPACE.len()..] == *id.hex().as_bytes();
            if new_heads.contains(&id) && name_good {
                no_gc_refs_to_keep_count += 1;
                continue;
            }
            // Check timestamp of loose ref, but this is still racy on re-import
            // because:
            // - existing packed ref won't be demoted to loose ref
            // - existing loose ref won't be touched
            //
            // TODO: might be better to switch to a dummy merge, where new no-gc ref
            // will always have a unique name. Doing that with the current
            // ref-per-head strategy would increase the number of the no-gc refs.
            // https://github.com/jj-vcs/jj/pull/2659#issuecomment-1837057782
            let loose_ref_path = self.repo.common_dir().join(&*name);
            if let Ok(metadata) = loose_ref_path.metadata() {
                let mtime = metadata.modified().expect("unsupported platform?");
                if mtime > keep_newer {
                    tracing::trace!(%name, "not deleting new");
                    no_gc_refs_to_keep_count += 1;
                    continue;
                }
            }
            // Also deletes no-gc ref of random name created by old jj.
            tracing::trace!(%name, ?name_good, "will delete");
            ref_edits.push(girt::refs::RefEdit {
                name: git_ref.name.clone(),
                dereference: false,
                target: None,
                expected: girt::refs::Expected::Value(git_ref.target.clone()),
                reflog: girt::refs::Reflog::Delete,
            });
        }
        tracing::info!(
            new_heads_count = new_heads.len(),
            no_gc_refs_to_keep_count,
            no_gc_refs_to_delete_count = ref_edits.len(),
            "collected reachable refs"
        );
        ref_edits.extend(new_heads.iter().map(|id| self.to_no_gc_ref_update(id)));
        self.edit_references(&ref_edits)
    }

    /// Perform garbage collection.
    ///
    /// All commits found in the `index` won't be removed. In addition to that,
    /// objects created after `keep_newer` will be preserved. This mitigates a
    /// risk of deleting new commits created concurrently by another process.
    #[tracing::instrument(skip(self, index))]
    pub fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        let new_heads = index
            .all_heads_for_gc()
            .map_err(|err| BackendError::Other(err.into()))?
            .filter(|id| *id != self.root_commit_id)
            .collect_vec();
        self.recreate_no_gc_refs(new_heads.iter().cloned(), keep_newer)?;

        // No locking is needed since we aren't going to add new "commits".
        let table = self.cached_extra_metadata_table()?;
        // TODO: remove unreachable entries from extras table if segment file
        // mtime <= keep_newer? (it won't be consistent with no-gc refs
        // preserved by the keep_newer timestamp though)
        self.extra_metadata_store
            .gc(&table, keep_newer)
            .map_err(|err| BackendError::Other(err.into()))?;

        crate::git_maintenance::collect_garbage(&self.git_repo(), keep_newer)
            .map_err(|err| BackendError::Other(err.into()))?;
        self.refresh_objects()
            .map_err(|err| BackendError::Other(err.into()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::process::Command;

    use gix::date::parse::TimeBuf;
    use indoc::indoc;
    use test_case::test_case;

    use super::*;
    use crate::config::StackedConfig;
    use crate::content_hash::blake2b_hash;
    use crate::hex_util;
    use crate::tests::TestResult;
    use crate::tests::new_temp_dir;

    const GIT_USER: &str = "Someone";
    const GIT_EMAIL: &str = "someone@example.com";

    fn git_config() -> Vec<bstr::BString> {
        vec![
            format!("user.name = {GIT_USER}").into(),
            format!("user.email = {GIT_EMAIL}").into(),
            "init.defaultBranch = master".into(),
        ]
    }

    fn open_options() -> gix::open::Options {
        gix::open::Options::isolated()
            .config_overrides(git_config())
            .strict_config(true)
    }

    /// Opens the backend's repository with gix, an independent implementation.
    fn gix_repo(backend: &GitBackend) -> gix::Repository {
        gix::open_opts(backend.git_repo_path(), open_options()).unwrap()
    }

    fn to_format(kind: gix::hash::Kind) -> ObjectFormat {
        match kind {
            gix::hash::Kind::Sha1 => ObjectFormat::Sha1,
            _ => ObjectFormat::Sha256,
        }
    }

    fn git_init(directory: impl AsRef<Path>, object_hash: gix::hash::Kind) -> gix::Repository {
        gix::ThreadSafeRepository::init_opts(
            directory,
            gix::create::Kind::WithWorktree,
            gix::create::Options {
                object_hash: Some(object_hash),
                ..Default::default()
            },
            open_options(),
        )
        .unwrap()
        .to_thread_local()
    }

    #[test]
    fn open_git_repo_at_workdir() -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;

        let git_repo_path = temp_dir.path().join("git1");
        let git_repo = git_init(&git_repo_path, gix::hash::Kind::default());
        let other_git_repo_path = temp_dir.path().join("git2");
        let _other_git_repo = git_init(&other_git_repo_path, gix::hash::Kind::default());

        let worktree_dir = temp_dir.path().join("git1-wt");
        let output = Command::new("git")
            .args(["worktree", "add", "--orphan"])
            .arg(&worktree_dir)
            .current_dir(&git_repo_path)
            .output()?;
        assert!(output.status.success(), "{output:?}");

        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;

        assert_matches!(
            backend.open_git_repo_at_workdir(&git_repo_path),
            Ok(repo) if repo.worktree() == gix_repo(&backend).workdir()
        );
        let canonical_worktree = dunce::canonicalize(&worktree_dir).unwrap();
        assert_matches!(
            backend.open_git_repo_at_workdir(&worktree_dir),
            Ok(repo) if repo.worktree() == Some(canonical_worktree.as_path())
        );
        assert_matches!(
            backend.open_git_repo_at_workdir(&temp_dir.path().join("unknown")),
            Err(GitRepoAtWorkdirError::NotFound { .. })
        );
        assert_matches!(
            backend.open_git_repo_at_workdir(&other_git_repo_path),
            Err(GitRepoAtWorkdirError::Unrelated { .. })
        );

        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_plain_git_commit(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        // Add a commit with some files in
        let blob1 = git_repo.write_blob(b"content1")?.detach();
        let blob2 = git_repo.write_blob(b"normal")?.detach();
        let mut dir_tree_editor = git_repo.empty_tree().edit()?;
        dir_tree_editor.upsert("normal", gix::object::tree::EntryKind::Blob, blob1)?;
        dir_tree_editor.upsert("symlink", gix::object::tree::EntryKind::Link, blob2)?;
        let dir_tree_id = dir_tree_editor.write()?.detach();
        let mut root_tree_builder = git_repo.empty_tree().edit()?;
        root_tree_builder.upsert("dir", gix::object::tree::EntryKind::Tree, dir_tree_id)?;
        let root_tree_id = root_tree_builder.write()?.detach();
        let git_author = gix::actor::Signature {
            name: "git author".into(),
            email: "git.author@example.com".into(),
            time: gix::date::Time::new(1000, 60 * 60),
        };
        let git_committer = gix::actor::Signature {
            name: "git committer".into(),
            email: "git.committer@example.com".into(),
            time: gix::date::Time::new(2000, -480 * 60),
        };
        let git_commit_id = git_repo
            .commit_as(
                git_committer.to_ref(&mut TimeBuf::default()),
                git_author.to_ref(&mut TimeBuf::default()),
                "refs/heads/dummy",
                "git commit message",
                root_tree_id,
                [] as [gix::ObjectId; 0],
            )?
            .detach();
        git_repo.find_reference("refs/heads/dummy")?.delete()?;
        // The change id is the leading reverse bits of the commit id
        let (commit_id, change_id) = match object_hash {
            gix::hash::Kind::Sha1 => (
                CommitId::from_hex("efdcea5ca4b3658149f899ca7feee6876d077263"),
                ChangeId::from_hex("c64ee0b6e16777fe53991f9281a6cd25"),
            ),
            gix::hash::Kind::Sha256 => (
                CommitId::from_hex(
                    "64366022e4938d697015b775945be93aea6d3fc221feeaf7c516420262e3fa54",
                ),
                ChangeId::from_hex("2a5fc746404268a3ef577f8443fcb657"),
            ),
            _ => unreachable!(),
        };
        // Check that the git commit above got the hash we expect
        assert_eq!(
            git_commit_id.as_bytes(),
            commit_id.as_bytes(),
            "{git_commit_id:?} vs {commit_id:?}"
        );

        // Add an empty commit on top
        let git_commit_id2 = git_repo
            .commit_as(
                git_committer.to_ref(&mut TimeBuf::default()),
                git_author.to_ref(&mut TimeBuf::default()),
                "refs/heads/dummy2",
                "git commit message 2",
                root_tree_id,
                [git_commit_id],
            )?
            .detach();
        git_repo.find_reference("refs/heads/dummy2")?.delete()?;
        let commit_id2 = CommitId::from_bytes(git_commit_id2.as_bytes());

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        // Import the head commit and its ancestors
        backend.import_head_commits([&commit_id2])?;
        // Ref should be created only for the head commit
        let git_refs = gix_repo(&backend)
            .references()?
            .prefixed("refs/jj/keep/")?
            .map(|git_ref| git_ref.unwrap().id().detach())
            .collect_vec();
        assert_eq!(git_refs, vec![git_commit_id2]);

        let commit = backend.read_commit(&commit_id).block_on()?;
        assert_eq!(&commit.change_id, &change_id);
        assert_eq!(
            commit.parents,
            vec![CommitId::from_bytes(object_hash.null_ref().as_bytes())]
        );
        assert_eq!(commit.predecessors, vec![]);
        assert_eq!(
            commit.root_tree,
            Merge::resolved(TreeId::from_bytes(root_tree_id.as_bytes()))
        );
        assert_eq!(commit.description, "git commit message");
        assert_eq!(commit.author.name, "git author");
        assert_eq!(commit.author.email, "git.author@example.com");
        assert_eq!(
            commit.author.timestamp.timestamp,
            MillisSinceEpoch(1000 * 1000)
        );
        assert_eq!(commit.author.timestamp.tz_offset, 60);
        assert_eq!(commit.committer.name, "git committer");
        assert_eq!(commit.committer.email, "git.committer@example.com");
        assert_eq!(
            commit.committer.timestamp.timestamp,
            MillisSinceEpoch(2000 * 1000)
        );
        assert_eq!(commit.committer.timestamp.tz_offset, -480);

        let root_tree = backend
            .read_tree(
                RepoPath::root(),
                &TreeId::from_bytes(root_tree_id.as_bytes()),
            )
            .block_on()?;
        let mut root_entries = root_tree.entries();
        let dir = root_entries.next().unwrap();
        assert_eq!(root_entries.next(), None);
        assert_eq!(dir.name().as_internal_str(), "dir");
        assert_eq!(
            dir.value(),
            &TreeValue::Tree(TreeId::from_bytes(dir_tree_id.as_bytes()))
        );

        let dir_tree = backend
            .read_tree(
                RepoPath::from_internal_string("dir")?,
                &TreeId::from_bytes(dir_tree_id.as_bytes()),
            )
            .block_on()?;
        let mut entries = dir_tree.entries();
        let file = entries.next().unwrap();
        let symlink = entries.next().unwrap();
        assert_eq!(entries.next(), None);
        assert_eq!(file.name().as_internal_str(), "normal");
        assert_eq!(
            file.value(),
            &TreeValue::File {
                id: FileId::from_bytes(blob1.as_bytes()),
                executable: false,
                copy_id: CopyId::placeholder(),
            }
        );
        assert_eq!(symlink.name().as_internal_str(), "symlink");
        assert_eq!(
            symlink.value(),
            &TreeValue::Symlink(SymlinkId::from_bytes(blob2.as_bytes()))
        );

        let commit2 = backend.read_commit(&commit_id2).block_on()?;
        assert_eq!(commit2.parents, vec![commit_id.clone()]);
        assert_eq!(commit.predecessors, vec![]);
        assert_eq!(
            commit.root_tree,
            Merge::resolved(TreeId::from_bytes(root_tree_id.as_bytes()))
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_git_commit_without_importing(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());
        let git_commit_id = git_repo.commit_as(
            signature.to_ref(&mut TimeBuf::default()),
            signature.to_ref(&mut TimeBuf::default()),
            "refs/heads/main",
            "git commit message",
            empty_tree_id,
            [] as [gix::ObjectId; 0],
        )?;

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        // read_commit() without import_head_commits() works as of now. This might be
        // changed later.
        assert!(
            backend
                .read_commit(&CommitId::from_bytes(git_commit_id.as_bytes()))
                .block_on()
                .is_ok()
        );
        assert!(
            backend
                .cached_extra_metadata_table()?
                .get_value(git_commit_id.as_bytes())
                .is_some(),
            "extra metadata should have been be created"
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn read_signed_git_commit(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());

        let secure_sig =
            "here are some ASCII bytes to be used as a test signature\n\ndefinitely not PGP\n";

        let mut commit = gix::objs::Commit {
            tree: empty_tree_id,
            parents: smallvec::SmallVec::new(),
            author: signature.clone(),
            committer: signature.clone(),
            encoding: None,
            message: "git commit message".into(),
            extra_headers: Vec::new(),
        };

        let mut commit_buf = Vec::new();
        gix::objs::WriteTo::write_to(&commit, &mut commit_buf)?;
        let commit_str = str::from_utf8(&commit_buf)?;

        let field = signature_field_name(to_format(object_hash));
        commit.extra_headers.push((field.into(), secure_sig.into()));

        let git_commit_id = git_repo.write_object(&commit)?;

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;

        let commit = backend
            .read_commit(&CommitId::from_bytes(git_commit_id.as_bytes()))
            .block_on()?;

        let sig = commit.secure_sig.expect("failed to read the signature");

        // converting to string for nicer assert diff
        assert_eq!(str::from_utf8(&sig.sig)?, secure_sig);
        assert_eq!(str::from_utf8(&sig.data)?, commit_str);
        Ok(())
    }

    #[test]
    fn change_id_parsing() {
        let id = |commit_object_bytes: &[u8]| extract_change_id_from_commit(commit_object_bytes);

        let commit_with_id = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            extra-header blah
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq

            test-commit
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit_with_id),
            @r#"Some(ChangeId("efbc06dc4721683f2a45568dbda31e99"))"#
        );

        let commit_without_id = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            extra-header blah

            no id in header
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit_without_id),
            @"None"
        );

        let commit = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq
            extra-header blah
            change-id abcabcabcabcabcabcabcabcabcabcab

            valid change id first
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit),
            @r#"Some(ChangeId("efbc06dc4721683f2a45568dbda31e99"))"#
        );

        // We only look at the first change id if multiple are present, so this should
        // error
        let commit = indoc! {b"
            tree 126799bf8058d1b5c531e93079f4fe79733920dd
            parent bd50783bdf38406dd6143475cd1a3c27938db2ee
            author JJ Fan <jjfan@example.com> 1757112665 -0700
            committer JJ Fan <jjfan@example.com> 1757359886 -0700
            change-id abcabcabcabcabcabcabcabcabcabcab
            extra-header blah
            change-id lkonztmnvsxytrwkxpvuutrmompwylqq

            valid change id first
        "};
        insta::assert_compact_debug_snapshot!(
            id(commit),
            @"None"
        );
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn round_trip_change_id_via_git_header(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();

        let store_path = temp_dir.path().join("store");
        fs::create_dir(&store_path)?;
        let empty_store_path = temp_dir.path().join("empty_store");
        fs::create_dir(&empty_store_path)?;
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, &store_path, git_repo.path())?;
        let original_change_id = ChangeId::from_hex("1111eeee1111eeee1111eeee1111eeee");
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: original_change_id.clone(),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let (initial_commit_id, _init_commit) = backend.write_commit(commit, None).block_on()?;
        let commit = backend.read_commit(&initial_commit_id).block_on()?;
        assert_eq!(
            commit.change_id, original_change_id,
            "The change-id header did not roundtrip"
        );

        // Because of how change ids are also persisted in extra proto files,
        // initialize a new store without those files, but reuse the same git
        // storage. This change-id must be derived from the git commit header.
        let no_extra_backend =
            GitBackend::init_external(&settings, &empty_store_path, git_repo.path())?;
        let no_extra_commit = no_extra_backend
            .read_commit(&initial_commit_id)
            .block_on()?;

        assert_eq!(
            no_extra_commit.change_id, original_change_id,
            "The change-id header did not roundtrip"
        );
        Ok(())
    }

    #[test]
    fn read_empty_string_placeholder() {
        let git_signature1 =
            format!("{EMPTY_STRING_PLACEHOLDER} <git.author@example.com> 1000 +0100");
        let signature1 =
            signature_from_git(girt::IdentityRef::parse(git_signature1.as_bytes()).unwrap());
        assert!(signature1.name.is_empty());
        assert_eq!(signature1.email, "git.author@example.com");
        let git_signature2 = format!("git committer <{EMPTY_STRING_PLACEHOLDER}> 2000 -0800");
        let signature2 =
            signature_from_git(girt::IdentityRef::parse(git_signature2.as_bytes()).unwrap());
        assert_eq!(signature2.name, "git committer");
        assert!(signature2.email.is_empty());
    }

    #[test]
    fn write_empty_string_placeholder() {
        let signature1 = Signature {
            name: "".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let git_signature1 = signature_to_git(&signature1);
        assert_eq!(git_signature1.name, EMPTY_STRING_PLACEHOLDER.as_bytes());
        assert_eq!(git_signature1.email, b"someone@example.com");
        let signature2 = Signature {
            name: "Someone".to_string(),
            email: "".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let git_signature2 = signature_to_git(&signature2);
        assert_eq!(git_signature2.name, b"Someone");
        assert_eq!(git_signature2.email, EMPTY_STRING_PLACEHOLDER.as_bytes());
    }

    /// Test that parents get written correctly
    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn git_commit_parents(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;
        let mut commit = Commit {
            parents: vec![],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        // No parents
        commit.parents = vec![];
        assert_matches!(
            write_commit(commit.clone()),
            Err(BackendError::Other(err)) if err.to_string().contains("no parents")
        );

        // Only root commit as parent
        commit.parents = vec![backend.root_commit_id().clone()];
        let first_id = write_commit(commit.clone())?.0;
        let first_commit = backend.read_commit(&first_id).block_on()?;
        assert_eq!(first_commit, commit);
        let first_git_commit = git_repo.find_commit(git_id(&first_id))?;
        assert!(first_git_commit.parent_ids().collect_vec().is_empty());

        // Only non-root commit as parent
        commit.parents = vec![first_id.clone()];
        let second_id = write_commit(commit.clone())?.0;
        let second_commit = backend.read_commit(&second_id).block_on()?;
        assert_eq!(second_commit, commit);
        let second_git_commit = git_repo.find_commit(git_id(&second_id))?;
        assert_eq!(
            second_git_commit.parent_ids().collect_vec(),
            vec![git_id(&first_id)]
        );

        // Merge commit
        commit.parents = vec![first_id.clone(), second_id.clone()];
        let merge_id = write_commit(commit.clone())?.0;
        let merge_commit = backend.read_commit(&merge_id).block_on()?;
        assert_eq!(merge_commit, commit);
        let merge_git_commit = git_repo.find_commit(git_id(&merge_id))?;
        assert_eq!(
            merge_git_commit.parent_ids().collect_vec(),
            vec![git_id(&first_id), git_id(&second_id)]
        );

        // Merge commit with root as one parent
        commit.parents = vec![first_id, backend.root_commit_id().clone()];
        assert_matches!(
            write_commit(commit),
            Err(BackendError::Unsupported(message)) if message.contains("root commit")
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn write_tree_conflicts(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(&git_repo_path, object_hash);

        let backend = GitBackend::init_external(&settings, store_path, git_repo.path())?;
        let create_tree = |i| {
            let blob_id = git_repo.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder = git_repo.empty_tree().edit().unwrap();
            tree_builder
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id,
                )
                .unwrap();
            TreeId::from_bytes(tree_builder.write().unwrap().as_bytes())
        };

        let root_tree = Merge::from_removes_adds(
            vec![create_tree(0), create_tree(1)],
            vec![create_tree(2), create_tree(3), create_tree(4)],
        );
        let mut commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: root_tree.clone(),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        // When writing a tree-level conflict, the root tree on the git side has the
        // individual trees as subtrees.
        let read_commit_id = write_commit(commit.clone())?.0;
        let read_commit = backend.read_commit(&read_commit_id).block_on()?;
        assert_eq!(read_commit, commit);
        let git_commit = git_repo.find_commit(gix::ObjectId::from_bytes_or_panic(
            read_commit_id.as_bytes(),
        ))?;
        let git_tree = git_repo.find_tree(git_commit.tree_id()?)?;
        let jj_conflict_entries = git_tree
            .iter()
            .map(Result::unwrap)
            .filter(|entry| {
                entry.filename().starts_with(b".jjconflict")
                    || entry.filename() == JJ_CONFLICT_README_FILE_NAME
            })
            .collect_vec();
        assert!(
            jj_conflict_entries
                .iter()
                .filter(|entry| entry.filename() != JJ_CONFLICT_README_FILE_NAME)
                .all(|entry| entry.mode().value() == 0o040000)
        );
        let mut iter = jj_conflict_entries.iter();
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-base-0");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_remove(0).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-base-1");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_remove(1).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-0");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(0).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-1");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(1).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b".jjconflict-side-2");
        assert_eq!(
            entry.id().as_bytes(),
            root_tree.get_add(2).unwrap().as_bytes()
        );
        let entry = iter.next().unwrap();
        assert_eq!(entry.filename(), b"JJ-CONFLICT-README");
        assert_eq!(entry.mode().value(), 0o100644);
        assert!(iter.next().is_none());

        // When writing a single tree using the new format, it's represented by a
        // regular git tree.
        commit.root_tree = Merge::resolved(create_tree(5));
        let read_commit_id = write_commit(commit.clone())?.0;
        let read_commit = backend.read_commit(&read_commit_id).block_on()?;
        assert_eq!(read_commit, commit);
        let git_commit = git_repo.find_commit(gix::ObjectId::from_bytes_or_panic(
            read_commit_id.as_bytes(),
        ))?;
        assert_eq!(
            Merge::resolved(TreeId::from_bytes(git_commit.tree_id()?.as_bytes())),
            commit.root_tree
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn commit_has_ref(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend =
            GitBackend::init_internal(&settings, temp_dir.path(), to_format(object_hash))?;
        let git_repo = gix_repo(&backend);
        let signature = Signature {
            name: "Someone".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::new(vec![42; 16]),
            description: "initial".to_string(),
            author: signature.clone(),
            committer: signature,
            secure_sig: None,
        };
        let commit_id = backend.write_commit(commit, None).block_on()?.0;
        let git_refs = git_repo.references()?;
        let git_ref_ids: Vec<_> = git_refs
            .prefixed("refs/jj/keep/")?
            .map(|x| x.unwrap().id().detach())
            .collect();
        assert!(git_ref_ids.iter().any(|id| *id == git_id(&commit_id)));

        // Concurrently-running GC deletes the ref, leaving the extra metadata.
        for git_ref in git_refs.prefixed("refs/jj/keep/")? {
            git_ref.unwrap().delete().unwrap();
        }
        // Re-imported commit should have new ref.
        backend.import_head_commits([&commit_id])?;
        let git_refs = git_repo.references()?;
        let git_ref_ids: Vec<_> = git_refs
            .prefixed("refs/jj/keep/")?
            .map(|x| x.unwrap().id().detach())
            .collect();
        assert!(git_ref_ids.iter().any(|id| *id == git_id(&commit_id)));
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn import_head_commits_duplicates(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend =
            GitBackend::init_internal(&settings, temp_dir.path(), to_format(object_hash))?;
        let git_repo = gix_repo(&backend);

        let signature = gix::actor::Signature {
            name: GIT_USER.into(),
            email: GIT_EMAIL.into(),
            time: gix::date::Time::now_utc(),
        };
        let empty_tree_id = gix::ObjectId::empty_tree(git_repo.object_hash());
        let git_commit_id = git_repo
            .commit_as(
                signature.to_ref(&mut TimeBuf::default()),
                signature.to_ref(&mut TimeBuf::default()),
                "refs/heads/main",
                "git commit message",
                empty_tree_id,
                [] as [gix::ObjectId; 0],
            )?
            .detach();
        let commit_id = CommitId::from_bytes(git_commit_id.as_bytes());

        // Ref creation shouldn't fail because of duplicated head ids.
        backend.import_head_commits([&commit_id, &commit_id])?;
        assert!(
            git_repo
                .references()?
                .prefixed("refs/jj/keep/")?
                .any(|git_ref| git_ref.unwrap().id().detach() == git_commit_id)
        );
        Ok(())
    }

    #[test_case(gix::hash::Kind::Sha1 ; "sha1")]
    #[test_case(gix::hash::Kind::Sha256; "sha256")]
    fn overlapping_git_commit_id(object_hash: gix::hash::Kind) -> TestResult {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend =
            GitBackend::init_internal(&settings, temp_dir.path(), to_format(object_hash))?;
        let commit1 = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("7f0a7ce70354b22efcccf7bf144017c4"),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        let (commit_id1, mut commit2) = write_commit(commit1)?;
        commit2.predecessors.push(commit_id1.clone());
        // `write_commit` should prevent the ids from being the same by changing the
        // committer timestamp of the commit it actually writes.
        let (commit_id2, mut actual_commit2) = write_commit(commit2.clone())?;
        // The returned matches the ID
        assert_eq!(backend.read_commit(&commit_id2).block_on()?, actual_commit2);
        assert_ne!(commit_id2, commit_id1);
        // The committer timestamp should differ
        assert_ne!(
            actual_commit2.committer.timestamp.timestamp,
            commit2.committer.timestamp.timestamp
        );
        // The rest of the commit should be the same
        actual_commit2.committer.timestamp.timestamp = commit2.committer.timestamp.timestamp;
        assert_eq!(actual_commit2, commit2);
        Ok(())
    }

    #[test]
    fn write_signed_commit_sha1() -> TestResult {
        let (obj, sig) = write_signed_commit(gix::hash::Kind::Sha1)?;
        insta::assert_snapshot!(&obj, @"
        tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp
        gpgsig test sig
         hash=03feb0caccbacce2e7b7bca67f4c82292dd487e669ed8a813120c9f82d3fd0801420a1f5d05e1393abfe4e9fc662399ec4a9a1898c5f1e547e0044a52bd4bd29

        initial
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.sig)?, @"
        test sig
        hash=03feb0caccbacce2e7b7bca67f4c82292dd487e669ed8a813120c9f82d3fd0801420a1f5d05e1393abfe4e9fc662399ec4a9a1898c5f1e547e0044a52bd4bd29
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.data)?, @"
        tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp

        initial
        ");
        Ok(())
    }

    #[test]
    fn write_signed_commit_sha256() -> TestResult {
        let (obj, sig) = write_signed_commit(gix::hash::Kind::Sha256)?;
        insta::assert_snapshot!(&obj, @"
        tree 6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp
        gpgsig-sha256 test sig
         hash=d6219e8e5169d409d115848dea4556b3accc76f3cd8dc9b128cc3fe9f71adae275f0e6ce9f98c581a89b960863b61c61b6479cdc20806009d63aecaaa82f4590

        initial
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.sig)?, @"
        test sig
        hash=d6219e8e5169d409d115848dea4556b3accc76f3cd8dc9b128cc3fe9f71adae275f0e6ce9f98c581a89b960863b61c61b6479cdc20806009d63aecaaa82f4590
        ");
        insta::assert_snapshot!(str::from_utf8(&sig.data)?, @"
        tree 6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321
        author Someone <someone@example.com> 0 +0000
        committer Someone <someone@example.com> 0 +0000
        change-id xpxpxpxpxpxpxpxpxpxpxpxpxpxpxpxp

        initial
        ");
        Ok(())
    }

    fn write_signed_commit(object_hash: gix::hash::Kind) -> TestResult<(String, SecureSig)> {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let backend =
            GitBackend::init_internal(&settings, temp_dir.path(), to_format(object_hash))?;

        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(backend.empty_tree_id().clone()),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::new(vec![42; 16]),
            description: "initial".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let mut signer = |data: &_| {
            let hash: String = hex_util::encode_hex(&blake2b_hash(data));
            Ok(format!("test sig\nhash={hash}\n").into_bytes())
        };

        let (id, commit) = backend
            .write_commit(commit, Some(&mut signer as &mut SigningFn))
            .block_on()?;
        let returned_sig = commit.secure_sig.expect("failed to return the signature");

        let commit = backend.read_commit(&id).block_on()?;
        let sig = commit.secure_sig.expect("failed to read the signature");
        assert_eq!(&sig, &returned_sig);

        let git_repo = gix_repo(&backend);
        let obj = git_repo.find_object(gix::ObjectId::from_bytes_or_panic(id.as_bytes()))?;
        Ok((String::from_utf8(obj.data.clone())?, sig))
    }

    fn git_id(commit_id: &CommitId) -> gix::ObjectId {
        gix::ObjectId::from_bytes_or_panic(commit_id.as_bytes())
    }

    fn create_signature() -> Signature {
        Signature {
            name: GIT_USER.to_string(),
            email: GIT_EMAIL.to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        }
    }

    // Not using testutils::user_settings() because there is a dependency cycle
    // 'jj_lib (1) -> testutils -> jj_lib (2)' which creates another distinct
    // UserSettings type. testutils returns jj_lib (2)'s UserSettings, whereas
    // our UserSettings type comes from jj_lib (1).
    fn user_settings() -> UserSettings {
        let config = StackedConfig::with_defaults();
        UserSettings::from_config_and_home_dir(config, None).unwrap()
    }
}
