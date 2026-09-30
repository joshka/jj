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

use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::default::Default;
use std::ffi::OsString;
use std::iter;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use bstr::BStr;
use bstr::BString;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream;
use itertools::Itertools as _;
use tempfile::TempDir;
use thiserror::Error;

use crate::backend::BackendError;
use crate::backend::ChangeId;
use crate::backend::CommitId;
use crate::backend::TreeValue;
use crate::commit::Commit;
use crate::config::ConfigGetError;
use crate::file_util::IoResultExt as _;
use crate::file_util::PathError;
use crate::file_util::is_empty_dir;
use crate::git_backend::GitBackend;
use crate::git_backend::to_git_object_id;
pub use crate::git_transport::GitCredentialPrompt;
pub use crate::git_transport::GitProgress;
pub use crate::git_transport::GitSidebandLineTerminator;
pub use crate::git_transport::GitSubprocessCallback;
use crate::git_transport::GitTransport;
pub use crate::git_transport::GitTransportError;
use crate::index::IndexError;
use crate::matchers::EverythingMatcher;
use crate::merge::Diff;
use crate::merged_tree::MergedTree;
use crate::merged_tree::TreeDiffEntry;
use crate::object_id::ObjectId as _;
use crate::op_store::ABSENT_REF_TARGET;
use crate::op_store::ABSENT_REMOTE_REF;
use crate::op_store::RefTarget;
use crate::op_store::RefTargetOptionExt as _;
use crate::op_store::RemoteRef;
use crate::op_store::RemoteRefState;
use crate::ref_name::GitRefName;
use crate::ref_name::GitRefNameBuf;
use crate::ref_name::RefName;
use crate::ref_name::RefNameBuf;
use crate::ref_name::RemoteName;
use crate::ref_name::RemoteNameBuf;
use crate::ref_name::RemoteRefSymbol;
use crate::ref_name::RemoteRefSymbolBuf;
use crate::ref_name::WorkspaceName;
use crate::repo::MutableRepo;
use crate::repo::Repo;
use crate::repo_path::RepoPath;
use crate::revset::ResolvedRevsetExpression;
use crate::revset::RevsetEvaluationError;
use crate::revset::RevsetExpression;
use crate::revset::RevsetStreamExt as _;
use crate::settings::UserSettings;
use crate::store::Store;
use crate::str_util::StringExpression;
use crate::str_util::StringMatcher;
use crate::str_util::StringPattern;
use crate::view::View;

/// Reserved remote name for the backing Git repo.
pub const REMOTE_NAME_FOR_LOCAL_GIT_REPO: &RemoteName = RemoteName::new("git");
/// Git ref prefix that would conflict with the reserved "git" remote.
pub const RESERVED_REMOTE_REF_NAMESPACE: &str = "refs/remotes/git/";
/// Git ref prefix where remote bookmarks are stored.
const REMOTE_BOOKMARK_REF_NAMESPACE: &str = "refs/remotes/";
/// Git ref prefix where remote tags will be temporarily fetched.
const REMOTE_TAG_REF_NAMESPACE: &str = "refs/jj/remote-tags/";
/// Ref name used as a placeholder to unset HEAD without a commit.
///
/// This is not a normal branch ref, and is deliberately left unborn: HEAD is
/// pointed at it symbolically while the ref itself is kept nonexistent. That
/// is how jj represents "HEAD has no commit yet".
const UNBORN_ROOT_REF_NAME: &str = "refs/jj/root";
/// Dummy file to be added to the index to indicate that the user is editing a
/// commit with a conflict that isn't represented in the Git index.
const INDEX_DUMMY_CONFLICT_FILE: &str = ".jj-do-not-resolve-this-conflict";

#[derive(Clone, Debug)]
pub struct GitSettings {
    pub abandon_unreachable_commits: bool,
    pub record_synthetic_predecessors: bool,
    pub write_change_id_header: bool,
}

impl GitSettings {
    pub fn from_settings(settings: &UserSettings) -> Result<Self, ConfigGetError> {
        Ok(Self {
            abandon_unreachable_commits: settings.get_bool("git.abandon-unreachable-commits")?,
            record_synthetic_predecessors: settings
                .get_bool("git.record-synthetic-predecessors")?,
            write_change_id_header: settings.get("git.write-change-id-header")?,
        })
    }

    pub fn to_transport_options(&self) -> GitTransportOptions {
        GitTransportOptions::default()
    }
}

/// Options for communicating with Git remotes.
#[derive(Clone, Debug, Default)]
pub struct GitTransportOptions {
    /// Environment variables to set (or override) for transports and the
    /// programs they run, such as `GIT_SSH_COMMAND` or `GIT_ASKPASS`. Setting
    /// these per operation avoids the need for process-wide state.
    pub environment: HashMap<OsString, OsString>,
}

impl GitTransportOptions {
    pub fn from_settings(_settings: &UserSettings) -> Result<Self, ConfigGetError> {
        Ok(Self::default())
    }
}

#[derive(Debug, Error)]
pub enum GitRemoteNameError {
    #[error(
        "Git remote named '{name}' is reserved for local Git repository",
        name = REMOTE_NAME_FOR_LOCAL_GIT_REPO.as_symbol()
    )]
    ReservedForLocalGitRepo,
    #[error("Git remotes with slashes are incompatible with jj: {}", .0.as_symbol())]
    WithSlash(RemoteNameBuf),
    #[error("Invalid Git remote name")]
    InvalidName(#[from] girt::remote::InvalidRemoteName),
}

fn validate_remote_name(name: &RemoteName) -> Result<(), GitRemoteNameError> {
    girt::remote::validate_name(name.as_str().as_bytes())?;
    if name == REMOTE_NAME_FOR_LOCAL_GIT_REPO {
        Err(GitRemoteNameError::ReservedForLocalGitRepo)
    } else if name.as_str().contains('/') {
        Err(GitRemoteNameError::WithSlash(name.to_owned()))
    } else {
        Ok(())
    }
}

/// Parses a full Git reference name.
fn git_ref_name(name: &str) -> Result<girt::refs::RefName, girt::refs::InvalidRefName> {
    girt::refs::RefName::new(name)
}

/// Returns the committed object id format of `git_repo` for the given id.
fn oid_from_commit_id(git_repo: &girt::Repository, id: &CommitId) -> girt::ObjectId {
    to_git_object_id(git_repo.object_format(), id)
}

/// Reference access bundled with an object snapshot for peeling.
pub(crate) struct GitRefs<'a> {
    repo: &'a girt::Repository,
    refs: girt::refs::References<'a>,
    objects: girt::Objects,
}

impl<'a> GitRefs<'a> {
    pub(crate) fn new(repo: &'a girt::Repository) -> Result<Self, BoxedError> {
        Ok(Self {
            repo,
            refs: repo.references()?,
            objects: repo.objects(girt::PackLimits::trusted())?,
        })
    }

    pub(crate) fn refs(&self) -> &girt::refs::References<'a> {
        &self.refs
    }

    /// Lists references (with peeled hints) under `prefix`, which must end in `/`.
    fn list_prefixed(
        &self,
        prefix: &str,
    ) -> Result<Vec<girt::refs::ReferenceObservation>, BoxedError> {
        let namespace = girt::refs::RefName::new(prefix.trim_end_matches('/'))?;
        Ok(self.refs.list_namespace_observations(&namespace)?)
    }

    fn find(&self, name: &str) -> Result<Option<girt::refs::ReferenceObservation>, BoxedError> {
        let Ok(name) = git_ref_name(name) else {
            return Ok(None);
        };
        Ok(self.refs.read_observation(&name)?)
    }

    /// Peels `id` through annotated tags and returns it if it's a commit.
    fn peel_to_commit(&self, id: girt::ObjectId) -> Option<girt::ObjectId> {
        let peeled = self
            .objects
            .peel(
                id,
                girt::PeelLimits::trusted(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .ok()?;
        (peeled.kind == girt::ObjectKind::Commit).then_some(peeled.target)
    }

    /// Resolves a reference's target (following symbolic refs) to a commit id.
    ///
    /// If the ref points to the previously `known_commit_oid` (i.e. unchanged),
    /// this avoids reading objects.
    fn resolve_to_commit_id(
        &self,
        observation: &girt::refs::ReferenceObservation,
        known_commit_oid: Option<girt::ObjectId>,
    ) -> Option<girt::ObjectId> {
        let id = match &observation.target {
            girt::refs::Target::Direct(id) => *id,
            girt::refs::Target::Symbolic(name) => self.refs.resolve(name, 5).ok()?.id?,
        };
        if let Some(known) = known_commit_oid
            && (id == known || observation.peeled_hint == Some(known))
        {
            return Some(known);
        }
        self.peel_to_commit(id)
    }

    fn transaction(&self, edits: &[girt::refs::RefEdit]) -> Result<(), BoxedError> {
        if !edits.is_empty() {
            self.refs.transaction(edits)?;
        }
        Ok(())
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Returns the commit HEAD resolves to, or `None` if HEAD is unborn,
/// unreadable, or doesn't point to an existing commit.
fn head_id(git_repo: &girt::Repository) -> Option<girt::ObjectId> {
    let git_refs = GitRefs::new(git_repo).ok()?;
    let head = git_refs.find("HEAD").ok()??;
    git_refs.resolve_to_commit_id(&head, None)
}

/// Reflog policy for a ref update made on behalf of the user, following
/// `core.logAllRefUpdates`.
fn reflog_for(
    git_repo: &girt::Repository,
    name: &girt::refs::RefName,
    committer: &girt::Signature,
    message: &str,
) -> Result<girt::refs::Reflog, girt::ReflogPolicyError> {
    Ok(if git_repo.logs_updates_to(name)? {
        girt::refs::Reflog::Append {
            committer: committer.clone(),
            message: message.as_bytes().to_vec(),
        }
    } else {
        girt::refs::Reflog::Preserve
    })
}

/// Type of Git ref to be imported or exported.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GitRefKind {
    Bookmark,
    Tag,
}

/// Stats from a git push
#[derive(Debug, Default)]
pub struct GitPushStats {
    /// reference accepted by the remote
    pub pushed: Vec<GitRefNameBuf>,
    /// rejected reference, due to lease failure, with an optional reason
    pub rejected: Vec<(GitRefNameBuf, Option<String>)>,
    /// reference rejected by the remote, with an optional reason
    pub remote_rejected: Vec<(GitRefNameBuf, Option<String>)>,
    /// remote bookmarks that couldn't be exported to local Git repo
    pub unexported_bookmarks: Vec<(RemoteRefSymbolBuf, FailedRefExportReason)>,
}

impl GitPushStats {
    pub fn all_ok(&self) -> bool {
        self.rejected.is_empty()
            && self.remote_rejected.is_empty()
            && self.unexported_bookmarks.is_empty()
    }

    /// Returns true if there are at least one bookmark that was successfully
    /// pushed to the remote and exported to the local Git repo.
    pub fn some_exported(&self) -> bool {
        self.pushed.len() > self.unexported_bookmarks.len()
    }
}

/// Newtype to look up `HashMap` entry by key of shorter lifetime.
///
/// https://users.rust-lang.org/t/unexpected-lifetime-issue-with-hashmap-remove/113961/6
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RemoteRefKey<'a>(RemoteRefSymbol<'a>);

impl<'a: 'b, 'b> Borrow<RemoteRefSymbol<'b>> for RemoteRefKey<'a> {
    fn borrow(&self) -> &RemoteRefSymbol<'b> {
        &self.0
    }
}

/// Representation of a Git refspec
///
/// It is often the case that we need only parts of the refspec,
/// Passing strings around and repeatedly parsing them is sub-optimal, confusing
/// and error prone
#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct RefSpec {
    forced: bool,
    // Source and destination may be fully-qualified ref name, glob pattern, or
    // object ID. The GitRefNameBuf type shouldn't be used.
    pub(crate) source: Option<String>,
    pub(crate) destination: String,
}

impl RefSpec {
    fn forced(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            forced: true,
            source: Some(source.into()),
            destination: destination.into(),
        }
    }

    fn delete(destination: impl Into<String>) -> Self {
        // We don't force push on branch deletion
        Self {
            forced: false,
            source: None,
            destination: destination.into(),
        }
    }

    pub(crate) fn to_git_format(&self) -> String {
        format!(
            "{}{}",
            if self.forced { "+" } else { "" },
            self.to_git_format_not_forced()
        )
    }

    /// Format git refspec without the leading force flag '+'
    ///
    /// When independently setting --force-with-lease, having the
    /// leading flag overrides the lease, so we need to print it
    /// without it
    pub(crate) fn to_git_format_not_forced(&self) -> String {
        if let Some(s) = &self.source {
            format!("{}:{}", s, self.destination)
        } else {
            format!(":{}", self.destination)
        }
    }
}

/// Representation of a negative Git refspec
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct NegativeRefSpec {
    source: String,
}

impl NegativeRefSpec {
    fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
        }
    }

    pub(crate) fn to_git_format(&self) -> String {
        format!("^{}", self.source)
    }
}

/// Helper struct that matches a refspec with its expected location in the
/// remote it's being pushed to
pub(crate) struct RefToPush<'a> {
    pub(crate) refspec: &'a RefSpec,
    pub(crate) expected_location: Option<&'a girt::ObjectId>,
}

impl<'a> RefToPush<'a> {
    fn new(
        refspec: &'a RefSpec,
        expected_locations: &'a HashMap<&GitRefName, Option<&girt::ObjectId>>,
    ) -> Self {
        let expected_location = *expected_locations
            .get(GitRefName::new(&refspec.destination))
            .expect(
                "The refspecs and the expected locations were both constructed from the same \
                 source of truth. This means the lookup should always work.",
            );

        Self {
            refspec,
            expected_location,
        }
    }
}

/// Translates Git ref name to jj's `name@remote` symbol. Returns `None` if the
/// ref cannot be represented in jj.
pub fn parse_git_ref(full_name: &GitRefName) -> Option<(GitRefKind, RemoteRefSymbol<'_>)> {
    if let Some(name) = full_name.as_str().strip_prefix("refs/heads/") {
        // Git CLI says 'HEAD' is not a valid branch name
        if name == "HEAD" {
            return None;
        }
        let name = RefName::new(name);
        let remote = REMOTE_NAME_FOR_LOCAL_GIT_REPO;
        Some((GitRefKind::Bookmark, RemoteRefSymbol { name, remote }))
    } else if let Some(remote_and_name) = full_name
        .as_str()
        .strip_prefix(REMOTE_BOOKMARK_REF_NAMESPACE)
    {
        let (remote, name) = remote_and_name.split_once('/')?;
        // "refs/remotes/origin/HEAD" isn't a real remote-tracking branch
        if remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO || name == "HEAD" {
            return None;
        }
        let name = RefName::new(name);
        let remote = RemoteName::new(remote);
        Some((GitRefKind::Bookmark, RemoteRefSymbol { name, remote }))
    } else if let Some(name) = full_name.as_str().strip_prefix("refs/tags/") {
        let name = RefName::new(name);
        let remote = REMOTE_NAME_FOR_LOCAL_GIT_REPO;
        Some((GitRefKind::Tag, RemoteRefSymbol { name, remote }))
    } else {
        None
    }
}

fn parse_remote_tag_ref(full_name: &GitRefName) -> Option<(GitRefKind, RemoteRefSymbol<'_>)> {
    let remote_and_name = full_name.as_str().strip_prefix(REMOTE_TAG_REF_NAMESPACE)?;
    let (remote, name) = remote_and_name.split_once('/')?;
    if remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO {
        return None;
    }
    let name = RefName::new(name);
    let remote = RemoteName::new(remote);
    Some((GitRefKind::Tag, RemoteRefSymbol { name, remote }))
}

fn to_git_ref_name(kind: GitRefKind, symbol: RemoteRefSymbol<'_>) -> Option<GitRefNameBuf> {
    let RemoteRefSymbol { name, remote } = symbol;
    let name = name.as_str();
    let remote = remote.as_str();
    if name.is_empty() || remote.is_empty() {
        return None;
    }
    match kind {
        GitRefKind::Bookmark => {
            if name == "HEAD" {
                return None;
            }
            if remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO {
                Some(format!("refs/heads/{name}").into())
            } else {
                Some(format!("{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/{name}").into())
            }
        }
        GitRefKind::Tag => {
            // Only local tags are mapped. Remote tags don't exist in Git world.
            (remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO).then(|| format!("refs/tags/{name}").into())
        }
    }
}

fn to_git_or_remote_tag_ref_name(symbol: RemoteRefSymbol<'_>) -> GitRefNameBuf {
    let RemoteRefSymbol { name, remote } = symbol;
    let name = name.as_str();
    let remote = remote.as_str();
    if remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO {
        format!("refs/tags/{name}").into()
    } else {
        format!("{REMOTE_TAG_REF_NAMESPACE}{remote}/{name}").into()
    }
}

#[derive(Debug, Error)]
#[error("The repo is not backed by a Git repo")]
pub struct UnexpectedGitBackendError;

/// Returns the underlying `GitBackend` implementation.
pub fn get_git_backend(store: &Store) -> Result<&GitBackend, UnexpectedGitBackendError> {
    store.backend_impl().ok_or(UnexpectedGitBackendError)
}

/// Returns the user's Git configuration (system, global and environment),
/// without any repository configuration.
pub fn global_git_config() -> Option<girt::Config> {
    girt::Config::resolve(&crate::git_backend::git_config_inputs()).ok()
}

/// Returns a freshly opened handle to the underlying Git repo.
pub fn get_git_repo(store: &Store) -> Result<girt::Repository, UnexpectedGitBackendError> {
    get_git_backend(store).map(|backend| backend.git_repo())
}

#[derive(Error, Debug)]
pub enum GitImportError {
    #[error("Failed to read Git HEAD target commit {id}")]
    MissingHeadTarget {
        id: CommitId,
        #[source]
        err: BackendError,
    },
    #[error("Ancestor of Git ref {symbol} is missing")]
    MissingRefAncestor {
        symbol: RemoteRefSymbolBuf,
        #[source]
        err: BackendError,
    },
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error(transparent)]
    RevsetEvaluation(#[from] RevsetEvaluationError),
    #[error(transparent)]
    Git(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

impl GitImportError {
    fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Git(source.into())
    }
}

/// Options for [`import_refs()`].
#[derive(Debug)]
pub struct GitImportOptions {
    /// Whether to abandon commits that became unreachable in Git.
    pub abandon_unreachable_commits: bool,
    /// Whether to generate synthetic predecessors for imported commits.
    pub record_synthetic_predecessors: bool,
    /// Per-remote patterns whether to track bookmarks automatically.
    pub remote_auto_track_bookmarks: HashMap<RemoteNameBuf, StringMatcher>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitImportRefUpdate {
    pub symbol: RemoteRefSymbolBuf,
    pub old_remote_ref: RemoteRef,
    pub new_target: RefTarget,
}

impl GitImportRefUpdate {
    pub fn new(
        symbol: RemoteRefSymbolBuf,
        old_remote_ref: RemoteRef,
        new_target: RefTarget,
    ) -> Self {
        Self {
            symbol,
            old_remote_ref,
            new_target,
        }
    }
}

/// Describes changes made by `import_refs()` or `fetch()`.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct GitImportStats {
    /// Commits that are no longer reachable nor rewritten to the new commits.
    pub abandoned_commits: Vec<Commit>,
    /// Commits that have been rewritten to the new commits.
    pub rewritten_commit_ids: HashSet<CommitId>,
    /// Remote bookmark updates to be merged in to the local bookmarks, sorted
    /// by `symbol`.
    pub changed_remote_bookmarks: Vec<GitImportRefUpdate>,
    /// Remote tag updates to be merged in to the local tags, sorted by
    /// `symbol`.
    pub changed_remote_tags: Vec<GitImportRefUpdate>,
    /// Git ref names that couldn't be imported, sorted by name.
    ///
    /// This list doesn't include refs that are supposed to be ignored, such as
    /// refs pointing to non-commit objects.
    pub failed_ref_names: Vec<BString>,
}

#[derive(Debug)]
struct RefsToImport {
    /// Git ref `(full_name, new_target)`s to be copied to the view, sorted by
    /// `full_name`.
    changed_git_refs: Vec<(GitRefNameBuf, RefTarget)>,
    /// Remote bookmark updates to be merged in to the local bookmarks, sorted
    /// by `symbol`.
    changed_remote_bookmarks: Vec<GitImportRefUpdate>,
    /// Remote tag updates to be merged in to the local tags, sorted by
    /// `symbol`.
    changed_remote_tags: Vec<GitImportRefUpdate>,
    /// Git ref names that couldn't be imported, sorted by name.
    failed_ref_names: Vec<BString>,
}

/// Reflect changes made in the underlying Git repo in the Jujutsu repo.
///
/// This function detects conflicts (if both Git and JJ modified a bookmark) and
/// records them in JJ's view.
pub async fn import_refs(
    mut_repo: &mut MutableRepo,
    options: &GitImportOptions,
) -> Result<GitImportStats, GitImportError> {
    import_some_refs(mut_repo, options, |_, _| true).await
}

/// Reflect changes made in the underlying Git repo in the Jujutsu repo.
///
/// Only bookmarks and tags whose remote symbol pass the filter will be
/// considered for addition, update, or deletion.
pub async fn import_some_refs(
    mut_repo: &mut MutableRepo,
    options: &GitImportOptions,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> Result<GitImportStats, GitImportError> {
    let git_repo = get_git_repo(mut_repo.store())?;

    // Allocate views for new remotes configured externally. There may be
    // remotes with no refs, but the user might still want to "track" absent
    // remote refs.
    for remote_name in iter_remote_names(&git_repo) {
        mut_repo.ensure_remote(&remote_name);
    }

    // Exclude real remote tags, which should never be updated by Git.
    let all_remote_tags = false;
    let refs_to_import =
        diff_refs_to_import(mut_repo.view(), &git_repo, all_remote_tags, git_ref_filter)?;
    import_refs_inner(mut_repo, refs_to_import, options).await
}

async fn import_refs_inner(
    mut_repo: &mut MutableRepo,
    refs_to_import: RefsToImport,
    options: &GitImportOptions,
) -> Result<GitImportStats, GitImportError> {
    let store = mut_repo.store();
    let git_backend = get_git_backend(store).expect("backend type should have been tested");

    let RefsToImport {
        changed_git_refs,
        changed_remote_bookmarks,
        changed_remote_tags,
        failed_ref_names,
    } = refs_to_import;

    let iter_changed_refs = || itertools::chain(&changed_remote_bookmarks, &changed_remote_tags);
    // List of changed old/new ref heads, which may include duplicates.
    let (old_referenced_heads, new_referenced_heads) = {
        let mut old_heads = Vec::new();
        let mut new_heads = Vec::new();
        for update in iter_changed_refs() {
            old_heads.extend(update.old_remote_ref.target.present_adds().cloned());
            new_heads.extend(update.new_target.present_adds().cloned());
        }
        (old_heads, new_heads)
    };
    let old_visible_heads = mut_repo.view().heads().iter().cloned().collect_vec();

    // Bulk-import all reachable Git commits to the backend to reduce overhead
    // of table merging and ref updates.
    //
    // changed_git_refs aren't respected because changed_remote_bookmarks/tags
    // should include all heads that will become reachable in jj.
    let index = mut_repo.index();
    let missing_head_ids: Vec<&CommitId> = stream::iter(&new_referenced_heads)
        .map(async move |id| (id, index.has_id(id).await))
        .buffered(mut_repo.store().concurrency())
        .filter_map(async move |m| match m {
            (id, Ok(false)) => Some(Ok(id)),
            (_, Ok(true)) => None,
            (_, Err(err)) => Some(Err(GitImportError::Index(err))),
        })
        .try_collect()
        .await?;
    let heads_imported = git_backend.import_head_commits(missing_head_ids).is_ok();

    // Import new remote heads
    let mut head_commits = Vec::new();
    let get_commit = async |id: &CommitId, symbol: &RemoteRefSymbolBuf| {
        let missing_ref_err = |err| GitImportError::MissingRefAncestor {
            symbol: symbol.clone(),
            err,
        };
        // If bulk-import failed, try again to find bad head or ref.
        if !heads_imported && !index.has_id(id).await? {
            git_backend
                .import_head_commits([id])
                .map_err(missing_ref_err)?;
        }
        store.get_commit_async(id).await.map_err(missing_ref_err)
    };
    // Uses iter_changed_refs() instead of new_referenced_heads to report error
    // with ref name.
    for update in iter_changed_refs() {
        for id in update.new_target.present_adds() {
            let commit = get_commit(id, &update.symbol).await?;
            head_commits.push(commit);
        }
    }
    // It's unlikely the imported commits were missing, but I/O-related error
    // can still occur.
    let imported_commits = mut_repo.index_commits(&head_commits).await?;
    mut_repo.add_heads(&head_commits).await?;

    // Apply the change that happened in git since last time we imported refs.
    for (full_name, new_target) in changed_git_refs {
        mut_repo.set_git_ref_target(&full_name, new_target);
    }
    for update in &changed_remote_bookmarks {
        let symbol = update.symbol.as_ref();
        let base_target = update.old_remote_ref.tracked_target();
        let new_remote_ref = RemoteRef {
            target: update.new_target.clone(),
            state: if update.old_remote_ref != ABSENT_REMOTE_REF {
                update.old_remote_ref.state
            } else {
                default_remote_ref_state_for(GitRefKind::Bookmark, symbol, options)
            },
        };
        if new_remote_ref.is_tracked() {
            mut_repo
                .merge_local_bookmark(symbol.name, base_target, &new_remote_ref.target)
                .await?;
        }
        // Remote-tracking branch is the last known state of the branch in the remote.
        // It shouldn't diverge even if we had inconsistent view.
        mut_repo.set_remote_bookmark(symbol, new_remote_ref);
    }
    for update in &changed_remote_tags {
        let symbol = update.symbol.as_ref();
        let base_target = update.old_remote_ref.tracked_target();
        let new_remote_ref = RemoteRef {
            target: update.new_target.clone(),
            state: if update.old_remote_ref != ABSENT_REMOTE_REF {
                update.old_remote_ref.state
            } else {
                default_remote_ref_state_for(GitRefKind::Tag, symbol, options)
            },
        };
        if new_remote_ref.is_tracked() {
            mut_repo
                .merge_local_tag(symbol.name, base_target, &new_remote_ref.target)
                .await?;
        }
        // Remote-tracking tag is the last known state of the tag in the remote.
        // It shouldn't diverge even if we had inconsistent view.
        mut_repo.set_remote_tag(symbol, new_remote_ref);
    }

    let any_old_referenced = !old_referenced_heads.is_empty();
    let any_new_referenced = !new_referenced_heads.is_empty();
    let old_visible_heads = RevsetExpression::commits(old_visible_heads);
    let old_referenced_heads = RevsetExpression::commits(old_referenced_heads);
    let new_referenced_heads = RevsetExpression::commits(new_referenced_heads);
    let mut abandoned_commits = if options.abandon_unreachable_commits && any_old_referenced {
        abandon_unreachable_commits(mut_repo, &old_referenced_heads).await?
    } else {
        vec![]
    };
    let rewritten_commit_ids = if options.record_synthetic_predecessors && any_new_referenced {
        record_synthetic_predecessors(
            mut_repo,
            &old_visible_heads,
            Diff::new(&old_referenced_heads, &new_referenced_heads),
            &imported_commits,
            // TODO: Maybe enable rewriting unconditionally? This should be more
            // reliable than reachability-based heuristic.
            options.abandon_unreachable_commits,
        )
        .await?
    } else {
        HashSet::new()
    };
    abandoned_commits.retain(|commit| !rewritten_commit_ids.contains(commit.id()));
    let stats = GitImportStats {
        abandoned_commits,
        rewritten_commit_ids,
        changed_remote_bookmarks,
        changed_remote_tags,
        failed_ref_names,
    };
    Ok(stats)
}

/// Finds commits that used to be reachable in git that no longer are reachable.
/// Those commits will be recorded as abandoned in the `MutableRepo`.
async fn abandon_unreachable_commits(
    mut_repo: &mut MutableRepo,
    hidable_git_heads: &Arc<ResolvedRevsetExpression>,
) -> Result<Vec<Commit>, GitImportError> {
    let pinned_expression = RevsetExpression::union_all(&[
        // Local refs are usually visible, no need to filter out hidden
        RevsetExpression::commits(pinned_commit_ids(mut_repo.view())),
        RevsetExpression::commits(remotely_pinned_commit_ids(mut_repo.view()))
            // Hidden remote refs should not contribute to pinning
            .intersection(&RevsetExpression::visible_heads().ancestors()),
        RevsetExpression::root(),
    ]);
    let abandoned_expression = pinned_expression
        .range(hidable_git_heads)
        // Don't include already-abandoned commits in GitImportStats
        .intersection(&RevsetExpression::visible_heads().ancestors());
    let abandoned_commits: Vec<_> = abandoned_expression
        .evaluate(mut_repo)?
        .stream()
        .commits(mut_repo.store())
        .try_collect()
        .await?;
    for commit in &abandoned_commits {
        mut_repo.record_abandoned_commit(commit);
    }
    Ok(abandoned_commits)
}

/// Deduces predecessors of `old_visible_heads..new_referenced_heads` based on
/// change IDs, records synthetic predecessors, and updates parent mappings.
///
/// The `imported_commits` should exclude any pre-existing commits, including
/// those that were previously hidden.
///
/// Returns old commit IDs that have been mapped to the new commits.
async fn record_synthetic_predecessors(
    mut_repo: &mut MutableRepo,
    old_visible_heads: &Arc<ResolvedRevsetExpression>,
    Diff {
        before: old_referenced_heads,
        after: new_referenced_heads,
    }: Diff<&Arc<ResolvedRevsetExpression>>,
    imported_commits: &[Commit],
    rewrite_commits: bool,
) -> Result<HashSet<CommitId>, GitImportError> {
    let build_change_to_commit_ids_map = async |expr: Arc<ResolvedRevsetExpression>| {
        let mut change_to_commit_ids: HashMap<ChangeId, Vec<CommitId>> = HashMap::new();
        let mut stream = expr.evaluate(mut_repo)?.commit_change_ids();
        while let Some((commit_id, change_id)) = stream.try_next().await? {
            let commit_ids = change_to_commit_ids.entry(change_id).or_default();
            commit_ids.push(commit_id);
        }
        Ok::<_, GitImportError>(change_to_commit_ids)
    };
    let old_referenced_change_to_commit_ids =
        build_change_to_commit_ids_map(new_referenced_heads.range(old_referenced_heads)).await?;
    let new_referenced_change_to_commit_ids =
        build_change_to_commit_ids_map(old_visible_heads.range(new_referenced_heads)).await?;
    let imported_commit_ids: HashSet<_> = imported_commits.iter().map(Commit::id).collect();
    let rewritable_commit_ids: HashSet<_> = if rewrite_commits {
        // Similar to old_referenced_change_to_commit_ids, but doesn't include
        // previously abandoned commits, which shouldn't be rewritten again.
        new_referenced_heads
            .range(old_referenced_heads)
            .intersection(&old_visible_heads.ancestors())
            .evaluate(mut_repo)?
            .stream()
            .try_collect()
            .await?
    } else {
        HashSet::new()
    };

    let mut rewritten_commit_ids = HashSet::new();
    for (change_id, new_commit_ids) in &new_referenced_change_to_commit_ids {
        let predecessor_id: Option<CommitId>;
        let rewrite_source_ids: &[CommitId];
        if let Some(old_commit_ids) = old_referenced_change_to_commit_ids.get(change_id) {
            // Pick the latest one if previously diverged. Divergence isn't
            // usually resolved by "squashing" the commits.
            predecessor_id = Some(old_commit_ids[0].clone());
            rewrite_source_ids = old_commit_ids;
        } else {
            // Record as newly created commit
            predecessor_id = None;
            rewrite_source_ids = &[];
        }
        // Predecessors are recorded only for newly imported commits to prevent
        // cycles in the evolution graph. While this restriction can be lifted
        // later, note that the existing predecessor chain may be more detailed
        // than "imported from Git" if the original commits were created locally.
        for new_commit_id in new_commit_ids
            .iter()
            .filter(|&id| imported_commit_ids.contains(id))
        {
            mut_repo.set_predecessors(new_commit_id.clone(), predecessor_id.as_slice().to_vec());
        }
        let rewrite_source_ids = rewrite_source_ids
            .iter()
            .filter(|id| rewritable_commit_ids.contains(id));
        if let [new_commit_id] = &**new_commit_ids {
            for old_commit_id in rewrite_source_ids {
                mut_repo.set_rewritten_commit(old_commit_id.clone(), new_commit_id.clone());
                rewritten_commit_ids.insert(old_commit_id.clone());
            }
        } else {
            for old_commit_id in rewrite_source_ids {
                mut_repo
                    .set_divergent_rewrite(old_commit_id.clone(), new_commit_ids.iter().cloned());
                rewritten_commit_ids.insert(old_commit_id.clone());
            }
        }
    }

    Ok(rewritten_commit_ids)
}

/// Calculates diff of git refs to be imported.
fn diff_refs_to_import(
    view: &View,
    git_repo: &girt::Repository,
    all_remote_tags: bool,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> Result<RefsToImport, GitImportError> {
    let mut known_git_refs = view
        .git_refs()
        .iter()
        .filter_map(|(full_name, target)| {
            // TODO: or clean up invalid ref in case it was stored due to historical bug?
            let (kind, symbol) =
                parse_git_ref(full_name).expect("stored git ref should be parsable");
            git_ref_filter(kind, symbol).then_some((full_name.as_ref(), target))
        })
        .collect();
    let mut known_remote_bookmarks = view
        .all_remote_bookmarks()
        .filter(|&(symbol, _)| git_ref_filter(GitRefKind::Bookmark, symbol))
        .map(|(symbol, remote_ref)| (RemoteRefKey(symbol), remote_ref))
        .collect();
    let mut known_remote_tags = if all_remote_tags {
        view.all_remote_tags()
            .filter(|&(symbol, _)| git_ref_filter(GitRefKind::Tag, symbol))
            .map(|(symbol, remote_ref)| (RemoteRefKey(symbol), remote_ref))
            .collect()
    } else {
        let remote = REMOTE_NAME_FOR_LOCAL_GIT_REPO;
        view.remote_tags(remote)
            .map(|(name, remote_ref)| (name.to_remote_symbol(remote), remote_ref))
            .filter(|&(symbol, _)| git_ref_filter(GitRefKind::Tag, symbol))
            .map(|(symbol, remote_ref)| (RemoteRefKey(symbol), remote_ref))
            .collect()
    };

    // TODO: Refactor (all_remote_tags, git_ref_filter) in a way that
    // uninteresting refs don't have to be scanned. For example, if the caller
    // imports bookmark changes from a specific remote, we only need to walk
    // refs/remotes/{remote}/.
    let mut changed_git_refs = Vec::new();
    let mut changed_remote_bookmarks = Vec::new();
    let mut changed_remote_tags = Vec::new();
    let mut failed_ref_names = Vec::new();
    let actual = GitRefs::new(git_repo).map_err(GitImportError::Git)?;
    collect_changed_refs_to_import(
        &actual,
        actual
            .list_prefixed("refs/heads/")
            .map_err(GitImportError::Git)?,
        &mut known_git_refs,
        &mut known_remote_bookmarks,
        &mut changed_git_refs,
        &mut changed_remote_bookmarks,
        &mut failed_ref_names,
        &git_ref_filter,
    )?;
    collect_changed_refs_to_import(
        &actual,
        actual
            .list_prefixed("refs/remotes/")
            .map_err(GitImportError::Git)?,
        &mut known_git_refs,
        &mut known_remote_bookmarks,
        &mut changed_git_refs,
        &mut changed_remote_bookmarks,
        &mut failed_ref_names,
        &git_ref_filter,
    )?;
    collect_changed_refs_to_import(
        &actual,
        actual
            .list_prefixed("refs/tags/")
            .map_err(GitImportError::Git)?,
        &mut known_git_refs,
        &mut known_remote_tags,
        &mut changed_git_refs,
        &mut changed_remote_tags,
        &mut failed_ref_names,
        &git_ref_filter,
    )?;
    if all_remote_tags {
        collect_changed_remote_tags_to_import(
            &actual,
            actual
                .list_prefixed(REMOTE_TAG_REF_NAMESPACE)
                .map_err(GitImportError::Git)?,
            &mut known_remote_tags,
            &mut changed_remote_tags,
            &mut failed_ref_names,
            &git_ref_filter,
        )?;
    }
    for full_name in known_git_refs.into_keys() {
        changed_git_refs.push((full_name.to_owned(), RefTarget::absent()));
    }
    for (RemoteRefKey(symbol), old) in known_remote_bookmarks {
        if old.is_present() {
            changed_remote_bookmarks.push(GitImportRefUpdate::new(
                symbol.to_owned(),
                old.clone(),
                RefTarget::absent(),
            ));
        }
    }
    for (RemoteRefKey(symbol), old) in known_remote_tags {
        if old.is_present() {
            changed_remote_tags.push(GitImportRefUpdate::new(
                symbol.to_owned(),
                old.clone(),
                RefTarget::absent(),
            ));
        }
    }

    // Stabilize merge order and output.
    changed_git_refs.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));
    changed_remote_bookmarks
        .sort_unstable_by(|update1, update2| update1.symbol.cmp(&update2.symbol));
    changed_remote_tags.sort_unstable_by(|update1, update2| update1.symbol.cmp(&update2.symbol));
    failed_ref_names.sort_unstable();
    Ok(RefsToImport {
        changed_git_refs,
        changed_remote_bookmarks,
        changed_remote_tags,
        failed_ref_names,
    })
}

#[expect(clippy::too_many_arguments)]
fn collect_changed_refs_to_import(
    git_refs: &GitRefs,
    actual_git_refs: Vec<girt::refs::ReferenceObservation>,
    known_git_refs: &mut HashMap<&GitRefName, &RefTarget>,
    known_remote_refs: &mut HashMap<RemoteRefKey<'_>, &RemoteRef>,
    changed_git_refs: &mut Vec<(GitRefNameBuf, RefTarget)>,
    changed_remote_refs: &mut Vec<GitImportRefUpdate>,
    failed_ref_names: &mut Vec<BString>,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> Result<(), GitImportError> {
    for git_ref in actual_git_refs {
        let full_name_bytes = git_ref.name.as_bytes();
        let Ok(full_name) = str::from_utf8(full_name_bytes) else {
            // Non-utf8 refs cannot be imported.
            failed_ref_names.push(full_name_bytes.into());
            continue;
        };
        if full_name.starts_with(RESERVED_REMOTE_REF_NAMESPACE) {
            failed_ref_names.push(full_name_bytes.into());
            continue;
        }
        let full_name = GitRefName::new(full_name);
        let Some((kind, symbol)) = parse_git_ref(full_name) else {
            // Skip special refs such as refs/remotes/*/HEAD.
            continue;
        };
        if !git_ref_filter(kind, symbol) {
            continue;
        }
        let old_git_target = known_git_refs.get(full_name).copied().flatten();
        let old_git_oid = old_git_target
            .as_normal()
            .map(|id| oid_from_commit_id(git_refs.repo, id));
        let Some(oid) = git_refs.resolve_to_commit_id(&git_ref, old_git_oid) else {
            // Skip (or remove existing) invalid refs.
            continue;
        };
        let new_target = RefTarget::normal(CommitId::from_bytes(oid.as_bytes()));
        known_git_refs.remove(full_name);
        if new_target != *old_git_target {
            changed_git_refs.push((full_name.to_owned(), new_target.clone()));
        }
        // TODO: Make it configurable which remotes are publishing and update public
        // heads here.
        let old_remote_ref = known_remote_refs
            .remove(&symbol)
            .unwrap_or(&ABSENT_REMOTE_REF);
        if new_target != old_remote_ref.target {
            changed_remote_refs.push(GitImportRefUpdate::new(
                symbol.to_owned(),
                old_remote_ref.clone(),
                new_target,
            ));
        }
    }
    Ok(())
}

/// Similar to [`collect_changed_refs_to_import()`], but doesn't track Git ref
/// changes. Remote tags should be managed solely by jj.
fn collect_changed_remote_tags_to_import(
    git_refs: &GitRefs,
    actual_git_refs: Vec<girt::refs::ReferenceObservation>,
    known_remote_refs: &mut HashMap<RemoteRefKey<'_>, &RemoteRef>,
    changed_remote_refs: &mut Vec<GitImportRefUpdate>,
    failed_ref_names: &mut Vec<BString>,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> Result<(), GitImportError> {
    for git_ref in actual_git_refs {
        let full_name_bytes = git_ref.name.as_bytes();
        let Ok(full_name) = str::from_utf8(full_name_bytes) else {
            // Non-utf8 refs cannot be imported.
            failed_ref_names.push(full_name_bytes.into());
            continue;
        };
        let full_name = GitRefName::new(full_name);
        let Some((kind, symbol)) = parse_remote_tag_ref(full_name) else {
            // Skip invalid ref names.
            continue;
        };
        if !git_ref_filter(kind, symbol) {
            continue;
        }
        let old_remote_ref = known_remote_refs
            .get(&symbol)
            .copied()
            .unwrap_or(&ABSENT_REMOTE_REF);
        let old_git_oid = old_remote_ref
            .target
            .as_normal()
            .map(|id| oid_from_commit_id(git_refs.repo, id));
        let Some(oid) = git_refs.resolve_to_commit_id(&git_ref, old_git_oid) else {
            // Skip (or remove existing) invalid refs.
            continue;
        };
        let new_target = RefTarget::normal(CommitId::from_bytes(oid.as_bytes()));
        known_remote_refs.remove(&symbol);
        if new_target != old_remote_ref.target {
            changed_remote_refs.push(GitImportRefUpdate::new(
                symbol.to_owned(),
                old_remote_ref.clone(),
                new_target,
            ));
        }
    }
    Ok(())
}

fn default_remote_ref_state_for(
    kind: GitRefKind,
    symbol: RemoteRefSymbol<'_>,
    options: &GitImportOptions,
) -> RemoteRefState {
    match kind {
        GitRefKind::Bookmark => {
            if symbol.remote == REMOTE_NAME_FOR_LOCAL_GIT_REPO
                || options
                    .remote_auto_track_bookmarks
                    .get(symbol.remote)
                    .is_some_and(|matcher| matcher.is_match(symbol.name.as_str()))
            {
                RemoteRefState::Tracked
            } else {
                RemoteRefState::New
            }
        }
        // TODO: add option to not track tags by default?
        GitRefKind::Tag => RemoteRefState::Tracked,
    }
}

/// Commits referenced by local branches or tags.
///
/// On `import_refs()`, this is similar to collecting commits referenced by
/// `view.git_refs()`. Main difference is that local branches can be moved by
/// tracking remotes, and such mutation isn't applied to `view.git_refs()` yet.
fn pinned_commit_ids(view: &View) -> Vec<CommitId> {
    itertools::chain(view.local_bookmarks(), view.local_tags())
        .flat_map(|(_, target)| target.present_adds())
        .cloned()
        .collect()
}

/// Commits referenced by untracked remote bookmarks/tags including hidden ones.
///
/// Tracked remote refs aren't included because they should have been merged
/// into the local counterparts, and the changes pulled from one remote should
/// propagate to the other remotes on later push. OTOH, untracked remote refs
/// are considered independent refs.
fn remotely_pinned_commit_ids(view: &View) -> Vec<CommitId> {
    itertools::chain(view.all_remote_bookmarks(), view.all_remote_tags())
        .filter(|(_, remote_ref)| !remote_ref.is_tracked())
        .map(|(_, remote_ref)| &remote_ref.target)
        .flat_map(|target| target.present_adds())
        .cloned()
        .collect()
}

/// Imports HEAD from the underlying Git repo.
///
/// Unlike `import_refs()`, the old HEAD branch is not abandoned because HEAD
/// move doesn't always mean the old HEAD branch has been rewritten.
///
/// Unlike `reset_head()`, this function doesn't move the working-copy commit to
/// the child of the new HEAD revision.
pub async fn import_head(
    mut_repo: &mut MutableRepo,
    workspace_name: &WorkspaceName,
    workspace_root: &Path,
) -> Result<(), GitImportError> {
    let store = mut_repo.store();
    let git_backend = get_git_backend(store)?;
    let git_repo = git_backend
        .open_git_repo_at_workdir(workspace_root)
        .map_err(GitImportError::from_git)?;

    let old_git_head = mut_repo.view().git_head(workspace_name);
    let new_git_head_id = head_id(&git_repo).map(|oid| CommitId::from_bytes(oid.as_bytes()));
    if old_git_head.as_resolved() == Some(&new_git_head_id) {
        return Ok(());
    }

    // Import new head
    if let Some(head_id) = &new_git_head_id {
        let index = mut_repo.index();
        if !index.has_id(head_id).await? {
            git_backend.import_head_commits([head_id]).map_err(|err| {
                GitImportError::MissingHeadTarget {
                    id: head_id.clone(),
                    err,
                }
            })?;
        }
        // It's unlikely the imported commits were missing, but I/O-related
        // error can still occur.
        let commit = store.get_commit_async(head_id).await?;
        mut_repo.add_head(&commit).await?;
    }

    mut_repo.set_git_head_target(workspace_name, RefTarget::resolved(new_git_head_id));
    Ok(())
}

/// Imports the HEAD commit without updating the view.
pub async fn import_head_commit(
    mut_repo: &mut MutableRepo,
) -> Result<Option<Commit>, GitImportError> {
    let store = mut_repo.store();
    let git_backend = get_git_backend(store)?;
    let git_repo = git_backend.git_repo();

    let Some(oid) = head_id(&git_repo) else {
        return Ok(None);
    };
    let head_id = CommitId::from_bytes(oid.as_bytes());

    let index = mut_repo.index();
    if !index.has_id(&head_id).await? {
        git_backend.import_head_commits([&head_id]).map_err(|err| {
            GitImportError::MissingHeadTarget {
                id: head_id.clone(),
                err,
            }
        })?;
    }
    // It's unlikely the imported commits were missing, but I/O-related error
    // can still occur.
    let commit = store.get_commit_async(&head_id).await?;
    mut_repo.add_head(&commit).await?;
    Ok(Some(commit))
}

#[derive(Error, Debug)]
pub enum GitExportError {
    #[error(transparent)]
    Git(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

impl GitExportError {
    fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Git(source.into())
    }
}

/// The reason we failed to export a ref to Git.
#[derive(Debug, Error)]
pub enum FailedRefExportReason {
    /// The name is not allowed in Git.
    #[error("Name is not allowed in Git")]
    InvalidGitName,
    /// The ref was in a conflicted state from the last import. A re-import
    /// should fix it.
    #[error("Ref was in a conflicted state from the last import")]
    ConflictedOldState,
    /// The ref points to the root commit, which Git doesn't have.
    #[error("Ref cannot point to the root commit in Git")]
    OnRootCommit,
    /// We wanted to delete it, but it had been modified in Git.
    #[error("Deleted ref had been modified in Git")]
    DeletedInJjModifiedInGit,
    /// We wanted to add it, but Git had added it with a different target
    #[error("Added ref had been added with a different target in Git")]
    AddedInJjAddedInGit,
    /// We wanted to modify it, but Git had deleted it
    #[error("Modified ref had been deleted in Git")]
    ModifiedInJjDeletedInGit,
    /// Failed to delete the ref from the Git repo
    #[error("Failed to delete")]
    FailedToDelete(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Failed to set the ref in the Git repo
    #[error("Failed to set")]
    FailedToSet(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Describes changes made by [`export_refs()`].
#[derive(Debug)]
pub struct GitExportStats {
    /// Remote bookmarks that couldn't be exported, sorted by `symbol`.
    pub failed_bookmarks: Vec<(RemoteRefSymbolBuf, FailedRefExportReason)>,
    /// Remote tags that couldn't be exported, sorted by `symbol`.
    ///
    /// Since Git doesn't have remote tags, this list only contains `@git` tags.
    pub failed_tags: Vec<(RemoteRefSymbolBuf, FailedRefExportReason)>,
}

#[derive(Debug)]
struct AllRefsToExport {
    bookmarks: RefsToExport,
    tags: RefsToExport,
}

#[derive(Debug)]
struct RefsToExport {
    /// Remote `(symbol, (old_oid, new_oid))`s to update, sorted by `symbol`.
    to_update: Vec<(RemoteRefSymbolBuf, (Option<CommitId>, CommitId))>,
    /// Remote `(symbol, old_oid)`s to delete, sorted by `symbol`.
    ///
    /// Deletion has to be exported first to avoid conflict with new refs on
    /// file-system.
    to_delete: Vec<(RemoteRefSymbolBuf, CommitId)>,
    /// Remote refs that couldn't be exported, sorted by `symbol`.
    failed: Vec<(RemoteRefSymbolBuf, FailedRefExportReason)>,
}

/// Export changes to bookmarks and tags made in the Jujutsu repo compared to
/// our last seen view of the Git repo in `mut_repo.view().git_refs()`.
///
/// We ignore changed refs that are conflicted (were also changed in the Git
/// repo compared to our last remembered view of the Git repo). These will be
/// marked conflicted by the next `jj git import`.
///
/// New/updated tags are exported as Git lightweight tags.
pub fn export_refs(mut_repo: &mut MutableRepo) -> Result<GitExportStats, GitExportError> {
    export_some_refs(mut_repo, |_, _| true)
}

pub fn export_some_refs(
    mut_repo: &mut MutableRepo,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> Result<GitExportStats, GitExportError> {
    fn get<'a, V>(map: &'a [(RemoteRefSymbolBuf, V)], key: RemoteRefSymbol<'_>) -> Option<&'a V> {
        debug_assert!(map.is_sorted_by_key(|(k, _)| k));
        let index = map.binary_search_by_key(&key, |(k, _)| k.as_ref()).ok()?;
        let (_, value) = &map[index];
        Some(value)
    }

    let AllRefsToExport { bookmarks, tags } = diff_refs_to_export(
        mut_repo.view(),
        mut_repo.store().root_commit_id(),
        &git_ref_filter,
    );

    let git_backend = get_git_backend(mut_repo.store())?;
    let committer = git_backend.committer_signature();
    let check_and_detach_head = |git_repo: &girt::Repository| -> Result<(), GitExportError> {
        let git_refs = GitRefs::new(git_repo).map_err(GitExportError::Git)?;
        let Some(head_ref) = git_refs.find("HEAD").map_err(GitExportError::Git)? else {
            return Ok(());
        };
        let girt::refs::Target::Symbolic(target_name) = &head_ref.target else {
            return Ok(());
        };
        if let Some((kind, symbol)) = str::from_utf8(target_name.as_bytes())
            .ok()
            .and_then(|name| parse_git_ref(name.as_ref()))
        {
            // Unborn ref should be considered absent
            let current_oid = git_refs
                .refs()
                .resolve(target_name, 5)
                .map_err(GitExportError::from_git)?
                .id;
            let refs = match kind {
                GitRefKind::Bookmark => &bookmarks,
                GitRefKind::Tag => &tags,
            };
            let new_oid = if let Some((_old_oid, new_oid)) = get(&refs.to_update, symbol) {
                Some(oid_from_commit_id(git_repo, new_oid))
            } else if get(&refs.to_delete, symbol).is_some() {
                None
            } else {
                current_oid
            };
            if new_oid != current_oid {
                update_git_head(
                    git_repo,
                    girt::refs::Expected::Value(head_ref.target.clone()),
                    current_oid,
                    &committer,
                )
                .map_err(GitExportError::Git)?;
            }
        }
        Ok(())
    };

    let git_repo = get_git_repo(mut_repo.store())?;

    check_and_detach_head(&git_repo)?;
    let cancel = std::sync::atomic::AtomicBool::new(false);
    for worktree in git_repo
        .worktrees(usize::MAX, &cancel)
        .map_err(GitExportError::from_git)?
    {
        if let Ok(worktree_repo) = crate::git_backend::open_git_repository(&worktree.git_dir) {
            check_and_detach_head(&worktree_repo)?;
        }
    }

    let failed_bookmarks = export_refs_to_git(mut_repo, &git_repo, GitRefKind::Bookmark, bookmarks);
    let failed_tags = export_refs_to_git(mut_repo, &git_repo, GitRefKind::Tag, tags);

    copy_exportable_local_bookmarks_to_remote_view(
        mut_repo,
        REMOTE_NAME_FOR_LOCAL_GIT_REPO,
        |name| {
            let symbol = name.to_remote_symbol(REMOTE_NAME_FOR_LOCAL_GIT_REPO);
            git_ref_filter(GitRefKind::Bookmark, symbol) && get(&failed_bookmarks, symbol).is_none()
        },
    );
    copy_exportable_local_tags_to_remote_view(mut_repo, REMOTE_NAME_FOR_LOCAL_GIT_REPO, |name| {
        let symbol = name.to_remote_symbol(REMOTE_NAME_FOR_LOCAL_GIT_REPO);
        git_ref_filter(GitRefKind::Tag, symbol) && get(&failed_tags, symbol).is_none()
    });

    Ok(GitExportStats {
        failed_bookmarks,
        failed_tags,
    })
}

fn export_refs_to_git(
    mut_repo: &mut MutableRepo,
    git_repo: &girt::Repository,
    kind: GitRefKind,
    refs: RefsToExport,
) -> Vec<(RemoteRefSymbolBuf, FailedRefExportReason)> {
    let mut failed = refs.failed;
    let git_refs = match GitRefs::new(git_repo) {
        Ok(git_refs) => git_refs,
        Err(err) => {
            let err: Arc<BoxedError> = Arc::new(err);
            failed.extend(
                itertools::chain(
                    refs.to_delete.into_iter().map(|(symbol, _)| symbol),
                    refs.to_update.into_iter().map(|(symbol, _)| symbol),
                )
                .map(|symbol| {
                    (
                        symbol,
                        FailedRefExportReason::FailedToSet(err.to_string().into()),
                    )
                }),
            );
            failed.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));
            return failed;
        }
    };
    let committer = get_git_backend(mut_repo.store())
        .expect("backend type should have been tested")
        .committer_signature();
    for (symbol, old_oid) in refs.to_delete {
        let Some(git_ref_name) = to_git_ref_name(kind, symbol.as_ref()) else {
            failed.push((symbol, FailedRefExportReason::InvalidGitName));
            continue;
        };
        let old_oid = oid_from_commit_id(git_repo, &old_oid);
        if let Err(reason) = delete_git_ref(&git_refs, &git_ref_name, old_oid) {
            failed.push((symbol, reason));
        } else {
            let new_target = RefTarget::absent();
            mut_repo.set_git_ref_target(&git_ref_name, new_target);
        }
    }
    for (symbol, (old_commit_oid, new_commit_oid)) in refs.to_update {
        let Some(git_ref_name) = to_git_ref_name(kind, symbol.as_ref()) else {
            failed.push((symbol, FailedRefExportReason::InvalidGitName));
            continue;
        };
        let new_ref_oid = match kind {
            GitRefKind::Bookmark => None,
            // Copy existing tag ref, which may point to annotated tag object.
            GitRefKind::Tag => {
                let remote_matcher = StringMatcher::all();
                find_git_tag_oid_to_copy(
                    mut_repo.view(),
                    &git_refs,
                    &symbol.name,
                    &remote_matcher,
                    oid_from_commit_id(git_repo, &new_commit_oid),
                )
            }
        };
        if let Err(reason) = update_git_ref(
            &git_refs,
            &git_ref_name,
            old_commit_oid
                .as_ref()
                .map(|id| oid_from_commit_id(git_repo, id)),
            oid_from_commit_id(git_repo, &new_commit_oid),
            new_ref_oid,
            &committer,
        ) {
            failed.push((symbol, reason));
        } else {
            let new_target = RefTarget::normal(new_commit_oid);
            mut_repo.set_git_ref_target(&git_ref_name, new_target);
        }
    }

    // Stabilize output, allow binary search.
    failed.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));
    failed
}

fn copy_exportable_local_bookmarks_to_remote_view(
    mut_repo: &mut MutableRepo,
    remote: &RemoteName,
    name_filter: impl Fn(&RefName) -> bool,
) {
    let new_local_bookmarks = mut_repo
        .view()
        .local_remote_bookmarks(remote)
        .filter_map(|(name, targets)| {
            // TODO: filter out untracked bookmarks (if we add support for untracked @git
            // bookmarks)
            let old_target = &targets.remote_ref.target;
            let new_target = targets.local_target;
            (new_target.is_resolved() && old_target != new_target).then_some((name, new_target))
        })
        .filter(|&(name, _)| name_filter(name))
        .map(|(name, new_target)| (name.to_owned(), new_target.clone()))
        .collect_vec();
    for (name, new_target) in new_local_bookmarks {
        let new_remote_ref = RemoteRef {
            target: new_target,
            state: RemoteRefState::Tracked,
        };
        mut_repo.set_remote_bookmark(name.to_remote_symbol(remote), new_remote_ref);
    }
}

fn copy_exportable_local_tags_to_remote_view(
    mut_repo: &mut MutableRepo,
    remote: &RemoteName,
    name_filter: impl Fn(&RefName) -> bool,
) {
    let new_local_tags = mut_repo
        .view()
        .local_remote_tags(remote)
        .filter_map(|(name, targets)| {
            // TODO: filter out untracked tags (if we add support for untracked @git tags)
            let old_target = &targets.remote_ref.target;
            let new_target = targets.local_target;
            (new_target.is_resolved() && old_target != new_target).then_some((name, new_target))
        })
        .filter(|&(name, _)| name_filter(name))
        .map(|(name, new_target)| (name.to_owned(), new_target.clone()))
        .collect_vec();
    for (name, new_target) in new_local_tags {
        let new_remote_ref = RemoteRef {
            target: new_target,
            state: RemoteRefState::Tracked,
        };
        mut_repo.set_remote_tag(name.to_remote_symbol(remote), new_remote_ref);
    }
}

/// Calculates diff of bookmarks and tags to be exported.
fn diff_refs_to_export(
    view: &View,
    root_commit_id: &CommitId,
    git_ref_filter: impl Fn(GitRefKind, RemoteRefSymbol<'_>) -> bool,
) -> AllRefsToExport {
    // Local targets will be copied to the "git" remote if successfully exported. So
    // the local refs are considered to be the new "git" remote refs.
    let mut all_bookmark_targets: HashMap<RemoteRefSymbol, (&RefTarget, &RefTarget)> =
        itertools::chain(
            view.local_bookmarks().map(|(name, target)| {
                let symbol = name.to_remote_symbol(REMOTE_NAME_FOR_LOCAL_GIT_REPO);
                (symbol, target)
            }),
            view.all_remote_bookmarks()
                .filter(|&(symbol, _)| symbol.remote != REMOTE_NAME_FOR_LOCAL_GIT_REPO)
                .map(|(symbol, remote_ref)| (symbol, &remote_ref.target)),
        )
        .filter(|&(symbol, _)| git_ref_filter(GitRefKind::Bookmark, symbol))
        .map(|(symbol, new_target)| (symbol, (&ABSENT_REF_TARGET, new_target)))
        .collect();
    // Remote tags aren't included because Git has no such concept.
    let mut all_tag_targets: HashMap<RemoteRefSymbol, (&RefTarget, &RefTarget)> = view
        .local_tags()
        .map(|(name, target)| {
            let symbol = name.to_remote_symbol(REMOTE_NAME_FOR_LOCAL_GIT_REPO);
            (symbol, target)
        })
        .filter(|&(symbol, _)| git_ref_filter(GitRefKind::Tag, symbol))
        .map(|(symbol, new_target)| (symbol, (&ABSENT_REF_TARGET, new_target)))
        .collect();
    let known_git_refs = view
        .git_refs()
        .iter()
        .map(|(full_name, target)| {
            let (kind, symbol) =
                parse_git_ref(full_name).expect("stored git ref should be parsable");
            ((kind, symbol), target)
        })
        // There are two situations where remote refs get out of sync:
        // 1. `jj bookmark forget --include-remotes`
        // 2. `jj op revert`/`restore` in colocated repo
        .filter(|&((kind, symbol), _)| git_ref_filter(kind, symbol));
    for ((kind, symbol), target) in known_git_refs {
        let ref_targets = match kind {
            GitRefKind::Bookmark => &mut all_bookmark_targets,
            GitRefKind::Tag => &mut all_tag_targets,
        };
        ref_targets
            .entry(symbol)
            .and_modify(|(old_target, _)| *old_target = target)
            .or_insert((target, &ABSENT_REF_TARGET));
    }

    let root_commit_target = RefTarget::normal(root_commit_id.clone());
    let bookmarks = collect_changed_refs_to_export(&all_bookmark_targets, &root_commit_target);
    let tags = collect_changed_refs_to_export(&all_tag_targets, &root_commit_target);
    AllRefsToExport { bookmarks, tags }
}

fn collect_changed_refs_to_export(
    old_new_ref_targets: &HashMap<RemoteRefSymbol, (&RefTarget, &RefTarget)>,
    root_commit_target: &RefTarget,
) -> RefsToExport {
    let mut to_update = Vec::new();
    let mut to_delete = Vec::new();
    let mut failed = Vec::new();
    for (&symbol, &(old_target, new_target)) in old_new_ref_targets {
        if new_target == old_target {
            continue;
        }
        if new_target == root_commit_target {
            // Git doesn't have a root commit
            failed.push((symbol.to_owned(), FailedRefExportReason::OnRootCommit));
            continue;
        }
        let old_oid = if let Some(id) = old_target.as_normal() {
            Some(id.clone())
        } else if !old_target.is_resolved() {
            // The old git ref should only be a conflict if there were concurrent import
            // operations while the value changed. Don't overwrite these values.
            failed.push((symbol.to_owned(), FailedRefExportReason::ConflictedOldState));
            continue;
        } else {
            assert!(old_target.is_absent());
            None
        };
        if let Some(id) = new_target.as_normal() {
            let new_oid = id.clone();
            to_update.push((symbol.to_owned(), (old_oid, new_oid)));
        } else if !new_target.is_resolved() {
            // Skip conflicts and leave the old value in git_refs
            continue;
        } else {
            assert!(new_target.is_absent());
            to_delete.push((symbol.to_owned(), old_oid.unwrap()));
        }
    }

    // Stabilize export order and output, allow binary search.
    to_update.sort_unstable_by(|(sym1, _), (sym2, _)| sym1.cmp(sym2));
    to_delete.sort_unstable_by(|(sym1, _), (sym2, _)| sym1.cmp(sym2));
    failed.sort_unstable_by(|(sym1, _), (sym2, _)| sym1.cmp(sym2));
    RefsToExport {
        to_update,
        to_delete,
        failed,
    }
}

/// Looks up tracked remote tag refs and returns the ref target object ID if
/// peeled to the given commit ID.
fn find_git_tag_oid_to_copy(
    view: &View,
    git_refs: &GitRefs,
    name: &RefName,
    remote_matcher: &StringMatcher,
    commit_oid: girt::ObjectId,
) -> Option<girt::ObjectId> {
    // Filter candidates by tag name and known commit id first
    view.remote_tags_matching(&StringMatcher::exact(name), remote_matcher)
        .filter(|(_, remote_ref)| {
            let maybe_id = remote_ref.tracked_target().as_normal();
            maybe_id.is_some_and(|id| id.as_bytes() == commit_oid.as_bytes())
        })
        // Query existing Git ref and tag object
        .filter_map(|(symbol, _)| {
            let git_ref_name = to_git_or_remote_tag_ref_name(symbol);
            git_refs.find(git_ref_name.as_str()).ok().flatten()
        })
        // This usually holds because remote tags are managed by jj, but jj's
        // view may be updated independently by undo/redo commands.
        .filter(|git_ref| {
            git_refs.resolve_to_commit_id(git_ref, Some(commit_oid)) == Some(commit_oid)
        })
        .find_map(|git_ref| match git_ref.target {
            girt::refs::Target::Direct(id) => Some(id),
            girt::refs::Target::Symbolic(_) => None,
        })
}

fn delete_git_ref(
    git_refs: &GitRefs,
    git_ref_name: &GitRefName,
    old_oid: girt::ObjectId,
) -> Result<(), FailedRefExportReason> {
    let Some(git_ref) = git_refs
        .find(git_ref_name.as_str())
        .map_err(FailedRefExportReason::FailedToDelete)?
    else {
        // The ref is already deleted
        return Ok(());
    };
    if git_refs.resolve_to_commit_id(&git_ref, Some(old_oid)) == Some(old_oid) {
        // The ref has not been updated by git, so go ahead and delete it
        let edit = girt::refs::RefEdit {
            name: git_ref.name,
            dereference: false,
            target: None,
            expected: girt::refs::Expected::Value(git_ref.target),
            reflog: girt::refs::Reflog::Delete,
        };
        git_refs
            .transaction(&[edit])
            .map_err(FailedRefExportReason::FailedToDelete)
    } else {
        // The ref was updated by git
        Err(FailedRefExportReason::DeletedInJjModifiedInGit)
    }
}

/// Writes `target` to the named ref if its stored value matches `expected`.
fn set_git_ref(
    git_refs: &GitRefs,
    git_ref_name: &GitRefName,
    target: girt::ObjectId,
    expected: girt::refs::Expected,
    committer: &girt::Signature,
) -> Result<(), BoxedError> {
    let name = git_ref_name_or_err(git_ref_name.as_str())?;
    let reflog = reflog_for(git_refs.repo, &name, committer, "export from jj")?;
    let edit = girt::refs::RefEdit {
        name,
        dereference: false,
        target: Some(girt::refs::Target::Direct(target)),
        expected,
        reflog,
    };
    git_refs.transaction(&[edit])
}

fn git_ref_name_or_err(name: &str) -> Result<girt::refs::RefName, BoxedError> {
    git_ref_name(name).map_err(|_| format!("Invalid Git ref name: {name}").into())
}

/// Creates new ref pointing to `new_ref_oid` or (peeled) `new_commit_oid`.
fn create_git_ref(
    git_refs: &GitRefs,
    git_ref_name: &GitRefName,
    new_commit_oid: girt::ObjectId,
    new_ref_oid: Option<girt::ObjectId>,
    committer: &girt::Signature,
) -> Result<(), FailedRefExportReason> {
    let new_oid = new_ref_oid.unwrap_or(new_commit_oid);
    let Err(set_err) = set_git_ref(
        git_refs,
        git_ref_name,
        new_oid,
        girt::refs::Expected::Absent,
        committer,
    ) else {
        // The ref was added in jj but still doesn't exist in git
        return Ok(());
    };
    let Some(git_ref) = git_refs
        .find(git_ref_name.as_str())
        .map_err(FailedRefExportReason::FailedToSet)?
    else {
        return Err(FailedRefExportReason::FailedToSet(set_err));
    };
    // The ref was added in jj and in git. We're good if and only if git
    // pointed it to our desired target.
    if git_refs.resolve_to_commit_id(&git_ref, None) == Some(new_commit_oid) {
        Ok(())
    } else {
        Err(FailedRefExportReason::AddedInJjAddedInGit)
    }
}

/// Updates existing ref to point to `new_ref_oid` or (peeled) `new_commit_oid`.
fn move_git_ref(
    git_refs: &GitRefs,
    git_ref_name: &GitRefName,
    old_commit_oid: girt::ObjectId,
    new_commit_oid: girt::ObjectId,
    new_ref_oid: Option<girt::ObjectId>,
    committer: &girt::Signature,
) -> Result<(), FailedRefExportReason> {
    let new_oid = new_ref_oid.unwrap_or(new_commit_oid);
    let expected = girt::refs::Expected::Value(girt::refs::Target::Direct(old_commit_oid));
    let Err(set_err) = set_git_ref(git_refs, git_ref_name, new_oid, expected, committer) else {
        // Successfully updated from old_oid to new_oid (unchanged in git)
        return Ok(());
    };
    // The reference was probably updated in git
    let Some(git_ref) = git_refs
        .find(git_ref_name.as_str())
        .map_err(FailedRefExportReason::FailedToSet)?
    else {
        // The reference was deleted in git and moved in jj
        return Err(FailedRefExportReason::ModifiedInJjDeletedInGit);
    };
    // We still consider this a success if it was updated to our desired target
    let git_commit_oid = git_refs.resolve_to_commit_id(&git_ref, Some(old_commit_oid));
    if git_commit_oid == Some(new_commit_oid) {
        Ok(())
    } else if git_commit_oid == Some(old_commit_oid) {
        // The reference would point to annotated tag, try again
        let expected = girt::refs::Expected::Value(git_ref.target);
        set_git_ref(git_refs, git_ref_name, new_oid, expected, committer)
            .map_err(FailedRefExportReason::FailedToSet)?;
        Ok(())
    } else {
        Err(FailedRefExportReason::FailedToSet(set_err))
    }
}

fn update_git_ref(
    git_refs: &GitRefs,
    git_ref_name: &GitRefName,
    old_commit_oid: Option<girt::ObjectId>,
    new_commit_oid: girt::ObjectId,
    new_ref_oid: Option<girt::ObjectId>,
    committer: &girt::Signature,
) -> Result<(), FailedRefExportReason> {
    match old_commit_oid {
        None => create_git_ref(
            git_refs,
            git_ref_name,
            new_commit_oid,
            new_ref_oid,
            committer,
        ),
        Some(old_oid) => move_git_ref(
            git_refs,
            git_ref_name,
            old_oid,
            new_commit_oid,
            new_ref_oid,
            committer,
        ),
    }
}

/// Ensures Git HEAD is detached and pointing to the `new_oid`. If `new_oid`
/// is `None` (meaning absent), dummy placeholder ref will be set.
fn update_git_head(
    git_repo: &girt::Repository,
    expected_ref: girt::refs::Expected,
    new_oid: Option<girt::ObjectId>,
    committer: &girt::Signature,
) -> Result<(), BoxedError> {
    let refs = git_repo.references()?;
    let head = git_ref_name("HEAD").unwrap();
    let new_target = if let Some(oid) = new_oid {
        girt::refs::Target::Direct(oid)
    } else {
        // Can't detach HEAD without a commit. Use placeholder ref to nullify
        // the HEAD. The placeholder ref isn't a normal branch ref. Git CLI
        // appears to deal with that, and can move the placeholder ref. So we
        // need to ensure that the ref doesn't exist. (A transaction can't both
        // delete a ref and make HEAD point to it.)
        let unborn = git_ref_name(UNBORN_ROOT_REF_NAME).unwrap();
        refs.transaction(&[girt::refs::RefEdit {
            name: unborn.clone(),
            dereference: false,
            target: None,
            expected: girt::refs::Expected::Any,
            reflog: girt::refs::Reflog::Delete,
        }])?;
        girt::refs::Target::Symbolic(unborn)
    };
    let reflog = reflog_for(git_repo, &head, committer, "export from jj")?;
    refs.transaction(&[girt::refs::RefEdit {
        name: head,
        dereference: false,
        target: Some(new_target),
        expected: expected_ref,
        reflog,
    }])?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum GitCreateWorktreeError {
    #[error("Failed to create Git worktree")]
    Create(#[source] girt::CreateWorktreeError),
    #[error("Failed to repair Git worktree links")]
    Repair(#[source] girt::WorktreeAdminError),
    #[error("Failed to inspect the Git worktree destination")]
    Destination(#[source] PathError),
    #[error("Failed to create temporary directory for Git worktree")]
    TempDir(#[source] std::io::Error),
    #[error("A .git file or directory already exists in the Git worktree destination")]
    ExistingGitLink,
    #[error("Failed to move .git gitlink file into place")]
    MoveGitLink(#[source] PathError),
    #[error(transparent)]
    Git(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

impl GitCreateWorktreeError {
    fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Git(source.into())
    }
}

/// Maximum number of registration names tried when the destination's name is
/// already used by another worktree.
const MAX_WORKTREE_NAMES: usize = 1000;

/// Creates a Git worktree at `destination` to back a jj workspace.
///
/// Nothing is checked out: the worktree is created with HEAD unborn, so jj
/// remains responsible for the working-copy contents. `destination` may
/// already hold a populated jj workspace, as it does when colocation is
/// enabled after the fact.
///
/// For a new workspace, the subsequent HEAD export (see [`reset_head()`])
/// points HEAD at the parent of the new working-copy commit. Git is only
/// responsible for the `.git` gitlink and the worktree bookkeeping under
/// `.git/worktrees/`.
pub fn create_worktree(store: &Store, destination: &Path) -> Result<(), GitCreateWorktreeError> {
    let git_backend = get_git_backend(store)?;
    let git_repo = git_backend.git_repo();

    // Worktree registration requires an unborn branch name. It gets a random
    // name: any name we could derive from the destination may already be
    // taken by an exported bookmark.
    let branch_name = format!("refs/heads/jj-worktree-{:016x}", rand::random::<u64>());
    let branch = git_ref_name(&branch_name).expect("valid ref name");

    let destination_exists = destination.exists();
    let destination_is_populated = destination_exists
        && !is_empty_dir(destination).map_err(GitCreateWorktreeError::Destination)?;
    let worktree_repo = if destination_is_populated {
        add_worktree_to_populated_dir(&git_repo, destination, &branch)?
    } else {
        if destination_exists {
            // Creation requires an absent destination.
            std::fs::remove_dir(destination)
                .context(destination)
                .map_err(GitCreateWorktreeError::Destination)?;
        }
        create_orphan_worktree(&git_repo, destination, &branch)?
    };

    // Nullifying HEAD puts the worktree in the same "HEAD has no commit yet"
    // state jj uses everywhere else, so that a `git commit` in the new
    // worktree can't materialize the branch nobody asked for. If the
    // working-copy commit turns out to have a real parent, the HEAD export
    // replaces this with a detached HEAD.
    update_git_head(
        &worktree_repo,
        girt::refs::Expected::Value(girt::refs::Target::Symbolic(branch)),
        None,
        &git_backend.committer_signature(),
    )
    .map_err(GitCreateWorktreeError::Git)
}

fn create_orphan_worktree(
    git_repo: &girt::Repository,
    destination: &Path,
    branch: &girt::refs::RefName,
) -> Result<girt::Repository, GitCreateWorktreeError> {
    // Use relative paths to match jj's convention for portable repositories.
    git_repo
        .create_orphan_worktree_with_link_style(
            destination,
            branch,
            MAX_WORKTREE_NAMES,
            girt::WorktreeLinkStyle::Relative,
        )
        .map_err(GitCreateWorktreeError::Create)
}

/// Adds a worktree for a directory that already has contents.
///
/// Worktree creation requires an absent directory, so the worktree is created
/// in a temporary directory inside `destination`, its gitlink moved into
/// place, and the recorded paths repaired.
fn add_worktree_to_populated_dir(
    git_repo: &girt::Repository,
    destination: &Path,
    branch: &girt::refs::RefName,
) -> Result<girt::Repository, GitCreateWorktreeError> {
    let dot_git = destination.join(".git");
    if dot_git.exists() {
        return Err(GitCreateWorktreeError::ExistingGitLink);
    }
    let tmp_dir = TempDir::new_in(destination).map_err(GitCreateWorktreeError::TempDir)?;
    // The temporary directory's own name becomes the registration name.
    let tmp_path = tmp_dir.path().join(
        destination
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("worktree")),
    );
    let tmp_repo = create_orphan_worktree(git_repo, &tmp_path, branch)?;
    let registration = tmp_repo.git_dir().to_owned();

    std::fs::rename(tmp_path.join(".git"), &dot_git)
        .context(&dot_git)
        .map_err(GitCreateWorktreeError::MoveGitLink)?;
    if let Err(err) = git_repo.repair_worktree_with_link_style(
        &registration,
        destination,
        girt::WorktreeLinkStyle::Relative,
    ) {
        std::fs::remove_file(&dot_git).ok();
        return Err(GitCreateWorktreeError::Repair(err));
    }
    crate::git_backend::open_git_repository(destination).map_err(GitCreateWorktreeError::from_git)
}

#[derive(Debug, Error)]
pub enum GitUnlinkWorktreeError {
    #[error("Failed to remove .git gitlink file")]
    RemoveGitLink(#[source] PathError),
    #[error("Failed to remove Git worktree metadata")]
    Prune(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

/// Disconnects the Git worktree at `worktree_path` from the repository.
///
/// Returns `false` if there was no Git worktree to disconnect.
///
/// The worktree directory and its contents are left in place: only the `.git`
/// gitlink and Git's bookkeeping under `.git/worktrees/` are removed. This is
/// the inverse of [`create_worktree()`], which likewise only sets those up.
///
/// The gitlink is removed before the bookkeeping is pruned, so an error means
/// either that nothing was done, or that the worktree is already disconnected
/// and only stale metadata remains. Neither is worth failing a command over,
/// so callers may treat all errors as non-fatal.
pub fn unlink_worktree(
    store: &Store,
    worktree_path: &Path,
) -> Result<bool, GitUnlinkWorktreeError> {
    let dot_git = worktree_path.join(".git");
    if !dot_git.is_file() {
        return Ok(false);
    }
    let git_backend = get_git_backend(store)?;
    let git_repo = git_backend.git_repo();
    // Identify this worktree's registration before removing its gitlink.
    let registration = girt::RepositoryLocation::at_git_dir(&dot_git)
        .ok()
        .map(|location| location.git_dir().to_owned())
        .filter(|path| path.starts_with(git_repo.common_dir().join("worktrees")));
    // `git worktree remove` semantics aren't used because they delete the
    // directory contents, and forgetting a workspace should preserve its files.
    std::fs::remove_file(&dot_git)
        .context(&dot_git)
        .map_err(GitUnlinkWorktreeError::RemoveGitLink)?;
    // Unlike `git worktree prune`, only this worktree's registration is
    // removed. Its gitlink is gone, so Git would consider it stale anyway.
    if let Some(registration) = registration {
        git_repo
            .prune_worktree(
                &registration,
                std::time::SystemTime::now() + std::time::Duration::from_secs(1),
                girt::WorktreeRetirement::Confirmed,
            )
            .map_err(|err| GitUnlinkWorktreeError::Prune(err.into()))?;
    }
    Ok(true)
}

#[derive(Debug, Error)]
pub enum GitResetHeadError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error(transparent)]
    Git(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to update Git HEAD ref")]
    UpdateHeadRef(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

impl GitResetHeadError {
    fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Git(source.into())
    }
}

/// Sets Git HEAD to the parent of the given working-copy commit and resets
/// the Git index.
pub async fn reset_head(
    mut_repo: &mut MutableRepo,
    workspace_name: &WorkspaceName,
    workspace_root: &Path,
    wc_commit: &Commit,
) -> Result<(), GitResetHeadError> {
    let git_backend = get_git_backend(mut_repo.store())?;
    let git_repo = git_backend
        .open_git_repo_at_workdir(workspace_root)
        .map_err(GitResetHeadError::from_git)?;

    let first_parent_id = &wc_commit.parent_ids()[0];
    let new_head_target = if first_parent_id != mut_repo.store().root_commit_id() {
        RefTarget::normal(first_parent_id.clone())
    } else {
        RefTarget::absent()
    };

    // If the first parent of the working copy has changed, reset the Git HEAD.
    let old_head_target = mut_repo.git_head(workspace_name);
    if *old_head_target != new_head_target {
        let expected_ref = if let Some(id) = old_head_target.as_normal() {
            // We have to check the actual HEAD state because we don't record a
            // symbolic ref as such.
            let head = git_ref_name("HEAD").unwrap();
            let actual_head = git_repo
                .references()
                .and_then(|refs| refs.read(&head))
                .map_err(GitResetHeadError::from_git)?;
            if let Some(girt::refs::Target::Direct(_)) = actual_head {
                let id = oid_from_commit_id(&git_repo, id);
                girt::refs::Expected::Value(girt::refs::Target::Direct(id))
            } else {
                // Just overwrite symbolic ref, which is unusual. Alternatively,
                // maybe we can test the target ref by issuing noop edit.
                girt::refs::Expected::Exists
            }
        } else {
            // Just overwrite if unborn (or conflict), which is also unusual.
            girt::refs::Expected::Exists
        };
        let new_oid = new_head_target
            .as_normal()
            .map(|id| oid_from_commit_id(&git_repo, id));
        update_git_head(
            &git_repo,
            expected_ref,
            new_oid,
            &git_backend.committer_signature(),
        )
        .map_err(GitResetHeadError::UpdateHeadRef)?;
        mut_repo.set_git_head_target(workspace_name, new_head_target);
    }

    // If there is an ongoing operation (merge, rebase, etc.), we need to clean it
    // up.
    clear_operation_state(&git_repo)?;

    reset_index(mut_repo, &git_repo, wc_commit).await
}

fn clear_operation_state(git_repo: &girt::Repository) -> Result<(), GitResetHeadError> {
    // Based on the files `git2::Repository::cleanup_state` deletes; when
    // upstreaming this logic should probably become more elaborate to match
    // `git(1)` behavior.
    const STATE_FILE_NAMES: &[&str] = &[
        "MERGE_HEAD",
        "MERGE_MODE",
        "MERGE_MSG",
        "REVERT_HEAD",
        "CHERRY_PICK_HEAD",
        "BISECT_LOG",
    ];
    const STATE_DIR_NAMES: &[&str] = &["rebase-merge", "rebase-apply", "sequencer"];
    let handle_err = |err: PathError| match err.source.kind() {
        std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(GitResetHeadError::from_git(err)),
    };
    for file_name in STATE_FILE_NAMES {
        let path = git_repo.git_dir().join(file_name);
        std::fs::remove_file(&path)
            .context(&path)
            .or_else(handle_err)?;
    }
    for dir_name in STATE_DIR_NAMES {
        let path = git_repo.git_dir().join(dir_name);
        std::fs::remove_dir_all(&path)
            .context(&path)
            .or_else(handle_err)?;
    }
    Ok(())
}

/// Git index entry for a tree value, or `None` for values that aren't
/// represented in the index.
fn to_index_entry(
    format: girt::ObjectFormat,
    path: &RepoPath,
    value: &TreeValue,
    stage: girt::index::Stage,
) -> Option<girt::index::Entry> {
    let (id, mode) = match value {
        TreeValue::File {
            id,
            executable,
            copy_id: _,
        } => {
            if *executable {
                (id.as_bytes(), girt::index::Mode::Executable)
            } else {
                (id.as_bytes(), girt::index::Mode::Regular)
            }
        }
        TreeValue::Symlink(id) => (id.as_bytes(), girt::index::Mode::Symlink),
        TreeValue::Tree(_) => {
            // This case is only possible if there is a file-directory conflict, since
            // `MergedTree::entries` handles the recursion otherwise. We only materialize a
            // file in the working copy for file-directory conflicts, so we don't add the
            // tree to the index here either.
            return None;
        }
        TreeValue::GitSubmodule(id) => (id.as_bytes(), girt::index::Mode::Gitlink),
    };
    let id = girt::ObjectId::from_bytes(format, id).expect("valid object id");
    let mut entry =
        girt::index::Entry::new(path.as_internal_file_string().as_bytes().to_vec(), mode, id);
    entry.stage = stage;
    Some(entry)
}

fn sort_index_entries(entries: &mut [girt::index::Entry]) {
    entries.sort_by(|a, b| a.path.cmp(&b.path).then(a.stage.cmp(&b.stage)));
}

async fn reset_index(
    repo: &dyn Repo,
    git_repo: &girt::Repository,
    wc_commit: &Commit,
) -> Result<(), GitResetHeadError> {
    let parent_tree = wc_commit.parent_tree(repo).await?;
    // Use the merged parent tree as the Git index, allowing `git diff` to show the
    // same changes as `jj diff`. If the merged parent tree has conflicts, then the
    // Git index will also be conflicted.
    let mut entries = build_index_from_merged_tree(git_repo, &parent_tree)?;

    let wc_tree = wc_commit.tree();
    update_intent_to_add_impl(git_repo, &mut entries, &parent_tree, &wc_tree).await?;

    // Entries in the new index that match entries in the old index keep their
    // cached stat information, so Git doesn't need to rehash unchanged files.
    let mut edit = git_repo
        .edit_index(girt::index::Limits::trusted())
        .map_err(GitResetHeadError::from_git)?;
    // Optional extensions (e.g. resolve-undo, untracked cache) describe the
    // old index contents.
    edit.make_standalone()
        .map_err(GitResetHeadError::from_git)?;
    edit.replace_entries_reusing_stat(entries)
        .map_err(GitResetHeadError::from_git)?;
    edit.commit().map_err(GitResetHeadError::from_git)
}

fn build_index_from_merged_tree(
    git_repo: &girt::Repository,
    merged_tree: &MergedTree,
) -> Result<Vec<girt::index::Entry>, GitResetHeadError> {
    let format = git_repo.object_format();
    let mut entries = Vec::new();
    let mut push_index_entry =
        |path: &RepoPath, maybe_entry: &Option<TreeValue>, stage: girt::index::Stage| {
            if let Some(entry) = maybe_entry
                .as_ref()
                .and_then(|value| to_index_entry(format, path, value, stage))
            {
                entries.push(entry);
            }
        };

    let mut has_many_sided_conflict = false;

    for (path, entry) in merged_tree.entries() {
        let entry = entry?;
        if let Some(resolved) = entry.as_resolved() {
            push_index_entry(&path, resolved, girt::index::Stage::Normal);
            continue;
        }

        let conflict = entry.simplify();
        if let [left, base, right] = conflict.as_slice() {
            // 2-sided conflicts can be represented in the Git index
            push_index_entry(&path, left, girt::index::Stage::Ours);
            push_index_entry(&path, base, girt::index::Stage::Base);
            push_index_entry(&path, right, girt::index::Stage::Theirs);
        } else {
            // We can't represent many-sided conflicts in the Git index, so just add the
            // first side as staged. This is preferable to adding the first 2 sides as a
            // conflict, since some tools rely on being able to resolve conflicts using the
            // index, which could lead to an incorrect conflict resolution if the index
            // didn't contain all of the conflict sides. Instead, we add a dummy conflict of
            // a file named ".jj-do-not-resolve-this-conflict" to prevent the user from
            // accidentally committing the conflict markers.
            has_many_sided_conflict = true;
            push_index_entry(&path, conflict.first(), girt::index::Stage::Normal);
        }
    }

    // If the conflict had an unrepresentable conflict and the dummy file path isn't
    // already added in the index, add a dummy file as a conflict.
    if has_many_sided_conflict
        && !entries
            .iter()
            .any(|entry| entry.path == INDEX_DUMMY_CONFLICT_FILE.as_bytes())
    {
        let file_blob = git_repo
            .loose_objects()
            .write_blob(
                b"The working copy commit contains conflicts which cannot be resolved using Git.\n",
            )
            .map_err(GitResetHeadError::from_git)?;
        let mut entry = girt::index::Entry::new(
            INDEX_DUMMY_CONFLICT_FILE.into(),
            girt::index::Mode::Regular,
            file_blob,
        );
        entry.stage = girt::index::Stage::Ours;
        entries.push(entry);
    }

    sort_index_entries(&mut entries);
    Ok(entries)
}

/// Diff `old_tree` to `new_tree` and mark added files as intent-to-add in the
/// Git index. Also removes current intent-to-add entries in the index if they
/// were removed in the diff.
///
/// Should be called when the diff between the working-copy commit and its
/// parent(s) has changed.
pub async fn update_intent_to_add(
    repo: &dyn Repo,
    workspace_root: &Path,
    old_tree: &MergedTree,
    new_tree: &MergedTree,
) -> Result<(), GitResetHeadError> {
    let git_backend = get_git_backend(repo.store())?;
    let git_repo = git_backend
        .open_git_repo_at_workdir(workspace_root)
        .map_err(GitResetHeadError::from_git)?;
    let mut edit = git_repo
        .edit_index(girt::index::Limits::trusted())
        .map_err(GitResetHeadError::from_git)?;
    let mut entries = edit.index().entries().to_vec();
    let changed = update_intent_to_add_impl(&git_repo, &mut entries, old_tree, new_tree).await?;
    if !changed {
        return edit.abort().map_err(GitResetHeadError::from_git);
    }
    // Replacing entries invalidates the cache-tree (TREE) extension, which
    // would otherwise describe stale entry counts.
    edit.replace_entries(entries)
        .map_err(GitResetHeadError::from_git)?;
    edit.commit().map_err(GitResetHeadError::from_git)
}

/// Returns whether `entries` changed.
async fn update_intent_to_add_impl(
    git_repo: &girt::Repository,
    entries: &mut Vec<girt::index::Entry>,
    old_tree: &MergedTree,
    new_tree: &MergedTree,
) -> Result<bool, GitResetHeadError> {
    let existing_paths: HashSet<Vec<u8>> = entries.iter().map(|entry| entry.path.clone()).collect();
    let mut diff_stream = old_tree.diff_stream(new_tree, &EverythingMatcher);
    let mut added_paths = vec![];
    let mut removed_paths = HashSet::new();
    while let Some(TreeDiffEntry { path, values }) = diff_stream.next().await {
        let values = values?;
        if values.before.is_absent() {
            let executable = match values.after.as_normal() {
                Some(TreeValue::File {
                    id: _,
                    executable,
                    copy_id: _,
                }) => *executable,
                Some(TreeValue::Symlink(_)) => false,
                _ => {
                    continue;
                }
            };
            let path = path.into_internal_string().into_bytes();
            if !existing_paths.contains(&path) {
                added_paths.push((path, executable));
            }
        } else if values.after.is_absent() {
            removed_paths.insert(path.into_internal_string().into_bytes());
        }
    }

    if added_paths.is_empty() && removed_paths.is_empty() {
        return Ok(false);
    }

    if !added_paths.is_empty() {
        // We need to write the empty blob, otherwise `jj util gc` will report an error.
        let empty_blob = git_repo
            .loose_objects()
            .write_blob(b"")
            .map_err(GitResetHeadError::from_git)?;
        for (path, executable) in added_paths {
            // We have checked that the index doesn't have this entry
            let mode = if executable {
                girt::index::Mode::Executable
            } else {
                girt::index::Mode::Regular
            };
            let mut entry = girt::index::Entry::new(path, mode, empty_blob);
            entry.intent_to_add = true;
            entries.push(entry);
        }
    }
    if !removed_paths.is_empty() {
        entries.retain(|entry| !(entry.intent_to_add && removed_paths.contains(&entry.path)));
    }

    sort_index_entries(entries);
    Ok(true)
}

#[derive(Debug, Error)]
pub enum GitRemoteManagementError {
    #[error("No git remote named '{}'", .0.as_symbol())]
    NoSuchRemote(RemoteNameBuf),
    #[error("Git remote named '{}' already exists", .0.as_symbol())]
    RemoteAlreadyExists(RemoteNameBuf),
    #[error(transparent)]
    RemoteName(#[from] GitRemoteNameError),
    #[error("Git remote named '{}' has nonstandard configuration", .0.as_symbol())]
    NonstandardConfiguration(RemoteNameBuf),
    #[error("Git remote named '{}' has invalid configuration", name.as_symbol())]
    InvalidRemote {
        name: RemoteNameBuf,
        #[source]
        source: girt::remote::ConfiguredRemoteError,
    },
    #[error("Error saving Git configuration")]
    GitConfigSaveError(#[source] std::io::Error),
    #[error("Unexpected Git error when managing remotes")]
    InternalGitError(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
    #[error(transparent)]
    RefExpansionError(#[from] GitRefExpansionError),
}

impl GitRemoteManagementError {
    fn from_git(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::InternalGitError(source.into())
    }
}

fn default_fetch_refspec(remote: &RemoteName) -> String {
    format!(
        "+refs/heads/*:{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/*",
        remote = remote.as_str()
    )
}

/// Maximum size of a Git config file edited by jj.
const MAX_CONFIG_BYTES: usize = 64 * 1024 * 1024;

/// A configured Git remote.
#[derive(Clone, Debug)]
pub struct GitRemote {
    fetch_url: Option<BString>,
    push_url: Option<BString>,
    fetch_refspecs: Vec<girt::remote::ConfiguredRefspec>,
    push_refspecs: Vec<girt::remote::ConfiguredRefspec>,
}

impl GitRemote {
    /// First fetch URL.
    pub fn fetch_url(&self) -> Option<&BStr> {
        self.fetch_url.as_ref().map(|url| url.as_ref())
    }

    /// First push URL, falling back to the fetch URL.
    pub fn push_url(&self) -> Option<&BStr> {
        self.push_url.as_ref().map(|url| url.as_ref())
    }

    /// Configured fetch refspecs, in order.
    pub fn fetch_refspecs(&self) -> &[girt::remote::ConfiguredRefspec] {
        &self.fetch_refspecs
    }

    /// Configured push refspecs, in order.
    pub fn push_refspecs(&self) -> &[girt::remote::ConfiguredRefspec] {
        &self.push_refspecs
    }
}

/// Opens the repository's local config file for editing.
fn edit_local_config(
    git_repo: &girt::Repository,
) -> Result<girt::config::ConfigEdit, GitRemoteManagementError> {
    git_repo
        .edit_config(MAX_CONFIG_BYTES)
        .map_err(GitRemoteManagementError::from_git)
}

fn commit_config(edit: girt::config::ConfigEdit) -> Result<(), GitRemoteManagementError> {
    edit.commit()
        .map_err(|err| GitRemoteManagementError::GitConfigSaveError(std::io::Error::other(err)))
}

/// Sets `section.name` (without subsection) in the Git config file at `path`,
/// replacing the last existing value or appending a new one.
pub fn set_git_config_value(
    path: &Path,
    section: &str,
    name: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut edit = girt::config::ConfigEdit::open(path, MAX_CONFIG_BYTES)?;
    let document = edit.document_mut();
    let last = document.config().entries().iter().rposition(|entry| {
        entry.section.eq_ignore_ascii_case(section.as_bytes())
            && entry.subsection.is_none()
            && entry.name.eq_ignore_ascii_case(name.as_bytes())
    });
    match last {
        Some(index) => document.set_value(index, value.as_bytes())?,
        None => document.append(section, None, name, value.as_bytes())?,
    }
    edit.commit()?;
    Ok(())
}

/// Returns physical `[branch]` section ordinals referring to the given remote.
fn git_config_branch_section_ordinals_by_remote(
    document: &girt::config::Document,
    remote_name: &RemoteName,
) -> Result<Vec<usize>, GitRemoteManagementError> {
    let entries = document.config().entries();
    document
        .sections()
        .filter(|section| section.name().eq_ignore_ascii_case(b"branch"))
        .filter_map(|section| {
            let section_entries = &entries[section.entry_range()];
            let values = |key: &str| {
                section_entries
                    .iter()
                    .filter(|entry| entry.name.eq_ignore_ascii_case(key.as_bytes()))
                    .map(|entry| entry.value.as_deref().unwrap_or_default())
                    .collect_vec()
            };
            let remote_values = values("remote");
            let push_remote_values = values("pushRemote");
            if !remote_values
                .iter()
                .chain(push_remote_values.iter())
                .any(|branch_remote_name| *branch_remote_name == remote_name.as_str().as_bytes())
            {
                return None;
            }
            // https://github.com/jj-vcs/jj/issues/6984#issuecomment-3073761797
            let is_supported_key = |name: &[u8]| -> bool {
                name.eq_ignore_ascii_case(b"remote")
                    || name.eq_ignore_ascii_case(b"merge")
                    || name.eq_ignore_ascii_case(b"rebase")
            };
            if remote_values.len() > 1
                || push_remote_values.len() > 1
                || !section_entries
                    .iter()
                    .all(|entry| is_supported_key(&entry.name))
            {
                return Some(Err(GitRemoteManagementError::NonstandardConfiguration(
                    remote_name.to_owned(),
                )));
            }
            Some(Ok(section.ordinal()))
        })
        .collect()
}

fn rename_remote_in_git_branch_config_sections(
    document: &mut girt::config::Document,
    old_remote_name: &RemoteName,
    new_remote_name: &RemoteName,
) -> Result<(), GitRemoteManagementError> {
    let ordinals = git_config_branch_section_ordinals_by_remote(document, old_remote_name)?;
    let indices = document
        .sections()
        .filter(|section| ordinals.contains(&section.ordinal()))
        .flat_map(|section| section.entry_range())
        .filter(|&index| {
            let entry = &document.config().entries()[index];
            entry.name.eq_ignore_ascii_case(b"remote")
                && entry.value.as_deref() == Some(old_remote_name.as_str().as_bytes())
        })
        .collect_vec();
    for index in indices {
        document
            .set_value(index, new_remote_name.as_str().as_bytes())
            .map_err(GitRemoteManagementError::from_git)?;
    }
    Ok(())
}

fn remove_remote_git_branch_config_sections(
    document: &mut girt::config::Document,
    remote_name: &RemoteName,
) -> Result<(), GitRemoteManagementError> {
    let ordinals = git_config_branch_section_ordinals_by_remote(document, remote_name)?;
    document
        .remove_sections(&ordinals)
        .map_err(GitRemoteManagementError::from_git)
}

fn remove_remote_git_config_sections(
    document: &mut girt::config::Document,
    remote_name: &RemoteName,
) -> Result<(), GitRemoteManagementError> {
    let entries = document.config().entries();
    let ordinals: Vec<_> = document
        .sections()
        .filter(|section| {
            section.name().eq_ignore_ascii_case(b"remote")
                && section.subsection() == Some(remote_name.as_str().as_bytes())
        })
        .map(|section| {
            if entries[section.entry_range()].iter().any(|entry| {
                !entry.name.eq_ignore_ascii_case(b"url")
                    && !entry.name.eq_ignore_ascii_case(b"fetch")
                    && !entry.name.eq_ignore_ascii_case(b"tagOpt")
            }) {
                return Err(GitRemoteManagementError::NonstandardConfiguration(
                    remote_name.to_owned(),
                ));
            }
            Ok(section.ordinal())
        })
        .try_collect()?;
    document
        .remove_sections(&ordinals)
        .map_err(GitRemoteManagementError::from_git)
}

/// Returns effective values of `remote.<name>.<key>`, including values from
/// global configuration.
fn remote_values(config: &girt::Config, remote_name: &RemoteName, key: &str) -> Vec<Vec<u8>> {
    config
        .values("remote", Some(remote_name.as_str().as_bytes()), key)
        .map(|value| value.unwrap_or_default().to_vec())
        .collect()
}

/// Writes a new `[remote "<name>"]` section at the end of the document.
fn append_remote_section(
    document: &mut girt::config::Document,
    remote_name: &RemoteName,
    urls: &[Vec<u8>],
    push_urls: &[Vec<u8>],
    fetch_refspecs: &[Vec<u8>],
    tag_opts: &[Vec<u8>],
) -> Result<(), GitRemoteManagementError> {
    let entries = itertools::chain!(
        urls.iter().map(|value| ("url", value.as_slice())),
        push_urls.iter().map(|value| ("pushurl", value.as_slice())),
        fetch_refspecs
            .iter()
            .map(|value| ("fetch", value.as_slice())),
        tag_opts.iter().map(|value| ("tagOpt", value.as_slice())),
    )
    .collect_vec();
    document
        .append_section("remote", Some(remote_name.as_str().as_bytes()), &entries)
        .map_err(GitRemoteManagementError::from_git)
}

/// Returns a sorted list of configured remote names.
pub fn get_all_remote_names(
    store: &Store,
) -> Result<Vec<RemoteNameBuf>, UnexpectedGitBackendError> {
    let git_repo = get_git_repo(store)?;
    Ok(iter_remote_names(&git_repo).collect())
}

/// Returns all configured remote names, including those without URLs, in
/// sorted order.
pub fn configured_remote_names(git_repo: &girt::Repository) -> Vec<RemoteNameBuf> {
    girt::remote::Remote::names(git_repo.config())
        .into_iter()
        // ignore non-UTF-8 remote names which we don't support
        .filter_map(|name| String::from_utf8(name.to_vec()).ok())
        .map(RemoteNameBuf::from)
        .sorted()
        .dedup()
        .collect()
}

fn iter_remote_names(git_repo: &girt::Repository) -> impl Iterator<Item = RemoteNameBuf> {
    configured_remote_names(git_repo)
        .into_iter()
        // exclude empty [remote "<name>"] section
        .filter(|name| try_find_active_remote_inner(git_repo, name).is_some())
}

/// Finds a configured remote with the given `name`. Returns `None` if it
/// doesn't exist or has no fetch or push URLs.
pub fn try_find_active_remote(
    git_repo: &girt::Repository,
    name: &RemoteName,
) -> Result<Option<GitRemote>, GitRemoteManagementError> {
    try_find_active_remote_inner(git_repo, name)
        .transpose()
        .map_err(|source| GitRemoteManagementError::InvalidRemote {
            name: name.to_owned(),
            source,
        })
}

fn try_find_active_remote_inner(
    git_repo: &girt::Repository,
    name: &RemoteName,
) -> Option<Result<GitRemote, girt::remote::ConfiguredRemoteError>> {
    let config = git_repo.config();
    let name_bytes = name.as_str().as_bytes();
    let record = match girt::remote::ConfiguredRemoteRecord::find(config, name_bytes) {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(err) => return Some(Err(err)),
    };
    if record.fetch_urls().len() == 0 && record.push_urls().len() == 0 {
        return None;
    }
    let remote = match girt::remote::ConfiguredRemote::find(config, name_bytes) {
        Ok(Some(remote)) => remote,
        Ok(None) => return None,
        Err(err) => return Some(Err(err)),
    };
    Some(Ok(GitRemote {
        fetch_url: remote.fetch_url().map(BString::from),
        push_url: remote.push_url().map(BString::from),
        fetch_refspecs: remote.fetch_refspecs().to_vec(),
        push_refspecs: remote.push_refspecs().to_vec(),
    }))
}

/// Finds a configured remote without applying `url.<base>.insteadOf` rewrites.
pub fn try_find_remote_without_url_rewrite(
    git_repo: &girt::Repository,
    name: &RemoteName,
) -> Result<Option<GitRemote>, GitRemoteManagementError> {
    let record =
        girt::remote::ConfiguredRemoteRecord::find(git_repo.config(), name.as_str().as_bytes())
            .map_err(|source| GitRemoteManagementError::InvalidRemote {
                name: name.to_owned(),
                source,
            })?;
    Ok(record.map(|record| {
        let fetch_url = record.fetch_urls().next().map(BString::from);
        let push_url = record
            .push_urls()
            .next()
            .map(BString::from)
            .or_else(|| fetch_url.clone());
        GitRemote {
            fetch_url,
            push_url,
            fetch_refspecs: record.fetch_refspecs().to_vec(),
            push_refspecs: record.push_refspecs().to_vec(),
        }
    }))
}

/// Returns the default push remote: `remote.pushDefault` if configured, else
/// `origin` if it exists, as with `git push`.
pub fn default_push_remote_name(git_repo: &girt::Repository) -> Option<RemoteNameBuf> {
    if let Some(name) = git_repo.config().string("remote", None, "pushdefault") {
        return str::from_utf8(name).ok().map(RemoteNameBuf::from);
    }
    let origin = RemoteName::new("origin");
    try_find_active_remote_inner(git_repo, origin).map(|_| origin.to_owned())
}

pub fn add_remote(
    mut_repo: &mut MutableRepo,
    remote_name: &RemoteName,
    url: &str,
    push_url: Option<&str>,
) -> Result<(), GitRemoteManagementError> {
    let git_repo = get_git_repo(mut_repo.store())?;

    validate_remote_name(remote_name)?;

    if try_find_active_remote_inner(&git_repo, remote_name).is_some() {
        return Err(GitRemoteManagementError::RemoteAlreadyExists(
            remote_name.to_owned(),
        ));
    }

    let mut edit = edit_local_config(&git_repo)?;
    let document = edit.document_mut();
    let push_urls = push_url
        .map(|url| url.as_bytes().to_vec())
        .into_iter()
        .collect_vec();
    append_remote_section(
        document,
        remote_name,
        &[url.as_bytes().to_vec()],
        &push_urls,
        &[default_fetch_refspec(remote_name).into_bytes()],
        &[],
    )?;
    commit_config(edit)?;

    mut_repo.ensure_remote(remote_name);

    Ok(())
}

pub fn remove_remote(
    mut_repo: &mut MutableRepo,
    remote_name: &RemoteName,
) -> Result<(), GitRemoteManagementError> {
    let git_repo = get_git_repo(mut_repo.store())?;

    if try_find_active_remote_inner(&git_repo, remote_name).is_none() {
        return Err(GitRemoteManagementError::NoSuchRemote(
            remote_name.to_owned(),
        ));
    }

    let mut edit = edit_local_config(&git_repo)?;
    let document = edit.document_mut();
    remove_remote_git_branch_config_sections(document, remote_name)?;
    remove_remote_git_config_sections(document, remote_name)?;
    commit_config(edit)?;

    remove_remote_git_refs(&git_repo, remote_name).map_err(GitRemoteManagementError::from_git)?;

    if remote_name != REMOTE_NAME_FOR_LOCAL_GIT_REPO {
        remove_remote_refs(mut_repo, remote_name);
    }

    Ok(())
}

fn remove_remote_git_refs(
    git_repo: &girt::Repository,
    remote: &RemoteName,
) -> Result<(), BoxedError> {
    let bookmark_prefix = format!(
        "{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/",
        remote = remote.as_str()
    );
    let tag_prefix = format!(
        "{REMOTE_TAG_REF_NAMESPACE}{remote}/",
        remote = remote.as_str()
    );
    let git_refs = GitRefs::new(git_repo)?;
    let edits = itertools::chain(
        git_refs.list_prefixed(&bookmark_prefix)?,
        git_refs.list_prefixed(&tag_prefix)?,
    )
    .map(|reference| girt::refs::RefEdit {
        name: reference.name,
        dereference: false,
        target: None,
        expected: girt::refs::Expected::Value(reference.target),
        reflog: girt::refs::Reflog::Delete,
    })
    .collect_vec();
    git_refs.transaction(&edits)
}

fn remove_remote_refs(mut_repo: &mut MutableRepo, remote: &RemoteName) {
    mut_repo.remove_remote(remote);
    let prefix = format!(
        "{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/",
        remote = remote.as_str()
    );
    let git_refs_to_delete = mut_repo
        .view()
        .git_refs()
        .keys()
        .filter(|&r| r.as_str().starts_with(&prefix))
        .cloned()
        .collect_vec();
    for git_ref in git_refs_to_delete {
        mut_repo.set_git_ref_target(&git_ref, RefTarget::absent());
    }
}

pub fn rename_remote(
    mut_repo: &mut MutableRepo,
    old_remote_name: &RemoteName,
    new_remote_name: &RemoteName,
) -> Result<(), GitRemoteManagementError> {
    let git_repo = get_git_repo(mut_repo.store())?;

    validate_remote_name(new_remote_name)?;

    let remote = try_find_active_remote(&git_repo, old_remote_name)?
        .ok_or_else(|| GitRemoteManagementError::NoSuchRemote(old_remote_name.to_owned()))?;

    if try_find_active_remote_inner(&git_repo, new_remote_name).is_some() {
        return Err(GitRemoteManagementError::RemoteAlreadyExists(
            new_remote_name.to_owned(),
        ));
    }

    match (remote.fetch_refspecs(), remote.push_refspecs()) {
        ([refspec], [])
            if refspec.to_bytes() == default_fetch_refspec(old_remote_name).as_bytes() => {}
        _ => {
            return Err(GitRemoteManagementError::NonstandardConfiguration(
                old_remote_name.to_owned(),
            ));
        }
    }

    let mut edit = edit_local_config(&git_repo)?;
    let document = edit.document_mut();
    // Values from global configuration are copied to the local file, as jj has
    // always done when renaming a remote.
    let urls = remote_values(git_repo.config(), old_remote_name, "url");
    let push_urls = remote_values(git_repo.config(), old_remote_name, "pushurl");
    let tag_opts = remote_values(git_repo.config(), old_remote_name, "tagOpt");
    append_remote_section(
        document,
        new_remote_name,
        &urls,
        &push_urls,
        &[default_fetch_refspec(new_remote_name).into_bytes()],
        &tag_opts,
    )?;
    rename_remote_in_git_branch_config_sections(document, old_remote_name, new_remote_name)?;
    remove_remote_git_config_sections(document, old_remote_name)?;
    commit_config(edit)?;

    rename_remote_git_refs(
        &git_repo,
        old_remote_name,
        new_remote_name,
        &get_git_backend(mut_repo.store())?.committer_signature(),
    )
    .map_err(GitRemoteManagementError::from_git)?;

    if old_remote_name != REMOTE_NAME_FOR_LOCAL_GIT_REPO {
        rename_remote_refs(mut_repo, old_remote_name, new_remote_name);
    }

    Ok(())
}

fn rename_remote_git_refs(
    git_repo: &girt::Repository,
    old_remote_name: &RemoteName,
    new_remote_name: &RemoteName,
    committer: &girt::Signature,
) -> Result<(), BoxedError> {
    let to_prefixes = |namespace: &str| {
        (
            format!("{namespace}{remote}/", remote = old_remote_name.as_str()),
            format!("{namespace}{remote}/", remote = new_remote_name.as_str()),
        )
    };
    let ref_log_message = format!(
        "renamed remote {old_remote_name} to {new_remote_name}",
        old_remote_name = old_remote_name.as_symbol(),
        new_remote_name = new_remote_name.as_symbol(),
    );
    let git_refs = GitRefs::new(git_repo)?;
    let mut edits = Vec::new();
    for namespace in [REMOTE_BOOKMARK_REF_NAMESPACE, REMOTE_TAG_REF_NAMESPACE] {
        let (old_prefix, new_prefix) = to_prefixes(namespace);
        for old_ref in git_refs.list_prefixed(&old_prefix)? {
            let new_name = girt::refs::RefName::new(
                [
                    new_prefix.as_bytes(),
                    &old_ref.name.as_bytes()[old_prefix.len()..],
                ]
                .concat(),
            )
            .map_err(|_| "new ref name to be valid")?;
            let reflog = reflog_for(git_repo, &new_name, committer, &ref_log_message)?;
            edits.push(girt::refs::RefEdit {
                name: new_name,
                dereference: false,
                target: Some(old_ref.target.clone()),
                expected: girt::refs::Expected::Absent,
                reflog,
            });
            edits.push(girt::refs::RefEdit {
                name: old_ref.name,
                dereference: false,
                target: None,
                expected: girt::refs::Expected::Value(old_ref.target),
                reflog: girt::refs::Reflog::Delete,
            });
        }
    }
    git_refs.transaction(&edits)
}

/// Sets the new URLs on the remote. If a URL of given kind is not provided, it
/// is not changed. I.e. it is not possible to remove a fetch/push URL from a
/// remote using this method.
pub fn set_remote_urls(
    store: &Store,
    remote_name: &RemoteName,
    new_url: Option<&str>,
    new_push_url: Option<&str>,
) -> Result<(), GitRemoteManagementError> {
    // quick sanity check
    if new_url.is_none() && new_push_url.is_none() {
        return Ok(());
    }

    let git_repo = get_git_repo(store)?;

    validate_remote_name(remote_name)?;

    if try_find_remote_without_url_rewrite(&git_repo, remote_name)?.is_none() {
        return Err(GitRemoteManagementError::NoSuchRemote(
            remote_name.to_owned(),
        ));
    }

    let mut edit = edit_local_config(&git_repo)?;
    let document = edit.document_mut();
    for (key, new_value) in [("url", new_url), ("pushurl", new_push_url)] {
        let Some(new_value) = new_value else {
            continue;
        };
        // Replace the first value (the one in effect) and drop the rest, as
        // there's only one URL of each kind.
        let occurrences = document
            .config()
            .entries()
            .iter()
            .positions(|entry| {
                entry.section.eq_ignore_ascii_case(b"remote")
                    && entry.subsection.as_deref() == Some(remote_name.as_str().as_bytes())
                    && entry.name.eq_ignore_ascii_case(key.as_bytes())
            })
            .collect_vec();
        if let Some(&first) = occurrences.first() {
            document
                .set_value(first, new_value.as_bytes())
                .map_err(GitRemoteManagementError::from_git)?;
            for &index in occurrences[1..].iter().rev() {
                document
                    .remove(index)
                    .map_err(GitRemoteManagementError::from_git)?;
            }
        } else {
            append_after_remote_urls(document, remote_name, key, new_value.as_bytes())?;
        }
    }
    commit_config(edit)?;

    Ok(())
}

/// Appends a URL-kind value to the remote's section, keeping `url`/`pushurl`
/// before other keys such as `fetch`, as Git does when writing a new remote.
fn append_after_remote_urls(
    document: &mut girt::config::Document,
    remote_name: &RemoteName,
    key: &str,
    value: &[u8],
) -> Result<(), GitRemoteManagementError> {
    let name = remote_name.as_str().as_bytes();
    let section = document
        .sections()
        .filter(|section| {
            section.name().eq_ignore_ascii_case(b"remote") && section.subsection() == Some(name)
        })
        .last()
        .map(|section| section.entry_range());
    // Rebuild the section's entries with the new value inserted after URLs.
    let Some(range) = section else {
        return document
            .append("remote", Some(name), key, value)
            .map_err(GitRemoteManagementError::from_git);
    };
    let entries = document.config().entries()[range.clone()]
        .iter()
        .map(|entry| (entry.name.clone(), entry.value.clone().unwrap_or_default()))
        .collect_vec();
    let split = entries
        .iter()
        .rposition(|(name, _)| {
            name.eq_ignore_ascii_case(b"url") || name.eq_ignore_ascii_case(b"pushurl")
        })
        .map_or(0, |i| i + 1);
    for index in range.rev() {
        document
            .remove(index)
            .map_err(GitRemoteManagementError::from_git)?;
    }
    let rebuilt = itertools::chain!(
        entries[..split]
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
        [(key.as_bytes().to_vec(), value.to_vec())],
        entries[split..]
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
    )
    .collect_vec();
    for (entry_name, entry_value) in rebuilt {
        let entry_name = String::from_utf8(entry_name).expect("config names are ASCII");
        document
            .append("remote", Some(name), &entry_name, &entry_value)
            .map_err(GitRemoteManagementError::from_git)?;
    }
    Ok(())
}

fn rename_remote_refs(
    mut_repo: &mut MutableRepo,
    old_remote_name: &RemoteName,
    new_remote_name: &RemoteName,
) {
    mut_repo.rename_remote(old_remote_name.as_ref(), new_remote_name.as_ref());
    let prefix = format!(
        "{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/",
        remote = old_remote_name.as_str()
    );
    let git_refs = mut_repo
        .view()
        .git_refs()
        .iter()
        .filter_map(|(old, target)| {
            old.as_str().strip_prefix(&prefix).map(|p| {
                let new: GitRefNameBuf = format!(
                    "{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/{p}",
                    remote = new_remote_name.as_str()
                )
                .into();
                (old.clone(), new, target.clone())
            })
        })
        .collect_vec();
    for (old, new, target) in git_refs {
        mut_repo.set_git_ref_target(&old, RefTarget::absent());
        mut_repo.set_git_ref_target(&new, target);
    }
}

const INVALID_REFSPEC_CHARS: [char; 5] = [':', '^', '?', '[', ']'];

#[derive(Error, Debug)]
pub enum GitFetchError {
    #[error("No git remote named '{}'", .0.as_symbol())]
    NoSuchRemote(RemoteNameBuf),
    #[error(transparent)]
    RemoteName(#[from] GitRemoteNameError),
    #[error("Failed to update refs: {}", .0.iter().map(|n| n.as_symbol()).join(", "))]
    RejectedUpdates(Vec<GitRefNameBuf>),
    #[error(transparent)]
    Transport(#[from] GitTransportError),
}

#[derive(Error, Debug)]
pub enum GitDefaultRefspecError {
    #[error("No git remote named '{}'", .0.as_symbol())]
    NoSuchRemote(RemoteNameBuf),
    #[error("Invalid configuration for remote `{}`", .0.as_symbol())]
    InvalidRemoteConfiguration(
        RemoteNameBuf,
        #[source] Box<girt::remote::ConfiguredRemoteError>,
    ),
}

struct FetchedRefs {
    remote: RemoteNameBuf,
    bookmark_matcher: StringMatcher,
    tag_matcher: StringMatcher,
}

/// Name patterns that will be transformed to Git refspecs.
#[derive(Clone, Debug)]
pub struct GitFetchRefExpression {
    /// Matches bookmark or branch names.
    pub bookmark: StringExpression,
    /// Matches tag names.
    ///
    /// Tags matching this expression will be fetched as "remote tags" and
    /// merged with tracking local tags. This is different from `git fetch`,
    /// which would directly update local tags.
    pub tag: StringExpression,
}

/// Represents the refspecs to fetch from a remote
#[derive(Debug)]
pub struct ExpandedFetchRefSpecs {
    /// Matches (positive) `refspecs`, but not `negative_refspecs`.
    expr: GitFetchRefExpression,
    refspecs: Vec<RefSpec>,
    negative_refspecs: Vec<NegativeRefSpec>,
}

#[derive(Error, Debug)]
pub enum GitRefExpansionError {
    #[error(transparent)]
    Expression(#[from] GitRefExpressionError),
    #[error(
        "Invalid branch pattern provided. When fetching, branch names and globs may not contain the characters `{chars}`",
        chars = INVALID_REFSPEC_CHARS.iter().join("`, `")
    )]
    InvalidBranchPattern(StringPattern),
}

/// Expand a list of branch string patterns to refspecs to fetch
pub fn expand_fetch_refspecs(
    remote: &RemoteName,
    expr: GitFetchRefExpression,
) -> Result<ExpandedFetchRefSpecs, GitRefExpansionError> {
    let (positive_bookmarks, negative_bookmarks) =
        split_into_positive_negative_patterns(&expr.bookmark)?;
    let (positive_tags, negative_tags) = split_into_positive_negative_patterns(&expr.tag)?;

    let refspecs = itertools::chain(
        positive_bookmarks
            .iter()
            .map(|&pattern| pattern_to_refspec_glob(pattern))
            .map_ok(|glob| {
                RefSpec::forced(
                    format!("refs/heads/{glob}"),
                    format!(
                        "{REMOTE_BOOKMARK_REF_NAMESPACE}{remote}/{glob}",
                        remote = remote.as_str()
                    ),
                )
            }),
        positive_tags
            .iter()
            .map(|&pattern| pattern_to_refspec_glob(pattern))
            .map_ok(|glob| {
                RefSpec::forced(
                    format!("refs/tags/{glob}"),
                    format!(
                        "{REMOTE_TAG_REF_NAMESPACE}{remote}/{glob}",
                        remote = remote.as_str()
                    ),
                )
            }),
    )
    .try_collect()?;

    let negative_refspecs = itertools::chain(
        negative_bookmarks
            .iter()
            .map(|&pattern| pattern_to_refspec_glob(pattern))
            .map_ok(|glob| NegativeRefSpec::new(format!("refs/heads/{glob}"))),
        negative_tags
            .iter()
            .map(|&pattern| pattern_to_refspec_glob(pattern))
            .map_ok(|glob| NegativeRefSpec::new(format!("refs/tags/{glob}"))),
    )
    .try_collect()?;

    Ok(ExpandedFetchRefSpecs {
        expr,
        refspecs,
        negative_refspecs,
    })
}

fn pattern_to_refspec_glob(pattern: &StringPattern) -> Result<Cow<'_, str>, GitRefExpansionError> {
    pattern
        .to_glob()
        // This triggered by non-glob `*`s in addition to INVALID_REFSPEC_CHARS
        // because `to_glob()` escapes such `*`s as `[*]`.
        .filter(|glob| !glob.contains(INVALID_REFSPEC_CHARS))
        .ok_or_else(|| GitRefExpansionError::InvalidBranchPattern(pattern.clone()))
}

#[derive(Debug, Error)]
pub enum GitRefExpressionError {
    #[error("Cannot use `~` in sub expression")]
    NestedNotIn,
    #[error("Cannot use `&` in sub expression")]
    NestedIntersection,
    #[error("Cannot use `&` for positive expressions")]
    PositiveIntersection,
}

/// Splits string matcher expression into Git-compatible `(positive | ...) &
/// ~(negative | ...)` form.
fn split_into_positive_negative_patterns(
    expr: &StringExpression,
) -> Result<(Vec<&StringPattern>, Vec<&StringPattern>), GitRefExpressionError> {
    static ALL: StringPattern = StringPattern::all();

    // Outer expression is considered an intersection of
    // - zero or one union of positive expressions
    // - zero or more unions of negative expressions
    // e.g.
    // - `a`                (1+)
    // - `~a&~b`            (1-, 1-)
    // - `(a|b)&~(c|d)&~e`  (2+, 2-, 1-)
    //
    // No negation nor intersection is allowed under union or not-in nodes.
    // - `a|~b`             (incompatible with Git refspecs)
    // - `~(~a&~b)`         (equivalent to `a|b`, but unsupported)
    // - `(a&~b)&(~c&~d)`   (equivalent to `a&~b&~c&~d`, but unsupported)

    fn visit_positive<'a>(
        expr: &'a StringExpression,
        positives: &mut Vec<&'a StringPattern>,
        negatives: &mut Vec<&'a StringPattern>,
    ) -> Result<(), GitRefExpressionError> {
        match expr {
            StringExpression::Pattern(pattern) => {
                positives.push(pattern);
                Ok(())
            }
            StringExpression::NotIn(complement) => {
                positives.push(&ALL);
                visit_negative(complement, negatives)
            }
            StringExpression::Union(expr1, expr2) => visit_union(expr1, expr2, positives),
            StringExpression::Intersection(expr1, expr2) => {
                match (expr1.as_ref(), expr2.as_ref()) {
                    (other, StringExpression::NotIn(complement))
                    | (StringExpression::NotIn(complement), other) => {
                        visit_positive(other, positives, negatives)?;
                        visit_negative(complement, negatives)
                    }
                    _ => Err(GitRefExpressionError::PositiveIntersection),
                }
            }
        }
    }

    fn visit_negative<'a>(
        expr: &'a StringExpression,
        negatives: &mut Vec<&'a StringPattern>,
    ) -> Result<(), GitRefExpressionError> {
        match expr {
            StringExpression::Pattern(pattern) => {
                negatives.push(pattern);
                Ok(())
            }
            StringExpression::NotIn(_) => Err(GitRefExpressionError::NestedNotIn),
            StringExpression::Union(expr1, expr2) => visit_union(expr1, expr2, negatives),
            StringExpression::Intersection(_, _) => Err(GitRefExpressionError::NestedIntersection),
        }
    }

    fn visit_union<'a>(
        expr1: &'a StringExpression,
        expr2: &'a StringExpression,
        patterns: &mut Vec<&'a StringPattern>,
    ) -> Result<(), GitRefExpressionError> {
        visit_union_sub(expr1, patterns)?;
        visit_union_sub(expr2, patterns)
    }

    fn visit_union_sub<'a>(
        expr: &'a StringExpression,
        patterns: &mut Vec<&'a StringPattern>,
    ) -> Result<(), GitRefExpressionError> {
        match expr {
            StringExpression::Pattern(pattern) => {
                patterns.push(pattern);
                Ok(())
            }
            StringExpression::NotIn(_) => Err(GitRefExpressionError::NestedNotIn),
            StringExpression::Union(expr1, expr2) => visit_union(expr1, expr2, patterns),
            StringExpression::Intersection(_, _) => Err(GitRefExpressionError::NestedIntersection),
        }
    }

    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    visit_positive(expr, &mut positives, &mut negatives)?;
    // Don't generate uninteresting patterns for `~*` (= none). `x~*`, `~(x|*)`,
    // etc. aren't special-cased because `x` may be Git-incompatible pattern.
    if positives.iter().all(|pattern| pattern.is_all())
        && !negatives.is_empty()
        && negatives.iter().all(|pattern| pattern.is_all())
    {
        Ok((vec![], vec![]))
    } else {
        Ok((positives, negatives))
    }
}

/// A list of fetch refspecs configured within a remote that were ignored during
/// an expansion. Callers should consider displaying these in the UI as
/// appropriate.
#[derive(Debug)]
#[must_use = "warnings should be surfaced in the UI"]
pub struct IgnoredRefspecs(pub Vec<IgnoredRefspec>);

/// A fetch refspec configured within a remote that was ignored during
/// expansion.
#[derive(Debug)]
pub struct IgnoredRefspec {
    /// The ignored refspec
    pub refspec: BString,
    /// The reason why it was ignored
    pub reason: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchRefSpecKind {
    Positive,
    Negative,
}

/// Loads the remote's fetch branch or bookmark patterns from Git config.
pub fn load_default_fetch_bookmarks(
    remote_name: &RemoteName,
    git_repo: &girt::Repository,
) -> Result<(IgnoredRefspecs, StringExpression), GitDefaultRefspecError> {
    let remote = try_find_active_remote_inner(git_repo, remote_name)
        .ok_or_else(|| GitDefaultRefspecError::NoSuchRemote(remote_name.to_owned()))?
        .map_err(|e| {
            GitDefaultRefspecError::InvalidRemoteConfiguration(remote_name.to_owned(), Box::new(e))
        })?;

    let remote_refspecs = remote.fetch_refspecs();
    let mut ignored_refspecs = Vec::with_capacity(remote_refspecs.len());
    let mut positive_bookmarks = Vec::with_capacity(remote_refspecs.len());
    let mut negative_bookmarks = Vec::new();
    for refspec in remote_refspecs {
        match parse_fetch_refspec(remote_name, refspec) {
            Ok((FetchRefSpecKind::Positive, bookmark)) => {
                positive_bookmarks.push(StringExpression::pattern(bookmark));
            }
            Ok((FetchRefSpecKind::Negative, bookmark)) => {
                negative_bookmarks.push(StringExpression::pattern(bookmark));
            }
            Err(reason) => {
                let refspec = refspec.to_bytes().into();
                ignored_refspecs.push(IgnoredRefspec { refspec, reason });
            }
        }
    }

    let mut bookmark_expr = StringExpression::union_all(positive_bookmarks);
    // Avoid double negation `~~*` when no negative patterns are provided.
    if !negative_bookmarks.is_empty() {
        bookmark_expr =
            bookmark_expr.intersection(StringExpression::union_all(negative_bookmarks).negated());
    }

    Ok((IgnoredRefspecs(ignored_refspecs), bookmark_expr))
}

fn parse_fetch_refspec(
    remote_name: &RemoteName,
    refspec: &girt::remote::ConfiguredRefspec,
) -> Result<(FetchRefSpecKind, StringPattern), &'static str> {
    let ensure_utf8 = |s| str::from_utf8(s).map_err(|_| "invalid UTF-8");

    let (src, positive_dst) = match refspec.kind() {
        girt::remote::ConfiguredRefspecKind::Selection => {
            return Err("fetch-only refspecs are not supported");
        }
        girt::remote::ConfiguredRefspecKind::Mapping => {
            if !refspec.force() {
                return Err("non-forced refspecs are not supported");
            }
            (
                ensure_utf8(refspec.source().unwrap_or_default())?,
                Some(ensure_utf8(refspec.destination().unwrap_or_default())?),
            )
        }
        girt::remote::ConfiguredRefspecKind::Exclusion => {
            (ensure_utf8(refspec.source().unwrap_or_default())?, None)
        }
        girt::remote::ConfiguredRefspecKind::AllMatching
        | girt::remote::ConfiguredRefspecKind::Deletion => {
            return Err("push refspecs are not supported");
        }
    };

    let src_branch = src
        .strip_prefix("refs/heads/")
        .ok_or("only refs/heads/ is supported for refspec sources")?;
    let branch = StringPattern::glob(src_branch).map_err(|_| "invalid pattern")?;

    if let Some(dst) = positive_dst {
        let dst_without_prefix = dst
            .strip_prefix(REMOTE_BOOKMARK_REF_NAMESPACE)
            .ok_or("only refs/remotes/ is supported for fetch destinations")?;
        let dst_branch = dst_without_prefix
            .strip_prefix(remote_name.as_str())
            .and_then(|d| d.strip_prefix("/"))
            .ok_or("remote renaming not supported")?;
        if src_branch != dst_branch {
            return Err("renaming is not supported");
        }
        Ok((FetchRefSpecKind::Positive, branch))
    } else {
        Ok((FetchRefSpecKind::Negative, branch))
    }
}

/// Helper struct to execute multiple `git fetch` operations
pub struct GitFetch<'a> {
    mut_repo: &'a mut MutableRepo,
    git_repo: Box<girt::Repository>,
    transport: GitTransport,
    import_options: &'a GitImportOptions,
    fetched: Vec<FetchedRefs>,
}

impl<'a> GitFetch<'a> {
    pub fn new(
        mut_repo: &'a mut MutableRepo,
        transport_options: GitTransportOptions,
        import_options: &'a GitImportOptions,
    ) -> Result<Self, UnexpectedGitBackendError> {
        let git_backend = get_git_backend(mut_repo.store())?;
        let git_repo = Box::new(git_backend.git_repo());
        let transport = GitTransport::from_git_backend(git_backend, transport_options);
        Ok(GitFetch {
            mut_repo,
            git_repo,
            transport,
            import_options,
            fetched: vec![],
        })
    }

    /// Perform a `git fetch` on the local git repo, updating the
    /// remote-tracking branches in the git repo.
    ///
    /// Keeps track of the {branch_names, remote_name} pair the refs can be
    /// subsequently imported into the `jj` repo by calling `import_refs()`.
    #[tracing::instrument(skip(self, callback))]
    pub fn fetch(
        &mut self,
        remote_name: &RemoteName,
        ExpandedFetchRefSpecs {
            expr,
            refspecs,
            negative_refspecs,
        }: ExpandedFetchRefSpecs,
        callback: &mut dyn GitSubprocessCallback,
        depth: Option<NonZeroU32>,
    ) -> Result<(), GitFetchError> {
        validate_remote_name(remote_name)?;

        // check the remote exists
        if try_find_active_remote_inner(&self.git_repo, remote_name).is_none() {
            return Err(GitFetchError::NoSuchRemote(remote_name.to_owned()));
        }

        if refspecs.is_empty() {
            // Don't fall back to the base refspecs.
            return Ok(());
        }

        // Destinations of exact refspecs whose source is missing on the remote
        // are pruned along with the other stale remote-tracking refs.
        let tag_namespace = format!(
            "{REMOTE_TAG_REF_NAMESPACE}{remote}/",
            remote = remote_name.as_str()
        );
        let outcome = self.transport.fetch(
            remote_name,
            &refspecs,
            &negative_refspecs,
            callback,
            depth,
            &tag_namespace,
        )?;
        for source in &outcome.missing_sources {
            tracing::debug!(source, "failed to fetch ref");
        }
        // The fetch installed new objects and moved refs.
        let git_backend =
            get_git_backend(self.mut_repo.store()).expect("backend type should have been tested");
        *self.git_repo = git_backend.git_repo();
        git_backend.refresh_objects().map_err(|err| {
            GitFetchError::Transport(GitTransportError::Transfer(
                girt::transfer::TransferError::Local(err.into()),
            ))
        })?;

        self.fetched.push(FetchedRefs {
            remote: remote_name.to_owned(),
            bookmark_matcher: expr.bookmark.to_matcher(),
            tag_matcher: expr.tag.to_matcher(),
        });
        Ok(())
    }

    /// Queries remote for the default branch name.
    #[tracing::instrument(skip(self))]
    pub fn get_default_branch(
        &self,
        remote_name: &RemoteName,
    ) -> Result<Option<RefNameBuf>, GitFetchError> {
        if try_find_active_remote_inner(&self.git_repo, remote_name).is_none() {
            return Err(GitFetchError::NoSuchRemote(remote_name.to_owned()));
        }
        let default_branch = self.transport.default_branch(remote_name)?;
        tracing::debug!(?default_branch);
        Ok(default_branch)
    }

    /// Import the previously fetched remote-tracking branches and tags into the
    /// jj repo and update jj's local bookmarks and tags.
    ///
    /// Clears all yet-to-be-imported {branch/tag_names, remote_name} pairs
    /// after the import. If `fetch()` has not been called since the last time
    /// `import_refs()` was called then this will be a no-op.
    #[tracing::instrument(skip(self))]
    pub async fn import_refs(&mut self) -> Result<GitImportStats, GitImportError> {
        tracing::debug!("import_refs");
        let all_remote_tags = true;
        let refs_to_import = diff_refs_to_import(
            self.mut_repo.view(),
            &self.git_repo,
            all_remote_tags,
            |kind, symbol| match kind {
                GitRefKind::Bookmark => self
                    .fetched
                    .iter()
                    .filter(|fetched| fetched.remote == symbol.remote)
                    .any(|fetched| fetched.bookmark_matcher.is_match(symbol.name.as_str())),
                GitRefKind::Tag => self
                    .fetched
                    .iter()
                    .filter(|fetched| fetched.remote == symbol.remote)
                    .any(|fetched| fetched.tag_matcher.is_match(symbol.name.as_str())),
            },
        )?;
        let import_stats =
            import_refs_inner(self.mut_repo, refs_to_import, self.import_options).await?;

        self.fetched.clear();

        Ok(import_stats)
    }
}

#[derive(Error, Debug)]
pub enum GitPushError {
    #[error("No git remote named '{}'", .0.as_symbol())]
    NoSuchRemote(RemoteNameBuf),
    #[error(transparent)]
    RemoteName(#[from] GitRemoteNameError),
    #[error(transparent)]
    Transport(#[from] GitTransportError),
    #[error(transparent)]
    UnexpectedBackend(#[from] UnexpectedGitBackendError),
}

#[derive(Clone, Debug, Default)]
pub struct GitPushRefTargets {
    /// Bookmark or branch `(name, [expected_target, new_target])`s to push.
    pub bookmarks: Vec<(RefNameBuf, Diff<Option<CommitId>>)>,
    /// Tag `(name, [expected_target, new_target])`s to push.
    pub tags: Vec<(RefNameBuf, Diff<Option<CommitId>>)>,
}

pub struct GitRefUpdate {
    pub qualified_name: GitRefNameBuf,
    /// Expected position on the remote and new position to push.
    ///
    /// The expected position is sourced from the local remote-tracking branch.
    /// This should be `None` if we expect the ref to not exist on the remote.
    pub targets: Diff<Option<girt::ObjectId>>,
}

/// Miscellaneous options for Git push command.
#[derive(Clone, Debug, Default)]
pub struct GitPushOptions {
    /// `--push-option` arguments.
    pub remote_push_options: Vec<String>,
}

/// Pushes the specified refs and updates the repo view accordingly.
pub fn push_refs(
    mut_repo: &mut MutableRepo,
    transport_options: GitTransportOptions,
    remote: &RemoteName,
    targets: &GitPushRefTargets,
    callback: &mut dyn GitSubprocessCallback,
    options: &GitPushOptions,
) -> Result<GitPushStats, GitPushError> {
    validate_remote_name(remote)?;

    let git_repo = get_git_repo(mut_repo.store())?;
    let git_refs = GitRefs::new(&git_repo).map_err(|err| {
        GitPushError::Transport(GitTransportError::Transfer(
            girt::transfer::TransferError::Local(err),
        ))
    })?;
    let to_oid = |id: &CommitId| oid_from_commit_id(&git_repo, id);
    let to_tag_target = |name: &RefName, remote: &RemoteName, id: &CommitId| {
        let remote_matcher = StringMatcher::exact(remote);
        let oid = to_oid(id);
        find_git_tag_oid_to_copy(mut_repo.view(), &git_refs, name, &remote_matcher, oid)
            .unwrap_or(oid)
    };
    let ref_updates = itertools::chain(
        targets.bookmarks.iter().map(|(name, update)| GitRefUpdate {
            qualified_name: format!("refs/heads/{name}", name = name.as_str()).into(),
            targets: update.as_ref().map(|id| id.as_ref().map(to_oid)),
        }),
        targets.tags.iter().map(|(name, update)| GitRefUpdate {
            qualified_name: format!("refs/tags/{name}", name = name.as_str()).into(),
            targets: Diff {
                before: update
                    .before
                    .as_ref()
                    .map(|id| to_tag_target(name, remote, id)),
                after: update
                    .after
                    .as_ref()
                    .map(|id| to_tag_target(name, REMOTE_NAME_FOR_LOCAL_GIT_REPO, id)),
            },
        }),
    )
    .collect_vec();

    let push_stats = push_updates(
        mut_repo,
        transport_options,
        remote,
        &ref_updates,
        callback,
        options,
    )?;
    tracing::debug!(?push_stats);

    let pushed: HashSet<&GitRefName> = push_stats.pushed.iter().map(AsRef::as_ref).collect();
    let pushed_bookmark_updates = || {
        iter::zip(&targets.bookmarks, &ref_updates[..targets.bookmarks.len()])
            .filter(|(_, ref_update)| pushed.contains(&*ref_update.qualified_name))
            .map(|((name, update), _)| (&**name, update))
    };
    let pushed_tag_updates = || {
        iter::zip(&targets.tags, &ref_updates[targets.bookmarks.len()..])
            .filter(|(_, ref_update)| pushed.contains(&*ref_update.qualified_name))
            .map(|((name, update), ref_update)| (&**name, update, ref_update))
    };

    // The remote refs in Git should usually be updated by `git push`. In that
    // case, this only updates our record about the last exported state.
    let unexported_bookmarks = {
        let refs = build_pushed_bookmarks_to_export(remote, pushed_bookmark_updates());
        export_refs_to_git(mut_repo, &git_repo, GitRefKind::Bookmark, refs)
    };
    // Update remote tags so we can look up annotated tag oid without fetching.
    // Since remote tags should never be imported without fetching from the
    // remote, update failure isn't a hard error.
    for (name, _, ref_update) in pushed_tag_updates() {
        let symbol = name.to_remote_symbol(remote);
        let edit = to_remote_tag_ref_update(symbol, ref_update.targets.after);
        if let Err(err) = git_refs.transaction(&[edit]) {
            tracing::warn!(?symbol, ?err, "failed to update remote tag ref");
        }
    }

    debug_assert!(unexported_bookmarks.is_sorted_by_key(|(symbol, _)| symbol));
    let is_exported_bookmark = |name: &RefName| {
        unexported_bookmarks
            .binary_search_by_key(&name, |(symbol, _)| &symbol.name)
            .is_err()
    };
    for (name, update) in pushed_bookmark_updates().filter(|(name, _)| is_exported_bookmark(name)) {
        let new_remote_ref = RemoteRef {
            target: RefTarget::resolved(update.after.clone()),
            state: RemoteRefState::Tracked,
        };
        mut_repo.set_remote_bookmark(name.to_remote_symbol(remote), new_remote_ref);
    }
    for (name, update, _) in pushed_tag_updates() {
        let new_remote_ref = RemoteRef {
            target: RefTarget::resolved(update.after.clone()),
            state: RemoteRefState::Tracked,
        };
        mut_repo.set_remote_tag(name.to_remote_symbol(remote), new_remote_ref);
    }

    // TODO: Maybe we can add new stats type which stores RemoteRefSymbol in
    // place of GitRefName, and remove unexported_bookmarks from the original
    // stats type. This will help find pushed bookmarks that failed to export.
    assert!(push_stats.unexported_bookmarks.is_empty());
    let push_stats = GitPushStats {
        pushed: push_stats.pushed,
        rejected: push_stats.rejected,
        remote_rejected: push_stats.remote_rejected,
        unexported_bookmarks,
    };
    Ok(push_stats)
}

/// Pushes the specified Git refs without updating the repo view.
pub fn push_updates(
    repo: &dyn Repo,
    transport_options: GitTransportOptions,
    remote_name: &RemoteName,
    updates: &[GitRefUpdate],
    callback: &mut dyn GitSubprocessCallback,
    options: &GitPushOptions,
) -> Result<GitPushStats, GitPushError> {
    let mut qualified_remote_refs_expected_locations = HashMap::new();
    let mut refspecs = vec![];
    for update in updates {
        qualified_remote_refs_expected_locations.insert(
            update.qualified_name.as_ref(),
            update.targets.before.as_ref(),
        );
        if let Some(new_target) = &update.targets.after {
            // We always force-push. We use the push_negotiation callback in
            // `push_refs` to check that the refs did not unexpectedly move on
            // the remote.
            refspecs.push(RefSpec::forced(
                new_target.to_string(),
                &update.qualified_name,
            ));
        } else {
            // Prefixing this with `+` to force-push or not should make no
            // difference. The push negotiation happens regardless, and wouldn't
            // allow creating a branch if it's not a fast-forward.
            refspecs.push(RefSpec::delete(&update.qualified_name));
        }
    }

    let git_backend = get_git_backend(repo.store())?;
    let git_repo = git_backend.git_repo();
    let transport = GitTransport::from_git_backend(git_backend, transport_options);

    // check the remote exists
    if try_find_active_remote_inner(&git_repo, remote_name).is_none() {
        return Err(GitPushError::NoSuchRemote(remote_name.to_owned()));
    }

    let refs_to_push: Vec<RefToPush> = refspecs
        .iter()
        .map(|full_refspec| RefToPush::new(full_refspec, &qualified_remote_refs_expected_locations))
        .collect();

    let mut push_stats = transport.push(remote_name, &refs_to_push, callback, options)?;
    push_stats.pushed.sort();
    push_stats.rejected.sort();
    push_stats.remote_rejected.sort();
    Ok(push_stats)
}

/// Builds diff of remote bookmarks corresponding to the given `pushed_updates`.
fn build_pushed_bookmarks_to_export<'a>(
    remote: &RemoteName,
    pushed_updates: impl IntoIterator<Item = (&'a RefName, &'a Diff<Option<CommitId>>)>,
) -> RefsToExport {
    let mut to_update = Vec::new();
    let mut to_delete = Vec::new();
    for (name, update) in pushed_updates {
        let symbol = name.to_remote_symbol(remote);
        match (update.before.as_ref(), update.after.as_ref()) {
            (old, Some(new)) => {
                to_update.push((symbol.to_owned(), (old.cloned(), new.clone())));
            }
            (Some(old), None) => {
                to_delete.push((symbol.to_owned(), old.clone()));
            }
            (None, None) => panic!("old/new targets should differ"),
        }
    }

    RefsToExport {
        to_update,
        to_delete,
        failed: vec![],
    }
}

/// Constructs `RefEdit` to update pushed remote tag ref.
fn to_remote_tag_ref_update(
    symbol: RemoteRefSymbol<'_>,
    new_oid: Option<girt::ObjectId>,
) -> girt::refs::RefEdit {
    let name = format!(
        "{REMOTE_TAG_REF_NAMESPACE}{remote}/{name}",
        remote = symbol.remote.as_str(),
        name = symbol.name.as_str()
    );
    girt::refs::RefEdit {
        name: git_ref_name(&name).expect("pushed ref name should be valid"),
        dereference: false,
        target: new_oid.map(girt::refs::Target::Direct),
        // No constraint on existing ref because remote tag ref shouldn't be moved
        // externally, and should always point to the actual remote ref.
        expected: girt::refs::Expected::Any,
        reflog: if new_oid.is_some() {
            girt::refs::Reflog::Preserve
        } else {
            girt::refs::Reflog::Delete
        },
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::revset;
    use crate::revset::RevsetDiagnostics;

    #[test]
    fn test_split_positive_negative_patterns() {
        fn split(text: &str) -> (Vec<StringPattern>, Vec<StringPattern>) {
            try_split(text).unwrap()
        }

        fn try_split(
            text: &str,
        ) -> Result<(Vec<StringPattern>, Vec<StringPattern>), GitRefExpressionError> {
            let mut diagnostics = RevsetDiagnostics::new();
            let expr = revset::parse_string_expression(&mut diagnostics, text).unwrap();
            let (positives, negatives) = split_into_positive_negative_patterns(&expr)?;
            Ok((
                positives.into_iter().cloned().collect(),
                negatives.into_iter().cloned().collect(),
            ))
        }

        insta::assert_compact_debug_snapshot!(
            split("a"),
            @r#"([Exact("a")], [])"#);
        insta::assert_compact_debug_snapshot!(
            split("~a"),
            @r#"([Substring("")], [Exact("a")])"#);
        insta::assert_compact_debug_snapshot!(
            split("~a~b"),
            @r#"([Substring("")], [Exact("a"), Exact("b")])"#);
        insta::assert_compact_debug_snapshot!(
            split("~(a|b)"),
            @r#"([Substring("")], [Exact("a"), Exact("b")])"#);
        insta::assert_compact_debug_snapshot!(
            split("a|b"),
            @r#"([Exact("a"), Exact("b")], [])"#);
        insta::assert_compact_debug_snapshot!(
            split("(a|b)&~c"),
            @r#"([Exact("a"), Exact("b")], [Exact("c")])"#);
        insta::assert_compact_debug_snapshot!(
            split("~a&b"),
            @r#"([Exact("b")], [Exact("a")])"#);
        insta::assert_compact_debug_snapshot!(
            split("a&~b&~c"),
            @r#"([Exact("a")], [Exact("b"), Exact("c")])"#);
        insta::assert_compact_debug_snapshot!(
            split("~a&b&~c"),
            @r#"([Exact("b")], [Exact("a"), Exact("c")])"#);
        insta::assert_compact_debug_snapshot!(
            split("a&~(b|c)"),
            @r#"([Exact("a")], [Exact("b"), Exact("c")])"#);
        insta::assert_compact_debug_snapshot!(
            split("((a|b)|c)&~(d|(e|f))"),
            @r#"([Exact("a"), Exact("b"), Exact("c")], [Exact("d"), Exact("e"), Exact("f")])"#);
        assert_matches!(
            try_split("a&b"),
            Err(GitRefExpressionError::PositiveIntersection)
        );
        assert_matches!(try_split("a|~b"), Err(GitRefExpressionError::NestedNotIn));
        assert_matches!(
            try_split("a&~(b&~c)"),
            Err(GitRefExpressionError::NestedIntersection)
        );
        assert_matches!(
            try_split("(a|b)&c"),
            Err(GitRefExpressionError::PositiveIntersection)
        );
        assert_matches!(
            try_split("(a&~b)&(~c&~d)"),
            Err(GitRefExpressionError::PositiveIntersection)
        );
        assert_matches!(try_split("a&~~b"), Err(GitRefExpressionError::NestedNotIn));
        assert_matches!(
            try_split("a&~b|c&~d"),
            Err(GitRefExpressionError::NestedIntersection)
        );

        // `~*` should generate empty patterns. `a~*` and `~(a|*)` don't because
        // `a` may be incompatible with Git refspecs.
        insta::assert_compact_debug_snapshot!(
            split("*"),
            @r#"([Glob(GlobPattern("*"))], [])"#);
        insta::assert_compact_debug_snapshot!(
            split("~*"),
            @"([], [])");
        insta::assert_compact_debug_snapshot!(
            split("a~*"),
            @r#"([Exact("a")], [Glob(GlobPattern("*"))])"#);
        insta::assert_compact_debug_snapshot!(
            split("~(a|*)"),
            @r#"([Substring("")], [Exact("a"), Glob(GlobPattern("*"))])"#);
    }
}
