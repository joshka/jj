//! Conversions between `cumulus.v1` protos and jj-lib's in-memory model.
//!
//! The commit/tree conversions mirror `lib/src/simple_backend.rs`; the
//! operation/view conversions mirror `lib/src/simple_op_store.rs`, native
//! (non-legacy) forms only. Ids are always computed from the in-memory
//! values via [`crate::ids`], never from encoded proto bytes, so proto
//! encoding details can never change an object's identity.

use std::collections::BTreeMap;

use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::SecureSig;
use jj_lib::backend::Signature;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::merge::Merge;
use jj_lib::merge::MergeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::OperationMetadata;
use jj_lib::op_store::RefTarget;
use jj_lib::op_store::RemoteRef;
use jj_lib::op_store::RemoteRefState;
use jj_lib::op_store::RemoteView;
use jj_lib::op_store::TimestampRange;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use jj_lib::ref_name::GitRefNameBuf;
use jj_lib::ref_name::RefNameBuf;
use jj_lib::ref_name::RemoteNameBuf;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use prost::Message as _;
use thiserror::Error;

use crate::ids::OPERATION_ID_LENGTH;
use crate::ids::VIEW_ID_LENGTH;
use crate::v1;

/// Error decoding a `cumulus.v1` proto into a jj-lib value.
#[derive(Debug, Error)]
pub enum ConvertError {
    /// An id field had the wrong length.
    #[error("invalid hash length (expected {expected} bytes, got {actual} bytes)")]
    InvalidHashLength {
        /// Expected id length.
        expected: usize,
        /// Actual field length.
        actual: usize,
    },
    /// Unknown `RemoteRefState` enum value.
    #[error("invalid remote ref state value {0}")]
    InvalidRemoteRefState(i32),
    /// Ref target terms must alternate positive/negative, so the count is
    /// always odd (or zero for an absent target).
    #[error("invalid number of ref target terms {0}")]
    EvenNumberOfRefTargetTerms(usize),
    /// A tree entry name was not a valid path component.
    #[error("invalid tree entry name {0:?}")]
    InvalidTreeEntryName(String),
    /// A tree entry had no value (unset oneof).
    #[error("missing value for tree entry {0:?}")]
    MissingTreeValue(String),
}

// commits (crib: simple_backend.rs)

/// Converts a commit to its wire/storage proto.
pub fn commit_to_proto(commit: &Commit) -> v1::Commit {
    let mut proto = v1::Commit {
        parents: commit.parents.iter().map(|id| id.to_bytes()).collect(),
        predecessors: commit.predecessors.iter().map(|id| id.to_bytes()).collect(),
        root_tree: commit.root_tree.iter().map(|id| id.to_bytes()).collect(),
        change_id: commit.change_id.to_bytes(),
        description: commit.description.clone(),
        author: Some(signature_to_proto(&commit.author)),
        committer: Some(signature_to_proto(&commit.committer)),
        secure_sig: commit.secure_sig.as_ref().map(|sig| sig.sig.clone()),
        conflict_labels: vec![],
    };
    if !commit.conflict_labels.is_resolved() {
        proto.conflict_labels = commit.conflict_labels.as_slice().to_owned();
    }
    proto
}

/// Converts a commit proto back to the in-memory representation.
pub fn commit_from_proto(mut proto: v1::Commit) -> Commit {
    // The signed payload is the proto encoded without the signature itself;
    // take() clears it before re-encoding (simple_backend crib).
    let secure_sig = proto.secure_sig.take().map(|sig| SecureSig {
        data: proto.encode_to_vec(),
        sig,
    });
    let parents = proto.parents.into_iter().map(CommitId::new).collect();
    let predecessors = proto.predecessors.into_iter().map(CommitId::new).collect();
    let merge_builder: MergeBuilder<_> = proto.root_tree.into_iter().map(TreeId::new).collect();
    let root_tree = merge_builder.build();
    let conflict_labels = ConflictLabels::from_vec(proto.conflict_labels);
    Commit {
        parents,
        predecessors,
        root_tree,
        conflict_labels: conflict_labels.into_merge(),
        change_id: ChangeId::new(proto.change_id),
        description: proto.description,
        author: signature_from_proto(proto.author.unwrap_or_default()),
        committer: signature_from_proto(proto.committer.unwrap_or_default()),
        secure_sig,
    }
}

/// The relations the server indexes at commit ingest, extracted from the
/// same single decode (spec §6.1): parents, and the change id for
/// `commit_changes` (§13.7).
#[derive(Debug)]
pub struct CommitRelations {
    /// Parent commit ids.
    pub parents: Vec<Vec<u8>>,
    /// The commit's change id.
    pub change_id: Vec<u8>,
}

/// Extracts [`CommitRelations`] from a decoded commit proto.
pub fn commit_relations(proto: &v1::Commit) -> CommitRelations {
    CommitRelations {
        parents: proto.parents.clone(),
        change_id: proto.change_id.clone(),
    }
}

fn signature_to_proto(signature: &Signature) -> v1::commit::Signature {
    v1::commit::Signature {
        name: signature.name.clone(),
        email: signature.email.clone(),
        timestamp: Some(v1::commit::Timestamp {
            millis_since_epoch: signature.timestamp.timestamp.0,
            tz_offset: signature.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: v1::commit::Signature) -> Signature {
    let timestamp = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(timestamp.millis_since_epoch),
            tz_offset: timestamp.tz_offset,
        },
    }
}

// trees (crib: simple_backend.rs)

/// Converts a tree to its wire/storage proto.
pub fn tree_to_proto(tree: &Tree) -> v1::Tree {
    v1::Tree {
        entries: tree
            .entries()
            .map(|entry| v1::tree::Entry {
                name: entry.name().as_internal_str().to_owned(),
                value: Some(tree_value_to_proto(entry.value())),
            })
            .collect(),
    }
}

/// Converts a tree proto back to the in-memory representation.
pub fn tree_from_proto(proto: v1::Tree) -> Result<Tree, ConvertError> {
    // Serialized data should be sorted.
    let entries = proto
        .entries
        .into_iter()
        .map(|proto_entry| {
            let value = tree_value_from_proto(
                proto_entry
                    .value
                    .ok_or_else(|| ConvertError::MissingTreeValue(proto_entry.name.clone()))?,
            )?;
            let name = RepoPathComponentBuf::new(&proto_entry.name)
                .map_err(|_| ConvertError::InvalidTreeEntryName(proto_entry.name))?;
            Ok((name, value))
        })
        .collect::<Result<_, ConvertError>>()?;
    Ok(Tree::from_sorted_entries(entries))
}

fn tree_value_to_proto(value: &TreeValue) -> v1::TreeValue {
    let value = match value {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => v1::tree_value::Value::File(v1::tree_value::File {
            id: id.to_bytes(),
            executable: *executable,
            copy_id: copy_id.to_bytes(),
        }),
        TreeValue::Symlink(id) => v1::tree_value::Value::SymlinkId(id.to_bytes()),
        TreeValue::Tree(id) => v1::tree_value::Value::TreeId(id.to_bytes()),
        // Carried opaquely (§5); SimpleBackend panics here, Cumulus must not.
        TreeValue::GitSubmodule(id) => v1::tree_value::Value::SubmoduleId(id.to_bytes()),
    };
    v1::TreeValue { value: Some(value) }
}

fn tree_value_from_proto(proto: v1::TreeValue) -> Result<TreeValue, ConvertError> {
    let value = proto
        .value
        .ok_or_else(|| ConvertError::MissingTreeValue(String::new()))?;
    Ok(match value {
        v1::tree_value::Value::File(file) => TreeValue::File {
            id: FileId::new(file.id),
            executable: file.executable,
            copy_id: CopyId::new(file.copy_id),
        },
        v1::tree_value::Value::SymlinkId(id) => TreeValue::Symlink(SymlinkId::new(id)),
        v1::tree_value::Value::TreeId(id) => TreeValue::Tree(TreeId::new(id)),
        v1::tree_value::Value::SubmoduleId(id) => TreeValue::GitSubmodule(CommitId::new(id)),
    })
}

// operations and views (crib: simple_op_store.rs, native forms only)

/// Converts an operation to its wire/storage proto.
pub fn operation_to_proto(operation: &Operation) -> v1::Operation {
    let (commit_predecessors, stores_commit_predecessors) = match &operation.commit_predecessors {
        Some(map) => (commit_predecessors_map_to_proto(map), true),
        None => (vec![], false),
    };
    v1::Operation {
        view_id: operation.view_id.to_bytes(),
        parents: operation.parents.iter().map(|id| id.to_bytes()).collect(),
        metadata: Some(operation_metadata_to_proto(&operation.metadata)),
        commit_predecessors,
        stores_commit_predecessors,
    }
}

/// Converts an operation proto back to the in-memory representation.
pub fn operation_from_proto(proto: v1::Operation) -> Result<Operation, ConvertError> {
    let parents = proto
        .parents
        .into_iter()
        .map(operation_id_from_proto)
        .collect::<Result<_, _>>()?;
    let view_id = view_id_from_proto(proto.view_id)?;
    let metadata = operation_metadata_from_proto(proto.metadata.unwrap_or_default());
    let commit_predecessors = proto
        .stores_commit_predecessors
        .then(|| commit_predecessors_map_from_proto(proto.commit_predecessors));
    Ok(Operation {
        view_id,
        parents,
        metadata,
        commit_predecessors,
    })
}

/// Converts a view to its wire/storage proto.
pub fn view_to_proto(view: &View) -> v1::View {
    // head_ids is a HashSet; sort for deterministic encoding (ids are
    // computed from the in-memory value, but stable bytes are nice for
    // caching and tests).
    let mut head_ids: Vec<_> = view.head_ids.iter().map(|id| id.to_bytes()).collect();
    head_ids.sort();
    v1::View {
        head_ids,
        local_bookmarks: named_ref_targets_to_proto(&view.local_bookmarks),
        local_tags: named_ref_targets_to_proto(&view.local_tags),
        remote_views: remote_views_to_proto(&view.remote_views),
        git_refs: view
            .git_refs
            .iter()
            .map(|(name, target)| v1::NamedRefTarget {
                name: name.as_str().to_owned(),
                target_terms: ref_target_to_terms(target),
            })
            .collect(),
        git_head: ref_target_to_terms(&view.git_head),
        wc_commit_ids: view
            .wc_commit_ids
            .iter()
            .map(|(name, id)| (name.into(), id.to_bytes()))
            .collect(),
    }
}

/// Converts a view proto back to the in-memory representation.
pub fn view_from_proto(proto: v1::View) -> Result<View, ConvertError> {
    Ok(View {
        head_ids: proto.head_ids.into_iter().map(CommitId::new).collect(),
        local_bookmarks: named_ref_targets_from_proto(proto.local_bookmarks)?,
        local_tags: named_ref_targets_from_proto(proto.local_tags)?,
        remote_views: remote_views_from_proto(proto.remote_views)?,
        git_refs: proto
            .git_refs
            .into_iter()
            .map(|named| {
                let name: GitRefNameBuf = named.name.into();
                Ok((name, ref_target_from_terms(named.target_terms)?))
            })
            .collect::<Result<_, ConvertError>>()?,
        git_head: ref_target_from_terms(proto.git_head)?,
        wc_commit_ids: proto
            .wc_commit_ids
            .into_iter()
            .map(|(name, id)| (WorkspaceNameBuf::from(name), CommitId::new(id)))
            .collect(),
    })
}

/// The view's head commit ids, from a decoded view proto. Used for the
/// shallow `PushOps` precondition (spec §6.2) without a full conversion.
pub fn view_head_ids(proto: &v1::View) -> Vec<Vec<u8>> {
    proto.head_ids.clone()
}

fn operation_id_from_proto(bytes: Vec<u8>) -> Result<OperationId, ConvertError> {
    if bytes.len() != OPERATION_ID_LENGTH {
        Err(ConvertError::InvalidHashLength {
            expected: OPERATION_ID_LENGTH,
            actual: bytes.len(),
        })
    } else {
        Ok(OperationId::new(bytes))
    }
}

fn view_id_from_proto(bytes: Vec<u8>) -> Result<ViewId, ConvertError> {
    if bytes.len() != VIEW_ID_LENGTH {
        Err(ConvertError::InvalidHashLength {
            expected: VIEW_ID_LENGTH,
            actual: bytes.len(),
        })
    } else {
        Ok(ViewId::new(bytes))
    }
}

fn timestamp_to_proto(timestamp: &Timestamp) -> v1::Timestamp {
    v1::Timestamp {
        millis_since_epoch: timestamp.timestamp.0,
        tz_offset: timestamp.tz_offset,
    }
}

fn timestamp_from_proto(proto: v1::Timestamp) -> Timestamp {
    Timestamp {
        timestamp: MillisSinceEpoch(proto.millis_since_epoch),
        tz_offset: proto.tz_offset,
    }
}

fn operation_metadata_to_proto(metadata: &OperationMetadata) -> v1::OperationMetadata {
    v1::OperationMetadata {
        start_time: Some(timestamp_to_proto(&metadata.time.start)),
        end_time: Some(timestamp_to_proto(&metadata.time.end)),
        description: metadata.description.clone(),
        hostname: metadata.hostname.clone(),
        username: metadata.username.clone(),
        is_snapshot: metadata.is_snapshot,
        workspace_name: metadata.workspace_name.clone().map(Into::into),
        attributes: metadata
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

fn operation_metadata_from_proto(proto: v1::OperationMetadata) -> OperationMetadata {
    OperationMetadata {
        time: TimestampRange {
            start: timestamp_from_proto(proto.start_time.unwrap_or_default()),
            end: timestamp_from_proto(proto.end_time.unwrap_or_default()),
        },
        description: proto.description,
        hostname: proto.hostname,
        username: proto.username,
        is_snapshot: proto.is_snapshot,
        workspace_name: proto.workspace_name.map(Into::into),
        attributes: proto.attributes.into_iter().collect(),
    }
}

fn commit_predecessors_map_to_proto(
    map: &BTreeMap<CommitId, Vec<CommitId>>,
) -> Vec<v1::CommitPredecessors> {
    map.iter()
        .map(|(commit_id, predecessor_ids)| v1::CommitPredecessors {
            commit_id: commit_id.to_bytes(),
            predecessor_ids: predecessor_ids.iter().map(|id| id.to_bytes()).collect(),
        })
        .collect()
}

fn commit_predecessors_map_from_proto(
    proto: Vec<v1::CommitPredecessors>,
) -> BTreeMap<CommitId, Vec<CommitId>> {
    proto
        .into_iter()
        .map(|entry| {
            let commit_id = CommitId::new(entry.commit_id);
            let predecessor_ids = entry
                .predecessor_ids
                .into_iter()
                .map(CommitId::new)
                .collect();
            (commit_id, predecessor_ids)
        })
        .collect()
}

fn named_ref_targets_to_proto(refs: &BTreeMap<RefNameBuf, RefTarget>) -> Vec<v1::NamedRefTarget> {
    refs.iter()
        .map(|(name, target)| v1::NamedRefTarget {
            name: name.as_str().to_owned(),
            target_terms: ref_target_to_terms(target),
        })
        .collect()
}

fn named_ref_targets_from_proto(
    proto: Vec<v1::NamedRefTarget>,
) -> Result<BTreeMap<RefNameBuf, RefTarget>, ConvertError> {
    proto
        .into_iter()
        .map(|named| {
            let name: RefNameBuf = named.name.into();
            Ok((name, ref_target_from_terms(named.target_terms)?))
        })
        .collect()
}

fn remote_views_to_proto(
    remote_views: &BTreeMap<RemoteNameBuf, RemoteView>,
) -> Vec<v1::RemoteView> {
    remote_views
        .iter()
        .map(|(name, view)| v1::RemoteView {
            name: name.as_str().to_owned(),
            bookmarks: remote_refs_to_proto(&view.bookmarks),
            tags: remote_refs_to_proto(&view.tags),
        })
        .collect()
}

fn remote_views_from_proto(
    proto: Vec<v1::RemoteView>,
) -> Result<BTreeMap<RemoteNameBuf, RemoteView>, ConvertError> {
    proto
        .into_iter()
        .map(|view_proto| {
            let name: RemoteNameBuf = view_proto.name.into();
            let view = RemoteView {
                bookmarks: remote_refs_from_proto(view_proto.bookmarks)?,
                tags: remote_refs_from_proto(view_proto.tags)?,
            };
            Ok((name, view))
        })
        .collect()
}

fn remote_refs_to_proto(remote_refs: &BTreeMap<RefNameBuf, RemoteRef>) -> Vec<v1::RemoteRef> {
    remote_refs
        .iter()
        .map(|(name, remote_ref)| v1::RemoteRef {
            name: name.as_str().to_owned(),
            target_terms: ref_target_to_terms(&remote_ref.target),
            state: remote_ref_state_to_proto(remote_ref.state),
        })
        .collect()
}

fn remote_refs_from_proto(
    proto: Vec<v1::RemoteRef>,
) -> Result<BTreeMap<RefNameBuf, RemoteRef>, ConvertError> {
    proto
        .into_iter()
        .map(|ref_proto| {
            let name: RefNameBuf = ref_proto.name.into();
            let remote_ref = RemoteRef {
                target: ref_target_from_terms(ref_proto.target_terms)?,
                state: remote_ref_state_from_proto(ref_proto.state)?,
            };
            Ok((name, remote_ref))
        })
        .collect()
}

fn ref_target_to_terms(value: &RefTarget) -> Vec<v1::RefTargetTerm> {
    value
        .as_merge()
        .iter()
        .map(|term| v1::RefTargetTerm {
            value: term.as_ref().map(|id| id.to_bytes()),
        })
        .collect()
}

fn ref_target_from_terms(proto: Vec<v1::RefTargetTerm>) -> Result<RefTarget, ConvertError> {
    if proto.is_empty() {
        // Not produced by ref_target_to_terms (absent encodes as one unset
        // term), but accepted for forward compatibility with omitted fields.
        return Ok(RefTarget::absent());
    }
    let terms: Vec<_> = proto
        .into_iter()
        .map(|term| term.value.map(CommitId::new))
        .collect();
    if terms.len().is_multiple_of(2) {
        Err(ConvertError::EvenNumberOfRefTargetTerms(terms.len()))
    } else {
        Ok(RefTarget::from_merge(Merge::from_vec(terms)))
    }
}

fn remote_ref_state_to_proto(state: RemoteRefState) -> i32 {
    let proto_state = match state {
        RemoteRefState::New => v1::RemoteRefState::New,
        RemoteRefState::Tracked => v1::RemoteRefState::Tracked,
    };
    proto_state as i32
}

fn remote_ref_state_from_proto(proto_value: i32) -> Result<RemoteRefState, ConvertError> {
    let proto_state = proto_value
        .try_into()
        .map_err(|prost::UnknownEnumValue(n)| ConvertError::InvalidRemoteRefState(n))?;
    let state = match proto_state {
        v1::RemoteRefState::New => RemoteRefState::New,
        v1::RemoteRefState::Tracked => RemoteRefState::Tracked,
    };
    Ok(state)
}
