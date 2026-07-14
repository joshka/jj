//! Content-addressed id computation (`cumulus/docs/SPEC.md` §5).
//!
//! Commit/tree/file/symlink ids are the first 32 bytes of a Blake2b-512
//! hash: commits and trees via jj-lib's `ContentHash` canonical form (the
//! SimpleBackend scheme, truncated per the TestBackend precedent), files
//! and symlinks over the raw bytes. Change ids are 16 bytes (jj
//! convention). Operation/view ids stay full-length 64 bytes, matching
//! `SimpleOpStore`.

use blake2::Blake2b512;
use digest::Digest as _;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::FileId;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;

/// Length of commit/tree/file/symlink ids in bytes.
pub const COMMIT_ID_LENGTH: usize = 32;
/// Length of change ids in bytes.
pub const CHANGE_ID_LENGTH: usize = 16;
/// Length of operation ids in bytes.
pub const OPERATION_ID_LENGTH: usize = 64;
/// Length of view ids in bytes.
pub const VIEW_ID_LENGTH: usize = 64;

/// The root commit id: 32 zero bytes.
pub fn root_commit_id() -> CommitId {
    CommitId::from_bytes(&[0; COMMIT_ID_LENGTH])
}

/// The root change id: 16 zero bytes.
pub fn root_change_id() -> jj_lib::backend::ChangeId {
    jj_lib::backend::ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH])
}

/// Computes the id of a commit.
pub fn commit_id(commit: &Commit) -> CommitId {
    CommitId::new(blake2b_hash(commit)[..COMMIT_ID_LENGTH].to_vec())
}

/// Computes the id of a tree.
pub fn tree_id(tree: &Tree) -> TreeId {
    TreeId::new(blake2b_hash(tree)[..COMMIT_ID_LENGTH].to_vec())
}

/// The id of the empty tree, computed at repo-create time and recorded in
/// the server's repo info (spec §5).
pub fn empty_tree_id() -> TreeId {
    tree_id(&Tree::default())
}

/// Computes the id of a file blob from its raw bytes.
pub fn file_id(data: &[u8]) -> FileId {
    FileId::new(hash_raw(data))
}

/// Computes the id of a symlink from its target.
pub fn symlink_id(target: &str) -> SymlinkId {
    SymlinkId::new(hash_raw(target.as_bytes()))
}

/// Computes the id of an operation (full-length, `SimpleOpStore` parity).
pub fn operation_id(operation: &Operation) -> OperationId {
    OperationId::new(blake2b_hash(operation).to_vec())
}

/// Computes the id of a view (full-length, `SimpleOpStore` parity).
pub fn view_id(view: &View) -> ViewId {
    ViewId::new(blake2b_hash(view).to_vec())
}

fn hash_raw(data: &[u8]) -> Vec<u8> {
    Blake2b512::digest(data)[..COMMIT_ID_LENGTH].to_vec()
}
