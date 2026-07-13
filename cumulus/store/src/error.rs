use thiserror::Error;

/// Error type for [`crate::Store`] operations.
#[derive(Debug, Error)]
pub enum StoreError {
    /// SQLite-level failure.
    #[error("sqlite error")]
    Sqlite(#[from] rusqlite::Error),
    /// Filesystem-level failure (CAS files, temp files).
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    /// A commit was ingested before one of its parents (spec §6.1
    /// parent-before-child invariant). Maps to FAILED_PRECONDITION.
    #[error("missing parent commit {}", hex::encode(.0))]
    MissingCommitParent(Vec<u8>),
    /// A commit with no parents was ingested; only the (unstored) root commit
    /// has no parents.
    #[error("commit {} has no parents", hex::encode(.0))]
    CommitWithoutParents(Vec<u8>),
    /// An operation was ingested before one of its parents. Maps to
    /// FAILED_PRECONDITION.
    #[error("missing parent operation {}", hex::encode(.0))]
    MissingOpParent(Vec<u8>),
    /// An operation referenced a view that is neither stored nor part of the
    /// same ingest batch. Maps to FAILED_PRECONDITION.
    #[error("missing view {} for operation {}", hex::encode(.view_id), hex::encode(.op_id))]
    MissingView {
        /// The operation being ingested.
        op_id: Vec<u8>,
        /// The view it references.
        view_id: Vec<u8>,
    },
    /// An operation's view referenced a head commit that is not stored. Maps
    /// to FAILED_PRECONDITION (spec §6.2 PushOps precondition).
    #[error("missing head commit {} for operation {}", hex::encode(.commit_id), hex::encode(.op_id))]
    MissingViewHead {
        /// The operation being ingested.
        op_id: Vec<u8>,
        /// The missing head commit.
        commit_id: Vec<u8>,
    },
    /// Blob bytes did not hash to the id they were stored under.
    #[error(
        "blob hash mismatch: expected {}, got {}",
        hex::encode(.expected),
        hex::encode(.actual)
    )]
    BlobHashMismatch {
        /// The id the writer promised.
        expected: Vec<u8>,
        /// The hash of the bytes actually received.
        actual: Vec<u8>,
    },
    /// A blob row exists but is malformed (unknown storage discriminant,
    /// missing inline data, or missing CAS file).
    #[error("corrupt blob {}: {reason}", hex::encode(.id))]
    CorruptBlob {
        /// The blob id.
        id: Vec<u8>,
        /// What was wrong with it.
        reason: String,
    },
}

/// A specialized [`Result`] type for [`crate::Store`] operations.
pub type StoreResult<T> = Result<T, StoreError>;

/// Minimal hex encoding for error messages (avoids a dependency).
pub(crate) mod hex {
    use std::fmt::Write as _;

    pub(crate) fn encode(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut s, b| {
            write!(s, "{b:02x}").unwrap();
            s
        })
    }
}
