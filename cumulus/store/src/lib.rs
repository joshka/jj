//! Shared SQLite+CAS storage engine for Cumulus (spec §6.1).
//!
//! One `Store` manages a per-repo directory containing `meta.sqlite` (WAL
//! mode) and a `blobs/` content-addressed file store. The same engine backs
//! the server (`cumulusd`) and the client-side cache; the client-only tables
//! (`outbox`, `sync_state`) simply stay empty on the server.

#![warn(missing_docs)]

mod blob;
mod error;
mod store;

pub use blob::Blob;
pub use blob::BlobWriter;
pub use blob::FinishedBlob;
pub use error::StoreError;
pub use error::StoreResult;
pub use store::CommitIngest;
pub use store::ObjectKind;
pub use store::OpIngest;
pub use store::Store;
pub use store::hash_bytes;

/// Threshold below which blob bytes are stored inline in SQLite rather than
/// as a CAS file (spec §6.1).
pub const INLINE_BLOB_MAX: u64 = 64 * 1024;

/// Length in bytes of content-addressed object and blob ids (truncated
/// Blake2b-512, spec §5).
pub const OBJECT_ID_LENGTH: usize = 32;
