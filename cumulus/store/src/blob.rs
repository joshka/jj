use std::fs::File;
use std::io::Read as _;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;

use blake2::Blake2b512;
use digest::Digest as _;
use tempfile::NamedTempFile;

use crate::INLINE_BLOB_MAX;
use crate::OBJECT_ID_LENGTH;
use crate::error::StoreError;
use crate::error::StoreResult;
use crate::error::hex;

/// The bytes of a stored blob, either inline or in a CAS file.
#[derive(Debug)]
pub enum Blob {
    /// Small blob stored inline in SQLite.
    Inline(Vec<u8>),
    /// Large blob stored as a CAS file; open/stream it from this path.
    File {
        /// Absolute path of the CAS file.
        path: PathBuf,
        /// Size in bytes, as recorded at ingest.
        size: u64,
    },
}

impl Blob {
    /// Size of the blob in bytes.
    pub fn size(&self) -> u64 {
        match self {
            Self::Inline(data) => data.len() as u64,
            Self::File { size, .. } => *size,
        }
    }

    /// Reads the full blob into memory. Prefer streaming from
    /// [`Blob::File`]'s path for large blobs.
    pub fn read_all(self) -> StoreResult<Vec<u8>> {
        match self {
            Self::Inline(data) => Ok(data),
            Self::File { path, .. } => {
                let mut buf = vec![];
                File::open(path)?.read_to_end(&mut buf)?;
                Ok(buf)
            }
        }
    }
}

/// Two-level fan-out path for a CAS file: `blobs/ab/cdef…`.
pub(crate) fn cas_path(blobs_dir: &Path, id: &[u8]) -> PathBuf {
    let hex = hex::encode(id);
    blobs_dir.join(&hex[..2]).join(&hex[2..])
}

/// Incremental writer for blob bytes of unbounded size.
///
/// Bytes are hashed and spooled to a temp file inside the blobs directory in
/// constant memory. [`BlobWriter::finish`] verifies the hash (if an expected
/// id was provided) and either persists the temp file into the CAS
/// (tempfile + atomic rename) or, for blobs under [`INLINE_BLOB_MAX`],
/// returns the bytes for inline storage. Registering the blob row in SQLite
/// is done by [`crate::Store::finish_blob`].
#[derive(Debug)]
pub struct BlobWriter {
    blobs_dir: PathBuf,
    temp_file: NamedTempFile,
    hasher: Blake2b512,
    size: u64,
}

/// Outcome of [`BlobWriter::finish`], to be registered with
/// [`crate::Store::finish_blob`].
#[derive(Debug)]
pub struct FinishedBlob {
    pub(crate) id: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) inline_data: Option<Vec<u8>>,
}

impl FinishedBlob {
    /// The content hash of the written bytes.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// The total number of bytes written.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl BlobWriter {
    pub(crate) fn new(blobs_dir: &Path) -> StoreResult<Self> {
        let temp_file = NamedTempFile::new_in(blobs_dir)?;
        Ok(Self {
            blobs_dir: blobs_dir.to_path_buf(),
            temp_file,
            hasher: Blake2b512::new(),
            size: 0,
        })
    }

    /// Appends bytes to the blob.
    pub fn write(&mut self, bytes: &[u8]) -> StoreResult<()> {
        self.temp_file.as_file_mut().write_all(bytes)?;
        self.hasher.update(bytes);
        self.size += bytes.len() as u64;
        Ok(())
    }

    /// Hashes, verifies, and moves the bytes into their final location.
    ///
    /// If `expected_id` is given (a client-supplied id), a mismatch fails
    /// without touching the CAS.
    pub fn finish(mut self, expected_id: Option<&[u8]>) -> StoreResult<FinishedBlob> {
        let id = self.hasher.finalize()[..OBJECT_ID_LENGTH].to_vec();
        if let Some(expected) = expected_id
            && expected != id
        {
            return Err(StoreError::BlobHashMismatch {
                expected: expected.to_vec(),
                actual: id,
            });
        }
        if self.size < INLINE_BLOB_MAX {
            let mut buf = Vec::with_capacity(self.size as usize);
            let mut file = self.temp_file.reopen()?;
            file.read_to_end(&mut buf)?;
            Ok(FinishedBlob {
                id,
                size: self.size,
                inline_data: Some(buf),
            })
        } else {
            let path = cas_path(&self.blobs_dir, &id);
            std::fs::create_dir_all(path.parent().unwrap())?;
            self.temp_file.as_file_mut().flush()?;
            // Content-addressed: a concurrent writer of the same blob wrote
            // the same bytes, so either file is fine to keep.
            match self.temp_file.persist(&path) {
                Ok(_) => {}
                Err(err) if path.is_file() => drop(err),
                Err(err) => return Err(err.error.into()),
            }
            Ok(FinishedBlob {
                id,
                size: self.size,
                inline_data: None,
            })
        }
    }
}
