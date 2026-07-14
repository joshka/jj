//! Mapping from store/convert errors to gRPC status codes (`cumulus/docs/SPEC.md` §9).

use cumulus_proto::convert::ConvertError;
use cumulus_store::StoreError;
use tonic::Status;

/// Maps a [`StoreError`] to the status codes promised in spec §9:
/// push-order and precondition violations are FAILED_PRECONDITION, bad
/// client bytes are INVALID_ARGUMENT, everything else is INTERNAL.
pub(crate) fn store_error_to_status(err: StoreError) -> Status {
    match &err {
        StoreError::MissingCommitParent(_)
        | StoreError::CommitWithoutParents(_)
        | StoreError::MissingOpParent(_)
        | StoreError::MissingView { .. }
        | StoreError::MissingViewHead { .. } => Status::failed_precondition(err.to_string()),
        StoreError::BlobHashMismatch { .. } => Status::invalid_argument(err.to_string()),
        StoreError::Sqlite(_) | StoreError::Io(_) | StoreError::CorruptBlob { .. } => {
            tracing::error!(error = %err, "store error");
            Status::internal(err.to_string())
        }
    }
}

/// Maps a proto decode/conversion failure of client-supplied bytes.
pub(crate) fn convert_error_to_status(err: ConvertError) -> Status {
    Status::invalid_argument(err.to_string())
}

pub(crate) fn decode_error_to_status(what: &str, err: prost::DecodeError) -> Status {
    Status::invalid_argument(format!("invalid {what} proto: {err}"))
}
