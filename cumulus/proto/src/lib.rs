//! Cumulus wire protocol and jj-lib conversions (`cumulus/docs/SPEC.md` §5, §6.2).
//!
//! The `v1` module contains the prost/tonic-generated types for the
//! `cumulus.v1` proto package. [`convert`] maps between those and jj-lib's
//! in-memory model; [`ids`] computes the content-addressed ids (§5).

#![warn(missing_docs)]

pub mod convert;
pub mod ids;

/// Generated types and service stubs for the `cumulus.v1` proto package.
#[expect(missing_docs)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/cumulus.v1.rs"));
}

/// Encoded `FileDescriptorSet` of the `cumulus.v1` package, for gRPC server
/// reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/cumulus_descriptor.bin"));

/// The protocol version served and expected by this build (spec §5 repo
/// info).
pub const PROTOCOL_VERSION: u32 = 1;

/// Metadata batch limit: maximum objects per `ObjectChunk` (spec §6.2).
pub const MAX_OBJECTS_PER_CHUNK: usize = 10_000;

/// Metadata batch limit: maximum encoded bytes per `ObjectChunk` (spec
/// §6.2).
pub const MAX_CHUNK_BYTES: usize = 32 * 1024 * 1024;

/// Size of `BlobFrame`/`PutBlobFrame` data payloads (spec §6.2).
pub const BLOB_FRAME_BYTES: usize = 1024 * 1024;
