//! gRPC service implementations (`cumulus/docs/SPEC.md` §6.2).

pub(crate) mod admin;
pub(crate) mod index;
pub(crate) mod object;
pub(crate) mod op;
pub(crate) mod repo;

use cumulus_proto::MAX_CHUNK_BYTES;
use cumulus_proto::MAX_OBJECTS_PER_CHUNK;
use cumulus_proto::v1;
use prost::Message as _;

/// Packs objects into `ObjectChunk`s within the spec §6.2 batch limits
/// (10k objects / 32 MiB per message).
pub(crate) fn chunk_objects(objects: Vec<v1::Object>) -> Vec<v1::ObjectChunk> {
    let mut chunks = vec![];
    let mut current = vec![];
    let mut current_bytes = 0;
    for object in objects {
        let encoded_len = object.encoded_len();
        if !current.is_empty()
            && (current.len() >= MAX_OBJECTS_PER_CHUNK
                || current_bytes + encoded_len > MAX_CHUNK_BYTES)
        {
            chunks.push(v1::ObjectChunk {
                repo: String::new(),
                objects: std::mem::take(&mut current),
            });
            current_bytes = 0;
        }
        current_bytes += encoded_len;
        current.push(object);
    }
    if !current.is_empty() {
        chunks.push(v1::ObjectChunk {
            repo: String::new(),
            objects: current,
        });
    }
    chunks
}
