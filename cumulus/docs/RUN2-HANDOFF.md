# Run 2 handoff notes

Historical wire-contract and reuse notes supplied by Run 1 on 2026-07-13. Run 2
is complete; this document remains the authoritative explanation of the Run 1
interfaces it consumed. See [`IMPLEMENTATION.md`](IMPLEMENTATION.md) for the
result and [`FORK_PATCHES.md`](FORK_PATCHES.md) for deviations.

## Reuse from cumulus-store

- `Store`: open per `.jj/repo/store/cumulus/` dir for the client cache — same
  engine as the server.
- `ObjectKind` numeric values == the wire enum, so the outbox `kind` column can
  store them directly.
- `BlobWriter` for GetBlob write-through (streaming, hash-verify-before-persist).
- `outbox_*` / `sync_state_*` accessors already exist.
- `Store` is Send+Sync behind `Arc` with an internal connection Mutex — wrap
  calls in the backend runtime's spawn_blocking equivalents as needed.

## Reuse from cumulus-proto

- `convert::{commit,tree,view,operation}_{to,from}_proto`.
- `convert::commit_relations` + `view_head_ids` — for client-side ingest and
  push negotiation.
- `ids::*` — the backend's `write_*` id computation MUST use these exactly;
  golden tests pin them (empty-tree id matches SimpleBackend's upstream
  constant).
- Constants: `PROTOCOL_VERSION`, `MAX_OBJECTS_PER_CHUNK`, `MAX_CHUNK_BYTES`,
  `BLOB_FRAME_BYTES`.

## Wire contracts Run 2 must match

- `PutObjects`: first chunk carries `repo`.
- `PutBlob`: frame 0 is the header (empty header id = skip verification, but
  always send the FileId).
- `GetOps`: always returns `view_data`.
- `PushOps`: ops oldest-first, commits already uploaded. Server checks: view
  head-commit presence, op-id/view-id content hashes, op-proto view id ==
  wire `view_id`.
- Errors: NOT_FOUND / FAILED_PRECONDITION / UNAUTHENTICATED / INVALID_ARGUMENT
  per SPEC.md §9.
- `GetObjects` is collect-then-stream server-side: a missing object fails the
  RPC before any chunk arrives — client retry logic can treat it as unary-like.
- Reachability (`GetCommitsReachableFrom`) is frontier-stopping, not exact
  set-minus: it may resend commits the client already has when history routes
  around the `have` frontier. Ingest is idempotent — pull logic must tolerate
  duplicates.
- Op/view ids are fixed at 64 bytes (SimpleOpStore parity); server enforces at
  PushOps.
