# Fork patches

Log of every touch to a pre-existing upstream file on the `cumulus` branch
(spec §3 diff discipline, goal G8). New code lives in new files under
`cumulus/` and is not listed here. When rebasing onto a new upstream release,
triage conflicts starting from this list.

Base: upstream release tag `v0.43.0` (commit 89f62ede8).

## Cargo.toml (root)

- `[workspace] members`: added `cumulus/backend`, `cumulus/e2e`,
  `cumulus/proto`, `cumulus/server`, `cumulus/store` (reformatted the member
  list to one entry per line).
- `[workspace.dependencies]`: added `rusqlite`, `tokio-stream`, `tonic`,
  `tonic-prost`, `tonic-prost-build`, `tonic-reflection`, and path entries
  `cumulus-backend`, `cumulus-proto`, `cumulus-server`, `cumulus-store`.

All additive; on rebase, re-apply the added lines.

## Cargo.lock

- Regenerated for the new workspace members and their dependencies.

## jj-cli Run 2 wiring

- `cli/src/commands/mod.rs`: registered the additive `cumulus` command family
  and dispatch arm.
- `cli/src/main.rs`: registered the Cumulus backend, op-store, and op-heads
  factories through `CliRunner::add_store_factories`.
- `cli/Cargo.toml`: added the `cumulus-backend` dependency used by the new
  command modules.

## Run 1 crate extensions

- `cumulus/store/src/store.rs`: added the narrow client-side transaction
  helpers required by Run 2 for atomic local view/op outbox writes, local and
  pulled op-head updates, operation-prefix lookup, and logical push-lock
  ownership. The existing server ingest and wire behavior are unchanged.

## Deviations from SPEC.md

Spec §3: pinned source wins on signatures, spec wins on semantics. Recorded
here per the same section.

- §6.1 op-head update: the spec's literal recipe ("insert op; delete every
  existing head that is an ancestor of it; insert as head") would let a
  re-pushed *old* op resurface as a head after newer ops exist, violating the
  same section's "re-push is a no-op" invariant. The implementation therefore
  also skips the head insert when the new op is an ancestor of an existing
  head, keeping `op_heads` = maximal elements of the op DAG.
- §6.2 `GetCommitsReachableFrom` semantics: implemented as the spec's stated
  CTE ("walk from `heads`, stopping at the `have` frontier"), which is a
  superset of the literal "ancestors-of(heads) minus ancestors-of(have)" when
  the DAG has paths around the `have` set. Extra commits re-sent this way are
  harmless (content-addressed, `INSERT OR IGNORE`) and computing the exact
  set-minus would cost O(repo) per sync.
- §6.2 message shapes: `cumulus.v1.View`/`Operation` mirror
  `simple_op_store.proto`'s *native* (non-deprecated) field shapes only; the
  legacy encodings (`wc_commit_id`, `Bookmark.remote_bookmarks`,
  `git_head_legacy`, `RefConflictLegacy`) never occur on the cumulus wire
  because conversions always start from in-memory jj-lib values.
- §5 `TreeValue`: unlike `simple_backend.rs` (which panics), the cumulus
  proto round-trips `GitSubmodule` opaquely via a dedicated oneof field, as
  required by §5 losslessness.
- Operation/View id lengths: unspecified in §5 (which only fixes commit-family
  ids at 32 bytes and change ids at 16); cumulus uses full 64-byte
  Blake2b-512, matching `SimpleOpStore`. The server enforces this at `PushOps`
  ingest.
- §7.3 detached-pusher lock: the push lock is claimed atomically in a short
  `BEGIN IMMEDIATE` transaction, then represented by its owned `sync_state`
  row while network work runs. Holding the SQLite write transaction across
  network calls would block foreground local writes and violate G5. Release
  deletes the row only when its owner token still matches.
- §3 CLI diff boundary: the top-level `cumulus` command is hidden from the
  parent command listing, while `jj cumulus --help` remains available. This
  preserves the required three-file jj-cli diff without changing the
  generated upstream CLI-reference snapshot.
