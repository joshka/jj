# Fork patches

Log of every touch to a pre-existing upstream file on the `cumulus` branch
(spec §3 diff discipline, goal G8). New code lives in new files under
`cumulus/` and is not listed here. When rebasing onto a new upstream release,
triage conflicts starting from this list.

Base: upstream release tag `v0.43.0` (commit 89f62ede8).

## Cargo.toml (root)

- `[workspace] members`: added `cumulus/backend`, `cumulus/proto`,
  `cumulus/server`, `cumulus/store` (reformatted the member list to one entry
  per line).
- `[workspace.dependencies]`: added `rusqlite`, `tokio-stream`, `tonic`,
  `tonic-prost`, `tonic-prost-build`, `tonic-reflection`, and path entries
  `cumulus-backend`, `cumulus-proto`, `cumulus-store`.

All additive; on rebase, re-apply the added lines.

## Cargo.lock

- Regenerated for the new workspace members and their dependencies.

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
