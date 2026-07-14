# Cumulus: a native local + remote backend for Jujutsu (in-tree)

**Status:** implementation spec, v2 — **Run 1 and Run 2 (§11 steps 0–7) are
complete** on the `cumulus` bookmark at jj v0.43.0. See
[`IMPLEMENTATION.md`](IMPLEMENTATION.md) for delivered behavior, verification,
findings, and remaining production work. [`RUN2-HANDOFF.md`](RUN2-HANDOFF.md)
preserves the Run 1 wire contract, and [`FORK_PATCHES.md`](FORK_PATCHES.md) records
the fork surface and implementation deviations.

**Dual-track rule (read first):** several subsystems are specified twice — an
**IMPLEMENT (v1)** design and a **SPEC-ONLY** alternative/upgrade. SPEC-ONLY sections
are complete normative designs kept so v1 shapes never block them, and so a later
run can implement them without redesign. An implementing agent MUST NOT write code
for sections marked `SPEC-ONLY — DO NOT IMPLEMENT`, but MUST honor their stated
schema/wire invariants in v1 code.

---

## 0. One-paragraph summary

Cumulus gives jj a **native (non-git) storage backend**, developed **in-tree in a
fork of jj**, so the user runs plain `jj` commands (their fork build) with no extra
binary. Objects live on a **remote server** (`cumulusd`, tonic/gRPC + SQLite + CAS
blob store), fetched **lazily** and cached locally. Commits and op-log metadata sync
eagerly (small); trees/files/symlinks fetch on demand — cloning a huge repo
transfers the history graph plus only the files you materialize. The op log is
local-first with auto-push (Mode A, implemented); a remote-first mode (Mode B, the
Google/commit-cloud model) is fully specified behind the same protocol. No git
anywhere.

## 0.5 Why this exists

**Why a native backend at all.** jj was designed backend-first (its Google ancestor
stored commits in a cloud service; git was always just one implementation), but
every usable jj setup today rides the git backend — and git's format imposes
structural ceilings that no amount of layering removes:

- **Git's on-disk format is its network format.** Packfiles can't be randomly
  seeked, can't be streamed in constant space (delta chains), and must be
  constructed on the fly per fetch — so clones are never free and laziness can't
  be retrofitted (jj's own partial-clone support is blocked on exactly this,
  issue #8920). A native store splits storage format from wire format on purpose:
  content-addressed objects with O(1) random access, streamed in constant space.
- **jj's model doesn't fit in git.** Change-ids, evolution/predecessors, and
  first-class conflicts are smuggled into git via extra-headers, a side
  `TableStore`, and the `jj:trees`/`JJ-CONFLICT-README` encodings (~2,400 lines of
  `git_backend.rs`, most of it impedance-matching). Natively they're just fields
  (~600 lines total, `simple_backend.rs` proves it).
- **Large files and huge trees** are bolt-ons in git (LFS, partial clone) and
  first-class here: lazy blob fetch now, CDC dedup as a designed upgrade (§13.1).

**Why lazy + remote is the payoff.** The one capability that justifies leaving the
git ecosystem is decoupling *repo size* from *local disk and wall-clock*: clone
transfers the history graph (small) and only the files you materialize;
`O(working-set)`, not `O(repo)`. On top of the same sync machinery, the op-log
gives commit-cloud semantics — multi-device continuity (same log, same undo
history everywhere) and, because conflicts are data, even conflicted
work-in-progress syncs losslessly. This whole shape is proven at scale
(Meta's Sapling/Mononoke/EdenFS; Google's jj-on-Piper commit cloud, where `.jj` is
a few pointer files and everything else is a service) — but no public/open jj
implementation of it exists.

**Why it doesn't exist yet, and why now.** Every shipping jj-adjacent forge
(juju.bi, revset.dev) is git-backed and explicitly waiting on an "official" native
backend; the serious native efforts are internal (Google/ERSC) or pre-product
(r2rn). The historical blockers were never the `Backend` trait — they were the
surrounding assumptions: the default index assumes a local fully-materialized DAG,
and the stores assume filesystem atomicity. Those walls have recently moved: the
index traits became fallible specifically to permit networked implementations
(PR #7790→#7799, landed), and index-storage extraction is charted (#9805). v1
deliberately routes *around* the remaining wall (stock local index + eager
commit-metadata backfill — correct, bounded, ~proven by thoughtpolice's qq-rpc
experiment) rather than through it, while §13.2 records the in-fork path through
it for v2.

**Why these constraints.** *In-tree fork:* this is a daily-driver tool — it must be
`jj`, not a second binary; and being in-tree converts the hardest v2 item (remote
index) from "reimplement the revset engine" into "extract a storage trait" —
refactor, not rewrite. *Offline-first as a hard invariant (G5):* Google's
remote-first model assumes a ~1ms always-on network; this targets a laptop and an
internet server, so availability coupling is unacceptable — the server must never
be able to make `jj new` slow. *No git interop at all:* git-as-interchange is
precisely what caps a native backend's upside (everything must round-trip through
git's model at the boundary); this project tests the native upside directly.
Normal git repos are unaffected — the fork keeps the git backend, and this backend
only engages in repos initialized with it.

**What v1 is testing.** The hypothesis: a native lazy backend gives instant clones
and full jj workflows on repos git handles badly (size, binaries, monorepo
subsets), plus multi-device commit-cloud sync, at personal/small-team scale — with
acceptable implementation cost (~10k LOC against stable trait seams). If that
holds, the SPEC-ONLY designs (remote index, CDC, Mode B) are the growth path; if
it doesn't, the failure will be informative about exactly which assumption broke.

**Implementation result.** The completed acceptance suite validates native lazy
content, offline-first writes, Mode A synchronization, and jj workflow fidelity at
the experiment's tested scale. It also narrows the likely product fit: full op-log
sync is most appropriate for trusted devices and agent workspaces, while team and
GitHub collaboration need a refs-and-published-evolution boundary. See
[`IMPLEMENTATION.md`](IMPLEMENTATION.md) for evidence and remaining gaps.

**Resolution rule for implementers:** when the spec is ambiguous, resolve toward
the invariants this section implies — laziness (never fetch what wasn't asked
for), offline-first (never block a command on the network), losslessness (never
drop jj model data), and content-addressing (ids are permanent, storage is
disposable).

## 1. Goals / non-goals

### Goals (v1, must all work)

- G1: Normal jj workflows (`new`, `describe`, `squash`, `rebase`, `log`, `diff`,
  `st`, bookmarks, `undo`) work unmodified on a cumulus-backed repo, via the fork's
  `jj` binary. New commands: `jj cumulus init|clone|sync`.
- G2: Lazy object fetch: after clone, local blob bytes ≈ commit metadata +
  materialized working-copy files. Trees/files fetched on first access, cached
  forever (content-addressed → immutable cache).
- G3: Sparse clone: `jj cumulus clone --sparse '<fileset>'` materializes and fetches
  only the matching subtree.
- G4: Op-log sync: two clients on one repo; each sees the other's commits, bookmarks,
  and full evolog after `jj cumulus sync`; concurrent edits converge via jj's
  existing op-head merge (no server-side conflict logic).
- G5: Offline-first: cached reads work offline; writes always succeed (local-first)
  and queue; clear errors on uncached reads naming `jj cumulus sync`. **Invariant:
  no command's critical path ever waits on the network — command latency is
  identical whether the server is up, down, or hanging.** Only explicit
  `jj cumulus sync` and lazy reads of uncached objects touch the network.
- G6: Multi-repo server, bearer-token auth, single static `cumulusd` binary, one
  TOML config.
- G7: Deterministic, content-addressed everything → all RPCs idempotent/retry-safe.
- G8: Fork hygiene: upstream diff is additive-only where possible; every
  non-additive touch is logged in [`FORK_PATCHES.md`](FORK_PATCHES.md) (rebase
  survival).

### Non-goals for v1 implementation (each has a full SPEC-ONLY design herein or a design note)

- Remote-first op mode (Mode B) — §8.2 (SPEC-ONLY).
- Remote/lazy index — §13.2 (SPEC-ONLY). v1 uses jj's default local index; the
  server maintains DAG tables that later serve it.
- CDC chunking for large blobs — §13.1 (SPEC-ONLY). v1 schema + wire format are
  CDC-shaped.
- Multi-remote / peer-to-peer (DVCS) mode — §13.6 (SPEC-ONLY). v1 topology is
  single-remote hub-and-spoke, but the protocol is peer-symmetric by design and v1
  carries three cheap door-openers (named-remote config, per-remote sync state,
  reserved completeness field).
- FUSE/NFS virtual filesystem (§13.4 note), GC enforcement (§13.5 note), per-repo
  ACLs, web UI, git interop of any kind, submodule semantics, copy tracking
  (`read_copy`/`write_copy`/`get_related_copies` → `Unsupported`, matching jj's git
  and simple backends), watchman integration.

## 2. Locked decisions

| Decision | Choice |
| --- | --- |
| Delivery | **In-tree fork of jj** (see §3). User's daily `jj` = fork build. Out-of-tree packaging: rejected (separate-binary UX). |
| Transport | tonic + prost (gRPC/h2). Plaintext default; TLS via rustls if configured. |
| Serialization | Own `.proto`s, field-shapes mirroring jj's `simple_store.proto` / `simple_op_store.proto` for mechanical conversion. |
| Index | v1: stock `DefaultIndexStore`, correct via eager commit-metadata backfill on pull. Server keeps commit-DAG tables from day one. Remote index: §13.2 SPEC-ONLY. |
| Op model | v1: Mode A local-first (§8.1) with **detached background auto-push** — commits never block on the network (G5 invariant). Mode B remote-first: §8.2 SPEC-ONLY, selectable later via `cumulus.op-mode`. |
| Server storage | Per-repo SQLite (WAL) + CAS blob dir. Blob indirection column reserves CDC (§13.1). |
| Hashing | jj-lib `content_hash::blake2b_hash` (Blake2b-512) truncated to 32 bytes for CommitId/TreeId/FileId/SymlinkId (TestBackend truncation precedent). ChangeId = 16 bytes, jj convention. |
| Client cache | Same storage engine as server (shared crate) + outbox + sync-state. Disposable except outbox. |
| Working copy | jj's standard local working copy + sparse filesets. |
| Naming | Backend name string `"cumulus"`. New workspace crates under `cumulus/`. Server binary `cumulusd`. Commands `jj cumulus …`. Config keys `cumulus.*`. |

## 3. Fork & pinning policy (critical for one-shot success)

Work happens on a bookmark (`cumulus`) of the jj repo, based on a **pinned upstream
release tag** (latest at implementation time). The signature authority is **the
pinned checkout itself** — this spec names traits and semantics; exact signatures
come from the tree the agent is editing.

Diff discipline (G8):

- New code lives in new workspace crates: `cumulus/proto`, `cumulus/store`,
  `cumulus/server`, `cumulus/backend` (workspace members added to root `Cargo.toml`).
- `jj-lib` is **untouched** in v1 (everything needed is already public: traits,
  `StoreFactories`, `Workspace::init`, protos patterns). If a seam turns out to be
  missing, patch minimally and record in [`FORK_PATCHES.md`](FORK_PATCHES.md) with
  a one-line rationale and an upstreamability note.
- `jj-cli` touches are exactly three, all small:
  1. `cli/src/commands/mod.rs`: register a `cumulus` subcommand module
     (pattern: the existing `git` command family, `cli/src/commands/git/`).
  2. Store-factory registration: add cumulus backend/op-store/op-heads factories
     where the CLI builds its `StoreFactories` (follow how default factories flow
     through `CliRunner`; prefer `CliRunner::add_store_factories` from `main.rs` if
     that avoids editing `cli_util.rs`).
  3. `cli/Cargo.toml`: depend on `cumulus-backend`.
- [`FORK_PATCHES.md`](FORK_PATCHES.md) lists every upstream-file touch (file, hunk
  purpose). Rebase procedure: rebase onto each upstream release; conflicts are
  triaged from this file.

Crib sheet inside the same tree (read before coding):

- `lib/src/simple_backend.rs` — **primary crib**: root-commit synthesis
  (`make_root_commit`), signing flow in `write_commit`, proto conversion style,
  `empty_tree_id` bootstrap, temp-file persistence.
- `lib/src/backend.rs` — `Backend` trait (async via `#[async_trait]`), `Commit`,
  `Tree`, `TreeValue`, `Merge<TreeId>` root trees, `BackendError` variants.
- `lib/src/simple_op_store.rs`, `op_store.rs`, `op_heads_store.rs` — `OpStore`,
  `View`/`Operation` model, `OpHeadsStore` trait + locking shape.
- `lib/src/repo.rs` — `StoreFactories`, `BackendInitializer`, `read_store_type`.
- `lib/src/workspace.rs` — `Workspace::init_*` with custom initializers.
- `cli/examples/custom-backend/main.rs`, `custom-command/main.rs`,
  `custom-working-copy/main.rs` — wiring patterns.
- `lib/src/protos/simple_store.proto`, `simple_op_store.proto` — message shapes to
  mirror.

Known-good facts to verify against the pinned tree (all confirmed mid-2026):

- `Backend`: `Any + Send + Sync + Debug`, ~17 methods; backends synthesize the root
  commit on read and reject writing it; `write_commit` takes
  `Option<&mut SigningFn>`; `get_copy_records` → empty `BoxStream`; copy methods →
  `Unsupported`.
- Index traits are fallible (`IndexResult<T>`; PR #7799 lineage);
  `ReadonlyIndex::start_modification()` intentionally infallible — irrelevant to v1
  (we don't implement Index), relevant to §13.2.
- `Store` handles conflicts/caching above the backend; the backend stores
  `Merge<TreeId>` root trees verbatim and resolves nothing.

## 4. Architecture

```text
┌────────────────────── client (fork-built `jj`) ─────────────────────┐
│  jj CLI  + commands/cumulus (init/clone/sync)                       │
│    ├─ CumulusBackend        impl Backend    ──┐                     │
│    ├─ CumulusOpStore        impl OpStore    ──┼── cumulus-store     │
│    ├─ CumulusOpHeadsStore   impl OpHeadsStore─┘   (SQLite+CAS cache,│
│    ├─ DefaultIndexStore (stock, unchanged)         outbox, syncpts) │
│    └─ SyncEngine (Mode A push/pull; §8.1)                           │
│              │ tonic Channel (connect_lazy, bearer token)           │
└──────────────┼──────────────────────────────────────────────────────┘
               ▼  gRPC / h2
┌───────────────────────────── cumulusd ──────────────────────────────┐
│  RepoService · ObjectService · OpService · AdminService             │
│  (IndexService + Mode-B RPCs: wired, return UNIMPLEMENTED in v1)    │
│  per-repo: <data>/repos/<name>/{meta.sqlite, blobs/}                │
│  commit-DAG tables (parents, generation) ← sync negotiation now,    │
│                                            remote index later       │
└──────────────────────────────────────────────────────────────────────┘
```

In-tree layout (additions to the jj workspace):

```text
jj/                          # fork, bookmark `cumulus`, based on pinned release tag
  cumulus/
    README.md                # project entry point and quickstart
    docs/                    # spec, implementation status, handoff, fork-patch log
    proto/                   # .proto + tonic-build; conversions to/from jj-lib types
    store/                   # shared SQLite+CAS engine, schema, migrations
    server/                  # cumulusd binary
    backend/                 # Backend/OpStore/OpHeadsStore impls, runtime bridge, SyncEngine
  cli/src/commands/cumulus/  # init.rs, clone.rs, sync.rs, mod.rs (new module)
  cli/…                      # + factory registration, Cargo.toml dep (see §3)
  cumulus/e2e/               # integration tests driving the built `jj` binary
```

## 5. Data model & identity

- **Ids:** CommitId/TreeId/FileId/SymlinkId = first 32 bytes of Blake2b-512 over the
  canonical serialized object (commits/trees via jj-lib `ContentHash` +
  `blake2b_hash`, like SimpleBackend; files/symlinks: raw bytes).
  `commit_id_length()==32`, `change_id_length()==16`.
- **Root ids:** root commit = 32 zero bytes; root change id = 16 zero bytes; empty
  tree id computed at repo-create by hashing the empty Tree proto (SimpleBackend
  pattern) and recorded in server repo info.
- **Repo info** (`GetRepoInfo`; cached at `.jj/repo/store/cumulus_repo.toml`): repo
  name, id lengths, root commit id, empty tree id, protocol version, op-mode, and a
  reserved `completeness` field (`FULL|LAZY`; unused in v1 — §13.6).
- **Object kinds:** `COMMIT`, `TREE`, `SYMLINK` = metadata objects (small, batched);
  `FILE` = blob (streamed). `OP`, `VIEW` handled by OpService. `CopyHistory`
  reserved.
- **TreeValue** round-trips completely: File{id, executable, copy_id}, Symlink,
  Tree, GitSubmodule (opaque). **Conflicts:** `Commit.root_tree` = `Merge<TreeId>`
  → repeated tree ids + conflict labels, mirroring `simple_store.proto`.
- **Operations/Views** mirror `simple_op_store.proto` field-for-field (native,
  non-deprecated shapes only — legacy encodings never occur on the cumulus wire);
  ids are content hashes (as in jj), **fixed at 64 bytes** (SimpleOpStore parity;
  server enforces at PushOps) → idempotent push.

## 6. Server

### 6.1 Storage (cumulus-store; shared with client cache)

Per-repo dir: `meta.sqlite` (WAL, busy_timeout=5s) + `blobs/` CAS
(`blobs/ab/cdef…`, tempfile + atomic rename).

```sql
CREATE TABLE kv        (key TEXT PRIMARY KEY, value BLOB);           -- repo info, schema version
CREATE TABLE objects   (kind INTEGER NOT NULL, id BLOB NOT NULL,
                        data BLOB NOT NULL, PRIMARY KEY (kind, id)); -- COMMIT/TREE/SYMLINK
CREATE TABLE blobs     (id BLOB PRIMARY KEY, size INTEGER NOT NULL,
                        storage INTEGER NOT NULL,                    -- 0=inline,1=cas_file,2=chunks(§13.1)
                        inline_data BLOB);                           -- storage=0 (<64 KiB)
CREATE TABLE commit_parents (commit_id BLOB NOT NULL, parent_id BLOB NOT NULL,
                        PRIMARY KEY (commit_id, parent_id));
CREATE TABLE commit_meta    (commit_id BLOB PRIMARY KEY, generation INTEGER NOT NULL);
-- reserved for §13.7 (populate at ingest from v1; no RPC reads it in v1):
CREATE TABLE commit_changes (change_id BLOB NOT NULL, commit_id BLOB NOT NULL,
                        PRIMARY KEY (change_id, commit_id));
CREATE TABLE ops       (id BLOB PRIMARY KEY, data BLOB NOT NULL, view_id BLOB NOT NULL);
CREATE TABLE op_parents(op_id BLOB NOT NULL, parent_id BLOB NOT NULL,
                        PRIMARY KEY (op_id, parent_id));
CREATE TABLE views     (id BLOB PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE op_heads  (id BLOB PRIMARY KEY);
-- reserved for §13.1 (create empty now; do not populate in v1):
CREATE TABLE blob_chunks (blob_id BLOB NOT NULL, seq INTEGER NOT NULL,
                        chunk_id BLOB NOT NULL, offset INTEGER NOT NULL,
                        len INTEGER NOT NULL, PRIMARY KEY (blob_id, seq));
CREATE TABLE chunk_data  (id BLOB PRIMARY KEY, storage INTEGER NOT NULL, inline_data BLOB);
-- client-cache-only:
CREATE TABLE outbox    (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind INTEGER, id BLOB);
CREATE TABLE sync_state(key TEXT PRIMARY KEY, value BLOB);
```

Invariants:

- `generation = 1 + max(parent generations)`; root generation 0 (unstored). Commits
  arrive parent-before-child (§8.1 push ordering); violation →
  FAILED_PRECONDITION.
- Commit ingest extracts parents, generation, and change-id in a single decode pass
  (populating `commit_parents`, `commit_meta`, `commit_changes`).
- Op-head update transactional (BEGIN IMMEDIATE): insert op; delete every existing
  head that is an ancestor of it (recursive CTE over `op_parents`); insert as head
  — **unless the new op is itself an ancestor of an existing head** (skip: a
  re-pushed old op must not resurface as a head; found in Run 1 — the naive recipe
  contradicted the re-push-is-a-no-op invariant below).
  Concurrent pushes → multiple heads retained. **Never merge views server-side.**
- Content-addressed → `INSERT OR IGNORE`; re-push is a no-op.

### 6.2 Services (cumulus/proto sketch — implementer expands messages)

```proto
syntax = "proto3";
package cumulus.v1;

service RepoService {
  rpc CreateRepo(CreateRepoRequest) returns (RepoInfo);        // idempotent by name
  rpc GetRepoInfo(RepoRef) returns (RepoInfo);
  rpc ListRepos(Empty) returns (ListReposResponse);
}
service ObjectService {
  rpc HaveObjects(HaveObjectsRequest) returns (HaveObjectsResponse);  // batch (kind,id) presence
  rpc GetObjects(GetObjectsRequest) returns (stream ObjectChunk);
  rpc PutObjects(stream ObjectChunk) returns (PutObjectsResponse);
  rpc GetBlob(GetBlobRequest) returns (stream BlobFrame);             // BlobFrame{offset,bytes} — CDC-ready
  rpc PutBlob(stream PutBlobFrame) returns (PutBlobResponse);         // frame 0 = header{id,size}
  rpc GetCommitsReachableFrom(ReachabilityRequest) returns (stream ObjectChunk);
      // Frontier-stopping walk: recursive CTE over commit_parents from heads[],
      // stopping at the `have` frontier. A SUPERSET of exact set-minus: may resend
      // commits the client already has when history routes around the frontier —
      // clients must tolerate duplicates (ingest is idempotent). Streams
      // parent-before-child (generation ASC). THE lazy-clone workhorse.
  // §13.1 SPEC-ONLY (serve UNIMPLEMENTED in v1):
  rpc GetBlobManifest(GetBlobRequest) returns (BlobManifest);
  rpc GetChunks(GetChunksRequest) returns (stream ChunkData);
}
service OpService {
  rpc GetOpHeads(RepoRef) returns (OpHeadsResponse);
  rpc HaveOps(HaveOpsRequest) returns (HaveOpsResponse);
  rpc GetOps(GetOpsRequest) returns (stream OpWithView);
  rpc PushOps(PushOpsRequest) returns (PushOpsResponse);
      // Precondition per op: view present (batch or store) AND view's head commit
      // ids present in objects — shallow check only. Updates op_heads atomically.
  // §8.2 SPEC-ONLY (serve UNIMPLEMENTED in v1):
  rpc UpdateOpHeadsCas(UpdateOpHeadsCasRequest) returns (UpdateOpHeadsCasResponse);
  rpc SubscribeOpHeads(RepoRef) returns (stream OpHeadsUpdate);
}
service AdminService {
  rpc Health(Empty) returns (HealthResponse);
  rpc Gc(GcRequest) returns (GcResponse);                       // UNIMPLEMENTED in v1
}
// §13.2 SPEC-ONLY; all RPCs return UNIMPLEMENTED in v1.
service IndexService {
  rpc GetIndexManifest(GetIndexManifestRequest) returns (IndexManifest);
  rpc GetIndexSegments(GetIndexSegmentsRequest) returns (stream IndexSegment);
}
```

All RPCs carry `repo` except ListRepos/Health. Limits: metadata batches ≤10k
objects / 32 MiB per message; blob frames 1 MiB. tonic-reflection enabled.

### 6.3 cumulusd binary

TOML config: `listen_addr`, `data_dir`, `[auth] tokens = { "<token>" = "<user>" }`,
optional `[tls] cert/key`. Bearer-token interceptor; user attached to request
extensions (logged only in v1). One tokio runtime; per-repo store handles in
`RwLock<HashMap<..>>`; rusqlite via `spawn_blocking` (never hold a connection
across awaits).

## 7. Client backend (cumulus/backend)

### 7.1 The runtime bridge (⚠ #1 one-shot killer)

jj does not guarantee an ambient tokio runtime when Backend futures are polled
(jj-lib uses `pollster` internally in places); tonic needs a reactor. Therefore:
`CumulusBackend` owns a lazily-built multi-thread runtime
(`OnceLock<tokio::runtime::Runtime>`, 2 workers). **Every** network call is
`self.rt().spawn(async move { … }).await` — a tokio `JoinHandle` is pollable from
any executor. Never poll tonic futures directly from Backend methods. Channel:
`Endpoint::from_shared(url)?.connect_lazy()` created inside the runtime context;
factory closures stay synchronous.

### 7.2 Backend methods

- Metadata getters from cached RepoInfo; `concurrency()` → 16.
- `read_commit/read_tree/read_symlink`: cache hit → decode; miss → GetObjects,
  write-through, return. Offline miss → `BackendError::ReadObject` with message
  naming `jj cumulus sync`. Root commit synthesized via `make_root_commit`; root
  write rejected (SimpleBackend crib).
- `read_file`: cache hit → reader over CAS file/inline. Miss → GetBlob streamed
  fully into CAS via tempfile+rename, **then** return a reader over the local file
  (never wrap the live gRPC stream; retry-safe and cache-populating).
- `write_*`: hash → local store → outbox append → return. **Never block on network.**
- `gc`: no-op. Copies: `Unsupported` / empty stream.

### 7.3 CumulusOpStore / CumulusOpHeadsStore (Mode A)

Implement the traits directly against the same local SQLite (`ops`, `views`,
`op_heads` + outbox in one transaction — do NOT wrap SimpleOpStore; one DB keeps
outbox and data atomic). OpHeadsStore locking: the SQLite transaction is the lock;
conform to the pinned trait's lock/callback shape.

**Auto-push hook (G5 invariant: never blocks):**
`CumulusOpHeadsStore::update_op_heads` (called exactly when a transaction lands)
**spawns a detached pusher process** and returns immediately — it never performs
network I/O in-process:
`std::process::Command::new(current_exe()) cumulus sync --push-only --quiet`,
stdio null, spawn-and-forget. The detached pusher:

- takes an exclusive push lock (BEGIN IMMEDIATE on a `sync_state` row); if held,
  exits immediately (the running pusher will pick up the new work — see drain loop);
- runs §8.1 push with short internal timeouts (connect 3s, per-RPC 30s) — these
  bound the *background* process only, never a user command;
- **drain loop:** after a successful push, re-checks whether local op heads/outbox
  advanced during the push (the lock-held race) and repeats until no new work;
- on failure: records `last_push_error` + timestamp in `sync_state` and exits;
  everything stays queued.
`cumulus.auto-push = true|false` (default true). Failure surfacing: detached
processes can't print to the user's terminal, so `jj cumulus sync --status` shows
outbox depth + last push error, and every `jj cumulus *` command prints a one-line
staleness warning when the outbox is non-empty and older than 5 minutes. This hook
is trait-local — zero extra CLI patching. (Alternatives rejected: synchronous push
with timeout — couples commit latency to server availability, violating G5; cli
post-command dispatch hook — bigger upstream diff.)

**Root operation bootstrap:** if the root op id is deterministic from
`RootOperationData` (expected; verify in the pinned tree + e2e test 8), clone =
local init + pull. Fallback branch (designed, use only if test fails): CreateRepo
records the initial op; clone bootstraps from it.

### 7.4 `jj cumulus` commands (cli/src/commands/cumulus/)

- `init --server <url> --repo <name> [--create]`: CreateRepo (if `--create`) →
  GetRepoInfo → `Workspace::init` with cumulus initializers → write
  `cumulus_repo.toml` → initial push.
- `clone <url>/<repo> [dir] [--sparse '<fileset>']`: init (no create); set sparse
  patterns BEFORE first checkout; pull (§8.1); check out the view's target.
- `sync [--push-only|--pull-only|--status]`: §8.1; `--status` = outbox depth, last
  sync, cache stats.
- Config — remotes are a **named set** even though v1 supports exactly one
  (keeps §13.6 DVCS mode open; retrofitting a singleton config shape is
  expensive): `cumulus.remotes.origin.url`, token via `JJ_CUMULUS_TOKEN` env or
  `cumulus.remotes.origin.token-file`. v1 hardcodes remote name `origin`;
  rejects any other configured remote with "multi-remote not yet supported".

## 8. Op sync — dual spec

### 8.1 Mode A: local-first + auto-push — **IMPLEMENT (v1)**

State: `sync_state["remote/origin/last_op_heads"]` — keys are per-remote-name even
though v1 has exactly one remote (§7.4, §13.6). All steps idempotent; crash-safe by
rerun.

**Push** (auto via §7.3 hook; also `jj cumulus sync`):

1. locals = local op heads; GetOpHeads → done if server has all.
2. Unsent ops: walk local op DAG from locals, stopping at server-known ids
   (batched HaveOps).
3. Referenced commits: from those ops' views (heads, bookmark targets, wc commits);
   HaveObjects → missing → walk parents until known → new-commit set.
4. Tree/blob closure via negotiation: level-order walk from each new commit's root
   trees; HaveObjects(batch) → recurse only into trees the server lacks. A subtree
   we never materialized can only reference server-known ids (we could only have
   obtained the id from the server) → walk terminates without local materialization.
   Upload missing trees/symlinks (PutObjects) and blobs (PutBlob), any order.
5. PutObjects(commits) parent-before-child (sort by local generation) — keeps §6.1
   generation invariant.
6. PushOps oldest-first — the causal barrier. Update sync_state. Drain covered
   outbox rows (outbox is a hint/superset, not the negotiation source of truth).

**Pull** (`jj cumulus sync`, clone):

1. GetOpHeads → unknown head ids.
2. GetOps walking ancestry until locally-known (batched); insert ops+views.
3. Commit backfill (keeps the stock index correct): collect view-referenced commit
   ids; `GetCommitsReachableFrom(heads=those, have=local frontier)` → stream commit
   protos into cache. **No trees/files fetched.**
4. Insert fetched heads via the OpHeadsStore update path. Next repo load: jj sees
   divergent heads → merges views automatically. **Implement no merge logic.**

### 8.2 Mode B: remote-first ("commit cloud") — **SPEC-ONLY — DO NOT IMPLEMENT IN V1**

Selected by `cumulus.op-mode = "remote"` (repo info records it; both modes share
the protocol). Tradeoffs vs Mode A: every command ≥1 RTT (unless subscription cache
is fresh); offline writes fail; in exchange, all machines/workspaces are always
coherent and no sync discipline exists.

Design:

- `CumulusRemoteOpHeadsStore` replaces §7.3's head handling:
  - `get_op_heads()`: serve from a subscription-fed cache if fresher than
    `cumulus.head-lease` (default 5s), else GetOpHeads. A long-lived
    `SubscribeOpHeads` stream (reconnect w/ backoff) feeds the cache; without it,
    every resolution is a unary call.
  - Head update: run §8.1 push steps 2–6 synchronously (objects/ops first), then
    `UpdateOpHeadsCas{expected_heads, new_head}`. Server compares-and-swaps the
    whole head set atomically. On CONFLICT (response carries current heads): fetch
    the new ops (§8.1 pull 1–3), surface to jj's op-head resolution the same way
    concurrent local processes do today (the trait's existing contention path), and
    let the retry loop re-run. Bounded retries (5) then error.
- OpStore reads: read-through cache (ops/views immutable → cache forever). Only
  head resolution is hot.
- Offline: `cumulus.offline-fallback = "fail" (default) | "queue"`. `fail`: reads
  proceed from cache with a stale-heads warning; mutations error. `queue`: degrade
  to Mode A semantics until connectivity returns (re-entry = normal Mode A push;
  CAS conflicts handled as above).
- Failure atomicity: objects/ops are idempotent uploads; the CAS is the only
  mutation; a crash between leaves harmless orphans (GC's problem, §13.5).
- Server additions: `UpdateOpHeadsCas` (transactional head-set swap; also runs the
  §6.1 ancestor-pruning), `SubscribeOpHeads` (fan-out watch per repo). Both already
  declared in §6.2; v1 returns UNIMPLEMENTED.
- When to choose: RTT ≤ a few ms (LAN/localhost server) or an editor/daemon keeping
  the subscription warm. Recommended default remains Mode A for internet servers.

## 9. Error & offline semantics

Network errors → `BackendError` with actionable messages; status codes: NOT_FOUND
(unknown object/repo), FAILED_PRECONDITION (push-order violation), UNAUTHENTICATED,
UNIMPLEMENTED (reserved RPCs). Idempotent unary RPCs retried 3× exponential;
streams restart from scratch in v1 (frames are offset-tagged → ranged resume is a
compatible later upgrade). Offline: reads cache-only; writes/ops local; background
push failures are recorded (`sync_state.last_push_error`) and surfaced via
`jj cumulus sync --status` and the staleness warning (§7.3) — never as command
failures. Retry policy for queued work: no daemon in v1; the next auto-push spawn
(any transaction) or explicit sync retries.

## 10. Testing & acceptance (definition of done)

Unit: store roundtrips (all kinds; conflict commits with multi-term root trees);
proto↔jj-lib conversion goldens; id-stability golden (fixed input → fixed 32-byte
id).

e2e (`cumulus/e2e`, in-process cumulusd on ephemeral port, `assert_cmd` driving the
fork-built `jj` in temp dirs):

1. **Roundtrip:** init → commits → log/diff/st work; kill server → cached reads fine.
2. **Two-client sync (G4):** A init+push; B clone; B sees A's log incl. change ids &
   evolog; B commits (auto-push); A syncs; A sees B. Bookmarks propagate.
3. **Concurrent convergence:** A,B commit before syncing; both push (two heads
   server-side — assert); both pull; identical `jj log` output both sides.
4. **Laziness (G2):** 200 files / 20 dirs / 50 commits; B clones; B's blob count ==
   working-copy files at head; `jj diff -r <old>` pulls exactly the needed old
   blobs (assert bounded cache growth).
5. **Sparse (G3):** `--sparse 'dir1/**'` fetches only dir1 blobs; widening via
   `jj sparse set` fetches the delta.
6. **Rebase-referencing-unfetched-subtree:** B rebases a commit whose unrelated
   subtree B never materialized; push succeeds (negotiation terminates at
   server-known trees).
7. **Offline queue (G5):** server down; B commits ×3 (succeeds, warns); sync fails
   gracefully, outbox=3; server up; sync drains; A sees all.
8. **Root-op determinism** gate (§7.3): two independent inits produce the same root
   op id; on failure, implement the fallback bootstrap branch.
9. **Big blob:** 50 MiB file streams both ways, bounded memory, hash verified.
10. **Auto-push hook:** commit with server up → arrives server-side without
    explicit sync (poll with deadline; it's async by design);
    `cumulus.auto-push=false` → stays in outbox.
11. **G5 latency invariant (the offline-first gate):** against a *stalling* server
    (TCP listener that accepts and never responds — the worst case, slower than
    "down"), `jj new` + `jj describe` complete in under 2 seconds. Also assert the
    detached-pusher lock: two rapid commits spawn pushers that don't duplicate work
    (drain loop covers the second).
Perf smoke (not CI-gated): generated 5k-commit/2k-file repo; report clone
wall-time + bytes.

## 11. Build order (agent execution plan)

**Execution split: two completed runs.**

- **Run 1 = steps 0–3** — **COMPLETE (2026-07-13)**: four changes based at
  v0.43.0, with 44 unit and integration tests. The store, protocol, and server
  wire behavior remain authoritative for later work.
- **Run 2 = steps 4–7** — **COMPLETE (2026-07-13)**: four changes implementing the
  lazy backend, Mode A op stores and synchronization, detached auto-push, CLI
  integration, and the §10 acceptance suite. All 11 acceptance behaviors pass.

See [`IMPLEMENTATION.md`](IMPLEMENTATION.md) for exact validation and findings,
[`RUN2-HANDOFF.md`](RUN2-HANDOFF.md) for the Run 1 wire contract, and
[`FORK_PATCHES.md`](FORK_PATCHES.md) for deviations and rebase guidance.

0. Fork setup: bookmark `cumulus` at pinned tag; add workspace members; create the
   fork-patch log.
1. `cumulus/store` + schema + unit tests (no jj deps beyond hashing helpers).
2. `cumulus/proto` (protos, tonic-build, conversions) + goldens.
3. `cumulusd`: Repo/Object/Op services; reachability-CTE tests; grpcurl smoke.
4. `cumulus/backend`: runtime bridge; Backend vs local cache only → basic jj ops in
   a unit harness; then remote fetch.
5. Op stores + SyncEngine (Mode A) + auto-push hook.
6. CLI wiring (`commands/cumulus`, factories, Cargo.toml) — the three §3 touches.
7. e2e suite and README quickstart. The non-CI perf smoke remains future work.

## 12. Dependencies (new crates only)

tokio (rt-multi-thread), tonic/prost/tonic-build/tonic-reflection, rusqlite
(bundled), tempfile, futures, async-trait, serde+toml, thiserror, tracing;
assert_cmd + insta (tests); rustls via tonic feature (optional). Reuse jj-lib's
blake2/content_hash where public.

## 13. SPEC-ONLY designs (complete; DO NOT IMPLEMENT IN V1)

### 13.1 CDC chunking (large files; #2865 lineage)

Invariants already honored by v1: `FileId` = whole-file hash forever (chunking never
leaks into identity); `blobs.storage` indirection; offset-tagged `BlobFrame`s;
empty `blob_chunks`/`chunk_data` tables exist.

Design: FastCDC (fastcdc crate, v2020, normalization level 2), min/avg/max =
256 KiB / 1 MiB / 4 MiB; blobs > 8 MiB are chunked on receipt (server-side;
`PutBlob` wire unchanged). `chunk_id` = 32-byte truncated Blake2b-512 of chunk
bytes. `GetBlob` unchanged (server reassembles → old clients keep working). New
fast path: `GetBlobManifest(blob_id) → [(seq, chunk_id, offset, len)]`;
client checks local `chunk_data`, fetches only missing via `GetChunks` → cross-file
and cross-version dedup on the wire. Client cache gains the same tables (shared
engine → free). Upload dedup (client-side chunking + HaveChunks) is a further
optional step, same shapes. Migration: none — storage=2 rows appear alongside 0/1.

### 13.2 Remote index (lazy history at scale; #9805 lineage)

Being in-tree changes the strategy vs any out-of-tree attempt: **refactor, don't
reimplement.** Steps:

1. In `lib/src/default_index/`, extract the fs reads/writes behind a
   `DefaultIndexStorage` trait (shape per issue #9805: op-link read/write,
   commit-segment read/write, changed-path-segment read/write). Stock behavior =
   `FsIndexStorage`. This is a fork-patch-logged jj-lib change, deliberately
   upstreamable (it IS #9805).
2. `RemoteSegmentStorage`: fetches segments via `GetIndexManifest(op_id)` +
   `GetIndexSegments` (content-addressed → cached forever in local files), layered
   under the **stock** default index and revset engine — no revset reimplementation
   (this dissolves the "OlshaMB wall").
3. Server builds canonical segments (it has jj-lib: run the default index build
   over its store on op-head advance; segments are derived data, rebuildable).
4. Client mutable index (new local commits) already layers mutable-over-readonly in
   the default index — unchanged.
5. Pull (§8.1 step 3) then DROPS commit backfill → true O(working-set) clone.
   `read_commit` still fetches lazily for displayed commits (small, batched).

Honest caveats: revset evaluation may page many segments (worst case: whole index
for global queries like `::` filters); generation-ordered segment layout keeps
recent-history queries (the common case) to recent segments. The infallible
`start_modification()` seam → interior mutability per PR #7790 discussion. This is
the largest v2 item; budget accordingly.

### 13.3 Mode B op sync — fully specified at §8.2

### 13.4 VFS working copy (note, not full spec)

Replace the working copy via jj's working-copy trait
(`cli/examples/custom-working-copy`) with FUSE (Linux) / FSKit or NFS-localhost
(macOS) materialize-on-open. Orthogonal to the protocol: consumes the same lazy
`read_file`/`read_tree`. Interacts with snapshotting (working-copy scans must not
fault in the universe — needs fsmonitor-style change tracking). Design when needed.

### 13.5 GC (note, not full spec)

Server: mark = union of reachable commits from all op-head views (DAG tables make
this cheap) minus retention window for abandoned ops; sweep objects/blobs/chunks.
Needs an op-retention policy decision first. `AdminService.Gc` reserved. Client
cache: LRU by atime, never evicting outbox-referenced ids.

### 13.6 DVCS / multi-remote mode — **SPEC-ONLY — DO NOT IMPLEMENT IN V1**

The protocol is already peer-symmetric; hub-and-spoke is v1 *topology*, not a
protocol commitment. Four already-specced properties do the heavy lifting:
the server holds no authority (§6.1 forbids server-side view merging — all
convergence is client-side op-head merge, and merging heads gathered from five
peers is the same machinery as from one); client and server share one store engine
(`cumulus-store`), so any client is a latent peer; objects are content-addressed
and self-verifying, so fetching from an untrusted peer is integrity-safe by
construction (trust is an availability/authorization question, never correctness)
and unconnected peers converge on identical ids for identical content; and jj's
own `RemoteView` model is per-remote-name upstream — per-remote bookmark tracking
is native jj, not an invention.

Design:

1. **Named remotes** — the §7.4 config shape generalizes to N remotes; sync (§8.1)
   runs pairwise per remote against per-remote `sync_state` keys; op heads from
   multiple remotes merge client-side as usual.
2. **`jj cumulus serve`** — run cumulusd against the local repo's own store; any
   client becomes a peer endpoint.
3. **Completeness declaration** — the reserved RepoInfo field: each node advertises
   `FULL` (eagerly replicates everything it hears about — the `jj cumulus mirror`
   command is the mechanism; a valid origin for others) or `LAZY` (leaf; cannot
   serve what it never fetched). This resolves the structural tension between
   laziness and git-style "every clone is an origin" (why Sapling went
   hub-and-spoke while git stayed complete): completeness becomes a per-node
   declaration, and topology emerges — DVCS among FULL replicas, hub-and-spoke at
   LAZY edges. Optional fetch-through (a LAZY peer proxying misses to its own
   upstream) is protocol-compatible but chains latency and availability — default
   off.
4. **Per-remote sync policy** — `ops` (full op-log sync; correct for one person's
   device set = commit cloud, the v1 Mode A behavior) vs `refs` (objects +
   bookmark refs only, git-like exchange; correct between collaborators — full op
   sync would ship undo history and working-copy snapshots of every workspace).
   The my-devices/other-people distinction is one git never had to articulate
   because it never synced op logs at all.
5. **Out of scope even here:** peer discovery, NAT traversal, decentralized
   identity (the radicle-grade problems). Peers are reachable cumulusd endpoints
   with per-remote tokens.

v1 door-openers already in place: named-remote config (§7.4), per-remote
sync_state keys (§8.1), reserved `completeness` in RepoInfo (§5).

### 13.7 Change-id-keyed server queries (note, not full spec)

The change-id is jj's durable name for a unit of work — it survives every rewrite
while commit ids churn. Anything built above this backend that reasons about
*work* rather than *snapshots* joins on it: evolog queries ("all commits this
change has been"), review-follows-the-change (Gerrit model), short human-facing
work names via change-id prefix resolution, and a possible future forge/work layer
where discussion and review artifacts attach to changes, not branches. Mode B
(§8.2) and the remote index (§13.2) also benefit from server-side change-id
resolution.

None of that is v1 — but the server already decodes every commit proto at ingest
to maintain `commit_parents`/`commit_meta`, so capturing the change-id in the same
pass is ~free now, whereas adding it later means a backfill migration over every
existing repo. This is the same retrofit-asymmetry rule that justified the
blob-indirection column (§13.1) and named-remotes config (§13.6): **reserve the
schema when ingest-time capture is free and retrofit is a migration; otherwise
rely on protobuf/SQLite being additive.**

## 14. Reference index

- In-tree cribs: listed in §3.
- jj issues shaping this design: #50 (txn-scoped write batching — the outbox+push
  batch is its moral equivalent), #7135 (native remote: full server process, DB
  trees), #7825/#7790/#7799 (fallible index traits), #9805 (index segmentation —
  §13.2 implements it), #2865 (CDC — §13.1), #7815 (sparse filesets), #8920
  (partial-clone pain bypassed entirely).
- Prior art: Meta Sapling/Mononoke/EdenFS (segmented changelog, lazy store, VFS);
  Google jj commit-cloud (`.jj` as pointer files — Mode B's ancestor);
  thoughtpolice `qq-rpc` (local default index over RPC backend — v1's index
  stance).
