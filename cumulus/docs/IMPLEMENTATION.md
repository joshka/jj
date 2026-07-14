# Implementation status and findings

## Status

Cumulus Run 1 and Run 2 are complete on the `cumulus` bookmark, based on jj
v0.43.0. The implementation covers specification build steps 0–7:

- the shared SQLite and content-addressed blob store;
- the gRPC protocol and jj-lib conversions;
- the multi-repository `cumulusd` server;
- the lazy client backend and owned Tokio runtime bridge;
- native operation, view, and operation-head stores;
- Mode A push, pull, detached auto-push, and offline queuing;
- `jj cumulus init`, `clone`, and `sync`;
- the end-to-end acceptance suite and quickstart.

Mode B, the remote index, content-defined chunking, garbage collection, multi-remote
operation, Git interoperability, and a forge UI remain specification-only or
future work.

## Validation

The completed stack passed the full workspace suite with `NO_COLOR` removed so
jj's ANSI-sensitive snapshots ran under their expected terminal environment:

```shell
env -u NO_COLOR TERM=xterm-256color COLORTERM=truecolor \
  cargo test --workspace --quiet
```

This includes:

- all 44 Run 1 store, protocol, and server tests;
- backend unit and synchronization integration tests;
- seven end-to-end scenarios covering all 11 specification acceptance behaviors;
- the existing jj workspace tests and doctests.

The Cumulus crates also pass Rustfmt, targeted Clippy with warnings denied, and
Markdown linting. The specification's non-CI 5,000-commit performance smoke was
not run.

## Technical findings

### The native backend seams are viable

The pinned jj traits were sufficient for a native remote backend without modifying
`jj-lib`. The required upstream integration is limited to workspace dependencies,
three small `jj-cli` registration points, and additive command modules. The local
store needed narrow additive helpers for atomic operation/view writes and push-lock
state; these are recorded in [the fork-patch log](FORK_PATCHES.md).

### Runtime ownership is a backend concern

Backend calls can originate under executors that are not Tokio. Cumulus therefore
owns a multi-threaded Tokio runtime, creates its lazy tonic channel inside that
runtime, and spawns every network future onto it before awaiting the join handle.
Relying on the caller's runtime makes otherwise valid jj commands fail or hang.

### Offline-first changes the locking design

Foreground writes persist to SQLite and the local CAS, append outbox state, and
return without network access. The detached pusher claims a logical lock in a
short transaction rather than holding a SQLite write transaction across network
calls. Holding that transaction would couple local command latency to server
availability and violate the offline-first invariant.

### Laziness is currently about content, not history

Clone and pull fetch operation history and commit metadata eagerly because jj's
stock index needs a locally materialized commit DAG. Trees and blobs remain lazy,
including sparse working-copy materialization. A remote index is required before
history scale is also independent of local storage and startup cost.

### Full op-log sync is a narrow collaboration policy

Full operation synchronization is useful for trusted devices, ephemeral
workspaces, and agent recovery because it preserves undo, evolution, bookmarks,
conflicts, and working-copy state. It is a poor default between unrelated
collaborators because operations can reveal private intermediate and abandoned
work.

A practical multi-user design should scope operation heads by actor or trusted
device set. Team and forge remotes should instead exchange selected refs and
reachable objects, with an optional published-only evolution chain. Existing
content-addressed operations should never be redacted in place.

### GitHub is a publication boundary, not a native peer

GitHub remains the practical forge for review, CI, issues, releases, permissions,
and outside contributors. A future integration should project selected Cumulus
changes into a sidecar Git repository and import GitHub refs back as remote
bookmarks. It should not send private operation logs to GitHub or place Git export
on ordinary jj command paths.

The likely product role is therefore a workspace fabric behind GitHub rather than
a general-purpose GitHub replacement: lazy workspaces, durable agent checkpoints,
large-repository acceleration, and cloud-development continuity, with explicit
publication to conventional GitHub branches and pull requests.

## Production gaps

Before production use, the experiment needs:

- authentication scopes and per-repository authorization rather than static
  bearer-token identity alone;
- operation-vs-ref sharing policies and per-principal synchronization scopes;
- server garbage collection, client cache eviction, retention, backup, and schema
  migration procedures;
- a remote/lazy index for very large histories;
- content-defined chunking for efficient large-file revisions;
- GitHub projection, webhook ingestion, and transactional native-to-Git identity
  mappings;
- load, fault-injection, upgrade, and long-running concurrency testing;
- observability and administrative tooling suitable for operating `cumulusd`.

These gaps do not invalidate the experiment's core result: native lazy storage and
offline-first Mode A synchronization work through jj's existing backend and store
interfaces. They define the difference between the completed prototype and a
service people could rely on.
