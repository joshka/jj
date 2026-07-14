# Cumulus

Cumulus is jj's native, lazy remote-storage backend. It stores local writes
immediately, synchronizes operation history in the background, and fetches missing
content from `cumulusd` only when jj reads it. The implementation is an experiment
on the jj v0.43.0 tree, not a production-ready backend or Git forge replacement.

Run 1 and Run 2 are complete. The backend, server, Mode A synchronization, CLI,
and all 11 acceptance behaviors in the specification are implemented.

## Quickstart

Build the fork and start a development server:

```shell
cargo build -p jj-cli -p cumulus-server --bins
mkdir -p /tmp/cumulus-data
cat >/tmp/cumulusd.toml <<'EOF'
listen_addr = "127.0.0.1:8620"
data_dir = "/tmp/cumulus-data"
EOF
target/debug/cumulusd --config /tmp/cumulusd.toml
```

In another terminal, initialize and use a repository:

```shell
mkdir project
cd project
path/to/target/debug/jj cumulus init \
  --server http://127.0.0.1:8620 \
  --repo project \
  --create
path/to/target/debug/jj describe -m "Start project"
path/to/target/debug/jj cumulus sync
```

Clone the native remote without Git:

```shell
path/to/target/debug/jj cumulus clone \
  http://127.0.0.1:8620/project \
  project-copy
```

Use `jj cumulus sync --status` to inspect queued local work and the last
background-push error. Use `jj cumulus sync` to push queued work and pull remote
operation history explicitly. All ordinary jj mutations remain local-first when
the server is unavailable.

## Documentation

- [Implementation status and findings](docs/IMPLEMENTATION.md)
- [Implementation specification](docs/SPEC.md)
- [Run 2 wire-contract handoff](docs/RUN2-HANDOFF.md)
- [Fork patches and rebase guide](docs/FORK_PATCHES.md)
