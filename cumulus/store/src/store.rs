use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use blake2::Blake2b512;
use digest::Digest as _;
use rusqlite::Connection;
use rusqlite::OptionalExtension as _;
use rusqlite::Transaction;
use rusqlite::TransactionBehavior;
use rusqlite::params;

use crate::OBJECT_ID_LENGTH;
use crate::blob::Blob;
use crate::blob::BlobWriter;
use crate::blob::FinishedBlob;
use crate::blob::cas_path;
use crate::error::StoreError;
use crate::error::StoreResult;

/// Kind discriminant for rows in the `objects` table. Numeric values match
/// the `cumulus.v1.ObjectKind` wire enum.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObjectKind {
    /// A commit proto (`cumulus.v1.Commit`).
    Commit = 1,
    /// A tree proto (`cumulus.v1.Tree`).
    Tree = 2,
    /// A symlink target (raw bytes).
    Symlink = 3,
}

impl ObjectKind {
    /// Converts a stored/wire discriminant back into an `ObjectKind`.
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::Commit),
            2 => Some(Self::Tree),
            3 => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// Everything extracted from a commit proto in the single ingest decode pass
/// (spec §6.1 invariant): the caller decodes, the store indexes.
#[derive(Debug)]
pub struct CommitIngest<'a> {
    /// The commit id (32-byte truncated Blake2b-512 of the commit).
    pub id: &'a [u8],
    /// The encoded commit proto, stored verbatim.
    pub data: &'a [u8],
    /// Parent commit ids. The all-zeros root id is allowed and has
    /// generation 0.
    pub parents: &'a [Vec<u8>],
    /// The commit's change id.
    pub change_id: &'a [u8],
}

/// Everything needed to ingest one operation atomically (spec §6.1 op-head
/// invariant and §6.2 PushOps precondition).
#[derive(Debug)]
pub struct OpIngest<'a> {
    /// The operation id.
    pub op_id: &'a [u8],
    /// The encoded operation proto, stored verbatim.
    pub op_data: &'a [u8],
    /// Parent operation ids. The all-zeros root op id is always "present".
    pub parents: &'a [Vec<u8>],
    /// The id of the view this operation references.
    pub view_id: &'a [u8],
    /// The encoded view proto if it accompanies the op; `None` means the
    /// view must already be stored.
    pub view_data: Option<&'a [u8]>,
    /// The view's head commit ids, for the shallow presence check.
    pub view_head_commits: &'a [Vec<u8>],
}

/// Hashes raw bytes to a content-addressed id: Blake2b-512 truncated to 32
/// bytes (spec §5). Used for file and symlink ids.
pub fn hash_bytes(data: &[u8]) -> Vec<u8> {
    Blake2b512::digest(data)[..OBJECT_ID_LENGTH].to_vec()
}

fn is_root_id(id: &[u8]) -> bool {
    id.iter().all(|b| *b == 0)
}

const SCHEMA_VERSION_KEY: &str = "schema_version";
const SCHEMA_VERSION: &[u8] = b"1";

// Spec §6.1 verbatim, including the tables reserved for §13.1 (CDC chunking,
// created empty) and the client-cache-only tables.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS kv        (key TEXT PRIMARY KEY, value BLOB);
CREATE TABLE IF NOT EXISTS objects   (kind INTEGER NOT NULL, id BLOB NOT NULL,
                        data BLOB NOT NULL, PRIMARY KEY (kind, id));
CREATE TABLE IF NOT EXISTS blobs     (id BLOB PRIMARY KEY, size INTEGER NOT NULL,
                        storage INTEGER NOT NULL,
                        inline_data BLOB);
CREATE TABLE IF NOT EXISTS commit_parents (commit_id BLOB NOT NULL, parent_id BLOB NOT NULL,
                        PRIMARY KEY (commit_id, parent_id));
CREATE TABLE IF NOT EXISTS commit_meta    (commit_id BLOB PRIMARY KEY, generation INTEGER NOT \
                      NULL);
CREATE TABLE IF NOT EXISTS commit_changes (change_id BLOB NOT NULL, commit_id BLOB NOT NULL,
                        PRIMARY KEY (change_id, commit_id));
CREATE TABLE IF NOT EXISTS ops       (id BLOB PRIMARY KEY, data BLOB NOT NULL, view_id BLOB NOT \
                      NULL);
CREATE TABLE IF NOT EXISTS op_parents(op_id BLOB NOT NULL, parent_id BLOB NOT NULL,
                        PRIMARY KEY (op_id, parent_id));
CREATE TABLE IF NOT EXISTS views     (id BLOB PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS op_heads  (id BLOB PRIMARY KEY);
CREATE TABLE IF NOT EXISTS blob_chunks (blob_id BLOB NOT NULL, seq INTEGER NOT NULL,
                        chunk_id BLOB NOT NULL, offset INTEGER NOT NULL,
                        len INTEGER NOT NULL, PRIMARY KEY (blob_id, seq));
CREATE TABLE IF NOT EXISTS chunk_data  (id BLOB PRIMARY KEY, storage INTEGER NOT NULL, inline_data \
                      BLOB);
CREATE TABLE IF NOT EXISTS outbox    (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind INTEGER, id \
                      BLOB);
CREATE TABLE IF NOT EXISTS sync_state(key TEXT PRIMARY KEY, value BLOB);
";

/// A per-repo SQLite+CAS store.
///
/// Cheap to share behind an [`std::sync::Arc`]; all methods take `&self`.
/// SQLite work is synchronous — callers on async runtimes must wrap calls in
/// `spawn_blocking`. Multiple `Store` instances (or processes) may open the
/// same directory; WAL mode plus `busy_timeout` arbitrates.
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    blobs_dir: PathBuf,
    conn: Mutex<Connection>,
}

impl Store {
    /// Opens (creating if needed) the store in `dir`.
    pub fn open(dir: &Path) -> StoreResult<Self> {
        std::fs::create_dir_all(dir)?;
        let blobs_dir = dir.join("blobs");
        std::fs::create_dir_all(&blobs_dir)?;
        let conn = Connection::open(dir.join("meta.sqlite"))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT OR IGNORE INTO kv (key, value) VALUES (?1, ?2)",
            params![SCHEMA_VERSION_KEY, SCHEMA_VERSION],
        )?;
        Ok(Self {
            dir: dir.to_path_buf(),
            blobs_dir,
            conn: Mutex::new(conn),
        })
    }

    /// The directory this store lives in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> StoreResult<T>) -> StoreResult<T> {
        let conn = self.conn.lock().unwrap();
        f(&conn)
    }

    fn with_tx<T>(&self, f: impl FnOnce(&Transaction) -> StoreResult<T>) -> StoreResult<T> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&tx)?;
        tx.commit()?;
        Ok(value)
    }

    // kv

    /// Reads a value from the `kv` table (repo info, schema version).
    pub fn kv_get(&self, key: &str) -> StoreResult<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let value = conn
                .query_row("SELECT value FROM kv WHERE key = ?1", params![key], |row| {
                    row.get(0)
                })
                .optional()?;
            Ok(value)
        })
    }

    /// Writes a value to the `kv` table.
    pub fn kv_set(&self, key: &str, value: &[u8]) -> StoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                params![key, value],
            )?;
            Ok(())
        })
    }

    // objects

    /// Stores a tree or symlink object. Content-addressed: re-put is a no-op.
    ///
    /// Commits must go through [`Store::put_commit`] so the DAG tables stay
    /// consistent.
    pub fn put_object(&self, kind: ObjectKind, id: &[u8], data: &[u8]) -> StoreResult<()> {
        assert!(
            kind != ObjectKind::Commit,
            "commits must be ingested via put_commit"
        );
        self.with_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO objects (kind, id, data) VALUES (?1, ?2, ?3)",
                params![kind as i64, id, data],
            )?;
            Ok(())
        })
    }

    /// Ingests a commit, populating `commit_parents`, `commit_meta`, and
    /// `commit_changes` in the same transaction (spec §6.1).
    ///
    /// Fails with [`StoreError::MissingCommitParent`] if a non-root parent
    /// has not been ingested yet (parent-before-child ordering).
    pub fn put_commit(&self, ingest: &CommitIngest) -> StoreResult<()> {
        self.with_tx(|tx| {
            if ingest.parents.is_empty() {
                return Err(StoreError::CommitWithoutParents(ingest.id.to_vec()));
            }
            let mut generation: i64 = 0;
            for parent in ingest.parents {
                let parent_generation = if is_root_id(parent) {
                    0
                } else {
                    tx.query_row(
                        "SELECT generation FROM commit_meta WHERE commit_id = ?1",
                        params![parent],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .ok_or_else(|| StoreError::MissingCommitParent(parent.clone()))?
                };
                generation = generation.max(parent_generation + 1);
            }
            tx.execute(
                "INSERT OR IGNORE INTO objects (kind, id, data) VALUES (?1, ?2, ?3)",
                params![ObjectKind::Commit as i64, ingest.id, ingest.data],
            )?;
            for parent in ingest.parents {
                tx.execute(
                    "INSERT OR IGNORE INTO commit_parents (commit_id, parent_id) VALUES (?1, ?2)",
                    params![ingest.id, parent],
                )?;
            }
            tx.execute(
                "INSERT OR IGNORE INTO commit_meta (commit_id, generation) VALUES (?1, ?2)",
                params![ingest.id, generation],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO commit_changes (change_id, commit_id) VALUES (?1, ?2)",
                params![ingest.change_id, ingest.id],
            )?;
            Ok(())
        })
    }

    /// Reads an object's stored bytes.
    pub fn get_object(&self, kind: ObjectKind, id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let data = conn
                .query_row(
                    "SELECT data FROM objects WHERE kind = ?1 AND id = ?2",
                    params![kind as i64, id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(data)
        })
    }

    /// Whether an object is stored.
    pub fn has_object(&self, kind: ObjectKind, id: &[u8]) -> StoreResult<bool> {
        self.with_conn(|conn| {
            let found = conn
                .query_row(
                    "SELECT 1 FROM objects WHERE kind = ?1 AND id = ?2",
                    params![kind as i64, id],
                    |_| Ok(()),
                )
                .optional()?;
            Ok(found.is_some())
        })
    }

    /// Batch presence check: returns the subset of `refs` that are missing.
    pub fn missing_objects(
        &self,
        refs: &[(ObjectKind, Vec<u8>)],
    ) -> StoreResult<Vec<(ObjectKind, Vec<u8>)>> {
        self.with_conn(|conn| {
            let mut stmt =
                conn.prepare_cached("SELECT 1 FROM objects WHERE kind = ?1 AND id = ?2")?;
            let mut missing = vec![];
            for (kind, id) in refs {
                let found = stmt
                    .query_row(params![*kind as i64, id], |_| Ok(()))
                    .optional()?;
                if found.is_none() {
                    missing.push((*kind, id.clone()));
                }
            }
            Ok(missing)
        })
    }

    /// The generation number of a stored commit, if present.
    pub fn commit_generation(&self, id: &[u8]) -> StoreResult<Option<i64>> {
        self.with_conn(|conn| {
            let generation = conn
                .query_row(
                    "SELECT generation FROM commit_meta WHERE commit_id = ?1",
                    params![id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(generation)
        })
    }

    /// Commit ids recorded for a change id (populated at ingest; no RPC reads
    /// this in v1 — spec §13.7).
    pub fn commits_for_change(&self, change_id: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT commit_id FROM commit_changes WHERE change_id = ?1 ORDER BY commit_id",
            )?;
            let ids = stmt
                .query_map(params![change_id], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            Ok(ids)
        })
    }

    /// The lazy-clone workhorse (spec §6.2): commits reachable from `heads`,
    /// walking `commit_parents` and stopping at the `have` frontier.
    ///
    /// Returns `(id, data)` pairs ordered by generation ascending, so a
    /// receiver ingesting in order never violates the parent-before-child
    /// invariant. Unknown ids in `heads` are silently skipped; the root
    /// commit is never returned (it is synthesized, not stored).
    pub fn commits_reachable_from(
        &self,
        heads: &[Vec<u8>],
        have: &[Vec<u8>],
    ) -> StoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.with_tx(|tx| {
            tx.execute_batch(
                "CREATE TEMP TABLE IF NOT EXISTS walk_heads(id BLOB PRIMARY KEY);
                 CREATE TEMP TABLE IF NOT EXISTS walk_have(id BLOB PRIMARY KEY);
                 DELETE FROM walk_heads;
                 DELETE FROM walk_have;",
            )?;
            {
                let mut insert_head =
                    tx.prepare("INSERT OR IGNORE INTO walk_heads (id) VALUES (?1)")?;
                for id in heads {
                    insert_head.execute(params![id])?;
                }
                let mut insert_have =
                    tx.prepare("INSERT OR IGNORE INTO walk_have (id) VALUES (?1)")?;
                for id in have {
                    insert_have.execute(params![id])?;
                }
            }
            let mut stmt = tx.prepare(
                "WITH RECURSIVE reach(id) AS (
                     SELECT id FROM walk_heads
                     WHERE id NOT IN (SELECT id FROM walk_have)
                     UNION
                     SELECT p.parent_id FROM commit_parents p
                     JOIN reach r ON p.commit_id = r.id
                     WHERE p.parent_id NOT IN (SELECT id FROM walk_have)
                 )
                 SELECT o.id, o.data FROM reach r
                 JOIN objects o ON o.kind = ?1 AND o.id = r.id
                 JOIN commit_meta m ON m.commit_id = r.id
                 ORDER BY m.generation ASC, o.id ASC",
            )?;
            let rows = stmt
                .query_map(params![ObjectKind::Commit as i64], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })
    }

    // blobs

    /// Starts streaming a blob into the store. Finish with
    /// [`BlobWriter::finish`] then [`Store::finish_blob`].
    pub fn blob_writer(&self) -> StoreResult<BlobWriter> {
        BlobWriter::new(&self.blobs_dir)
    }

    /// Registers a finished blob in SQLite. Idempotent.
    pub fn finish_blob(&self, finished: &FinishedBlob) -> StoreResult<Vec<u8>> {
        self.with_conn(|conn| {
            let storage: i64 = if finished.inline_data.is_some() { 0 } else { 1 };
            conn.execute(
                "INSERT OR IGNORE INTO blobs (id, size, storage, inline_data)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    finished.id,
                    finished.size as i64,
                    storage,
                    finished.inline_data
                ],
            )?;
            Ok(finished.id.clone())
        })
    }

    /// Stores an in-memory blob; returns its id.
    pub fn put_blob(&self, data: &[u8]) -> StoreResult<Vec<u8>> {
        let mut writer = self.blob_writer()?;
        writer.write(data)?;
        let finished = writer.finish(None)?;
        self.finish_blob(&finished)
    }

    /// Whether a blob is stored.
    pub fn has_blob(&self, id: &[u8]) -> StoreResult<bool> {
        self.with_conn(|conn| {
            let found = conn
                .query_row("SELECT 1 FROM blobs WHERE id = ?1", params![id], |_| Ok(()))
                .optional()?;
            Ok(found.is_some())
        })
    }

    /// Looks up a blob's bytes (inline) or CAS file path.
    pub fn get_blob(&self, id: &[u8]) -> StoreResult<Option<Blob>> {
        let row = self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT size, storage, inline_data FROM blobs WHERE id = ?1",
                    params![id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<Vec<u8>>>(2)?,
                        ))
                    },
                )
                .optional()?;
            Ok(row)
        })?;
        let Some((size, storage, inline_data)) = row else {
            return Ok(None);
        };
        match storage {
            0 => {
                let data = inline_data.ok_or_else(|| StoreError::CorruptBlob {
                    id: id.to_vec(),
                    reason: "inline blob without inline_data".into(),
                })?;
                Ok(Some(Blob::Inline(data)))
            }
            1 => {
                let path = cas_path(&self.blobs_dir, id);
                if !path.is_file() {
                    return Err(StoreError::CorruptBlob {
                        id: id.to_vec(),
                        reason: format!("missing CAS file {}", path.display()),
                    });
                }
                Ok(Some(Blob::File {
                    path,
                    size: size as u64,
                }))
            }
            // storage=2 (chunked) is reserved for §13.1 and never written in v1.
            other => Err(StoreError::CorruptBlob {
                id: id.to_vec(),
                reason: format!("unknown storage discriminant {other}"),
            }),
        }
    }

    // ops and views

    /// Stores a view. Content-addressed: re-put is a no-op.
    pub fn put_view(&self, id: &[u8], data: &[u8]) -> StoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO views (id, data) VALUES (?1, ?2)",
                params![id, data],
            )?;
            Ok(())
        })
    }

    /// Reads a view's stored bytes.
    pub fn get_view(&self, id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let data = conn
                .query_row("SELECT data FROM views WHERE id = ?1", params![id], |row| {
                    row.get(0)
                })
                .optional()?;
            Ok(data)
        })
    }

    /// Reads an operation's stored bytes and view id.
    pub fn get_op(&self, id: &[u8]) -> StoreResult<Option<(Vec<u8>, Vec<u8>)>> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT data, view_id FROM ops WHERE id = ?1",
                    params![id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            Ok(row)
        })
    }

    /// Whether an operation is stored.
    pub fn has_op(&self, id: &[u8]) -> StoreResult<bool> {
        self.with_conn(|conn| {
            let found = conn
                .query_row("SELECT 1 FROM ops WHERE id = ?1", params![id], |_| Ok(()))
                .optional()?;
            Ok(found.is_some())
        })
    }

    /// Batch presence check: returns the subset of `ids` that are missing.
    pub fn missing_ops(&self, ids: &[Vec<u8>]) -> StoreResult<Vec<Vec<u8>>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare_cached("SELECT 1 FROM ops WHERE id = ?1")?;
            let mut missing = vec![];
            for id in ids {
                let found = stmt.query_row(params![id], |_| Ok(())).optional()?;
                if found.is_none() {
                    missing.push(id.clone());
                }
            }
            Ok(missing)
        })
    }

    /// The current op heads.
    pub fn op_heads(&self) -> StoreResult<Vec<Vec<u8>>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id FROM op_heads ORDER BY id")?;
            let ids = stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            Ok(ids)
        })
    }

    /// Ingests one operation and updates op heads, atomically (spec §6.1):
    /// checks the view and its head commits are present (shallow), inserts
    /// op + view, deletes every existing head that is an ancestor of the new
    /// op, and inserts the new op as a head unless it is itself an ancestor
    /// of a remaining head (keeps re-push a no-op). Never merges views.
    ///
    /// Returns the resulting op heads.
    pub fn push_op(&self, ingest: &OpIngest) -> StoreResult<Vec<Vec<u8>>> {
        self.with_tx(|tx| {
            // Precondition: view available (batch or store).
            match ingest.view_data {
                Some(view_data) => {
                    tx.execute(
                        "INSERT OR IGNORE INTO views (id, data) VALUES (?1, ?2)",
                        params![ingest.view_id, view_data],
                    )?;
                }
                None => {
                    let found = tx
                        .query_row(
                            "SELECT 1 FROM views WHERE id = ?1",
                            params![ingest.view_id],
                            |_| Ok(()),
                        )
                        .optional()?;
                    if found.is_none() {
                        return Err(StoreError::MissingView {
                            op_id: ingest.op_id.to_vec(),
                            view_id: ingest.view_id.to_vec(),
                        });
                    }
                }
            }
            // Precondition: the view's head commits are present (shallow).
            {
                let mut stmt =
                    tx.prepare_cached("SELECT 1 FROM objects WHERE kind = ?1 AND id = ?2")?;
                for commit_id in ingest.view_head_commits {
                    if is_root_id(commit_id) {
                        continue;
                    }
                    let found = stmt
                        .query_row(params![ObjectKind::Commit as i64, commit_id], |_| Ok(()))
                        .optional()?;
                    if found.is_none() {
                        return Err(StoreError::MissingViewHead {
                            op_id: ingest.op_id.to_vec(),
                            commit_id: commit_id.clone(),
                        });
                    }
                }
            }
            // Precondition: parent ops present (PushOps is oldest-first).
            {
                let mut stmt = tx.prepare_cached("SELECT 1 FROM ops WHERE id = ?1")?;
                for parent in ingest.parents {
                    if is_root_id(parent) {
                        continue;
                    }
                    let found = stmt.query_row(params![parent], |_| Ok(())).optional()?;
                    if found.is_none() {
                        return Err(StoreError::MissingOpParent(parent.clone()));
                    }
                }
            }
            tx.execute(
                "INSERT OR IGNORE INTO ops (id, data, view_id) VALUES (?1, ?2, ?3)",
                params![ingest.op_id, ingest.op_data, ingest.view_id],
            )?;
            for parent in ingest.parents {
                tx.execute(
                    "INSERT OR IGNORE INTO op_parents (op_id, parent_id) VALUES (?1, ?2)",
                    params![ingest.op_id, parent],
                )?;
            }
            // Delete every existing head that is an ancestor of the new op.
            tx.execute(
                "WITH RECURSIVE anc(id) AS (
                     SELECT parent_id FROM op_parents WHERE op_id = ?1
                     UNION
                     SELECT p.parent_id FROM op_parents p JOIN anc ON p.op_id = anc.id
                 )
                 DELETE FROM op_heads WHERE id IN (SELECT id FROM anc)",
                params![ingest.op_id],
            )?;
            // Insert as head unless it is an ancestor of an existing head.
            let is_ancestor_of_head = tx
                .query_row(
                    "WITH RECURSIVE anc(id) AS (
                         SELECT p.parent_id FROM op_parents p
                         WHERE p.op_id IN (SELECT id FROM op_heads)
                         UNION
                         SELECT p.parent_id FROM op_parents p JOIN anc ON p.op_id = anc.id
                     )
                     SELECT 1 FROM anc WHERE id = ?1",
                    params![ingest.op_id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !is_ancestor_of_head {
                tx.execute(
                    "INSERT OR IGNORE INTO op_heads (id) VALUES (?1)",
                    params![ingest.op_id],
                )?;
            }
            let mut stmt = tx.prepare("SELECT id FROM op_heads ORDER BY id")?;
            let heads = stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            Ok(heads)
        })
    }

    // client-cache-only tables (outbox, sync_state)

    /// Appends an entry to the client outbox. Returns the assigned sequence
    /// number.
    pub fn outbox_append(&self, kind: i64, id: &[u8]) -> StoreResult<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO outbox (kind, id) VALUES (?1, ?2)",
                params![kind, id],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// Lists outbox entries in sequence order.
    pub fn outbox_list(&self) -> StoreResult<Vec<(i64, i64, Vec<u8>)>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT seq, kind, id FROM outbox ORDER BY seq")?;
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })
    }

    /// Removes outbox entries with sequence numbers `<= up_to_seq`.
    pub fn outbox_drain(&self, up_to_seq: i64) -> StoreResult<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM outbox WHERE seq <= ?1", params![up_to_seq])?;
            Ok(())
        })
    }

    /// Reads a client sync-state value.
    pub fn sync_state_get(&self, key: &str) -> StoreResult<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM sync_state WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(value)
        })
    }

    /// Writes a client sync-state value.
    pub fn sync_state_set(&self, key: &str, value: &[u8]) -> StoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?1, ?2)",
                params![key, value],
            )?;
            Ok(())
        })
    }
}
