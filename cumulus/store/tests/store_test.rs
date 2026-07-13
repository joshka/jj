use assert_matches::assert_matches;
use cumulus_store::Blob;
use cumulus_store::CommitIngest;
use cumulus_store::INLINE_BLOB_MAX;
use cumulus_store::OBJECT_ID_LENGTH;
use cumulus_store::ObjectKind;
use cumulus_store::OpIngest;
use cumulus_store::Store;
use cumulus_store::StoreError;
use cumulus_store::hash_bytes;

fn new_store() -> (tempfile::TempDir, Store) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = Store::open(temp_dir.path()).unwrap();
    (temp_dir, store)
}

fn id(byte: u8) -> Vec<u8> {
    vec![byte; OBJECT_ID_LENGTH]
}

fn op_id(byte: u8) -> Vec<u8> {
    vec![byte; 64]
}

fn root_id() -> Vec<u8> {
    id(0)
}

/// Ingests a commit whose data is just its id, with the given parents.
fn put_commit(store: &Store, commit_id: &[u8], parents: &[Vec<u8>]) {
    store
        .put_commit(&CommitIngest {
            id: commit_id,
            data: commit_id,
            parents,
            change_id: &commit_id[..16],
        })
        .unwrap();
}

/// Pushes an op with a trivially-satisfied view precondition.
fn push_op(store: &Store, new_op_id: &[u8], parents: &[Vec<u8>]) -> Vec<Vec<u8>> {
    store
        .push_op(&OpIngest {
            op_id: new_op_id,
            op_data: new_op_id,
            parents,
            view_id: new_op_id,
            view_data: Some(new_op_id),
            view_head_commits: &[],
        })
        .unwrap()
}

#[test]
fn kv_roundtrip() {
    let (_dir, store) = new_store();
    assert_eq!(store.kv_get("repo_info").unwrap(), None);
    store.kv_set("repo_info", b"hello").unwrap();
    assert_eq!(store.kv_get("repo_info").unwrap(), Some(b"hello".to_vec()));
    store.kv_set("repo_info", b"world").unwrap();
    assert_eq!(store.kv_get("repo_info").unwrap(), Some(b"world".to_vec()));
    // Schema version was written at open.
    assert_eq!(store.kv_get("schema_version").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn object_roundtrip_all_kinds() {
    let (_dir, store) = new_store();
    put_commit(&store, &id(1), &[root_id()]);
    store
        .put_object(ObjectKind::Tree, &id(2), b"tree data")
        .unwrap();
    store
        .put_object(ObjectKind::Symlink, &id(3), b"target/path")
        .unwrap();

    assert_eq!(
        store.get_object(ObjectKind::Commit, &id(1)).unwrap(),
        Some(id(1))
    );
    assert_eq!(
        store.get_object(ObjectKind::Tree, &id(2)).unwrap(),
        Some(b"tree data".to_vec())
    );
    assert_eq!(
        store.get_object(ObjectKind::Symlink, &id(3)).unwrap(),
        Some(b"target/path".to_vec())
    );
    // Kinds are namespaced: a tree id is not a commit id.
    assert_eq!(store.get_object(ObjectKind::Commit, &id(2)).unwrap(), None);
    assert!(store.has_object(ObjectKind::Tree, &id(2)).unwrap());
    assert!(!store.has_object(ObjectKind::Tree, &id(9)).unwrap());

    // Content-addressed re-put is a no-op.
    store
        .put_object(ObjectKind::Tree, &id(2), b"tree data")
        .unwrap();

    let missing = store
        .missing_objects(&[
            (ObjectKind::Commit, id(1)),
            (ObjectKind::Tree, id(2)),
            (ObjectKind::Tree, id(9)),
            (ObjectKind::Symlink, id(9)),
        ])
        .unwrap();
    assert_eq!(
        missing,
        vec![(ObjectKind::Tree, id(9)), (ObjectKind::Symlink, id(9))]
    );
}

#[test]
fn commit_generations() {
    let (_dir, store) = new_store();
    // root (gen 0, unstored) -> a (1) -> b (2); merge m of (b, a) -> gen 3.
    put_commit(&store, &id(1), &[root_id()]);
    put_commit(&store, &id(2), &[id(1)]);
    put_commit(&store, &id(3), &[id(2), id(1)]);
    assert_eq!(store.commit_generation(&id(1)).unwrap(), Some(1));
    assert_eq!(store.commit_generation(&id(2)).unwrap(), Some(2));
    assert_eq!(store.commit_generation(&id(3)).unwrap(), Some(3));
    assert_eq!(store.commit_generation(&id(9)).unwrap(), None);
}

#[test]
fn commit_requires_parents_first() {
    let (_dir, store) = new_store();
    let orphan = CommitIngest {
        id: &id(2),
        data: &id(2),
        parents: &[id(1)],
        change_id: &id(2)[..16],
    };
    assert_matches!(
        store.put_commit(&orphan),
        Err(StoreError::MissingCommitParent(parent)) if parent == id(1)
    );
    // Nothing was stored.
    assert!(!store.has_object(ObjectKind::Commit, &id(2)).unwrap());

    let no_parents = CommitIngest {
        id: &id(2),
        data: &id(2),
        parents: &[],
        change_id: &id(2)[..16],
    };
    assert_matches!(
        store.put_commit(&no_parents),
        Err(StoreError::CommitWithoutParents(_))
    );

    // After the parent arrives, ingest succeeds.
    put_commit(&store, &id(1), &[root_id()]);
    put_commit(&store, &id(2), &[id(1)]);
}

#[test]
fn commit_changes_populated_at_ingest() {
    let (_dir, store) = new_store();
    let change_id = vec![7; 16];
    store
        .put_commit(&CommitIngest {
            id: &id(1),
            data: b"v1",
            parents: &[root_id()],
            change_id: &change_id,
        })
        .unwrap();
    store
        .put_commit(&CommitIngest {
            id: &id(2),
            data: b"v2",
            parents: &[root_id()],
            change_id: &change_id,
        })
        .unwrap();
    assert_eq!(
        store.commits_for_change(&change_id).unwrap(),
        vec![id(1), id(2)]
    );
    assert_eq!(
        store.commits_for_change(&[9; 16]).unwrap(),
        Vec::<Vec<u8>>::new()
    );
}

#[test]
fn reachability_walk() {
    let (_dir, store) = new_store();
    // root -> a -> b -> c
    //          \-> d ---^   (c merges b and d)
    let (a, b, d, c) = (id(1), id(2), id(3), id(4));
    put_commit(&store, &a, &[root_id()]);
    put_commit(&store, &b, std::slice::from_ref(&a));
    put_commit(&store, &d, std::slice::from_ref(&a));
    put_commit(&store, &c, &[b.clone(), d.clone()]);

    let ids =
        |rows: Vec<(Vec<u8>, Vec<u8>)>| rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>();

    // Full clone: everything reachable from c, parents before children.
    let rows = store
        .commits_reachable_from(std::slice::from_ref(&c), &[])
        .unwrap();
    let generation_of = |commit_id: &Vec<u8>| store.commit_generation(commit_id).unwrap().unwrap();
    let generations: Vec<i64> = rows.iter().map(|(id, _)| generation_of(id)).collect();
    assert!(generations.is_sorted());
    assert_eq!(rows.len(), 4);

    // Incremental: have=[b] stops the walk at b, but d and a survive because
    // the c->d->a path routes around the frontier (documented superset
    // semantics; harmless because ingest is idempotent).
    let rows = store
        .commits_reachable_from(std::slice::from_ref(&c), std::slice::from_ref(&b))
        .unwrap();
    assert_eq!(ids(rows), vec![a.clone(), d.clone(), c.clone()]);

    // have covering all heads -> nothing.
    let rows = store
        .commits_reachable_from(std::slice::from_ref(&c), std::slice::from_ref(&c))
        .unwrap();
    assert_eq!(rows, vec![]);

    // Unknown head ids are skipped, not errors.
    let rows = store.commits_reachable_from(&[id(9)], &[]).unwrap();
    assert_eq!(rows, vec![]);

    // Multiple heads unioned.
    let rows = store
        .commits_reachable_from(&[b.clone(), d.clone()], std::slice::from_ref(&a))
        .unwrap();
    assert_eq!(ids(rows).len(), 2);
}

#[test]
fn blob_inline_and_cas() {
    let (_dir, store) = new_store();

    let small = b"small blob".to_vec();
    let small_id = store.put_blob(&small).unwrap();
    assert_eq!(small_id, hash_bytes(&small));
    assert_matches!(
        store.get_blob(&small_id).unwrap(),
        Some(Blob::Inline(data)) if data == small
    );

    let large = vec![42u8; INLINE_BLOB_MAX as usize + 1];
    let large_id = store.put_blob(&large).unwrap();
    assert_eq!(large_id, hash_bytes(&large));
    let blob = store.get_blob(&large_id).unwrap().unwrap();
    assert_matches!(&blob, Blob::File { size, .. } if *size == large.len() as u64);
    assert_eq!(blob.read_all().unwrap(), large);

    assert!(store.has_blob(&small_id).unwrap());
    assert!(!store.has_blob(&id(9)).unwrap());
    assert_matches!(store.get_blob(&id(9)).unwrap(), None);

    // Idempotent re-put.
    assert_eq!(store.put_blob(&large).unwrap(), large_id);
}

#[test]
fn blob_writer_verifies_expected_id() {
    let (_dir, store) = new_store();
    let mut writer = store.blob_writer().unwrap();
    writer.write(b"some bytes").unwrap();
    let err = writer.finish(Some(&id(9))).unwrap_err();
    assert_matches!(err, StoreError::BlobHashMismatch { .. });
    assert!(!store.has_blob(&hash_bytes(b"some bytes")).unwrap());

    let mut writer = store.blob_writer().unwrap();
    writer.write(b"some bytes").unwrap();
    let finished = writer.finish(Some(&hash_bytes(b"some bytes"))).unwrap();
    store.finish_blob(&finished).unwrap();
    assert!(store.has_blob(&hash_bytes(b"some bytes")).unwrap());
}

#[test]
fn blob_streamed_in_chunks() {
    let (_dir, store) = new_store();
    let mut writer = store.blob_writer().unwrap();
    let chunk = vec![7u8; 1 << 16];
    for _ in 0..3 {
        writer.write(&chunk).unwrap();
    }
    let finished = writer.finish(None).unwrap();
    let blob_id = store.finish_blob(&finished).unwrap();
    let blob = store.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(blob.size(), 3 * (1 << 16));
    let mut expected = vec![];
    for _ in 0..3 {
        expected.extend_from_slice(&chunk);
    }
    assert_eq!(hash_bytes(&expected), blob_id);
    assert_eq!(blob.read_all().unwrap(), expected);
}

#[test]
fn op_push_linear_heads() {
    let (_dir, store) = new_store();
    let heads = push_op(&store, &op_id(1), &[op_id(0)]);
    assert_eq!(heads, vec![op_id(1)]);
    let heads = push_op(&store, &op_id(2), &[op_id(1)]);
    assert_eq!(heads, vec![op_id(2)]);
    assert_eq!(store.op_heads().unwrap(), vec![op_id(2)]);
    // Ops and views readable back.
    let (data, view_id) = store.get_op(&op_id(1)).unwrap().unwrap();
    assert_eq!(data, op_id(1));
    assert_eq!(view_id, op_id(1));
    assert_eq!(store.get_view(&op_id(1)).unwrap(), Some(op_id(1)));
}

#[test]
fn op_push_concurrent_heads_retained_and_merged() {
    let (_dir, store) = new_store();
    push_op(&store, &op_id(1), &[op_id(0)]);
    // Two clients push children of op 1 concurrently: both heads retained,
    // never merged server-side.
    push_op(&store, &op_id(2), &[op_id(1)]);
    let heads = push_op(&store, &op_id(3), &[op_id(1)]);
    assert_eq!(heads.len(), 2);
    assert!(heads.contains(&op_id(2)) && heads.contains(&op_id(3)));
    // A client-side merge op with both as parents collapses the heads.
    let heads = push_op(&store, &op_id(4), &[op_id(2), op_id(3)]);
    assert_eq!(heads, vec![op_id(4)]);
}

#[test]
fn op_repush_is_noop() {
    let (_dir, store) = new_store();
    push_op(&store, &op_id(1), &[op_id(0)]);
    push_op(&store, &op_id(2), &[op_id(1)]);
    // Re-pushing an old (ancestor) op must not resurrect it as a head.
    let heads = push_op(&store, &op_id(1), &[op_id(0)]);
    assert_eq!(heads, vec![op_id(2)]);
    // Re-pushing the current head is also a no-op.
    let heads = push_op(&store, &op_id(2), &[op_id(1)]);
    assert_eq!(heads, vec![op_id(2)]);
}

#[test]
fn op_push_preconditions() {
    let (_dir, store) = new_store();
    // Missing parent op.
    let err = store
        .push_op(&OpIngest {
            op_id: &op_id(2),
            op_data: &op_id(2),
            parents: &[op_id(1)],
            view_id: &op_id(2),
            view_data: Some(&op_id(2)),
            view_head_commits: &[],
        })
        .unwrap_err();
    assert_matches!(err, StoreError::MissingOpParent(parent) if parent == op_id(1));

    // Missing view (not in batch, not in store).
    let err = store
        .push_op(&OpIngest {
            op_id: &op_id(1),
            op_data: &op_id(1),
            parents: &[op_id(0)],
            view_id: &op_id(9),
            view_data: None,
            view_head_commits: &[],
        })
        .unwrap_err();
    assert_matches!(err, StoreError::MissingView { .. });

    // View head commit not in objects.
    let err = store
        .push_op(&OpIngest {
            op_id: &op_id(1),
            op_data: &op_id(1),
            parents: &[op_id(0)],
            view_id: &op_id(1),
            view_data: Some(&op_id(1)),
            view_head_commits: &[id(5)],
        })
        .unwrap_err();
    assert_matches!(err, StoreError::MissingViewHead { .. });
    // The failed push left no op behind (transactional).
    assert!(!store.has_op(&op_id(1)).unwrap());

    // The all-zeros root commit id counts as present.
    store
        .push_op(&OpIngest {
            op_id: &op_id(1),
            op_data: &op_id(1),
            parents: &[op_id(0)],
            view_id: &op_id(1),
            view_data: Some(&op_id(1)),
            view_head_commits: &[root_id()],
        })
        .unwrap();

    // And once the commit exists, the check passes.
    put_commit(&store, &id(5), &[root_id()]);
    store
        .push_op(&OpIngest {
            op_id: &op_id(2),
            op_data: &op_id(2),
            parents: &[op_id(1)],
            view_id: &op_id(2),
            view_data: Some(&op_id(2)),
            view_head_commits: &[id(5)],
        })
        .unwrap();
}

#[test]
fn missing_ops_batch() {
    let (_dir, store) = new_store();
    push_op(&store, &op_id(1), &[op_id(0)]);
    let missing = store.missing_ops(&[op_id(1), op_id(2), op_id(3)]).unwrap();
    assert_eq!(missing, vec![op_id(2), op_id(3)]);
}

#[test]
fn outbox_and_sync_state() {
    let (_dir, store) = new_store();
    let seq1 = store.outbox_append(1, &id(1)).unwrap();
    let seq2 = store.outbox_append(2, &id(2)).unwrap();
    assert!(seq2 > seq1);
    assert_eq!(
        store.outbox_list().unwrap(),
        vec![(seq1, 1, id(1)), (seq2, 2, id(2))]
    );
    store.outbox_drain(seq1).unwrap();
    assert_eq!(store.outbox_list().unwrap(), vec![(seq2, 2, id(2))]);

    assert_eq!(
        store.sync_state_get("remote/origin/last_op_heads").unwrap(),
        None
    );
    store
        .sync_state_set("remote/origin/last_op_heads", b"heads")
        .unwrap();
    assert_eq!(
        store.sync_state_get("remote/origin/last_op_heads").unwrap(),
        Some(b"heads".to_vec())
    );
}

#[test]
fn reopen_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        put_commit(&store, &id(1), &[root_id()]);
        store
            .put_blob(&vec![1u8; INLINE_BLOB_MAX as usize * 2])
            .unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert!(store.has_object(ObjectKind::Commit, &id(1)).unwrap());
    assert!(
        store
            .has_blob(&hash_bytes(&vec![1u8; INLINE_BLOB_MAX as usize * 2]))
            .unwrap()
    );
}

#[test]
fn reserved_cdc_tables_exist_and_are_empty() {
    // Spec §13.1: blob_chunks/chunk_data are created empty in v1 so the CDC
    // upgrade needs no migration.
    let (_dir, store) = new_store();
    store
        .put_blob(&vec![9u8; INLINE_BLOB_MAX as usize * 2])
        .unwrap();
    // No public API populates them; verify at the SQL level.
    let conn = rusqlite::Connection::open(store.dir().join("meta.sqlite")).unwrap();
    let chunks: i64 = conn
        .query_row("SELECT COUNT(*) FROM blob_chunks", [], |row| row.get(0))
        .unwrap();
    let chunk_data: i64 = conn
        .query_row("SELECT COUNT(*) FROM chunk_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!((chunks, chunk_data), (0, 0));
}
