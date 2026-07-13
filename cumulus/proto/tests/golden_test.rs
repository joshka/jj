//! Round-trip and id-stability goldens (SPEC.md §10).
//!
//! The pinned hex constants are the point: they freeze the identity scheme
//! (32-byte truncated Blake2b-512 over jj-lib's `ContentHash` canonical
//! form, §5). If one of these tests fails, the change breaks every existing
//! cumulus repo — do not update the constants without a migration story.

use cumulus_proto::convert::ConvertError;
use cumulus_proto::convert::commit_from_proto;
use cumulus_proto::convert::commit_relations;
use cumulus_proto::convert::commit_to_proto;
use cumulus_proto::convert::operation_from_proto;
use cumulus_proto::convert::operation_to_proto;
use cumulus_proto::convert::tree_from_proto;
use cumulus_proto::convert::tree_to_proto;
use cumulus_proto::convert::view_from_proto;
use cumulus_proto::convert::view_head_ids;
use cumulus_proto::convert::view_to_proto;
use cumulus_proto::ids;
use cumulus_proto::v1;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::SecureSig;
use jj_lib::backend::Signature;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::OperationMetadata;
use jj_lib::op_store::RefTarget;
use jj_lib::op_store::RemoteRef;
use jj_lib::op_store::RemoteRefState;
use jj_lib::op_store::RemoteView;
use jj_lib::op_store::TimestampRange;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use jj_lib::repo_path::RepoPathComponentBuf;
use maplit::btreemap;
use maplit::hashset;
use prost::Message as _;

fn signature(name: &str) -> Signature {
    Signature {
        name: name.to_owned(),
        email: format!("{name}@example.com"),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(1_700_000_000_000),
            tz_offset: -480,
        },
    }
}

/// A fixed commit whose id must never change.
fn golden_commit() -> Commit {
    Commit {
        parents: vec![ids::root_commit_id(), CommitId::new(vec![1; 32])],
        predecessors: vec![CommitId::new(vec![2; 32])],
        root_tree: Merge::resolved(ids::empty_tree_id()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::new(vec![3; 16]),
        description: "golden commit\n".to_owned(),
        author: signature("author"),
        committer: signature("committer"),
        secure_sig: None,
    }
}

fn conflicted_commit() -> Commit {
    Commit {
        parents: vec![CommitId::new(vec![1; 32]), CommitId::new(vec![2; 32])],
        predecessors: vec![],
        root_tree: Merge::from_vec(vec![
            TreeId::new(vec![4; 32]),
            TreeId::new(vec![5; 32]),
            TreeId::new(vec![6; 32]),
        ]),
        conflict_labels: Merge::from_vec(vec![
            "left".to_owned(),
            "base".to_owned(),
            "right".to_owned(),
        ]),
        change_id: ChangeId::new(vec![7; 16]),
        description: "conflicted".to_owned(),
        author: signature("author"),
        committer: signature("committer"),
        secure_sig: None,
    }
}

fn golden_tree() -> Tree {
    Tree::from_sorted_entries(vec![
        (
            RepoPathComponentBuf::new("dir").unwrap(),
            TreeValue::Tree(TreeId::new(vec![8; 32])),
        ),
        (
            RepoPathComponentBuf::new("exe").unwrap(),
            TreeValue::File {
                id: FileId::new(vec![9; 32]),
                executable: true,
                copy_id: CopyId::placeholder(),
            },
        ),
        (
            RepoPathComponentBuf::new("file").unwrap(),
            TreeValue::File {
                id: FileId::new(vec![10; 32]),
                executable: false,
                copy_id: CopyId::new(vec![11; 32]),
            },
        ),
        (
            RepoPathComponentBuf::new("link").unwrap(),
            TreeValue::Symlink(SymlinkId::new(vec![12; 32])),
        ),
        (
            RepoPathComponentBuf::new("submodule").unwrap(),
            TreeValue::GitSubmodule(CommitId::new(vec![13; 32])),
        ),
    ])
}

fn golden_view() -> View {
    let normal = |byte: u8| RefTarget::normal(CommitId::new(vec![byte; 32]));
    let conflicted = RefTarget::from_merge(Merge::from_vec(vec![
        Some(CommitId::new(vec![20; 32])),
        None,
        Some(CommitId::new(vec![21; 32])),
    ]));
    View {
        head_ids: hashset! {
            CommitId::new(vec![14; 32]),
            CommitId::new(vec![15; 32]),
        },
        local_bookmarks: btreemap! {
            "conflicted".into() => conflicted.clone(),
            "main".into() => normal(14),
        },
        local_tags: btreemap! {
            "v1.0".into() => normal(15),
        },
        remote_views: btreemap! {
            "origin".into() => RemoteView {
                bookmarks: btreemap! {
                    "main".into() => RemoteRef {
                        target: normal(16),
                        state: RemoteRefState::Tracked,
                    },
                    "new".into() => RemoteRef {
                        target: normal(17),
                        state: RemoteRefState::New,
                    },
                },
                tags: btreemap! {
                    "v1.0".into() => RemoteRef {
                        target: normal(15),
                        state: RemoteRefState::Tracked,
                    },
                },
            },
        },
        git_refs: btreemap! {
            "refs/heads/main".into() => normal(18),
        },
        git_head: RefTarget::absent(),
        wc_commit_ids: btreemap! {
            "default".into() => CommitId::new(vec![14; 32]),
            "laptop".into() => CommitId::new(vec![15; 32]),
        },
    }
}

fn golden_operation() -> jj_lib::op_store::Operation {
    jj_lib::op_store::Operation {
        view_id: ViewId::new(vec![22; 64]),
        parents: vec![OperationId::new(vec![23; 64])],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 60,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_001_000),
                    tz_offset: 60,
                },
            },
            description: "describe -m golden".to_owned(),
            hostname: "host".to_owned(),
            username: "user".to_owned(),
            is_snapshot: false,
            workspace_name: Some("default".into()),
            attributes: btreemap! {
                "key".to_owned() => "value".to_owned(),
            },
        },
        commit_predecessors: Some(btreemap! {
            CommitId::new(vec![24; 32]) => vec![CommitId::new(vec![25; 32])],
        }),
    }
}

// id-stability goldens (§5, §10)

#[test]
fn id_lengths() {
    assert_eq!(ids::commit_id(&golden_commit()).as_bytes().len(), 32);
    assert_eq!(ids::root_commit_id().as_bytes().len(), 32);
    assert_eq!(ids::root_change_id().as_bytes().len(), 16);
    assert_eq!(ids::file_id(b"data").as_bytes().len(), 32);
    assert_eq!(ids::operation_id(&golden_operation()).as_bytes().len(), 64);
    assert_eq!(ids::view_id(&golden_view()).as_bytes().len(), 64);
}

#[test]
fn empty_tree_id_golden() {
    // First 32 bytes of the Blake2b-512 ContentHash of the empty tree; the
    // full 64-byte value is pinned upstream in SimpleBackend::load.
    assert_eq!(
        ids::empty_tree_id().hex(),
        "482ae5a29fbe856c7272f2071b8b0f0359ee2d89ff392b8a900643fbd0836ecc"
    );
}

#[test]
fn commit_id_golden() {
    assert_eq!(
        ids::commit_id(&golden_commit()).hex(),
        "d0bab56cdaedb2758eb50fc90b55c9fbf06892380ef4eab975b624f42df8366c"
    );
}

#[test]
fn conflicted_commit_id_golden() {
    assert_eq!(
        ids::commit_id(&conflicted_commit()).hex(),
        "90ca2c38275754fbadc6aa5e9ceb6f6493271a5fc1be4a586bca9fdc89610555"
    );
}

#[test]
fn tree_id_golden() {
    assert_eq!(
        ids::tree_id(&golden_tree()).hex(),
        "9b28af0b982995392205c90e18396c48e30c876cf6c36a3fbc91c6b4c15cfa1a"
    );
}

#[test]
fn file_and_symlink_id_goldens() {
    // Truncated Blake2b-512 over raw bytes. The empty-input value is the
    // well-known Blake2b-512 empty digest, truncated.
    assert_eq!(
        ids::file_id(b"").hex(),
        "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419"
    );
    assert_eq!(
        ids::file_id(b"cumulus file contents\n").hex(),
        "403de5c08fcfb995f74a44952b464140f4167bcf085b0abc4750082461d471bd"
    );
    assert_eq!(
        ids::symlink_id("target/path").hex(),
        "7223a259eacbe37b6430d988aaf5334fa04c00cf5ea70c682ea57aa81a3fde52"
    );
    // File and symlink ids use the same raw-bytes hash.
    assert_eq!(
        ids::file_id(b"target/path").as_bytes(),
        ids::symlink_id("target/path").as_bytes()
    );
}

#[test]
fn operation_and_view_id_goldens() {
    assert_eq!(ids::view_id(&golden_view()).hex(), "3294a051f0a65b80528a6294eadeb04e247ece3444e09a11f20d8ffc35211565dcb20e2682ee77327f47a69dd59c8dcaf95cb5ec8985093ba10a91b73d230969");
    assert_eq!(ids::operation_id(&golden_operation()).hex(), "a41c87175f6e081b67898097a04d1366213315a287dfdc52f0e5cb3eb0ca71dffed85abc605baba45c326d8703566e19fdccc696540a60d5613b7c288c5915ae");
}

#[test]
fn commit_proto_encoding_golden() {
    // Guards accidental field renumbering; stored bytes are long-lived.
    let encoded = commit_to_proto(&golden_commit()).encode_to_vec();
    let hex = encoded.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write as _;
        write!(s, "{b:02x}").unwrap();
        s
    });
    assert_eq!(hex, "0a2000000000000000000000000000000000000000000000000000000000000000000a200101010101010101010101010101010101010101010101010101010101010101122002020202020202020202020202020202020202020202020202020202020202021a20482ae5a29fbe856c7272f2071b8b0f0359ee2d89ff392b8a900643fbd0836ecc2210030303030303030303030303030303032a0e676f6c64656e20636f6d6d69740a32300a06617574686f721212617574686f72406578616d706c652e636f6d1a120880d095ffbc3110a0fcffffffffffffff013a360a09636f6d6d69747465721215636f6d6d6974746572406578616d706c652e636f6d1a120880d095ffbc3110a0fcffffffffffffff01");
}

// round-trips (§10)

#[test]
fn commit_roundtrip() {
    for commit in [golden_commit(), conflicted_commit()] {
        let proto = commit_to_proto(&commit);
        let restored = commit_from_proto(proto.clone());
        assert_eq!(restored, commit);
        // Ids survive the round trip.
        assert_eq!(ids::commit_id(&restored), ids::commit_id(&commit));
        // And re-decoding the encoded bytes gives the same value.
        let reencoded = v1::Commit::decode(&*proto.encode_to_vec()).unwrap();
        assert_eq!(commit_from_proto(reencoded), commit);
    }
}

#[test]
fn signed_commit_roundtrip() {
    let mut commit = golden_commit();
    let unsigned_payload = commit_to_proto(&commit).encode_to_vec();
    commit.secure_sig = Some(SecureSig {
        data: unsigned_payload,
        sig: b"fake signature".to_vec(),
    });
    let restored = commit_from_proto(commit_to_proto(&commit));
    assert_eq!(restored, commit);
}

#[test]
fn commit_relations_extraction() {
    let proto = commit_to_proto(&golden_commit());
    let relations = commit_relations(&proto);
    assert_eq!(relations.parents, vec![vec![0; 32], vec![1; 32]],);
    assert_eq!(relations.change_id, vec![3; 16]);
}

#[test]
fn tree_roundtrip() {
    let tree = golden_tree();
    let restored = tree_from_proto(tree_to_proto(&tree)).unwrap();
    assert_eq!(restored, tree);
    assert_eq!(ids::tree_id(&restored), ids::tree_id(&tree));

    let empty = Tree::default();
    assert_eq!(tree_from_proto(tree_to_proto(&empty)).unwrap(), empty);
}

#[test]
fn tree_decode_rejects_bad_entries() {
    let proto = v1::Tree {
        entries: vec![v1::tree::Entry {
            name: "file".to_owned(),
            value: None,
        }],
    };
    assert!(matches!(
        tree_from_proto(proto),
        Err(ConvertError::MissingTreeValue(name)) if name == "file"
    ));

    let proto = v1::Tree {
        entries: vec![v1::tree::Entry {
            name: String::new(),
            value: Some(v1::TreeValue {
                value: Some(v1::tree_value::Value::TreeId(vec![1; 32])),
            }),
        }],
    };
    assert!(matches!(
        tree_from_proto(proto),
        Err(ConvertError::InvalidTreeEntryName(_))
    ));
}

#[test]
fn view_roundtrip() {
    let view = golden_view();
    let proto = view_to_proto(&view);
    let restored = view_from_proto(proto.clone()).unwrap();
    assert_eq!(restored, view);
    assert_eq!(ids::view_id(&restored), ids::view_id(&view));

    // The shallow head extraction sees the same ids as the full conversion.
    let mut heads = view_head_ids(&proto);
    heads.sort();
    let mut expected: Vec<_> = view.head_ids.iter().map(|id| id.to_bytes()).collect();
    expected.sort();
    assert_eq!(heads, expected);

    // A minimal root-like view also round-trips.
    let root_view = View::make_root(ids::root_commit_id());
    assert_eq!(
        view_from_proto(view_to_proto(&root_view)).unwrap(),
        root_view
    );
}

#[test]
fn operation_roundtrip() {
    let operation = golden_operation();
    let restored = operation_from_proto(operation_to_proto(&operation)).unwrap();
    assert_eq!(restored, operation);
    assert_eq!(ids::operation_id(&restored), ids::operation_id(&operation));

    // Without predecessor tracking.
    let mut operation = golden_operation();
    operation.commit_predecessors = None;
    operation.metadata.workspace_name = None;
    let restored = operation_from_proto(operation_to_proto(&operation)).unwrap();
    assert_eq!(restored, operation);
}

#[test]
fn operation_decode_validates_id_lengths() {
    let mut proto = operation_to_proto(&golden_operation());
    proto.view_id = vec![1; 32];
    assert!(matches!(
        operation_from_proto(proto),
        Err(ConvertError::InvalidHashLength {
            expected: 64,
            actual: 32
        })
    ));

    let mut proto = operation_to_proto(&golden_operation());
    proto.parents = vec![vec![1; 16]];
    assert!(matches!(
        operation_from_proto(proto),
        Err(ConvertError::InvalidHashLength {
            expected: 64,
            actual: 16
        })
    ));
}

#[test]
fn ref_target_terms_must_be_odd() {
    let mut proto = view_to_proto(&golden_view());
    proto.git_head = vec![
        v1::RefTargetTerm {
            value: Some(vec![1; 32]),
        },
        v1::RefTargetTerm { value: None },
    ];
    assert!(matches!(
        view_from_proto(proto),
        Err(ConvertError::EvenNumberOfRefTargetTerms(2))
    ));

    // Empty terms decode as an absent target (omitted-field compatibility).
    let mut proto = view_to_proto(&golden_view());
    proto.git_head = vec![];
    assert!(view_from_proto(proto).unwrap().git_head.is_absent());
}
