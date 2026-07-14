//! In-process integration tests for cumulusd (`cumulus/docs/SPEC.md` §10, §11 Run-1 exit
//! criteria): health, repo lifecycle, object/blob round trips, the
//! reachability CTE, op-head semantics, auth, and the UNIMPLEMENTED
//! surface of SPEC-ONLY RPCs.

use std::collections::HashMap;
use std::net::SocketAddr;

use cumulus_proto::convert;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_proto::v1::admin_service_client::AdminServiceClient;
use cumulus_proto::v1::index_service_client::IndexServiceClient;
use cumulus_proto::v1::object_service_client::ObjectServiceClient;
use cumulus_proto::v1::op_service_client::OpServiceClient;
use cumulus_proto::v1::repo_service_client::RepoServiceClient;
use cumulus_server::build_router;
use cumulus_server::config::Config;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::OperationMetadata;
use jj_lib::op_store::TimestampRange;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use prost::Message as _;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::Channel;

struct TestServer {
    addr: SocketAddr,
    _data_dir: tempfile::TempDir,
}

impl TestServer {
    async fn start(tokens: HashMap<String, String>) -> Self {
        let data_dir = tempfile::tempdir().unwrap();
        let config = Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            data_dir: data_dir.path().to_path_buf(),
            auth: cumulus_server::config::AuthConfig { tokens },
            tls: None,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = build_router(&config).unwrap();
        tokio::spawn(async move {
            router
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            addr,
            _data_dir: data_dir,
        }
    }

    async fn channel(&self) -> Channel {
        tonic::transport::Endpoint::from_shared(format!("http://{}", self.addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }
}

fn signature() -> Signature {
    Signature {
        name: "Test".to_owned(),
        email: "test@example.com".to_owned(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(1_700_000_000_000),
            tz_offset: 0,
        },
    }
}

fn make_commit(parents: Vec<CommitId>, description: &str) -> (CommitId, Commit, Vec<u8>) {
    let commit = Commit {
        parents,
        predecessors: vec![],
        root_tree: Merge::resolved(ids::empty_tree_id()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::new(cumulus_store::hash_bytes(description.as_bytes())[..16].to_vec()),
        description: description.to_owned(),
        author: signature(),
        committer: signature(),
        secure_sig: None,
    };
    let id = ids::commit_id(&commit);
    let data = convert::commit_to_proto(&commit).encode_to_vec();
    (id, commit, data)
}

fn commit_object(id: &CommitId, data: &[u8]) -> v1::Object {
    v1::Object {
        kind: v1::ObjectKind::Commit as i32,
        id: id.to_bytes(),
        data: data.to_vec(),
    }
}

async fn put_objects(
    client: &mut ObjectServiceClient<Channel>,
    repo: &str,
    objects: Vec<v1::Object>,
) -> Result<v1::PutObjectsResponse, tonic::Status> {
    let chunk = v1::ObjectChunk {
        repo: repo.to_owned(),
        objects,
    };
    Ok(client
        .put_objects(futures::stream::iter([chunk]))
        .await?
        .into_inner())
}

async fn collect_objects(
    mut stream: tonic::Streaming<v1::ObjectChunk>,
) -> Result<Vec<v1::Object>, tonic::Status> {
    let mut objects = vec![];
    while let Some(chunk) = stream.message().await? {
        objects.extend(chunk.objects);
    }
    Ok(objects)
}

fn make_view(head_ids: Vec<CommitId>) -> (ViewId, View, Vec<u8>) {
    let view = View {
        head_ids: head_ids.into_iter().collect(),
        local_bookmarks: Default::default(),
        local_tags: Default::default(),
        remote_views: Default::default(),
        git_refs: Default::default(),
        git_head: jj_lib::op_store::RefTarget::absent(),
        wc_commit_ids: Default::default(),
    };
    let id = ids::view_id(&view);
    let data = convert::view_to_proto(&view).encode_to_vec();
    (id, view, data)
}

fn make_op(
    parents: Vec<OperationId>,
    view_id: &ViewId,
    description: &str,
) -> (OperationId, Vec<u8>) {
    let operation = jj_lib::op_store::Operation {
        view_id: view_id.clone(),
        parents,
        metadata: OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 0,
                },
            },
            description: description.to_owned(),
            hostname: "host".to_owned(),
            username: "user".to_owned(),
            is_snapshot: false,
            workspace_name: None,
            attributes: Default::default(),
        },
        commit_predecessors: Some(Default::default()),
    };
    let id = ids::operation_id(&operation);
    let data = convert::operation_to_proto(&operation).encode_to_vec();
    (id, data)
}

fn root_op_id() -> OperationId {
    OperationId::new(vec![0; 64])
}

#[tokio::test]
async fn health_check() {
    let server = TestServer::start(HashMap::new()).await;
    let mut admin = AdminServiceClient::new(server.channel().await);
    let response = admin.health(v1::Empty {}).await.unwrap().into_inner();
    assert_eq!(response.status, "ok");
    assert_eq!(response.protocol_version, cumulus_proto::PROTOCOL_VERSION);
}

#[tokio::test]
async fn auth_required_when_tokens_configured() {
    let tokens = HashMap::from([("secret-token".to_owned(), "alice".to_owned())]);
    let server = TestServer::start(tokens).await;
    let channel = server.channel().await;

    // No token -> UNAUTHENTICATED.
    let mut admin = AdminServiceClient::new(channel.clone());
    let err = admin.health(v1::Empty {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // Wrong token -> UNAUTHENTICATED.
    let mut request = tonic::Request::new(v1::Empty {});
    request
        .metadata_mut()
        .insert("authorization", "Bearer wrong".parse().unwrap());
    let err = admin.health(request).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // Valid token -> OK.
    let mut request = tonic::Request::new(v1::Empty {});
    request
        .metadata_mut()
        .insert("authorization", "Bearer secret-token".parse().unwrap());
    let response = admin.health(request).await.unwrap().into_inner();
    assert_eq!(response.status, "ok");
}

#[tokio::test]
async fn repo_lifecycle() {
    let server = TestServer::start(HashMap::new()).await;
    let mut repos = RepoServiceClient::new(server.channel().await);

    let info = repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, "demo");
    assert_eq!(info.commit_id_length, 32);
    assert_eq!(info.change_id_length, 16);
    assert_eq!(info.root_commit_id, vec![0; 32]);
    assert_eq!(info.empty_tree_id, ids::empty_tree_id().to_bytes());
    assert_eq!(info.protocol_version, cumulus_proto::PROTOCOL_VERSION);
    assert_eq!(info.op_mode, v1::OpMode::LocalFirst as i32);

    // Idempotent by name.
    let again = repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(again, info);

    let fetched = repos
        .get_repo_info(v1::RepoRef {
            repo: "demo".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched, info);

    // Unknown repo -> NOT_FOUND.
    let err = repos
        .get_repo_info(v1::RepoRef {
            repo: "nope".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // Path-traversal-shaped names -> INVALID_ARGUMENT.
    for name in ["../evil", "a/b", "", ".hidden", "-flag"] {
        let err = repos
            .create_repo(v1::CreateRepoRequest {
                name: name.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "name {name:?}");
    }

    repos
        .create_repo(v1::CreateRepoRequest {
            name: "second".to_owned(),
        })
        .await
        .unwrap();
    let listed = repos.list_repos(v1::Empty {}).await.unwrap().into_inner();
    let names: Vec<_> = listed.repos.iter().map(|info| info.name.clone()).collect();
    assert_eq!(names, vec!["demo", "second"]);
}

#[tokio::test]
async fn object_roundtrip_and_reachability() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    let mut objects = ObjectServiceClient::new(channel);
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    // root -> a -> b, plus a tree and a symlink.
    let root = ids::root_commit_id();
    let (a_id, _, a_data) = make_commit(vec![root.clone()], "commit a");
    let (b_id, _, b_data) = make_commit(vec![a_id.clone()], "commit b");
    let tree = Tree::default();
    let tree_id = ids::tree_id(&tree);
    let tree_data = convert::tree_to_proto(&tree).encode_to_vec();
    let symlink_target = "target/path";
    let symlink_id = ids::symlink_id(symlink_target);

    let response = put_objects(
        &mut objects,
        "demo",
        vec![
            commit_object(&a_id, &a_data),
            commit_object(&b_id, &b_data),
            v1::Object {
                kind: v1::ObjectKind::Tree as i32,
                id: tree_id.to_bytes(),
                data: tree_data.clone(),
            },
            v1::Object {
                kind: v1::ObjectKind::Symlink as i32,
                id: symlink_id.to_bytes(),
                data: symlink_target.as_bytes().to_vec(),
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.object_count, 4);

    // HaveObjects: b present, an unknown id missing.
    let missing_ref = v1::ObjectRef {
        kind: v1::ObjectKind::Commit as i32,
        id: vec![9; 32],
    };
    let have = objects
        .have_objects(v1::HaveObjectsRequest {
            repo: "demo".to_owned(),
            refs: vec![
                v1::ObjectRef {
                    kind: v1::ObjectKind::Commit as i32,
                    id: b_id.to_bytes(),
                },
                missing_ref.clone(),
            ],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(have.missing, vec![missing_ref]);

    // GetObjects returns the stored bytes.
    let fetched = collect_objects(
        objects
            .get_objects(v1::GetObjectsRequest {
                repo: "demo".to_owned(),
                refs: vec![
                    v1::ObjectRef {
                        kind: v1::ObjectKind::Commit as i32,
                        id: a_id.to_bytes(),
                    },
                    v1::ObjectRef {
                        kind: v1::ObjectKind::Tree as i32,
                        id: tree_id.to_bytes(),
                    },
                ],
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].data, a_data);
    assert_eq!(fetched[1].data, tree_data);

    // Unknown object -> NOT_FOUND (raised before the stream starts).
    let err = objects
        .get_objects(v1::GetObjectsRequest {
            repo: "demo".to_owned(),
            refs: vec![v1::ObjectRef {
                kind: v1::ObjectKind::Commit as i32,
                id: vec![9; 32],
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // Reachability: full walk from b, parents first.
    let reachable = collect_objects(
        objects
            .get_commits_reachable_from(v1::ReachabilityRequest {
                repo: "demo".to_owned(),
                heads: vec![b_id.to_bytes()],
                have: vec![],
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    let reachable_ids: Vec<_> = reachable.iter().map(|object| object.id.clone()).collect();
    assert_eq!(reachable_ids, vec![a_id.to_bytes(), b_id.to_bytes()]);

    // Incremental: have=[a] -> only b.
    let reachable = collect_objects(
        objects
            .get_commits_reachable_from(v1::ReachabilityRequest {
                repo: "demo".to_owned(),
                heads: vec![b_id.to_bytes()],
                have: vec![a_id.to_bytes()],
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    let reachable_ids: Vec<_> = reachable.iter().map(|object| object.id.clone()).collect();
    assert_eq!(reachable_ids, vec![b_id.to_bytes()]);
}

#[tokio::test]
async fn put_objects_enforces_order_and_hashes() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    let mut objects = ObjectServiceClient::new(channel);
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    let root = ids::root_commit_id();
    let (a_id, _, a_data) = make_commit(vec![root], "commit a");
    let (b_id, _, b_data) = make_commit(vec![a_id.clone()], "commit b");

    // Child before parent -> FAILED_PRECONDITION (§6.1).
    let err = put_objects(&mut objects, "demo", vec![commit_object(&b_id, &b_data)])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Wrong id -> INVALID_ARGUMENT (content addressing).
    let err = put_objects(&mut objects, "demo", vec![commit_object(&b_id, &a_data)])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // Correct order works, and re-push is a no-op.
    for _ in 0..2 {
        put_objects(
            &mut objects,
            "demo",
            vec![commit_object(&a_id, &a_data), commit_object(&b_id, &b_data)],
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn blob_streaming_roundtrip() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    let mut objects = ObjectServiceClient::new(channel);
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    // 3 MiB, several frames each way.
    let blob: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let blob_id = ids::file_id(&blob);

    let mut frames = vec![v1::PutBlobFrame {
        frame: Some(v1::put_blob_frame::Frame::Header(
            v1::put_blob_frame::Header {
                repo: "demo".to_owned(),
                id: blob_id.to_bytes(),
                size: blob.len() as u64,
            },
        )),
    }];
    for chunk in blob.chunks(cumulus_proto::BLOB_FRAME_BYTES) {
        frames.push(v1::PutBlobFrame {
            frame: Some(v1::put_blob_frame::Frame::Data(chunk.to_vec())),
        });
    }
    let response = objects
        .put_blob(futures::stream::iter(frames))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.id, blob_id.to_bytes());
    assert_eq!(response.size, blob.len() as u64);

    // Stream it back; frames are offset-tagged and in order.
    let mut stream = objects
        .get_blob(v1::GetBlobRequest {
            repo: "demo".to_owned(),
            id: blob_id.to_bytes(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut fetched = vec![];
    while let Some(frame) = stream.message().await.unwrap() {
        assert_eq!(frame.offset, fetched.len() as u64);
        fetched.extend(frame.data);
    }
    assert_eq!(fetched, blob);

    // Unknown blob -> NOT_FOUND.
    let err = objects
        .get_blob(v1::GetBlobRequest {
            repo: "demo".to_owned(),
            id: vec![9; 32],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // Hash mismatch -> INVALID_ARGUMENT, blob not stored.
    let bad_frames = vec![
        v1::PutBlobFrame {
            frame: Some(v1::put_blob_frame::Frame::Header(
                v1::put_blob_frame::Header {
                    repo: "demo".to_owned(),
                    id: vec![9; 32],
                    size: 5,
                },
            )),
        },
        v1::PutBlobFrame {
            frame: Some(v1::put_blob_frame::Frame::Data(b"hello".to_vec())),
        },
    ];
    let err = objects
        .put_blob(futures::stream::iter(bad_frames))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn op_push_heads_and_pruning() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    let mut objects = ObjectServiceClient::new(channel.clone());
    let mut ops = OpServiceClient::new(channel);
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    // Fresh repo: no op heads.
    let heads = ops
        .get_op_heads(v1::RepoRef {
            repo: "demo".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(heads.op_head_ids, Vec::<Vec<u8>>::new());

    // A commit for the views to reference.
    let (commit_id, _, commit_data) = make_commit(vec![ids::root_commit_id()], "commit");
    put_objects(
        &mut objects,
        "demo",
        vec![commit_object(&commit_id, &commit_data)],
    )
    .await
    .unwrap();

    let (view1_id, _, view1_data) = make_view(vec![commit_id.clone()]);
    let (op1_id, op1_data) = make_op(vec![root_op_id()], &view1_id, "op 1");
    let (view2_id, _, view2_data) = make_view(vec![commit_id.clone(), ids::root_commit_id()]);
    let (op2_id, op2_data) = make_op(vec![op1_id.clone()], &view2_id, "op 2");

    // Push both ops oldest-first in one batch; op2 reuses op1's... distinct
    // views here, so both carry view data.
    let response = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![
                v1::OpWithView {
                    op_id: op1_id.to_bytes(),
                    op_data: op1_data.clone(),
                    view_id: view1_id.to_bytes(),
                    view_data: view1_data.clone(),
                },
                v1::OpWithView {
                    op_id: op2_id.to_bytes(),
                    op_data: op2_data.clone(),
                    view_id: view2_id.to_bytes(),
                    view_data: view2_data.clone(),
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();
    // Linear history: op1's head was pruned as an ancestor of op2.
    assert_eq!(response.op_head_ids, vec![op2_id.to_bytes()]);

    // A concurrent op on top of op1 -> two heads retained, never merged.
    let (op3_id, op3_data) = make_op(vec![op1_id.clone()], &view1_id, "op 3");
    let response = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: op3_id.to_bytes(),
                op_data: op3_data,
                // view1 is already stored; omit the data.
                view_id: view1_id.to_bytes(),
                view_data: vec![],
            }],
        })
        .await
        .unwrap()
        .into_inner();
    let mut heads = response.op_head_ids;
    heads.sort();
    let mut expected = vec![op2_id.to_bytes(), op3_id.to_bytes()];
    expected.sort();
    assert_eq!(heads, expected);

    // Idempotent re-push of an old op leaves the heads alone.
    let response = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: op1_id.to_bytes(),
                op_data: op1_data.clone(),
                view_id: view1_id.to_bytes(),
                view_data: view1_data.clone(),
            }],
        })
        .await
        .unwrap()
        .into_inner();
    let mut heads = response.op_head_ids;
    heads.sort();
    assert_eq!(heads, expected);

    // HaveOps.
    let response = ops
        .have_ops(v1::HaveOpsRequest {
            repo: "demo".to_owned(),
            op_ids: vec![op1_id.to_bytes(), vec![9; 64]],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.missing_op_ids, vec![vec![9; 64]]);

    // GetOps returns op and view bytes.
    let mut stream = ops
        .get_ops(v1::GetOpsRequest {
            repo: "demo".to_owned(),
            op_ids: vec![op2_id.to_bytes()],
        })
        .await
        .unwrap()
        .into_inner();
    let fetched = stream.message().await.unwrap().unwrap();
    assert_eq!(fetched.op_id, op2_id.to_bytes());
    assert_eq!(fetched.op_data, op2_data);
    assert_eq!(fetched.view_id, view2_id.to_bytes());
    assert_eq!(fetched.view_data, view2_data);
    assert!(stream.message().await.unwrap().is_none());
}

#[tokio::test]
async fn push_ops_preconditions() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    let mut ops = OpServiceClient::new(channel);
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    // View references a commit the server does not have (shallow check).
    let (view_id, _, view_data) = make_view(vec![CommitId::new(vec![9; 32])]);
    let (op_id, op_data) = make_op(vec![root_op_id()], &view_id, "op");
    let err = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: op_id.to_bytes(),
                op_data: op_data.clone(),
                view_id: view_id.to_bytes(),
                view_data: view_data.clone(),
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // A view referencing only the root commit is fine; but a missing parent
    // op is FAILED_PRECONDITION.
    let (root_view_id, _, root_view_data) = make_view(vec![ids::root_commit_id()]);
    let (orphan_op_id, orphan_op_data) =
        make_op(vec![OperationId::new(vec![7; 64])], &root_view_id, "orphan");
    let err = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: orphan_op_id.to_bytes(),
                op_data: orphan_op_data,
                view_id: root_view_id.to_bytes(),
                view_data: root_view_data.clone(),
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Omitted view data with no stored view -> FAILED_PRECONDITION.
    let (op2_id, op2_data) = make_op(vec![root_op_id()], &root_view_id, "no view");
    let err = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: op2_id.to_bytes(),
                op_data: op2_data.clone(),
                view_id: root_view_id.to_bytes(),
                view_data: vec![],
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Tampered op id -> INVALID_ARGUMENT.
    let err = ops
        .push_ops(v1::PushOpsRequest {
            repo: "demo".to_owned(),
            ops: vec![v1::OpWithView {
                op_id: vec![9; 64],
                op_data: op2_data,
                view_id: root_view_id.to_bytes(),
                view_data: root_view_data,
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn spec_only_rpcs_are_unimplemented() {
    let server = TestServer::start(HashMap::new()).await;
    let channel = server.channel().await;
    let mut repos = RepoServiceClient::new(channel.clone());
    repos
        .create_repo(v1::CreateRepoRequest {
            name: "demo".to_owned(),
        })
        .await
        .unwrap();

    let mut admin = AdminServiceClient::new(channel.clone());
    let err = admin
        .gc(v1::GcRequest {
            repo: "demo".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    let mut objects = ObjectServiceClient::new(channel.clone());
    let err = objects
        .get_blob_manifest(v1::GetBlobRequest {
            repo: "demo".to_owned(),
            id: vec![1; 32],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
    let err = objects
        .get_chunks(v1::GetChunksRequest {
            repo: "demo".to_owned(),
            chunk_ids: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    let mut ops = OpServiceClient::new(channel.clone());
    let err = ops
        .update_op_heads_cas(v1::UpdateOpHeadsCasRequest {
            repo: "demo".to_owned(),
            expected_heads: vec![],
            new_head: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
    let err = ops
        .subscribe_op_heads(v1::RepoRef {
            repo: "demo".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    let mut index = IndexServiceClient::new(channel);
    let err = index
        .get_index_manifest(v1::GetIndexManifestRequest {
            repo: "demo".to_owned(),
            op_id: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
    let err = index
        .get_index_segments(v1::GetIndexSegmentsRequest {
            repo: "demo".to_owned(),
            segment_ids: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
}

/// Run-1 exit criterion: the actual cumulusd binary starts with a sample
/// TOML config and answers a health check plus a create-repo /
/// put-objects / get-commits-reachable round trip.
#[tokio::test]
async fn cumulusd_binary_smoke() {
    let data_dir = tempfile::tempdir().unwrap();
    // Reserve an ephemeral port, then hand it to the binary. Slightly racy
    // in principle, harmless for a test.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let config_path = data_dir.path().join("cumulusd.toml");
    std::fs::write(
        &config_path,
        format!(
            "listen_addr = \"127.0.0.1:{port}\"\ndata_dir = {:?}\n\n[auth]\ntokens = {{ \
             \"smoke-token\" = \"smoke-user\" }}\n",
            data_dir.path().join("data")
        ),
    )
    .unwrap();

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_cumulusd"))
        .arg("--config")
        .arg(&config_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // Poll until the server answers (or time out).
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let channel = loop {
        match endpoint.connect().await {
            Ok(channel) => break channel,
            Err(err) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "cumulusd did not come up: {err}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };

    fn authed<T>(mut request: tonic::Request<T>) -> tonic::Request<T> {
        request
            .metadata_mut()
            .insert("authorization", "Bearer smoke-token".parse().unwrap());
        request
    }

    let result = async {
        let mut admin = AdminServiceClient::new(channel.clone());
        let health = admin
            .health(authed(tonic::Request::new(v1::Empty {})))
            .await?
            .into_inner();
        assert_eq!(health.status, "ok");

        let mut repos = RepoServiceClient::new(channel.clone());
        repos
            .create_repo(authed(tonic::Request::new(v1::CreateRepoRequest {
                name: "smoke".to_owned(),
            })))
            .await?;

        let mut objects = ObjectServiceClient::new(channel);
        let (a_id, _, a_data) = make_commit(vec![ids::root_commit_id()], "smoke a");
        let (b_id, _, b_data) = make_commit(vec![a_id.clone()], "smoke b");
        objects
            .put_objects(authed(tonic::Request::new(futures::stream::iter([
                v1::ObjectChunk {
                    repo: "smoke".to_owned(),
                    objects: vec![commit_object(&a_id, &a_data), commit_object(&b_id, &b_data)],
                },
            ]))))
            .await?;
        let stream = objects
            .get_commits_reachable_from(authed(tonic::Request::new(v1::ReachabilityRequest {
                repo: "smoke".to_owned(),
                heads: vec![b_id.to_bytes()],
                have: vec![],
            })))
            .await?
            .into_inner();
        let fetched = collect_objects(stream).await?;
        let fetched_ids: Vec<_> = fetched.iter().map(|object| object.id.clone()).collect();
        assert_eq!(fetched_ids, vec![a_id.to_bytes(), b_id.to_bytes()]);
        Ok::<_, tonic::Status>(())
    }
    .await;

    child.kill().unwrap();
    child.wait().unwrap();
    result.unwrap();
}
