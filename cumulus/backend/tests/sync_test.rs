use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

use cumulus_backend::CumulusBackend;
use cumulus_backend::CumulusConfig;
use cumulus_backend::CumulusOpHeadsStore;
use cumulus_backend::CumulusOpStore;
use cumulus_backend::SyncEngine;
use cumulus_proto::ids;
use cumulus_proto::v1;
use cumulus_server::build_router;
use cumulus_server::config::AuthConfig;
use cumulus_server::config::Config;
use cumulus_store::ObjectKind;
use cumulus_store::Store;
use futures::io::Cursor;
use jj_lib::backend::Backend as _;
use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CopyId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeValue;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore as _;
use jj_lib::op_store::OpStore as _;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationMetadata;
use jj_lib::op_store::RootOperationData;
use jj_lib::op_store::TimestampRange;
use jj_lib::op_store::View;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathComponentBuf;
use pollster::FutureExt as _;
use tokio_stream::wrappers::TcpListenerStream;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct TestServer {
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TestServer {
    fn start(data_dir: &Path) -> Self {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let config = Config {
            listen_addr: address,
            data_dir: data_dir.to_owned(),
            auth: AuthConfig::default(),
            tls: None,
        };
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind(address).await.unwrap();
                let router = build_router(&config).unwrap();
                ready_sender.send(()).unwrap();
                router
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        shutdown_receiver.await.ok();
                    })
                    .await
                    .unwrap();
            });
        });
        ready_receiver.recv().unwrap();
        Self {
            address,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.take().unwrap().send(()).ok();
        self.thread.take().unwrap().join().unwrap();
    }
}

struct Client {
    backend: CumulusBackend,
    op_store: CumulusOpStore,
    op_heads: CumulusOpHeadsStore,
    engine: SyncEngine,
}

fn init_client(path: &Path, url: &str) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let repo_path = path.join(".jj/repo");
    let backend_path = repo_path.join("store");
    let op_store_path = repo_path.join("op_store");
    let op_heads_path = repo_path.join("op_heads");
    std::fs::create_dir_all(&backend_path)?;
    std::fs::create_dir_all(&op_store_path)?;
    std::fs::create_dir_all(&op_heads_path)?;
    let config = CumulusConfig::from_repo_info(
        url,
        None,
        &v1::RepoInfo {
            name: "test".into(),
            commit_id_length: ids::COMMIT_ID_LENGTH as u32,
            change_id_length: ids::CHANGE_ID_LENGTH as u32,
            root_commit_id: ids::root_commit_id().to_bytes(),
            empty_tree_id: ids::empty_tree_id().to_bytes(),
            protocol_version: cumulus_proto::PROTOCOL_VERSION,
            op_mode: v1::OpMode::LocalFirst as i32,
            completeness: v1::Completeness::Unspecified as i32,
        },
    )?;
    let mut config = config;
    config.auto_push = false;
    let backend = CumulusBackend::init(&backend_path, config.clone())?;
    let root_data = RootOperationData {
        root_commit_id: backend.root_commit_id().clone(),
    };
    let op_store = CumulusOpStore::init(&op_store_path, root_data)?;
    let op_heads = CumulusOpHeadsStore::init(&op_heads_path, op_store.root_operation_id())?;
    let engine = SyncEngine::new(config, backend.store().clone())?;
    Ok(Client {
        backend,
        op_store,
        op_heads,
        engine,
    })
}

fn signature() -> Signature {
    Signature {
        name: "Test".into(),
        email: "test@example.com".into(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(1_700_000_000_000),
            tz_offset: 0,
        },
    }
}

fn operation_metadata() -> OperationMetadata {
    let timestamp = Timestamp {
        timestamp: MillisSinceEpoch(1_700_000_000_000),
        tz_offset: 0,
    };
    OperationMetadata {
        time: TimestampRange {
            start: timestamp,
            end: timestamp,
        },
        description: "test operation".into(),
        hostname: "host".into(),
        username: "user".into(),
        is_snapshot: false,
        workspace_name: Some(WorkspaceName::DEFAULT.to_owned()),
        attributes: HashMap::new().into_iter().collect(),
    }
}

#[test]
fn mode_a_push_and_pull_keep_tree_content_lazy() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    Store::open(&data_dir.path().join("repos/test"))?;
    let server = TestServer::start(data_dir.path());
    let first_dir = tempfile::tempdir()?;
    let first = init_client(first_dir.path(), &server.url())?;

    let file_id = first
        .backend
        .write_file(RepoPath::root(), &mut Cursor::new(b"content".to_vec()))
        .block_on()?;
    let tree = Tree::from_sorted_entries(vec![(
        RepoPathComponentBuf::new("file")?,
        TreeValue::File {
            id: file_id,
            executable: false,
            copy_id: CopyId::new(vec![]),
        },
    )]);
    let tree_id = first
        .backend
        .write_tree(RepoPath::root(), &tree)
        .block_on()?;
    let commit = Commit {
        parents: vec![first.backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id.clone()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::from_bytes(&[1; ids::CHANGE_ID_LENGTH]),
        description: "first".into(),
        author: signature(),
        committer: signature(),
        secure_sig: None,
    };
    let (commit_id, _) = first.backend.write_commit(commit, None).block_on()?;
    let mut view = View::make_root(first.backend.root_commit_id().clone());
    view.head_ids = HashSet::from([commit_id.clone()]);
    view.wc_commit_ids
        .insert(WorkspaceName::DEFAULT.to_owned(), commit_id.clone());
    let view_id = first.op_store.write_view(&view).block_on()?;
    let operation = Operation {
        view_id,
        parents: vec![first.op_store.root_operation_id().clone()],
        metadata: operation_metadata(),
        commit_predecessors: Some(Default::default()),
    };
    let operation_id = first.op_store.write_operation(&operation).block_on()?;
    first
        .op_heads
        .update_op_heads(&[first.op_store.root_operation_id().clone()], &operation_id)
        .block_on()?;

    let pushed = first.engine.push().block_on()?;
    assert_eq!(pushed.pushed_operations, 1);
    assert_eq!(pushed.pushed_commits, 1);
    assert_eq!(first.engine.status()?.outbox_depth, 0);

    let second_dir = tempfile::tempdir()?;
    let second = init_client(second_dir.path(), &server.url())?;
    let pulled = second.engine.pull().block_on()?;
    assert_eq!(pulled.pulled_operations, 1);
    assert_eq!(pulled.pulled_commits, 1);
    assert!(
        second
            .backend
            .store()
            .has_object(ObjectKind::Commit, commit_id.as_bytes())?
    );
    assert!(
        !second
            .backend
            .store()
            .has_object(ObjectKind::Tree, tree_id.as_bytes())?
    );
    assert_eq!(
        second.op_heads.get_op_heads().block_on()?,
        vec![operation_id]
    );
    assert_eq!(
        second.op_store.read_view(&operation.view_id).block_on()?,
        view
    );
    Ok(())
}
