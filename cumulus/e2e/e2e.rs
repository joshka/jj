use std::fs;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use assert_cmd::Command;
use cumulus_server::config::AuthConfig;
use cumulus_server::config::Config;
use tempfile::TempDir;
use tokio::sync::oneshot;

static COMMAND_ID: AtomicU64 = AtomicU64::new(1);

struct TestEnvironment {
    root: TempDir,
    config_dir: PathBuf,
}

impl TestEnvironment {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        fs::create_dir(&config_dir).unwrap();
        Self { root, config_dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn create_dir(&self, name: &str) -> PathBuf {
        let path = self.path(name);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn jj(&self, cwd: &Path, auto_push: bool) -> Command {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin!("cumulus-test-jj"));
        command
            .current_dir(cwd)
            .env_clear()
            .env("HOME", self.root.path())
            .env("JJ_CONFIG", &self.config_dir)
            .env("JJ_USER", "Cumulus Test")
            .env("JJ_EMAIL", "cumulus@example.com")
            .env("JJ_OP_HOSTNAME", "cumulus.test")
            .env("JJ_OP_USERNAME", "tester")
            .env(
                "JJ_RANDOMNESS_SEED",
                COMMAND_ID.fetch_add(1, Ordering::Relaxed).to_string(),
            )
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .args([
                "--config",
                if auto_push {
                    "cumulus.auto-push=true"
                } else {
                    "cumulus.auto-push=false"
                },
                "--config",
                "snapshot.max-new-file-size=100000000",
            ]);
        command
    }

    fn run(&self, cwd: &Path, auto_push: bool, args: &[&str]) -> Output {
        let output = self.jj(cwd, auto_push).args(args).output().expect("run jj");
        assert!(
            output.status.success(),
            "jj {args:?} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn init(&self, cwd: &Path, server: &Server, repo: &str, auto_push: bool) {
        self.run(
            cwd,
            auto_push,
            &[
                "cumulus",
                "init",
                "--server",
                &server.url(),
                "--repo",
                repo,
                "--create",
            ],
        );
    }

    fn clone(
        &self,
        cwd: &Path,
        server: &Server,
        repo: &str,
        destination: &str,
        sparse: Option<&str>,
        auto_push: bool,
    ) -> PathBuf {
        let source = format!("{}/{repo}", server.url());
        let mut args = vec!["cumulus", "clone", &source, destination];
        if let Some(pattern) = sparse {
            args.extend(["--sparse", pattern]);
        }
        self.run(cwd, auto_push, &args);
        cwd.join(destination)
    }
}

struct Server {
    addr: SocketAddr,
    data_dir: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn start(data_dir: PathBuf) -> Self {
        let addr = reserve_addr();
        Self::start_at(data_dir, addr)
    }

    fn start_at(data_dir: PathBuf, addr: SocketAddr) -> Self {
        let config = Config {
            listen_addr: addr,
            data_dir: data_dir.clone(),
            auth: AuthConfig::default(),
            tls: None,
        };
        let (shutdown, receiver) = oneshot::channel();
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let router = cumulus_server::build_router(&config).unwrap();
                router
                    .serve_with_shutdown(config.listen_addr, async {
                        receiver.await.ok();
                    })
                    .await
                    .unwrap();
            });
        });
        wait_for_server(addr);
        Self {
            addr,
            data_dir,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn repo_database(&self, repo: &str) -> PathBuf {
        self.data_dir.join("repos").join(repo).join("meta.sqlite")
    }

    fn stop(mut self) -> (PathBuf, SocketAddr) {
        self.shutdown.take().unwrap().send(()).ok();
        self.thread.take().unwrap().join().unwrap();
        (self.data_dir.clone(), self.addr)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).ok();
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn reserve_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn wait_for_server(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "server did not start at {addr}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn text(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn database_count(database: &Path, table: &str) -> usize {
    assert!(matches!(table, "blobs" | "ops" | "op_heads" | "outbox"));
    let connection = rusqlite::Connection::open(database).unwrap();
    let count: i64 = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap();
    usize::try_from(count).unwrap()
}

fn local_database(workspace: &Path) -> PathBuf {
    workspace.join(".jj/repo/store/cumulus/meta.sqlite")
}

fn write_file(workspace: &Path, relative: &str, contents: impl AsRef<[u8]>) {
    let path = workspace.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, contents).unwrap();
}

#[test]
fn roundtrip_and_cached_reads_work_offline() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace = environment.create_dir("a");
    environment.init(&workspace, &server, "roundtrip", false);

    write_file(&workspace, "hello", "one\n");
    environment.run(&workspace, false, &["describe", "-m", "first"]);
    environment.run(&workspace, false, &["bookmark", "create", "first"]);
    environment.run(&workspace, false, &["cumulus", "sync"]);
    assert!(text(&environment.run(&workspace, false, &["log", "-r", "all()"])).contains("first"));
    environment.run(&workspace, false, &["diff", "-r", "@"]);
    environment.run(&workspace, false, &["status"]);

    let _stopped = server.stop();
    environment.run(&workspace, false, &["log", "-r", "all()"]);
    environment.run(&workspace, false, &["diff", "-r", "@"]);
    environment.run(&workspace, false, &["status"]);
}

#[test]
fn two_clients_exchange_history_change_ids_and_bookmarks() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace_a = environment.create_dir("a");
    environment.init(&workspace_a, &server, "two-client", false);
    write_file(&workspace_a, "a", "a\n");
    environment.run(&workspace_a, false, &["describe", "-m", "from-a"]);
    environment.run(&workspace_a, false, &["bookmark", "create", "shared"]);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);

    let workspace_b = environment.clone(
        environment.root.path(),
        &server,
        "two-client",
        "b",
        None,
        false,
    );
    let log_b = text(&environment.run(
        &workspace_b,
        false,
        &[
            "log",
            "-r",
            "all()",
            "-T",
            r#"change_id ++ " " ++ description ++ "\n""#,
        ],
    ));
    assert!(log_b.contains("from-a"));
    assert!(text(&environment.run(&workspace_b, false, &["bookmark", "list"])).contains("shared"));
    environment.run(
        &workspace_b,
        false,
        &["evolog", "-r", "description(from-a)"],
    );

    write_file(&workspace_b, "b", "b\n");
    environment.run(&workspace_b, false, &["describe", "-m", "from-b"]);
    environment.run(&workspace_b, false, &["cumulus", "sync"]);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);
    assert!(
        text(&environment.run(&workspace_a, false, &["log", "-r", "all()"])).contains("from-b")
    );
}

#[test]
fn concurrent_clients_converge_without_losing_heads() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace_a = environment.create_dir("a");
    environment.init(&workspace_a, &server, "concurrent", false);
    environment.run(&workspace_a, false, &["describe", "-m", "base"]);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);
    let workspace_b = environment.clone(
        environment.root.path(),
        &server,
        "concurrent",
        "b",
        None,
        false,
    );

    write_file(&workspace_a, "a", "a\n");
    environment.run(&workspace_a, false, &["new", "-m", "branch-a"]);
    write_file(&workspace_b, "b", "b\n");
    environment.run(&workspace_b, false, &["new", "-m", "branch-b"]);
    environment.run(&workspace_a, false, &["cumulus", "sync", "--push-only"]);
    environment.run(&workspace_b, false, &["cumulus", "sync", "--push-only"]);
    assert_eq!(
        database_count(&server.repo_database("concurrent"), "op_heads"),
        2
    );

    environment.run(&workspace_a, false, &["cumulus", "sync", "--pull-only"]);
    environment.run(&workspace_b, false, &["cumulus", "sync", "--pull-only"]);
    for workspace in [&workspace_a, &workspace_b] {
        let log = text(&environment.run(workspace, false, &["log", "-r", "all()"]));
        assert!(log.contains("branch-a"));
        assert!(log.contains("branch-b"));
    }
}

#[test]
fn clone_is_lazy_and_sparse_checkout_fetches_only_selected_blobs() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace_a = environment.create_dir("a");
    environment.init(&workspace_a, &server, "lazy", false);
    for directory in 0..20 {
        for file in 0..10 {
            write_file(
                &workspace_a,
                &format!("dir{directory}/file{file}"),
                format!("{directory}-{file}-initial\n"),
            );
        }
    }
    environment.run(&workspace_a, false, &["describe", "-m", "initial"]);
    let old_id = text(&environment.run(
        &workspace_a,
        false,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
    ));
    for revision in 0..50 {
        write_file(
            &workspace_a,
            "dir0/file0",
            format!("historical version {revision}\n"),
        );
        environment.run(
            &workspace_a,
            false,
            &["new", "-m", &format!("revision-{revision}")],
        );
    }
    environment.run(&workspace_a, false, &["cumulus", "sync"]);

    let workspace_b = environment.clone(environment.root.path(), &server, "lazy", "b", None, false);
    let checkout_log = text(&environment.run(
        &workspace_b,
        false,
        &[
            "log",
            "-r",
            "@ | @-",
            "--no-graph",
            "-T",
            r#"commit_id ++ " " ++ description ++ "\n""#,
        ],
    ));
    assert!(
        workspace_b.join("dir0/file0").is_file(),
        "clone did not materialize head:\n{checkout_log}"
    );
    let before_diff = database_count(&local_database(&workspace_b), "blobs");
    assert!(
        (200..=201).contains(&before_diff),
        "blob count was {before_diff}"
    );
    environment.run(&workspace_b, false, &["diff", "-r", old_id.trim()]);
    let after_diff = database_count(&local_database(&workspace_b), "blobs");
    assert!(after_diff <= before_diff + 2);

    let workspace_c = environment.clone(
        environment.root.path(),
        &server,
        "lazy",
        "c",
        Some("dir1/**"),
        false,
    );
    assert_eq!(database_count(&local_database(&workspace_c), "blobs"), 10);
    environment.run(&workspace_c, false, &["sparse", "set", "--add", "dir2"]);
    assert_eq!(database_count(&local_database(&workspace_c), "blobs"), 20);
}

#[test]
fn rebase_and_push_do_not_fetch_an_unrelated_sparse_subtree() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace_a = environment.create_dir("a");
    environment.init(&workspace_a, &server, "rebase", false);
    write_file(&workspace_a, "dir1/local", "base local\n");
    write_file(&workspace_a, "dir2/remote", "base remote\n");
    environment.run(&workspace_a, false, &["describe", "-m", "base"]);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);

    let workspace_b = environment.clone(
        environment.root.path(),
        &server,
        "rebase",
        "b",
        Some("dir1/**"),
        false,
    );
    assert_eq!(database_count(&local_database(&workspace_b), "blobs"), 1);
    write_file(&workspace_b, "dir1/local", "local change\n");
    environment.run(&workspace_b, false, &["new", "-m", "local"]);

    write_file(&workspace_a, "dir2/remote", "remote change\n");
    environment.run(&workspace_a, false, &["new", "-m", "remote"]);
    let remote_id = text(&environment.run(
        &workspace_a,
        false,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
    ));
    environment.run(&workspace_a, false, &["cumulus", "sync"]);
    environment.run(&workspace_b, false, &["cumulus", "sync", "--pull-only"]);
    environment.run(
        &workspace_b,
        false,
        &["rebase", "-s", "description(local)", "-d", remote_id.trim()],
    );
    assert!(database_count(&local_database(&workspace_b), "blobs") <= 2);
    environment.run(&workspace_b, false, &["cumulus", "sync", "--push-only"]);
    environment.run(&workspace_a, false, &["cumulus", "sync", "--pull-only"]);
    assert!(text(&environment.run(&workspace_a, false, &["log", "-r", "all()"])).contains("local"));
}

#[test]
fn offline_queue_big_blob_and_root_operation_are_lossless() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace_a = environment.create_dir("a");
    environment.init(&workspace_a, &server, "offline", false);
    let roots = text(&environment.run(
        &workspace_a,
        false,
        &["op", "log", "--no-graph", "-T", r#"id ++ "\n""#],
    ));
    assert!(roots.contains(&"0".repeat(64)));
    let independent = environment.create_dir("independent");
    environment.init(&independent, &server, "independent", false);
    let independent_roots = text(&environment.run(
        &independent,
        false,
        &["op", "log", "--no-graph", "-T", r#"id ++ "\n""#],
    ));
    assert!(independent_roots.contains(&"0".repeat(64)));

    let big_blob = vec![b'x'; 50 * 1024 * 1024];
    write_file(&workspace_a, "large", &big_blob);
    environment.run(&workspace_a, false, &["describe", "-m", "large"]);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);
    let (data_dir, addr) = server.stop();

    for revision in 0..3 {
        environment.run(
            &workspace_a,
            false,
            &["new", "-m", &format!("offline-{revision}")],
        );
    }
    assert!(database_count(&local_database(&workspace_a), "outbox") >= 3);
    let status = text(&environment.run(&workspace_a, false, &["cumulus", "sync", "--status"]));
    assert!(status.contains("Outbox: 3"), "unexpected status:\n{status}");
    let failed_sync = environment
        .jj(&workspace_a, false)
        .args(["cumulus", "sync"])
        .output()
        .unwrap();
    assert!(!failed_sync.status.success());

    let server = Server::start_at(data_dir, addr);
    environment.run(&workspace_a, false, &["cumulus", "sync"]);
    assert_eq!(database_count(&local_database(&workspace_a), "outbox"), 0);
    let workspace_b = environment.clone(
        environment.root.path(),
        &server,
        "offline",
        "b",
        None,
        false,
    );
    let log = text(&environment.run(&workspace_b, false, &["log", "-r", "all()"]));
    let cloned_blob = fs::read(workspace_b.join("large")).unwrap_or_else(|error| {
        panic!(
            "clone omitted large blob ({error}); cached blobs: {}; log:\n{log}",
            database_count(&local_database(&workspace_b), "blobs")
        )
    });
    assert_eq!(cloned_blob, big_blob);
    assert!(log.contains("offline-0"));
    assert!(log.contains("offline-2"));
}

#[test]
fn auto_push_and_stalling_remote_do_not_block_mutations() {
    let environment = TestEnvironment::new();
    let server = Server::start(environment.path("server"));
    let workspace = environment.create_dir("a");
    environment.init(&workspace, &server, "auto", true);
    let initial_ops = database_count(&server.repo_database("auto"), "ops");
    write_file(&workspace, "auto", "push\n");
    environment.run(&workspace, true, &["describe", "-m", "auto-pushed"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while database_count(&server.repo_database("auto"), "ops") == initial_ops {
        assert!(Instant::now() < deadline, "detached push did not arrive");
        thread::sleep(Duration::from_millis(25));
    }

    let (data_dir, addr) = server.stop();
    let staller = StallingServer::start(addr);
    let started = Instant::now();
    environment.run(&workspace, true, &["new", "-m", "rapid-one"]);
    environment.run(&workspace, true, &["describe", "-m", "rapid-two"]);
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(staller);

    let server = Server::start_at(data_dir, addr);
    environment.run(&workspace, false, &["cumulus", "sync"]);
    assert_eq!(database_count(&local_database(&workspace), "outbox"), 0);
    assert_eq!(database_count(&server.repo_database("auto"), "op_heads"), 1);

    let no_auto = environment.create_dir("no-auto");
    environment.init(&no_auto, &server, "no-auto", false);
    write_file(&no_auto, "queued", "queued\n");
    environment.run(&no_auto, false, &["describe", "-m", "queued"]);
    thread::sleep(Duration::from_millis(200));
    assert!(database_count(&local_database(&no_auto), "outbox") > 0);
}

struct StallingServer {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl StallingServer {
    fn start(addr: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_thread = stop.clone();
        let thread = thread::spawn(move || {
            let mut connections = Vec::new();
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => connections.push(stream),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("stalling listener failed: {error}"),
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for StallingServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
