use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use assert_fs::prelude::*;
use assert_fs::TempDir;
use nfs3_client::tokio::TokioConnector;
use nfs3_client::Nfs3ConnectionBuilder;
use nfs3_types::nfs3::Nfs3Result;
use nfs3_types::nfs3::READDIR3args;
use predicates::prelude::*;
use realize_lib::logic::config::Config;
use realize_lib::model;
use realize_lib::model::Arena;
use realize_lib::model::Peer;
use realize_lib::network::config::PeerConfig;
use realize_lib::network::rpc::realstore;
use realize_lib::network::rpc::realstore::client::ClientOptions;
use realize_lib::network::rpc::realstore::Options;
use realize_lib::network::Networking;
use realize_lib::storage::config::ArenaConfig;
use realize_lib::storage::config::CacheConfig;
use reqwest::Client;
use tarpc::context;
use tokio::io::AsyncBufReadExt as _;
use tokio::process::Command;

fn command_path() -> PathBuf {
    // Expecting a path for the current exe to look like
    // target/debug/deps/integration_test
    // TODO: fix this. CARGO_BIN_EXE_ mysteriously stopped working
    // after transitioning to a workspace.
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("realize-daemon")
}

struct Fixture {
    pub config: Config,
    pub resources: PathBuf,
    pub testdir: PathBuf,
    pub tempdir: TempDir,
}

impl Fixture {
    pub async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        let mut config = Config::new();
        let arena = Arena::from("testdir");

        // Setup temp directory for the daemon to serve
        let tempdir = TempDir::new()?;

        let testdir = tempdir.child("testdir");
        testdir.create_dir_all()?;
        config.storage.arenas.insert(
            arena.clone(),
            ArenaConfig {
                path: testdir.to_path_buf(),
            },
        );

        testdir.child("foo.txt").write_str("hello")?;

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");

        config.network.peers.insert(
            Peer::from("a"),
            PeerConfig {
                address: None,
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
            },
        );
        config.network.peers.insert(
            Peer::from("b"),
            PeerConfig {
                address: None,
                pubkey: std::fs::read_to_string(resources.join("b-spki.pem"))?,
            },
        );

        Ok(Self {
            config,
            resources,
            testdir: testdir.to_path_buf(),
            tempdir,
        })
    }

    pub fn command(&self) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path());

        let config_file = self.tempdir.child("config.toml").to_path_buf();
        std::fs::write(&config_file, toml::to_string_pretty(&self.config)?)?;

        cmd.arg("--address")
            .arg("127.0.0.1:0")
            .arg("--privkey")
            .arg(self.resources.join("a.key"))
            .arg("--config")
            .arg(config_file)
            .env_remove("RUST_LOG")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        Ok(cmd)
    }
}

#[tokio::test]
async fn daemon_starts_and_lists_files() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()?
        .env(
            "RUST_LOG",
            "realize_lib::network=debug,realize_daemon=debug",
        )
        .spawn()?;

    // The first line that's output to stdout must be Listening on
    // <address>:<port>. Anything else is an error.
    let portstr = wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;

    let a = Peer::from("a");
    let mut peers = fixture.config.network.peers.clone();
    peers.get_mut(&a).expect("PeerConfig of a").address = Some(format!("127.0.0.1:{portstr}"));

    let networking = Networking::from_config(&peers, &fixture.resources.join("b.key"))?;
    let client = realstore::client::connect(&networking, &a, ClientOptions::default()).await?;
    let files = client
        .list(
            context::current(),
            Arena::from("testdir"),
            Options::default(),
        )
        .await??;
    assert_unordered::assert_eq_unordered!(
        files.into_iter().map(|f| f.path).collect(),
        vec![model::Path::parse("foo.txt")?]
    );

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    // Make sure stderr contains the expected log message.
    let stdout = collect_stdout(daemon.stdout.as_mut().unwrap()).await?;
    let stderr = collect_stderr(daemon.stderr.as_mut().unwrap()).await?;
    assert!(
        stderr.contains("Accepted peer b from "),
        "stderr: {stderr} stdout: {stdout}"
    );

    Ok(())
}

#[tokio::test]
async fn metrics_endpoint_works() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    let metrics_port = portpicker::pick_unused_port().expect("No ports free");
    let metrics_addr = format!("127.0.0.1:{}", metrics_port);

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()?
        .arg("--metrics-addr")
        .arg(&metrics_addr)
        .spawn()?;

    // Waiting for the daemon to be ready, to be sure the metrics
    // endpoint is up.
    wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;

    // Now test the endpoint
    let client = Client::new();
    let metrics_url = format!("http://{}/metrics", metrics_addr);
    let resp = client.get(&metrics_url).send().await?;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["Content-Type"], "text/plain; version=0.0.4");
    let body = resp.text().await?;
    assert!(
        body.contains("realize_daemon_up 1"),
        "metrics output missing expected Prometheus format: {}",
        body
    );

    // Random paths should not return anything
    // TODO: move to a unit test in src/metrics.rs
    let notfound = client
        .get(format!("http://{}/notfound", metrics_addr))
        .send()
        .await?;
    assert_eq!(notfound.status(), 404);

    Ok(())
}

#[tokio::test]
async fn daemon_fails_on_missing_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    fs::remove_dir_all(&fixture.testdir)?;

    let output = fixture.command()?.output().await?;
    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not exist"),
        "stderr: {stderr}, stdout: {stdout}"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_fails_on_unreadable_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    std::fs::set_permissions(&fixture.testdir, std::fs::Permissions::from_mode(0o000))?;

    let output = fixture.command()?.output().await?;
    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No read access"),
        "stderr: {stderr}, stdout: {stdout}"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_warns_on_unwritable_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    std::fs::set_permissions(&fixture.testdir, std::fs::Permissions::from_mode(0o500))?; // read+exec only

    let mut daemon = fixture.command()?.spawn()?;
    let _ = wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    let stdout = collect_stdout(daemon.stdout.as_mut().unwrap()).await?;
    let stderr = collect_stderr(daemon.stderr.as_mut().unwrap()).await?;
    assert!(
        stderr.contains("No write access"),
        "stderr: {stderr}, stdout: {stdout}"
    );
    Ok(())
}

#[tokio::test]
async fn daemon_systemd_log_output_format() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()?
        .env("RUST_LOG_FORMAT", "SYSTEMD")
        .spawn()?;

    wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;
    // Kill to make sure stderr ends
    daemon.start_kill()?;

    // Make sure stderr contains the expected log message.
    let stdout = collect_stdout(daemon.stdout.as_mut().unwrap()).await?;
    let stderr = collect_stderr(daemon.stderr.as_mut().unwrap()).await?;
    assert!(
        stderr.contains("<5>realize_daemon: Listening on 127.0.0.1:"),
        "stderr: {stderr} stdout: {stdout}"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_interrupted() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    let mut daemon = fixture
        .command()?
        .env(
            "RUST_LOG",
            "realize_lib::network::tcp=debug,realize_daemon=debug",
        )
        .spawn()?;

    let _ = wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;
    let pid = daemon.id().expect("no pid");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGTERM,
    )?;

    let status = daemon.wait().await?;
    assert_eq!(status.code(), Some(0));

    // Make sure stderr contains the expected log message.
    let stdout = collect_stdout(daemon.stdout.as_mut().unwrap()).await?;
    let stderr = collect_stderr(daemon.stderr.as_mut().unwrap()).await?;
    assert!(
        stderr.contains("Interrupted"),
        "stderr: {stderr} stdout: {stdout}"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_exports_nfs() -> anyhow::Result<()> {
    let mut fixture = Fixture::setup().await?;

    let nfs_port = portpicker::pick_unused_port().expect("No ports free");
    let nfs_addr = format!("127.0.0.1:{}", nfs_port);

    fixture.config.storage.cache = Some(CacheConfig {
        db: fixture.tempdir.child("cache.db").to_path_buf(),
    });

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()?
        .arg("--nfs")
        .arg(nfs_addr)
        .env("RUST_LOG", "debug")
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    // The first line that's output to stdout must be Listening on
    // <address>:<port>. Anything else is an error.
    wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;

    log::debug!("Connecting to NFS port {nfs_port}");
    let mut connection =
        Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1".to_string(), "/".to_string())
            .connect_from_privileged_port(false)
            .mount_port(nfs_port)
            .nfs3_port(nfs_port)
            .mount()
            .await?;

    log::debug!("Connected to NFS port {nfs_port}");
    let root = connection.root_nfs_fh3();
    let readdir = connection
        .readdir(READDIR3args {
            dir: root,
            cookie: 0,
            cookieverf: nfs3_types::nfs3::cookieverf3::default(),
            count: 128 * 1024 * 1024,
        })
        .await?;
    match readdir {
        Nfs3Result::Err(err) => panic!("readdir failed:{:?}", err),
        Nfs3Result::Ok(res) => {
            assert_unordered::assert_eq_unordered!(
                vec!["testdir"],
                res.reply
                    .entries
                    .0
                    .iter()
                    .map(|e| std::str::from_utf8(e.name.as_ref()).expect("utf-8"))
                    .collect::<Vec<_>>()
            );
        }
    };
    let _ = connection.unmount().await;

    daemon.start_kill()?;

    Ok(())
}

async fn collect_stderr(stderr: &mut tokio::process::ChildStderr) -> anyhow::Result<String> {
    let reader = tokio::io::BufReader::new(stderr);
    let mut lines = reader.lines();
    let mut all_lines = vec![];
    while let Ok(Some(line)) = lines.next_line().await {
        eprintln!("{}", line);
        all_lines.push(line);
    }

    Ok(all_lines.join("\n"))
}

async fn collect_stdout(stdout: &mut tokio::process::ChildStdout) -> anyhow::Result<String> {
    let reader = tokio::io::BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut all_lines = vec![];
    while let Ok(Some(line)) = lines.next_line().await {
        eprintln!("{}", line);
        all_lines.push(line);
    }

    Ok(all_lines.join("\n"))
}

async fn wait_for_listening_port(
    stdout: &mut tokio::process::ChildStdout,
) -> anyhow::Result<String> {
    let reader = tokio::io::BufReader::new(stdout);
    let mut lines = reader.lines();
    let line = lines.next_line().await?.unwrap();
    assert!(
        predicate::str::starts_with("Listening on").eval(&line),
        "Unexpected output: {line}"
    );

    Ok(line
        .split(":")
        .last()
        .expect("Unexpected output: {line}")
        .to_string())
}
