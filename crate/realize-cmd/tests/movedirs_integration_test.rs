use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use assert_unordered::assert_eq_unordered;
use hyper_util::rt::TokioIo;
use realize_lib::model::Arena;
use realize_lib::model::Peer;
use realize_lib::network::hostport::HostPort;
use realize_lib::network::rpc::realstore;
use realize_lib::network::security::PeerVerifier;
use realize_lib::network::security::RawPublicKeyResolver;
use realize_lib::network::Networking;
use realize_lib::network::Server;
use realize_lib::storage::real::RealStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::SubjectPublicKeyInfoDer;
use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Output;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;

fn command_path() -> PathBuf {
    // Expecting a path for the current exe to look like
    // target/debug/deps/move_file_integration_test
    // TODO: fix this. CARGO_BIN_EXE_ mysteriously stopped working
    // after transitioning to a workspace.
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("realize-cmd")
}

struct Fixture {
    tempdir: TempDir,
    arenas: Vec<(Arena, PathBuf, PathBuf)>,
    src_dir: ChildPath,
    dst_dir: ChildPath,
    arena: Arena,
    keys: TestKeys,
    src_addr: Option<SocketAddr>,
    dst_addr: Option<SocketAddr>,
    _src_server: Option<Arc<Server>>,
    _dst_server: Option<Arc<Server>>,
}

impl Fixture {
    async fn setup_and_start_servers() -> anyhow::Result<Self> {
        let mut fixture = Self::setup().await?;
        fixture.start_servers().await?;

        Ok(fixture)
    }
    async fn setup() -> anyhow::Result<Self> {
        env_logger::try_init().ok();

        let tempdir = TempDir::new()?;
        let src_dir = tempdir.child("src");
        src_dir.create_dir_all()?;

        let dst_dir = tempdir.child("dst");
        dst_dir.create_dir_all()?;

        let arena = Arena::from("dir");

        let keys = test_keys();

        Ok(Self {
            tempdir,
            arenas: vec![(arena.clone(), src_dir.to_path_buf(), dst_dir.to_path_buf())],
            src_dir,
            dst_dir,
            arena,
            keys,
            src_addr: None,
            dst_addr: None,
            _src_server: None,
            _dst_server: None,
        })
    }

    async fn start_servers(&mut self) -> anyhow::Result<()> {
        let verifier = setup_verifier(&self.keys);

        let mut src_server = Server::new(Networking::new(
            vec![],
            RawPublicKeyResolver::from_private_key_file(&self.keys.privkey_a_path)?,
            Arc::clone(&verifier),
        ));
        realstore::server::register(
            &mut src_server,
            RealStore::new(
                self.arenas
                    .iter()
                    .map(|(a, src_path, _)| (a.clone(), src_path.clone())),
            ),
        );
        let src_server = Arc::new(src_server);
        let src_addr = Arc::clone(&src_server)
            .listen(&HostPort::localhost(0))
            .await?;

        let mut dst_server = Server::new(Networking::new(
            vec![],
            RawPublicKeyResolver::from_private_key_file(&self.keys.privkey_b_path)?,
            verifier,
        ));
        realstore::server::register(
            &mut dst_server,
            RealStore::new(
                self.arenas
                    .iter()
                    .map(|(a, _, dst_path)| (a.clone(), dst_path.clone())),
            ),
        );
        let dst_server = Arc::new(dst_server);
        let dst_addr = Arc::clone(&dst_server)
            .listen(&HostPort::localhost(0))
            .await?;

        self._src_server = Some(src_server);
        self.src_addr = Some(src_addr);
        self._dst_server = Some(dst_server);
        self.dst_addr = Some(dst_addr);

        Ok(())
    }

    /// A command with all required args.
    fn command(&self) -> anyhow::Result<Command> {
        let mut cmd = Command::new(command_path());
        cmd.arg("--config")
            .arg(write_config(
                self.tempdir.child("config.toml"),
                self.src_addr.expect("src server started"),
                self.dst_addr.expect("dst server started"),
                &self.keys,
            )?)
            .arg("--src")
            .arg("a")
            .arg("--dst")
            .arg("b")
            .arg("--privkey")
            .arg(self.keys.privkey_a_path.as_ref())
            .arg(self.arena.as_str())
            .kill_on_drop(true);

        Ok(cmd)
    }

    /// A command with extra arguments.
    async fn run_with_args(&self, args: Vec<&str>) -> anyhow::Result<Output> {
        let mut cmd = self.command()?;
        for arg in args {
            cmd.arg(arg);
        }
        let output = cmd.output().await?;

        Ok(output)
    }

    /// Run the command and return its output.
    async fn run(&self) -> anyhow::Result<Output> {
        Ok(self.command()?.output().await?)
    }
}

#[tokio::test]
async fn move_file() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(&fixture.src_dir, &[("qux.txt", "qux")])?;

    let output = fixture.run().await?;
    assert!(
        fixture.run().await?.status.success(),
        "stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("qux.txt")]
    );
    assert_eq_unordered!(dir_content(&fixture.src_dir)?, vec![]);

    Ok(())
}

#[tokio::test]
async fn partial_failure() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(&fixture.src_dir, &[("good.txt", "ok"), ("bad.txt", "fail")])?;
    fs::set_permissions(
        fixture.src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o000),
    )?;

    let result = fixture.run().await;

    // Restore permissions for cleanup
    fs::set_permissions(
        fixture.src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o644),
    )?;

    let output = result?;
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ERROR      [1/2] bad.txt: "),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("ERROR: 1 file(s) failed, 1 file(s) moved, 0 interrupted"),
        "stderr: {stderr}"
    );
    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("good.txt")]
    );

    Ok(())
}

#[tokio::test]
async fn success_output() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(
        &fixture.src_dir,
        &[("foo.txt", "hello"), ("bar.txt", "world")],
    )?;
    let output = fixture.run().await?;
    assert!(output.status.success(), "Should succeed if all files moved");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("SUCCESS 2 file(s) moved"),
        "stdout: {stdout}"
    );
    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(dir_content(&fixture.src_dir)?, vec![]);
    Ok(())
}

#[tokio::test]
async fn progress_output() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(
        &fixture.src_dir,
        &[("foo.txt", "hello"), ("bar.txt", "world")],
    )?;
    let output = fixture.run().await?;
    assert!(output.status.success(), "Should succeed if all files moved");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Moved      [1/2] bar.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("Moved      [2/2] foo.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("SUCCESS 2 file(s) moved"),
        "stdout: {stdout}"
    );
    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(dir_content(&fixture.src_dir)?, vec![]);
    Ok(())
}

#[tokio::test]
async fn quiet_success() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(
        &fixture.src_dir,
        &[("foo.txt", "hello"), ("bar.txt", "world")],
    )?;
    let output = fixture.run_with_args(vec!["--output", "quiet"]).await?;
    assert!(output.status.success(), "Should succeed if all files moved");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.trim().is_empty(),
        "stdout should be empty in quiet mode, got: {stdout}"
    );
    assert!(
        stderr.trim().is_empty(),
        "stderr should be empty in quiet mode on success, got: {stderr}"
    );
    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(dir_content(&fixture.src_dir)?, vec![]);
    Ok(())
}

#[tokio::test]
async fn quiet_failure() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(&fixture.src_dir, &[("good.txt", "ok"), ("bad.txt", "fail")])?;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        fixture.src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o000),
    )?;
    let result = fixture.run_with_args(vec!["--output", "quiet"]).await;
    // Restore permissions for cleanup
    fs::set_permissions(
        fixture.src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o644),
    )?;
    let output = result?;
    assert!(!output.status.success(), "Should fail if any file fails");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.trim().is_empty(),
        "stdout should be empty in quiet mode, got: {stdout}"
    );
    assert!(
        stderr.contains("ERROR"),
        "stderr should contain error in quiet mode, got: {stderr}"
    );
    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("good.txt")]
    );
    Ok(())
}

#[tokio::test]
async fn log_output_events() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(
        &fixture.src_dir,
        &[("foo.txt", "hello"), ("bar.txt", "world")],
    )?;
    // Run with --output log, clear RUST_LOG so code sets it
    let output = fixture
        .command()?
        .arg("--output")
        .arg("log")
        .env_remove("RUST_LOG")
        .output()
        .await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // 1. stdout must be empty
    assert!(
        stdout.trim().is_empty(),
        "stdout should be empty in log mode, got: {stdout}"
    );

    // 2. stderr must contain log entries for all expected events
    // (MovingDir, MovingFile, CopyingFile, FileSuccess, summary)
    let mut found = vec![];
    for line in stderr.lines() {
        if line.contains("files to move") {
            found.push("MovingDir");
        }
        if line.contains("Preparing to move") {
            found.push("MovingFile");
        }
        if line.contains("Copying") {
            found.push("CopyingFile");
        }
        if line.contains("Moved") {
            found.push("FileSuccess");
        }
        if line.contains("SUCCESS") {
            found.push("Summary");
        }
        // There should be no ERROR or WARN lines in a successful run
        assert!(
            !line.contains("ERROR") && !line.contains("WARN"),
            "Unexpected error/warning in log: {line}"
        );
    }
    // Each event should be present at least once
    for event in &[
        "MovingDir",
        "MovingFile",
        "CopyingFile",
        "FileSuccess",
        "Summary",
    ] {
        assert!(
            found.contains(event),
            "Missing log entry for {event} in log output: {stderr}"
        );
    }
    // 3. stderr should not be empty (should contain logs)
    assert!(
        !stderr.trim().is_empty(),
        "stderr should contain log output in log mode"
    );
    Ok(())
}

#[tokio::test]
async fn systemd_log_output_format() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(&fixture.src_dir, &[("foo.txt", "bar")])?;
    let output = fixture
        .command()?
        .arg("--output")
        .arg("log")
        .env_remove("RUST_LOG")
        .env("RUST_LOG_FORMAT", "SYSTEMD")
        .output()
        .await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        eprintln!("{}", line);

        if line.starts_with("<5>realize_cmd::progress: Moved foo.txt") {
            return Ok(());
        }
    }

    panic!("stderr did not contain the expected message. stderr: {stderr} stdout: {stdout}",)
}

#[tokio::test]
async fn max_duration_timeout() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;

    let output = fixture
        .run_with_args(vec!["--max-duration", "10ms"])
        .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(11),
        "Should exit with code 11 on timeout. stderr: {stderr} stdout: {stdout}"
    );
    assert!(
        stderr.contains("INTERRUPTED 0 file(s) moved, 0 interrupted"),
        "stderr: {stderr} stdout: {stdout}"
    );
    Ok(())
}

#[tokio::test]
async fn realize_metrics_export() -> anyhow::Result<()> {
    use reqwest::Client;
    use tokio::io::AsyncBufReadExt as _;

    let fixture = Fixture::setup_and_start_servers().await?;

    // Pick a random available port for metrics
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let metrics_addr = listener.local_addr()?;
    drop(listener); // release port for realize
    let metrics_addr = format!("{}", metrics_addr);

    // We want to have time to check the metrics. Make sure it'll
    // hang by giving it an address that won't ever answer.
    let realize_metrics_addr = metrics_addr.clone();
    let mut child = fixture
        .command()?
        .arg("--metrics-addr")
        .arg(&realize_metrics_addr)
        .env(
            "RUST_LOG",
            "realize_lib::network::rpc::realstore::metrics=debug",
        )
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    // Wait for metrics endpoint to be up by reading log output.
    let stderr = child.stderr.as_mut().unwrap();
    let mut err_reader = tokio::io::BufReader::new(stderr).lines();
    let mut found = false;
    let mut captured = vec![];
    for _ in 0..100 {
        if let Some(line) = err_reader.next_line().await? {
            eprintln!("{}", line);
            if line.contains("[metrics] server listening on") {
                found = true;
                break;
            }
            captured.push(line);
        }
    }
    assert!(
        found,
        "Metrics server did not print listening message. captured: {}",
        captured.join("\n")
    );

    // Now test the endpoint
    let client = Client::new();
    let metrics_url = format!("http://{}/metrics", metrics_addr);
    let resp = client.get(&metrics_url).send().await?;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["Content-Type"], "text/plain; version=0.0.4");
    let body = resp.text().await?;
    assert!(
        body.contains("realize_cmd_up "),
        "metrics output missing expected Prometheus format: {}",
        body
    );

    Ok(())
}

#[tokio::test]
async fn realize_metrics_pushgateway() -> anyhow::Result<()> {
    use realize_lib::utils::async_utils::AbortOnDrop;

    // A fake pushgateway that expect one specific request and then
    // dies.
    let pushgw_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let pushgw_addr = pushgw_listener.local_addr()?;
    let pushgw_handle: AbortOnDrop<anyhow::Result<()>> =
        AbortOnDrop::new(tokio::spawn(async move {
            let (stream, _) = pushgw_listener.accept().await?;
            let io = TokioIo::new(stream);
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    hyper::service::service_fn(async move |req| {
                        assert_eq!(
                            "PUT /metrics/job/dir",
                            format!("{} {}", req.method(), req.uri())
                        );
                        Ok::<hyper::Response<String>, anyhow::Error>(
                            hyper::Response::builder()
                                .status(hyper::StatusCode::OK)
                                .body("OK".to_string())?,
                        )
                    }),
                )
                .await?;

            Ok(())
        }));

    let fixture = Fixture::setup_and_start_servers().await?;
    create_files(&fixture.src_dir, &[("foo.txt", "hello")])?;

    // Run realize with pushgateway
    let output = fixture
        .command()?
        .arg("--metrics-pushgateway")
        .arg(format!("http://{}/", pushgw_addr))
        .arg("--metrics-job")
        .arg("dir")
        .output()
        .await?;
    assert!(
        output.status.success(),
        "realize did not succeed {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // The HTTP server task must have died successfully after having
    // received the expected request.
    tokio::time::timeout(std::time::Duration::ZERO, pushgw_handle.join()).await???;

    Ok(())
}

#[tokio::test]
async fn set_rate_limits() -> anyhow::Result<()> {
    let fixture = Fixture::setup_and_start_servers().await?;

    create_files(&fixture.src_dir, &[("foo.txt", "hello")])?;
    let output = fixture
        .command()?
        .arg("--throttle-down")
        .arg("2k")
        .arg("--throttle-up")
        .arg("1k")
        .env("RUST_LOG", "debug")
        .output()
        .await?;
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stderr.contains("Throttling downloads: 2.00 KiB/s"),
        "ERR: {stderr}\nOUT: {stdout}"
    );
    assert!(
        stderr.contains("Throttling uploads: 1.00 KiB/s"),
        "ERR: {stderr}\nOUT: {stdout}"
    );
    Ok(())
}

#[tokio::test]
async fn multiple_directory_ids() -> anyhow::Result<()> {
    let mut fixture = Fixture::setup().await?;
    let arena2 = Arena::from("dir2");
    let src_dir2 = fixture.tempdir.child("src2");
    let dst_dir2 = fixture.tempdir.child("dst2");
    fixture.arenas.push((
        arena2.clone(),
        src_dir2.to_path_buf(),
        dst_dir2.to_path_buf(),
    ));

    fixture.start_servers().await?;

    create_files(&fixture.src_dir, &[("foo1.txt", "foo1")])?;
    create_files(&src_dir2, &[("foo2.txt", "foo2")])?;

    // Run realize with two directory ids
    let output = fixture.command()?.arg(arena2.as_str()).output().await?;
    assert!(output.status.success());

    assert_eq_unordered!(
        dir_content(&fixture.dst_dir)?,
        vec![PathBuf::from("foo1.txt")]
    );
    assert_eq_unordered!(dir_content(&dst_dir2)?, vec![PathBuf::from("foo2.txt")]);
    assert_eq_unordered!(dir_content(&fixture.src_dir)?, vec![]);
    assert_eq_unordered!(dir_content(&src_dir2)?, vec![]);

    // When multiple directory ids are specified, they must be
    // displayed as part of the path.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("[1/2] dir/foo1.txt"),
        "OUT: {stdout} ERR: {stderr}"
    );
    assert!(
        stdout.contains("[2/2] dir2/foo2.txt"),
        "OUT: {stdout} ERR: {stderr}"
    );

    Ok(())
}

pub struct TestKeys {
    pub privkey_a_path: Arc<PathBuf>,
    pub privkey_b_path: Arc<PathBuf>,
    pub pubkey_a_path: PathBuf,
    pub pubkey_b_path: PathBuf,
}

pub fn test_keys() -> TestKeys {
    let resources =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../../resources/test");
    TestKeys {
        privkey_a_path: Arc::new(resources.join("a.key")),
        privkey_b_path: Arc::new(resources.join("b.key")),
        pubkey_a_path: resources.join("a-spki.pem"),
        pubkey_b_path: resources.join("b-spki.pem"),
    }
}

pub fn create_files(
    dir: &assert_fs::fixture::ChildPath,
    files: &[(&str, &str)],
) -> anyhow::Result<()> {
    for (name, content) in files {
        dir.child(name).write_str(content)?;
    }
    Ok(())
}

pub fn dir_content(dir: &assert_fs::fixture::ChildPath) -> anyhow::Result<Vec<PathBuf>> {
    Ok(dir
        .read_dir()?
        .flatten()
        .flat_map(|d| pathdiff::diff_paths(d.path(), dir.path()))
        .collect::<Vec<_>>())
}

pub fn setup_verifier(keys: &TestKeys) -> Arc<PeerVerifier> {
    let mut verifier = PeerVerifier::new();
    verifier.add_peer(
        &Peer::from("a"),
        SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_a_path).unwrap(),
    );
    verifier.add_peer(
        &Peer::from("b"),
        SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_b_path).unwrap(),
    );

    Arc::new(verifier)
}

/// Write configuration to file in tempdir and return it.
pub fn write_config(
    tempfile: ChildPath,
    src_addr: SocketAddr,
    dst_addr: SocketAddr,
    keys: &TestKeys,
) -> anyhow::Result<PathBuf> {
    tempfile.write_str(&format!(
        r#"
[peers.a]
address = "{}"
pubkey = """
{}
"""

[peers.b]
address = "{}"
pubkey = """
{}
"""
"#,
        src_addr.to_string(),
        fs::read_to_string(&keys.pubkey_a_path)?,
        dst_addr.to_string(),
        fs::read_to_string(&keys.pubkey_b_path)?,
    ))?;

    Ok(tempfile.to_path_buf())
}
