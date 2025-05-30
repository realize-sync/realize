use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use assert_fs::TempDir;
use assert_fs::prelude::*;
use predicates::prelude::*;
use realize_lib::network::services::realize::{DirectoryId, Options};
use realize_lib::network::security;
use realize_lib::network::security::PeerVerifier;
use realize_lib::network::tcp::{self, HostPort};
use reqwest::Client;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::SubjectPublicKeyInfoDer;
use rustls::pki_types::pem::PemObject as _;
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
    pub config_file: PathBuf,
    pub resources: PathBuf,
    pub testdir: PathBuf,
    pub _temp_dir: TempDir,
}

impl Fixture {
    pub async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        // Setup temp directory for the daemon to serve
        let temp_dir = TempDir::new()?;
        let testdir = temp_dir.child("server");
        testdir.create_dir_all()?;
        testdir.child("foo.txt").write_str("hello")?;

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");
        let config_file = temp_dir.child("config.yaml").to_path_buf();
        write_config_file(&config_file, testdir.path(), &resources.join("a-spki.pem"))?;

        Ok(Self {
            config_file,
            resources,
            testdir: testdir.path().to_path_buf(),
            _temp_dir: temp_dir,
        })
    }

    pub fn command(&self) -> Command {
        let mut cmd = tokio::process::Command::new(command_path());

        cmd.arg("--address")
            .arg("127.0.0.1:0")
            .arg("--privkey")
            .arg(self.resources.join("a.key"))
            .arg("--config")
            .arg(&self.config_file)
            .env_remove("RUST_LOG")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        cmd
    }
}

#[tokio::test]
async fn daemon_starts_and_lists_files() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()
        .env(
            "RUST_LOG",
            "realize_lib::network::tcp=debug,realize_daemon=debug",
        )
        .spawn()?;

    // The first line that's output to stdout must be Listening on
    // <address>:<port>. Anything else is an error.
    let portstr = wait_for_listening_port(daemon.stdout.as_mut().unwrap()).await?;

    // Connect to the port the daemon listens to.
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(SubjectPublicKeyInfoDer::from_pem_file(
        fixture.resources.join("a-spki.pem"),
    )?);
    let verifier = Arc::new(verifier);

    let client = tcp::connect_client(
        &HostPort::parse(&format!("127.0.0.1:{portstr}")).await?,
        verifier,
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(
                fixture.resources.join("a.key"),
            )?)?,
        tcp::ClientOptions::default(),
    )
    .await?;
    let files = client
        .list(
            context::current(),
            DirectoryId::from("testdir"),
            Options::default(),
        )
        .await??;
    assert_unordered::assert_eq_unordered!(
        files.into_iter().map(|f| f.path).collect(),
        vec![PathBuf::from("foo.txt")]
    );

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    // Make sure stderr contains the expected log message.
    let stdout = collect_stdout(daemon.stdout.as_mut().unwrap()).await?;
    let stderr = collect_stderr(daemon.stderr.as_mut().unwrap()).await?;
    assert!(
        stderr.contains("Accepted peer testpeer from "),
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
        .command()
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

    let output = fixture.command().output().await?;
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

    let output = fixture.command().output().await?;
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

    let mut daemon = fixture.command().spawn()?;
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
        .command()
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

fn write_config_file(
    config_file: &Path,
    testdir_server: &Path,
    pubkey_file: &Path,
) -> anyhow::Result<()> {
    // Write config YAML
    let config_yaml = format!(
        r#"dirs:
  - testdir: {}
peers:
  - testpeer: |
      {}
"#,
        testdir_server.display(),
        std::fs::read_to_string(pubkey_file)?.replace("\n", "\n      ")
    );
    std::fs::write(config_file, config_yaml)?;

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
