use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use assert_cmd::cargo::cargo_bin;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use predicates::prelude::*;
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::TokioCommandWrap;
use realize::model::service::DirectoryId;
use realize::model::service::Options;
use realize::transport::security;
use realize::transport::security::PeerVerifier;
use realize::transport::tcp;
use reqwest::Client;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::SubjectPublicKeyInfoDer;
use rustls::pki_types::pem::PemObject as _;
use tokio::fs;
use tokio::io::AsyncBufReadExt as _;

#[tokio::test]
async fn daemon_starts_and_lists_files() -> anyhow::Result<()> {
    env_logger::init();

    // Setup temp directory for the daemon to serve
    let temp_dir = TempDir::new()?;
    let testdir_server = temp_dir.child("server");
    fs::create_dir(&testdir_server).await?;
    testdir_server.child("foo.txt").write_str("hello")?;

    let privkey_file = PathBuf::from("resources/test/a.key");
    let pubkey_file = PathBuf::from("resources/test/a-spki.pem");

    let config_file = temp_dir.child("config.yaml");
    write_config_file(
        config_file.path(),
        testdir_server.path(),
        pubkey_file.as_path(),
    )?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = TokioCommandWrap::with_new(cargo_bin!("realized"), |cmd| {
        cmd.arg("--address")
            .arg("127.0.0.1:0")
            .arg("--privkey")
            .arg(&privkey_file)
            .arg("--config")
            .arg(config_file.path())
            .env("RUST_LOG", "realize::transport::tcp=debug")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
    })
    .wrap(ProcessGroup::leader())
    .spawn()?;

    // The first line that's output to stdout must be Listening on
    // <address>:<port>. Anything else is an error.
    let portstr = {
        let stdout = daemon.stdout().as_mut().unwrap();
        let reader = tokio::io::BufReader::new(stdout);
        let mut lines = reader.lines();
        let line = lines.next_line().await?.unwrap();
        assert_eq!(
            true,
            predicate::str::starts_with("Listening on").eval(&line),
            "Unexpected output: {line}"
        );

        line.split(":")
            .last()
            .expect("Unexpected output: {line}")
            .to_string()
    };

    // Connect to the port the daemon listens to.
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(SubjectPublicKeyInfoDer::from_pem_file(&pubkey_file)?);
    let verifier = Arc::new(verifier);

    let client = tcp::connect_client(
        "localhost",
        format!("127.0.0.1:{}", portstr),
        verifier,
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(&privkey_file)?)?,
    )
    .await?;
    let files = client
        .list(
            tarpc::context::current(),
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

    // Collect all of stderr
    let stderr = {
        let stderr = daemon.stderr().as_mut().unwrap();
        let reader = tokio::io::BufReader::new(stderr);
        let mut lines = reader.lines();
        let mut all_lines = vec![];
        while let Ok(Some(line)) = lines.next_line().await {
            all_lines.push(line);
        }

        all_lines.join("\n")
    };

    // Make sure stderr contains the expected log message.
    assert!(
        stderr.contains("Accepted peer testpeer from "),
        "stderr: {stderr}"
    );

    Ok(())
}

#[tokio::test]
async fn metrics_endpoint_works() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let testdir_server = temp_dir.child("server");
    fs::create_dir(&testdir_server).await?;
    let privkey_file = PathBuf::from("resources/test/a.key");
    let pubkey_file = PathBuf::from("resources/test/a-spki.pem");
    let config_file = temp_dir.child("config.yaml");
    write_config_file(
        config_file.path(),
        testdir_server.path(),
        pubkey_file.as_path(),
    )?;

    let metrics_port = portpicker::pick_unused_port().expect("No ports free");
    let metrics_addr = format!("127.0.0.1:{}", metrics_port);
    let mut daemon = TokioCommandWrap::with_new(cargo_bin!("realized"), |cmd| {
        cmd.arg("--address")
            .arg("127.0.0.1:0")
            .arg("--privkey")
            .arg(&privkey_file)
            .arg("--config")
            .arg(config_file.path())
            .arg("--metrics-addr")
            .arg(&metrics_addr)
            .env("RUST_LOG", "realize::metrics=debug")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true); // TODO: is this enough? Is the TokioCommandWrap actually necessary?
    })
    .wrap(ProcessGroup::leader())
    .spawn()?;

    // Wait for metrics endpoint to be up by reading log output.
    let stderr = daemon.stdout().as_mut().unwrap();
    let mut err_reader = tokio::io::BufReader::new(stderr).lines();
    let mut found = false;
    let mut captured = vec![];
    for _ in 0..100 {
        if let Some(line) = err_reader.next_line().await? {
            if line.contains("Listening on") {
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
        std::fs::read_to_string(&pubkey_file)?.replace("\n", "\n      ")
    );
    std::fs::write(config_file, config_yaml)?;

    Ok(())
}
