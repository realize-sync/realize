use std::path::PathBuf;
use std::sync::Arc;

use assert_cmd::cargo::cargo_bin;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use predicates::prelude::*;
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::TokioCommandWrap;
use realize::model::service::DirectoryId;
use realize::transport::security;
use realize::transport::security::PeerVerifier;
use realize::transport::tcp;
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

    // Write config YAML
    let config_yaml = format!(
        r#"dirs:
  - testdir: {}
peers:
  - test: |
      {}
"#,
        testdir_server.display(),
        std::fs::read_to_string(&pubkey_file)?.replace("\n", "\n      ")
    );
    let config_file = temp_dir.child("config.yaml");
    config_file.write_str(&config_yaml)?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = TokioCommandWrap::with_new(cargo_bin!("realized"), |cmd| {
        cmd.arg("--address")
            .arg("127.0.0.1:0")
            .arg("--privkey")
            .arg(&privkey_file)
            .arg("--config")
            .arg(config_file.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
    })
    .wrap(ProcessGroup::leader())
    .spawn()?;

    // The first line that's output to stdout must be Listening on
    // <address>:<port>. Anything else is an error.
    let stdout = daemon.stdout().as_mut().unwrap();
    let reader = tokio::io::BufReader::new(stdout);
    let mut lines = reader.lines();
    let line = lines.next_line().await?.unwrap();
    assert_eq!(
        true,
        predicate::str::starts_with("Listening on").eval(&line),
        "Unexpected output: {line}"
    );
    let portstr = line.split(":").last().expect("Unexpected output: {line}");

    // Connect to the port the daemon listens to.
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_file)?);
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
        .list(tarpc::context::current(), DirectoryId::from("testdir"))
        .await??;
    assert_unordered::assert_eq_unordered!(
        files.into_iter().map(|f| f.path).collect(),
        vec![PathBuf::from("foo.txt")]
    );

    Ok(())
}
