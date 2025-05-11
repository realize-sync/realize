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

    // Generate a test private key and public key (ed25519, PEM)
    let privkey = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPaGEL0B7EAMQb5anN+DTH0vZ/qI90AQpbwYuklDABpV
-----END PRIVATE KEY-----
"#;

    let pubkey = r#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA/CMSGfePPViYEUoHMNTrywE+mwTmB0poO0A1ATNIJGo=
-----END PUBLIC KEY-----
"#;

    // Write keys to temp files
    let privkey_file = temp_dir.child("test.key");
    privkey_file.write_str(privkey)?;

    // Write config YAML
    let config_yaml = format!(
        r#"dirs:
  - testdir: {}
peers:
  - test: |
      {}
"#,
        testdir_server.display(),
        pubkey.replace("\n", "\n      ")
    );
    let config_file = temp_dir.child("config.yaml");
    config_file.write_str(&config_yaml)?;

    let mut daemon = TokioCommandWrap::with_new(cargo_bin!("realized"), |command| {
        command
            .arg("--port")
            .arg("0")
            .arg("--privkey")
            .arg(privkey_file.path())
            .arg("--config")
            .arg(config_file.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
    })
    .wrap(ProcessGroup::leader())
    .spawn()?;

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

    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_slice(pubkey.as_bytes())?);
    let verifier = Arc::new(verifier);

    let client = tcp::connect_client(
        "localhost",
        format!("127.0.0.1:{}", portstr),
        verifier,
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_slice(privkey.as_bytes())?)?,
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
