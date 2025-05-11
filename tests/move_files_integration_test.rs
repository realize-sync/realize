use assert_cmd::cargo::cargo_bin;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use assert_unordered::assert_eq_unordered;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp::RunningServer;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn test_move_files_integration() -> anyhow::Result<()> {
    env_logger::init();

    let tempdir = TempDir::new()?;

    let src_dir = &tempdir.child("src");
    fs::create_dir(src_dir.path())?;

    let dst_dir = &tempdir.child("dst");
    fs::create_dir(dst_dir.path())?;

    src_dir.child("foo.txt").write_str("hello")?;
    src_dir.child("bar.txt").write_str("world")?;

    // Setup keys
    let privkey_b_path = PathBuf::from("resources/test/b.key");
    let pubkey_a_path = PathBuf::from("resources/test/a-spki.pem");

    // Start server (daemon) in background
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_a_path)?);
    let verifier = Arc::new(verifier);
    let privkey = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(&privkey_b_path)?)?,
    );
    let server = RealizeServer::for_dir(&"testdir".into(), dst_dir.path());
    let running = RunningServer::bind("127.0.0.1:0", server, verifier, privkey).await?;
    let addr = running.local_addr()?.to_string();
    let server_handle = running.spawn();

    let testdir_client = src_dir.path().to_path_buf();
    let test = tokio::spawn(async move {
        // Run realize CLI to move files from src_dir to dst_dir
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--address")
            .arg(addr)
            .arg("--private-key")
            .arg("resources/test/a.key")
            .arg("--server-public-key")
            .arg("resources/test/b-spki.pem")
            .arg("--directory")
            .arg(testdir_client)
            .arg("--directory-id")
            .arg("testdir")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await?;
        assert!(status.success());

        Ok::<(), anyhow::Error>(())
    });

    // Let the command run, abort the server.
    let result = test.await;
    server_handle.abort();
    result??;

    // Test the final state.
    assert_eq_unordered!(
        dst_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), dst_dir))
            .collect(),
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
        "dst dir must contain all files from src dir"
    );
    assert_eq_unordered!(
        src_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), src_dir))
            .collect(),
        vec![],
        "src dir must be empty"
    );

    Ok(())
}
