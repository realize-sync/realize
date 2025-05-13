use assert_cmd::cargo::cargo_bin;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use assert_unordered::assert_eq_unordered;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn test_local_to_remote() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src");
    let dst_dir = tempdir.child("dst");
    let src_dir_path = src_dir.path().to_path_buf();
    let dst_dir_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello"), ("bar.txt", "world")])?;
    std::fs::create_dir(&dst_dir_path)?;
    let keys = util::test_keys();
    let (crypto, verifier): (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) =
        util::setup_crypto_and_verifier();
    let privkey_b = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(keys.privkey_b_path.as_ref())?)?,
    );
    let server = RealizeServer::for_dir(&"testdir".into(), dst_dir.path());
    let (addr, server_handle) =
        tcp::start_server("127.0.0.1:0", server, verifier, privkey_b).await?;
    let src_dir_path_for_cmd = src_dir_path.clone();
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--quiet")
            .arg("--src-path")
            .arg(&src_dir_path_for_cmd)
            .arg("--dst-addr")
            .arg(&addr.to_string())
            .arg("--privkey")
            .arg(keys.privkey_a_path.as_ref())
            .arg("--peers")
            .arg(&keys.peers_path)
            .arg("--directory-id")
            .arg("testdir")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await?;
        assert!(status.success());
        Ok::<(), anyhow::Error>(())
    });
    let result = test.await;
    server_handle.abort();
    result??;
    util::assert_dir_contents(
        &dst_dir,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
    )?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_remote_to_local() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src");
    let dst_dir = tempdir.child("dst");
    let dst_dir_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello"), ("bar.txt", "world")])?;
    std::fs::create_dir(&dst_dir_path)?;
    let keys = util::test_keys();
    let (crypto, verifier): (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) =
        util::setup_crypto_and_verifier();
    let privkey_a = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(keys.privkey_a_path.as_ref())?)?,
    );
    let server = RealizeServer::for_dir(&"testdir2".into(), src_dir.path());
    let (src_addr, server_handle) =
        tcp::start_server("127.0.0.1:0", server, verifier, privkey_a).await?;
    let dst_dir_path_for_cmd = dst_dir_path.clone();
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--quiet")
            .arg("--src-addr")
            .arg(&src_addr.to_string())
            .arg("--dst-path")
            .arg(&dst_dir_path_for_cmd)
            .arg("--privkey")
            .arg(keys.privkey_b_path.as_ref())
            .arg("--peers")
            .arg(&keys.peers_path)
            .arg("--directory-id")
            .arg("testdir2")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await?;
        assert!(status.success());
        Ok::<(), anyhow::Error>(())
    });
    let result = test.await;
    server_handle.abort();
    result??;
    util::assert_dir_contents(
        &dst_dir,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
    )?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src2");
    let dst_dir = tempdir.child("dst2");
    let src2_path = src_dir.path().to_path_buf();
    let dst2_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("baz.txt", "baz")])?;
    let status = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-path")
        .arg(&src2_path)
        .arg("--dst-path")
        .arg(&dst2_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .await?;
    assert!(status.success());
    util::assert_dir_contents(&dst_dir, vec![PathBuf::from("baz.txt")])?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_remote_to_remote() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src3");
    let dst_dir = tempdir.child("dst3");
    util::create_dir_with_files(&src_dir, &[("qux.txt", "qux")])?;
    let keys = util::test_keys();
    let (crypto, verifier): (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) =
        util::setup_crypto_and_verifier();
    let privkey_a = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(keys.privkey_a_path.as_ref())?)?,
    );
    let privkey_b = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(keys.privkey_b_path.as_ref())?)?,
    );
    let server_src = RealizeServer::for_dir(&"dir".into(), src_dir.path());
    let (src_addr3, server_handle_src) =
        tcp::start_server("127.0.0.1:0", server_src, verifier.clone(), privkey_a).await?;
    let server_dst = RealizeServer::for_dir(&"dir".into(), dst_dir.path());
    let (dst_addr3, server_handle_dst) =
        tcp::start_server("127.0.0.1:0", server_dst, verifier.clone(), privkey_b).await?;
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--quiet")
            .arg("--src-addr")
            .arg(&src_addr3.to_string())
            .arg("--dst-addr")
            .arg(&dst_addr3.to_string())
            .arg("--privkey")
            .arg(keys.privkey_a_path.as_ref())
            .arg("--peers")
            .arg(&keys.peers_path)
            .arg("--directory-id")
            .arg("dir")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await?;
        assert!(status.success());
        Ok::<(), anyhow::Error>(())
    });
    let result = test.await;
    server_handle_src.abort();
    server_handle_dst.abort();
    result??;
    util::assert_dir_contents(&dst_dir, vec![PathBuf::from("qux.txt")])?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local_partial_failure() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src4");
    let dst_dir = tempdir.child("dst4");
    let src4_path = src_dir.path().to_path_buf();
    let dst4_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("good.txt", "ok"), ("bad.txt", "fail")])?;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o000),
    )?;
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--src-path")
        .arg(&src4_path)
        .arg("--dst-path")
        .arg(&dst4_path)
        .output()
        .await?;
    // Restore permissions for cleanup
    fs::set_permissions(
        src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o644),
    )?;
    assert!(!output.status.success(), "Should fail if any file fails");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ERROR     dir/bad.txt: "),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("ERROR: 1 file(s) failed, and 1 files(s) moved"),
        "stderr: {stderr}"
    );
    util::assert_dir_contents(&dst_dir, vec![PathBuf::from("good.txt")])?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local_success_output() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src5");
    let dst_dir = tempdir.child("dst5");
    let src5_path = src_dir.path().to_path_buf();
    let dst5_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello"), ("bar.txt", "world")])?;
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--src-path")
        .arg(&src5_path)
        .arg("--dst-path")
        .arg(&dst5_path)
        .output()
        .await?;
    assert!(output.status.success(), "Should succeed if all files moved");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("SUCCESS 2 file(s) moved"),
        "stdout: {stdout}"
    );
    util::assert_dir_contents(
        &dst_dir,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
    )?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local_progress_output() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src6");
    let dst_dir = tempdir.child("dst6");
    let src6_path = src_dir.path().to_path_buf();
    let dst6_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello"), ("bar.txt", "world")])?;
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--src-path")
        .arg(&src6_path)
        .arg("--dst-path")
        .arg(&dst6_path)
        .output()
        .await?;
    assert!(output.status.success(), "Should succeed if all files moved");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/2] Moved     dir/foo.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("/2] Moved     dir/bar.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("SUCCESS 2 file(s) moved"),
        "stdout: {stdout}"
    );
    util::assert_dir_contents(
        &dst_dir,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
    )?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local_quiet_success() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src_quiet");
    let dst_dir = tempdir.child("dst_quiet");
    let src_path = src_dir.path().to_path_buf();
    let dst_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello"), ("bar.txt", "world")])?;
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-path")
        .arg(&src_path)
        .arg("--dst-path")
        .arg(&dst_path)
        .output()
        .await?;
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
    util::assert_dir_contents(
        &dst_dir,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
    )?;
    util::assert_dir_empty(&src_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_local_to_local_quiet_failure() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src_quiet_fail");
    let dst_dir = tempdir.child("dst_quiet_fail");
    let src_path = src_dir.path().to_path_buf();
    let dst_path = dst_dir.path().to_path_buf();
    util::create_dir_with_files(&src_dir, &[("good.txt", "ok"), ("bad.txt", "fail")])?;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o000),
    )?;
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-path")
        .arg(&src_path)
        .arg("--dst-path")
        .arg(&dst_path)
        .output()
        .await?;
    // Restore permissions for cleanup
    fs::set_permissions(
        src_dir.child("bad.txt").path(),
        fs::Permissions::from_mode(0o644),
    )?;
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
    util::assert_dir_contents(&dst_dir, vec![PathBuf::from("good.txt")])?;
    Ok(())
}

mod util {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    pub struct TestKeys {
        pub privkey_a_path: Arc<PathBuf>,
        pub privkey_b_path: Arc<PathBuf>,
        pub pubkey_a_path: PathBuf,
        pub pubkey_b_path: PathBuf,
        pub peers_path: PathBuf,
    }

    pub fn test_keys() -> TestKeys {
        TestKeys {
            privkey_a_path: Arc::new(PathBuf::from("resources/test/a.key")),
            privkey_b_path: Arc::new(PathBuf::from("resources/test/b.key")),
            pubkey_a_path: PathBuf::from("resources/test/a-spki.pem"),
            pubkey_b_path: PathBuf::from("resources/test/b-spki.pem"),
            peers_path: PathBuf::from("resources/test/peers.pem"),
        }
    }

    pub fn create_dir_with_files(
        dir: &assert_fs::fixture::ChildPath,
        files: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        std::fs::create_dir(dir.path())?;
        for (name, content) in files {
            dir.child(name).write_str(content)?;
        }
        Ok(())
    }

    pub fn assert_dir_contents(
        dir: &assert_fs::fixture::ChildPath,
        expected: Vec<PathBuf>,
    ) -> anyhow::Result<()> {
        assert_eq_unordered!(
            dir.read_dir()?
                .flatten()
                .flat_map(|d| pathdiff::diff_paths(d.path(), dir.path()))
                .collect::<Vec<_>>(),
            expected
        );
        Ok(())
    }

    pub fn assert_dir_empty(dir: &assert_fs::fixture::ChildPath) -> anyhow::Result<()> {
        assert_eq_unordered!(
            dir.read_dir()?
                .flatten()
                .flat_map(|d| pathdiff::diff_paths(d.path(), dir.path()))
                .collect::<Vec<_>>(),
            Vec::<PathBuf>::new()
        );
        Ok(())
    }

    pub fn setup_crypto_and_verifier() -> (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) {
        let crypto = Arc::new(security::default_provider());
        let keys = test_keys();
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_a_path).unwrap());
        verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_b_path).unwrap());
        (crypto, Arc::new(verifier))
    }
}
