use assert_cmd::cargo::cargo_bin;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use assert_unordered::assert_eq_unordered;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn test_local_to_remote() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src");
    let src_dir_path = src_dir.path().to_path_buf();
    let src_dir_path_for_cmd = src_dir_path.clone();
    fs::create_dir(&src_dir_path)?;
    let dst_dir = tempdir.child("dst");
    let dst_dir_path = dst_dir.path().to_path_buf();
    fs::create_dir(&dst_dir_path)?;
    src_dir.child("foo.txt").write_str("hello")?;
    src_dir.child("bar.txt").write_str("world")?;
    let privkey_a_path = Arc::new(PathBuf::from("resources/test/a.key"));
    let privkey_b_path = Arc::new(PathBuf::from("resources/test/b.key"));
    let pubkey_a_path = PathBuf::from("resources/test/a-spki.pem");
    let pubkey_b_path = PathBuf::from("resources/test/b-spki.pem");
    let peers_path = PathBuf::from("resources/test/peers.pem");
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_a_path)?);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_b_path)?);
    let verifier = Arc::new(verifier);
    let privkey_b = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(privkey_b_path.as_ref())?)?,
    );
    let server = RealizeServer::for_dir(&"testdir".into(), dst_dir.path());
    let (addr, server_handle) =
        tcp::start_server("127.0.0.1:0", server, verifier, privkey_b).await?;
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--src-path")
            .arg(&src_dir_path_for_cmd)
            .arg("--dst-addr")
            .arg(&addr.to_string())
            .arg("--privkey")
            .arg(privkey_a_path.as_ref())
            .arg("--peers")
            .arg(&peers_path)
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
    assert_eq_unordered!(
        dst_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &dst_dir_path))
            .collect::<Vec<_>>(),
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
        "dst dir must contain all files from src dir"
    );
    assert_eq_unordered!(
        src_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &src_dir_path))
            .collect::<Vec<_>>(),
        vec![],
        "src dir must be empty"
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_to_local() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src");
    let src_dir_path = src_dir.path().to_path_buf();
    fs::create_dir(&src_dir_path)?;
    let dst_dir = tempdir.child("dst");
    let dst_dir_path = dst_dir.path().to_path_buf();
    let dst_dir_path_for_cmd = dst_dir_path.clone();
    fs::create_dir(&dst_dir_path)?;
    src_dir.child("foo.txt").write_str("hello")?;
    src_dir.child("bar.txt").write_str("world")?;
    let privkey_a_path = Arc::new(PathBuf::from("resources/test/a.key"));
    let privkey_b_path = Arc::new(PathBuf::from("resources/test/b.key"));
    let pubkey_a_path = PathBuf::from("resources/test/a-spki.pem");
    let pubkey_b_path = PathBuf::from("resources/test/b-spki.pem");
    let peers_path = PathBuf::from("resources/test/peers.pem");
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_a_path)?);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_b_path)?);
    let verifier = Arc::new(verifier);
    let privkey_a = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(privkey_a_path.as_ref())?)?,
    );
    let server = RealizeServer::for_dir(&"testdir2".into(), src_dir.path());
    let (src_addr, server_handle) =
        tcp::start_server("127.0.0.1:0", server, verifier, privkey_a).await?;
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--src-addr")
            .arg(&src_addr.to_string())
            .arg("--dst-path")
            .arg(&dst_dir_path_for_cmd)
            .arg("--privkey")
            .arg(privkey_b_path.as_ref())
            .arg("--peers")
            .arg(&peers_path)
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
    assert_eq_unordered!(
        dst_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &dst_dir_path))
            .collect::<Vec<_>>(),
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")],
        "dst dir must contain all files from src dir"
    );
    assert_eq_unordered!(
        src_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &src_dir_path))
            .collect::<Vec<_>>(),
        vec![],
        "src dir must be empty"
    );
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
    fs::create_dir(&src2_path)?;
    fs::create_dir(&dst2_path)?;
    src_dir.child("baz.txt").write_str("baz")?;
    let status = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--src-path")
        .arg(&src2_path)
        .arg("--dst-path")
        .arg(&dst2_path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .await?;
    assert!(status.success());
    assert_eq_unordered!(
        dst_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &dst2_path))
            .collect::<Vec<_>>(),
        vec![PathBuf::from("baz.txt")],
        "dst2 dir must contain all files from src2 dir"
    );
    assert_eq_unordered!(
        src_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &src2_path))
            .collect::<Vec<_>>(),
        vec![],
        "src2 dir must be empty"
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_to_remote() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src3");
    let dst_dir = tempdir.child("dst3");
    let src3_path = src_dir.path().to_path_buf();
    let dst3_path = dst_dir.path().to_path_buf();
    fs::create_dir(&src3_path)?;
    fs::create_dir(&dst3_path)?;
    src_dir.child("qux.txt").write_str("qux")?;
    let privkey_a_path = Arc::new(PathBuf::from("resources/test/a.key"));
    let privkey_b_path = Arc::new(PathBuf::from("resources/test/b.key"));
    let pubkey_a_path = PathBuf::from("resources/test/a-spki.pem");
    let pubkey_b_path = PathBuf::from("resources/test/b-spki.pem");
    let peers_path = PathBuf::from("resources/test/peers.pem");
    let crypto = Arc::new(security::default_provider());
    let mut verifier = PeerVerifier::new(&crypto);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_a_path)?);
    verifier.add_peer(&SubjectPublicKeyInfoDer::from_pem_file(&pubkey_b_path)?);
    let verifier = Arc::new(verifier);
    let privkey_a = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(privkey_a_path.as_ref())?)?,
    );
    let privkey_b = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(privkey_b_path.as_ref())?)?,
    );
    let server_src = RealizeServer::for_dir(&"dir".into(), src_dir.path());
    let (src_addr3, server_handle_src) =
        tcp::start_server("127.0.0.1:0", server_src, verifier.clone(), privkey_a).await?;
    let server_dst = RealizeServer::for_dir(&"dir".into(), dst_dir.path());
    let (dst_addr3, server_handle_dst) =
        tcp::start_server("127.0.0.1:0", server_dst, verifier.clone(), privkey_b).await?;
    let test = tokio::spawn(async move {
        let status = tokio::process::Command::new(cargo_bin!("realize"))
            .arg("--src-addr")
            .arg(&src_addr3.to_string())
            .arg("--dst-addr")
            .arg(&dst_addr3.to_string())
            .arg("--privkey")
            .arg(privkey_a_path.as_ref())
            .arg("--peers")
            .arg(&peers_path)
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
    assert_eq_unordered!(
        dst_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &dst3_path))
            .collect::<Vec<_>>(),
        vec![PathBuf::from("qux.txt")],
        "dst3 dir must contain all files from src3 dir"
    );
    assert_eq_unordered!(
        src_dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), &src3_path))
            .collect::<Vec<_>>(),
        vec![],
        "src3 dir must be empty"
    );
    Ok(())
}
