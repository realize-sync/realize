use assert_cmd::cargo::cargo_bin;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use assert_unordered::assert_eq_unordered;
use realize::model::service::DirectoryId;
use realize::server::DirectoryMap;
use realize::server::RealizeServer;
use realize::transport::security::{self, PeerVerifier};
use realize::transport::tcp;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
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
        tcp::start_server("127.0.0.1:0", server.dirs.clone(), verifier, privkey_b).await?;
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
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
        tcp::start_server("127.0.0.1:0", server.dirs.clone(), verifier, privkey_a).await?;
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
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
    assert_eq_unordered!(util::dir_content(&dst_dir)?, vec![PathBuf::from("baz.txt")]);
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
    let (src_addr3, server_handle_src) = tcp::start_server(
        "127.0.0.1:0",
        server_src.dirs.clone(),
        verifier.clone(),
        privkey_a,
    )
    .await?;
    let server_dst = RealizeServer::for_dir(&"dir".into(), dst_dir.path());
    let (dst_addr3, server_handle_dst) = tcp::start_server(
        "127.0.0.1:0",
        server_dst.dirs.clone(),
        verifier.clone(),
        privkey_b,
    )
    .await?;
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
    assert_eq_unordered!(util::dir_content(&dst_dir)?, vec![PathBuf::from("qux.txt")]);
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
        stderr.contains("ERROR     [1/2] bad.txt: "),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("ERROR: 1 file(s) failed, and 1 files(s) moved"),
        "stderr: {stderr}"
    );
    assert_eq_unordered!(
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("good.txt")]
    );
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
    assert_eq_unordered!(
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
        stdout.contains("Moved     [1/2] bar.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("Moved     [2/2] foo.txt"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("SUCCESS 2 file(s) moved"),
        "stdout: {stdout}"
    );
    assert_eq_unordered!(
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
    assert_eq_unordered!(
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("foo.txt"), PathBuf::from("bar.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir)?, vec![]);
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
    assert_eq_unordered!(
        util::dir_content(&dst_dir)?,
        vec![PathBuf::from("good.txt")]
    );
    Ok(())
}

#[tokio::test]
async fn test_max_duration_timeout() -> anyhow::Result<()> {
    // Bind a TCP port but never accept connections
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;

    // Run realize with a very short max-duration
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-path")
        .arg("/tmp") // any existing dir, won't be used
        .arg("--dst-addr")
        .arg(addr.to_string())
        .arg("--privkey")
        .arg("resources/test/a.key")
        .arg("--peers")
        .arg("resources/test/peers.pem")
        .arg("--max-duration")
        .arg("100ms")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let output = output.wait_with_output().await?;
    assert_eq!(
        output.status.code(),
        Some(11),
        "Should exit with code 11 on timeout"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Maximum duration (100ms) exceeded. Giving up."),
        "stderr: {stderr}"
    );
    Ok(())
}

#[tokio::test]
#[cfg(feature = "push")]
async fn test_realize_metrics_export() -> anyhow::Result<()> {
    use reqwest::Client;
    use tokio::io::AsyncBufReadExt as _;

    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src_metrics");
    let dst_dir = tempdir.child("dst_metrics");
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello")])?;
    std::fs::create_dir(dst_dir.path())?;

    // Pick a random available port for metrics
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let metrics_addr = listener.local_addr()?;
    drop(listener); // release port for realize
    let metrics_addr_str = format!("{}", metrics_addr);

    // We want to have time to check the metrics. Make sure it'll
    // hang by giving it an address that won't ever answer.
    let nonanswering_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let nonanswering_addr = nonanswering_listener.local_addr()?.to_string();
    let keys = util::test_keys();
    let realize_metrics_addr = metrics_addr_str.clone();
    let mut child = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-addr")
        .arg(&nonanswering_addr)
        .arg("--dst-addr")
        .arg(&nonanswering_addr)
        .arg("--privkey")
        .arg(keys.privkey_a_path.as_ref())
        .arg("--peers")
        .arg(&keys.peers_path)
        .arg("--metrics-addr")
        .arg(&realize_metrics_addr)
        .env("RUST_LOG", "realize::metrics=debug")
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
    // The command can't connect, so up stays 0.
    assert!(
        body.contains("realize_cmd_up 0"),
        "metrics output missing expected Prometheus format: {}",
        body
    );

    Ok(())
}

#[tokio::test]
#[cfg(feature = "push")]
async fn test_realize_metrics_pushgateway() -> anyhow::Result<()> {
    use realize::utils::async_utils::AbortOnDrop;

    // A fake pushgateway that expect one specific request and then
    // dies.
    let pushgw_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let pushgw_addr = pushgw_listener.local_addr()?;
    let pushgw_handle: AbortOnDrop<anyhow::Result<()>> =
        AbortOnDrop::new(tokio::spawn(async move {
            let (stream, _) = pushgw_listener.accept().await?;
            let io = hyper_util::rt::TokioIo::new(stream);
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    hyper::service::service_fn(async move |req| {
                        assert_eq!(
                            "PUT /metrics/job/testjob".to_string(),
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

    // Prepare test dirs
    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src_push");
    let dst_dir = tempdir.child("dst_push");
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello")])?;
    std::fs::create_dir(dst_dir.path())?;

    // Run realize with pushgateway
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-path")
        .arg(src_dir.path())
        .arg("--dst-path")
        .arg(dst_dir.path())
        .arg("--metrics-pushgateway")
        .arg(&format!("http://{}/", pushgw_addr))
        .arg("--metrics-job")
        .arg("testjob")
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
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
async fn test_set_rate_limits() -> anyhow::Result<()> {
    env_logger::try_init().ok();

    let tempdir = TempDir::new()?;
    let src_dir = tempdir.child("src");
    util::create_dir_with_files(&src_dir, &[("foo.txt", "hello")])?;

    let dst_dir = tempdir.child("dst");
    dst_dir.create_dir_all()?;

    let keys = util::test_keys();
    let (crypto, verifier): (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) =
        util::setup_crypto_and_verifier();
    let privkey_b = Arc::from(
        crypto
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_file(keys.privkey_b_path.as_ref())?)?,
    );
    let dir_id = DirectoryId::from("testdir");
    let (src_addr, _src_handle) = tcp::start_server(
        "127.0.0.1:0",
        DirectoryMap::for_dir(&dir_id, src_dir.path()),
        Arc::clone(&verifier),
        Arc::clone(&privkey_b),
    )
    .await?;
    let (dst_addr, _dst_handle) = tcp::start_server(
        "127.0.0.1:0",
        DirectoryMap::for_dir(&dir_id, dst_dir.path()),
        verifier,
        privkey_b,
    )
    .await?;

    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--quiet")
        .arg("--src-addr")
        .arg(&src_addr.to_string())
        .arg("--dst-addr")
        .arg(&dst_addr.to_string())
        .arg("--privkey")
        .arg(keys.privkey_a_path.as_ref())
        .arg("--peers")
        .arg(&keys.peers_path)
        .arg(dir_id.to_string())
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
async fn test_multiple_directory_ids() -> anyhow::Result<()> {
    env_logger::try_init().ok();
    let tempdir = TempDir::new()?;
    let src_dir1 = tempdir.child("src_multi1");
    let src_dir2 = tempdir.child("src_multi2");
    let dst_dir1 = tempdir.child("dst_multi1");
    let dst_dir2 = tempdir.child("dst_multi2");
    util::create_dir_with_files(&src_dir1, &[("foo1.txt", "foo1")])?;
    util::create_dir_with_files(&src_dir2, &[("foo2.txt", "foo2")])?;
    std::fs::create_dir(dst_dir1.path())?;
    std::fs::create_dir(dst_dir2.path())?;
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
    // Setup src server with two directories
    let src_dirs = DirectoryMap::new([
        realize::server::Directory::new(&DirectoryId::from("dir1"), src_dir1.path()),
        realize::server::Directory::new(&DirectoryId::from("dir2"), src_dir2.path()),
    ]);
    let (src_addr, src_handle) =
        tcp::start_server("127.0.0.1:0", src_dirs, verifier.clone(), privkey_a).await?;
    // Setup dst server with two directories
    let dst_dirs = DirectoryMap::new([
        realize::server::Directory::new(&DirectoryId::from("dir1"), dst_dir1.path()),
        realize::server::Directory::new(&DirectoryId::from("dir2"), dst_dir2.path()),
    ]);
    let (dst_addr, dst_handle) =
        tcp::start_server("127.0.0.1:0", dst_dirs, verifier, privkey_b).await?;

    // Run realize with two directory ids
    let output = tokio::process::Command::new(cargo_bin!("realize"))
        .arg("--src-addr")
        .arg(&src_addr.to_string())
        .arg("--dst-addr")
        .arg(&dst_addr.to_string())
        .arg("--privkey")
        .arg(keys.privkey_a_path.as_ref())
        .arg("--peers")
        .arg(&keys.peers_path)
        .arg("dir1")
        .arg("dir2")
        .output()
        .await?;
    assert!(output.status.success());

    src_handle.abort();
    dst_handle.abort();
    assert_eq_unordered!(
        util::dir_content(&dst_dir1)?,
        vec![PathBuf::from("foo1.txt")]
    );
    assert_eq_unordered!(
        util::dir_content(&dst_dir2)?,
        vec![PathBuf::from("foo2.txt")]
    );
    assert_eq_unordered!(util::dir_content(&src_dir1)?, vec![]);
    assert_eq_unordered!(util::dir_content(&src_dir2)?, vec![]);

    // When multiple directory ids are specified, they must be
    // displayed as part of the path.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("[1/1] dir1/foo1.txt"),
        "OUT: {stdout} ERR: {stderr}"
    );
    assert!(
        stdout.contains("[2/2] dir2/foo2.txt"),
        "OUT: {stdout} ERR: {stderr}"
    );

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

    pub fn dir_content(dir: &assert_fs::fixture::ChildPath) -> anyhow::Result<Vec<PathBuf>> {
        Ok(dir
            .read_dir()?
            .flatten()
            .flat_map(|d| pathdiff::diff_paths(d.path(), dir.path()))
            .collect::<Vec<_>>())
    }

    pub fn setup_crypto_and_verifier() -> (Arc<rustls::crypto::CryptoProvider>, Arc<PeerVerifier>) {
        let crypto = Arc::new(security::default_provider());
        let keys = test_keys();
        let mut verifier = PeerVerifier::new(&crypto);
        verifier.add_peer(SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_a_path).unwrap());
        verifier.add_peer(SubjectPublicKeyInfoDer::from_pem_file(&keys.pubkey_b_path).unwrap());
        (crypto, Arc::new(verifier))
    }
}
