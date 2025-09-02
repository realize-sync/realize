use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_network::config::PeerConfig;
use realize_network::unixsocket;
use realize_storage::config::{ArenaConfig, CacheConfig};
use realize_types;
use realize_types::{Arena, Peer};
use std::env;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Child;
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::task::LocalSet;

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
        .join("realized")
}

struct Fixture {
    pub config: Config,
    pub resources: PathBuf,
    pub testdir: PathBuf,
    pub tempdir: TempDir,
    pub server_address: String,
    pub server_privkey: PathBuf,
    pub debug_output: bool,
    pub socket: PathBuf,
}

impl Fixture {
    pub async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        // To debug, set env variable TEST_DEBUG to get output from
        // the commands and pass --nocapture.
        //
        // Example: TEST_DEBUG=1 cargo nextest run -p realize-daemon --test daemon_integration_test --nocapture
        let debug = env::var("TEST_DEBUG").is_ok_and(|v| !v.is_empty());
        if debug {
            eprintln!("TEST_DEBUG detected");
        }
        let mut config = Config::new();
        let arena = Arena::from("testdir");

        // Setup temp directory for the daemon to serve
        let tempdir = TempDir::new()?;

        let testdir = tempdir.child("testdir");
        testdir.create_dir_all()?;

        // Configure cache (now required)
        config.storage.cache = CacheConfig {
            db: tempdir.child("cache.db").to_path_buf(),
        };

        // Configure arena with required cache and optional local path
        config.storage.arenas.push(ArenaConfig::new(
            arena,
            testdir.to_path_buf(),
            tempdir.child("testdir-metadata").to_path_buf(),
        ));

        testdir.child("foo.txt").write_str("hello")?;

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");

        config.network.peers.push(PeerConfig {
            peer: Peer::from("a"),
            pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
            address: None,
            batch_rate_limit: None,
        });
        config.network.peers.push(PeerConfig {
            peer: Peer::from("b"),
            pubkey: std::fs::read_to_string(resources.join("b-spki.pem"))?,
            address: None,
            batch_rate_limit: None,
        });

        let server_privkey = resources.join("a.key");
        let server_port = portpicker::pick_unused_port().expect("No ports free");
        let server_address = format!("127.0.0.1:{server_port}");
        let socket = tempdir.path().join("realize/control.socket");
        Ok(Self {
            config,
            resources,
            testdir: testdir.to_path_buf(),
            tempdir,
            server_address,
            server_privkey,
            debug_output: debug,
            socket,
        })
    }

    pub fn command(&self) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path());

        let config_file = self.tempdir.child("config.toml").to_path_buf();
        let config_str = toml::to_string_pretty(&self.config)?;
        if self.debug_output {
            eprintln!("config.toml <<EOF\n{config_str}\nEOF");
        }
        std::fs::write(&config_file, config_str)?;

        cmd.arg("--address")
            .arg(&self.server_address)
            .arg("--privkey")
            .arg(&self.server_privkey)
            .arg("--config")
            .arg(config_file)
            .arg("--socket")
            .arg(&self.socket)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        if self.debug_output {
            cmd.env("RUST_LOG", "debug");
        } else {
            cmd.env_remove("RUST_LOG");
        }

        Ok(cmd)
    }

    /// Collect stderr from a child process, optionally print it out.
    ///
    /// Set env variable TEST_DEBUG=1 to get child process output as
    /// it comes.
    ///
    /// Call it as early as possible, just after creating the process,
    /// so process output is immediately available.
    fn collect_stderr(
        &self,
        name: &'static str,
        daemon: &mut Child,
    ) -> JoinHandle<anyhow::Result<String>> {
        let debug_output = self.debug_output;
        let mut stderr = daemon
            .stderr
            .take()
            .expect("call Command::stderr(Stdio::piped())");
        tokio::spawn(async move {
            let reader = tokio::io::BufReader::new(&mut stderr);
            let mut lines = reader.lines();
            let mut all_lines = vec![];

            while let Ok(Some(line)) = lines.next_line().await {
                if debug_output {
                    eprintln!("[{name}] STDERR: {line}");
                }
                all_lines.push(line);
            }

            Ok(all_lines.join("\n"))
        })
    }

    fn collect_stdout(
        &self,
        name: &'static str,
        daemon: &mut Child,
    ) -> JoinHandle<anyhow::Result<String>> {
        let debug_output = self.debug_output;
        let mut stdout = daemon
            .stdout
            .take()
            .expect("call Command::stdout(Stdio::piped())");
        tokio::spawn(async move {
            let reader = tokio::io::BufReader::new(&mut stdout);
            let mut lines = reader.lines();
            let mut all_lines = vec![];

            while let Ok(Some(line)) = lines.next_line().await {
                if debug_output {
                    eprintln!("[{name}] STDOUT: {line}");
                }
                all_lines.push(line);
            }

            Ok(all_lines.join("\n"))
        })
    }

    async fn assert_listening(&self) {
        assert_listening_to(&self.server_address).await;
    }
}

async fn assert_listening_to(address: &str) {
    for _ in 0..150 {
        // 150 * 100ms = 15 seconds
        match tokio::net::TcpStream::connect(address).await {
            Ok(mut stream) => {
                let _ = stream.shutdown().await;
                return;
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    panic!("Server never listened on {address}");
}

fn kill(pid: Option<u32>) -> anyhow::Result<()> {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid.expect("no pid") as i32),
        nix::sys::signal::Signal::SIGTERM,
    )?;

    Ok(())
}

#[tokio::test]
async fn daemon_fails_on_missing_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    fs::remove_dir_all(&fixture.testdir)?;

    let output = fixture.command()?.output().await?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr<<EOF\n{stderr}\nEOF");
    assert!(
        stderr.contains("datadir not found"),
        "stderr<<EOF\n{stderr}\nEOF"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_fails_on_unreadable_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    std::fs::set_permissions(&fixture.testdir, std::fs::Permissions::from_mode(0o000))?;

    let output = fixture.command()?.output().await?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr<<EOF\n{stderr}\nEOF");
    assert!(
        stderr.contains("readable dir"),
        "stderr<<EOF\n{stderr}\nEOF"
    );

    Ok(())
}

#[tokio::test]
async fn daemon_warns_on_unwritable_directory() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;
    std::fs::set_permissions(&fixture.testdir, std::fs::Permissions::from_mode(0o500))?; // read+exec only

    let mut daemon = fixture.command()?.stderr(Stdio::piped()).spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    let stderr = fixture.collect_stderr("daemon", &mut daemon);
    fixture.assert_listening().await;

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    let stderr = stderr.await??;
    assert!(
        stderr.contains("writable dir"),
        "stderr<<EOF\n{stderr}\nEOF"
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
        .env("RUST_LOG", "realize_=debug")
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    let stderr = fixture.collect_stderr("daemon", &mut daemon);
    fixture.assert_listening().await;

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    // Make sure stderr contains the expected log message.
    let stderr = stderr.await??;
    assert!(
        stderr.contains("<7>realize_") || stderr.contains("<5>realize_"),
        "stderr<<EOF\n{stderr}\nEOF"
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
            "realize_network::network::tcp=debug,realized=info",
        )
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    let stderr = fixture.collect_stderr("daemon", &mut daemon);
    fixture.assert_listening().await;

    kill(daemon.id())?;

    let status = daemon.wait().await?;
    assert_eq!(status.code(), Some(0));

    // Make sure stderr contains the expected log message.
    let stderr = stderr.await??;
    assert!(stderr.contains("Interrupted"), "stderr<<EOF\n{stderr}\nEOF");

    Ok(())
}

#[tokio::test]
async fn daemon_updates_cache() -> anyhow::Result<()> {
    use std::time::Duration;

    let mut fixture_a = Fixture::setup().await?;
    let arena_config = fixture_a
        .config
        .storage
        .arena_config_mut(Arena::from("testdir"))
        .unwrap();
    arena_config.datadir = Some(fixture_a.testdir.clone());

    let mut daemon_a = fixture_a
        .command()?
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let pid = daemon_a.id();
    scopeguard::defer! { let _ = kill(pid); }

    // collect_stderr and collect_stdout are used here to add a prefix
    // to the daemon output and tell one from the other.
    fixture_a.collect_stderr("A", &mut daemon_a);
    fixture_a.collect_stdout("A", &mut daemon_a);
    fixture_a.assert_listening().await;

    let mut fixture_b = Fixture::setup().await?;
    fixture_b.server_privkey = fixture_b.resources.join("b.key");
    fixture_b
        .config
        .network
        .peer_config_mut(Peer::from("a"))
        .expect("peer a")
        .address = Some(fixture_a.server_address);

    // Create a mount point for FUSE
    let mount_point = fixture_b.tempdir.child("fuse-mount");
    mount_point.create_dir_all()?;
    let mut daemon_b = fixture_b
        .command()?
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .arg("--fuse")
        .arg(mount_point.path())
        .spawn()?;
    let pid = daemon_b.id();
    scopeguard::defer! { let _ = kill(pid); }

    fixture_b.collect_stderr("B", &mut daemon_b);
    fixture_b.collect_stdout("B", &mut daemon_b);
    fixture_b.assert_listening().await;

    tokio::fs::write(fixture_a.testdir.join("hello.txt"), "Hello, world!").await?;

    // It might take a while for the new file to be reported by
    // inotify and then daemon_a, so retry.
    let goal = mount_point.path().join("testdir/hello.txt");
    let limit = Instant::now() + Duration::from_secs(3);
    while !tokio::fs::metadata(&goal).await.is_ok() && Instant::now() < limit {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(tokio::fs::metadata(&goal).await.is_ok());
    assert_eq!("Hello, world!", tokio::fs::read_to_string(&goal).await?);

    daemon_b.start_kill()?;
    daemon_a.start_kill()?;

    Ok(())
}

#[tokio::test]
async fn daemon_binds_socket() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    let daemon = fixture.command()?.spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    fixture.assert_listening().await;

    let local = LocalSet::new();
    local
        .run_until(async move {
            let control: realize_core::rpc::control::control_capnp::control::Client =
                unixsocket::connect(&fixture.socket).await?;

            let churten = control
                .churten_request()
                .send()
                .promise
                .await?
                .get()?
                .get_churten()?;

            let is_running_result = churten.is_running_request().send().promise.await?;
            assert!(!is_running_result.get()?.get_running());

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn daemon_exports_fuse() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Create a mount point for FUSE
    let mount_point = fixture.tempdir.child("fuse-mount");
    mount_point.create_dir_all()?;
    let original_dev = fs::metadata(mount_point.path())?.dev();

    // Run daemon with FUSE mount
    let mut daemon = fixture
        .command()?
        .arg("--fuse")
        .arg(mount_point.path())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    fixture.collect_stderr("daemon", &mut daemon);

    // The FUSE filesystem is mounted once the mount point reports being on another device.
    let limit = Instant::now() + Duration::from_secs(3);
    while fs::metadata(mount_point.path())?.dev() == original_dev && Instant::now() < limit {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(fs::metadata(mount_point.path())?.dev(), original_dev);

    // List the root directory content - the arena must appear
    let entries = std::fs::read_dir(mount_point.path())?;
    let entry_names: Vec<String> = entries
        .filter_map(|entry| {
            entry
                .ok()
                .and_then(|e| e.file_name().to_str().map(|s| s.to_string()))
        })
        .collect();

    // The arena "testdir" should be visible in the FUSE mount
    assert!(
        entry_names.contains(&"testdir".to_string()),
        "Expected to find 'testdir' arena in FUSE mount, found: {:?}",
        entry_names
    );

    // List the arena directory content - it should be empty
    let arena_path = mount_point.path().join("testdir");
    let arena_entries = std::fs::read_dir(&arena_path)?;
    let arena_entry_names: Vec<String> = arena_entries
        .filter_map(|entry| {
            entry
                .ok()
                .and_then(|e| e.file_name().to_str().map(|s| s.to_string()))
        })
        .collect();

    // The arena directory should be empty (no files synced yet)
    assert!(
        arena_entry_names.is_empty(),
        "Expected arena directory to be empty, found: {:?}",
        arena_entry_names
    );

    daemon.start_kill()?;

    Ok(())
}
