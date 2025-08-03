use assert_fs::TempDir;
use assert_fs::prelude::*;
use nfs3_client::Nfs3ConnectionBuilder;
use nfs3_client::tokio::TokioConnector;
use nfs3_types::nfs3::{Nfs3Result, READDIR3args};
use realize_core::config::Config;
use realize_core::rpc::realstore;
use realize_core::rpc::realstore::client::ClientOptions;
use realize_network::Networking;
use realize_network::config::PeerConfig;
use realize_network::unixsocket;
use realize_storage::RealStoreOptions;
use realize_storage::config::{ArenaConfig, CacheConfig};
use realize_types;
use realize_types::{Arena, Peer};
use reqwest::Client;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tarpc::context;
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
        .join("realize-daemon")
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
        config.storage.arenas.insert(
            arena,
            ArenaConfig::new(
                testdir.to_path_buf(),
                tempdir.child("testdir-cache.db").to_path_buf(),
                tempdir.child("testdir-blobs").to_path_buf(),
            ),
        );

        testdir.child("foo.txt").write_str("hello")?;

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");

        config.network.peers.insert(
            Peer::from("a"),
            PeerConfig {
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
                ..Default::default()
            },
        );
        config.network.peers.insert(
            Peer::from("b"),
            PeerConfig {
                pubkey: std::fs::read_to_string(resources.join("b-spki.pem"))?,
                ..Default::default()
            },
        );

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

    fn configure_cache(&mut self) {
        // Cache is now always configured in setup, but this method can be used
        // to override cache configuration if needed for specific tests
        for (arena, arena_config) in &mut self.config.storage.arenas {
            let child = self.tempdir.child(format!("{arena}-cache.db"));
            let blob_dir = self.tempdir.child(format!("{arena}-blobs"));
            arena_config.db = child.to_path_buf();
            arena_config.blob_dir = blob_dir.to_path_buf();
        }
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
async fn daemon_starts_and_lists_files() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture
        .command()?
        // We're looking for specific log output on stderr
        .env("RUST_LOG", "realize_=debug")
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }
    let stderr = fixture.collect_stderr("daemon", &mut daemon);

    fixture.assert_listening().await;

    let a = Peer::from("a");
    let mut peers = fixture.config.network.peers.clone();
    peers.get_mut(&a).expect("PeerConfig of a").address = Some(fixture.server_address.clone());

    let networking = Networking::from_config(&peers, &fixture.resources.join("b.key"))?;
    let client = realstore::client::connect(&networking, a, ClientOptions::default()).await?;
    let files = client
        .list(
            context::current(),
            Arena::from("testdir"),
            RealStoreOptions::default(),
        )
        .await??;
    assert_unordered::assert_eq_unordered!(
        files.into_iter().map(|f| f.path).collect(),
        vec![realize_types::Path::parse("foo.txt")?]
    );

    // Kill to make sure stderr ends
    daemon.start_kill()?;

    // Make sure stderr contains the expected log message.
    let stderr = stderr.await??;
    assert!(
        stderr.contains("Accepted peer b from "),
        "stderr<<EOF\n{stderr}\nEOF"
    );

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
        stderr.contains("does not exist"),
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
        stderr.contains("No read access"),
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
        stderr.contains("No write access"),
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
            "realize_network::network::tcp=debug,realize_daemon=debug",
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
async fn daemon_exports_nfs() -> anyhow::Result<()> {
    let mut fixture = Fixture::setup().await?;

    let nfs_port = portpicker::pick_unused_port().expect("No ports free");
    let nfs_addr = format!("127.0.0.1:{nfs_port}");

    fixture.configure_cache();

    // Run process in the background and in a way that allows reading
    // its output, so we know what port to connect to.
    let mut daemon = fixture.command()?.arg("--nfs").arg(&nfs_addr).spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    assert_listening_to(&nfs_addr).await;

    log::debug!("Connecting to NFS port {nfs_port}");
    let mut connection =
        Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1".to_string(), "/".to_string())
            .connect_from_privileged_port(false)
            .mount_port(nfs_port)
            .nfs3_port(nfs_port)
            .mount()
            .await?;

    log::debug!("Connected to NFS port {nfs_port}");
    let root = connection.root_nfs_fh3();
    let readdir = connection
        .readdir(READDIR3args {
            dir: root,
            cookie: 0,
            cookieverf: nfs3_types::nfs3::cookieverf3::default(),
            count: 128 * 1024 * 1024,
        })
        .await?;
    match readdir {
        Nfs3Result::Err(err) => panic!("readdir failed:{err:?}"),
        Nfs3Result::Ok(res) => {
            assert_unordered::assert_eq_unordered!(
                vec!["testdir"],
                res.reply
                    .entries
                    .0
                    .iter()
                    .map(|e| std::str::from_utf8(e.name.as_ref()).expect("utf-8"))
                    .collect::<Vec<_>>()
            );
        }
    };
    let _ = connection.unmount().await;

    daemon.start_kill()?;

    Ok(())
}

#[tokio::test]
async fn daemon_updates_cache() -> anyhow::Result<()> {
    use nfs3_types::nfs3::{LOOKUP3args, READ3args, diropargs3, nfs_fh3};
    use std::time::Duration;
    use tokio_retry::strategy::FixedInterval;

    let mut fixture_a = Fixture::setup().await?;
    fixture_a
        .config
        .storage
        .arenas
        .get_mut(&Arena::from("testdir"))
        .unwrap()
        .db = fixture_a.tempdir.join("index.db");

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
        .peers
        .get_mut(&Peer::from("a"))
        .expect("peer a")
        .address = Some(fixture_a.server_address);

    let nfs_port = portpicker::pick_unused_port().expect("No ports free");
    let nfs_addr = format!("127.0.0.1:{nfs_port}");
    fixture_b.configure_cache();

    let mut daemon_b = fixture_b
        .command()?
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .arg("--nfs")
        .arg(&nfs_addr)
        .spawn()?;
    let pid = daemon_b.id();
    scopeguard::defer! { let _ = kill(pid); }

    fixture_b.collect_stderr("B", &mut daemon_b);
    fixture_b.collect_stdout("B", &mut daemon_b);
    assert_listening_to(&nfs_addr).await;

    tokio::fs::write(fixture_a.testdir.join("hello.txt"), "Hello, world!").await?;

    log::debug!("Connecting to NFS port {nfs_port}");
    let mut connection =
        Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1".to_string(), "/".to_string())
            .connect_from_privileged_port(false)
            .mount_port(nfs_port)
            .nfs3_port(nfs_port)
            .mount()
            .await?;

    let root_fh3 = connection.root_nfs_fh3();
    let lookup = connection
        .lookup(LOOKUP3args {
            what: diropargs3 {
                dir: root_fh3,
                name: "testdir".as_bytes().into(),
            },
        })
        .await?;
    let testdir_fh3 = match lookup {
        Nfs3Result::Err(e) => panic!("testdir lookup: {e:?}"),
        Nfs3Result::Ok(res) => res.object,
    };

    // It might take a while for the new file to be reported by
    // inotify and then daemon_a.
    let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
    let hello_txt_fh3: nfs_fh3;
    loop {
        let lookup = connection
            .lookup(LOOKUP3args {
                what: diropargs3 {
                    dir: testdir_fh3.clone(),
                    name: "hello.txt".as_bytes().into(),
                },
            })
            .await?;
        match lookup {
            Nfs3Result::Err(e) => {
                if let Some(delay) = retry.next() {
                    tokio::time::sleep(delay).await;
                    continue;
                } else {
                    panic!("hello.txt lookup: {e:?}");
                }
            }
            Nfs3Result::Ok(res) => {
                hello_txt_fh3 = res.object;
                break;
            }
        };
    }

    let read = connection
        .read(READ3args {
            file: hello_txt_fh3,
            offset: 0,
            count: 100,
        })
        .await?;
    let hello_txt_content = match read {
        Nfs3Result::Err(e) => panic!("hello.txt read: {e:?}"),
        Nfs3Result::Ok(res) => String::from_utf8(res.data.to_vec())?,
    };

    assert_eq!("Hello, world!", hello_txt_content);

    let _ = connection.unmount().await;

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
            let control = unixsocket::connect::<
                realize_core::rpc::control::control_capnp::control::Client,
            >(&fixture.socket)
            .await?;

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
