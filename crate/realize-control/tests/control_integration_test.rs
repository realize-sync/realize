use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_storage::config::{ArenaConfig, CacheConfig};
use realize_types::{Arena, Peer};
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

fn command_path(cmd: &str) -> PathBuf {
    // Expecting a path for the current exe to look like
    // target/debug/deps/integration_test
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(cmd)
}

struct Fixture {
    pub config: Config,
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
        // Example: TEST_DEBUG=1 cargo nextest run -p realize-control --test control_integration_test --nocapture
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

        // Add a simple peer configuration
        config.network.peers.insert(
            Peer::from("a"),
            realize_network::config::PeerConfig {
                address: None,
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
            },
        );

        let server_privkey = resources.join("a.key");
        let server_port = portpicker::pick_unused_port().expect("No ports free");
        let server_address = format!("127.0.0.1:{server_port}");
        let socket = tempdir.path().join("realize/control.socket");
        Ok(Self {
            config,
            tempdir,
            server_address,
            server_privkey,
            debug_output: debug,
            socket,
        })
    }

    pub fn daemon_command(&self) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path("realize-daemon"));

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

    pub fn control_command(&self, args: &[&str]) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path("realize-control"));
        cmd.arg("--socket")
            .arg(&self.socket)
            .args(args)
            .kill_on_drop(true);

        if self.debug_output {
            cmd.env("RUST_LOG", "debug");
        } else {
            cmd.env_remove("RUST_LOG");
        }

        Ok(cmd)
    }

    async fn assert_listening(&self) {
        // Wait for the daemon to start and bind to the socket
        for i in 0..150 {
            // 150 * 100ms = 15 seconds
            if self.socket.exists() {
                if self.debug_output {
                    eprintln!("Socket found at {:?} after {} attempts", self.socket, i);
                }
                // Give the daemon a moment to fully start up
                sleep(Duration::from_millis(500)).await;
                return;
            }
            if self.debug_output && i % 10 == 0 {
                eprintln!("Waiting for socket at {:?}, attempt {}", self.socket, i);
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("Daemon never created socket at {:?}", self.socket);
    }
}

fn kill(pid: Option<u32>) -> anyhow::Result<()> {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid.expect("no pid") as i32),
        nix::sys::signal::Signal::SIGTERM,
    )?;

    Ok(())
}

#[tokio::test]
async fn control_connects_to_daemon() -> anyhow::Result<()> {
    let fixture = Fixture::setup().await?;

    // Start the daemon
    let daemon = fixture.daemon_command()?.spawn()?;
    let pid = daemon.id();
    scopeguard::defer! { let _ = kill(pid); }

    // Wait for the daemon to start and bind to the socket
    if fixture.debug_output {
        eprintln!("Waiting for daemon to start...");
    }
    fixture.assert_listening().await;

    if fixture.debug_output {
        eprintln!("Daemon started, testing control command...");
    }

    // Test the control command with timeout
    let mut control_cmd = fixture.control_command(&["churten", "is-running"])?;
    if fixture.debug_output {
        eprintln!("Running control command: {:?}", control_cmd);
    }

    let output = tokio::time::timeout(Duration::from_secs(3), control_cmd.output()).await??;
    if !output.status.success() {
        panic!("Control command failed: {output:?}");
    }

    // Check that the output is either "true" or "false"
    let output_str = String::from_utf8(output.stdout)?;
    let output_str = output_str.trim();
    assert!(
        output_str == "true" || output_str == "false",
        "Expected 'true' or 'false', got '{}'",
        output_str
    );

    // Test the quiet mode
    let output = fixture
        .control_command(&["churten", "is-running", "-q"])?
        .output()
        .await?;

    // Quiet mode should exit with 0 or 10, not print anything
    assert!(
        output.status.code() == Some(0) || output.status.code() == Some(10),
        "Expected exit code 0 or 10, got {}",
        output.status
    );

    let output_str = String::from_utf8(output.stdout)?;
    assert!(
        output_str.is_empty(),
        "Expected empty output in quiet mode, got '{}'",
        output_str
    );

    Ok(())
}
