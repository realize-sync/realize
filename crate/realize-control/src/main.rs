use anyhow::Result;
use clap::{Parser, Subcommand};
use realize_core::utils::logging;
use realize_network::unixsocket;
use std::path::PathBuf;
use tokio::task::LocalSet;

/// Command-line tool for controlling a running instance of realize-daemon
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Socket path for connecting to the daemon
    #[arg(short, long, value_name = "SOCKET")]
    socket: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Churten-related commands
    Churten {
        #[command(subcommand)]
        command: ChurtenCommands,
    },
}

#[derive(Subcommand, Debug)]
enum ChurtenCommands {
    /// Start churten
    Start,
    /// Stop churten
    Stop,
    /// Check if churten is running
    IsRunning {
        /// Quiet mode - exit with 0 if running, 10 if not running
        #[arg(short, long)]
        quiet: bool,
    },
}

/// Get the default socket path by checking for the first existing socket
/// among the standard locations
fn get_default_socket_path() -> Option<PathBuf> {
    let socket_paths = [
        PathBuf::from("/run/realize/control.socket"),
        PathBuf::from("/var/run/realize/control.socket"),
        PathBuf::from("/tmp/realize/control.socket"),
    ];

    for path in socket_paths {
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Resolve the socket path from command line arguments or defaults
fn resolve_socket_path(socket_arg: Option<PathBuf>) -> Result<PathBuf> {
    match socket_arg {
        Some(path) => Ok(path),
        None => get_default_socket_path()
            .ok_or_else(|| anyhow::anyhow!("No socket found in default locations")),
    }
}

/// Execute the churten start command
async fn execute_churten_start(socket_path: &PathBuf) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(async move {
            let control = unixsocket::connect::<
                realize_core::rpc::control::control_capnp::control::Client,
            >(socket_path)
            .await?;

            let churten = control
                .churten_request()
                .send()
                .promise
                .await?
                .get()?
                .get_churten()?;

            churten.start_request().send().promise.await?;
            println!("Churten started successfully");

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

/// Execute the churten stop command
async fn execute_churten_stop(socket_path: &PathBuf) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(async move {
            let control = unixsocket::connect::<
                realize_core::rpc::control::control_capnp::control::Client,
            >(socket_path)
            .await?;

            let churten = control
                .churten_request()
                .send()
                .promise
                .await?
                .get()?
                .get_churten()?;

            churten.shutdown_request().send().promise.await?;
            println!("Churten stopped successfully");

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

/// Execute the churten is_running command
async fn execute_churten_is_running(socket_path: &PathBuf, quiet: bool) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(async move {
            log::debug!("RPC client start");
            let control = unixsocket::connect::<
                realize_core::rpc::control::control_capnp::control::Client,
            >(socket_path)
            .await?;
            log::debug!("RPC client started");

            let churten = control
                .churten_request()
                .send()
                .promise
                .await?
                .get()?
                .get_churten()?;

            log::debug!("churten");

            let is_running_result = churten.is_running_request().send().promise.await?;
            let is_running = is_running_result.get()?.get_running();

            log::debug!("is_running");

            if quiet {
                if is_running {
                    std::process::exit(0);
                } else {
                    std::process::exit(10);
                }
            } else {
                println!("{}", is_running);
            }

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    logging::init_with_info_modules(vec!["realize_control"]);

    // Resolve socket path
    let socket_path = resolve_socket_path(cli.socket)?;
    log::debug!("Connecting to {socket_path:?}");

    // Execute the appropriate command
    match cli.command {
        Commands::Churten { command } => match command {
            ChurtenCommands::Start => {
                execute_churten_start(&socket_path).await?;
            }
            ChurtenCommands::Stop => {
                execute_churten_stop(&socket_path).await?;
            }
            ChurtenCommands::IsRunning { quiet } => {
                execute_churten_is_running(&socket_path, quiet).await?;
            }
        },
    }

    Ok(())
}
