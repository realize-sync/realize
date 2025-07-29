use anyhow::Result;
use capnp::capability::Promise;
use capnp_rpc;
use clap::{Parser, Subcommand};
use realize_core::rpc::control::control_capnp;
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
    /// Run churten and print notifications
    Run,
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

/// Execute the churten run command
async fn execute_churten_run(socket_path: &PathBuf) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(async move {
            let control =
                unixsocket::connect::<control_capnp::control::Client>(socket_path).await?;

            // Start churten
            let churten = control
                .churten_request()
                .send()
                .promise
                .await?
                .get()?
                .get_churten()?;

            churten.start_request().send().promise.await?;
            println!("Churten started. Subscribing to notifications...");

            let res = run_churten(&churten).await;

            churten.shutdown_request().send().promise.await?;
            println!("Churten stopped.");

            res?;

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

async fn run_churten(
    churten: &realize_core::rpc::control::control_capnp::churten::Client,
) -> Result<(), anyhow::Error> {
    let ctrl_c = tokio::spawn(tokio::signal::ctrl_c());
    struct PrintSubscriber;
    impl control_capnp::churten::subscriber::Server for PrintSubscriber {
        fn notify(
            &mut self,
            params: control_capnp::churten::subscriber::NotifyParams,
        ) -> Promise<(), capnp::Error> {
            let notification = params.get().and_then(|p| p.get_notification());
            match notification {
                Ok(n) => println!("Notification: {:?}", n),
                Err(e) => println!("Notification error: {e}"),
            }
            Promise::ok(())
        }
    }
    let subscriber = capnp_rpc::new_client(PrintSubscriber);
    let mut req = churten.subscribe_request();
    req.get().set_subscriber(subscriber);
    req.send().promise.await?;
    let _ = ctrl_c.await;
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
            ChurtenCommands::Run => {
                execute_churten_run(&socket_path).await?;
            }
        },
    }

    Ok(())
}
