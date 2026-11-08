//! `zed-audit-loop`: the audit daemon plus the two queries useful before
//! starting it, matching the check-list in the bridge spec's CLI section.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::{Parser, Subcommand};

use zed_audit_loop::config::Config;
use zed_audit_loop::daemon::{Daemon, InstanceLock};
use zed_audit_loop::logging::{Level, Logger};
use zed_audit_loop::protocol::{BridgeClient, Status};
use zed_audit_loop::time::unix_millis;

#[derive(Parser)]
#[command(
    name = "zed-audit-loop",
    about = "Run an audit loop against a Zed agent thread through the external bridge",
    version
)]
struct Cli {
    /// Config file. Required by `run`, used by the queries when present.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Bridge socket, overriding the config.
    #[arg(long, global = true, value_name = "PATH")]
    socket: Option<PathBuf>,

    /// Zed release channel, overriding the config (stable, preview, nightly,
    /// dev).
    #[arg(long, global = true, value_name = "CHANNEL")]
    channel: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the audit loop until interrupted.
    Run,
    /// Check the bridge connection by listing threads.
    ListThreads,
    /// Read one thread's current state.
    ThreadStatus {
        /// The thread id to query.
        thread_id: String,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("zed-audit-loop: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let home = std::env::var_os("HOME").map(PathBuf::from);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the tokio runtime")?;

    match &cli.command {
        Command::Run => {
            let path = cli
                .config
                .as_deref()
                .context("`run` needs --config pointing at a TOML file")?;
            let mut config = Config::load(path)?;
            apply_overrides(&mut config, &cli);
            config.validate()?;

            let home = home.context(
                "HOME is not set; the daemon needs it for its storage directory \
                 (set [storage] dir explicitly is not enough to expand `~`)",
            )?;
            let logger = build_logger(&config, &home)?;
            logger.info(
                "config_loaded",
                &[
                    ("config", &path.display().to_string()),
                    ("socket", &config.socket_path(&home).display().to_string()),
                    ("storage", &config.storage_dir(&home).display().to_string()),
                ],
            );

            let storage_dir = config.storage_dir(&home);
            runtime.block_on(async move {
                let _lock = InstanceLock::acquire(&storage_dir)?;
                let mut daemon = Daemon::new(config, logger, home)?;
                daemon.run().await
            })
        }
        Command::ListThreads => runtime.block_on(list_threads(&cli, home.as_deref())),
        Command::ThreadStatus { thread_id } => {
            runtime.block_on(thread_status(&cli, home.as_deref(), thread_id))
        }
    }
}

async fn list_threads(cli: &Cli, home: Option<&std::path::Path>) -> Result<()> {
    let mut client = build_client(cli, home)?;
    let request_id = format!("cli-list-{}", unix_millis(std::time::SystemTime::now()));
    let response = client.list_threads(&request_id).await?;
    if response.is_rejection() {
        anyhow::bail!(
            "bridge refused list_threads: {}",
            response
                .error_code()
                .map(ToString::to_string)
                .unwrap_or_else(|| response.status.to_string())
        );
    }
    print_threads(&response.threads.unwrap_or_default());
    Ok(())
}

async fn thread_status(cli: &Cli, home: Option<&std::path::Path>, thread_id: &str) -> Result<()> {
    let mut client = build_client(cli, home)?;
    let request_id = format!("cli-status-{}", unix_millis(std::time::SystemTime::now()));
    let response = client.thread_status(&request_id, thread_id).await?;
    match &response.thread {
        Some(thread) => {
            println!("thread: {thread_id}");
            println!("found: {}", thread.found);
            println!("archived: {}", thread.archived);
            println!("state: {}", thread.state);
            println!("deliverable: {}", thread.deliverable);
            println!("loaded: {}", thread.loaded);
        }
        None => println!("status: {}", response.status),
    }
    // `thread_not_found` is an answer, not a failure.
    if response.status != Status::ThreadNotFound && response.is_rejection() {
        anyhow::bail!(
            "bridge refused get_thread_status: {}",
            response
                .error_code()
                .map(ToString::to_string)
                .unwrap_or_else(|| response.status.to_string())
        );
    }
    Ok(())
}

fn apply_overrides(config: &mut Config, cli: &Cli) {
    if let Some(socket) = &cli.socket {
        config.bridge.socket = Some(socket.clone());
    }
    if let Some(channel) = &cli.channel {
        config.bridge.channel = channel.clone();
    }
}

fn build_client(cli: &Cli, home: Option<&std::path::Path>) -> Result<BridgeClient> {
    // The queries only need bridge settings, so a config file is optional and is
    // not validated for a target it will never use.
    let mut config = match &cli.config {
        Some(path) => Config::load(path)?,
        None => Config::default(),
    };
    apply_overrides(&mut config, cli);

    if config.bridge.socket.is_none() && home.is_none() {
        // Without HOME the default socket path cannot be built, so say what to do
        // rather than resolving a nonsense path.
        anyhow::bail!("HOME is not set; pass --socket or set [bridge] socket in the config");
    }
    Ok(BridgeClient::new(
        config.client_config(home.unwrap_or(std::path::Path::new(""))),
    ))
}

fn build_logger(config: &Config, home: &std::path::Path) -> Result<Logger> {
    let mut logger = Logger::stderr(config.log.level).with_stderr(config.log.stderr);
    if let Some(path) = config.log_file(home) {
        logger = logger.with_file(&path)?;
    }
    // Surfacing the level keeps the log self-describing.
    logger.debug(
        "log_ready",
        &[(
            "level",
            match config.log.level {
                Level::Debug => "debug",
                Level::Info => "info",
                Level::Warn => "warn",
                Level::Error => "error",
            },
        )],
    );
    Ok(logger)
}

fn print_threads(threads: &[zed_audit_loop::protocol::ThreadSummary]) {
    if threads.is_empty() {
        println!("no threads");
        return;
    }
    println!("{:<38} {:<18} TITLE", "THREAD ID", "STATE");
    for thread in threads {
        let archived = if thread.archived { " (archived)" } else { "" };
        println!(
            "{:<38} {:<18} {}{}",
            thread.thread_id,
            thread.state,
            thread.title.as_deref().unwrap_or("(untitled)"),
            archived
        );
    }
}
