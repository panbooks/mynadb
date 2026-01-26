//! MynaDB - The Voice of Your Machines
//!
//! Time series database with InfluxDB line protocol ingestion,
//! Parquet storage, and SQL queries via Apache DataFusion.
//!
//! Usage:
//! - `mynadb` or `mynadb standalone` - Run in standalone mode (default)
//! - `mynadb meta start|stop` - Meta node (Raft-based metadata service)
//! - `mynadb router start|stop` - Router node (client protocols + routing)
//! - `mynadb data start|stop` - Data node (storage + query execution)
//! - `mynadb version` - Show version information

mod config;
mod data;
mod jobs;
mod meta;
mod node;
mod query;
mod router;
mod services;
mod storage;

use clap::{Parser, Subcommand};

/// Version information compiled at build time
pub mod version {
    pub const GIT_VERSION: &str = env!("GIT_VERSION");
    pub const GIT_HASH: &str = env!("GIT_HASH");
    pub const GIT_COMMIT_DATE: &str = env!("GIT_COMMIT_DATE");
    pub const BUILD_TIME: &str = env!("BUILD_TIME");
    pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");
    pub const CARGO_VERSION: &str = env!("CARGO_PKG_VERSION");

    pub fn print_version() {
        println!("MynaDB - The Voice of Your Machines");
        println!("  Version:       {}", GIT_VERSION);
        println!("  Git Hash:      {}", GIT_HASH);
        println!("  Commit Date:   {}", GIT_COMMIT_DATE);
        println!("  Build Time:    {}", BUILD_TIME);
        println!("  Cargo Version: {}", CARGO_VERSION);
        println!("  Rust Version:  {}", RUSTC_VERSION);
    }
}

// ============================================================================
// CLI Structure
// ============================================================================

/// MynaDB - The Voice of Your Machines
/// Time series database with Arrow Flight API
#[derive(Parser, Debug)]
#[command(name = "mynadb")]
#[command(version = env!("GIT_VERSION"))]
#[command(about = "MynaDB - The Voice of Your Machines", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short = 'c', long = "config", default_value = "config.toml", global = true)]
    config: String,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Option<Commands>,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run in standalone mode (all-in-one, no cluster)
    /// This is the default if no subcommand is specified
    Standalone,

    /// Meta node operations (Raft-based metadata service)
    Meta {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Router node operations (client protocols and request routing)
    Router {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Data node operations (storage and query execution)
    Data {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Show version information
    Version,
}

/// Node start/stop actions
#[derive(Subcommand, Debug, Clone)]
enum NodeAction {
    /// Start the node (runs in foreground)
    Start,
    /// Stop a running node (sends SIGTERM)
    Stop,
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        // No subcommand or explicit standalone
        None | Some(Commands::Standalone) => {
            node::standalone::run(&cli.config).await;
        }

        // Meta node
        Some(Commands::Meta { action }) => match action {
            NodeAction::Start => node::meta::start(&cli.config).await,
            NodeAction::Stop => node::meta::stop(&cli.config),
        },

        // Router node
        Some(Commands::Router { action }) => match action {
            NodeAction::Start => node::router::start(&cli.config).await,
            NodeAction::Stop => node::router::stop(&cli.config),
        },

        // Data node
        Some(Commands::Data { action }) => match action {
            NodeAction::Start => node::data::start(&cli.config).await,
            NodeAction::Stop => node::data::stop(&cli.config),
        },

        // Version
        Some(Commands::Version) => {
            version::print_version();
        }
    }
}
