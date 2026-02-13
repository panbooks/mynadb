//! MynaDB - The Voice of Your Machines
//!
//! Time series database with InfluxDB line protocol ingestion,
//! Parquet storage, and SQL queries via Apache DataFusion.
//!
//! Usage:
//! - `mynadb` or `mynadb standalone` - Run in standalone mode (default)
//! - `mynadb stop` or `mynadb standalone stop` - Stop a running standalone instance
//! - `mynadb meta start|stop` - Meta node (Raft-based metadata service)
//! - `mynadb router start|stop` - Router node (client protocols + routing)
//! - `mynadb data start|stop` - Data node (storage + query execution)
//! - `mynadb admin start|stop` - Admin console for cluster management
//! - `mynadb version` - Show version information

mod admin;
mod aggregates;
mod auth;
mod config;
mod data;
mod flight_protocol;
mod iceberg;
mod jobs;
mod meta;
mod metrics;
mod node;
mod observability;
mod query;
mod router;
mod services;
mod storage;
mod utils;

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
    #[arg(
        short = 'c',
        long = "config",
        default_value = "config.toml",
        global = true
    )]
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
    Standalone {
        #[command(subcommand)]
        action: Option<StandaloneAction>,
    },

    /// Stop a running standalone instance (shortcut for `standalone stop`)
    Stop,

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

    /// Admin console for cluster management
    Admin {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Show version information
    Version,

    /// Hash a password for use in config file
    HashPassword {
        /// Password to hash (if not provided, prompts for input)
        password: Option<String>,
    },

}

/// Standalone mode actions
#[derive(Subcommand, Debug, Clone)]
enum StandaloneAction {
    /// Stop a running standalone instance (sends SIGTERM, flushes data)
    Stop,
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
        // No subcommand or explicit standalone (run)
        None | Some(Commands::Standalone { action: None }) => {
            node::standalone::run(&cli.config).await;
        }

        // Standalone stop
        Some(Commands::Standalone {
            action: Some(StandaloneAction::Stop),
        })
        | Some(Commands::Stop) => {
            node::standalone::stop(&cli.config);
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

        // Admin console
        Some(Commands::Admin { action }) => match action {
            NodeAction::Start => node::admin::start(&cli.config).await,
            NodeAction::Stop => node::admin::stop(&cli.config),
        },

        // Version
        Some(Commands::Version) => {
            version::print_version();
        }

        // Hash password
        Some(Commands::HashPassword { password }) => {
            let pwd = match password {
                Some(p) => p,
                None => {
                    // Read from stdin
                    eprint!("Enter password: ");
                    let mut input = String::new();
                    std::io::stdin()
                        .read_line(&mut input)
                        .expect("Failed to read password");
                    input.trim().to_string()
                }
            };

            match auth::hash_password(&pwd) {
                Ok(hash) => {
                    println!("{}", hash);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

    }
}
