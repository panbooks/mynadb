//! Router Node module for MynaDB cluster mode
//!
//! The Router node is responsible for:
//! - Handling client protocols (HTTP, Arrow Flight, MySQL wire)
//! - Routing writes to appropriate Data nodes based on partition affinity
//! - Coordinating distributed queries across Data nodes
//! - Caching partition→node mappings from Meta service

pub mod api;
pub mod client;
pub mod config;
pub mod flight;
pub mod routing;
pub mod state;

pub use config::RouterConfig;
pub use state::RouterAppState;
