//! Router Node module for MynaDB cluster mode
//!
//! The Router node is responsible for:
//! - Handling client protocols (HTTP, Arrow Flight, MySQL wire)
//! - Routing writes to appropriate Data nodes based on partition affinity
//! - Coordinating distributed queries across Data nodes
//! - Caching partition→node mappings from Meta service

pub mod client;
pub mod config;
pub mod routing;

pub use config::RouterConfig;
