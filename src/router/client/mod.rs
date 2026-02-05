//! Router client module
//!
//! Provides clients for communicating with Meta and Data nodes.

pub mod circuit_breaker;
pub mod data_node_pool;

pub use crate::flight_protocol::QueryScope;
pub use data_node_pool::DataNodePool;
