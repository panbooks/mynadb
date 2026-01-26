//! Raft-related types and implementations
//!
//! This module provides the foundation for Raft consensus.
//! Currently implements single-node mode with sled-backed storage.
//! Full multi-node Raft replication will be added in a later iteration.

// TODO: log_store is incomplete - needs TypeConfig definition and openraft API updates
// mod log_store;
pub mod network;
pub mod state_machine;

pub use network::MetaNetworkFactory;
pub use state_machine::MetaStateMachine;

/// Type alias for node ID
pub type NodeId = u64;

/// Node configuration for Raft
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct NodeConfig {
    pub addr: String,
}

impl std::fmt::Display for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}
