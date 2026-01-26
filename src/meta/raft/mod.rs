//! Raft consensus implementation for Meta service HA
//!
//! This module provides Raft-based replication for the Meta service using OpenRaft.
//! It enables multi-node Meta clusters with automatic leader election and log replication.
//!
//! ## Architecture
//!
//! - `TypeConfig`: Defines the Raft type configuration (NodeId, Entry types, etc.)
//! - `SledLogStore`: Persistent log storage using sled embedded database
//! - `MetaStateMachine`: State machine that applies commands to the metadata store
//! - `MetaNetworkFactory`: Network layer for peer-to-peer Raft communication

pub mod log_store;
pub mod network;
pub mod state_machine;

pub use log_store::SledLogStore;
pub use network::{MetaNetworkConnection, MetaNetworkFactory};
pub use state_machine::{MetaStateMachine, MetaSnapshot, MetaSnapshotBuilder};

use crate::meta::types::{MetaCommand, MetaResponse};
use openraft::raft::responder::OneshotResponder;
use openraft::BasicNode;
use std::io::Cursor;

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

// ============================================================================
// OpenRaft Type Configuration
// ============================================================================

/// Raft type configuration for Meta service
///
/// This defines all the concrete types used by OpenRaft:
/// - `D`: Request/command type (MetaCommand)
/// - `R`: Response type (MetaResponse)
/// - `NodeId`: Node identifier (u64)
/// - `Node`: Node metadata (BasicNode with address)
/// - `Entry`: Log entry type (openraft::Entry)
/// - `SnapshotData`: Snapshot data type (Cursor<Vec<u8>>)
/// - `AsyncRuntime`: Async runtime (Tokio)
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    /// The command/request type that gets replicated
    type D = MetaCommand;

    /// The response type returned after applying a command
    type R = MetaResponse;

    /// Node identifier type
    type NodeId = NodeId;

    /// Node metadata type (contains address info)
    type Node = BasicNode;

    /// Log entry type
    type Entry = openraft::Entry<Self>;

    /// Snapshot data type - serialized state
    type SnapshotData = Cursor<Vec<u8>>;

    /// Async runtime to use
    type AsyncRuntime = openraft::TokioRuntime;

    /// Responder type for client write operations
    type Responder = OneshotResponder<Self>;
}

/// Type alias for Raft instance
pub type MetaRaft = openraft::Raft<TypeConfig>;

/// Type alias for Raft log entry
pub type Entry = openraft::Entry<TypeConfig>;

/// Type alias for log ID
pub type LogId = openraft::LogId<NodeId>;

/// Type alias for Vote
pub type Vote = openraft::Vote<NodeId>;

/// Type alias for snapshot metadata
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, BasicNode>;

/// Type alias for stored membership
pub type StoredMembership = openraft::StoredMembership<NodeId, BasicNode>;
