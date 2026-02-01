//! Meta Service - Raft-based coordination for MynaDB cluster
//!
//! This module implements a distributed metadata service using the Raft consensus
//! algorithm (via openraft) with sled as the embedded storage backend.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────┐
//! │                         Meta Service                                │
//! │                                                                     │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
//! │  │ gRPC Server │───►│ Raft Engine │───►│ State Machine (sled)    │ │
//! │  │  (tonic)    │    │  (openraft) │    │  - Nodes, Files, Locks  │ │
//! │  └─────────────┘    └─────────────┘    └─────────────────────────┘ │
//! │                            │                                        │
//! │                            ▼                                        │
//! │                     ┌─────────────┐                                 │
//! │                     │ Raft Network│ (peer communication)            │
//! │                     └─────────────┘                                 │
//! └────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Features
//!
//! - **Linearizable operations**: All writes go through Raft log
//! - **Atomic locks with fencing tokens**: Prevents zombie lock issues
//! - **Sequence-based events**: No message loss in pub/sub
//! - **Sub-second failover**: Fast leader election (~650ms)

pub mod client;
pub mod config;
pub mod raft;
pub mod replica_assigner;
pub mod scale_in;
pub mod service;
pub mod types;

pub use types::*;

// Re-export generated protobuf types
pub mod proto {
    tonic::include_proto!("meta");
}
