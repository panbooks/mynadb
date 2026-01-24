//! Node identity and registration module
//!
//! Handles node identification, registration with the cluster coordinator,
//! and health monitoring.

mod identity;
mod registration;
mod health;

pub use identity::{NodeInfo, NodeRole, NodeState, NodeId};
pub use registration::NodeRegistration;
pub use health::{HealthChecker, HealthStatus};
