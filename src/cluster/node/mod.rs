//! Node identity and registration module
//!
//! Handles node identification, registration with the cluster coordinator,
//! and health monitoring.

mod health;
mod identity;
mod registration;

pub use health::{HealthChecker, HealthStatus};
pub use identity::{NodeId, NodeInfo, NodeRole, NodeState};
pub use registration::NodeRegistration;
