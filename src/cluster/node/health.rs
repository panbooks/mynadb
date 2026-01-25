//! Health checking for cluster nodes
//!
//! Monitors the health of this node and other nodes in the cluster.

use super::identity::{NodeId, NodeInfo, NodeState};
use super::registration::NodeRegistration;
use crate::cluster::coordinator::Coordinator;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Health status of a component
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but functional
    Degraded,
    /// Component is unhealthy
    Unhealthy,
    /// Component status is unknown
    Unknown,
}

impl Default for HealthStatus {
    fn default() -> Self {
        HealthStatus::Unknown
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
            HealthStatus::Unknown => write!(f, "unknown"),
        }
    }
}

/// Health check result for a single check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Name of the check
    pub name: String,
    /// Status of the check
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
    /// When the check was performed
    pub checked_at: DateTime<Utc>,
    /// Duration of the check in milliseconds
    pub duration_ms: u64,
}

/// Overall health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Overall status
    pub status: HealthStatus,
    /// Individual health checks
    pub checks: Vec<HealthCheck>,
    /// When the report was generated
    pub timestamp: DateTime<Utc>,
    /// Node ID
    pub node_id: NodeId,
}

impl HealthReport {
    /// Create a new health report
    pub fn new(node_id: NodeId, checks: Vec<HealthCheck>) -> Self {
        let status = checks.iter().fold(HealthStatus::Healthy, |acc, check| {
            match (acc, check.status) {
                (HealthStatus::Unhealthy, _) | (_, HealthStatus::Unhealthy) => {
                    HealthStatus::Unhealthy
                }
                (HealthStatus::Degraded, _) | (_, HealthStatus::Degraded) => HealthStatus::Degraded,
                (HealthStatus::Unknown, _) | (_, HealthStatus::Unknown) => HealthStatus::Unknown,
                _ => HealthStatus::Healthy,
            }
        });

        Self {
            status,
            checks,
            timestamp: Utc::now(),
            node_id,
        }
    }
}

/// Health check function type
pub type HealthCheckFn = Box<dyn Fn() -> (HealthStatus, Option<String>) + Send + Sync>;

/// Health checker for monitoring node health
pub struct HealthChecker {
    /// Node registration
    registration: Arc<NodeRegistration>,
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// Health checks to run
    checks: Arc<RwLock<Vec<(String, HealthCheckFn)>>>,
    /// Last health report
    last_report: Arc<RwLock<Option<HealthReport>>>,
    /// Node health cache (node_id -> last_seen)
    node_health: Arc<RwLock<HashMap<NodeId, NodeHealth>>>,
    /// Check interval
    check_interval: Duration,
    /// Unhealthy threshold (seconds since last heartbeat)
    unhealthy_threshold: ChronoDuration,
    /// Health check task handle
    check_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

/// Cached health info for a node
#[derive(Debug, Clone)]
struct NodeHealth {
    /// Last seen timestamp
    last_seen: DateTime<Utc>,
    /// Current state
    state: NodeState,
    /// Consecutive failed checks
    failed_checks: u32,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(
        registration: Arc<NodeRegistration>,
        coordinator: Arc<dyn Coordinator>,
        check_interval: Duration,
        unhealthy_threshold_secs: u64,
    ) -> Self {
        Self {
            registration,
            coordinator,
            checks: Arc::new(RwLock::new(Vec::new())),
            last_report: Arc::new(RwLock::new(None)),
            node_health: Arc::new(RwLock::new(HashMap::new())),
            check_interval,
            unhealthy_threshold: ChronoDuration::seconds(unhealthy_threshold_secs as i64),
            check_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Add a health check
    pub fn add_check<F>(&self, name: impl Into<String>, check: F)
    where
        F: Fn() -> (HealthStatus, Option<String>) + Send + Sync + 'static,
    {
        self.checks.write().push((name.into(), Box::new(check)));
    }

    /// Run all health checks
    pub async fn run_checks(&self) -> HealthReport {
        let node_id = self.registration.node_id();

        let mut results = Vec::new();

        // Run each check (collect names first to release lock)
        let check_names: Vec<String> = self.checks.read().iter().map(|(n, _)| n.clone()).collect();

        // Run each check
        for name in check_names {
            if let Some((_, check_fn)) = self.checks.read().iter().find(|(n, _)| n == &name) {
                let start = std::time::Instant::now();
                let (status, message) = check_fn();
                let duration_ms = start.elapsed().as_millis() as u64;

                results.push(HealthCheck {
                    name,
                    status,
                    message,
                    checked_at: Utc::now(),
                    duration_ms,
                });
            }
        }

        // Add coordinator connectivity check
        let coord_start = std::time::Instant::now();
        let coord_connected = self.coordinator.is_connected().await;
        results.push(HealthCheck {
            name: "coordinator".to_string(),
            status: if coord_connected {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: if coord_connected {
                None
            } else {
                Some("Coordinator not connected".to_string())
            },
            checked_at: Utc::now(),
            duration_ms: coord_start.elapsed().as_millis() as u64,
        });

        let report = HealthReport::new(node_id, results);
        *self.last_report.write() = Some(report.clone());

        report
    }

    /// Get the last health report
    pub fn last_report(&self) -> Option<HealthReport> {
        self.last_report.read().clone()
    }

    /// Start periodic health checks
    pub fn start(&self) {
        let coordinator = Arc::clone(&self.coordinator);
        let registration = Arc::clone(&self.registration);
        let checks = Arc::clone(&self.checks);
        let last_report = Arc::clone(&self.last_report);
        let node_health = Arc::clone(&self.node_health);
        let interval = self.check_interval;
        let unhealthy_threshold = self.unhealthy_threshold;

        let handle = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);

            loop {
                timer.tick().await;

                let node_id = registration.node_id();
                let check_names: Vec<String> =
                    checks.read().iter().map(|(n, _)| n.clone()).collect();

                let mut results = Vec::new();

                // Run custom checks
                for name in check_names {
                    if let Some((_, check_fn)) = checks.read().iter().find(|(n, _)| n == &name) {
                        let start = std::time::Instant::now();
                        let (status, message) = check_fn();
                        results.push(HealthCheck {
                            name,
                            status,
                            message,
                            checked_at: Utc::now(),
                            duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                }

                // Check coordinator
                let coord_start = std::time::Instant::now();
                let coord_connected = coordinator.is_connected().await;
                results.push(HealthCheck {
                    name: "coordinator".to_string(),
                    status: if coord_connected {
                        HealthStatus::Healthy
                    } else {
                        HealthStatus::Unhealthy
                    },
                    message: if coord_connected {
                        None
                    } else {
                        Some("Coordinator not connected".to_string())
                    },
                    checked_at: Utc::now(),
                    duration_ms: coord_start.elapsed().as_millis() as u64,
                });

                let report = HealthReport::new(node_id.clone(), results);
                *last_report.write() = Some(report.clone());

                // Check other nodes' health
                if let Ok(nodes) = registration.list_nodes().await {
                    let now = Utc::now();
                    let mut health = node_health.write();

                    for node in nodes {
                        if node.node_id == node_id {
                            continue; // Skip self
                        }

                        let entry = health.entry(node.node_id.clone()).or_insert(NodeHealth {
                            last_seen: node.last_heartbeat,
                            state: node.state,
                            failed_checks: 0,
                        });

                        if node.last_heartbeat > entry.last_seen {
                            entry.last_seen = node.last_heartbeat;
                            entry.state = node.state;
                            entry.failed_checks = 0;
                        } else if now - entry.last_seen > unhealthy_threshold {
                            entry.failed_checks += 1;
                            if entry.failed_checks >= 3 {
                                warn!(
                                    "Node {} appears unhealthy (last seen: {})",
                                    node.node_id, entry.last_seen
                                );
                            }
                        }
                    }
                }

                debug!("Health check completed: status={}", report.status);
            }
        });

        *self.check_handle.lock() = Some(handle);
    }

    /// Stop periodic health checks
    pub fn stop(&self) {
        if let Some(handle) = self.check_handle.lock().take() {
            handle.abort();
        }
    }

    /// Get health status of a specific node
    pub fn get_node_health(&self, node_id: &str) -> Option<HealthStatus> {
        let health = self.node_health.read();
        health.get(node_id).map(|h| {
            let now = Utc::now();
            if now - h.last_seen > self.unhealthy_threshold {
                HealthStatus::Unhealthy
            } else if h.state == NodeState::Draining {
                HealthStatus::Degraded
            } else if h.state == NodeState::Active {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unknown
            }
        })
    }

    /// Get all nodes' health status
    pub fn all_nodes_health(&self) -> HashMap<NodeId, HealthStatus> {
        let health = self.node_health.read();
        let now = Utc::now();

        health
            .iter()
            .map(|(id, h)| {
                let status = if now - h.last_seen > self.unhealthy_threshold {
                    HealthStatus::Unhealthy
                } else if h.state == NodeState::Draining {
                    HealthStatus::Degraded
                } else if h.state == NodeState::Active {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unknown
                };
                (id.clone(), status)
            })
            .collect()
    }
}

impl Drop for HealthChecker {
    fn drop(&mut self) {
        self.stop();
    }
}
