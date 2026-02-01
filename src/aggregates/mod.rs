//! Continuous Aggregates Module
//!
//! TODO: Need to integrate with the main pipeline:
//! - Hook ContinuousAggregateRewriter into QueryEngine
//! - Start ContinuousAggregateJob in standalone/data node startup
//! - Add REST API endpoints for CRUD operations
//! - Implement DataFusionRefreshExecutor to execute refresh queries
//!
//! This module provides TimescaleDB-style continuous aggregates for MynaDB.
//! Continuous aggregates are materialized views that are automatically refreshed
//! as new data arrives, enabling fast dashboard queries without repeated computation.
//!
//! # Architecture
//!
//! ```text
//! Raw Data (7 days) → 1-min Agg (30 days) → 1-hour Agg (1 year) → 1-day Agg (forever)
//!       │                    │                    │                    │
//!       └────────────────────┴────────────────────┴────────────────────┘
//!                           Query Rewriter selects best tier
//! ```
//!
//! # Features
//!
//! - **Incremental Refresh**: Only processes new data since last watermark
//! - **Hierarchical Aggregates**: Aggregates can be built on other aggregates
//! - **Query Rewriting**: Automatically redirects queries to pre-aggregated data
//! - **Retention Per Tier**: Different retention policies for each granularity
//! - **Pluggable Design**: Can be enabled/disabled via configuration
//!
//! # Usage
//!
//! ```sql
//! -- Create a continuous aggregate
//! CREATE CONTINUOUS AGGREGATE hourly_cpu
//! WITH (
//!     refresh_interval = '5 minutes',
//!     retention_days = 365
//! )
//! AS SELECT
//!     time_bucket(time, 3600000000000, 0) as hour,  -- 1 hour in nanos
//!     host,
//!     avg(cpu_usage) as avg_cpu,
//!     max(cpu_usage) as max_cpu,
//!     min(cpu_usage) as min_cpu,
//!     count(*) as sample_count
//! FROM mydb.default.cpu
//! GROUP BY hour, host;
//!
//! -- Query is automatically rewritten to use aggregate
//! SELECT time_bucket(time, 3600000000000, 0) as hour, host, avg(cpu_usage)
//! FROM mydb.default.cpu
//! WHERE time >= now() - interval '7 days'
//! GROUP BY hour, host;
//! -- Rewrites to: SELECT hour, host, avg_cpu FROM mydb.default._ca_hourly_cpu WHERE ...
//! ```

pub mod config;
pub mod definition;
pub mod job;
pub mod query_rewriter;
pub mod storage;
pub mod types;

// Re-export main types for convenience
pub use config::ContinuousAggregateConfig;
pub use definition::{
    AggregateColumn, AggregateDefinition, AggregateFunction, GroupByColumn, RefreshPolicy,
};
pub use job::ContinuousAggregateJob;
pub use query_rewriter::ContinuousAggregateRewriter;
pub use storage::AggregateStorage;
pub use types::{AggregateState, RefreshResult, WatermarkInfo};

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Feature flag check - is continuous aggregates enabled?
pub fn is_enabled(config: &ContinuousAggregateConfig) -> bool {
    config.enabled
}

/// Manager for continuous aggregates
///
/// This is the main entry point for the continuous aggregates subsystem.
/// It coordinates definition storage, refresh jobs, and query rewriting.
pub struct ContinuousAggregateManager {
    config: ContinuousAggregateConfig,
    storage: Arc<AggregateStorage>,
    job: Option<Arc<ContinuousAggregateJob>>,
    rewriter: Arc<ContinuousAggregateRewriter>,
    /// Runtime state
    running: Arc<RwLock<bool>>,
}

impl ContinuousAggregateManager {
    /// Create a new manager with the given configuration
    pub fn new(config: ContinuousAggregateConfig, data_dir: &str) -> Self {
        let storage = Arc::new(AggregateStorage::new(data_dir));
        let rewriter = Arc::new(ContinuousAggregateRewriter::new(Arc::clone(&storage)));

        Self {
            config,
            storage,
            job: None,
            rewriter,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Check if the feature is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the query rewriter (for integration with QueryEngine)
    pub fn rewriter(&self) -> &Arc<ContinuousAggregateRewriter> {
        &self.rewriter
    }

    /// Get the storage backend
    pub fn storage(&self) -> &Arc<AggregateStorage> {
        &self.storage
    }

    /// Start the background refresh job
    pub async fn start(&mut self) -> Result<(), String> {
        if !self.config.enabled {
            info!("Continuous aggregates disabled, skipping job start");
            return Ok(());
        }

        let mut running = self.running.write().await;
        if *running {
            return Err("Continuous aggregate job already running".to_string());
        }

        let job = Arc::new(ContinuousAggregateJob::new(
            self.config.clone(),
            Arc::clone(&self.storage),
        ));

        // Clone for the spawned task
        let job_clone = Arc::clone(&job);
        let running_clone = Arc::clone(&self.running);

        tokio::spawn(async move {
            job_clone.run(running_clone).await;
        });

        self.job = Some(job);
        *running = true;

        info!(
            "Continuous aggregate refresh job started (interval: {}s)",
            self.config.refresh_check_interval_secs
        );
        Ok(())
    }

    /// Stop the background refresh job
    pub async fn stop(&mut self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("Continuous aggregate refresh job stopped");
    }

    /// Create a new continuous aggregate definition
    pub async fn create_aggregate(
        &self,
        definition: AggregateDefinition,
    ) -> Result<(), String> {
        if !self.config.enabled {
            return Err("Continuous aggregates feature is disabled".to_string());
        }
        self.storage.save_definition(&definition).await
    }

    /// Drop a continuous aggregate
    pub async fn drop_aggregate(&self, name: &str) -> Result<(), String> {
        self.storage.delete_definition(name).await
    }

    /// List all continuous aggregates
    pub async fn list_aggregates(&self) -> Result<Vec<AggregateDefinition>, String> {
        self.storage.list_definitions().await
    }

    /// Get a specific aggregate definition
    pub async fn get_aggregate(&self, name: &str) -> Result<Option<AggregateDefinition>, String> {
        self.storage.get_definition(name).await
    }

    /// Manually trigger a refresh for a specific aggregate
    pub async fn refresh_aggregate(&self, name: &str) -> Result<RefreshResult, String> {
        if let Some(job) = &self.job {
            job.refresh_single(name).await
        } else {
            Err("Refresh job not running".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_flag() {
        let config = ContinuousAggregateConfig::default();
        assert!(!config.enabled); // Disabled by default

        let config = ContinuousAggregateConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(is_enabled(&config));
    }
}
