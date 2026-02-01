//! Background Refresh Job for Continuous Aggregates
//!
//! This module implements the background job that periodically checks
//! for aggregates that need refreshing and executes the refresh logic.
//!
//! The job runs on a configurable interval and processes pending refreshes
//! with concurrency limits to avoid overwhelming the system.

use super::config::ContinuousAggregateConfig;
use super::definition::AggregateDefinition;
use super::storage::AggregateStorage;
use super::types::{AggregateState, RefreshResult, WatermarkInfo};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Background job for refreshing continuous aggregates
pub struct ContinuousAggregateJob {
    config: ContinuousAggregateConfig,
    storage: Arc<AggregateStorage>,
    /// Semaphore to limit concurrent refreshes
    semaphore: Arc<Semaphore>,
    /// Statistics
    stats: Arc<RwLock<JobStats>>,
}

/// Statistics for the refresh job
#[derive(Debug, Default, Clone)]
pub struct JobStats {
    /// Total number of refresh cycles completed
    pub cycles_completed: u64,
    /// Total number of refreshes performed
    pub refreshes_completed: u64,
    /// Total number of failed refreshes
    pub refreshes_failed: u64,
    /// Total rows processed across all refreshes
    pub total_rows_processed: u64,
    /// Total time spent refreshing (milliseconds)
    pub total_refresh_time_ms: u64,
    /// Last cycle timestamp
    pub last_cycle_at: i64,
    /// Last cycle duration (milliseconds)
    pub last_cycle_duration_ms: u64,
}

impl ContinuousAggregateJob {
    /// Create a new refresh job
    pub fn new(config: ContinuousAggregateConfig, storage: Arc<AggregateStorage>) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_refreshes));

        Self {
            config,
            storage,
            semaphore,
            stats: Arc::new(RwLock::new(JobStats::default())),
        }
    }

    /// Run the refresh job loop
    ///
    /// This method runs until `running` becomes false.
    pub async fn run(&self, running: Arc<RwLock<bool>>) {
        info!(
            "Starting continuous aggregate refresh job (interval: {}s, max_concurrent: {})",
            self.config.refresh_check_interval_secs, self.config.max_concurrent_refreshes
        );

        // Initialize storage
        if let Err(e) = self.storage.initialize().await {
            error!("Failed to initialize aggregate storage: {}", e);
            return;
        }

        let interval = Duration::from_secs(self.config.refresh_check_interval_secs);

        loop {
            // Check if we should stop
            {
                let r = running.read().await;
                if !*r {
                    info!("Continuous aggregate refresh job stopping");
                    break;
                }
            }

            // Run one refresh cycle
            let cycle_start = Instant::now();
            if let Err(e) = self.run_cycle().await {
                error!("Refresh cycle failed: {}", e);
            }
            let cycle_duration = cycle_start.elapsed();

            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.cycles_completed += 1;
                stats.last_cycle_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                stats.last_cycle_duration_ms = cycle_duration.as_millis() as u64;
            }

            // Sleep until next cycle
            // Subtract cycle duration to maintain consistent intervals
            let sleep_duration = interval.saturating_sub(cycle_duration);
            if sleep_duration > Duration::ZERO {
                tokio::time::sleep(sleep_duration).await;
            }
        }
    }

    /// Run one refresh cycle
    async fn run_cycle(&self) -> Result<(), String> {
        // Get pending refreshes
        let pending = self.storage.get_pending_refreshes().await?;

        if pending.is_empty() {
            debug!("No aggregates pending refresh");
            return Ok(());
        }

        info!("Found {} aggregates pending refresh", pending.len());

        // Process each pending aggregate
        let mut handles = Vec::new();

        for def in pending {
            let storage = Arc::clone(&self.storage);
            let semaphore = Arc::clone(&self.semaphore);
            let stats = Arc::clone(&self.stats);
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.unwrap();

                // Perform refresh
                let result = Self::refresh_aggregate(&def, &storage, &config).await;

                // Update stats
                {
                    let mut s = stats.write().await;
                    match &result {
                        Ok(r) if r.success => {
                            s.refreshes_completed += 1;
                            s.total_rows_processed += r.rows_processed;
                            s.total_refresh_time_ms += r.duration_ms;
                        }
                        _ => {
                            s.refreshes_failed += 1;
                        }
                    }
                }

                result
            });

            handles.push(handle);
        }

        // Wait for all refreshes to complete
        for handle in handles {
            match handle.await {
                Ok(Ok(result)) => {
                    if result.success {
                        debug!(
                            "Refreshed {}: {} rows in {}ms",
                            result.aggregate_name, result.rows_written, result.duration_ms
                        );
                    } else if let Some(error) = &result.error {
                        if !error.starts_with("Skipped") {
                            warn!("Refresh {} failed: {}", result.aggregate_name, error);
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!("Refresh error: {}", e);
                }
                Err(e) => {
                    error!("Task join error: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Refresh a single aggregate
    async fn refresh_aggregate(
        def: &AggregateDefinition,
        storage: &AggregateStorage,
        _config: &ContinuousAggregateConfig,
    ) -> Result<RefreshResult, String> {
        let start = Instant::now();
        let name = &def.name;

        info!("Starting refresh for aggregate: {}", name);

        // Calculate time range for refresh
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        // Start from last watermark (or beginning of time if first refresh)
        let from_watermark = if def.watermark.high_watermark_ns > 0 {
            def.watermark.high_watermark_ns
        } else {
            // For first refresh, start from 7 days ago by default
            now_ns - (7 * 24 * 3600 * 1_000_000_000_i64)
        };

        // End at now minus start_offset (to allow for late data)
        let to_watermark = now_ns - (def.refresh_policy.start_offset_secs as i64 * 1_000_000_000);

        // Check if there's any new data to process
        if from_watermark >= to_watermark {
            return Ok(RefreshResult::skipped(name, "No new data to process"));
        }

        // Generate the incremental refresh SQL
        let refresh_sql = def.to_incremental_refresh_sql(from_watermark, to_watermark);
        debug!("Refresh SQL for {}: {}", name, refresh_sql);

        // TODO: Need to integrate with the main pipeline
        // Execute the SQL and write results to target table:
        // 1. Execute refresh_sql against the source table using QueryEngine
        // 2. Write results to def.target_table using ParquetWriter
        // 3. Update watermark
        //
        // For now, we'll simulate the refresh with placeholder logic
        let rows_processed = 0u64;
        let rows_written = 0u64;

        // Update watermark
        let mut watermark = def.watermark.clone();
        watermark.update(to_watermark, rows_written, 0);

        // Save watermark
        storage.save_watermark(&watermark).await?;

        // Update state to Active if it was Creating
        if def.state == AggregateState::Creating {
            storage.update_state(name, AggregateState::Active).await?;
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(RefreshResult::success(
            name,
            (from_watermark, to_watermark),
            rows_processed,
            rows_written,
            duration_ms,
            to_watermark,
            vec![], // partitions processed
        ))
    }

    /// Manually trigger a refresh for a specific aggregate
    pub async fn refresh_single(&self, name: &str) -> Result<RefreshResult, String> {
        let def = self
            .storage
            .get_definition(name)
            .await?
            .ok_or_else(|| format!("Aggregate not found: {}", name))?;

        Self::refresh_aggregate(&def, &self.storage, &self.config).await
    }

    /// Get job statistics
    pub async fn stats(&self) -> JobStats {
        self.stats.read().await.clone()
    }
}

/// Executor trait for running aggregate refresh queries
///
/// This trait allows the refresh job to be decoupled from the query engine,
/// making it easier to test and extend.
#[async_trait::async_trait]
pub trait RefreshExecutor: Send + Sync {
    /// Execute a refresh query and return the number of rows processed
    async fn execute_refresh(
        &self,
        source_sql: &str,
        target_table: &str,
        from_watermark: i64,
        to_watermark: i64,
    ) -> Result<(u64, u64), String>; // (rows_processed, rows_written)
}

/// Default executor that uses DataFusion
#[derive(Default)]
pub struct DataFusionRefreshExecutor {
    // Will hold QueryEngine reference when integrated
}

#[async_trait::async_trait]
impl RefreshExecutor for DataFusionRefreshExecutor {
    async fn execute_refresh(
        &self,
        source_sql: &str,
        target_table: &str,
        _from_watermark: i64,
        _to_watermark: i64,
    ) -> Result<(u64, u64), String> {
        // TODO: Implement actual execution
        // 1. Execute source_sql using QueryEngine
        // 2. Convert results to RecordBatch
        // 3. Write to target_table using ParquetWriter
        debug!(
            "Would execute refresh: {} -> {}",
            source_sql, target_table
        );
        Ok((0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::definition::AggregateFunction;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_job_creation() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(AggregateStorage::with_dir(dir.path().to_path_buf()));
        let config = ContinuousAggregateConfig::enabled();

        let job = ContinuousAggregateJob::new(config, storage);
        let stats = job.stats().await;
        assert_eq!(stats.cycles_completed, 0);
    }

    #[tokio::test]
    async fn test_refresh_single() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(AggregateStorage::with_dir(dir.path().to_path_buf()));
        let config = ContinuousAggregateConfig::enabled();

        // Create a definition
        let def = AggregateDefinition::builder("test_agg", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .avg("value", "avg_value")
            .build()
            .unwrap();
        storage.save_definition(&def).await.unwrap();

        let job = ContinuousAggregateJob::new(config, storage);
        let result = job.refresh_single("test_agg").await.unwrap();
        assert!(result.success);
    }
}
