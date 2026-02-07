//! Background Downsampling Job
//!
//! Periodically scans for partitions eligible for downsampling and
//! executes aggregation queries to produce tier tables.

use super::query_builder::build_downsample_query;
use super::storage::DownsamplingStorage;
use super::types::{DownsamplingPolicy, DownsamplingTier, TierStatus};
use arrow::array::Array;
use crate::config::DownsamplingConfig;
use crate::jobs::database_manager::DatabaseManager;
use crate::query::query_engine::QueryEngine;
use crate::storage::compression::CompressionStrategy;
use crate::storage::parquet_writer::ParquetWriter;
use crate::storage::storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

/// Background downsampling job
pub struct DownsamplingJob {
    storage: Arc<DownsamplingStorage>,
    query_engine: Arc<QueryEngine>,
    db_manager: Arc<DatabaseManager>,
    parquet_writer: ParquetWriter,
    config: DownsamplingConfig,
    semaphore: Arc<Semaphore>,
}

/// Result of processing a single tier for a single partition
#[derive(Debug)]
struct TierPartitionResult {
    rows_written: u64,
    bytes_written: u64,
}

impl DownsamplingJob {
    /// Create a new downsampling job
    pub fn new(
        storage: Arc<DownsamplingStorage>,
        query_engine: Arc<QueryEngine>,
        db_manager: Arc<DatabaseManager>,
        object_storage: Arc<Storage>,
        config: DownsamplingConfig,
    ) -> Self {
        Self::with_compression(
            storage,
            query_engine,
            db_manager,
            object_storage,
            config,
            CompressionStrategy::default(),
        )
    }

    /// Create a new downsampling job with a specific compression strategy
    pub fn with_compression(
        storage: Arc<DownsamplingStorage>,
        query_engine: Arc<QueryEngine>,
        db_manager: Arc<DatabaseManager>,
        object_storage: Arc<Storage>,
        config: DownsamplingConfig,
        compression_strategy: CompressionStrategy,
    ) -> Self {
        Self {
            storage,
            query_engine,
            db_manager,
            parquet_writer: ParquetWriter::with_compression(object_storage, compression_strategy),
            config: config.clone(),
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_jobs)),
        }
    }

    /// Start background downsampling loop
    pub fn start_background(self: Arc<Self>) {
        if !self.config.enabled {
            info!("Downsampling is disabled");
            return;
        }

        let interval_secs = self.config.check_interval_secs;
        info!(
            "Starting background downsampling (interval: {}s, max_concurrent: {})",
            interval_secs, self.config.max_concurrent_jobs
        );

        tokio::spawn(async move {
            // Initialize storage on first run
            if let Err(e) = self.storage.initialize().await {
                error!("Failed to initialize downsampling storage: {}", e);
                return;
            }

            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            // Skip the first immediate tick
            interval.tick().await;

            loop {
                interval.tick().await;
                debug!("Running downsampling check...");

                match self.run_cycle().await {
                    Ok(stats) => {
                        if stats.partitions_processed > 0 {
                            info!(
                                "Downsampling cycle: {} policies, {} partitions processed, {} rows written",
                                stats.policies_processed,
                                stats.partitions_processed,
                                stats.rows_written
                            );
                        } else {
                            debug!("Downsampling cycle: no eligible partitions found");
                        }
                    }
                    Err(e) => {
                        error!("Downsampling cycle failed: {}", e);
                    }
                }
            }
        });
    }

    /// Run a single downsampling cycle
    async fn run_cycle(&self) -> Result<CycleStats, String> {
        let policies = self.storage.get_enabled_policies().await?;
        let mut stats = CycleStats::default();

        for policy in policies {
            match self.process_policy(&policy).await {
                Ok(policy_stats) => {
                    stats.policies_processed += 1;
                    stats.partitions_processed += policy_stats.partitions_processed;
                    stats.rows_written += policy_stats.rows_written;
                }
                Err(e) => {
                    warn!("Failed to process policy '{}': {}", policy.name, e);
                }
            }
        }

        Ok(stats)
    }

    /// Process a single downsampling policy
    async fn process_policy(&self, policy: &DownsamplingPolicy) -> Result<CycleStats, String> {
        let (catalog, schema, table) = policy.parse_source_table()?;
        let mut stats = CycleStats::default();

        // Discover partitions for source table
        let partitions = self
            .db_manager
            .discover_table_partitions_async(&catalog, &schema, &table)
            .await?;

        if partitions.is_empty() {
            return Ok(stats);
        }

        let cutoff_today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        for tier in &policy.tiers {
            let cutoff_date = chrono::Utc::now()
                - chrono::Duration::days(tier.after_days as i64);
            let cutoff_str = cutoff_date.format("%Y-%m-%d").to_string();

            // Get current tier status to know what's already processed
            let tier_status = self
                .storage
                .get_tier_status(&policy.name, &tier.name)
                .await;
            let last_processed = tier_status
                .as_ref()
                .and_then(|s| s.last_processed_date.clone());

            // Find eligible partitions (older than cutoff, not yet processed)
            let eligible: Vec<_> = partitions
                .iter()
                .filter(|p| {
                    // Must be older than tier cutoff
                    p.date.as_str() < cutoff_str.as_str()
                        // Must not be in the future
                        && p.date.as_str() <= cutoff_today.as_str()
                        // Must not already be processed
                        && match &last_processed {
                            Some(lp) => p.date.as_str() > lp.as_str(),
                            None => true,
                        }
                })
                .collect();

            if eligible.is_empty() {
                continue;
            }

            debug!(
                "Policy '{}' tier '{}': {} eligible partitions",
                policy.name,
                tier.name,
                eligible.len()
            );

            // Discover tag columns from the source table schema
            let tag_columns = self
                .discover_tag_columns(&catalog, &schema, &table)
                .await
                .unwrap_or_default();

            let tier_table_short = policy.tier_table_short_name(tier)?;
            let mut tier_rows = 0u64;
            let mut tier_bytes = 0u64;
            let mut last_date = last_processed.clone();

            for partition in eligible {
                // Acquire concurrency permit
                let _permit = self.semaphore.acquire().await.map_err(|e| e.to_string())?;

                match self
                    .downsample_partition(
                        policy,
                        tier,
                        &partition.date,
                        &catalog,
                        &schema,
                        &tier_table_short,
                        &tag_columns,
                    )
                    .await
                {
                    Ok(result) => {
                        tier_rows += result.rows_written;
                        tier_bytes += result.bytes_written;
                        stats.partitions_processed += 1;
                        stats.rows_written += result.rows_written;
                        last_date = Some(partition.date.clone());

                        debug!(
                            "Downsampled {}.{}.{} date={} → {} ({} rows)",
                            catalog, schema, table, partition.date, tier_table_short, result.rows_written
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to downsample {}.{}.{} date={} for tier '{}': {}",
                            catalog, schema, table, partition.date, tier.name, e
                        );
                        // Continue with next partition
                    }
                }
            }

            // Update tier status
            if let Some(date) = last_date {
                let prev = tier_status.unwrap_or_default();
                let new_status = TierStatus {
                    tier_name: tier.name.clone(),
                    last_processed_date: Some(date),
                    total_rows_written: prev.total_rows_written + tier_rows,
                    total_bytes_written: prev.total_bytes_written + tier_bytes,
                    partitions_processed: prev.partitions_processed + stats.partitions_processed as u64,
                    last_run_at: chrono::Utc::now().timestamp_millis(),
                };
                if let Err(e) = self
                    .storage
                    .update_tier_status(&policy.name, new_status)
                    .await
                {
                    warn!("Failed to update tier status: {}", e);
                }
            }
        }

        stats.policies_processed = 1;
        Ok(stats)
    }

    /// Downsample a single partition for a tier
    async fn downsample_partition(
        &self,
        policy: &DownsamplingPolicy,
        tier: &DownsamplingTier,
        partition_date: &str,
        catalog: &str,
        schema: &str,
        tier_table_short: &str,
        tag_columns: &[String],
    ) -> Result<TierPartitionResult, String> {
        // Build aggregation SQL
        let sql = build_downsample_query(
            &policy.source_table,
            tier,
            partition_date,
            tag_columns,
        );

        debug!("Downsample SQL: {}", sql);

        // Execute aggregation query against the source database
        let batches = self
            .query_engine
            .execute_query_with_db(&sql, catalog)
            .await?;

        if batches.is_empty() {
            return Ok(TierPartitionResult {
                rows_written: 0,
                bytes_written: 0,
            });
        }

        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if total_rows == 0 {
            return Ok(TierPartitionResult {
                rows_written: 0,
                bytes_written: 0,
            });
        }

        // Convert date string to date32 (days since epoch)
        let date32 = date_str_to_date32(partition_date)?;

        // Write downsampled data to tier table
        let write_result = self
            .parquet_writer
            .write_batches_partitioned(
                "standalone",
                catalog,
                schema,
                tier_table_short,
                date32,
                batches,
            )
            .await?;

        Ok(TierPartitionResult {
            rows_written: total_rows,
            bytes_written: write_result.size_bytes,
        })
    }

    /// Discover tag columns (string-type columns) for a table
    async fn discover_tag_columns(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>, String> {
        // Query the information_schema for string columns
        let sql = format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' \
             AND data_type IN ('Utf8', 'LargeUtf8', 'Dictionary(Int32, Utf8)') \
             AND column_name != 'time' AND column_name != 'date'",
            catalog, schema, table
        );

        let batches = self
            .query_engine
            .execute_query_with_db(&sql, catalog)
            .await;

        match batches {
            Ok(batches) => {
                let mut columns = Vec::new();
                for batch in &batches {
                    if batch.num_columns() > 0 {
                        let col = batch.column(0);
                        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    columns.push(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                }
                Ok(columns)
            }
            Err(e) => {
                debug!("Failed to discover tag columns: {}", e);
                Ok(Vec::new())
            }
        }
    }

    /// Manually trigger downsampling for a specific policy
    pub async fn run_policy(&self, policy_name: &str) -> Result<CycleStats, String> {
        let policy = self
            .storage
            .get_policy(policy_name)
            .await?
            .ok_or_else(|| format!("Policy not found: {}", policy_name))?;

        self.process_policy(&policy).await
    }
}

/// Convert a date string (YYYY-MM-DD) to date32 (days since epoch)
fn date_str_to_date32(date_str: &str) -> Result<i32, String> {
    let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| format!("Invalid date '{}': {}", date_str, e))?;

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| "Failed to create epoch date".to_string())?;

    let days = (date - epoch).num_days() as i32;
    Ok(days)
}

/// Statistics from a downsampling cycle
#[derive(Debug, Default)]
pub struct CycleStats {
    pub policies_processed: usize,
    pub partitions_processed: usize,
    pub rows_written: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_str_to_date32() {
        let d = date_str_to_date32("1970-01-01").unwrap();
        assert_eq!(d, 0);

        let d = date_str_to_date32("1970-01-02").unwrap();
        assert_eq!(d, 1);

        let d = date_str_to_date32("2026-01-15").unwrap();
        assert!(d > 0);

        assert!(date_str_to_date32("not-a-date").is_err());
    }

    #[test]
    fn test_cycle_stats_default() {
        let stats = CycleStats::default();
        assert_eq!(stats.policies_processed, 0);
        assert_eq!(stats.partitions_processed, 0);
        assert_eq!(stats.rows_written, 0);
    }
}
