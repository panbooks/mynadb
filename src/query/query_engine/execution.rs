//! Query Execution for QueryEngine
//!
//! Handles SQL query execution, EXPLAIN, and ANALYZE operations.

use crate::query::cache::query_cache::QueryCacheKey;
use crate::query::sql_utils::{
    extract_optimization_info, extract_table_references, extract_time_range,
    inject_date_predicates, qualify_table_names, summarize_plan,
};
use crate::query::timeseries::{FillOption, FillProcessor, GroupByTimeTransformer};
use crate::query::types::{QueryAnalysis, QueryExplainResult};
use datafusion::arrow::array::{Array, RecordBatch};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::QueryEngine;

/// Query scope - determines what data to return
///
/// Used for standby failover:
/// - Memtable data: Query the backing Standby (has replicated memtable)
/// - Parquet data: Query any fallback Primary (S3 is shared storage)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryScope {
    /// Query both memtable and Parquet (normal operation)
    #[default]
    Full,
    /// Query only memtable (for Standby nodes on Primary failover)
    MemtableOnly,
    /// Query only Parquet/S3 (for fallback Primary on failover)
    ParquetOnly,
}

impl QueryEngine {
    // ========================================================================
    // Query Execution
    // ========================================================================

    /// Execute a SQL query with database context
    /// Handles table registration, qualification, GROUP BY time() transformation, and caching
    pub async fn execute_query_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        self.execute_query_with_db_and_scope(sql, db_name, QueryScope::Full)
            .await
    }

    /// Execute a SQL query with database context and query scope
    ///
    /// Query scope controls what data is returned:
    /// - `Full`: Both memtable and Parquet (normal operation)
    /// - `MemtableOnly`: Only memtable data (for Standby on Primary failover)
    /// - `ParquetOnly`: Only Parquet/S3 data (for fallback Primary on failover)
    pub async fn execute_query_with_db_and_scope(
        &self,
        sql: &str,
        db_name: &str,
        scope: QueryScope,
    ) -> Result<Vec<RecordBatch>, String> {
        // Skip cache for scoped queries (failover scenario, want fresh data)
        if scope == QueryScope::Full {
            let cache_key = QueryCacheKey::new(db_name, sql);
            if let Some(cached_result) = self.query_cache.get(&cache_key) {
                debug!("Query cache HIT for db={}", db_name);
                return Ok(cached_result);
            }
        }

        let table_refs = extract_table_references(sql, db_name);

        // Register tables based on query scope
        for (catalog, schema, table) in &table_refs {
            match scope {
                QueryScope::Full => {
                    // Normal: register with both memtable and parquet
                    let _ = self.register_table(catalog, schema, table).await;
                }
                QueryScope::MemtableOnly => {
                    // Standby failover: register only memtable
                    let _ = self.register_memtable_only(catalog, schema, table).await;
                }
                QueryScope::ParquetOnly => {
                    // Fallback Primary failover: register only parquet (skip memtable wrap)
                    let _ = self.register_parquet_only(catalog, schema, table).await;
                }
            }
        }

        let qualified_sql = qualify_table_names(sql, db_name);
        let optimized_sql = inject_date_predicates(&qualified_sql);

        let transformer = GroupByTimeTransformer::new();
        let transformed = transformer
            .transform(&optimized_sql)
            .map_err(|e| format!("GROUP BY time() transformation error: {}", e))?;

        let final_sql = &transformed.sql;

        debug!("SQL: {}", sql);
        if qualified_sql != sql {
            debug!("Qualified: {}", qualified_sql);
        }
        if optimized_sql != qualified_sql {
            debug!("Optimized (date predicates injected): {}", optimized_sql);
        }
        if transformed.was_transformed {
            debug!("GROUP BY time() transformed: {}", final_sql);
        }

        let start_time = Instant::now();
        let mut result = self.execute_query(final_sql).await?;
        let query_time_ms = start_time.elapsed().as_millis() as u64;

        if transformed.was_transformed && !matches!(transformed.fill_option, FillOption::None) {
            if let Some(interval_nanos) = transformed.interval_nanos {
                let time_col_idx = if !result.is_empty() {
                    result[0]
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == "time")
                        .unwrap_or(0)
                } else {
                    0
                };

                let group_col_indices: Vec<usize> = if !result.is_empty() {
                    transformed
                        .group_columns
                        .iter()
                        .filter_map(|col_name| {
                            result[0]
                                .schema()
                                .fields()
                                .iter()
                                .position(|f| f.name() == col_name)
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                let processor = FillProcessor::new(
                    interval_nanos,
                    transformed.fill_option.clone(),
                    time_col_idx,
                    group_col_indices,
                );
                result = processor
                    .apply(result)
                    .map_err(|e| format!("Fill processing error: {}", e))?;
            }
        }

        // Only cache full-scope queries (not failover queries)
        if scope == QueryScope::Full {
            let referenced_tables: Vec<String> = table_refs
                .iter()
                .map(|(c, s, t)| format!("{}.{}.{}", c, s, t))
                .collect();

            let cache_key = QueryCacheKey::new(db_name, sql);
            self.query_cache
                .put(cache_key, result.clone(), referenced_tables, query_time_ms);
        }

        Ok(result)
    }

    /// Execute a raw SQL query without database context
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Execution error: {}", e))?;
        Ok(batches)
    }

    /// Get the schema for a query without executing it
    pub async fn get_query_schema(
        &self,
        db_name: &str,
        sql: &str,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
        self.ensure_database_registered(db_name).await?;
        let qualified_sql = qualify_table_names(sql, db_name);
        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        Ok(df.schema().inner().clone())
    }

    // ========================================================================
    // EXPLAIN & ANALYZE
    // ========================================================================

    /// Explain a query's execution plan
    pub async fn explain_query(
        &self,
        sql: &str,
        db_name: &str,
        verbose: bool,
        analyze: bool,
    ) -> Result<QueryExplainResult, String> {
        // Register tables referenced in the query
        let table_refs = extract_table_references(sql, db_name);
        for (catalog, schema, table) in table_refs {
            let _ = self.register_table(&catalog, &schema, &table).await;
        }

        // Qualify table names and create DataFrame
        let qualified_sql = qualify_table_names(sql, db_name);
        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let logical_plan = format!("{:?}", df.logical_plan());

        // Get explain output
        let explain_df = if analyze {
            df.explain(verbose, true)
                .map_err(|e| format!("Explain error: {}", e))?
        } else {
            df.explain(verbose, false)
                .map_err(|e| format!("Explain error: {}", e))?
        };

        let batches = explain_df
            .collect()
            .await
            .map_err(|e| format!("Collect error: {}", e))?;

        let mut plan_lines = Vec::new();
        for batch in &batches {
            let plan_type_col = batch.column(0);
            let plan_col = batch.column(1);

            let plan_types = plan_type_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or("Failed to get plan_type column")?;
            let plans = plan_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or("Failed to get plan column")?;

            for i in 0..batch.num_rows() {
                let plan_type = if plans.is_valid(i) { plan_types.value(i) } else { "" };
                let plan = if plans.is_valid(i) { plans.value(i) } else { "" };
                plan_lines.push(format!("{}:\n{}", plan_type, plan));
            }
        }

        let physical_plan = plan_lines.join("\n\n");
        let optimizations = extract_optimization_info(&physical_plan);

        Ok(QueryExplainResult {
            sql: sql.to_string(),
            qualified_sql,
            logical_plan,
            physical_plan,
            optimizations,
        })
    }

    /// Analyze a query and return statistics about tables, partitions, and files
    pub async fn analyze_query(&self, sql: &str, db_name: &str) -> Result<QueryAnalysis, String> {
        let explain = self.explain_query(sql, db_name, true, false).await?;
        let time_range = extract_time_range(sql);
        let table_refs = extract_table_references(sql, db_name);

        let mut partition_count = 0;
        let mut total_files = 0;

        for (catalog, schema, table) in &table_refs {
            let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
            if let Ok(partitions) = self.discover_table_partitions(&table_path) {
                if let Some(ref tr) = time_range {
                    for partition in &partitions {
                        if let Some(date) = partition.strip_prefix("date=") {
                            let include = match (&tr.start_date, &tr.end_date) {
                                (Some(start), Some(end)) => {
                                    date >= start.as_str() && date <= end.as_str()
                                }
                                (Some(start), None) => date >= start.as_str(),
                                (None, Some(end)) => date <= end.as_str(),
                                (None, None) => true,
                            };
                            if include {
                                partition_count += 1;
                                let partition_path = format!("{}/{}", table_path, partition);
                                total_files += self.count_parquet_files_cached(&partition_path);
                            }
                        }
                    }
                } else {
                    partition_count += partitions.len();
                    for partition in &partitions {
                        let partition_path = format!("{}/{}", table_path, partition);
                        total_files += self.count_parquet_files_cached(&partition_path);
                    }
                }
            }
        }

        Ok(QueryAnalysis {
            sql: sql.to_string(),
            tables: table_refs
                .iter()
                .map(|(c, s, t)| format!("{}.{}.{}", c, s, t))
                .collect(),
            time_range,
            partitions_to_scan: partition_count,
            files_to_scan: total_files,
            optimizations: explain.optimizations,
            plan_summary: summarize_plan(&explain.physical_plan),
        })
    }

    // ========================================================================
    // JSON Output
    // ========================================================================

    /// Execute query and return as JSON (with DB context)
    pub async fn execute_query_json_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<String, String> {
        let batches = self.execute_query_with_db(sql, db_name).await?;

        if batches.is_empty() {
            return Ok("[]".to_string());
        }

        // Convert to JSON using shared utility
        let results = crate::utils::json::batches_to_json(&batches)?;

        serde_json::to_string_pretty(&results)
            .map_err(|e| format!("JSON serialization error: {}", e))
    }
}
