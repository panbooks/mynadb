//! Shared API handler logic
//!
//! This module contains shared handler functions that can be used by both
//! standalone mode (services/api.rs) and data node mode (data/server/api.rs).
//!
//! These functions take the required components as parameters directly,
//! allowing them to be called from handlers with different state types.

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::config;
use crate::jobs::compactor::{CompactionConfig, Compactor};
use crate::jobs::database_manager::DatabaseManager;
use crate::query::query_engine::QueryEngine;
use crate::storage::cache::DataCache;
use crate::storage::line_protocol::parse_multiple_lines;
use crate::storage::wal::Wal;

// ============================================================================
// Query Operations
// ============================================================================

/// Execute a SQL query and return JSON result
pub async fn execute_query_impl(
    query_engine: &QueryEngine,
    db: &str,
    sql: &str,
) -> Result<String, String> {
    query_engine
        .execute_query_json_with_db(sql, db)
        .await
        .map_err(|e| format!("Query error: {}", e))
}

/// Execute EXPLAIN query
pub async fn explain_query_impl(
    query_engine: &QueryEngine,
    db: &str,
    sql: &str,
    verbose: bool,
    analyze: bool,
) -> Result<String, String> {
    match query_engine.explain_query(sql, db, verbose, analyze).await {
        Ok(result) => {
            serde_json::to_string_pretty(&result).map_err(|e| format!("JSON error: {}", e))
        }
        Err(e) => Err(format!("Explain error: {}", e)),
    }
}

/// Execute ANALYZE query
pub async fn analyze_query_impl(
    query_engine: &QueryEngine,
    db: &str,
    sql: &str,
) -> Result<String, String> {
    match query_engine.analyze_query(sql, db).await {
        Ok(result) => {
            serde_json::to_string_pretty(&result).map_err(|e| format!("JSON error: {}", e))
        }
        Err(e) => Err(format!("Analyze error: {}", e)),
    }
}

// ============================================================================
// Write Operations
// ============================================================================

/// Process write data (line protocol)
pub async fn process_write_impl(
    cache: &DataCache,
    config: &config::Config,
    wal: Option<&Arc<Wal>>,
    db: &str,
    data: &str,
) -> Result<usize, String> {
    // WAL write (if enabled)
    if let Some(wal) = wal {
        let sync_on_write = wal.is_sync_on_write();
        if sync_on_write {
            wal.log_data_write(db, data)
                .await
                .map_err(|e| format!("WAL error: {}", e))?;
        } else {
            let wal = wal.clone();
            let db_owned = db.to_string();
            let data_owned = data.to_string();
            tokio::spawn(async move {
                if let Err(e) = wal.log_data_write(&db_owned, &data_owned).await {
                    error!("WAL write failed (async): {}", e);
                }
            });
        }
    }

    // Parse line protocol
    let points = parse_multiple_lines(data, &config.schema);
    if points.is_empty() {
        return Err("No valid points to write".to_string());
    }

    let point_count = points.len();

    // Group by schema and table
    let mut grouped: HashMap<(String, String), Vec<_>> = HashMap::with_capacity(8);
    for point in points {
        let (schema, table) = match config.schema.strategy {
            config::SchemaTableStrategy::ServiceAsSchema => {
                (point.service.clone(), point.measurement.clone())
            }
            config::SchemaTableStrategy::ServiceAsTable => {
                (config.schema.default_name.clone(), point.service.clone())
            }
        };
        grouped
            .entry((schema, table))
            .or_insert_with(|| Vec::with_capacity(point_count / 4))
            .push(point);
    }

    // Add to cache
    for ((schema, table), points) in grouped {
        cache
            .add_points(db, &schema, &table, points)
            .await
            .map_err(|e| format!("Cache full, try again later: {}", e))?;
    }

    debug!("Added {} points to cache", point_count);
    Ok(point_count)
}

// ============================================================================
// Schema/Table Operations
// ============================================================================

/// List tables in a database
pub async fn list_tables_impl(
    query_engine: &QueryEngine,
    db: &str,
) -> Result<serde_json::Value, String> {
    if let Err(e) = query_engine.ensure_database_registered(db).await {
        return Err(format!("Error registering database: {}", e));
    }

    let tables = query_engine.list_tables().await;
    let db_tables: Vec<String> = tables
        .into_iter()
        .filter(|t| t.starts_with(&format!("{}.", db)))
        .collect();

    Ok(serde_json::json!({
        "database": db,
        "tables": db_tables,
        "count": db_tables.len()
    }))
}

/// List schemas in a database
pub async fn list_schemas_impl(
    query_engine: &QueryEngine,
    db: &str,
) -> Result<serde_json::Value, String> {
    match query_engine.list_schemas(db).await {
        Ok(schemas) => Ok(serde_json::json!({
            "database": db,
            "schemas": schemas,
            "count": schemas.len()
        })),
        Err(e) => Err(format!("Error: {}", e)),
    }
}

/// List tables in a specific schema
pub async fn list_schema_tables_impl(
    query_engine: &QueryEngine,
    db: &str,
    schema: &str,
) -> Result<serde_json::Value, String> {
    match query_engine.list_tables_in_schema(db, schema).await {
        Ok(tables) => Ok(serde_json::json!({
            "database": db,
            "schema": schema,
            "tables": tables,
            "count": tables.len()
        })),
        Err(e) => Err(format!("Error: {}", e)),
    }
}

/// Get table schema
pub async fn get_table_schema_impl(
    query_engine: &QueryEngine,
    db: &str,
    table: &str,
) -> Result<String, String> {
    if let Err(e) = query_engine.ensure_database_registered(db).await {
        return Err(format!("Error: {}", e));
    }

    let full_table_name = format!("{}_{}", db, table);
    query_engine
        .get_table_schema(&full_table_name)
        .await
        .map_err(|e| format!("Error: {}", e))
}

// ============================================================================
// Cache Operations
// ============================================================================

/// Get memory/cache statistics
pub fn get_memory_stats(cache_stats: &crate::storage::cache::CacheStats) -> String {
    let format_bytes = |bytes: usize| -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
        } else if bytes >= 1024 {
            format!("{:.2} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    };

    let mut output = String::new();
    output.push_str("Memory Usage Statistics:\n");
    output.push_str("══════════════════════════════════════════\n\n");
    output.push_str("Data Cache:\n");
    output.push_str(&format!(
        "  Size: {} ({} bytes)\n",
        format_bytes(cache_stats.size_bytes),
        cache_stats.size_bytes
    ));
    output.push_str(&format!(
        "  Limit: {} ({} bytes)\n\n",
        format_bytes(cache_stats.max_size_bytes),
        cache_stats.max_size_bytes
    ));
    output.push_str("══════════════════════════════════════════\n");
    output
}

/// Flush cache and refresh tables
pub async fn flush_cache_impl(
    cache: &DataCache,
    query_engine: Option<&Arc<QueryEngine>>,
) -> Result<usize, String> {
    match cache.flush().await {
        Ok(files) => {
            let count = files.len();
            info!("Manual flush completed: {} files", count);

            // Refresh table registrations
            if let Some(qe) = query_engine {
                let mut invalidated_tables: std::collections::HashSet<(String, String, String)> =
                    std::collections::HashSet::new();
                for file in &files {
                    invalidated_tables.insert((
                        file.catalog.clone(),
                        file.schema.clone(),
                        file.table.clone(),
                    ));
                }
                for (catalog, schema, table) in &invalidated_tables {
                    qe.refresh_table_after_flush(catalog, schema, table);
                }
            }

            Ok(count)
        }
        Err(e) => Err(format!("Flush error: {}", e)),
    }
}

/// Get query cache statistics
pub fn get_query_cache_stats_impl(query_engine: &QueryEngine) -> serde_json::Value {
    let metadata_stats = query_engine.get_cache_stats();
    let query_stats = query_engine.get_query_cache_stats();

    serde_json::json!({
        "metadata_cache": {
            "table_cache": {
                "size": metadata_stats.table_cache_size,
                "hits": metadata_stats.table_hits,
                "misses": metadata_stats.table_misses,
                "hit_rate_percent": format!("{:.2}", metadata_stats.table_hit_rate())
            },
            "file_listing_cache": {
                "size": metadata_stats.file_listing_cache_size,
                "hits": metadata_stats.file_listing_hits,
                "misses": metadata_stats.file_listing_misses
            },
            "parquet_metadata_cache": {
                "size": metadata_stats.parquet_metadata_cache_size,
                "hits": metadata_stats.parquet_metadata_hits,
                "misses": metadata_stats.parquet_metadata_misses,
                "hit_rate_percent": format!("{:.2}", metadata_stats.parquet_hit_rate())
            }
        },
        "query_result_cache": {
            "enabled": query_stats.enabled,
            "entry_count": query_stats.entry_count,
            "size_bytes": query_stats.size_bytes,
            "size_mb": format!("{:.2}", query_stats.size_bytes as f64 / 1024.0 / 1024.0),
            "max_size_mb": format!("{:.2}", query_stats.max_size_bytes as f64 / 1024.0 / 1024.0),
            "usage_percent": format!("{:.2}", query_stats.usage_percent()),
            "hits": query_stats.hits,
            "misses": query_stats.misses,
            "hit_rate_percent": format!("{:.2}", query_stats.hit_rate()),
            "evictions": query_stats.evictions,
            "invalidations": query_stats.invalidations
        }
    })
}

/// Pre-warm cache for a catalog
pub async fn prewarm_cache_impl(
    query_engine: &QueryEngine,
    catalog: &str,
) -> Result<usize, String> {
    query_engine
        .prewarm_catalog_cache(catalog)
        .await
        .map_err(|e| e.to_string())
}

/// Pre-warm cache for a specific table
pub async fn prewarm_table_cache_impl(
    query_engine: &QueryEngine,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<usize, String> {
    query_engine
        .prewarm_table_cache(catalog, schema, table)
        .await
        .map_err(|e| e.to_string())
}

// ============================================================================
// Compaction Operations
// ============================================================================

/// Compact a catalog
pub async fn compact_catalog_impl(
    compactor: &Compactor,
    catalog: &str,
    config: &CompactionConfig,
) -> Result<String, String> {
    match compactor.compact_catalog(catalog, config).await {
        Ok(results) => {
            if results.is_empty() {
                Ok("No compaction needed (minimum file threshold not met)".to_string())
            } else {
                let summary = results
                    .iter()
                    .map(|r| {
                        format!(
                            "{}.{}.{}/{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
                            r.catalog, r.schema, r.table, r.date_partition,
                            r.l0_files_before, r.l0_files_after,
                            r.files_before, r.files_after,
                            r.bytes_before as f64 / 1024.0 / 1024.0,
                            r.bytes_after as f64 / 1024.0 / 1024.0,
                            r.rows_compacted
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(format!(
                    "Compacted {} partition(s) in catalog '{}'\n\n{}",
                    results.len(),
                    catalog,
                    summary
                ))
            }
        }
        Err(e) => Err(format!("Compaction error: {}", e)),
    }
}

/// Compact a table
pub async fn compact_table_impl(
    compactor: &Compactor,
    catalog: &str,
    schema: &str,
    table: &str,
    config: &CompactionConfig,
) -> Result<String, String> {
    match compactor
        .compact_table_all_partitions(catalog, schema, table, config)
        .await
    {
        Ok(results) => {
            if results.is_empty() {
                Ok(format!(
                    "No compaction needed for {}.{}.{}",
                    catalog, schema, table
                ))
            } else {
                let summary = results
                    .iter()
                    .map(|r| {
                        format!(
                            "{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
                            r.date_partition,
                            r.l0_files_before, r.l0_files_after,
                            r.files_before, r.files_after,
                            r.bytes_before as f64 / 1024.0 / 1024.0,
                            r.bytes_after as f64 / 1024.0 / 1024.0,
                            r.rows_compacted
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(format!(
                    "Compacted {} partition(s) in {}.{}.{}\n\n{}",
                    results.len(),
                    catalog,
                    schema,
                    table,
                    summary
                ))
            }
        }
        Err(e) => Err(format!("Compaction error: {}", e)),
    }
}

/// Compact a specific partition
pub async fn compact_partition_impl(
    compactor: &Compactor,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
    config: &CompactionConfig,
) -> Result<String, String> {
    let date_partition = if date.starts_with("date=") {
        date.to_string()
    } else {
        format!("date={}", date)
    };

    match compactor
        .compact_table_partition(catalog, schema, table, &date_partition, config)
        .await
    {
        Ok(Some(result)) => Ok(format!(
            "Compacted {}.{}.{}/{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
            result.catalog, result.schema, result.table, result.date_partition,
            result.l0_files_before, result.l0_files_after,
            result.files_before, result.files_after,
            result.bytes_before as f64 / 1024.0 / 1024.0,
            result.bytes_after as f64 / 1024.0 / 1024.0,
            result.rows_compacted
        )),
        Ok(None) => Ok(format!(
            "No compaction needed for {}.{}.{}/{}",
            catalog, schema, table, date_partition
        )),
        Err(e) => Err(format!("Compaction error: {}", e)),
    }
}

// ============================================================================
// Partition Operations
// ============================================================================

/// List partitions for a table
pub fn list_partitions_impl(
    db_manager: &DatabaseManager,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<serde_json::Value, String> {
    match db_manager.discover_table_partitions(catalog, schema, table) {
        Ok(partitions) => Ok(serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "partitions": partitions.iter().map(|p| {
                serde_json::json!({
                    "partition": p.partition,
                    "date": p.date,
                    "file_count": p.file_count,
                    "size_mb": p.total_size_bytes as f64 / 1024.0 / 1024.0
                })
            }).collect::<Vec<_>>(),
            "count": partitions.len(),
            "total_size_mb": partitions.iter().map(|p| p.total_size_bytes).sum::<u64>() as f64 / 1024.0 / 1024.0
        })),
        Err(e) => Err(format!("Error: {}", e)),
    }
}

/// Delete a specific partition
pub fn delete_partition_impl(
    db_manager: &DatabaseManager,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
) -> Result<String, String> {
    db_manager
        .delete_partition(catalog, schema, table, date)
        .map_err(|e| format!("Error: {}", e))
}

/// Delete old partitions based on retention policy
pub fn delete_old_partitions_impl(
    db_manager: &DatabaseManager,
    catalog: &str,
    schema: &str,
    table: &str,
    older_than_days: Option<u32>,
    before_date: Option<&str>,
) -> Result<String, String> {
    let deleted = if let Some(days) = older_than_days {
        info!("Deleting partitions older than {} days", days);
        db_manager.delete_partitions_older_than_days(catalog, schema, table, days)
    } else if let Some(date) = before_date {
        info!("Deleting partitions before {}", date);
        db_manager.delete_partitions_before(catalog, schema, table, date)
    } else {
        return Err("Must specify either 'older_than_days' or 'before_date'".to_string());
    };

    match deleted {
        Ok(messages) => {
            if messages.is_empty() {
                Ok("No partitions to delete".to_string())
            } else {
                let summary = format!(
                    "Deleted {} partition(s):\n{}",
                    messages.len(),
                    messages.join("\n")
                );
                info!("{}", summary);
                Ok(summary)
            }
        }
        Err(e) => Err(format!("Error: {}", e)),
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Generate Prometheus metrics output
pub fn generate_metrics(
    cache_stats: &crate::storage::cache::CacheStats,
    node_type: &str,
) -> String {
    let mut output = String::new();

    // Cache metrics
    output.push_str("# HELP mynadb_cache_rows_total Total number of rows in the data cache\n");
    output.push_str("# TYPE mynadb_cache_rows_total gauge\n");
    output.push_str(&format!(
        "mynadb_cache_rows_total {}\n",
        cache_stats.total_rows
    ));

    output.push_str("# HELP mynadb_cache_tables_total Number of tables in the data cache\n");
    output.push_str("# TYPE mynadb_cache_tables_total gauge\n");
    output.push_str(&format!(
        "mynadb_cache_tables_total {}\n",
        cache_stats.num_measurements
    ));

    output.push_str("# HELP mynadb_cache_size_bytes Current size of the data cache in bytes\n");
    output.push_str("# TYPE mynadb_cache_size_bytes gauge\n");
    output.push_str(&format!(
        "mynadb_cache_size_bytes {}\n",
        cache_stats.size_bytes
    ));

    output.push_str("# HELP mynadb_cache_max_size_bytes Maximum size of the data cache in bytes\n");
    output.push_str("# TYPE mynadb_cache_max_size_bytes gauge\n");
    output.push_str(&format!(
        "mynadb_cache_max_size_bytes {}\n",
        cache_stats.max_size_bytes
    ));

    output.push_str("# HELP mynadb_cache_usage_ratio Cache usage as a ratio (0.0-1.0)\n");
    output.push_str("# TYPE mynadb_cache_usage_ratio gauge\n");
    output.push_str(&format!(
        "mynadb_cache_usage_ratio {:.4}\n",
        cache_stats.usage_percent() as f64 / 100.0
    ));

    // Build info
    output.push_str("# HELP mynadb_build_info Build information\n");
    output.push_str("# TYPE mynadb_build_info gauge\n");
    output.push_str(&format!(
        "mynadb_build_info{{version=\"{}\",git_hash=\"{}\",node_type=\"{}\"}} 1\n",
        env!("GIT_VERSION"),
        env!("GIT_HASH"),
        node_type
    ));

    // Up metric
    output.push_str("# HELP mynadb_up Whether the MynaDB server is up\n");
    output.push_str("# TYPE mynadb_up gauge\n");
    output.push_str("mynadb_up 1\n");

    output
}
