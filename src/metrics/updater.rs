use super::METRICS;
use crate::config::Config;
use crate::storage::cache::DataCache;
use crate::storage::wal::Wal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error};

/// Background task that periodically updates storage and memory metrics
pub async fn start_metrics_updater(
    config: Arc<Config>,
    cache: Arc<DataCache>,
    wal: Option<Arc<Wal>>,
) {
    let mut interval = time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Update WAL size
        if let Some(wal_ref) = &wal {
            if let Ok(size) = get_wal_size(&config.storage.data_dir).await {
                METRICS.set_wal_size(size);
            }
        }

        // Update Parquet file counts and size
        let data_dir = config.storage.data_dir.clone();
        if let Ok((l0, l1, l2, l3, l4, l5, total_size)) = tokio::task::spawn_blocking(move || {
            count_parquet_files(&data_dir)
        }).await.unwrap_or_else(|_| Ok((0, 0, 0, 0, 0, 0, 0))) {
            METRICS.set_parquet_files(l0, l1, l2, l3, l4, l5, total_size);
        }

        // Update memory usage
        let cache_stats = cache.stats().await;
        let cache_bytes = cache_stats.size_bytes;
        // WAL size is tracked separately via get_wal_size()
        let wal_bytes = 0;

        let rss_bytes = get_process_memory().unwrap_or(0);

        METRICS.set_memory_usage(rss_bytes, cache_bytes as u64, wal_bytes, 0);

        // Update cache sizes for metrics
        let metadata_cache_size = 0; // TODO: Get from metadata cache if exposed
        let query_cache_size = 0; // TODO: Get from query cache if exposed
        METRICS.set_cache_sizes(metadata_cache_size, query_cache_size);

        debug!(
            "Updated metrics: wal={}, parquet_l0={}, cache={}, rss={}",
            METRICS.snapshot().storage.wal_size_bytes,
            METRICS.snapshot().storage.parquet_files_l0,
            cache_bytes,
            rss_bytes
        );
    }
}

/// Get total WAL size in bytes
async fn get_wal_size(data_dir: &str) -> Result<u64, String> {
    let wal_dir = PathBuf::from(data_dir).join("wal");

    if !wal_dir.exists() {
        return Ok(0);
    }

    let mut total_size = 0u64;

    match tokio::fs::read_dir(&wal_dir).await {
        Ok(mut entries) => {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    if metadata.is_file() {
                        total_size += metadata.len();
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to read WAL directory: {}", e);
            return Err(format!("Failed to read WAL directory: {}", e));
        }
    }

    Ok(total_size)
}

/// Count Parquet files by level and total size
fn count_parquet_files(data_dir: &str) -> Result<(u64, u64, u64, u64, u64, u64, u64), String> {
    let data_path = PathBuf::from(data_dir);

    let mut l0 = 0u64;
    let mut l1 = 0u64;
    let mut l2 = 0u64;
    let mut l3 = 0u64;
    let mut l4 = 0u64;
    let mut l5 = 0u64;
    let mut total_size = 0u64;

    // Walk the data directory recursively
    match walk_dir_for_parquet(&data_path, &mut l0, &mut l1, &mut l2, &mut l3, &mut l4, &mut l5, &mut total_size) {
        Ok(_) => Ok((l0, l1, l2, l3, l4, l5, total_size)),
        Err(e) => {
            error!("Failed to count Parquet files: {}", e);
            Err(format!("Failed to count Parquet files: {}", e))
        }
    }
}

/// Recursively walk directory and count Parquet files
fn walk_dir_for_parquet(
    path: &PathBuf,
    l0: &mut u64,
    l1: &mut u64,
    l2: &mut u64,
    l3: &mut u64,
    l4: &mut u64,
    l5: &mut u64,
    total_size: &mut u64,
) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }

    match std::fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let entry_path = entry.path();

                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_dir() {
                            // Recurse into subdirectories
                            let _ = walk_dir_for_parquet(&entry_path, l0, l1, l2, l3, l4, l5, total_size);
                        } else if metadata.is_file() {
                            if let Some(filename) = entry_path.file_name().and_then(|s| s.to_str()) {
                                if filename.ends_with(".parquet") {
                                    *total_size += metadata.len();

                                    // Count by level based on filename prefix
                                    if filename.starts_with("L0_") {
                                        *l0 += 1;
                                    } else if filename.starts_with("L1_") {
                                        *l1 += 1;
                                    } else if filename.starts_with("L2_") {
                                        *l2 += 1;
                                    } else if filename.starts_with("L3_") {
                                        *l3 += 1;
                                    } else if filename.starts_with("L4_") {
                                        *l4 += 1;
                                    } else if filename.starts_with("L5_") {
                                        *l5 += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            // Ignore permission errors and continue
            if e.kind() != std::io::ErrorKind::PermissionDenied {
                return Err(format!("Failed to read directory {:?}: {}", path, e));
            }
        }
    }

    Ok(())
}

/// Get process memory usage (RSS) in bytes
fn get_process_memory() -> Result<u64, String> {
    #[cfg(target_os = "linux")]
    {
        // Read /proc/self/status for memory info
        match std::fs::read_to_string("/proc/self/status") {
            Ok(contents) => {
                for line in contents.lines() {
                    if line.starts_with("VmRSS:") {
                        // VmRSS is in kB
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                return Ok(kb * 1024); // Convert to bytes
                            }
                        }
                    }
                }
                Err("VmRSS not found in /proc/self/status".to_string())
            }
            Err(e) => Err(format!("Failed to read /proc/self/status: {}", e)),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        // For non-Linux, return 0 or use platform-specific code
        Ok(0)
    }
}
