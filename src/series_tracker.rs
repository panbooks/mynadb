use crate::line_protocol::Point;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Series tracker for monitoring metric cardinality
/// Tracks unique series by service, metric, and tag combination hash
pub struct SeriesTracker {
    data_dir: String,
    /// In-memory cache: (service, metric, hash) -> exists
    cache: Arc<RwLock<HashSet<(String, String, i64)>>>,
}

impl SeriesTracker {
    pub fn new(data_dir: &str) -> Self {
        SeriesTracker {
            data_dir: data_dir.to_string(),
            cache: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Track a point and record its series metadata
    pub async fn track_point(&self, db: &str, point: &Point) {
        // Extract service from tags
        let service = point
            .tags
            .iter()
            .find(|(k, _)| *k == "MONITOR.service")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // Calculate hash of non-MONITOR.service tags
        let hash = self.calculate_tag_hash(&point.tags);

        // Check if this series is already tracked
        let key = (service.clone(), point.measurement.clone(), hash);
        
        {
            let cache = self.cache.read().await;
            if cache.contains(&key) {
                // Already tracked
                return;
            }
        }

        // New series - add to cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(key);
        }

        // Persist to disk (append to series stats file)
        if let Err(e) = self.append_series_stat(db, &service, &point.measurement, hash).await {
            eprintln!("Failed to persist series stat: {}", e);
        }
    }

    /// Calculate hash of tags excluding MONITOR.service
    fn calculate_tag_hash(&self, tags: &HashMap<String, String>) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Filter out MONITOR.service and sort the rest
        let mut filtered_tags: Vec<(&str, &str)> = tags
            .iter()
            .filter(|(k, _)| *k != "MONITOR.service")
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        
        filtered_tags.sort();

        // Create a stable hash from sorted tags
        let mut hasher = DefaultHasher::new();
        for (key, value) in filtered_tags {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        
        hasher.finish() as i64
    }

    /// Append a new series stat to the Parquet file
    async fn append_series_stat(
        &self,
        db: &str,
        service: &str,
        metric: &str,
        hash: i64,
    ) -> Result<(), String> {
        let stats_dir = format!("{}/_system/series_stats", self.data_dir);
        std::fs::create_dir_all(&stats_dir)
            .map_err(|e| format!("Failed to create stats directory: {}", e))?;

        let file_path = format!("{}/{}.parquet", stats_dir, db);
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("hash", DataType::Int64, false),
        ]));

        // Collect existing data if file exists
        let mut services = Vec::new();
        let mut metrics = Vec::new();
        let mut hashes = Vec::new();

        if Path::new(&file_path).exists() {
            use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            let read_file = File::open(&file_path)
                .map_err(|e| format!("Failed to open existing file: {}", e))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(read_file)
                .map_err(|e| format!("Failed to create reader: {}", e))?;
            
            let mut reader = builder.build()
                .map_err(|e| format!("Failed to build reader: {}", e))?;

            while let Some(Ok(batch)) = reader.next() {
                let service_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let metric_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let hash_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    services.push(service_col.value(i).to_string());
                    metrics.push(metric_col.value(i).to_string());
                    hashes.push(hash_col.value(i));
                }
            }
        }

        // Add new record
        services.push(service.to_string());
        metrics.push(metric.to_string());
        hashes.push(hash);

        // Create arrays from all data
        let service_array = StringArray::from(services);
        let metric_array = StringArray::from(metrics);
        let hash_array = Int64Array::from(hashes);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(service_array),
                Arc::new(metric_array),
                Arc::new(hash_array),
            ],
        )
        .map_err(|e| format!("Failed to create record batch: {}", e))?;

        // Write all data (overwrites file)
        let file = File::create(&file_path)
            .map_err(|e| format!("Failed to create file: {}", e))?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        writer
            .write(&batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;
        
        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(())
    }

    /// Get series count statistics grouped by service
    pub async fn get_series_stats(&self, db: &str) -> Result<HashMap<String, SeriesStats>, String> {
        let stats_dir = format!("{}/_system/series_stats", self.data_dir);
        let file_path = format!("{}/{}.parquet", stats_dir, db);

        if !Path::new(&file_path).exists() {
            return Ok(HashMap::new());
        }

        // Read the Parquet file
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(&file_path)
            .map_err(|e| format!("Failed to open stats file: {}", e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("Failed to create reader: {}", e))?;
        
        let mut reader = builder.build()
            .map_err(|e| format!("Failed to build reader: {}", e))?;

        let mut stats: HashMap<String, HashMap<String, HashSet<i64>>> = HashMap::new();

        while let Some(Ok(batch)) = reader.next() {
            let service_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let metric_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hash_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let service = service_col.value(i).to_string();
                let metric = metric_col.value(i).to_string();
                let hash = hash_col.value(i);

                stats
                    .entry(service)
                    .or_insert_with(HashMap::new)
                    .entry(metric)
                    .or_insert_with(HashSet::new)
                    .insert(hash);
            }
        }

        // Convert to SeriesStats
        let result: HashMap<String, SeriesStats> = stats
            .into_iter()
            .map(|(service, metrics)| {
                let mut metric_stats: HashMap<String, usize> = HashMap::new();
                let mut total_series = 0;

                for (metric, hashes) in metrics {
                    let count = hashes.len();
                    metric_stats.insert(metric, count);
                    total_series += count;
                }

                (
                    service,
                    SeriesStats {
                        total_series,
                        metrics: metric_stats,
                    },
                )
            })
            .collect();

        Ok(result)
    }

    /// Load existing stats into cache on startup
    pub async fn load_cache(&self, db: &str) -> Result<(), String> {
        let stats_dir = format!("{}/_system/series_stats", self.data_dir);
        let file_path = format!("{}/{}.parquet", stats_dir, db);

        if !Path::new(&file_path).exists() {
            return Ok(());
        }

        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(&file_path)
            .map_err(|e| format!("Failed to open stats file: {}", e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("Failed to create reader: {}", e))?;
        
        let mut reader = builder.build()
            .map_err(|e| format!("Failed to build reader: {}", e))?;

        let mut cache = self.cache.write().await;

        while let Some(Ok(batch)) = reader.next() {
            let service_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let metric_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hash_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let service = service_col.value(i).to_string();
                let metric = metric_col.value(i).to_string();
                let hash = hash_col.value(i);

                cache.insert((service, metric, hash));
            }
        }

        println!("📊 Loaded {} series into cache for database '{}'", cache.len(), db);
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SeriesStats {
    pub total_series: usize,
    pub metrics: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_hash() {
        let tracker = SeriesTracker::new("./test_data");
        
        // Same tags in different order should produce same hash
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("type".to_string(), "http".to_string());
        tags1.insert("MONITOR.service".to_string(), "api".to_string());
        
        let mut tags2 = HashMap::new();
        tags2.insert("type".to_string(), "http".to_string());
        tags2.insert("MONITOR.service".to_string(), "api".to_string());
        tags2.insert("host".to_string(), "server1".to_string());
        
        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);
        
        assert_eq!(hash1, hash2, "Same tags should produce same hash");
    }

    #[test]
    fn test_tag_hash_excludes_monitor_service() {
        let tracker = SeriesTracker::new("./test_data");
        
        // Different MONITOR.service should produce same hash if other tags are same
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("MONITOR.service".to_string(), "api".to_string());
        
        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert("MONITOR.service".to_string(), "web".to_string());
        
        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);
        
        assert_eq!(hash1, hash2, "MONITOR.service should not affect hash");
    }
}
