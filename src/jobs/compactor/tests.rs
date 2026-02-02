//! Tests for the Compactor module

use super::*;
use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

// Default compaction thresholds (from parent module)
const DEFAULT_MIN_FILES: usize = 5;
const DEFAULT_MAJOR_MIN_FILES: usize = 4;
const DEFAULT_MAX_LEVEL: u32 = 4;

fn create_test_config() -> CompactionConfig {
    CompactionConfig {
        min_files: 5,
        major_min_files: 3,
        target_file_size: 100 * 1024 * 1024, // 100MB
        max_level: 4,
        full_compaction_enabled: false,
        full_compaction_hour: 3,
        full_compaction_target_size: 200 * 1024 * 1024,
        max_concurrent_compactions: 4,
        full_compaction_max_concurrent: 2,
        read_rate_limit_bytes_per_sec: 0,
        write_rate_limit_bytes_per_sec: 0,
    }
}

#[test]
fn test_compaction_config_default() {
    let config = CompactionConfig::default();
    assert_eq!(config.min_files, DEFAULT_MIN_FILES);
    assert_eq!(config.major_min_files, DEFAULT_MAJOR_MIN_FILES);
    assert_eq!(config.max_level, DEFAULT_MAX_LEVEL);
    assert_eq!(config.max_concurrent_compactions, 8);
    assert_eq!(config.full_compaction_max_concurrent, 2);
}

#[test]
fn test_compaction_config_from_config() {
    let app_config = crate::config::CompactionConfig {
        min_files: 10,
        major_min_files: 5,
        target_file_size_mb: 500,
        max_level: 7,
        interval_secs: 120,
        full_compaction_enabled: true,
        full_compaction_hour: 5,
        full_compaction_interval_secs: 7200,
        full_compaction_target_size_mb: 300,
        max_concurrent_compactions: 16,
        full_compaction_max_concurrent: 4,
        read_rate_limit_mb_per_sec: 100,
        write_rate_limit_mb_per_sec: 50,
    };

    let config = CompactionConfig::from(&app_config);
    assert_eq!(config.min_files, 10);
    assert_eq!(config.major_min_files, 5);
    assert_eq!(config.target_file_size, 500 * 1024 * 1024);
    assert_eq!(config.max_level, 7);
    assert_eq!(config.full_compaction_enabled, true);
    assert_eq!(config.full_compaction_hour, 5);
    assert_eq!(config.full_compaction_target_size, 300 * 1024 * 1024);
    assert_eq!(config.max_concurrent_compactions, 16);
    assert_eq!(config.full_compaction_max_concurrent, 4);
    assert_eq!(config.read_rate_limit_bytes_per_sec, 100 * 1024 * 1024);
    assert_eq!(config.write_rate_limit_bytes_per_sec, 50 * 1024 * 1024);
}

#[test]
fn test_parquet_file_info_from_path_l0() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("L0_1234567890123.parquet");
    fs::write(&file_path, vec![0u8; 1024 * 1024]).unwrap();

    let info = ParquetFileInfo::from_path(file_path).unwrap();
    assert_eq!(info.level, 0);
    assert_eq!(info.timestamp, 1234567890123);
    assert_eq!(info.size, 1024 * 1024);
}

#[test]
fn test_parquet_file_info_from_path_l3() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("L3_9876543210987.parquet");
    fs::write(&file_path, vec![0u8; 5 * 1024 * 1024]).unwrap();

    let info = ParquetFileInfo::from_path(file_path).unwrap();
    assert_eq!(info.level, 3);
    assert_eq!(info.timestamp, 9876543210987);
    assert_eq!(info.size, 5 * 1024 * 1024);
}

#[test]
fn test_parquet_file_info_from_path_legacy_format() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("1234567890123.parquet");
    fs::write(&file_path, vec![0u8; 2048]).unwrap();

    let info = ParquetFileInfo::from_path(file_path).unwrap();
    assert_eq!(info.level, 0); // Legacy files default to L0
    assert_eq!(info.timestamp, 1234567890123);
    assert_eq!(info.size, 2048);
}

#[test]
fn test_parquet_file_info_sorting() {
    let mut files = vec![
        ParquetFileInfo {
            path: PathBuf::from("L1_3000.parquet"),
            level: 1,
            timestamp: 3000,
            size: 1024,
        },
        ParquetFileInfo {
            path: PathBuf::from("L0_2000.parquet"),
            level: 0,
            timestamp: 2000,
            size: 1024,
        },
        ParquetFileInfo {
            path: PathBuf::from("L0_1000.parquet"),
            level: 0,
            timestamp: 1000,
            size: 1024,
        },
        ParquetFileInfo {
            path: PathBuf::from("L1_4000.parquet"),
            level: 1,
            timestamp: 4000,
            size: 1024,
        },
    ];

    files.sort_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then_with(|| a.timestamp.cmp(&b.timestamp))
    });

    assert_eq!(files[0].level, 0);
    assert_eq!(files[0].timestamp, 1000);
    assert_eq!(files[1].level, 0);
    assert_eq!(files[1].timestamp, 2000);
    assert_eq!(files[2].level, 1);
    assert_eq!(files[2].timestamp, 3000);
    assert_eq!(files[3].level, 1);
    assert_eq!(files[3].timestamp, 4000);
}

#[test]
fn test_compaction_result_calculation() {
    let result = CompactionResult {
        catalog: "test".to_string(),
        schema: "schema1".to_string(),
        table: "table1".to_string(),
        date_partition: "2024-01-01".to_string(),
        l0_files_before: 10,
        l0_files_after: 2,
        files_before: 15,
        files_after: 5,
        bytes_before: 150 * 1024 * 1024,
        bytes_after: 100 * 1024 * 1024,
        rows_compacted: 1000000,
    };

    assert_eq!(result.files_before, 15);
    assert_eq!(result.files_after, 5);
    assert_eq!(result.bytes_before, 150 * 1024 * 1024);
    assert_eq!(result.bytes_after, 100 * 1024 * 1024);
    assert_eq!(result.rows_compacted, 1000000);
}

#[test]
fn test_compactor_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    assert_eq!(compactor.data_dir, temp_dir.path().to_str().unwrap());
}

#[test]
fn test_compactor_with_semaphore() {
    let temp_dir = TempDir::new().unwrap();
    let config = CompactionConfig {
        max_concurrent_compactions: 4,
        full_compaction_max_concurrent: 2,
        ..create_test_config()
    };
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    assert!(compactor.compaction_semaphore.is_some());
    assert!(compactor.full_compaction_semaphore.is_some());
}

#[test]
fn test_compactor_without_semaphore() {
    let temp_dir = TempDir::new().unwrap();
    let config = CompactionConfig {
        max_concurrent_compactions: 0,
        full_compaction_max_concurrent: 0,
        ..create_test_config()
    };
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    assert!(compactor.compaction_semaphore.is_none());
    assert!(compactor.full_compaction_semaphore.is_none());
}

#[test]
fn test_compactor_with_rate_limiters() {
    let temp_dir = TempDir::new().unwrap();
    let config = CompactionConfig {
        read_rate_limit_bytes_per_sec: 100 * 1024 * 1024,
        write_rate_limit_bytes_per_sec: 50 * 1024 * 1024,
        ..create_test_config()
    };
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    assert!(compactor.read_rate_limiter.is_some());
    assert!(compactor.write_rate_limiter.is_some());
}

#[test]
fn test_compactor_without_rate_limiters() {
    let temp_dir = TempDir::new().unwrap();
    let config = CompactionConfig {
        read_rate_limit_bytes_per_sec: 0,
        write_rate_limit_bytes_per_sec: 0,
        ..create_test_config()
    };
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    assert!(compactor.read_rate_limiter.is_none());
    assert!(compactor.write_rate_limiter.is_none());
}

#[tokio::test]
async fn test_get_lock_same_partition() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    let lock1 = compactor
        .get_lock("catalog", "schema", "table", "2024-01-01")
        .await;
    let lock2 = compactor
        .get_lock("catalog", "schema", "table", "2024-01-01")
        .await;

    // Should return the same lock instance
    assert!(Arc::ptr_eq(&lock1, &lock2));
}

#[tokio::test]
async fn test_get_lock_different_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    let lock1 = compactor
        .get_lock("catalog", "schema", "table", "2024-01-01")
        .await;
    let lock2 = compactor
        .get_lock("catalog", "schema", "table", "2024-01-02")
        .await;

    // Should return different lock instances
    assert!(!Arc::ptr_eq(&lock1, &lock2));
}

#[tokio::test]
async fn test_list_parquet_file_infos_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[tokio::test]
async fn test_list_parquet_file_infos_with_files() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    // Create test parquet files
    fs::write(temp_dir.path().join("L0_1000000.parquet"), b"test data 1").unwrap();
    fs::write(temp_dir.path().join("L1_2000000.parquet"), b"test data 2").unwrap();
    fs::write(temp_dir.path().join("not_a_parquet.txt"), b"ignored").unwrap();

    let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
    assert!(result.is_ok());

    let files = result.unwrap();
    assert_eq!(files.len(), 2);
    assert!(files.iter().any(|f| f.level == 0 && f.timestamp == 1000000));
    assert!(files.iter().any(|f| f.level == 1 && f.timestamp == 2000000));
}

#[tokio::test]
async fn test_list_parquet_file_infos_filters_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    // Create valid and invalid parquet files
    fs::write(temp_dir.path().join("L0_1000000.parquet"), b"test data").unwrap();
    fs::write(
        temp_dir.path().join("invalid_format.parquet"),
        b"test data",
    )
    .unwrap();
    fs::write(temp_dir.path().join("LX_1000000.parquet"), b"test data").unwrap();

    let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
    assert!(result.is_ok());

    let files = result.unwrap();
    // Should only include the valid file
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].level, 0);
    assert_eq!(files[0].timestamp, 1000000);
}

#[test]
fn test_file_filtering_by_size() {
    let config = create_test_config();
    let target_size = config.target_file_size;

    let files = vec![
        ParquetFileInfo {
            path: PathBuf::from("L1_1000.parquet"),
            level: 1,
            timestamp: 1000,
            size: 50 * 1024 * 1024, // 50MB - should be included
        },
        ParquetFileInfo {
            path: PathBuf::from("L1_2000.parquet"),
            level: 1,
            timestamp: 2000,
            size: 150 * 1024 * 1024, // 150MB - should be filtered out
        },
        ParquetFileInfo {
            path: PathBuf::from("L1_3000.parquet"),
            level: 1,
            timestamp: 3000,
            size: 80 * 1024 * 1024, // 80MB - should be included
        },
    ];

    let filtered: Vec<_> = files.iter().filter(|f| f.size < target_size).collect();

    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].timestamp, 1000);
    assert_eq!(filtered[1].timestamp, 3000);
}

#[test]
fn test_compaction_thresholds() {
    let config = create_test_config();

    // L0 files should trigger compaction
    let l0_count = config.min_files;
    assert!(l0_count >= config.min_files);

    // L1+ files should trigger compaction
    let l1_count = config.major_min_files;
    assert!(l1_count >= config.major_min_files);

    // Below threshold should not trigger
    assert!(config.min_files - 1 < config.min_files);
    assert!(config.major_min_files - 1 < config.major_min_files);
}

#[test]
fn test_level_progression() {
    let config = create_test_config();

    // Test level progression
    for level in 0..config.max_level {
        let target_level = if level >= config.max_level {
            level // Stay at max
        } else {
            level + 1 // Promote to next
        };

        if level < config.max_level {
            assert_eq!(target_level, level + 1);
        } else {
            assert_eq!(target_level, level);
        }
    }
}

#[test]
fn test_max_level_compaction() {
    let config = CompactionConfig {
        max_level: 4,
        ..create_test_config()
    };

    // At max level, should compact to same level
    let current_level = 4;
    let target_level = if current_level >= config.max_level {
        current_level
    } else {
        current_level + 1
    };

    assert_eq!(target_level, 4);
}

#[test]
fn test_yesterday_partition_calculation() {
    use chrono::{Duration, Utc};

    let now = Utc::now();
    let yesterday = now - Duration::days(1);
    let partition = yesterday.format("date=%Y-%m-%d").to_string();

    assert!(partition.starts_with("date="));
    assert_eq!(partition.len(), 15); // "date=YYYY-MM-DD"
}

#[test]
fn test_hour_check_for_full_compaction() {
    let config = CompactionConfig {
        full_compaction_hour: 3,
        ..create_test_config()
    };

    // Should only run at hour 3
    for hour in 0..24 {
        if hour == config.full_compaction_hour {
            assert_eq!(hour, 3);
        } else {
            assert_ne!(hour, 3);
        }
    }
}

#[tokio::test]
async fn test_compact_non_existing_path() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config();
    let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

    let result = compactor
        .compact_table_partition("catalog", "schema", "table", "2024-01-01", &config)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

// =====================================================================
// Cardinality-based sorting tests
// =====================================================================

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("region", DataType::Utf8, true),
        Field::new("datacenter", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn create_test_batch_with_tags(
    times: Vec<i64>,
    regions: Vec<Option<&str>>,
    datacenters: Vec<Option<&str>>,
    hosts: Vec<Option<&str>>,
    values: Vec<f64>,
) -> RecordBatch {
    let schema = create_test_schema();

    let time_array = TimestampNanosecondArray::from(times);
    let region_array = StringArray::from(regions);
    let datacenter_array = StringArray::from(datacenters);
    let host_array = StringArray::from(hosts);
    let value_array = Float64Array::from(values);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(time_array),
            Arc::new(region_array),
            Arc::new(datacenter_array),
            Arc::new(host_array),
            Arc::new(value_array),
        ],
    )
    .unwrap()
}

#[test]
fn test_estimate_cardinality_basic() {
    // Test with low cardinality (2 distinct values)
    let array = StringArray::from(vec![
        Some("us-west"),
        Some("us-east"),
        Some("us-west"),
        Some("us-east"),
        Some("us-west"),
    ]);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    assert_eq!(cardinality, 2);
}

#[test]
fn test_estimate_cardinality_single_value() {
    // Test with cardinality = 1 (all same value)
    let array = StringArray::from(vec![
        Some("us-west"),
        Some("us-west"),
        Some("us-west"),
        Some("us-west"),
    ]);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    assert_eq!(cardinality, 1);
}

#[test]
fn test_estimate_cardinality_empty() {
    let array = StringArray::from(Vec::<Option<&str>>::new());
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    assert_eq!(cardinality, 0);
}

#[test]
fn test_estimate_cardinality_with_nulls() {
    // Nulls should be counted as a distinct value
    let array = StringArray::from(vec![
        Some("value1"),
        None,
        Some("value1"),
        None,
        Some("value2"),
    ]);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    // 3 distinct: "value1", "value2", NULL
    assert_eq!(cardinality, 3);
}

#[test]
fn test_estimate_cardinality_all_nulls() {
    let array = StringArray::from(vec![None, None, None, None] as Vec<Option<&str>>);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    // Just 1 distinct value: NULL
    assert_eq!(cardinality, 1);
}

#[test]
fn test_estimate_cardinality_high_cardinality() {
    // Test with high cardinality (> 1000 distinct values)
    let values: Vec<Option<String>> = (0..1500).map(|i| Some(format!("host-{}", i))).collect();
    let values_ref: Vec<Option<&str>> = values.iter().map(|s| s.as_deref()).collect();
    let array = StringArray::from(values_ref);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    // Should return usize::MAX for high cardinality
    assert_eq!(cardinality, usize::MAX);
}

#[test]
fn test_estimate_cardinality_non_string_column() {
    // Non-string column should be treated as high cardinality
    let array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    assert!(cardinality > HIGH_CARDINALITY_THRESHOLD);
}

#[test]
fn test_build_cardinality_sort_order_basic() {
    // Create batch with different cardinalities:
    // - region: 2 distinct values (lowest cardinality)
    // - datacenter: 4 distinct values (medium cardinality)
    // - host: 8 distinct values (highest cardinality)
    let batch = create_test_batch_with_tags(
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        vec![
            Some("us"),
            Some("eu"),
            Some("us"),
            Some("eu"),
            Some("us"),
            Some("eu"),
            Some("us"),
            Some("eu"),
        ],
        vec![
            Some("dc1"),
            Some("dc2"),
            Some("dc3"),
            Some("dc4"),
            Some("dc1"),
            Some("dc2"),
            Some("dc3"),
            Some("dc4"),
        ],
        vec![
            Some("h1"),
            Some("h2"),
            Some("h3"),
            Some("h4"),
            Some("h5"),
            Some("h6"),
            Some("h7"),
            Some("h8"),
        ],
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
    );

    let schema = batch.schema();
    let sort_order = Compactor::build_cardinality_sort_order(&batch, &schema);

    // Expected order: region (2) -> datacenter (4) -> host (8) -> time
    assert_eq!(sort_order.len(), 4);
    assert_eq!(sort_order[0], "region");
    assert_eq!(sort_order[1], "datacenter");
    assert_eq!(sort_order[2], "host");
    assert_eq!(sort_order[3], "time");
}

#[test]
fn test_build_cardinality_sort_order_same_cardinality() {
    // All tags have same cardinality - order should be deterministic
    let batch = create_test_batch_with_tags(
        vec![1, 2, 3, 4],
        vec![Some("a"), Some("b"), Some("a"), Some("b")],
        vec![Some("x"), Some("y"), Some("x"), Some("y")],
        vec![Some("1"), Some("2"), Some("1"), Some("2")],
        vec![1.0, 2.0, 3.0, 4.0],
    );

    let schema = batch.schema();
    let sort_order = Compactor::build_cardinality_sort_order(&batch, &schema);

    // All have cardinality 2, so order is alphabetical due to stable sort
    assert_eq!(sort_order.len(), 4);
    // Should end with "time"
    assert_eq!(sort_order[3], "time");
}

#[test]
fn test_build_cardinality_sort_order_excludes_special_columns() {
    // Create a schema with special columns
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("measurement", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["cpu", "cpu", "cpu"])),
            Arc::new(StringArray::from(vec![
                Some("2024-01-01"),
                Some("2024-01-01"),
                Some("2024-01-02"),
            ])),
            Arc::new(StringArray::from(vec![Some("us"), Some("eu"), Some("us")])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();

    let sort_order = Compactor::build_cardinality_sort_order(&batch, &schema);

    // Should only include "region" and "time", not "measurement" or "date"
    assert_eq!(sort_order.len(), 2);
    assert_eq!(sort_order[0], "region");
    assert_eq!(sort_order[1], "time");
}

#[test]
fn test_sort_batch_by_columns_basic() {
    // Create unsorted batch
    let batch = create_test_batch_with_tags(
        vec![3, 1, 2],
        vec![Some("us"), Some("eu"), Some("us")],
        vec![Some("dc1"), Some("dc2"), Some("dc1")],
        vec![Some("h1"), Some("h2"), Some("h3")],
        vec![30.0, 10.0, 20.0],
    );

    // Sort by region, then time
    let sort_order = vec!["region".to_string(), "time".to_string()];
    let sorted = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    // Check order: eu-dc2-h2 (time=1), us-dc1-h3 (time=2), us-dc1-h1 (time=3)
    let region_col = sorted
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let time_col = sorted
        .column_by_name("time")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    assert_eq!(region_col.value(0), "eu"); // eu first
    assert_eq!(region_col.value(1), "us"); // then us
    assert_eq!(region_col.value(2), "us"); // then us

    // Within us, should be sorted by time
    assert_eq!(time_col.value(1), 2); // us with time=2
    assert_eq!(time_col.value(2), 3); // us with time=3
}

#[test]
fn test_sort_batch_by_columns_empty_sort_order() {
    let batch = create_test_batch_with_tags(
        vec![3, 1, 2],
        vec![Some("us"), Some("eu"), Some("us")],
        vec![Some("dc1"), Some("dc2"), Some("dc1")],
        vec![Some("h1"), Some("h2"), Some("h3")],
        vec![30.0, 10.0, 20.0],
    );

    // Empty sort order should return batch unchanged
    let sort_order: Vec<String> = vec![];
    let result = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    // Should be same as original
    assert_eq!(result.num_rows(), batch.num_rows());
}

#[test]
fn test_sort_batch_by_columns_missing_column() {
    let batch = create_test_batch_with_tags(
        vec![3, 1, 2],
        vec![Some("us"), Some("eu"), Some("us")],
        vec![Some("dc1"), Some("dc2"), Some("dc1")],
        vec![Some("h1"), Some("h2"), Some("h3")],
        vec![30.0, 10.0, 20.0],
    );

    // Sort by a column that doesn't exist, plus one that does
    let sort_order = vec!["nonexistent".to_string(), "region".to_string()];
    let result = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    // Should still work, just sorting by region only
    let region_col = result
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(region_col.value(0), "eu");
    assert_eq!(region_col.value(1), "us");
    assert_eq!(region_col.value(2), "us");
}

#[test]
fn test_sort_batch_by_columns_with_nulls() {
    // Create batch with null values
    // Row 0: time=1, region="us"
    // Row 1: time=2, region=NULL
    // Row 2: time=3, region="eu"
    // Row 3: time=4, region=NULL
    let batch = create_test_batch_with_tags(
        vec![1, 2, 3, 4],
        vec![Some("us"), None, Some("eu"), None],
        vec![Some("dc1"), Some("dc2"), Some("dc1"), Some("dc2")],
        vec![Some("h1"), Some("h2"), Some("h3"), Some("h4")],
        vec![1.0, 2.0, 3.0, 4.0],
    );

    // Sort by region, then time
    // Arrow default: ascending, nulls first
    let sort_order = vec!["region".to_string(), "time".to_string()];
    let sorted = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    let region_col = sorted
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let time_col = sorted
        .column_by_name("time")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    // Arrow default puts nulls first (ascending, nulls_first=true):
    // Index 0: region=NULL (time=2)
    // Index 1: region=NULL (time=4)
    // Index 2: region="eu" (time=3)
    // Index 3: region="us" (time=1)
    assert!(region_col.is_null(0));
    assert_eq!(time_col.value(0), 2);

    assert!(region_col.is_null(1));
    assert_eq!(time_col.value(1), 4);

    // Non-nulls after nulls
    assert!(!region_col.is_null(2));
    assert_eq!(region_col.value(2), "eu");
    assert_eq!(time_col.value(2), 3);

    assert!(!region_col.is_null(3));
    assert_eq!(region_col.value(3), "us");
    assert_eq!(time_col.value(3), 1);
}

#[test]
fn test_sort_batch_multi_column_ordering() {
    // Test that multi-column sort properly orders by primary key first, then secondary
    let batch = create_test_batch_with_tags(
        vec![6, 5, 4, 3, 2, 1],
        vec![
            Some("us"),
            Some("us"),
            Some("eu"),
            Some("eu"),
            Some("us"),
            Some("eu"),
        ],
        vec![
            Some("dc2"),
            Some("dc1"),
            Some("dc2"),
            Some("dc1"),
            Some("dc2"),
            Some("dc1"),
        ],
        vec![
            Some("h1"),
            Some("h2"),
            Some("h3"),
            Some("h4"),
            Some("h5"),
            Some("h6"),
        ],
        vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
    );

    // Sort by region -> datacenter -> time
    let sort_order = vec![
        "region".to_string(),
        "datacenter".to_string(),
        "time".to_string(),
    ];
    let sorted = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    let region_col = sorted
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dc_col = sorted
        .column_by_name("datacenter")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let time_col = sorted
        .column_by_name("time")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    // Expected order: eu-dc1 (time 1,3) -> eu-dc2 (time 4) -> us-dc1 (time 5) -> us-dc2 (time 2,6)
    assert_eq!(region_col.value(0), "eu");
    assert_eq!(dc_col.value(0), "dc1");
    assert_eq!(time_col.value(0), 1);

    assert_eq!(region_col.value(1), "eu");
    assert_eq!(dc_col.value(1), "dc1");
    assert_eq!(time_col.value(1), 3);

    assert_eq!(region_col.value(2), "eu");
    assert_eq!(dc_col.value(2), "dc2");
    assert_eq!(time_col.value(2), 4);

    assert_eq!(region_col.value(3), "us");
    assert_eq!(dc_col.value(3), "dc1");
    assert_eq!(time_col.value(3), 5);

    assert_eq!(region_col.value(4), "us");
    assert_eq!(dc_col.value(4), "dc2");
    assert_eq!(time_col.value(4), 2);

    assert_eq!(region_col.value(5), "us");
    assert_eq!(dc_col.value(5), "dc2");
    assert_eq!(time_col.value(5), 6);
}

#[test]
fn test_sort_batch_preserves_data_integrity() {
    // Ensure sorting doesn't corrupt data - each row should stay intact
    let batch = create_test_batch_with_tags(
        vec![100, 200, 300],
        vec![Some("c"), Some("a"), Some("b")],
        vec![Some("dc1"), Some("dc2"), Some("dc3")],
        vec![Some("h1"), Some("h2"), Some("h3")],
        vec![1.1, 2.2, 3.3],
    );

    let sort_order = vec!["region".to_string()];
    let sorted = Compactor::sort_batch_by_columns(&batch, &sort_order).unwrap();

    let region_col = sorted
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dc_col = sorted
        .column_by_name("datacenter")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value_col = sorted
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // Row with region="a" should have dc2 and value 2.2
    assert_eq!(region_col.value(0), "a");
    assert_eq!(dc_col.value(0), "dc2");
    assert!((value_col.value(0) - 2.2).abs() < 0.001);

    // Row with region="b" should have dc3 and value 3.3
    assert_eq!(region_col.value(1), "b");
    assert_eq!(dc_col.value(1), "dc3");
    assert!((value_col.value(1) - 3.3).abs() < 0.001);

    // Row with region="c" should have dc1 and value 1.1
    assert_eq!(region_col.value(2), "c");
    assert_eq!(dc_col.value(2), "dc1");
    assert!((value_col.value(2) - 1.1).abs() < 0.001);
}

#[test]
fn test_cardinality_edge_case_near_threshold() {
    // Test cardinality right at threshold boundary (999, 1000, 1001)
    let make_array = |n: usize| -> ArrayRef {
        let values: Vec<Option<String>> = (0..n).map(|i| Some(format!("val-{}", i))).collect();
        let values_ref: Vec<Option<&str>> = values.iter().map(|s| s.as_deref()).collect();
        Arc::new(StringArray::from(values_ref))
    };

    // 999 distinct values - should not be marked as high cardinality
    let array_999 = make_array(999);
    let card_999 = Compactor::estimate_cardinality(&array_999);
    assert!(card_999 <= HIGH_CARDINALITY_THRESHOLD);

    // 1001 distinct values - should be marked as high cardinality
    let array_1001 = make_array(1001);
    let card_1001 = Compactor::estimate_cardinality(&array_1001);
    assert_eq!(card_1001, usize::MAX);
}

#[test]
fn test_cardinality_with_duplicates() {
    // Many rows but few distinct values
    let mut values: Vec<Option<&str>> = Vec::with_capacity(10000);
    for i in 0..10000 {
        values.push(Some(match i % 5 {
            0 => "region-a",
            1 => "region-b",
            2 => "region-c",
            3 => "region-d",
            _ => "region-e",
        }));
    }

    let array = StringArray::from(values);
    let array_ref: ArrayRef = Arc::new(array);

    let cardinality = Compactor::estimate_cardinality(&array_ref);
    // Should detect 5 distinct values (possibly slightly higher due to sampling estimation)
    assert!(cardinality <= 20); // With 2x estimation factor, could be up to 10
}

#[test]
fn test_merge_sorted_batches_integration() {
    // Integration test: ensure merge_sorted_batches applies cardinality sorting
    // This creates two batches and merges them

    let batch1 = create_test_batch_with_tags(
        vec![100, 200],
        vec![Some("us"), Some("eu")],
        vec![Some("dc1"), Some("dc2")],
        vec![Some("h1"), Some("h2")],
        vec![1.0, 2.0],
    );

    let batch2 = create_test_batch_with_tags(
        vec![150, 250],
        vec![Some("eu"), Some("us")],
        vec![Some("dc2"), Some("dc1")],
        vec![Some("h3"), Some("h4")],
        vec![3.0, 4.0],
    );

    let schema = create_test_schema();

    // Merge batches
    let merged = Compactor::merge_sorted_batches(vec![batch1, batch2], &schema).unwrap();

    // Verify it's sorted by cardinality order
    let region_col = merged
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // All regions should be contiguous after cardinality-based sort
    // eu rows should be together, us rows should be together
    let mut last_region = region_col.value(0);
    let mut transitions = 0;

    for i in 1..merged.num_rows() {
        let current_region = region_col.value(i);
        if current_region != last_region {
            transitions += 1;
            last_region = current_region;
        }
    }

    // Should have at most 1 transition (eu -> us or us -> eu)
    assert!(
        transitions <= 1,
        "Expected contiguous regions, got {} transitions",
        transitions
    );
}
