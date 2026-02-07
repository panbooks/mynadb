//! Adaptive Compression Strategy
//!
//! Provides per-column compression selection based on data type, column
//! characteristics, and partition age. Supports fixed (single codec for all)
//! and adaptive (per-column heuristic) strategies.

use crate::config::CompressionConfig;
use arrow::datatypes::{DataType, Schema};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tracing::debug;

/// Compression strategy that can be fixed or adaptive
#[derive(Debug, Clone)]
pub enum CompressionStrategy {
    /// Use a single compression codec for all columns
    Fixed(Compression),
    /// Select compression per-column based on data type and age
    Adaptive(AdaptiveConfig),
}

/// Configuration for adaptive compression
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Default compression for unmatched columns
    pub default: Compression,
    /// Compression for hot (recent) data
    pub hot_compression: Compression,
    /// Compression for cold (old) data
    pub cold_compression: Compression,
    /// Compression for numeric columns (Int*, Float*, UInt*)
    pub numeric_compression: Compression,
    /// Compression for string columns
    pub string_compression: Compression,
    /// Days threshold: data newer than this is "hot"
    pub hot_days: u32,
    /// Days threshold: data older than this is "cold"
    pub cold_days: u32,
}

impl CompressionStrategy {
    /// Create a strategy from the config
    pub fn from_config(config: &CompressionConfig) -> Self {
        if config.strategy == "adaptive" {
            let default = parse_compression(&config.default_codec, config.zstd_level);
            let hot = parse_compression(&config.hot_codec, config.zstd_level);
            let cold = parse_compression(&config.cold_codec, config.cold_zstd_level);
            let numeric = parse_compression(&config.numeric_codec, config.zstd_level);
            let string = parse_compression(&config.string_codec, config.zstd_level);

            CompressionStrategy::Adaptive(AdaptiveConfig {
                default,
                hot_compression: hot,
                cold_compression: cold,
                numeric_compression: numeric,
                string_compression: string,
                hot_days: config.hot_days,
                cold_days: config.cold_days,
            })
        } else {
            let codec = parse_compression(&config.default_codec, config.zstd_level);
            CompressionStrategy::Fixed(codec)
        }
    }

    /// Select compression for a specific column and partition age
    pub fn select_for_column(
        &self,
        data_type: &DataType,
        _column_name: &str,
        partition_age_days: Option<u32>,
    ) -> Compression {
        match self {
            CompressionStrategy::Fixed(c) => *c,
            CompressionStrategy::Adaptive(config) => {
                // 1. Age-based override (highest priority)
                if let Some(age) = partition_age_days {
                    if age <= config.hot_days {
                        return config.hot_compression;
                    }
                    if age >= config.cold_days {
                        return config.cold_compression;
                    }
                }

                // 2. Type-based selection
                match data_type {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64 => config.numeric_compression,

                    DataType::Utf8 | DataType::LargeUtf8 => config.string_compression,

                    DataType::Boolean => Compression::UNCOMPRESSED,

                    // Timestamps get numeric treatment
                    DataType::Timestamp(_, _)
                    | DataType::Date32
                    | DataType::Date64 => config.numeric_compression,

                    // Dictionary-encoded strings
                    DataType::Dictionary(_, _) => config.string_compression,

                    // Default
                    _ => config.default,
                }
            }
        }
    }

    /// Build WriterProperties with per-column compression applied
    pub fn build_writer_properties(
        &self,
        schema: &Schema,
        partition_age_days: Option<u32>,
        row_group_size: usize,
    ) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(row_group_size)
            .set_data_page_size_limit(1024 * 1024) // 1MB
            .set_dictionary_page_size_limit(1024 * 1024) // 1MB
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_write_batch_size(10000);

        // Set global default compression
        let global = match self {
            CompressionStrategy::Fixed(c) => *c,
            CompressionStrategy::Adaptive(config) => config.default,
        };
        builder = builder.set_compression(global);

        // Apply per-column compression and bloom filters
        for field in schema.fields() {
            let col_path: ColumnPath = field.name().clone().into();
            let col_compression =
                self.select_for_column(field.data_type(), field.name(), partition_age_days);

            // Only set per-column if it differs from global
            if col_compression != global {
                builder = builder.set_column_compression(col_path.clone(), col_compression);
            }

            // Bloom filters for string columns
            if field.data_type() == &DataType::Utf8 {
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), 0.01)
                    .set_column_bloom_filter_ndv(col_path, 10_000);
            }
        }

        if matches!(self, CompressionStrategy::Adaptive(_)) {
            debug!(
                "Adaptive compression: {} columns, partition_age={:?} days",
                schema.fields().len(),
                partition_age_days
            );
        }

        builder.build()
    }

    /// Check if this is the adaptive strategy
    pub fn is_adaptive(&self) -> bool {
        matches!(self, CompressionStrategy::Adaptive(_))
    }
}

impl Default for CompressionStrategy {
    fn default() -> Self {
        CompressionStrategy::Fixed(Compression::ZSTD(
            ZstdLevel::try_new(3).unwrap_or_default(),
        ))
    }
}

/// Parse a compression codec string into a Compression enum
pub fn parse_compression(codec: &str, zstd_level: i32) -> Compression {
    match codec.to_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(zstd_level).unwrap_or_default()),
        "snappy" => Compression::SNAPPY,
        "lz4" => Compression::LZ4,
        "gzip" => Compression::GZIP(Default::default()),
        "brotli" => Compression::BROTLI(Default::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        _ => Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
    }
}

/// Get the name of a compression codec for display
pub fn compression_name(codec: &Compression) -> &'static str {
    match codec {
        Compression::UNCOMPRESSED => "uncompressed",
        Compression::SNAPPY => "snappy",
        Compression::GZIP(_) => "gzip",
        Compression::LZ4 => "lz4",
        Compression::BROTLI(_) => "brotli",
        Compression::ZSTD(_) => "zstd",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_fixed_strategy() {
        let strategy = CompressionStrategy::Fixed(Compression::SNAPPY);
        let result = strategy.select_for_column(&DataType::Int64, "value", None);
        assert_eq!(result, Compression::SNAPPY);

        let result = strategy.select_for_column(&DataType::Utf8, "host", Some(100));
        assert_eq!(result, Compression::SNAPPY);
    }

    #[test]
    fn test_adaptive_type_based() {
        let config = AdaptiveConfig {
            default: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_compression: Compression::SNAPPY,
            cold_compression: Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            numeric_compression: Compression::LZ4,
            string_compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_days: 7,
            cold_days: 30,
        };
        let strategy = CompressionStrategy::Adaptive(config);

        // Numeric columns get LZ4 (no age info)
        let result = strategy.select_for_column(&DataType::Int64, "value", None);
        assert_eq!(result, Compression::LZ4);

        let result = strategy.select_for_column(&DataType::Float64, "usage", None);
        assert_eq!(result, Compression::LZ4);

        // String columns get ZSTD
        let result = strategy.select_for_column(&DataType::Utf8, "host", None);
        assert!(matches!(result, Compression::ZSTD(_)));

        // Boolean columns are uncompressed
        let result = strategy.select_for_column(&DataType::Boolean, "active", None);
        assert_eq!(result, Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_adaptive_age_based() {
        let config = AdaptiveConfig {
            default: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_compression: Compression::SNAPPY,
            cold_compression: Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            numeric_compression: Compression::LZ4,
            string_compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_days: 7,
            cold_days: 30,
        };
        let strategy = CompressionStrategy::Adaptive(config);

        // Hot data (< 7 days) → Snappy
        let result = strategy.select_for_column(&DataType::Int64, "value", Some(3));
        assert_eq!(result, Compression::SNAPPY);

        // Warm data (7-30 days) → type-based (Int64 → LZ4)
        let result = strategy.select_for_column(&DataType::Int64, "value", Some(15));
        assert_eq!(result, Compression::LZ4);

        // Cold data (> 30 days) → ZSTD high
        let result = strategy.select_for_column(&DataType::Int64, "value", Some(60));
        assert!(matches!(result, Compression::ZSTD(_)));
    }

    #[test]
    fn test_from_config_fixed() {
        let config = CompressionConfig {
            strategy: "fixed".to_string(),
            default_codec: "snappy".to_string(),
            zstd_level: 3,
            ..Default::default()
        };
        let strategy = CompressionStrategy::from_config(&config);
        assert!(!strategy.is_adaptive());

        let result = strategy.select_for_column(&DataType::Utf8, "host", None);
        assert_eq!(result, Compression::SNAPPY);
    }

    #[test]
    fn test_from_config_adaptive() {
        let config = CompressionConfig {
            strategy: "adaptive".to_string(),
            ..Default::default()
        };
        let strategy = CompressionStrategy::from_config(&config);
        assert!(strategy.is_adaptive());
    }

    #[test]
    fn test_build_writer_properties() {
        let config = AdaptiveConfig {
            default: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_compression: Compression::SNAPPY,
            cold_compression: Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            numeric_compression: Compression::LZ4,
            string_compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_days: 7,
            cold_days: 30,
        };
        let strategy = CompressionStrategy::Adaptive(config);

        let schema = Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
        ]);

        // Should not panic
        let _props = strategy.build_writer_properties(&schema, Some(3), 500_000);
        let _props = strategy.build_writer_properties(&schema, Some(60), 500_000);
        let _props = strategy.build_writer_properties(&schema, None, 1_000_000);
    }

    #[test]
    fn test_parse_compression() {
        assert_eq!(parse_compression("snappy", 3), Compression::SNAPPY);
        assert_eq!(parse_compression("lz4", 3), Compression::LZ4);
        assert_eq!(
            parse_compression("uncompressed", 3),
            Compression::UNCOMPRESSED
        );
        assert!(matches!(parse_compression("zstd", 9), Compression::ZSTD(_)));
        // Unknown falls back to ZSTD
        assert!(matches!(
            parse_compression("invalid", 3),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn test_compression_name() {
        assert_eq!(compression_name(&Compression::SNAPPY), "snappy");
        assert_eq!(compression_name(&Compression::LZ4), "lz4");
        assert_eq!(
            compression_name(&Compression::ZSTD(ZstdLevel::default())),
            "zstd"
        );
    }

    #[test]
    fn test_default_strategy() {
        let strategy = CompressionStrategy::default();
        assert!(!strategy.is_adaptive());
        // Default is ZSTD L3
        let result = strategy.select_for_column(&DataType::Utf8, "x", None);
        assert!(matches!(result, Compression::ZSTD(_)));
    }
}
