//! Incremental Snapshot Builder
//!
//! Builds Iceberg snapshots from MynaDB Parquet files by scanning
//! the data directory and creating manifests for each table.

use crate::iceberg::metadata::{
    manifest::ManifestFile,
    manifest_list::ManifestList,
    partition_spec, schema,
    snapshot::{self, Snapshot},
    table_metadata::TableMetadata,
};
use crate::storage::layout::DataLayout;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Scans a MynaDB table directory and builds an Iceberg snapshot
pub struct SnapshotBuilder {
    layout: DataLayout,
}

impl SnapshotBuilder {
    pub fn new(data_dir: &str) -> Self {
        Self {
            layout: DataLayout::new(data_dir),
        }
    }

    /// Build or update Iceberg metadata for a single table
    pub fn build_table_snapshot(
        &self,
        catalog: &str,
        schema_name: &str,
        table: &str,
    ) -> Result<TableMetadata, String> {
        let table_dir = self.layout.table_dir(catalog, schema_name, table);
        if !table_dir.exists() {
            return Err(format!("Table directory not found: {}", table_dir.display()));
        }

        let metadata_dir = self.layout.iceberg_table_metadata_dir(catalog, schema_name, table);
        std::fs::create_dir_all(&metadata_dir)
            .map_err(|e| format!("Failed to create metadata dir: {}", e))?;

        // Try to load existing metadata
        let metadata_file = metadata_dir.join("v1.metadata.json");
        let mut metadata = if metadata_file.exists() {
            TableMetadata::load_json(&metadata_file)?
        } else {
            self.create_initial_metadata(catalog, schema_name, table, &table_dir)?
        };

        // Scan for Parquet files
        let parquet_files = self.scan_parquet_files(&table_dir)?;
        if parquet_files.is_empty() {
            debug!("No Parquet files found for {}.{}.{}", catalog, schema_name, table);
            return Ok(metadata);
        }

        // Build manifest
        let snapshot_id = snapshot::generate_snapshot_id();
        let manifest_filename = format!("manifest-{}.json", snapshot_id);
        let manifest_path = metadata_dir.join(&manifest_filename);

        let mut manifest = ManifestFile::new(manifest_filename.clone(), snapshot_id);

        for (partition_date, files) in &parquet_files {
            for (file_path, file_size) in files {
                let record_count = estimate_record_count(*file_size);
                manifest.add_data_file(
                    file_path.clone(),
                    partition_date,
                    record_count,
                    *file_size as i64,
                );
            }
        }

        manifest.write_json(&manifest_path)?;

        // Build manifest list
        let manifest_list_filename = format!("snap-{}-manifest-list.json", snapshot_id);
        let manifest_list_path = metadata_dir.join(&manifest_list_filename);

        let mut manifest_list = ManifestList::new();
        let manifest_json_size = std::fs::metadata(&manifest_path)
            .map(|m| m.len() as i64)
            .unwrap_or(0);

        manifest_list.add_manifest(
            manifest_filename,
            manifest_json_size,
            snapshot_id,
            manifest.entries.len() as i32,
            0,
            manifest.total_records(),
            0,
            metadata.last_sequence_number + 1,
        );
        manifest_list.write_json(&manifest_list_path)?;

        // Create snapshot
        let snapshot = Snapshot::new_append(
            snapshot_id,
            metadata.current_snapshot_id,
            manifest_list_filename,
            manifest.active_file_count(),
            manifest.total_records(),
            manifest.active_file_count(),
            manifest.total_records(),
            manifest.total_size_bytes(),
        );

        metadata.add_snapshot(snapshot);

        // Write updated metadata
        let version = metadata.last_sequence_number;
        let versioned_metadata_file = metadata_dir.join(format!("v{}.metadata.json", version));
        metadata.write_json(&versioned_metadata_file)?;

        // Also write as v1.metadata.json (latest pointer)
        metadata.write_json(&metadata_file)?;

        info!(
            "Iceberg snapshot built for {}.{}.{}: {} files, {} records (snapshot {})",
            catalog,
            schema_name,
            table,
            manifest.active_file_count(),
            manifest.total_records(),
            snapshot_id
        );

        Ok(metadata)
    }

    /// Create initial table metadata when none exists
    fn create_initial_metadata(
        &self,
        catalog: &str,
        schema_name: &str,
        table: &str,
        table_dir: &Path,
    ) -> Result<TableMetadata, String> {
        // Try to read schema from first Parquet file
        let iceberg_schema = self.read_schema_from_parquet(table_dir)?;

        // Create partition spec based on available fields
        let partition_spec = if let Some(date_id) = schema::find_date_field_id(&iceberg_schema) {
            partition_spec::create_date_partition_spec(date_id)
        } else if let Some(time_id) = schema::find_time_field_id(&iceberg_schema) {
            partition_spec::create_time_day_partition_spec(time_id)
        } else {
            partition_spec::create_unpartitioned_spec()
        };

        let location = format!(
            "{}/store/{}/{}/{}",
            self.layout.root().display(),
            catalog,
            schema_name,
            table
        );

        Ok(TableMetadata::new(
            uuid::Uuid::new_v4().to_string(),
            location,
            iceberg_schema,
            partition_spec,
        ))
    }

    /// Read Arrow schema from the first Parquet file in a table directory
    fn read_schema_from_parquet(
        &self,
        table_dir: &Path,
    ) -> Result<schema::IcebergSchema, String> {
        // Find first Parquet file by scanning partition dirs
        for entry in std::fs::read_dir(table_dir)
            .map_err(|e| format!("Failed to read table dir: {}", e))?
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                // Look for .parquet files in partition subdirectories
                if let Ok(files) = std::fs::read_dir(&path) {
                    for file_entry in files.flatten() {
                        let file_path = file_entry.path();
                        if file_path
                            .extension()
                            .map_or(false, |ext| ext == "parquet")
                        {
                            return self.read_parquet_schema(&file_path);
                        }
                    }
                }
            }
        }

        Err("No Parquet files found to read schema from".to_string())
    }

    /// Read Arrow schema from a single Parquet file and convert to Iceberg
    fn read_parquet_schema(
        &self,
        parquet_path: &Path,
    ) -> Result<schema::IcebergSchema, String> {
        use parquet::file::reader::FileReader;

        let file = std::fs::File::open(parquet_path)
            .map_err(|e| format!("Failed to open Parquet file: {}", e))?;

        let reader = parquet::file::reader::SerializedFileReader::new(file)
            .map_err(|e| format!("Failed to read Parquet file: {}", e))?;

        let parquet_metadata = reader.metadata();
        let schema_descr = parquet_metadata.file_metadata().schema_descr();
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
            schema_descr,
            parquet_metadata.file_metadata().key_value_metadata(),
        )
        .map_err(|e| format!("Failed to convert Parquet to Arrow schema: {}", e))?;

        Ok(schema::arrow_to_iceberg_schema(&arrow_schema, 0))
    }

    /// Scan a table directory for all Parquet files, organized by partition
    fn scan_parquet_files(
        &self,
        table_dir: &Path,
    ) -> Result<Vec<(String, Vec<(String, u64)>)>, String> {
        let mut partitions: Vec<(String, Vec<(String, u64)>)> = Vec::new();

        let entries = std::fs::read_dir(table_dir)
            .map_err(|e| format!("Failed to read table dir: {}", e))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let dir_name = entry.file_name().to_string_lossy().to_string();
            // Parse date=YYYY-MM-DD partition format
            let partition_date = if dir_name.starts_with("date=") {
                dir_name.trim_start_matches("date=").to_string()
            } else {
                continue;
            };

            let mut files = Vec::new();
            if let Ok(file_entries) = std::fs::read_dir(&path) {
                for file_entry in file_entries.flatten() {
                    let file_path = file_entry.path();
                    if file_path
                        .extension()
                        .map_or(false, |ext| ext == "parquet")
                    {
                        let size = std::fs::metadata(&file_path)
                            .map(|m| m.len())
                            .unwrap_or(0);
                        let relative_path = file_path.to_string_lossy().to_string();
                        files.push((relative_path, size));
                    }
                }
            }

            if !files.is_empty() {
                files.sort_by(|a, b| a.0.cmp(&b.0));
                partitions.push((partition_date, files));
            }
        }

        partitions.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(partitions)
    }
}

/// Estimate record count from file size (rough heuristic)
/// Assumes ~100 bytes per record for typical time-series data
fn estimate_record_count(file_size_bytes: u64) -> i64 {
    (file_size_bytes / 100).max(1) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_record_count() {
        assert_eq!(estimate_record_count(10000), 100);
        assert_eq!(estimate_record_count(0), 1);
        assert_eq!(estimate_record_count(1024 * 1024), 10485);
    }
}
