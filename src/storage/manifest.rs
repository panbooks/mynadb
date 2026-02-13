//! Compaction Manifest
//!
//! Tracks which parquet files have been superseded by compaction output.
//! Prevents duplicate rows when source file deletion fails after compaction.
//!
//! Each partition directory (e.g., `date=2024-01-15/node_id=0/`) gets a
//! `MANIFEST.json` listing filenames that should be excluded from queries.
//! The compactor writes to the manifest before attempting source file deletion;
//! the query discovery path reads it to filter out stale files.

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, warn};

use super::storage::Storage;

/// Manifest filename placed in each partition directory.
pub const MANIFEST_FILENAME: &str = "MANIFEST.json";

/// Tracks superseded files within a single partition directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionManifest {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// Filenames (not full paths) of parquet files that have been compacted
    /// into higher-level files and should be excluded from queries.
    pub superseded: Vec<String>,
}

impl PartitionManifest {
    pub fn new() -> Self {
        Self {
            version: 1,
            superseded: Vec::new(),
        }
    }

    /// Return the superseded filenames as a HashSet for efficient lookups.
    pub fn superseded_set(&self) -> HashSet<String> {
        self.superseded.iter().cloned().collect()
    }
}

/// Extract the filename component from a path string.
///
/// Example: `"store/db/default/cpu/date=2024-01-15/node_id=0/L0_1000.parquet"` → `"L0_1000.parquet"`
pub fn filename_from_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

// ---------------------------------------------------------------------------
// Storage-based API (for compactor — uses string paths)
// ---------------------------------------------------------------------------

/// Read the manifest from a partition directory via Storage.
/// Returns `None` if no manifest exists or it can't be parsed (backward compat).
pub async fn read_manifest(storage: &Storage, dir_path: &str) -> Option<PartitionManifest> {
    let manifest_path = format!("{}/{}", dir_path, MANIFEST_FILENAME);
    let data = storage.get(&manifest_path).await.ok()?;
    serde_json::from_slice(&data).ok()
}

/// Write a manifest to a partition directory via Storage.
pub async fn write_manifest(
    storage: &Storage,
    dir_path: &str,
    manifest: &PartitionManifest,
) -> Result<(), String> {
    let manifest_path = format!("{}/{}", dir_path, MANIFEST_FILENAME);
    let data = serde_json::to_vec(manifest)
        .map_err(|e| format!("Failed to serialize manifest: {}", e))?;
    storage.put(&manifest_path, Bytes::from(data)).await
}

/// Mark the given filenames as superseded in the partition manifest.
///
/// Reads the current manifest (or creates a new one), appends the filenames,
/// deduplicates, and writes back. This must be called BEFORE attempting to
/// delete source files so that the query path can immediately skip them.
pub async fn mark_files_superseded(
    storage: &Storage,
    dir_path: &str,
    filenames: &[String],
) -> Result<(), String> {
    if filenames.is_empty() {
        return Ok(());
    }

    let mut manifest = read_manifest(storage, dir_path)
        .await
        .unwrap_or_else(PartitionManifest::new);

    let existing: HashSet<String> = manifest.superseded.iter().cloned().collect();
    for name in filenames {
        if !existing.contains(name.as_str()) {
            manifest.superseded.push(name.clone());
        }
    }

    write_manifest(storage, dir_path, &manifest).await?;
    debug!(
        "Manifest updated: {} superseded files in {}",
        manifest.superseded.len(),
        dir_path
    );
    Ok(())
}

/// Load the superseded filename set from Storage (for compactor-side filtering).
pub async fn get_superseded_set_from_storage(
    storage: &Storage,
    dir_path: &str,
) -> HashSet<String> {
    read_manifest(storage, dir_path)
        .await
        .map(|m| m.superseded_set())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// ObjectStore-based API (for query discovery — uses ObjectPath)
// ---------------------------------------------------------------------------

/// Load the superseded filename set from an ObjectStore path.
///
/// Used by `discover_files()` to exclude stale files from query results.
/// Returns an empty set if no manifest exists (backward compat: all files are live).
pub async fn get_superseded_set(
    object_store: &Arc<dyn ObjectStore>,
    dir_path: &ObjectPath,
) -> HashSet<String> {
    let manifest_path = ObjectPath::from(format!(
        "{}/{}",
        dir_path.as_ref(),
        MANIFEST_FILENAME
    ));

    let data = match object_store.get(&manifest_path).await {
        Ok(result) => match result.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => return HashSet::new(),
        },
        Err(_) => return HashSet::new(),
    };

    match serde_json::from_slice::<PartitionManifest>(&data) {
        Ok(manifest) => manifest.superseded_set(),
        Err(e) => {
            warn!("Failed to parse manifest at {}: {}", manifest_path, e);
            HashSet::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filename_from_path() {
        assert_eq!(
            filename_from_path("store/db/default/cpu/date=2024-01-15/node_id=0/L0_1000.parquet"),
            "L0_1000.parquet"
        );
        assert_eq!(filename_from_path("L1_2000.parquet"), "L1_2000.parquet");
        assert_eq!(
            filename_from_path("FULL_3000.parquet"),
            "FULL_3000.parquet"
        );
    }

    #[test]
    fn test_manifest_new() {
        let m = PartitionManifest::new();
        assert_eq!(m.version, 1);
        assert!(m.superseded.is_empty());
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let m = PartitionManifest {
            version: 1,
            superseded: vec![
                "L0_1000.parquet".to_string(),
                "L0_2000.parquet".to_string(),
            ],
        };
        let json = serde_json::to_vec(&m).unwrap();
        let m2: PartitionManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(m2.version, 1);
        assert_eq!(m2.superseded.len(), 2);
        assert!(m2.superseded.contains(&"L0_1000.parquet".to_string()));
    }

    #[test]
    fn test_superseded_set() {
        let m = PartitionManifest {
            version: 1,
            superseded: vec![
                "L0_1000.parquet".to_string(),
                "L0_2000.parquet".to_string(),
            ],
        };
        let set = m.superseded_set();
        assert_eq!(set.len(), 2);
        assert!(set.contains("L0_1000.parquet"));
        assert!(set.contains("L0_2000.parquet"));
        assert!(!set.contains("L1_3000.parquet"));
    }

    #[tokio::test]
    async fn test_read_write_manifest_via_storage() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::new_local(temp_dir.path().to_str().unwrap()).unwrap();

        // No manifest initially
        assert!(read_manifest(&storage, "test_partition").await.is_none());

        // Write a manifest
        let manifest = PartitionManifest {
            version: 1,
            superseded: vec!["L0_1000.parquet".to_string()],
        };
        // Create the directory first
        storage.create_dir("test_partition").await.unwrap();
        write_manifest(&storage, "test_partition", &manifest)
            .await
            .unwrap();

        // Read it back
        let read_back = read_manifest(&storage, "test_partition").await.unwrap();
        assert_eq!(read_back.superseded.len(), 1);
        assert_eq!(read_back.superseded[0], "L0_1000.parquet");
    }

    #[tokio::test]
    async fn test_mark_files_superseded() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::new_local(temp_dir.path().to_str().unwrap()).unwrap();
        storage.create_dir("partition").await.unwrap();

        // First batch
        mark_files_superseded(
            &storage,
            "partition",
            &[
                "L0_1000.parquet".to_string(),
                "L0_2000.parquet".to_string(),
            ],
        )
        .await
        .unwrap();

        let set = get_superseded_set_from_storage(&storage, "partition").await;
        assert_eq!(set.len(), 2);

        // Second batch (with one duplicate)
        mark_files_superseded(
            &storage,
            "partition",
            &[
                "L0_2000.parquet".to_string(), // duplicate
                "L1_3000.parquet".to_string(),
            ],
        )
        .await
        .unwrap();

        let set = get_superseded_set_from_storage(&storage, "partition").await;
        assert_eq!(set.len(), 3);
        assert!(set.contains("L0_1000.parquet"));
        assert!(set.contains("L0_2000.parquet"));
        assert!(set.contains("L1_3000.parquet"));
    }

    #[tokio::test]
    async fn test_mark_empty_filenames_is_noop() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::new_local(temp_dir.path().to_str().unwrap()).unwrap();

        // Should succeed without creating a manifest file
        mark_files_superseded(&storage, "partition", &[])
            .await
            .unwrap();
        assert!(read_manifest(&storage, "partition").await.is_none());
    }

    #[tokio::test]
    async fn test_get_superseded_set_from_object_store() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::new_local(temp_dir.path().to_str().unwrap()).unwrap();
        let object_store = storage.object_store();

        // No manifest — returns empty set
        let dir = ObjectPath::from("no_such_dir");
        let set = get_superseded_set(&object_store, &dir).await;
        assert!(set.is_empty());

        // Write manifest via Storage, read via ObjectStore
        storage.create_dir("part").await.unwrap();
        let manifest = PartitionManifest {
            version: 1,
            superseded: vec![
                "L0_100.parquet".to_string(),
                "L0_200.parquet".to_string(),
            ],
        };
        write_manifest(&storage, "part", &manifest).await.unwrap();

        let dir = ObjectPath::from("part");
        let set = get_superseded_set(&object_store, &dir).await;
        assert_eq!(set.len(), 2);
        assert!(set.contains("L0_100.parquet"));
        assert!(set.contains("L0_200.parquet"));
    }
}
