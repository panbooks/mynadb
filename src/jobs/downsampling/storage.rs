//! Storage for Downsampling Policy Definitions
//!
//! Persists policies as JSON files in `{data_dir}/_downsampling/`.
//! Follows the same pattern as `aggregates/storage.rs`.

use super::types::{DownsamplingPolicy, PolicyStatus, TierStatus};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// File extension for policy definition files
const DEFINITION_EXT: &str = ".json";

/// Subdirectory for status tracking files
const STATUS_DIR: &str = "status";

/// Storage backend for downsampling policies
pub struct DownsamplingStorage {
    /// Base directory for storing policies
    base_dir: PathBuf,
    /// In-memory cache of policies
    cache: Arc<RwLock<HashMap<String, DownsamplingPolicy>>>,
    /// In-memory cache of policy statuses
    status_cache: Arc<RwLock<HashMap<String, PolicyStatus>>>,
    /// Whether the cache has been initialized
    initialized: Arc<RwLock<bool>>,
}

impl DownsamplingStorage {
    /// Create a new storage backend
    pub fn new(data_dir: &str) -> Self {
        let layout = crate::storage::layout::DataLayout::new(data_dir);
        let base_dir = layout.downsampling_dir();
        Self {
            base_dir,
            cache: Arc::new(RwLock::new(HashMap::new())),
            status_cache: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a storage backend with custom directory (for testing)
    pub fn with_dir(dir: PathBuf) -> Self {
        Self {
            base_dir: dir,
            cache: Arc::new(RwLock::new(HashMap::new())),
            status_cache: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Ensure the storage directories exist
    fn ensure_dirs(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.base_dir).map_err(|e| {
            format!(
                "Failed to create downsampling storage directory {}: {}",
                self.base_dir.display(),
                e
            )
        })?;

        let status_dir = self.base_dir.join(STATUS_DIR);
        std::fs::create_dir_all(&status_dir).map_err(|e| {
            format!(
                "Failed to create status directory {}: {}",
                status_dir.display(),
                e
            )
        })?;

        Ok(())
    }

    /// Get the file path for a policy definition
    fn policy_path(&self, name: &str) -> PathBuf {
        self.base_dir.join(format!("{}{}", name, DEFINITION_EXT))
    }

    /// Get the file path for a policy status
    fn status_path(&self, name: &str) -> PathBuf {
        self.base_dir
            .join(STATUS_DIR)
            .join(format!("{}{}", name, DEFINITION_EXT))
    }

    /// Initialize the cache by loading all policies from disk
    pub async fn initialize(&self) -> Result<(), String> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            return Ok(());
        }

        self.ensure_dirs()?;
        self.load_all_policies().await?;
        *initialized = true;

        Ok(())
    }

    /// Load all policies from disk into cache
    async fn load_all_policies(&self) -> Result<(), String> {
        let entries = std::fs::read_dir(&self.base_dir).map_err(|e| {
            format!(
                "Failed to read downsampling directory {}: {}",
                self.base_dir.display(),
                e
            )
        })?;

        let mut cache = self.cache.write().await;
        let mut status_cache = self.status_cache.write().await;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(stem) = path.file_stem() {
                    let name = stem.to_string_lossy().to_string();
                    match self.load_policy_from_file(&path) {
                        Ok(policy) => {
                            // Load status if exists
                            if let Ok(status) = self.load_status_from_file(&name) {
                                status_cache.insert(name.clone(), status);
                            }
                            debug!("Loaded downsampling policy: {}", name);
                            cache.insert(name, policy);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load downsampling policy {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }

        info!("Loaded {} downsampling policies", cache.len());
        Ok(())
    }

    /// Load a policy from a file
    fn load_policy_from_file(&self, path: &Path) -> Result<DownsamplingPolicy, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

        serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse {}: {}", path.display(), e))
    }

    /// Load a status from file
    fn load_status_from_file(&self, name: &str) -> Result<PolicyStatus, String> {
        let path = self.status_path(name);
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read status {}: {}", path.display(), e))?;

        serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse status {}: {}", path.display(), e))
    }

    /// Save a policy to disk
    pub async fn save_policy(&self, policy: &DownsamplingPolicy) -> Result<(), String> {
        self.ensure_dirs()?;

        let path = self.policy_path(&policy.name);
        let content = serde_json::to_string_pretty(policy)
            .map_err(|e| format!("Failed to serialize policy: {}", e))?;

        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;

        // Update cache
        let mut cache = self.cache.write().await;
        cache.insert(policy.name.clone(), policy.clone());

        info!("Saved downsampling policy: {}", policy.name);
        Ok(())
    }

    /// Save policy status to disk
    pub async fn save_status(&self, status: &PolicyStatus) -> Result<(), String> {
        self.ensure_dirs()?;

        let path = self.status_path(&status.policy_name);
        let content = serde_json::to_string_pretty(status)
            .map_err(|e| format!("Failed to serialize status: {}", e))?;

        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;

        // Update cache
        let mut status_cache = self.status_cache.write().await;
        status_cache.insert(status.policy_name.clone(), status.clone());

        debug!("Saved status for policy: {}", status.policy_name);
        Ok(())
    }

    /// Delete a policy and its status
    pub async fn delete_policy(&self, name: &str) -> Result<(), String> {
        let path = self.policy_path(name);
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete {}: {}", path.display(), e))?;
        }

        // Also delete status
        let status_path = self.status_path(name);
        if status_path.exists() {
            let _ = std::fs::remove_file(&status_path);
        }

        // Update caches
        let mut cache = self.cache.write().await;
        cache.remove(name);
        let mut status_cache = self.status_cache.write().await;
        status_cache.remove(name);

        info!("Deleted downsampling policy: {}", name);
        Ok(())
    }

    /// Get a policy by name
    pub async fn get_policy(&self, name: &str) -> Result<Option<DownsamplingPolicy>, String> {
        self.ensure_initialized().await?;
        let cache = self.cache.read().await;
        Ok(cache.get(name).cloned())
    }

    /// Get policy status by name
    pub async fn get_status(&self, name: &str) -> Result<Option<PolicyStatus>, String> {
        self.ensure_initialized().await?;
        let cache = self.status_cache.read().await;
        Ok(cache.get(name).cloned())
    }

    /// List all policies
    pub async fn list_policies(&self) -> Result<Vec<DownsamplingPolicy>, String> {
        self.ensure_initialized().await?;
        let cache = self.cache.read().await;
        Ok(cache.values().cloned().collect())
    }

    /// Get all enabled policies
    pub async fn get_enabled_policies(&self) -> Result<Vec<DownsamplingPolicy>, String> {
        let policies = self.list_policies().await?;
        Ok(policies.into_iter().filter(|p| p.enabled).collect())
    }

    /// Find policies that reference a given source table
    pub async fn find_policies_for_table(
        &self,
        source_table: &str,
    ) -> Result<Vec<DownsamplingPolicy>, String> {
        let policies = self.list_policies().await?;
        Ok(policies
            .into_iter()
            .filter(|p| p.enabled && p.source_table == source_table)
            .collect())
    }

    /// Ensure initialized, initializing if needed
    async fn ensure_initialized(&self) -> Result<(), String> {
        let initialized = self.initialized.read().await;
        if !*initialized {
            drop(initialized);
            self.initialize().await?;
        }
        Ok(())
    }

    /// Update tier status within a policy status
    pub async fn update_tier_status(
        &self,
        policy_name: &str,
        tier_status: TierStatus,
    ) -> Result<(), String> {
        let mut status_cache = self.status_cache.write().await;
        let status = status_cache
            .entry(policy_name.to_string())
            .or_insert_with(|| PolicyStatus {
                policy_name: policy_name.to_string(),
                tiers: Vec::new(),
            });

        // Update or insert tier status
        if let Some(existing) = status.tiers.iter_mut().find(|t| t.tier_name == tier_status.tier_name) {
            *existing = tier_status;
        } else {
            status.tiers.push(tier_status);
        }

        // Write to disk
        let status_clone = status.clone();
        drop(status_cache);

        self.ensure_dirs()?;
        let path = self.status_path(policy_name);
        let content = serde_json::to_string_pretty(&status_clone)
            .map_err(|e| format!("Failed to serialize status: {}", e))?;
        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;

        Ok(())
    }

    /// Get tier status for a specific tier in a policy
    pub async fn get_tier_status(
        &self,
        policy_name: &str,
        tier_name: &str,
    ) -> Option<TierStatus> {
        let cache = self.status_cache.read().await;
        cache.get(policy_name).and_then(|s| {
            s.tiers.iter().find(|t| t.tier_name == tier_name).cloned()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::downsampling::types::{AggregateSpec, AggregateFn, DownsamplingTier};
    use tempfile::tempdir;

    fn test_policy() -> DownsamplingPolicy {
        DownsamplingPolicy {
            name: "cpu_ds".to_string(),
            source_table: "mydb.default.cpu".to_string(),
            tiers: vec![DownsamplingTier {
                name: "1m".to_string(),
                after_days: 7,
                interval: "1 minute".to_string(),
                aggregates: vec![AggregateSpec {
                    function: AggregateFn::Avg,
                    column: "usage".to_string(),
                }],
                retention_days: Some(90),
            }],
            enabled: true,
            created_at: 0,
            modified_at: 0,
        }
    }

    #[tokio::test]
    async fn test_storage_crud() {
        let dir = tempdir().unwrap();
        let storage = DownsamplingStorage::with_dir(dir.path().to_path_buf());

        let policy = test_policy();

        // Save
        storage.save_policy(&policy).await.unwrap();

        // Get
        let loaded = storage.get_policy("cpu_ds").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().source_table, "mydb.default.cpu");

        // List
        let all = storage.list_policies().await.unwrap();
        assert_eq!(all.len(), 1);

        // Delete
        storage.delete_policy("cpu_ds").await.unwrap();
        let loaded = storage.get_policy("cpu_ds").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_status_tracking() {
        let dir = tempdir().unwrap();
        let storage = DownsamplingStorage::with_dir(dir.path().to_path_buf());

        let policy = test_policy();
        storage.save_policy(&policy).await.unwrap();

        // Update tier status
        let tier_status = TierStatus {
            tier_name: "1m".to_string(),
            last_processed_date: Some("2026-01-15".to_string()),
            total_rows_written: 1000,
            total_bytes_written: 4096,
            partitions_processed: 5,
            last_run_at: 1000000,
        };
        storage
            .update_tier_status("cpu_ds", tier_status)
            .await
            .unwrap();

        let status = storage.get_tier_status("cpu_ds", "1m").await;
        assert!(status.is_some());
        let s = status.unwrap();
        assert_eq!(s.total_rows_written, 1000);
        assert_eq!(s.last_processed_date, Some("2026-01-15".to_string()));
    }

    #[tokio::test]
    async fn test_persistence_across_instances() {
        let dir = tempdir().unwrap();

        // Save with first instance
        {
            let storage = DownsamplingStorage::with_dir(dir.path().to_path_buf());
            storage.save_policy(&test_policy()).await.unwrap();

            let tier_status = TierStatus {
                tier_name: "1m".to_string(),
                last_processed_date: Some("2026-01-20".to_string()),
                total_rows_written: 500,
                total_bytes_written: 2048,
                partitions_processed: 3,
                last_run_at: 0,
            };
            storage
                .update_tier_status("cpu_ds", tier_status)
                .await
                .unwrap();
        }

        // Load with second instance
        {
            let storage = DownsamplingStorage::with_dir(dir.path().to_path_buf());
            storage.initialize().await.unwrap();

            let policy = storage.get_policy("cpu_ds").await.unwrap();
            assert!(policy.is_some());

            let status = storage.get_tier_status("cpu_ds", "1m").await;
            assert!(status.is_some());
            assert_eq!(status.unwrap().total_rows_written, 500);
        }
    }

    #[tokio::test]
    async fn test_enabled_policies() {
        let dir = tempdir().unwrap();
        let storage = DownsamplingStorage::with_dir(dir.path().to_path_buf());

        let mut p1 = test_policy();
        p1.name = "p1".to_string();
        p1.enabled = true;
        storage.save_policy(&p1).await.unwrap();

        let mut p2 = test_policy();
        p2.name = "p2".to_string();
        p2.enabled = false;
        storage.save_policy(&p2).await.unwrap();

        let enabled = storage.get_enabled_policies().await.unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "p1");
    }
}
