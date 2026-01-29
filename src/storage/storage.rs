use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::StorageConfig;

/// Storage backend types
#[derive(Debug, Clone)]
pub enum StorageBackend {
    /// Local filesystem storage
    Local(Arc<LocalFileSystem>),
    /// S3-compatible storage (AWS S3, MinIO, Aliyun OSS)
    S3(Arc<dyn ObjectStore>),
}

/// Unified storage interface for Parquet files
#[derive(Clone)]
pub struct Storage {
    backend: StorageBackend,
    /// Base path prefix for all operations
    base_path: String,
    /// URL prefix for DataFusion registration
    url_prefix: String,
}

impl Storage {
    /// Create storage from configuration
    pub fn from_config(config: &StorageConfig) -> Result<Self, String> {
        match config.backend.as_str() {
            "local" | "file" => Self::new_local(&config.data_dir),
            "s3" | "minio" | "oss" => Self::new_s3(config),
            _ => Err(format!("Unknown storage backend: {}", config.backend)),
        }
    }

    /// Create local filesystem storage
    pub fn new_local(data_dir: &str) -> Result<Self, String> {
        let path = std::path::Path::new(data_dir);
        if !path.exists() {
            std::fs::create_dir_all(path)
                .map_err(|e| format!("Failed to create data directory: {}", e))?;
        }

        let abs_path = path
            .canonicalize()
            .map_err(|e| format!("Failed to get absolute path: {}", e))?;

        let local = LocalFileSystem::new_with_prefix(&abs_path)
            .map_err(|e| format!("Failed to create local storage: {}", e))?;

        info!("Initialized local storage at: {}", abs_path.display());

        Ok(Storage {
            backend: StorageBackend::Local(Arc::new(local)),
            base_path: String::new(),
            url_prefix: format!("file://{}", abs_path.display()),
        })
    }

    /// Create S3-compatible storage (AWS S3, MinIO, Aliyun OSS)
    pub fn new_s3(config: &StorageConfig) -> Result<Self, String> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        // Set credentials
        if let Some(ref key) = config.access_key {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = config.secret_key {
            builder = builder.with_secret_access_key(secret);
        }

        // Set custom endpoint for MinIO/OSS
        if let Some(ref endpoint) = config.endpoint {
            builder = builder.with_endpoint(endpoint);
            // For MinIO and other S3-compatible services
            builder = builder.with_virtual_hosted_style_request(false);
        }

        // Allow HTTP for local MinIO
        if config.allow_http {
            builder = builder.with_allow_http(true);
        }

        let store = builder
            .build()
            .map_err(|e| format!("Failed to create S3 storage: {}", e))?;

        let url_prefix = format!("s3://{}", config.bucket);

        info!(
            "Initialized S3 storage: bucket={}, region={}, endpoint={:?}",
            config.bucket, config.region, config.endpoint
        );

        Ok(Storage {
            backend: StorageBackend::S3(Arc::new(store)),
            base_path: config.prefix.clone().unwrap_or_default(),
            url_prefix,
        })
    }

    /// Get the underlying ObjectStore
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.backend {
            StorageBackend::Local(store) => store.clone(),
            StorageBackend::S3(store) => store.clone(),
        }
    }

    /// Get URL prefix for DataFusion table registration
    pub fn url_prefix(&self) -> &str {
        &self.url_prefix
    }

    /// Build full path from relative path
    fn full_path(&self, path: &str) -> ObjectPath {
        if self.base_path.is_empty() {
            ObjectPath::from(path)
        } else {
            ObjectPath::from(format!("{}/{}", self.base_path, path))
        }
    }

    /// Write data to storage
    pub async fn put(&self, path: &str, data: Bytes) -> Result<(), String> {
        let full_path = self.full_path(path);
        debug!("Writing {} bytes to {}", data.len(), full_path);

        let store = self.object_store();
        store
            .put(&full_path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| format!("Failed to write {}: {}", full_path, e))?;

        Ok(())
    }

    /// Read data from storage
    pub async fn get(&self, path: &str) -> Result<Bytes, String> {
        let full_path = self.full_path(path);
        debug!("Reading from {}", full_path);

        let store = self.object_store();
        let result = store
            .get(&full_path)
            .await
            .map_err(|e| format!("Failed to read {}: {}", full_path, e))?;

        result
            .bytes()
            .await
            .map_err(|e| format!("Failed to read bytes from {}: {}", full_path, e))
    }

    /// Delete a file from storage
    pub async fn delete(&self, path: &str) -> Result<(), String> {
        let full_path = self.full_path(path);
        debug!("Deleting {}", full_path);

        let store = self.object_store();
        store
            .delete(&full_path)
            .await
            .map_err(|e| format!("Failed to delete {}: {}", full_path, e))
    }

    /// List files with prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let full_prefix = self.full_path(prefix);
        debug!("Listing files with prefix: {}", full_prefix);

        let store = self.object_store();
        let mut paths = Vec::new();

        let list_result = store.list(Some(&full_prefix));

        use futures::StreamExt;
        let mut stream = list_result;
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    let path_str = meta.location.to_string();
                    // Remove base_path prefix if present
                    let relative =
                        if !self.base_path.is_empty() && path_str.starts_with(&self.base_path) {
                            path_str[self.base_path.len()..]
                                .trim_start_matches('/')
                                .to_string()
                        } else {
                            path_str
                        };
                    paths.push(relative);
                }
                Err(e) => {
                    return Err(format!("Error listing files: {}", e));
                }
            }
        }

        Ok(paths)
    }

    /// Check if a path exists
    pub async fn exists(&self, path: &str) -> bool {
        let full_path = self.full_path(path);
        let store = self.object_store();
        store.head(&full_path).await.is_ok()
    }

    /// Create directory (no-op for object stores, creates parent dirs for local)
    pub async fn create_dir(&self, path: &str) -> Result<(), String> {
        match &self.backend {
            StorageBackend::Local(_) => {
                let full_path = if self.base_path.is_empty() {
                    path.to_string()
                } else {
                    format!("{}/{}", self.base_path, path)
                };
                // For local storage, extract the directory from URL prefix
                let base = self.url_prefix.trim_start_matches("file://");
                let dir_path = format!("{}/{}", base, full_path);
                std::fs::create_dir_all(&dir_path)
                    .map_err(|e| format!("Failed to create directory {}: {}", dir_path, e))
            }
            StorageBackend::S3(_) => {
                // S3 doesn't have directories, they're implicit
                Ok(())
            }
        }
    }

    /// Get file size
    pub async fn size(&self, path: &str) -> Result<u64, String> {
        let full_path = self.full_path(path);
        let store = self.object_store();
        let meta = store
            .head(&full_path)
            .await
            .map_err(|e| format!("Failed to get metadata for {}: {}", full_path, e))?;
        Ok(meta.size as u64)
    }

    /// Check if this is local storage
    pub fn is_local(&self) -> bool {
        matches!(self.backend, StorageBackend::Local(_))
    }

    /// Get local filesystem path (only for local backend)
    pub fn local_path(&self, relative_path: &str) -> Option<std::path::PathBuf> {
        if let StorageBackend::Local(_) = &self.backend {
            let base = self.url_prefix.trim_start_matches("file://");
            Some(std::path::PathBuf::from(format!(
                "{}/{}",
                base, relative_path
            )))
        } else {
            None
        }
    }

    /// Build table URL for DataFusion registration
    pub fn table_url(&self, catalog: &str, schema: &str, table: &str) -> String {
        format!("{}/{}/{}/{}/", self.url_prefix, catalog, schema, table)
    }

    /// Build table URL for DataFusion registration with node_id prefix
    /// Used in cluster mode where files are stored under {node_id}/{catalog}/{schema}/{table}/
    pub fn table_url_with_node(
        &self,
        node_id: &str,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> String {
        format!(
            "{}/{}/{}/{}/{}/",
            self.url_prefix, node_id, catalog, schema, table
        )
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field(
                "backend",
                &match &self.backend {
                    StorageBackend::Local(_) => "local",
                    StorageBackend::S3(_) => "s3",
                },
            )
            .field("base_path", &self.base_path)
            .field("url_prefix", &self.url_prefix)
            .finish()
    }
}
