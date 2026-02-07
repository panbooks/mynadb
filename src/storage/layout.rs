//! Data Directory Layout
//!
//! Single source of truth for all data directory paths.
//! Replaces scattered hardcoded path strings throughout the codebase.
//!
//! ## Directory Structure
//!
//! ```text
//! {data_dir}/
//! ├── store/           # All user Parquet data
//! ├── wal/             # Write-ahead log files
//! ├── meta/            # Metadata: catalog_snapshot.json, aggregates/, downsampling/
//! ├── cache/           # Parquet file cache (for object store backends)
//! ├── system/          # System tables
//! ├── run/             # PID files ({node_type}.pid)
//! └── index/           # Reserved for future index support
//! ```

use std::path::{Path, PathBuf};

// ============================================================================
// Directory name constants — use these instead of hardcoded strings
// ============================================================================

/// User Parquet data directory name
pub const STORE_DIR: &str = "store";
/// Write-ahead log directory name
pub const WAL_DIR: &str = "wal";
/// Metadata directory name (snapshots, aggregates, downsampling)
pub const META_DIR: &str = "meta";
/// File cache directory name (for object store backends)
pub const CACHE_DIR: &str = "cache";
/// System tables directory name
pub const SYSTEM_DIR: &str = "system";
/// PID files directory name
pub const RUN_DIR: &str = "run";
/// Reserved index directory name
pub const INDEX_DIR: &str = "index";

/// Subdirectory under cache for parquet file cache
pub const PARQUET_CACHE_SUBDIR: &str = "parquet";
/// Subdirectory under meta for aggregate definitions
pub const AGGREGATES_SUBDIR: &str = "aggregates";
/// Subdirectory under meta for downsampling definitions
pub const DOWNSAMPLING_SUBDIR: &str = "downsampling";
/// Catalog snapshot filename
pub const CATALOG_SNAPSHOT_FILE: &str = "catalog_snapshot.json";

/// Computes all paths from a single `data_dir` root.
#[derive(Debug, Clone)]
pub struct DataLayout {
    root: PathBuf,
}

impl DataLayout {
    /// Create a new layout rooted at `data_dir`.
    pub fn new(data_dir: &str) -> Self {
        Self {
            root: PathBuf::from(data_dir),
        }
    }

    // ========================================================================
    // Top-level directories
    // ========================================================================

    /// User data: `{root}/store`
    pub fn store_dir(&self) -> PathBuf {
        self.root.join(STORE_DIR)
    }

    /// Write-ahead log: `{root}/wal`
    pub fn wal_dir(&self) -> PathBuf {
        self.root.join(WAL_DIR)
    }

    /// Metadata: `{root}/meta`
    pub fn meta_dir(&self) -> PathBuf {
        self.root.join(META_DIR)
    }

    /// File cache: `{root}/cache`
    pub fn cache_dir(&self) -> PathBuf {
        self.root.join(CACHE_DIR)
    }

    /// System tables: `{root}/system`
    pub fn system_dir(&self) -> PathBuf {
        self.root.join(SYSTEM_DIR)
    }

    /// PID files: `{root}/run`
    pub fn run_dir(&self) -> PathBuf {
        self.root.join(RUN_DIR)
    }

    /// Future index support: `{root}/index`
    pub fn index_dir(&self) -> PathBuf {
        self.root.join(INDEX_DIR)
    }

    // ========================================================================
    // Derived data paths
    // ========================================================================

    /// Catalog directory: `store/{catalog}`
    pub fn catalog_dir(&self, catalog: &str) -> PathBuf {
        self.store_dir().join(catalog)
    }

    /// Schema directory: `store/{catalog}/{schema}`
    pub fn schema_dir(&self, catalog: &str, schema: &str) -> PathBuf {
        self.store_dir().join(catalog).join(schema)
    }

    /// Table directory: `store/{catalog}/{schema}/{table}`
    pub fn table_dir(&self, catalog: &str, schema: &str, table: &str) -> PathBuf {
        self.store_dir().join(catalog).join(schema).join(table)
    }

    /// Partition directory: `store/{catalog}/{schema}/{table}/date={date}`
    pub fn partition_dir(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> PathBuf {
        self.table_dir(catalog, schema, table)
            .join(format!("date={}", date))
    }

    // ========================================================================
    // Metadata paths
    // ========================================================================

    /// Catalog snapshot: `meta/catalog_snapshot.json`
    pub fn catalog_snapshot_path(&self) -> PathBuf {
        self.meta_dir().join(CATALOG_SNAPSHOT_FILE)
    }

    /// Aggregates definitions: `meta/aggregates`
    pub fn aggregates_dir(&self) -> PathBuf {
        self.meta_dir().join(AGGREGATES_SUBDIR)
    }

    /// Downsampling definitions: `meta/downsampling`
    pub fn downsampling_dir(&self) -> PathBuf {
        self.meta_dir().join(DOWNSAMPLING_SUBDIR)
    }

    // ========================================================================
    // Cache paths
    // ========================================================================

    /// Parquet file cache: `cache/parquet`
    pub fn parquet_cache_dir(&self) -> PathBuf {
        self.cache_dir().join(PARQUET_CACHE_SUBDIR)
    }

    // ========================================================================
    // PID file
    // ========================================================================

    /// PID file for a node type: `run/{node_type}.pid`
    pub fn pid_file(&self, node_type: &str) -> PathBuf {
        self.run_dir().join(format!("{}.pid", node_type))
    }

    // ========================================================================
    // Object store prefixes (for S3/MinIO/OSS)
    // ========================================================================

    /// Store prefix for object store paths
    pub fn store_prefix(&self) -> &str {
        STORE_DIR
    }

    /// Table prefix for object store paths: `store/{catalog}/{schema}/{table}/`
    pub fn table_prefix(&self, catalog: &str, schema: &str, table: &str) -> String {
        format!("{}/{}/{}/{}/", STORE_DIR, catalog, schema, table)
    }

    // ========================================================================
    // Initialization
    // ========================================================================

    /// Create all top-level directories.
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.root)?;
        std::fs::create_dir_all(self.store_dir())?;
        std::fs::create_dir_all(self.wal_dir())?;
        std::fs::create_dir_all(self.meta_dir())?;
        std::fs::create_dir_all(self.cache_dir())?;
        std::fs::create_dir_all(self.system_dir())?;
        std::fs::create_dir_all(self.run_dir())?;
        Ok(())
    }

    /// Get the root directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the store directory as a String (for use as data_dir in QueryEngine).
    pub fn store_dir_string(&self) -> String {
        self.store_dir().to_string_lossy().to_string()
    }
}

// ============================================================================
// Migration support
// ============================================================================

/// Check if a directory name looks like a user catalog (database).
fn looks_like_catalog(name: &str) -> bool {
    if name.starts_with('_') || name.starts_with('.') {
        return false;
    }
    !matches!(
        name,
        "store" | "wal" | "meta" | "cache" | "system" | "run" | "index"
    )
}

/// Move all contents of `src_dir` into `dst_dir`, creating `dst_dir` if needed.
fn move_dir_contents(src_dir: &Path, dst_dir: &Path) -> std::io::Result<()> {
    if !src_dir.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(dst_dir)?;
    for entry in std::fs::read_dir(src_dir)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst_dir.join(entry.file_name());
        std::fs::rename(&src_path, &dst_path)?;
    }
    std::fs::remove_dir(src_dir)?;
    Ok(())
}

/// Migrate an old-style data directory to the new standardized layout.
pub fn migrate_layout(data_dir: &str) -> std::io::Result<()> {
    let root = Path::new(data_dir);
    if !root.exists() {
        eprintln!("Data directory does not exist: {}", data_dir);
        return Ok(());
    }

    let layout = DataLayout::new(data_dir);
    layout.ensure_dirs()?;

    let migrations: &[(&str, PathBuf)] = &[
        ("_wal", layout.wal_dir()),
        ("_metadata", layout.meta_dir()),
        ("_cache", layout.cache_dir()),
        ("_parquet_cache", layout.parquet_cache_dir()),
        ("_system", layout.system_dir()),
        ("_aggregates", layout.aggregates_dir()),
        ("_downsampling", layout.downsampling_dir()),
    ];

    for (old_name, new_path) in migrations {
        let old_path = root.join(old_name);
        if old_path.exists() {
            println!("  Moving {} -> {}", old_name, new_path.display());
            move_dir_contents(&old_path, new_path)?;
        }
    }

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".pid") {
                    let dst = layout.run_dir().join(name);
                    println!("  Moving {} -> {}", name, dst.display());
                    std::fs::rename(&path, &dst)?;
                }
            }
        }
    }

    let mut catalogs_to_move = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if entry.path().is_dir() && looks_like_catalog(&name) {
            catalogs_to_move.push(name);
        }
    }

    for catalog in &catalogs_to_move {
        let src = root.join(catalog);
        let dst = layout.store_dir().join(catalog);
        println!("  Moving catalog {} -> {}", catalog, dst.display());
        std::fs::rename(&src, &dst)?;
    }

    println!(
        "  Migration complete: {} catalog(s) moved.",
        catalogs_to_move.len()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_layout_paths() {
        let layout = DataLayout::new("/data/mynadb");

        assert_eq!(layout.store_dir(), PathBuf::from("/data/mynadb/store"));
        assert_eq!(layout.wal_dir(), PathBuf::from("/data/mynadb/wal"));
        assert_eq!(layout.meta_dir(), PathBuf::from("/data/mynadb/meta"));
        assert_eq!(layout.cache_dir(), PathBuf::from("/data/mynadb/cache"));
        assert_eq!(layout.system_dir(), PathBuf::from("/data/mynadb/system"));
        assert_eq!(layout.run_dir(), PathBuf::from("/data/mynadb/run"));
        assert_eq!(layout.index_dir(), PathBuf::from("/data/mynadb/index"));
    }

    #[test]
    fn test_derived_paths() {
        let layout = DataLayout::new("/data");

        assert_eq!(
            layout.catalog_dir("mydb"),
            PathBuf::from("/data/store/mydb")
        );
        assert_eq!(
            layout.schema_dir("mydb", "default"),
            PathBuf::from("/data/store/mydb/default")
        );
        assert_eq!(
            layout.table_dir("mydb", "default", "cpu"),
            PathBuf::from("/data/store/mydb/default/cpu")
        );
        assert_eq!(
            layout.partition_dir("mydb", "default", "cpu", "2024-01-15"),
            PathBuf::from("/data/store/mydb/default/cpu/date=2024-01-15")
        );
    }

    #[test]
    fn test_metadata_paths() {
        let layout = DataLayout::new("/data");

        assert_eq!(
            layout.catalog_snapshot_path(),
            PathBuf::from("/data/meta/catalog_snapshot.json")
        );
        assert_eq!(
            layout.aggregates_dir(),
            PathBuf::from("/data/meta/aggregates")
        );
        assert_eq!(
            layout.downsampling_dir(),
            PathBuf::from("/data/meta/downsampling")
        );
    }

    #[test]
    fn test_cache_paths() {
        let layout = DataLayout::new("/data");

        assert_eq!(
            layout.parquet_cache_dir(),
            PathBuf::from("/data/cache/parquet")
        );
    }

    #[test]
    fn test_pid_file() {
        let layout = DataLayout::new("/data");

        assert_eq!(
            layout.pid_file("standalone"),
            PathBuf::from("/data/run/standalone.pid")
        );
    }

    #[test]
    fn test_object_store_prefixes() {
        let layout = DataLayout::new("/data");

        assert_eq!(layout.store_prefix(), "store");
        assert_eq!(
            layout.table_prefix("mydb", "default", "cpu"),
            "store/mydb/default/cpu/"
        );
    }

    #[test]
    fn test_ensure_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = DataLayout::new(tmp.path().to_str().unwrap());

        layout.ensure_dirs().unwrap();

        assert!(layout.store_dir().is_dir());
        assert!(layout.wal_dir().is_dir());
        assert!(layout.meta_dir().is_dir());
        assert!(layout.cache_dir().is_dir());
        assert!(layout.system_dir().is_dir());
        assert!(layout.run_dir().is_dir());
    }
}
