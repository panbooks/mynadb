use bloomfilter::Bloom;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};
use wildmatch::WildMatch;

/// Configuration for bloom filter
#[derive(Debug, Clone)]
pub struct BloomConfig {
    pub expected_series_per_day: usize,
    pub false_positive_rate: f64,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            expected_series_per_day: 1_000_000_000, // 1 billion
            false_positive_rate: 0.01,                // 1%
        }
    }
}

/// Strategy for deduplicating series hashes
/// Each strategy tracks which series have been seen per (schema, database, date) tuple
pub trait DeduplicationStrategy: Send + Sync {
    /// Check if series hash exists for given schema, database and date
    fn contains(&self, schema: &str, db: &str, date: i32, series_hash: u64) -> bool;

    /// Insert series hash for given schema, database and date
    fn insert(&mut self, schema: &str, db: &str, date: i32, series_hash: u64);

    /// Clear data before given date (for cache retention)
    fn clear_before_date(&mut self, cutoff_date: i32);

    /// Get memory usage estimate in bytes
    fn memory_bytes(&self) -> usize;

    /// Get strategy name for logging/metrics
    fn name(&self) -> &str;

    /// Clone box (for dynamic dispatch)
    fn clone_box(&self) -> Box<dyn DeduplicationStrategy>;
}

// Implement Clone for Box<dyn DeduplicationStrategy>
impl Clone for Box<dyn DeduplicationStrategy> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// HashSet-based deduplication strategy (exact, high memory)
/// Best for: < 100M series per database
/// Memory: ~24 bytes per series
#[derive(Clone)]
pub struct HashSetStrategy {
    /// Map: (schema, date, db) -> HashSet<series_hash>
    cache: HashMap<(String, i32, String), HashSet<u64>>,
}

impl HashSetStrategy {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl DeduplicationStrategy for HashSetStrategy {
    fn contains(&self, schema: &str, db: &str, date: i32, series_hash: u64) -> bool {
        self.cache
            .get(&(schema.to_string(), date, db.to_string()))
            .map(|set| set.contains(&series_hash))
            .unwrap_or(false)
    }

    fn insert(&mut self, schema: &str, db: &str, date: i32, series_hash: u64) {
        self.cache
            .entry((schema.to_string(), date, db.to_string()))
            .or_insert_with(HashSet::new)
            .insert(series_hash);
    }

    fn clear_before_date(&mut self, cutoff_date: i32) {
        self.cache.retain(|(_, date, _), _| *date >= cutoff_date);
    }

    fn memory_bytes(&self) -> usize {
        // 8 bytes per hash + HashSet overhead (~16 bytes) + HashMap overhead
        self.cache
            .values()
            .map(|set| set.len() * 24)
            .sum::<usize>()
            + self.cache.len() * 50 // HashMap entry overhead
    }

    fn name(&self) -> &str {
        "hashset"
    }

    fn clone_box(&self) -> Box<dyn DeduplicationStrategy> {
        Box::new(self.clone())
    }
}

/// Bloom filter-based deduplication strategy (probabilistic, low memory)
/// Best for: > 100M series per database (up to 10B+)
/// Memory: ~1.4 bytes per series (with 1% FP rate)
/// Trade-off: ~1% false positive rate (might skip ~1% of NEW series)
#[derive(Clone)]
pub struct BloomFilterStrategy {
    /// Map: (schema, date, db) -> Bloom<u64>
    filters: HashMap<(String, i32, String), Bloom<u64>>,
    expected_items: usize,
    false_positive_rate: f64,
}

impl BloomFilterStrategy {
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        Self {
            filters: HashMap::new(),
            expected_items,
            false_positive_rate: fp_rate,
        }
    }

    fn get_or_create_filter(&mut self, schema: &str, date: i32, db: &str) -> &mut Bloom<u64> {
        self.filters
            .entry((schema.to_string(), date, db.to_string()))
            .or_insert_with(|| {
                Bloom::new_for_fp_rate(self.expected_items, self.false_positive_rate)
            })
    }
}

impl DeduplicationStrategy for BloomFilterStrategy {
    fn contains(&self, schema: &str, db: &str, date: i32, series_hash: u64) -> bool {
        self.filters
            .get(&(schema.to_string(), date, db.to_string()))
            .map(|bloom| bloom.check(&series_hash))
            .unwrap_or(false)
    }

    fn insert(&mut self, schema: &str, db: &str, date: i32, series_hash: u64) {
        let bloom = self.get_or_create_filter(schema, date, db);
        bloom.set(&series_hash);
    }

    fn clear_before_date(&mut self, cutoff_date: i32) {
        self.filters.retain(|(_, date, _), _| *date >= cutoff_date);
    }

    fn memory_bytes(&self) -> usize {
        self.filters
            .values()
            .map(|bloom| (bloom.number_of_bits() / 8) as usize)
            .sum::<usize>()
            + self.filters.len() * 50 // HashMap entry overhead
    }

    fn name(&self) -> &str {
        "bloom_filter"
    }

    fn clone_box(&self) -> Box<dyn DeduplicationStrategy> {
        Box::new(self.clone())
    }
}

/// Strategy type enum
#[derive(Debug, Clone)]
enum StrategyType {
    HashSet,
    Bloom,
}

/// Manages deduplication strategies per database
/// Allows different databases to use different strategies based on cardinality
pub struct StrategyManager {
    /// Default strategy type for databases not explicitly configured
    default_strategy: StrategyType,

    /// Bloom database patterns (supports wildcards like "prod_*")
    bloom_patterns: Vec<WildMatch>,

    /// Default bloom configuration
    bloom_config: BloomConfig,

    /// Per-database bloom customization
    bloom_custom: HashMap<String, BloomConfig>,

    /// Active strategies per database (lazy-initialized)
    /// Stores Arc<RwLock> to share the same strategy instance across all callers
    strategies: RwLock<HashMap<String, Arc<RwLock<Box<dyn DeduplicationStrategy>>>>>,
}

impl StrategyManager {
    pub fn new(
        default_strategy: &str,
        bloom_databases: Vec<String>,
        bloom_config: BloomConfig,
        bloom_custom: HashMap<String, BloomConfig>,
    ) -> Self {
        let bloom_patterns = bloom_databases
            .iter()
            .map(|pattern| WildMatch::new(pattern))
            .collect();

        Self {
            default_strategy: match default_strategy {
                "bloom" => StrategyType::Bloom,
                _ => StrategyType::HashSet,
            },
            bloom_patterns,
            bloom_config,
            bloom_custom,
            strategies: RwLock::new(HashMap::new()),
        }
    }

    /// Determine strategy type for a database
    fn get_strategy_type(&self, db: &str) -> StrategyType {
        // Check if database matches any bloom pattern
        for pattern in &self.bloom_patterns {
            if pattern.matches(db) {
                return StrategyType::Bloom;
            }
        }

        // Use default strategy
        self.default_strategy.clone()
    }

    /// Get or create strategy for a database
    /// Returns a cloned Arc that shares the same underlying strategy instance
    pub async fn get_or_create_strategy(&self, db: &str) -> Arc<RwLock<Box<dyn DeduplicationStrategy>>> {
        // Quick check with read lock
        {
            let strategies = self.strategies.read().await;
            if let Some(strategy) = strategies.get(db) {
                // Strategy exists, return a clone of the Arc (shares same instance)
                return Arc::clone(strategy);
            }
        }

        // Get or create with write lock
        let mut strategies = self.strategies.write().await;

        // Double-check after acquiring write lock (another thread might have created it)
        if let Some(strategy) = strategies.get(db) {
            return Arc::clone(strategy);
        }

        // Create new strategy and wrap in Arc<RwLock>
        let strategy = self.create_strategy(db);
        info!(
            "Created {} strategy for database '{}' (estimated memory: {:.2}MB)",
            strategy.name(),
            db,
            strategy.memory_bytes() as f64 / 1024.0 / 1024.0
        );

        let strategy_arc = Arc::new(RwLock::new(strategy));
        strategies.insert(db.to_string(), Arc::clone(&strategy_arc));

        strategy_arc
    }

    /// Create strategy for a database
    fn create_strategy(&self, db: &str) -> Box<dyn DeduplicationStrategy> {
        match self.get_strategy_type(db) {
            StrategyType::HashSet => {
                debug!("Using HashSet strategy for database '{}'", db);
                Box::new(HashSetStrategy::new())
            }
            StrategyType::Bloom => {
                // Check for custom config
                let config = self.bloom_custom.get(db).unwrap_or(&self.bloom_config);

                debug!(
                    "Using Bloom filter strategy for database '{}' (expected: {}, FP rate: {})",
                    db, config.expected_series_per_day, config.false_positive_rate
                );

                Box::new(BloomFilterStrategy::new(
                    config.expected_series_per_day,
                    config.false_positive_rate,
                ))
            }
        }
    }

    /// Get total memory usage across all databases
    pub async fn total_memory_bytes(&self) -> usize {
        let strategies = self.strategies.read().await;
        let mut total = 0;
        for strategy_arc in strategies.values() {
            let strategy = strategy_arc.read().await;
            total += strategy.memory_bytes();
        }
        total
    }

    /// Get statistics per database
    pub async fn get_stats(&self) -> HashMap<String, StrategyStats> {
        let strategies = self.strategies.read().await;
        let mut stats = HashMap::new();

        for (db, strategy_arc) in strategies.iter() {
            let strategy = strategy_arc.read().await;
            stats.insert(
                db.clone(),
                StrategyStats {
                    strategy_type: strategy.name().to_string(),
                    memory_bytes: strategy.memory_bytes(),
                    memory_mb: strategy.memory_bytes() as f64 / 1024.0 / 1024.0,
                },
            );
        }

        stats
    }

    /// Clear old data from all strategies (for cache retention)
    pub async fn clear_before_date(&self, cutoff_date: i32) {
        let strategies = self.strategies.read().await;
        for strategy_arc in strategies.values() {
            let mut strategy = strategy_arc.write().await;
            strategy.clear_before_date(cutoff_date);
        }
        debug!("Cleared strategy data before date {}", cutoff_date);
    }
}

/// Statistics for a strategy
#[derive(Debug, Clone, serde::Serialize)]
pub struct StrategyStats {
    pub strategy_type: String,
    pub memory_bytes: usize,
    pub memory_mb: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashset_strategy() {
        let mut strategy = HashSetStrategy::new();

        // Insert some hashes
        strategy.insert("pmc", "db1", 100, 12345);
        strategy.insert("pmc", "db1", 100, 67890);
        strategy.insert("umc", "db2", 100, 12345);

        // Check contains
        assert!(strategy.contains("pmc", "db1", 100, 12345));
        assert!(strategy.contains("pmc", "db1", 100, 67890));
        assert!(strategy.contains("umc", "db2", 100, 12345));
        assert!(!strategy.contains("pmc", "db1", 100, 99999));
        assert!(!strategy.contains("pmc", "db1", 101, 12345));

        // Different schema should not match
        assert!(!strategy.contains("umc", "db1", 100, 12345));

        // Clear old data
        strategy.clear_before_date(101);
        assert!(!strategy.contains("pmc", "db1", 100, 12345));
    }

    #[test]
    fn test_bloom_strategy() {
        let mut strategy = BloomFilterStrategy::new(1000, 0.01);

        // Insert some hashes
        strategy.insert("pmc", "db1", 100, 12345);
        strategy.insert("pmc", "db1", 100, 67890);

        // Check contains (should be true for inserted items)
        assert!(strategy.contains("pmc", "db1", 100, 12345));
        assert!(strategy.contains("pmc", "db1", 100, 67890));

        // May have false positives, but no false negatives
        // So we can't reliably test for non-existence
    }

    #[tokio::test]
    async fn test_strategy_manager_selection() {
        let manager = StrategyManager::new(
            "hashset",
            vec!["production".to_string(), "metrics_*".to_string()],
            BloomConfig::default(),
            HashMap::new(),
        );

        // HashSet for normal database
        let strat1 = manager.get_or_create_strategy("dev").await;
        assert_eq!(strat1.read().await.name(), "hashset");

        // Bloom for exact match
        let strat2 = manager.get_or_create_strategy("production").await;
        assert_eq!(strat2.read().await.name(), "bloom_filter");

        // Bloom for wildcard match
        let strat3 = manager.get_or_create_strategy("metrics_us").await;
        assert_eq!(strat3.read().await.name(), "bloom_filter");
    }

    #[tokio::test]
    async fn test_strategy_manager_returns_same_instance() {
        let manager = Arc::new(StrategyManager::new(
            "hashset",
            vec![],
            BloomConfig::default(),
            HashMap::new(),
        ));

        // Get strategy twice for the same database
        let strat1 = manager.get_or_create_strategy("testdb").await;
        let strat2 = manager.get_or_create_strategy("testdb").await;

        // Verify they point to the same underlying instance (Arc pointer equality)
        assert!(Arc::ptr_eq(&strat1, &strat2), "Multiple calls should return the same Arc instance");

        // Insert into one
        {
            let mut s1 = strat1.write().await;
            s1.insert("pmc", "testdb", 100, 12345);
        }

        // Should be visible in the other (they share the same instance)
        {
            let s2 = strat2.read().await;
            assert!(s2.contains("pmc", "testdb", 100, 12345), "Updates should be visible across all Arc clones");
        }
    }

    #[tokio::test]
    async fn test_concurrent_strategy_access() {
        use tokio::task;

        let manager = Arc::new(StrategyManager::new(
            "hashset",
            vec![],
            BloomConfig::default(),
            HashMap::new(),
        ));

        // Spawn multiple concurrent tasks that all access the same database strategy
        let mut handles = vec![];

        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            let handle = task::spawn(async move {
                let strategy = manager_clone.get_or_create_strategy("concurrent_db").await;
                let mut strat = strategy.write().await;

                // Each task inserts a unique hash
                strat.insert("pmc", "concurrent_db", 100, 1000 + i);
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all insertions are visible in the shared strategy
        let strategy = manager.get_or_create_strategy("concurrent_db").await;
        let strat = strategy.read().await;

        for i in 0..10 {
            assert!(
                strat.contains("pmc", "concurrent_db", 100, 1000 + i),
                "Hash {} should be present after concurrent insert", 1000 + i
            );
        }
    }

    #[tokio::test]
    async fn test_concurrent_deduplication_prevents_duplicates() {
        use tokio::task;

        let manager = Arc::new(StrategyManager::new(
            "hashset",
            vec![],
            BloomConfig::default(),
            HashMap::new(),
        ));

        // Spawn multiple concurrent tasks that all try to insert the SAME hash
        let mut handles = vec![];
        let duplicate_count = Arc::new(tokio::sync::Mutex::new(0u32));

        for _ in 0..20 {
            let manager_clone = Arc::clone(&manager);
            let dup_count = Arc::clone(&duplicate_count);

            let handle = task::spawn(async move {
                let strategy = manager_clone.get_or_create_strategy("dedup_test").await;

                // Try to insert the same hash (simulating duplicate series)
                let mut strat = strategy.write().await;

                // Check if it already exists
                if !strat.contains("umc", "dedup_test", 100, 99999) {
                    strat.insert("umc", "dedup_test", 100, 99999);

                    // This simulates creating a new series record
                    let mut count = dup_count.lock().await;
                    *count += 1;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Only ONE task should have successfully inserted the hash (no duplicates)
        let final_count = *duplicate_count.lock().await;
        assert_eq!(
            final_count, 1,
            "Only 1 insertion should succeed, but got {} (indicates duplicate detection failed)",
            final_count
        );

        // Verify the hash exists exactly once
        let strategy = manager.get_or_create_strategy("dedup_test").await;
        let strat = strategy.read().await;
        assert!(strat.contains("umc", "dedup_test", 100, 99999));
    }

    #[tokio::test]
    async fn test_schema_isolation_pmc_vs_umc() {
        let manager = Arc::new(StrategyManager::new(
            "hashset",
            vec![],
            BloomConfig::default(),
            HashMap::new(),
        ));

        let strategy = manager.get_or_create_strategy("isolation_test").await;

        // Insert same hash for both PMC and UMC schemas
        {
            let mut strat = strategy.write().await;
            strat.insert("pmc", "isolation_test", 100, 77777);
            strat.insert("umc", "isolation_test", 100, 77777);
        }

        // Verify isolation: PMC and UMC are tracked separately
        {
            let strat = strategy.read().await;
            assert!(strat.contains("pmc", "isolation_test", 100, 77777));
            assert!(strat.contains("umc", "isolation_test", 100, 77777));

            // Insert new hash only in PMC
        }

        {
            let mut strat = strategy.write().await;
            strat.insert("pmc", "isolation_test", 100, 88888);
        }

        {
            let strat = strategy.read().await;
            assert!(strat.contains("pmc", "isolation_test", 100, 88888));
            assert!(!strat.contains("umc", "isolation_test", 100, 88888),
                "UMC should not contain hash that was only inserted into PMC");
        }
    }

    #[tokio::test]
    async fn test_bloom_filter_concurrent_access() {
        use tokio::task;

        let manager = Arc::new(StrategyManager::new(
            "bloom",
            vec!["*".to_string()], // Use bloom for all databases
            BloomConfig {
                expected_series_per_day: 10000,
                false_positive_rate: 0.01,
            },
            HashMap::new(),
        ));

        // Spawn concurrent tasks using bloom filter
        let mut handles = vec![];

        for i in 0..50 {
            let manager_clone = Arc::clone(&manager);
            let handle = task::spawn(async move {
                let strategy = manager_clone.get_or_create_strategy("bloom_db").await;
                let mut strat = strategy.write().await;

                // Each task inserts multiple hashes
                for j in 0..10 {
                    strat.insert("pmc", "bloom_db", 100, (i * 10 + j) as u64);
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify insertions
        let strategy = manager.get_or_create_strategy("bloom_db").await;
        let strat = strategy.read().await;

        assert_eq!(strat.name(), "bloom_filter");

        // Check that most hashes are found (bloom filter may have false positives, but no false negatives)
        let mut found_count = 0;
        for i in 0..50 {
            for j in 0..10 {
                if strat.contains("pmc", "bloom_db", 100, (i * 10 + j) as u64) {
                    found_count += 1;
                }
            }
        }

        // All 500 hashes should be found (bloom filters have no false negatives)
        assert_eq!(found_count, 500, "Bloom filter should find all inserted hashes (no false negatives)");
    }
}
