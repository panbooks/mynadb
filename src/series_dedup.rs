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
/// Each strategy tracks which series have been seen per (database, date) pair
pub trait DeduplicationStrategy: Send + Sync {
    /// Check if series hash exists for given database and date
    fn contains(&self, db: &str, date: i32, series_hash: u64) -> bool;

    /// Insert series hash for given database and date
    fn insert(&mut self, db: &str, date: i32, series_hash: u64);

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
    /// Map: (date, db) -> HashSet<series_hash>
    cache: HashMap<(i32, String), HashSet<u64>>,
}

impl HashSetStrategy {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl DeduplicationStrategy for HashSetStrategy {
    fn contains(&self, db: &str, date: i32, series_hash: u64) -> bool {
        self.cache
            .get(&(date, db.to_string()))
            .map(|set| set.contains(&series_hash))
            .unwrap_or(false)
    }

    fn insert(&mut self, db: &str, date: i32, series_hash: u64) {
        self.cache
            .entry((date, db.to_string()))
            .or_insert_with(HashSet::new)
            .insert(series_hash);
    }

    fn clear_before_date(&mut self, cutoff_date: i32) {
        self.cache.retain(|(date, _), _| *date >= cutoff_date);
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
    /// Map: (date, db) -> Bloom<u64>
    filters: HashMap<(i32, String), Bloom<u64>>,
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

    fn get_or_create_filter(&mut self, date: i32, db: &str) -> &mut Bloom<u64> {
        self.filters
            .entry((date, db.to_string()))
            .or_insert_with(|| {
                Bloom::new_for_fp_rate(self.expected_items, self.false_positive_rate)
            })
    }
}

impl DeduplicationStrategy for BloomFilterStrategy {
    fn contains(&self, db: &str, date: i32, series_hash: u64) -> bool {
        self.filters
            .get(&(date, db.to_string()))
            .map(|bloom| bloom.check(&series_hash))
            .unwrap_or(false)
    }

    fn insert(&mut self, db: &str, date: i32, series_hash: u64) {
        let bloom = self.get_or_create_filter(date, db);
        bloom.set(&series_hash);
    }

    fn clear_before_date(&mut self, cutoff_date: i32) {
        self.filters.retain(|(date, _), _| *date >= cutoff_date);
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
    strategies: Arc<RwLock<HashMap<String, Box<dyn DeduplicationStrategy>>>>,
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
            strategies: Arc::new(RwLock::new(HashMap::new())),
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
    pub async fn get_or_create_strategy(&self, db: &str) -> Arc<RwLock<Box<dyn DeduplicationStrategy>>> {
        // Quick check with read lock
        {
            let strategies = self.strategies.read().await;
            if let Some(_) = strategies.get(db) {
                // Strategy exists, but we can't return it from read lock
                // Fall through to get it with proper Arc wrapping
            }
        }

        // Get or create with write lock
        let mut strategies = self.strategies.write().await;

        if !strategies.contains_key(db) {
            let strategy = self.create_strategy(db);
            info!(
                "Created {} strategy for database '{}' (estimated memory: {:.2}MB)",
                strategy.name(),
                db,
                strategy.memory_bytes() as f64 / 1024.0 / 1024.0
            );
            strategies.insert(db.to_string(), strategy);
        }

        // Return wrapped reference
        let strategy = strategies.get(db).unwrap();
        Arc::new(RwLock::new(strategy.clone_box()))
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
        strategies.values().map(|s| s.memory_bytes()).sum()
    }

    /// Get statistics per database
    pub async fn get_stats(&self) -> HashMap<String, StrategyStats> {
        let strategies = self.strategies.read().await;
        strategies
            .iter()
            .map(|(db, strategy)| {
                (
                    db.clone(),
                    StrategyStats {
                        strategy_type: strategy.name().to_string(),
                        memory_bytes: strategy.memory_bytes(),
                        memory_mb: strategy.memory_bytes() as f64 / 1024.0 / 1024.0,
                    },
                )
            })
            .collect()
    }

    /// Clear old data from all strategies (for cache retention)
    pub async fn clear_before_date(&self, cutoff_date: i32) {
        let mut strategies = self.strategies.write().await;
        for strategy in strategies.values_mut() {
            strategy.clear_before_date(cutoff_date);
        }
        debug!("Cleared strategy data before date {}", cutoff_date);
    }
}

/// Statistics for a strategy
#[derive(Debug, Clone)]
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
        strategy.insert("db1", 100, 12345);
        strategy.insert("db1", 100, 67890);
        strategy.insert("db2", 100, 12345);

        // Check contains
        assert!(strategy.contains("db1", 100, 12345));
        assert!(strategy.contains("db1", 100, 67890));
        assert!(strategy.contains("db2", 100, 12345));
        assert!(!strategy.contains("db1", 100, 99999));
        assert!(!strategy.contains("db1", 101, 12345));

        // Clear old data
        strategy.clear_before_date(101);
        assert!(!strategy.contains("db1", 100, 12345));
    }

    #[test]
    fn test_bloom_strategy() {
        let mut strategy = BloomFilterStrategy::new(1000, 0.01);

        // Insert some hashes
        strategy.insert("db1", 100, 12345);
        strategy.insert("db1", 100, 67890);

        // Check contains (should be true for inserted items)
        assert!(strategy.contains("db1", 100, 12345));
        assert!(strategy.contains("db1", 100, 67890));

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
}
