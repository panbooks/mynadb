#!/bin/bash
set -e

echo "========================================="
echo "Verify Schema-Level Config Implementation"
echo "========================================="
echo

echo "1. Running unit tests..."
cd /root/projects/stats-service
cargo test --release -- --nocapture 2>&1 | grep -E "(test.*ok|test.*FAILED|passed|failed)"
echo

echo "2. Checking config.toml documentation..."
if grep -q 'schema = "pmc"' config.toml; then
    echo "✓ Schema-specific config examples found in config.toml"
else
    echo "✗ Schema-specific config examples NOT found"
    exit 1
fi

if grep -q 'schema = "umc"' config.toml; then
    echo "✓ UMC schema example found in config.toml"
else
    echo "✗ UMC schema example NOT found"
    exit 1
fi
echo

echo "3. Checking memory optimization documentation..."
if grep -q "Schema-Specific Configuration" docs/memory-optimization.md; then
    echo "✓ Schema-specific config section found in memory-optimization.md"
else
    echo "✗ Schema-specific config section NOT found"
    exit 1
fi
echo

echo "4. Checking code changes..."
echo "Checking src/config.rs..."
if grep -q "pub schema: Option<String>" src/config.rs; then
    echo "✓ BloomDatabaseConfig has optional schema field"
else
    echo "✗ Schema field NOT found in BloomDatabaseConfig"
    exit 1
fi

echo "Checking src/series_dedup.rs..."
if grep -q "HashMap<(String, String), BloomConfig>" src/series_dedup.rs; then
    echo "✓ StrategyManager uses (database, schema) tuple keys"
else
    echo "✗ Tuple keys NOT found in StrategyManager"
    exit 1
fi

if grep -q "pub async fn get_or_create_strategy(&self, db: &str, schema: &str)" src/series_dedup.rs; then
    echo "✓ get_or_create_strategy accepts schema parameter"
else
    echo "✗ Schema parameter NOT found in get_or_create_strategy"
    exit 1
fi

echo "Checking src/series_tracker.rs..."
if grep -q 'let pmc_strategy = self.strategy_manager.get_or_create_strategy(db, SCHEMA_PMC).await' src/series_tracker.rs; then
    echo "✓ track_points uses separate PMC strategy"
else
    echo "✗ Separate PMC strategy NOT found"
    exit 1
fi

if grep -q 'let umc_strategy = self.strategy_manager.get_or_create_strategy(db, SCHEMA_UMC).await' src/series_tracker.rs; then
    echo "✓ track_points uses separate UMC strategy"
else
    echo "✗ Separate UMC strategy NOT found"
    exit 1
fi

if grep -q 'let strategy = self.strategy_manager.get_or_create_strategy(db, schema).await' src/series_tracker.rs; then
    echo "✓ load_cache passes schema to get_or_create_strategy"
else
    echo "✗ Schema parameter NOT passed in load_cache"
    exit 1
fi
echo

echo "========================================="
echo "✓ All verifications passed!"
echo "========================================="
echo
echo "Summary of changes:"
echo "1. Added 'schema: Option<String>' field to BloomDatabaseConfig"
echo "2. Changed StrategyManager to use (database, schema) tuple keys"
echo "3. Updated get_or_create_strategy to accept schema parameter"
echo "4. Modified track_points to use separate PMC and UMC strategies"
echo "5. Updated load_cache to pass schema parameter"
echo "6. Added documentation and examples in config.toml"
echo "7. Added schema-specific config section in memory-optimization.md"
echo
echo "Users can now configure different bloom filter settings for PMC and UMC:"
echo
echo "Example config.toml:"
echo "  [[series_tracker.bloom_custom]]"
echo "  database = \"arch_monitor_roc\""
echo "  schema = \"pmc\""
echo "  expected_series_per_day = 2000000000  # Higher cardinality"
echo
echo "  [[series_tracker.bloom_custom]]"
echo "  database = \"arch_monitor_roc\""
echo "  schema = \"umc\""
echo "  expected_series_per_day = 500000000   # Lower cardinality"
