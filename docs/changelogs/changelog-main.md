# Changelog

## Configuration System Update

### Added
- **TOML Configuration System**: Full configuration management via `config.toml`
- **Environment-based Config**: Support for multiple config files (dev, prod, custom)
- **Config Module**: `src/config.rs` with structured configuration types
- **Default Values**: All settings have sensible defaults if config file is missing

### Changed
- **Hard-coded Values Removed**: All magic numbers converted to configuration
- **Ports Configurable**: HTTP (7000) and Flight (7001) ports now configurable
- **Data Directory Configurable**: Default `./data` can be changed to `/data0/data` or any path
- **Flush Intervals Configurable**: Cache (30s) and Series Tracker (60s) intervals
- **Compaction Settings**: All compaction thresholds are now configurable

### Configuration Files
```
config.toml              - Default development config
config.production.toml   - Production example with optimized settings
CONFIG.md                - Complete configuration guide
```

### Usage
```bash
# Default config
./stats

# Production config
CONFIG_PATH=config.production.toml ./stats

# Custom config
CONFIG_PATH=/etc/stats/config.toml ./stats
```

### Breaking Changes
None - The system uses defaults if no config file is provided, maintaining backward compatibility.

### Migration Guide

**Before:**
```rust
// Hard-coded values
let cache = DataCache::new("./data");
cache.clone().start_background_flush(30);
```

**After:**
```rust
// Config-driven
let config = Config::load_or_default("config.toml");
let cache = DataCache::new(&config.storage.data_dir);
cache.clone().start_background_flush(config.cache.flush_interval_secs);
```

### Configuration Structure

```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data0/data"

[cache]
flush_interval_secs = 30
max_size_mb = 2048

[compaction]
min_files = 10
max_file_size_mb = 200
target_file_size_mb = 1024
max_level = 7

[series_tracker]
flush_interval_secs = 60
```

See `CONFIG.md` for complete documentation.
