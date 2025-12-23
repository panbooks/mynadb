# Code Standards and Refactoring Guide

## Overview
This document outlines code quality standards and refactoring improvements applied to the Stats Service codebase to ensure maintainability, consistency, and reliability.

## Refactoring: Series Tracker Constants

### Summary
Refactored `src/series_tracker.rs` to eliminate hard-coded string literals and use centralized constants for better maintainability and consistency.

### Constants Defined
```rust
// System directory and file constants
const SYSTEM_DIR: &str = "_system";
const SERIES_STATS_DIR: &str = "series_stats";
const SERIES_STATS_FILE_EXT: &str = "parquet";
const MONITOR_SERVICE_TAG: &str = "MONITOR.service";
const DEFAULT_SERVICE_NAME: &str = "unknown";
```

### Helper Methods
```rust
/// Get the series stats directory path
fn get_stats_dir(&self) -> PathBuf {
    Path::new(&self.data_dir)
        .join(SYSTEM_DIR)
        .join(SERIES_STATS_DIR)
}

/// Get the series stats file path for a database
fn get_stats_file_path(&self, db: &str) -> PathBuf {
    self.get_stats_dir()
        .join(format!("{}.{}", db, SERIES_STATS_FILE_EXT))
}
```

### Before & After

#### Before:
```rust
let stats_dir = format!("{}/_system/series_stats", self.data_dir);
let file_path = format!("{}/{}.parquet", stats_dir, db);

let service = point.tags.iter()
    .find(|(k, _)| *k == "MONITOR.service")
    .map(|(_, v)| v.clone())
    .unwrap_or_else(|| "unknown".to_string());
```

#### After:
```rust
let stats_dir = self.get_stats_dir();
let file_path = self.get_stats_file_path(db);

let service = point.tags.iter()
    .find(|(k, _)| *k == MONITOR_SERVICE_TAG)
    .map(|(_, v)| v.clone())
    .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());
```

## Code Quality Principles

### 1. No Hard-Coded Strings
**Bad:**
```rust
let path = format!("{}/_system/data", dir);
let tag = "MONITOR.service";
```

**Good:**
```rust
const SYSTEM_DIR: &str = "_system";
const DATA_DIR: &str = "data";
const MONITOR_TAG: &str = "MONITOR.service";

let path = Path::new(dir).join(SYSTEM_DIR).join(DATA_DIR);
let tag = MONITOR_TAG;
```

### 2. Use PathBuf for Paths
**Bad:**
```rust
let path = format!("{}/{}/{}", base, dir, file);
```

**Good:**
```rust
let path = Path::new(base).join(dir).join(file);
```

**Benefits:**
- Platform-independent (handles `/` vs `\`)
- Type-safe
- Prevents common path errors

### 3. Single Source of Truth
**Bad:**
```rust
// In file1.rs
let dir = "./data/system";

// In file2.rs
let dir = "./data/system";  // Risk of typo!
```

**Good:**
```rust
// In constants.rs or at module top
const DATA_SYSTEM_DIR: &str = "./data/system";

// Used everywhere
let dir = DATA_SYSTEM_DIR;
```

### 4. Meaningful Constant Names
**Bad:**
```rust
const D: &str = "data";
const S: &str = "_system";
const F: &str = ".parquet";
```

**Good:**
```rust
const DATA_DIR: &str = "data";
const SYSTEM_DIR: &str = "_system";
const PARQUET_EXT: &str = ".parquet";
```

### 5. Helper Methods for Complex Operations
**Bad:**
```rust
// Repeated in multiple places
let path = format!("{}/_system/stats/{}.parquet", data_dir, db);
```

**Good:**
```rust
fn get_stats_file_path(&self, db: &str) -> PathBuf {
    Path::new(&self.data_dir)
        .join(SYSTEM_DIR)
        .join(STATS_DIR)
        .join(format!("{}.{}", db, PARQUET_EXT))
}

// Usage
let path = self.get_stats_file_path(db);
```

## Standards by Module

### Cache Module (`cache.rs`)
- Define flush interval as constant
- Use PathBuf for file paths
- Centralize measurement directory logic

### Compactor Module (`compactor.rs`)
- Define level prefixes as constants (`L0_`, `L1_`, etc.)
- Use constants for file size thresholds
- Define compaction configuration as struct

### Database Manager (`database_manager.rs`)
- Define config file name as constant
- Centralize database path construction
- Use PathBuf for all paths

### Parquet Writer (`parquet_writer.rs`)
- Define schema field names as constants
- Use constants for file extensions
- Centralize filename generation

### Query Engine (`query_engine.rs`)
- Define table name separator as constant
- Use constants for SQL keywords
- Centralize table registration logic

## Benefits Achieved

### ✅ Maintainability
- Single source of truth for system paths and special tags
- Easy to change directory structure or file extensions
- No risk of typos in string literals scattered across code

### ✅ Consistency
- All file paths constructed using same method
- Guaranteed consistency across all operations (read, write, check existence)

### ✅ Type Safety
- Using `PathBuf` instead of string concatenation
- Platform-independent path handling
- Compile-time checking of path construction

### ✅ Readability
- Clear intent with named constants
- Self-documenting code
- Helper methods with descriptive names

### ✅ Testability
- Constants can be referenced in tests
- Consistent behavior between production and test code
- Easy to mock or override in tests

## Implementation Checklist

When adding new features or refactoring:

- [ ] Identify all string literals in the code
- [ ] Extract repeated strings to constants
- [ ] Use `PathBuf` for all file system paths
- [ ] Create helper methods for complex path construction
- [ ] Update tests to use constants
- [ ] Document constants with comments
- [ ] Ensure backward compatibility
- [ ] Run tests to verify behavior unchanged

## Example: Adding a New System Directory

**Step 1: Define Constants**
```rust
const BACKUP_DIR: &str = "backups";
const BACKUP_FILE_EXT: &str = "bak";
```

**Step 2: Create Helper Methods**
```rust
fn get_backup_dir(&self) -> PathBuf {
    Path::new(&self.data_dir)
        .join(SYSTEM_DIR)
        .join(BACKUP_DIR)
}

fn get_backup_file_path(&self, name: &str) -> PathBuf {
    self.get_backup_dir()
        .join(format!("{}.{}", name, BACKUP_FILE_EXT))
}
```

**Step 3: Use in Code**
```rust
let backup_path = self.get_backup_file_path("mydb");
std::fs::create_dir_all(self.get_backup_dir())?;
```

## Related Documentation

- [Architecture Overview](./architecture-directory-structure.md) - Project structure
- [Feature: Series Cardinality](./features-series-cardinality.md) - Series tracking details
- [Parquet Writer API](./architecture-parquet-writer.md) - File writing standards
- [DataFusion Integration](./architecture-datafusion-integration.md) - Query engine

## Testing

All refactoring changes maintain backward compatibility and pass existing tests:

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test --lib series_tracker
cargo test --lib cache
cargo test --lib compactor
```

## Backward Compatibility

All refactoring is **fully backward compatible**:
- ✅ No changes to public API
- ✅ Same file paths and directory structure
- ✅ Existing data files work without migration
- ✅ Same behavior in all edge cases

## Summary

These code standards ensure the Stats Service codebase remains:
- **Maintainable**: Easy to update and modify
- **Consistent**: Same patterns used throughout
- **Safe**: Type-checked paths and operations
- **Readable**: Self-documenting code
- **Testable**: Easy to verify correctness

Following these standards reduces bugs, improves developer experience, and makes the codebase more professional and production-ready.
