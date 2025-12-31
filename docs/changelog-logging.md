# Logging System Implementation

## Overview
Implemented comprehensive logging system using `tracing` and `tracing-subscriber` crates. All operational logs are now written to rotating log files, while only welcome/startup messages are displayed on stdout.

## Changes Made

### 1. Dependencies Added (`Cargo.toml`)
```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-appender = "0.2"
```

### 2. New Logging Module (`src/logger.rs`)
- **`init_logging(config: &LoggingConfig)`**: Initializes the logging system with configuration
- **Features**:
  - File-based logging with rotation (daily, hourly, or never)
  - Configurable log levels (trace, debug, info, warn, error)
  - Optional JSON formatting
  - Non-blocking writes for better performance
  - Automatic log file management

- **`welcome!` macro**: Special macro for stdout messages (startup banner only)

### 3. Configuration Updates (`src/config.rs`)
Added `LoggingConfig` struct with fields:
- `level`: Log level (default: "info")
- `log_dir`: Directory for log files (default: "./logs")
- `file_prefix`: Log file name prefix (default: "stats-service")
- `rotation`: Rotation strategy - daily, hourly, never (default: "daily")
- `max_files`: Maximum log files to retain (default: 30)
- `json_format`: Enable JSON logs (default: false)

### 4. Updated Files with Logging

#### `src/main.rs`
- Initialize logging before any other operations
- Welcome banner printed to stdout only
- All HTTP handlers now use `tracing` macros
- Server startup/shutdown messages logged

#### `src/query_engine.rs`
- Catalog/schema creation: `info!`
- Table registration: `debug!`
- SQL queries: `debug!`
- Registration failures: `warn!`
- Auto-registration count: `info!`

#### `src/series_tracker.rs`
- Background flush start: `info!`
- Flush operations: `info!` (with count)
- Flush errors: `error!`
- Cache loading: `info!`
- File corruption detection: `warn!`, `error!`
- Temp file cleanup: `info!`, `warn!`

#### `src/cache.rs`
- Background flush start: `info!`
- Cache statistics: `debug!`
- Flush operations: `info!`, `debug!`
- Write errors: `error!`
- RecordBatch conversion errors: `error!`

#### `src/compactor.rs` (To be updated)
- Compaction start/complete: `info!`
- File operations: `debug!`
- Compaction errors: `warn!`, `error!`

## Log Levels Usage

| Level | Usage |
|-------|-------|
| `error!` | Fatal errors, corruption detection, failed operations |
| `warn!` | Recoverable errors, missing files, failed registrations |
| `info!` | Service lifecycle, background tasks, important operations |
| `debug!` | Detailed operation info, SQL queries, file operations |
| `trace!` | Very detailed debugging (not currently used) |

## Log File Structure

```
logs/
├── stats-service.2024-01-24    # Daily rotation
├── stats-service.2024-01-25
└── stats-service.2024-01-26
```

With hourly rotation:
```
logs/
├── stats-service.2024-01-24-00
├── stats-service.2024-01-24-01
└── ...
```

## Configuration Example

See `config.toml.example` for a complete configuration example.

```toml
[logging]
level = "info"
log_dir = "./logs"
file_prefix = "stats-service"
rotation = "daily"
max_files = 30
json_format = false
```

## Environment Variable Override

You can override the log level using the `RUST_LOG` environment variable:
```bash
RUST_LOG=debug cargo run
```

## Benefits

1. **Production Ready**: All operational logs go to files, stdout remains clean
2. **Rotation**: Automatic log rotation prevents disk space issues
3. **Performance**: Non-blocking writes don't impact request latency
4. **Configurable**: Easy to adjust log levels and formats
5. **Structured**: Optional JSON format for log aggregation tools
6. **Debugging**: Debug and trace levels for troubleshooting
7. **Monitoring**: Clear separation of log levels for alerting

## Stdout Output (Welcome Messages Only)

```
═══════════════════════════════════════════════════════
   📊 Stats Service - Time Series Database
═══════════════════════════════════════════════════════

🚀 Starting server...
   HTTP API: http://0.0.0.0:7000
   Flight API: 0.0.0.0:7001
   Data Directory: ./data
   Log Directory: ./logs
   Log Level: info

✅ Server started successfully!
═══════════════════════════════════════════════════════
```

All other messages are in log files!
