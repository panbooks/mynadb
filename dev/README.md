# MynaDB Development Environment

This directory contains configuration files and scripts for running MynaDB locally in different modes.

## Prerequisites

- **Rust toolchain**: Install from https://rustup.rs
- **goreman** (for cluster mode): Process manager for Procfile-based apps

### Installing goreman

```bash
# Using Go
go install github.com/mattn/goreman@latest

# On macOS with Homebrew
brew install goreman

# On Linux, download from releases
# https://github.com/mattn/goreman/releases
```

## Quick Start

### Standalone Mode

Single-node MynaDB with all services:

```bash
./dev/start-standalone.sh
```

### Cluster Mode

Full cluster with 3 meta nodes, 1 router, and 2 data nodes:

```bash
./dev/start-cluster.sh
```

### Stop All

```bash
./dev/stop-all.sh
```

### Clean Data

Remove all data and logs:

```bash
./dev/clean.sh
```

## Port Assignments

### Cluster Mode

| Node | Role | gRPC/Internal | HTTP | Flight | MySQL |
|------|------|---------------|------|--------|-------|
| meta1 | Meta | 7100 | - | - | - |
| meta2 | Meta | 7101 | - | - | - |
| meta3 | Meta | 7102 | - | - | - |
| router1 | Router | - | 7000 | 7001 | 7002 |
| data1 | Data | 7200 | - | - | - |
| data2 | Data | 7201 | - | - | - |

### Standalone Mode

| Service | Port |
|---------|------|
| HTTP | 7000 |
| Flight | 7001 |
| MySQL | 7002 |

## Directory Structure

```
dev/
├── Procfile                # Default Procfile (cluster mode)
├── Procfile.cluster        # Full cluster configuration
├── Procfile.standalone     # Standalone configuration
├── configs/
│   ├── meta1.toml          # Meta node 1 config
│   ├── meta2.toml          # Meta node 2 config
│   ├── meta3.toml          # Meta node 3 config
│   ├── router1.toml        # Router node config
│   ├── data1.toml          # Data node 1 config
│   ├── data2.toml          # Data node 2 config
│   └── standalone.toml     # Standalone mode config
├── start-cluster.sh        # Start full cluster
├── start-standalone.sh     # Start standalone instance
├── stop-all.sh             # Stop all processes
├── clean.sh                # Clean data and logs
├── data/                   # Runtime data (gitignored)
├── logs/                   # Runtime logs (gitignored)
└── README.md               # This file
```

## Testing

### Health Check

```bash
# Standalone or Router health
curl http://localhost:7000/health
```

### Cluster Status (Cluster Mode)

```bash
curl http://localhost:7000/cluster/status
```

### Write Test Data

```bash
curl -X POST http://localhost:7000/write/testdb \
  -d "cpu,host=server1 value=0.5"
```

### Query Data

```bash
curl "http://localhost:7000/query/testdb?sql=SELECT%20*%20FROM%20cpu"
```

### MySQL Connection

```bash
mysql -h 127.0.0.1 -P 7002 -u admin testdb
```

## Manual Process Management

If you prefer to run nodes manually:

```bash
# Build first
cargo build --release

# Meta nodes (run in separate terminals)
./target/release/mynadb --role meta -c ./dev/configs/meta1.toml
./target/release/mynadb --role meta -c ./dev/configs/meta2.toml
./target/release/mynadb --role meta -c ./dev/configs/meta3.toml

# Wait for meta nodes to elect a leader, then:

# Router node
./target/release/mynadb --role router -c ./dev/configs/router1.toml

# Data nodes
./target/release/mynadb --role data -c ./dev/configs/data1.toml
./target/release/mynadb --role data -c ./dev/configs/data2.toml
```

## Configuration

Configuration files in `configs/` can be modified for testing different scenarios:

- **Meta nodes**: Raft consensus settings, heartbeat intervals
- **Router node**: Connection pooling, query routing
- **Data nodes**: Storage, caching, compaction settings

### Key Configuration Sections

| Section | Description |
|---------|-------------|
| `[meta]` | Meta node Raft settings |
| `[router]` | Router routing and pooling |
| `[data]` | Data node settings |
| `[storage]` | Data directory and object store |
| `[cache]` | Write cache settings |
| `[wal]` | Write-ahead log settings |
| `[compaction]` | Background compaction |
| `[logging]` | Log level and directory |

## Troubleshooting

### Port Already in Use

```bash
# Find process using port 7000
lsof -i :7000

# Or use the stop script
./dev/stop-all.sh
```

### goreman Not Found

Make sure `$GOPATH/bin` is in your PATH:

```bash
export PATH=$PATH:$(go env GOPATH)/bin
```

### Build Errors

```bash
# Clean and rebuild
cargo clean
cargo build --release
```

### Data Corruption

```bash
# Clean all data and start fresh
./dev/clean.sh
./dev/start-standalone.sh  # or start-cluster.sh
```
