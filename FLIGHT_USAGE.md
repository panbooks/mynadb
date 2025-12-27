# Arrow Flight Integration Guide

## Overview

The stats-service now supports **Apache Arrow Flight** protocol, allowing you to connect using `datafusion-cli` and other Arrow-based SQL clients.

## What Was Added

### 1. Dependencies (Cargo.toml)
```toml
arrow-flight = "53"
tonic = "0.12"
futures = "0.3"
tokio-stream = "0.1"
```

### 2. Flight Service Implementation
- **File**: `src/flight_service.rs`
- **Port**: `7001` (gRPC)
- **Features**:
  - Database selection via `USE <database>` command
  - SQL query execution via Arrow Flight protocol
  - Schema introspection
  - Streaming query results

### 3. Server Configuration
The server now runs **two services simultaneously**:
- **HTTP REST API**: `http://0.0.0.0:7000`
- **Arrow Flight API**: `grpc://0.0.0.0:7001`

## Usage Examples

### 1. Using datafusion-cli

```bash
# Install datafusion-cli
cargo install datafusion-cli

# Connect to the Flight server
datafusion-cli --host localhost --port 7001

# Select database
> USE mydb;

# Run queries
> SELECT * FROM cpu LIMIT 10;
> SELECT host, AVG(usage) FROM cpu GROUP BY host;
> SELECT COUNT(*) FROM cpu WHERE time > 1234567890;
```

### 2. Using Python with pyarrow

```python
from pyarrow import flight

# Connect to Flight server
client = flight.connect("grpc://localhost:7001")

# Set current database
client.do_action(flight.Action("USE", b"mydb"))

# Execute query
query = "SELECT * FROM cpu LIMIT 10"
ticket = flight.Ticket(query.encode())
reader = client.do_get(ticket)

# Read results
for batch in reader:
    print(batch.to_pandas())
```

### 3. Using HTTP REST API (existing)

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
```

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ├──────HTTP (7000)──────┐
       │                        │
       └──────gRPC (7001)───────┤
                                │
                         ┌──────▼──────┐
                         │   Server    │
                         ├─────────────┤
                         │  HTTP API   │
                         │  Flight API │
                         ├─────────────┤
                         │ QueryEngine │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │  Parquet    │
                         │   Storage   │
                         └─────────────┘
```

## Benefits

1. **Standard Protocol**: Arrow Flight is an industry-standard protocol for analytics
2. **High Performance**: Zero-copy data transfer with columnar format
3. **Language Support**: Available in Python, Java, C++, Go, Rust, etc.
4. **Tool Compatibility**: Works with datafusion-cli, DBeaver, and other Arrow-enabled tools
5. **Streaming**: Efficient handling of large result sets

## Limitations

- **Database Context**: You must issue `USE <database>` before running queries
- **Read-Only**: Write operations via Flight are not supported (use HTTP API)
- **No Authentication**: Currently no auth/encryption (suitable for internal networks)

## Performance Comparison

| Method | Use Case | Performance |
|--------|----------|-------------|
| HTTP REST | Simple queries, web apps | Good |
| Arrow Flight | Analytics, large datasets, BI tools | Excellent |
| Direct Parquet | Batch processing | Best |

## Next Steps

Consider adding:
1. **Authentication**: TLS + token-based auth
2. **Multi-tenancy**: User-based access control
3. **Flight SQL**: Full SQL support via Flight SQL extension
4. **Prepared Statements**: Query plan caching
