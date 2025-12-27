# Arrow Flight

## Overview

The stats-service supports Apache Arrow Flight protocol on port 7001, enabling high-performance SQL queries from datafusion-cli and other Arrow clients.

## Quick Start

```bash
# Install datafusion-cli
cargo install datafusion-cli

# Connect
datafusion-cli --host localhost --port 7001

# Select database and query
> USE mydb;
> SELECT * FROM cpu LIMIT 10;
> SELECT host, AVG(usage) FROM cpu GROUP BY host;
```

## Python Example

```python
from pyarrow import flight

# Connect
client = flight.connect("grpc://localhost:7001")

# Set database
client.do_action(flight.Action("USE", b"mydb"))

# Query
ticket = flight.Ticket(b"SELECT * FROM cpu LIMIT 10")
reader = client.do_get(ticket)

for batch in reader:
    print(batch.to_pandas())
```

## Architecture

```
Clients                     Server
┌─────────────┐
│ HTTP client ├──:7000──┐
└─────────────┘          │   ┌─────────────┐
                         ├──►│ QueryEngine │──► Parquet files
┌─────────────┐          │   └─────────────┘
│datafusion-cli├──:7001──┘
└─────────────┘
```

## Features

| Feature | HTTP API | Arrow Flight |
|---------|----------|--------------|
| SQL queries | ✅ | ✅ |
| Write data | ✅ | ❌ |
| Streaming results | ❌ | ✅ |
| Zero-copy transfer | ❌ | ✅ |
| datafusion-cli | ❌ | ✅ |

## Configuration

```toml
[server]
http_port = 7000
flight_port = 7001
```

## Limitations

- Must issue `USE <database>` before queries
- Read-only (use HTTP API for writes)
- No authentication (use in trusted networks)
