# Memory and CPU Profiling Guide

This guide covers memory allocation and CPU profiling for MynaDB using pprof-compatible tools.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [API Endpoints](#api-endpoints)
- [Output Formats](#output-formats)
- [Analysis Workflows](#analysis-workflows)
- [Scripts](#scripts)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

## Overview

MynaDB includes built-in profiling capabilities compatible with `go tool pprof`. This allows you to:

- **Analyze memory allocations** - Find memory leaks and optimize allocation patterns
- **Profile CPU usage** - Identify performance bottlenecks
- **Generate flamegraphs** - Visualize call stacks and resource usage
- **Compare profiles** - Track changes over time

### Features

- ✅ On-demand profiling (no constant overhead)
- ✅ Multiple output formats (protobuf, flamegraph, text)
- ✅ Compatible with `go tool pprof`
- ✅ Concurrent request protection
- ✅ Ready-to-use analysis scripts

## Quick Start

### 0. Quick Test (Recommended First Step)

```bash
# Verify profiling works with automatic load generation
./scripts/test_profile_with_load.sh

# Should show profile with samples and top functions
```

**Note:** Profiling requires active service load. An idle service will show zero samples.

### 1. Basic Heap Profile

```bash
# Get heap allocation profile (10 seconds)
curl "http://localhost:7000/debug/heap?duration=10&format=protobuf" -o heap.pb

# Analyze with go tool pprof
go tool pprof heap.pb
```

### 2. Interactive Web UI

```bash
# Capture profile
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o heap.pb

# Open interactive web interface
go tool pprof -http=:8080 heap.pb
```

Open browser at http://localhost:8080 to explore the profile interactively.

### 3. Quick Flamegraph

```bash
# Generate flamegraph directly
curl "http://localhost:7000/debug/heap?duration=10&format=flamegraph" -o heap.svg

# Open in browser
firefox heap.svg
```

### 4. Using Analysis Scripts

```bash
# Automated analysis with load generation
./scripts/analyze_memory.sh 30

# Simple profile fetch and analysis
./scripts/analyze_profile.sh heap 30

# Direct flamegraph generation
./scripts/generate_flamegraph.sh cpu 60
```

## API Endpoints

### Heap Profiling

**Endpoint:** `GET /debug/heap` or `GET /debug/profile/heap`

Profiles memory allocations over a specified duration.

**Query Parameters:**
- `duration` (integer, default: 10) - Profile duration in seconds (max: 300)
- `format` (string, default: "protobuf") - Output format: `protobuf`, `flamegraph`, or `text`
- `download` (boolean, default: false) - Add Content-Disposition header for download

**Examples:**

```bash
# Default: 10 seconds, protobuf format
curl "http://localhost:7000/debug/heap" -o heap.pb

# 30 seconds, text format
curl "http://localhost:7000/debug/heap?duration=30&format=text"

# Flamegraph with download header
curl "http://localhost:7000/debug/heap?duration=60&format=flamegraph&download=true" -o heap.svg
```

### CPU Profiling

**Endpoint:** `GET /debug/cpu` or `GET /debug/profile/cpu`

Profiles CPU usage by sampling thread stacks at 100Hz.

**Query Parameters:** Same as heap profiling

**Examples:**

```bash
# Default: 10 seconds, protobuf format
curl "http://localhost:7000/debug/cpu" -o cpu.pb

# 60 seconds, text format
curl "http://localhost:7000/debug/cpu?duration=60&format=text"

# Interactive analysis
curl "http://localhost:7000/debug/cpu?duration=30&format=protobuf" -o cpu.pb
go tool pprof -http=:8080 cpu.pb
```

## Output Formats

### 1. Protobuf Format (Recommended)

Binary format compatible with `go tool pprof`. Provides the most flexibility for analysis.

```bash
curl "http://localhost:7000/debug/heap?format=protobuf" -o heap.pb

# View top allocators
go tool pprof -top heap.pb

# Interactive web UI
go tool pprof -http=:8080 heap.pb

# Text report
go tool pprof -text heap.pb

# Generate flamegraph
go tool pprof -http=:8080 heap.pb
# (Select "Flame Graph" from VIEW menu)
```

### 2. Flamegraph Format

SVG flamegraph generated directly by the service.

```bash
curl "http://localhost:7000/debug/heap?format=flamegraph" -o heap.svg

# Open in browser
firefox heap.svg
google-chrome heap.svg
open heap.svg  # macOS
```

**Interpreting Flamegraphs:**
- **Width** = Time/samples spent (wider = more resources)
- **Height** = Call stack depth
- **Colors** = Random (no semantic meaning)
- **Click** = Zoom into function
- **Search** = Ctrl+F to find functions

### 3. Text Format

Human-readable report with top functions by sample count.

```bash
curl "http://localhost:7000/debug/heap?format=text"
```

**Example Output:**

```
Profile Report
═══════════════════════════════════════════════════

Top Functions by Sample Count:
───────────────────────────────────────────────────
Function                                              Samples
───────────────────────────────────────────────────
 1. alloc::alloc::alloc                                    1250
 2. arrow::array::builder::StringBuilder::append           890
 3. parquet::file::writer::SerializedFileWriter            654
 4. datafusion::execution::context::SessionContext         432
...
```

## Analysis Workflows

### Workflow 1: Find Memory Leaks

```bash
# Step 1: Capture baseline profile during idle
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o baseline.pb

# Step 2: Generate load and wait
# ... run your workload ...

# Step 3: Capture profile after load
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o after_load.pb

# Step 4: Compare profiles
go tool pprof -base=baseline.pb after_load.pb
```

**In pprof interactive mode:**
```
(pprof) top
(pprof) list <function_name>
(pprof) web
```

### Workflow 2: Identify CPU Hotspots

```bash
# Capture CPU profile during load
curl "http://localhost:7000/debug/cpu?duration=60&format=protobuf" -o cpu.pb

# Open interactive web UI
go tool pprof -http=:8080 cpu.pb
```

**In web UI:**
1. Select **VIEW → Flame Graph** for visualization
2. Select **VIEW → Top** for top functions
3. Use **SAMPLE** menu to focus on specific metrics
4. Click functions to drill down

### Workflow 3: Optimize Memory Allocations

```bash
# Step 1: Profile with load
./scripts/analyze_memory.sh 30

# Step 2: Identify top allocators
go tool pprof -top /tmp/heap_profile_*.pb

# Step 3: View source code for specific functions
go tool pprof -list=<function_name> /tmp/heap_profile_*.pb

# Step 4: Generate call graph
go tool pprof -pdf /tmp/heap_profile_*.pb > callgraph.pdf
```

### Workflow 4: Compare Before/After Changes

```bash
# Before optimization
curl "http://localhost:7000/debug/heap?duration=60&format=protobuf" -o before.pb

# Make your code changes and restart service
# ...

# After optimization
curl "http://localhost:7000/debug/heap?duration=60&format=protobuf" -o after.pb

# Compare (shows difference)
go tool pprof -base=before.pb after.pb -http=:8080
```

### Workflow 5: Continuous Monitoring

```bash
# Script to capture profiles every hour
cat > monitor_memory.sh << 'EOF'
#!/bin/bash
while true; do
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    curl -s "http://localhost:7000/debug/heap?duration=30&format=protobuf" \
         -o "/var/log/profiles/heap_${TIMESTAMP}.pb"
    sleep 3600  # 1 hour
done
EOF

chmod +x monitor_memory.sh
./monitor_memory.sh &
```

## Scripts

### analyze_profile.sh

Fetch and analyze profiles with `go tool pprof`.

**Usage:**
```bash
./scripts/analyze_profile.sh [ENDPOINT] [DURATION] [SERVER]
```

**Arguments:**
- `ENDPOINT` - Profile type: `heap` or `cpu` (default: heap)
- `DURATION` - Duration in seconds (default: 10)
- `SERVER` - Server URL (default: http://localhost:7000)

**Examples:**
```bash
# Heap profile, 30 seconds
./scripts/analyze_profile.sh heap 30

# CPU profile, 60 seconds
./scripts/analyze_profile.sh cpu 60

# Remote server
./scripts/analyze_profile.sh heap 30 http://production-server:7000
```

**Output:**
- Saves profile to `/tmp/profile_<type>_<timestamp>.pb`
- Displays text report with top functions
- Shows commands for further analysis

### generate_flamegraph.sh

Generate SVG flamegraphs directly.

**Usage:**
```bash
./scripts/generate_flamegraph.sh [ENDPOINT] [DURATION] [SERVER]
```

**Examples:**
```bash
# Heap flamegraph, 30 seconds
./scripts/generate_flamegraph.sh heap 30

# CPU flamegraph, 60 seconds
./scripts/generate_flamegraph.sh cpu 60
```

**Output:**
- Saves flamegraph to `/tmp/flamegraph_<type>_<timestamp>.svg`
- Shows instructions to open in browser

### analyze_memory.sh

Memory profiling with automated load generation.

**Usage:**
```bash
./scripts/analyze_memory.sh [DURATION] [SERVER]
```

**Examples:**
```bash
# 10 second profile with load
./scripts/analyze_memory.sh 10

# 60 second profile, remote server
./scripts/analyze_memory.sh 60 http://production-server:7000
```

**What it does:**
1. Writes test data (100 metrics)
2. Starts heap profiling
3. Generates query load during profiling
4. Analyzes results with `go tool pprof`
5. Displays recommendations

### test_profile_with_load.sh

Quick test to verify profiling works with load generation.

**Usage:**
```bash
./scripts/test_profile_with_load.sh [SERVER]
```

**Examples:**
```bash
# Test local server
./scripts/test_profile_with_load.sh

# Test remote server
./scripts/test_profile_with_load.sh http://production-server:7000
```

**What it does:**
1. Starts 5-second heap profiling
2. Generates writes, queries, and stats requests
3. Displays text format results
4. Verifies profiling captures active samples

**Use case:** Quick validation that profiling is working and capturing activity.

## Advanced Usage

### Custom Analysis Commands

#### Top Functions by Memory
```bash
go tool pprof -top heap.pb
go tool pprof -top20 heap.pb
```

#### List Source Code
```bash
# Show source for specific function
go tool pprof -list=arrow::array::builder heap.pb
```

#### Filter by Sample Type
```bash
# Show allocations only
go tool pprof -sample_index=alloc_space heap.pb

# Show allocation count
go tool pprof -sample_index=alloc_objects heap.pb
```

#### Export to Different Formats
```bash
# PDF call graph
go tool pprof -pdf heap.pb > callgraph.pdf

# SVG flamegraph
go tool pprof -svg heap.pb > flamegraph.svg

# PNG graph
go tool pprof -png heap.pb > graph.png

# DOT format
go tool pprof -dot heap.pb > graph.dot
```

#### Focus/Ignore Functions
```bash
# Focus on specific package
go tool pprof -focus=parquet heap.pb

# Ignore standard library
go tool pprof -ignore=std heap.pb
```

### Profiling During Specific Operations

#### During Compaction
```bash
# Trigger compaction
curl -X POST "http://localhost:7000/compact/mydb" &

# Profile immediately
sleep 1
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o compaction.pb

# Analyze
go tool pprof -http=:8080 compaction.pb
```

#### During Query Load
```bash
# Start profiling
curl "http://localhost:7000/debug/cpu?duration=60&format=protobuf" -o query_load.pb &

# Generate queries
for i in {1..1000}; do
    curl -s "http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 100" > /dev/null &
done

wait
go tool pprof -http=:8080 query_load.pb
```

#### During Data Ingestion
```bash
# Start profiling
curl "http://localhost:7000/debug/heap?duration=60&format=protobuf" -o ingestion.pb &

# Write data
for i in {1..10000}; do
    curl -s -X POST "http://localhost:7000/write/mydb" \
         -d "metric,tag=$i value=$i $(date +%s)000000000" &
done

wait
go tool pprof -http=:8080 ingestion.pb
```

### Comparing Multiple Profiles

```bash
# Collect profiles over time
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o profile_1.pb
sleep 600  # 10 minutes
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o profile_2.pb
sleep 600
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o profile_3.pb

# Compare first and last
go tool pprof -base=profile_1.pb profile_3.pb -http=:8080

# Show only growth
go tool pprof -base=profile_1.pb profile_3.pb -top
```

### Remote Profiling

```bash
# Profile production server
SERVER="http://production.example.com:7000"

# Heap profile
curl "${SERVER}/debug/heap?duration=30&format=protobuf" -o prod_heap.pb

# CPU profile
curl "${SERVER}/debug/cpu?duration=30&format=protobuf" -o prod_cpu.pb

# Analyze locally
go tool pprof -http=:8080 prod_heap.pb
```

### Automated Profiling Pipeline

```bash
#!/bin/bash
# automated_profiling.sh

SERVER="http://localhost:7000"
OUTPUT_DIR="/var/log/profiles"
RETENTION_DAYS=7

mkdir -p "$OUTPUT_DIR"

# Capture profile
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
HEAP_FILE="${OUTPUT_DIR}/heap_${TIMESTAMP}.pb"
CPU_FILE="${OUTPUT_DIR}/cpu_${TIMESTAMP}.pb"

echo "$(date): Capturing profiles..."
curl -s "${SERVER}/debug/heap?duration=30&format=protobuf" -o "$HEAP_FILE"
curl -s "${SERVER}/debug/cpu?duration=30&format=protobuf" -o "$CPU_FILE"

# Generate reports
go tool pprof -text "$HEAP_FILE" > "${OUTPUT_DIR}/heap_${TIMESTAMP}.txt"
go tool pprof -text "$CPU_FILE" > "${OUTPUT_DIR}/cpu_${TIMESTAMP}.txt"

# Cleanup old profiles
find "$OUTPUT_DIR" -name "*.pb" -mtime +${RETENTION_DAYS} -delete
find "$OUTPUT_DIR" -name "*.txt" -mtime +${RETENTION_DAYS} -delete

echo "$(date): Profiling complete"
```

## Troubleshooting

### Issue: "Profiling already in progress"

**Cause:** Another profiling request is active.

**Solution:**
```bash
# Wait for current profiling to complete
# Profiling duration is limited to 300 seconds (5 minutes)

# Or check service logs
tail -f /data0/mynadb/logs/mynadb.*.log | grep -i profile
```

### Issue: "Failed to start profiler"

**Cause:** System limitations or permissions.

**Solution:**
```bash
# Check system limits
ulimit -n

# Increase if needed
ulimit -n 65536

# Check process permissions
ps aux | grep mynadb
```

### Issue: Empty or zero-sample profiles

**Cause:** Not enough activity during profiling period. The pprof profiler samples active call stacks at 100Hz - if the service is idle (sleeping, waiting for requests), there are no active stacks to sample.

**Solution:**
```bash
# Quick test with automatic load generation
./scripts/test_profile_with_load.sh

# Use the full analysis script (recommended)
./scripts/analyze_memory.sh 30

# Or manually generate load while profiling
# Terminal 1: Start profiling
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o heap.pb &

# Terminal 2: Generate load immediately
for i in {1..1000}; do
    curl -s -X POST "http://localhost:7000/write/testdb" \
         -d "cpu,host=h${i} value=${i} $(date +%s)000000000" > /dev/null &
    curl -s "http://localhost:7000/query/testdb?sql=SELECT * FROM cpu" > /dev/null &
done

wait
go tool pprof -text heap.pb
```

**Key Points:**
- Profiling captures **active** CPU usage, not idle waiting
- Generate load **during** the profiling period
- Use scripts like `analyze_memory.sh` which automatically generate load
- On production systems, profile during peak traffic

### Issue: "go tool pprof not found"

**Cause:** Go is not installed.

**Solution:**
```bash
# Install Go from https://golang.org/dl/

# Ubuntu/Debian
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Verify
go version
```

### Issue: Profile file is very small (< 100 bytes)

**Cause:** Service is idle or profiling duration too short.

**Solution:**
```bash
# Generate activity and profile with longer duration
./scripts/analyze_memory.sh 60

# Or manually generate load while profiling
```

### Issue: Cannot access profiling endpoints

**Cause:** Authentication enabled and endpoints not public.

**Solution:**
```bash
# Check config.toml
cat config.toml | grep -A 5 "\[auth\]"

# Ensure profiling endpoints are in public_endpoints list:
public_endpoints = ["/health", "/stats", "/memory", "/debug/heap", "/debug/cpu"]

# Or use authentication
curl -u admin:password "http://localhost:7000/debug/heap?duration=10&format=protobuf" -o heap.pb
```

## Best Practices

### 1. Profile Duration

- **Short profiles (5-10s)**: Quick checks, low overhead
- **Medium profiles (30-60s)**: Standard analysis, good balance
- **Long profiles (120-300s)**: Detailed analysis, peak load scenarios

### 2. Timing Profiling

- Profile during **peak load** for realistic results
- Profile during **specific operations** (compaction, queries, writes)
- Profile **before and after** changes for comparison
- Use **consistent duration** for comparable results

### 3. Analysis Workflow

1. Start with **text format** for quick overview
2. Use **protobuf + pprof** for detailed analysis
3. Use **flamegraphs** for visualization
4. **Compare profiles** to track trends

### 4. Regular Monitoring

- Set up **automated profiling** (hourly or daily)
- Keep profiles for **trend analysis**
- **Compare** profiles over time
- Monitor for **memory leaks** (growing allocations)

### 5. Production Use

- **Limit duration** to avoid long-running profiling
- **Schedule during low traffic** if possible
- **Monitor impact** on service performance
- **Automate collection** and analysis

## Integration Examples

### Prometheus Integration

```bash
# Add to prometheus.yml
scrape_configs:
  - job_name: 'mynadb-profiles'
    scrape_interval: 1h
    static_configs:
      - targets: ['localhost:7000']
    metrics_path: '/debug/heap'
    params:
      duration: ['30']
      format: ['protobuf']
```

### Grafana Dashboard

Create alerts based on memory growth:

```promql
# Memory growth rate
rate(process_resident_memory_bytes[1h]) > 100000000
```

### CI/CD Pipeline

```yaml
# .gitlab-ci.yml
performance_test:
  script:
    - ./scripts/analyze_memory.sh 60
    - go tool pprof -top /tmp/heap_profile_*.pb
    - if [ $(go tool pprof -top /tmp/heap_profile_*.pb | grep -c "MB") -gt 100 ]; then exit 1; fi
```

## Additional Resources

- [Go pprof Documentation](https://pkg.go.dev/runtime/pprof)
- [go tool pprof Guide](https://github.com/google/pprof/blob/main/doc/README.md)
- [Flamegraph Interpretation](http://www.brendangregg.com/flamegraphs.html)
- [MynaDB Documentation](../CLAUDE.md)

## Summary

**Common Commands:**

```bash
# Quick heap analysis
./scripts/analyze_profile.sh heap 30

# Quick CPU analysis
./scripts/analyze_profile.sh cpu 60

# Memory profiling with load
./scripts/analyze_memory.sh 30

# Generate flamegraph
./scripts/generate_flamegraph.sh heap 30

# Interactive web UI
curl "http://localhost:7000/debug/heap?duration=30&format=protobuf" -o heap.pb
go tool pprof -http=:8080 heap.pb

# Compare profiles
go tool pprof -base=before.pb after.pb -http=:8080
```

**Key Endpoints:**

- Heap: `GET /debug/heap?duration=30&format=protobuf`
- CPU: `GET /debug/cpu?duration=30&format=protobuf`
- Formats: `protobuf` (default), `flamegraph`, `text`

**Remember:**

- Maximum duration: 300 seconds (5 minutes)
- Only one profile can run at a time
- Use protobuf format for go tool pprof
- Generate load during profiling for meaningful results
