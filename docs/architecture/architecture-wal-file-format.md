# WAL File Format Specification

## Overview

The Write-Ahead Log (WAL) uses a custom binary format optimized for sequential writes and crash recovery. Each WAL file contains a sequence of length-prefixed entries with CRC32 checksums for data integrity.

## File Organization

### Directory Structure

```
data/
└── _wal/                          # WAL root directory
    ├── data_1703001234.wal       # Data write log (timestamp in filename)
    ├── data_1703005678.wal       # Rotated data log
    ├── series_1703001234.wal     # Series tracking log
    └── series_1703005678.wal     # Rotated series log
```

### File Naming Convention

**Format:** `{prefix}_{timestamp}.wal`

- **prefix**: Either `data` or `series`
  - `data`: Contains line protocol write operations
  - `series`: Contains series metadata tracking operations
- **timestamp**: Unix timestamp (seconds since epoch) when file was created
- **extension**: `.wal` (Write-Ahead Log)

**Examples:**
- `data_1703001234.wal` - Created at 2023-12-19 17:13:54 UTC
- `series_1703005678.wal` - Created at 2023-12-19 18:27:58 UTC

### File Lifecycle

1. **Creation**: New WAL file created on first write or after rotation
2. **Active**: File receives append-only writes until rotation threshold
3. **Rotation**: When file size exceeds `rotate_size_mb`, new file is created
4. **Retention**: Files kept for `retention_hours` after creation
5. **Cleanup**: Background task deletes files older than retention period

## Binary Format

### File Structure

A WAL file consists of a sequence of length-prefixed entries:

```
[File]
├── Entry 1: [Length Prefix: 4 bytes] [Entry Data: N bytes]
├── Entry 2: [Length Prefix: 4 bytes] [Entry Data: M bytes]
├── Entry 3: [Length Prefix: 4 bytes] [Entry Data: P bytes]
└── ...
```

### Entry Structure

Each entry has the following binary layout:

```
[Entry with Length Prefix]
┌─────────────────┬──────────────────────────────────────────────────┐
│ Length Prefix   │ Entry Data                                       │
│ (4 bytes)       │ (variable length)                                │
└─────────────────┴──────────────────────────────────────────────────┘
```

**Entry Data Layout:**

```
┌──────────────┬─────────────┬────────────┬─────────────┬──────────────┬─────────────┬─────────────┐
│  Timestamp   │ Entry Type  │  DB Name   │  DB Name    │  Payload     │   Payload   │  Checksum   │
│  (8 bytes)   │  (1 byte)   │  Length    │  (variable) │  Length      │  (variable) │  (4 bytes)  │
│              │             │  (4 bytes) │             │  (4 bytes)   │             │             │
└──────────────┴─────────────┴────────────┴─────────────┴──────────────┴─────────────┴─────────────┘
   u64 LE         u8           u32 LE       UTF-8         u32 LE         bytes         u32 LE
```

### Field Specifications

#### 1. Length Prefix (4 bytes)
- **Type**: `u32` (unsigned 32-bit integer)
- **Encoding**: Little-endian
- **Purpose**: Length of the following entry data in bytes
- **Range**: 0 to 4,294,967,295 bytes
- **Note**: Does NOT include the 4-byte length prefix itself

#### 2. Timestamp (8 bytes)
- **Type**: `u64` (unsigned 64-bit integer)
- **Encoding**: Little-endian
- **Unit**: Nanoseconds since Unix epoch (1970-01-01 00:00:00 UTC)
- **Purpose**: Precise timestamp of when entry was logged
- **Example**: `1703001234567890123` = 2023-12-19 17:13:54.567890123 UTC

#### 3. Entry Type (1 byte)
- **Type**: `u8` (unsigned 8-bit integer)
- **Values**:
  - `0x00`: Data write (line protocol)
  - `0x01`: Series tracking (metadata)
- **Purpose**: Discriminates between different operation types
- **Future**: Reserved values `0x02-0xFF` for future use

#### 4. Database Name Length (4 bytes)
- **Type**: `u32` (unsigned 32-bit integer)
- **Encoding**: Little-endian
- **Purpose**: Length of database name in bytes
- **Range**: 1 to 4,294,967,295 bytes (practical limit: 255 bytes)

#### 5. Database Name (variable)
- **Type**: UTF-8 encoded string
- **Length**: Specified by previous field
- **Purpose**: Name of the database this operation belongs to
- **Example**: `"mydb"`, `"production"`, `"metrics_db"`
- **Validation**: Must be valid UTF-8

#### 6. Payload Length (4 bytes)
- **Type**: `u32` (unsigned 32-bit integer)
- **Encoding**: Little-endian
- **Purpose**: Length of payload data in bytes
- **Range**: 0 to 4,294,967,295 bytes

#### 7. Payload (variable)
- **Type**: Binary data
- **Length**: Specified by previous field
- **Content**: Depends on entry type:
  - **Data write** (`0x00`): Raw line protocol string (UTF-8)
  - **Series tracking** (`0x01`): Bincode-serialized `Vec<SeriesRecord>`

#### 8. Checksum (4 bytes)
- **Type**: `u32` (unsigned 32-bit integer)
- **Encoding**: Little-endian
- **Algorithm**: CRC32 (polynomial: 0xEDB88320)
- **Coverage**: All bytes from timestamp through payload (excludes length prefix)
- **Purpose**: Detect data corruption
- **Library**: `crc32fast` crate

## Entry Types

### Type 0x00: Data Write

**Purpose**: Logs raw line protocol data before processing

**Payload Format**: UTF-8 string containing InfluxDB line protocol

**Example Payload:**
```
cpu,host=server01,region=us-west usage=23.5,temp=45i 1672531200000000000
memory,host=server01 usage=85.5 1672531200000000000
```

**Use Case**: Replay writes on crash recovery

### Type 0x01: Series Tracking

**Purpose**: Logs new series metadata for cardinality tracking

**Payload Format**: Bincode-serialized `Vec<SeriesRecord>`

**SeriesRecord Structure:**
```rust
struct SeriesRecord {
    service: String,  // Schema/service name
    metric: String,   // Measurement name
    hash: u64,        // Tag combination hash
    date: i32,        // Date32 (days since Unix epoch)
}
```

**Example (conceptual):**
```json
[
  {
    "service": "production",
    "metric": "cpu",
    "hash": 12345678901234567890,
    "date": 19358
  }
]
```

**Use Case**: Rebuild series cardinality cache after crash

## Encoding Process

### Step-by-Step Encoding

```rust
// 1. Create entry
let entry = WalEntry {
    timestamp: 1703001234567890123,
    entry_type: EntryType::DataWrite,
    db_name: "mydb".to_string(),
    payload: b"cpu,host=server01 usage=23.5".to_vec(),
};

// 2. Encode entry to bytes
let mut buf = Vec::new();

// Timestamp (8 bytes, little-endian)
buf.extend_from_slice(&entry.timestamp.to_le_bytes());

// Entry type (1 byte)
buf.push(entry.entry_type as u8);

// DB name length (4 bytes, little-endian)
let db_bytes = entry.db_name.as_bytes();
buf.extend_from_slice(&(db_bytes.len() as u32).to_le_bytes());

// DB name (variable)
buf.extend_from_slice(db_bytes);

// Payload length (4 bytes, little-endian)
buf.extend_from_slice(&(entry.payload.len() as u32).to_le_bytes());

// Payload (variable)
buf.extend_from_slice(&entry.payload);

// Checksum (4 bytes, little-endian)
let checksum = crc32fast::hash(&buf);
buf.extend_from_slice(&checksum.to_le_bytes());

// 3. Write length prefix + data to file
let entry_len = buf.len() as u32;
file.write_all(&entry_len.to_le_bytes())?;
file.write_all(&buf)?;
```

## Decoding Process

### Step-by-Step Decoding

```rust
// 1. Read length prefix
let mut len_buf = [0u8; 4];
file.read_exact(&mut len_buf)?;
let entry_len = u32::from_le_bytes(len_buf) as usize;

// 2. Read entry data
let mut entry_buf = vec![0u8; entry_len];
file.read_exact(&mut entry_buf)?;

// 3. Parse entry fields
let mut pos = 0;

// Timestamp (8 bytes)
let timestamp = u64::from_le_bytes(entry_buf[pos..pos+8].try_into()?);
pos += 8;

// Entry type (1 byte)
let entry_type = match entry_buf[pos] {
    0 => EntryType::DataWrite,
    1 => EntryType::SeriesTrack,
    _ => return Err("Invalid entry type"),
};
pos += 1;

// DB name length (4 bytes)
let db_len = u32::from_le_bytes(entry_buf[pos..pos+4].try_into()?) as usize;
pos += 4;

// DB name (variable)
let db_name = String::from_utf8(entry_buf[pos..pos+db_len].to_vec())?;
pos += db_len;

// Payload length (4 bytes)
let payload_len = u32::from_le_bytes(entry_buf[pos..pos+4].try_into()?) as usize;
pos += 4;

// Payload (variable)
let payload = entry_buf[pos..pos+payload_len].to_vec();
pos += payload_len;

// Checksum (4 bytes)
let stored_checksum = u32::from_le_bytes(entry_buf[pos..pos+4].try_into()?);

// 4. Verify checksum
let computed_checksum = crc32fast::hash(&entry_buf[0..pos]);
if stored_checksum != computed_checksum {
    return Err("Checksum mismatch");
}
```

## Example: Complete Entry

### Example Data Write Entry

**Input:**
- Database: `"production"`
- Line Protocol: `"cpu,host=web01 usage=42.5 1703001234000000000"`

**Binary Representation (Hex Dump):**

```
Offset(h)  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F

# Length Prefix (4 bytes)
00000000   49 00 00 00                                      I...

# Timestamp (8 bytes): 1703001234567890123
00000004   CB 35 1A 4E B9 A1 9D 17                         Ë5.Nº¡..

# Entry Type (1 byte): 0x00 (DataWrite)
0000000C   00                                               .

# DB Name Length (4 bytes): 10
0000000D   0A 00 00 00                                      ....

# DB Name (10 bytes): "production"
00000011   70 72 6F 64 75 63 74 69 6F 6E                   production

# Payload Length (4 bytes): 39
0000001B   27 00 00 00                                      '...

# Payload (39 bytes): "cpu,host=web01 usage=42.5 1703001234000000000"
0000001F   63 70 75 2C 68 6F 73 74 3D 77 65 62 30 31 20 75  cpu,host=web01 u
0000002F   73 61 67 65 3D 34 32 2E 35 20 31 37 30 33 30 30  sage=42.5 170300
0000003F   31 32 33 34 30 30 30 30 30 30 30 30 30            1234000000000

# Checksum (4 bytes): CRC32 of all above data
0000004C   A7 B3 2E 1F                                      §³..
```

**Breakdown:**
- **Length Prefix**: `0x00000049` = 73 bytes
- **Timestamp**: `0x179DA1B94E1A35CB` = 1703001234567890123 nanoseconds
- **Entry Type**: `0x00` = Data Write
- **DB Length**: `0x0000000A` = 10 bytes
- **DB Name**: UTF-8 "production"
- **Payload Length**: `0x00000027` = 39 bytes
- **Payload**: UTF-8 line protocol string
- **Checksum**: `0x1F2EB3A7` (CRC32)

**Total Entry Size**: 4 (length prefix) + 73 (entry data) = **77 bytes**

## Checksum Algorithm

### CRC32 Details

**Polynomial**: `0xEDB88320` (reflected form of `0x04C11DB7`)

**Implementation**: Using `crc32fast` crate for optimized performance

**Coverage**: All entry data from timestamp through payload (excluding length prefix and checksum itself)

**Example Calculation:**
```rust
use crc32fast::Hasher;

let mut hasher = Hasher::new();
hasher.update(&timestamp_bytes);
hasher.update(&[entry_type]);
hasher.update(&db_len_bytes);
hasher.update(db_name_bytes);
hasher.update(&payload_len_bytes);
hasher.update(payload_bytes);
let checksum = hasher.finalize();
```

**Verification on Read:**
```rust
// Read entry data (excluding checksum)
let data_without_checksum = &entry_buf[0..pos];
let stored_checksum = read_u32_le(&entry_buf[pos..pos+4]);
let computed_checksum = crc32fast::hash(data_without_checksum);

if stored_checksum != computed_checksum {
    // Entry is corrupted, skip or abort
}
```

## Recovery Process

### Startup Recovery Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Scan WAL Directory                                       │
│    - Find all *.wal files                                   │
│    - Sort by filename (timestamp)                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Replay Data Writes (data_*.wal)                          │
│    - Read each entry sequentially                           │
│    - Validate checksum                                      │
│    - Re-execute write operation                             │
│    - Skip corrupted entries with warning                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Replay Series Tracking (series_*.wal)                    │
│    - Read each entry sequentially                           │
│    - Validate checksum                                      │
│    - Rebuild series cache                                   │
│    - Skip corrupted entries with warning                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Normal Operation                                         │
│    - Continue accepting new writes                          │
│    - Append to current WAL files                            │
└─────────────────────────────────────────────────────────────┘
```

### Recovery Guarantees

**What is recovered:**
- ✅ All writes logged to WAL before crash
- ✅ Order preservation (entries replayed in written order)
- ✅ Database isolation (writes apply to correct database)

**What may be lost:**
- ❌ Writes not yet fsynced (if `sync_on_write=false`)
- ❌ Corrupted entries (detected by checksum, skipped with warning)
- ❌ Incomplete entries (truncated during crash, EOF detection)

**Recovery Time:**
- Depends on WAL file size and number of entries
- Typical: 1-10 seconds for < 100MB WAL
- Large WAL: May take minutes, consider reducing `rotate_size_mb`

## File Rotation

### Rotation Trigger

Rotation occurs when active WAL file exceeds `rotate_size_mb`:

```rust
if writer.size() >= rotate_size_bytes {
    // 1. Flush current file to disk
    writer.flush()?;

    // 2. Create new file with current timestamp
    let new_file = format!("{}_{}.wal", prefix, current_timestamp());

    // 3. Switch to new writer
    writer = WalWriter::new(new_file)?;
}
```

### Rotation Example

**Before Rotation:**
```
_wal/
└── data_1703001234.wal  (65 MB, active)
```

**After Rotation:**
```
_wal/
├── data_1703001234.wal  (65 MB, closed)
└── data_1703005678.wal  (0 MB, active)  ← New file
```

### Multiple Generations

**Over time with multiple rotations:**
```
_wal/
├── data_1703001234.wal  (65 MB, age: 24h) ← Will be deleted by cleanup
├── data_1703005678.wal  (65 MB, age: 20h)
├── data_1703010000.wal  (65 MB, age: 15h)
├── data_1703015000.wal  (65 MB, age: 10h)
└── data_1703020000.wal  (32 MB, active)
```

## Performance Characteristics

### Write Performance

| Mode                | Latency      | Durability          | Use Case                    |
|---------------------|--------------|---------------------|-----------------------------|
| `sync_on_write=true`| 1-5 ms       | 100% durable        | Financial data, critical    |
| `sync_on_write=false`| <1 ms       | May lose seconds    | High-throughput metrics     |

### Space Overhead

**Per Entry Overhead:**
- Length prefix: 4 bytes
- Timestamp: 8 bytes
- Entry type: 1 byte
- DB name length: 4 bytes
- DB name: N bytes (avg ~10 bytes)
- Payload length: 4 bytes
- Checksum: 4 bytes
- **Total overhead**: ~35 bytes + DB name length

**Example:**
- Line protocol: 100 bytes
- Entry overhead: ~35 bytes
- Total entry size: ~135 bytes
- **Overhead**: ~35%

### Disk I/O Patterns

**Sequential Writes**: All writes are append-only, optimal for HDDs and SSDs

**Read Pattern (Recovery)**: Sequential reads during startup only

**Cleanup**: Infrequent delete operations (hourly by default)

## Error Handling

### Corruption Detection

**Checksum Mismatch:**
```
WARNING: Skipping corrupted WAL entry: Checksum mismatch: expected 3456789012, got 1234567890
```
- Entry is skipped
- Recovery continues with next entry
- Data loss for that specific entry

**Truncated Entry:**
```
ERROR: Failed to read entry data: Unexpected EOF
```
- Indicates crash during write
- Remaining entries in file are ignored
- Recovery continues with next file

**Invalid UTF-8:**
```
ERROR: Invalid UTF-8 in db_name
```
- Entry is skipped
- Recovery continues

### Validation Levels

1. **Length consistency**: Entry length matches actual data
2. **UTF-8 validation**: Database name is valid UTF-8
3. **Checksum verification**: CRC32 matches computed value
4. **Type validation**: Entry type is known (0 or 1)

## Best Practices

### Configuration Recommendations

**High Durability (Financial/Critical):**
```toml
[wal]
enabled = true
sync_on_write = true          # Force fsync
rotate_size_mb = 32           # Smaller files, faster recovery
retention_hours = 48          # Keep longer for debugging
```

**High Performance (Metrics/Logs):**
```toml
[wal]
enabled = true
sync_on_write = false         # Buffered writes
rotate_size_mb = 128          # Larger files, less overhead
retention_hours = 12          # Shorter retention
```

**Disabled (Development/Testing):**
```toml
[wal]
enabled = false               # No durability overhead
```

### Monitoring

**Key Metrics to Monitor:**
- WAL file count (should stay relatively constant)
- WAL directory size (should not grow unbounded)
- Recovery time on startup
- Corrupted entry count in logs

**Alert Thresholds:**
- WAL directory > 1GB → Check cleanup task
- Recovery time > 30s → Reduce `rotate_size_mb`
- Corrupted entries > 0 → Investigate disk issues

## Troubleshooting

### Common Issues

**Issue: WAL directory growing unbounded**
- **Cause**: Cleanup task not running or retention too high
- **Fix**: Check `cleanup_interval_secs` and `retention_hours`
- **Command**: `ls -lh data/_wal/`

**Issue: Slow startup recovery**
- **Cause**: Too many large WAL files
- **Fix**: Reduce `rotate_size_mb`, lower `retention_hours`
- **Workaround**: Manually delete old WAL files (data loss!)

**Issue: Frequent corruption warnings**
- **Cause**: Disk issues, crashes during writes
- **Fix**: Check disk health, enable `sync_on_write`
- **Diagnostic**: `dmesg | grep -i error`

### Manual Recovery

**Force delete all WAL files (data loss!):**
```bash
rm -f data/_wal/*.wal
```

**Inspect WAL file with hexdump:**
```bash
hexdump -C data/_wal/data_1703001234.wal | head -100
```

**Count entries in WAL file:**
```bash
# Simple heuristic: count length prefixes
wc -c data/_wal/data_1703001234.wal
```

## Version History

### Version 1.0 (Current)
- Initial implementation
- CRC32 checksum
- Two entry types: data write, series tracking
- Bincode serialization for series records

### Future Enhancements

**Potential Improvements:**
- Entry type versioning for backward compatibility
- Compression (LZ4/Zstd) for large payloads
- Index files for faster seeking during recovery
- Parallel replay for faster recovery
- Per-database WAL files for better isolation

## References

- **CRC32 Algorithm**: https://en.wikipedia.org/wiki/Cyclic_redundancy_check
- **Little-endian**: https://en.wikipedia.org/wiki/Endianness
- **Bincode Format**: https://github.com/bincode-org/bincode
- **InfluxDB Line Protocol**: https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/

---

**Document Version**: 1.0
**Last Updated**: 2025-12-29
**Author**: Stats Service Team
