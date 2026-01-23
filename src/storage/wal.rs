use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::WalConfig;
use crate::storage::storage::Storage;

/// WAL entry type discriminator
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    DataWrite = 0,
    SeriesTrack = 1,
}

impl EntryType {
    fn from_u8(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(EntryType::DataWrite),
            1 => Ok(EntryType::SeriesTrack),
            _ => Err(format!("Invalid entry type: {}", value)),
        }
    }
}

/// WAL entry structure
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub timestamp: u64,
    pub entry_type: EntryType,
    pub db_name: String,
    pub payload: Vec<u8>,
}

impl WalEntry {
    /// Encode entry to binary format with checksum
    /// Format: [timestamp:8][type:1][db_len:4][db_name:N][payload_len:4][payload:M][checksum:4]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Timestamp (8 bytes)
        buf.extend_from_slice(&self.timestamp.to_le_bytes());

        // Entry type (1 byte)
        buf.push(self.entry_type as u8);

        // Database name length (4 bytes)
        let db_bytes = self.db_name.as_bytes();
        buf.extend_from_slice(&(db_bytes.len() as u32).to_le_bytes());

        // Database name
        buf.extend_from_slice(db_bytes);

        // Payload length (4 bytes)
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());

        // Payload
        buf.extend_from_slice(&self.payload);

        // Checksum (CRC32 of all previous data)
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Decode entry from binary format with checksum validation
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        if data.len() < 17 {
            // Minimum: 8 + 1 + 4 + 0 + 4 + 0 + 4 = 21, but we check 17 first
            return Err("Data too short".to_string());
        }

        let mut pos = 0;

        // Read timestamp
        let timestamp = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .map_err(|_| "Invalid timestamp")?,
        );
        pos += 8;

        // Read entry type
        let entry_type = EntryType::from_u8(data[pos])?;
        pos += 1;

        // Read db name length
        let db_len = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "Invalid db_len")?,
        ) as usize;
        pos += 4;

        // Read db name
        if pos + db_len > data.len() {
            return Err("Invalid db_name length".to_string());
        }
        let db_name = String::from_utf8(data[pos..pos + db_len].to_vec())
            .map_err(|_| "Invalid UTF-8 in db_name")?;
        pos += db_len;

        // Read payload length
        if pos + 4 > data.len() {
            return Err("Missing payload_len".to_string());
        }
        let payload_len = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "Invalid payload_len")?,
        ) as usize;
        pos += 4;

        // Read payload
        if pos + payload_len + 4 > data.len() {
            return Err("Invalid payload length".to_string());
        }
        let payload = data[pos..pos + payload_len].to_vec();
        pos += payload_len;

        // Read and verify checksum
        let stored_checksum = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| "Invalid checksum")?,
        );

        let computed_checksum = crc32fast::hash(&data[0..pos]);
        if stored_checksum != computed_checksum {
            return Err(format!(
                "Checksum mismatch: expected {}, got {}",
                stored_checksum, computed_checksum
            ));
        }

        Ok(WalEntry {
            timestamp,
            entry_type,
            db_name,
            payload,
        })
    }
}

/// Buffered WAL segment for object store writes
/// Since object stores don't support append, we buffer entries and write complete segments
struct WalSegment {
    /// Segment file name (without path)
    filename: String,
    /// Buffered encoded entries with length prefixes
    buffer: Vec<u8>,
    /// Number of entries in this segment
    entry_count: usize,
    /// Maximum buffer size in bytes
    max_size_bytes: u64,
    /// Maximum entries before forcing write
    max_entries: usize,
    /// Bytes already persisted (for incremental writes)
    persisted_bytes: usize,
    /// Entries already persisted
    persisted_entries: usize,
}

impl WalSegment {
    fn new(filename: String, max_size_mb: u64, max_entries: usize) -> Self {
        WalSegment {
            filename,
            buffer: Vec::new(),
            entry_count: 0,
            max_size_bytes: max_size_mb * 1024 * 1024,
            max_entries,
            persisted_bytes: 0,
            persisted_entries: 0,
        }
    }

    /// Add entry to buffer
    fn append(&mut self, entry: &WalEntry) {
        let encoded = entry.encode();
        let entry_len = encoded.len() as u32;

        // Write entry length prefix (4 bytes)
        self.buffer.extend_from_slice(&entry_len.to_le_bytes());

        // Write entry data
        self.buffer.extend_from_slice(&encoded);

        self.entry_count += 1;
    }

    /// Check if segment needs rotation (based on size)
    fn needs_rotation(&self) -> bool {
        self.buffer.len() as u64 >= self.max_size_bytes
    }

    /// Check if buffer reached max entries and needs persistence
    fn needs_persist(&self) -> bool {
        self.unpersisted_entries() >= self.max_entries
    }

    /// Get unpersisted entry count
    fn unpersisted_entries(&self) -> usize {
        self.entry_count.saturating_sub(self.persisted_entries)
    }

    /// Check if there are unpersisted entries
    fn has_unpersisted(&self) -> bool {
        self.buffer.len() > self.persisted_bytes
    }

    /// Get buffer size
    fn size(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Get entry count
    fn entries(&self) -> usize {
        self.entry_count
    }

    /// Get the full buffer for writing (used when persisting)
    /// After calling this, mark_persisted() should be called
    fn get_buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Mark all current data as persisted
    fn mark_persisted(&mut self) {
        self.persisted_bytes = self.buffer.len();
        self.persisted_entries = self.entry_count;
    }

    /// Take the buffer contents and reset (for rotation)
    fn take_buffer(&mut self) -> Vec<u8> {
        self.persisted_bytes = 0;
        self.persisted_entries = 0;
        self.entry_count = 0;
        std::mem::take(&mut self.buffer)
    }

    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Main WAL manager with object store support
pub struct Wal {
    storage: Arc<Storage>,
    wal_dir: String,
    data_segment: Arc<Mutex<WalSegment>>,
    series_segment: Arc<Mutex<WalSegment>>,
    rotate_size_mb: u64,
    buffer_entries: usize,
    /// Sequence counter to ensure unique filenames even within the same millisecond
    sequence: AtomicU64,
    /// Recovery mode flag - prevents WAL deletion during startup recovery
    recovery_mode: Arc<std::sync::atomic::AtomicBool>,
    /// Sync on write flag
    sync_on_write: bool,
}

impl Wal {
    pub async fn new(storage: Arc<Storage>, config: &WalConfig) -> Result<Self, String> {
        let wal_dir = config.wal_dir.clone();

        // Create WAL directory if needed (no-op for object stores)
        storage
            .create_dir(&wal_dir)
            .await
            .map_err(|e| format!("Failed to create WAL dir: {}", e))?;

        let rotate_size_mb = config.rotate_size_mb;
        let buffer_entries = config.buffer_entries;

        // Initialize sequence counter
        let sequence = AtomicU64::new(0);

        // Create initial WAL segments
        let data_filename = Self::create_wal_filename("data", &sequence);
        let series_filename = Self::create_wal_filename("series", &sequence);

        let data_segment = Arc::new(Mutex::new(WalSegment::new(
            data_filename,
            rotate_size_mb,
            buffer_entries,
        )));

        let series_segment = Arc::new(Mutex::new(WalSegment::new(
            series_filename,
            rotate_size_mb,
            buffer_entries,
        )));

        info!(
            "WAL initialized at {} (sync_on_write={}, rotate_size={}MB, buffer_entries={})",
            wal_dir, config.sync_on_write, rotate_size_mb, buffer_entries
        );

        Ok(Wal {
            storage,
            wal_dir,
            data_segment,
            series_segment,
            rotate_size_mb,
            buffer_entries,
            sequence,
            recovery_mode: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sync_on_write: config.sync_on_write,
        })
    }

    /// Check if sync_on_write is enabled (strict durability mode)
    pub fn is_sync_on_write(&self) -> bool {
        self.sync_on_write
    }

    fn create_wal_filename(prefix: &str, sequence: &AtomicU64) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Fetch and increment sequence to ensure uniqueness even within same millisecond
        let seq = sequence.fetch_add(1, Ordering::Relaxed);

        format!("{}_{}_{}.wal", prefix, timestamp, seq)
    }

    fn create_new_filename(&self, prefix: &str) -> String {
        Self::create_wal_filename(prefix, &self.sequence)
    }

    /// Get full path for a WAL file
    fn wal_path(&self, filename: &str) -> String {
        format!("{}/{}", self.wal_dir, filename)
    }

    /// Set recovery mode (true during WAL replay at startup)
    pub fn set_recovery_mode(&self, enabled: bool) {
        self.recovery_mode
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
        if enabled {
            info!("WAL recovery mode: ENABLED (WAL files will not be deleted during recovery)");
        } else {
            info!("WAL recovery mode: DISABLED (normal operation)");
        }
    }

    /// Check if in recovery mode
    pub fn is_recovery_mode(&self) -> bool {
        self.recovery_mode
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Start background periodic flush task (every second)
    pub fn start_background_flush(self: &Arc<Self>) {
        let wal = Arc::clone(self);

        tokio::spawn(async move {
            use tokio::time::{interval, Duration};
            let mut ticker = interval(Duration::from_secs(1));

            debug!("WAL background flush started: every 1 second");

            loop {
                ticker.tick().await;

                // Persist data segment if there are unpersisted entries
                if let Err(e) = wal.persist_if_needed().await {
                    warn!("Background WAL persist failed: {}", e);
                }
            }
        });
    }

    /// Persist buffer if there are unpersisted entries
    async fn persist_if_needed(&self) -> Result<(), String> {
        let mut segment = self.data_segment.lock().await;
        if segment.has_unpersisted() {
            self.persist_segment_locked(&mut segment).await?;
        }
        Ok(())
    }

    /// Log a data write operation
    pub async fn log_data_write(&self, db: &str, line_protocol: &str) -> Result<(), String> {
        let entry = WalEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            entry_type: EntryType::DataWrite,
            db_name: db.to_string(),
            payload: line_protocol.as_bytes().to_vec(),
        };

        let (needs_rotation, needs_persist) = {
            let mut segment = self.data_segment.lock().await;
            segment.append(&entry);

            // If sync_on_write, persist immediately
            if self.sync_on_write {
                self.persist_segment_locked(&mut segment).await?;
            }

            (segment.needs_rotation(), segment.needs_persist())
        };

        // Check if rotation is needed (size limit)
        if needs_rotation {
            self.rotate_data_wal().await?;
        } else if needs_persist {
            // Persist when buffer_entries is reached (without rotation)
            let mut segment = self.data_segment.lock().await;
            self.persist_segment_locked(&mut segment).await?;
        }

        Ok(())
    }

    /// Persist a segment to storage (must hold lock)
    /// This writes the full buffer and marks it as persisted (for incremental writes)
    async fn persist_segment_locked(&self, segment: &mut WalSegment) -> Result<(), String> {
        if segment.is_empty() || !segment.has_unpersisted() {
            return Ok(());
        }

        let path = self.wal_path(&segment.filename);
        let data = Bytes::from(segment.get_buffer().to_vec());
        let entries = segment.entries();

        self.storage.put(&path, data).await?;
        segment.mark_persisted();

        debug!("Persisted WAL segment: {} ({} entries)", path, entries);
        Ok(())
    }

    /// Persist and take buffer for rotation (clears the segment)
    async fn persist_and_take_segment(&self, segment: &mut WalSegment) -> Result<(), String> {
        if segment.is_empty() {
            return Ok(());
        }

        let path = self.wal_path(&segment.filename);
        let data = Bytes::from(segment.take_buffer());

        self.storage.put(&path, data).await?;

        debug!("Persisted and rotated WAL segment: {}", path);
        Ok(())
    }

    /// Rotate data WAL file and delete all old ones (post-flush cleanup)
    /// This ensures only unflushed data remains in WAL directory
    ///
    /// IMPORTANT: This method deletes ALL old data WAL files, not just the one being rotated.
    /// This handles high write throughput where multiple WAL files accumulate between flushes.
    /// During recovery mode, rotation happens but deletion is skipped.
    pub async fn rotate_and_delete_old_wal(&self) -> Result<(), String> {
        let new_filename = self.create_new_filename("data");

        // Persist current segment and rotate to new one
        let _old_filename = {
            let mut segment = self.data_segment.lock().await;

            // Persist current buffer if not empty (and take it for rotation)
            if !segment.is_empty() {
                self.persist_and_take_segment(&mut segment).await?;
            }

            let old = segment.filename.clone();
            info!("Rotating data WAL: {} -> {}", old, new_filename);

            // Create new segment
            *segment = WalSegment::new(new_filename, self.rotate_size_mb, self.buffer_entries);

            old
        };

        // Skip deletion if in recovery mode (WAL files needed for potential re-replay)
        if self.is_recovery_mode() {
            debug!("Recovery mode active: skipping WAL file deletion after flush");
            return Ok(());
        }

        // Now delete ALL old data WAL files (not just the one we rotated)
        // This handles high write throughput where multiple WAL files accumulate between flushes
        let current_filename = {
            let segment = self.data_segment.lock().await;
            segment.filename.clone()
        };

        let files = self.list_wal_files("data").await?;
        let mut deleted = 0;

        for file_path in files {
            // Extract filename from path
            let filename = file_path.split('/').last().unwrap_or(&file_path);

            // Skip the current active file
            if filename == current_filename {
                debug!("Keeping active WAL file: {}", file_path);
                continue;
            }

            // Delete old file (data is now flushed to Parquet)
            match self.storage.delete(&file_path).await {
                Ok(_) => {
                    info!("Deleted old WAL file after flush: {}", file_path);
                    deleted += 1;
                }
                Err(e) => {
                    warn!("Failed to delete old WAL file {}: {}", file_path, e);
                    // Don't fail - rotation succeeded, deletion is best-effort
                }
            }
        }

        if deleted > 0 {
            info!("Cleaned up {} old data WAL file(s) after flush", deleted);
        }

        // Also clean up any empty WAL files immediately to prevent accumulation
        if let Err(e) = self.cleanup_empty_files().await {
            warn!("Failed to clean up empty WAL files: {}", e);
            // Don't fail the rotation on cleanup error
        }

        Ok(())
    }

    /// Clean up empty WAL files (zero bytes)
    /// Called after rotation to prevent accumulation of empty files
    async fn cleanup_empty_files(&self) -> Result<usize, String> {
        let mut deleted = 0;

        // Get current file names to avoid deleting active files
        let current_data_filename = {
            let segment = self.data_segment.lock().await;
            segment.filename.clone()
        };
        let current_series_filename = {
            let segment = self.series_segment.lock().await;
            segment.filename.clone()
        };

        let files = self.storage.list(&self.wal_dir).await?;

        for file_path in files {
            let filename = file_path.split('/').last().unwrap_or(&file_path);

            // Skip if it's the current active file
            if filename == current_data_filename || filename == current_series_filename {
                continue;
            }

            if filename.ends_with(".wal") {
                // Check if file is empty
                match self.storage.size(&file_path).await {
                    Ok(size) if size == 0 => {
                        debug!("Deleting empty WAL file: {}", file_path);
                        if let Err(e) = self.storage.delete(&file_path).await {
                            warn!("Failed to delete empty WAL file {}: {}", file_path, e);
                        } else {
                            deleted += 1;
                        }
                    }
                    _ => {}
                }
            }
        }

        if deleted > 0 {
            debug!("Cleaned up {} empty WAL files", deleted);
        }

        Ok(deleted)
    }

    /// Rotate data WAL file (public for post-flush rotation)
    pub async fn rotate_data_wal(&self) -> Result<(), String> {
        let new_filename = self.create_new_filename("data");
        let mut segment = self.data_segment.lock().await;

        // Persist current buffer if not empty (and take it for rotation)
        if !segment.is_empty() {
            self.persist_and_take_segment(&mut segment).await?;
        }

        info!(
            "Rotating data WAL: {} -> {}",
            segment.filename, new_filename
        );

        // Create new segment
        *segment = WalSegment::new(new_filename, self.rotate_size_mb, self.buffer_entries);

        Ok(())
    }

    /// Flush current segment to storage without rotation
    pub async fn flush(&self) -> Result<(), String> {
        let mut segment = self.data_segment.lock().await;
        if !segment.is_empty() {
            self.persist_segment_locked(&mut segment).await?;
        }
        Ok(())
    }

    /// Replay all data write entries
    pub async fn replay_data_writes<F>(&self, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(&str, &str) -> Result<(), String>,
    {
        let files = self.list_wal_files("data").await?;
        let mut count = 0;

        for file_path in files {
            debug!("Replaying data WAL: {}", file_path);
            count += self
                .replay_file(&file_path, |entry| {
                    if entry.entry_type == EntryType::DataWrite {
                        let line_protocol = String::from_utf8(entry.payload.clone())
                            .map_err(|_| "Invalid UTF-8 in payload")?;
                        callback(&entry.db_name, &line_protocol)?;
                    }
                    Ok(())
                })
                .await?;
        }

        // NOTE: Do NOT delete old WAL files here!
        // The replayed data is now in cache (volatile), but not yet in Parquet (durable).
        // If we crash again before flush → data would be lost.
        // Old WAL files will be cleaned up after successful flush to Parquet.

        Ok(count)
    }

    /// Clean up old data WAL files after recovery flush
    /// Deletes all data WAL files except the current active one
    /// Called after recovered data has been safely flushed to Parquet
    pub async fn cleanup_old_data_wal_files(&self) -> Result<(), String> {
        let current_filename = {
            let segment = self.data_segment.lock().await;
            segment.filename.clone()
        };

        let files = self.list_wal_files("data").await?;
        let mut deleted = 0;

        for file_path in files {
            let filename = file_path.split('/').last().unwrap_or(&file_path);

            // Skip the current active file
            if filename == current_filename {
                debug!("Keeping active WAL file: {}", file_path);
                continue;
            }

            // Delete old replayed file (data is now safe in Parquet)
            match self.storage.delete(&file_path).await {
                Ok(_) => {
                    info!("Deleted old WAL file (data in Parquet): {}", file_path);
                    deleted += 1;
                }
                Err(e) => {
                    warn!("Failed to delete old WAL file {}: {}", file_path, e);
                }
            }
        }

        if deleted > 0 {
            info!("Cleaned up {} old WAL files after recovery", deleted);
        }

        Ok(())
    }

    /// Replay a single WAL file from storage
    async fn replay_file<F>(&self, path: &str, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(&WalEntry) -> Result<(), String>,
    {
        let data = match self.storage.get(path).await {
            Ok(d) => d,
            Err(e) => {
                // File might not exist if it was just created but never written
                warn!("Could not read WAL file {}: {}", path, e);
                return Ok(0);
            }
        };

        let mut count = 0;
        let mut pos = 0;

        while pos < data.len() {
            // Read entry length prefix
            if pos + 4 > data.len() {
                break; // End of file or corrupted
            }

            let entry_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .map_err(|_| "Invalid entry length")?,
            ) as usize;
            pos += 4;

            // Read entry data
            if pos + entry_len > data.len() {
                warn!("Truncated entry in WAL file {}", path);
                break;
            }

            let entry_data = &data[pos..pos + entry_len];
            pos += entry_len;

            // Decode entry
            match WalEntry::decode(entry_data) {
                Ok(entry) => {
                    callback(&entry)?;
                    count += 1;
                }
                Err(e) => {
                    warn!("Skipping corrupted WAL entry: {}", e);
                }
            }
        }

        Ok(count)
    }

    /// List WAL files with given prefix, sorted by creation time
    async fn list_wal_files(&self, prefix: &str) -> Result<Vec<String>, String> {
        let files = self.storage.list(&self.wal_dir).await?;

        let mut wal_files: Vec<String> = files
            .into_iter()
            .filter(|path| {
                let filename = path.split('/').last().unwrap_or(path);
                filename.starts_with(prefix) && filename.ends_with(".wal")
            })
            .collect();

        // Sort by filename (which includes timestamp)
        wal_files.sort();
        Ok(wal_files)
    }

    /// Clean up old WAL files
    pub async fn cleanup_old_files(&self, retention_hours: u64) -> Result<usize, String> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff_secs = now_secs - (retention_hours * 3600);
        let cutoff_millis = (cutoff_secs * 1000) as u128;

        let mut deleted = 0;

        // Get current file names to avoid deleting active files
        let current_data_filename = {
            let segment = self.data_segment.lock().await;
            segment.filename.clone()
        };
        let current_series_filename = {
            let segment = self.series_segment.lock().await;
            segment.filename.clone()
        };

        let files = self.storage.list(&self.wal_dir).await?;

        for file_path in files {
            let filename = file_path.split('/').last().unwrap_or(&file_path);

            // Skip if it's the current active file
            if filename == current_data_filename || filename == current_series_filename {
                continue;
            }

            if filename.ends_with(".wal") {
                // Get file size
                let file_size = self.storage.size(&file_path).await.unwrap_or(0);

                let should_delete = if file_size == 0 {
                    // Delete empty files immediately (no data to preserve)
                    debug!("Deleting empty WAL file: {}", file_path);
                    true
                } else {
                    // Delete old files based on retention policy
                    // Parse filename: data_1767670539.wal (old seconds format)
                    //             or: data_1767670539000.wal (old milliseconds format)
                    //             or: data_1767670539000_0.wal (new format with sequence)
                    let parts: Vec<&str> = filename.split('_').collect();
                    if parts.len() >= 2 {
                        // Get timestamp (second element)
                        let timestamp_str = parts[1].trim_end_matches(".wal");
                        if let Ok(file_timestamp) = timestamp_str.parse::<u128>() {
                            // Handle both old and new timestamp formats
                            // Old seconds: ~1767670539 (10 digits)
                            // Old/new milliseconds: ~1767670539000 (13 digits)
                            let is_old = file_timestamp < cutoff_millis;
                            if is_old {
                                debug!("Deleting old WAL file: {}", file_path);
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if should_delete {
                    self.storage
                        .delete(&file_path)
                        .await
                        .map_err(|e| format!("Failed to delete WAL file: {}", e))?;
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            info!("Cleaned up {} old WAL files", deleted);
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_encode_decode() {
        let entry = WalEntry {
            timestamp: 123456789,
            entry_type: EntryType::DataWrite,
            db_name: "testdb".to_string(),
            payload: b"test data".to_vec(),
        };

        let encoded = entry.encode();
        let decoded = WalEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.timestamp, entry.timestamp);
        assert_eq!(decoded.entry_type, entry.entry_type);
        assert_eq!(decoded.db_name, entry.db_name);
        assert_eq!(decoded.payload, entry.payload);
    }

    #[test]
    fn test_corrupted_checksum() {
        let entry = WalEntry {
            timestamp: 123456789,
            entry_type: EntryType::DataWrite,
            db_name: "testdb".to_string(),
            payload: b"test data".to_vec(),
        };

        let mut encoded = entry.encode();
        // Corrupt the checksum
        let len = encoded.len();
        encoded[len - 1] ^= 0xFF;

        let result = WalEntry::decode(&encoded);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Checksum mismatch"));
    }
}
