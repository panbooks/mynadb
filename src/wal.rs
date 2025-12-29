use crate::series_tracker::SeriesRecord;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

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

/// WAL writer for a single file
struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    bytes_written: u64,
    sync_on_write: bool,
}

impl WalWriter {
    fn new(path: PathBuf, sync_on_write: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        // Get current file size
        let metadata = file.metadata()?;
        let bytes_written = metadata.len();

        Ok(WalWriter {
            file: BufWriter::new(file),
            path,
            bytes_written,
            sync_on_write,
        })
    }

    fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let encoded = entry.encode();
        let entry_len = encoded.len() as u32;

        // Write entry length prefix (4 bytes)
        self.file.write_all(&entry_len.to_le_bytes())?;

        // Write entry data
        self.file.write_all(&encoded)?;

        self.bytes_written += 4 + entry_len as u64;

        // Sync if configured
        if self.sync_on_write {
            self.file.flush()?;
            self.file.get_ref().sync_all()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }

    fn size(&self) -> u64 {
        self.bytes_written
    }
}

/// Main WAL manager
pub struct Wal {
    wal_dir: PathBuf,
    data_writer: Arc<Mutex<WalWriter>>,
    series_writer: Arc<Mutex<WalWriter>>,
    rotate_size_bytes: u64,
}

impl Wal {
    pub fn new(wal_dir: &str, sync_on_write: bool, rotate_size_mb: u64) -> Result<Self, String> {
        let wal_path = Path::new(wal_dir);

        // Create WAL directory if it doesn't exist
        fs::create_dir_all(wal_path).map_err(|e| format!("Failed to create WAL dir: {}", e))?;

        let rotate_size_bytes = rotate_size_mb * 1024 * 1024;

        // Create initial WAL files
        let data_file = Self::create_wal_file(wal_path, "data")?;
        let series_file = Self::create_wal_file(wal_path, "series")?;

        let data_writer = Arc::new(Mutex::new(
            WalWriter::new(data_file, sync_on_write)
                .map_err(|e| format!("Failed to create data WAL writer: {}", e))?,
        ));

        let series_writer = Arc::new(Mutex::new(
            WalWriter::new(series_file, sync_on_write)
                .map_err(|e| format!("Failed to create series WAL writer: {}", e))?,
        ));

        info!("WAL initialized at {} (sync_on_write={})", wal_dir, sync_on_write);

        Ok(Wal {
            wal_dir: wal_path.to_path_buf(),
            data_writer,
            series_writer,
            rotate_size_bytes,
        })
    }

    fn create_wal_file(dir: &Path, prefix: &str) -> Result<PathBuf, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let filename = format!("{}_{}.wal", prefix, timestamp);
        Ok(dir.join(filename))
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

        let mut writer = self.data_writer.lock().await;
        writer
            .append(&entry)
            .map_err(|e| format!("Failed to write to WAL: {}", e))?;

        // Check if rotation is needed
        if writer.size() >= self.rotate_size_bytes {
            drop(writer); // Release lock before rotating
            self.rotate_data_wal().await?;
        }

        Ok(())
    }

    /// Log series tracking operation
    pub async fn log_series_track(&self, db: &str, records: &[SeriesRecord]) -> Result<(), String> {
        // Serialize records using bincode
        let payload = bincode::serialize(records)
            .map_err(|e| format!("Failed to serialize series records: {}", e))?;

        let entry = WalEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            entry_type: EntryType::SeriesTrack,
            db_name: db.to_string(),
            payload,
        };

        let mut writer = self.series_writer.lock().await;
        writer
            .append(&entry)
            .map_err(|e| format!("Failed to write to WAL: {}", e))?;

        // Check if rotation is needed
        if writer.size() >= self.rotate_size_bytes {
            drop(writer); // Release lock before rotating
            self.rotate_series_wal().await?;
        }

        Ok(())
    }

    /// Rotate data WAL file and delete the old one (post-flush cleanup)
    /// This ensures only unflushed data remains in WAL directory
    pub async fn rotate_and_delete_old_wal(&self) -> Result<(), String> {
        let new_file = Self::create_wal_file(&self.wal_dir, "data")?;
        let old_file = {
            let mut writer = self.data_writer.lock().await;

            // Flush current file
            writer
                .flush()
                .map_err(|e| format!("Failed to flush before rotation: {}", e))?;

            let old_path = writer.path.clone();

            info!("Rotating data WAL: {} -> {}", old_path.display(), new_file.display());

            // Create new writer
            *writer = WalWriter::new(new_file, writer.sync_on_write)
                .map_err(|e| format!("Failed to create new WAL writer: {}", e))?;

            old_path
        };

        // Delete old WAL file (contains only flushed data)
        match fs::remove_file(&old_file) {
            Ok(_) => {
                info!("Deleted old WAL file after flush: {}", old_file.display());
            }
            Err(e) => {
                warn!("Failed to delete old WAL file {}: {}", old_file.display(), e);
                // Don't fail - rotation succeeded, deletion is best-effort
            }
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

        // Get current file paths to avoid deleting active files
        let current_data_path = {
            let writer = self.data_writer.lock().await;
            writer.path.clone()
        };
        let current_series_path = {
            let writer = self.series_writer.lock().await;
            writer.path.clone()
        };

        let entries = fs::read_dir(&self.wal_dir)
            .map_err(|e| format!("Failed to read WAL directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip if it's the current active file
            if path == current_data_path || path == current_series_path {
                continue;
            }

            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    if filename.ends_with(".wal") {
                        // Check if file is empty
                        if let Ok(metadata) = fs::metadata(&path) {
                            if metadata.len() == 0 {
                                debug!("Deleting empty WAL file: {}", path.display());
                                if let Err(e) = fs::remove_file(&path) {
                                    warn!("Failed to delete empty WAL file {}: {}", path.display(), e);
                                } else {
                                    deleted += 1;
                                }
                            }
                        }
                    }
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
        let new_file = Self::create_wal_file(&self.wal_dir, "data")?;
        let mut writer = self.data_writer.lock().await;

        // Flush current file
        writer
            .flush()
            .map_err(|e| format!("Failed to flush before rotation: {}", e))?;

        info!("Rotating data WAL: {} -> {}", writer.path.display(), new_file.display());

        // Create new writer
        *writer = WalWriter::new(new_file, writer.sync_on_write)
            .map_err(|e| format!("Failed to create new WAL writer: {}", e))?;

        Ok(())
    }

    /// Rotate series WAL file
    async fn rotate_series_wal(&self) -> Result<(), String> {
        let new_file = Self::create_wal_file(&self.wal_dir, "series")?;
        let mut writer = self.series_writer.lock().await;

        // Flush current file
        writer
            .flush()
            .map_err(|e| format!("Failed to flush before rotation: {}", e))?;

        info!("Rotating series WAL: {} -> {}", writer.path.display(), new_file.display());

        // Create new writer
        *writer = WalWriter::new(new_file, writer.sync_on_write)
            .map_err(|e| format!("Failed to create new WAL writer: {}", e))?;

        Ok(())
    }

    /// Replay all data write entries
    pub async fn replay_data_writes<F>(&self, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(&str, &str) -> Result<(), String>,
    {
        let files = self.list_wal_files("data")?;
        let mut count = 0;

        for file_path in files {
            debug!("Replaying data WAL: {}", file_path.display());
            count += self.replay_file(&file_path, |entry| {
                if entry.entry_type == EntryType::DataWrite {
                    let line_protocol = String::from_utf8(entry.payload.clone())
                        .map_err(|_| "Invalid UTF-8 in payload")?;
                    callback(&entry.db_name, &line_protocol)?;
                }
                Ok(())
            })?;
        }

        // NOTE: Do NOT delete old WAL files here!
        // The replayed data is now in cache (volatile), but not yet in Parquet (durable).
        // If we crash again before flush → data would be lost.
        // Old WAL files will be cleaned up by rotate_and_delete_old_wal() after the first flush.

        Ok(count)
    }

    /// Replay all series tracking entries
    pub async fn replay_series_tracks<F>(&self, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(&str, &[SeriesRecord]) -> Result<(), String>,
    {
        let files = self.list_wal_files("series")?;
        let mut count = 0;

        for file_path in files {
            debug!("Replaying series WAL: {}", file_path.display());
            count += self.replay_file(&file_path, |entry| {
                if entry.entry_type == EntryType::SeriesTrack {
                    let records: Vec<SeriesRecord> = bincode::deserialize(&entry.payload)
                        .map_err(|e| format!("Failed to deserialize records: {}", e))?;
                    callback(&entry.db_name, &records)?;
                }
                Ok(())
            })?;
        }

        Ok(count)
    }

    /// Replay a single WAL file
    fn replay_file<F>(&self, path: &Path, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(&WalEntry) -> Result<(), String>,
    {
        let file = File::open(path).map_err(|e| format!("Failed to open WAL file: {}", e))?;
        let mut reader = BufReader::new(file);
        let mut count = 0;

        loop {
            // Read entry length prefix
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // End of file
                Err(e) => return Err(format!("Failed to read entry length: {}", e)),
            }

            let entry_len = u32::from_le_bytes(len_buf) as usize;

            // Read entry data
            let mut entry_buf = vec![0u8; entry_len];
            reader
                .read_exact(&mut entry_buf)
                .map_err(|e| format!("Failed to read entry data: {}", e))?;

            // Decode entry
            match WalEntry::decode(&entry_buf) {
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
    fn list_wal_files(&self, prefix: &str) -> Result<Vec<PathBuf>, String> {
        let entries = fs::read_dir(&self.wal_dir)
            .map_err(|e| format!("Failed to read WAL directory: {}", e))?;

        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    if filename.starts_with(prefix) && filename.ends_with(".wal") {
                        files.push(path);
                    }
                }
            }
        }

        // Sort by filename (which includes timestamp)
        files.sort();
        Ok(files)
    }

    /// Clean up old WAL files
    pub async fn cleanup_old_files(&self, retention_hours: u64) -> Result<usize, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now - (retention_hours * 3600);

        let mut deleted = 0;

        // Get current file paths to avoid deleting active files
        let current_data_path = {
            let writer = self.data_writer.lock().await;
            writer.path.clone()
        };
        let current_series_path = {
            let writer = self.series_writer.lock().await;
            writer.path.clone()
        };

        let entries = fs::read_dir(&self.wal_dir)
            .map_err(|e| format!("Failed to read WAL directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip if it's the current active file
            if path == current_data_path || path == current_series_path {
                continue;
            }

            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    if filename.ends_with(".wal") {
                        // Get file metadata to check size and age
                        let metadata = fs::metadata(&path)
                            .map_err(|e| format!("Failed to read metadata for {}: {}", path.display(), e))?;

                        let file_size = metadata.len();
                        let should_delete = if file_size == 0 {
                            // Delete empty files immediately (no data to preserve)
                            debug!("Deleting empty WAL file: {}", path.display());
                            true
                        } else {
                            // Delete old files based on retention policy
                            if let Some(timestamp_str) = filename.split('_').nth(1).and_then(|s| s.strip_suffix(".wal")) {
                                if let Ok(file_timestamp) = timestamp_str.parse::<u64>() {
                                    if file_timestamp < cutoff {
                                        debug!("Deleting old WAL file: {}", path.display());
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
                            fs::remove_file(&path)
                                .map_err(|e| format!("Failed to delete WAL file: {}", e))?;
                            deleted += 1;
                        }
                    }
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
