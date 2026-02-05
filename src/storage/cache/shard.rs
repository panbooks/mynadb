//! Cache shard implementation for reduced lock contention

use arrow::array::RecordBatch;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cache key: (catalog/database, schema/service, table/measurement, date)
pub type CacheKey = (String, String, String, i32);

/// A batch with its replication sequence number (0 for non-replicated/Primary data)
pub type SeqBatch = (u64, RecordBatch);

/// Internal cache data structure: key -> list of (sequence, batch) pairs
pub type CacheData = HashMap<CacheKey, Vec<SeqBatch>>;

/// A single shard of the cache
pub struct CacheShard {
    pub(crate) data: Mutex<CacheData>,
    size_bytes: AtomicUsize,
}

impl CacheShard {
    pub fn new() -> Self {
        CacheShard {
            data: Mutex::new(HashMap::new()),
            size_bytes: AtomicUsize::new(0),
        }
    }

    /// Add batch to this shard with sequence number, returns batch size
    pub fn add(&self, key: CacheKey, seq: u64, batch: RecordBatch) -> usize {
        let batch_size = batch.get_array_memory_size();
        {
            let mut data = self.data.lock();
            data.entry(key).or_default().push((seq, batch));
        }
        self.size_bytes.fetch_add(batch_size, Ordering::Relaxed);
        batch_size
    }

    /// Take all data from this shard (for flushing)
    pub fn take(&self) -> CacheData {
        let mut data = self.data.lock();
        self.size_bytes.store(0, Ordering::Relaxed);
        std::mem::take(&mut *data)
    }

    /// Clear batches with sequence <= flushed_seq (for Standby sync)
    /// Returns number of bytes cleared
    pub fn clear_up_to_seq(&self, flushed_seq: u64) -> usize {
        let mut data = self.data.lock();
        let mut cleared_bytes = 0usize;

        for batches in data.values_mut() {
            let mut i = 0;
            while i < batches.len() {
                if batches[i].0 <= flushed_seq {
                    cleared_bytes += batches[i].1.get_array_memory_size();
                    batches.swap_remove(i);
                } else {
                    i += 1;
                }
            }
        }

        // Remove empty entries
        data.retain(|_, v| !v.is_empty());

        // Update size
        if cleared_bytes > 0 {
            let current = self.size_bytes.load(Ordering::Relaxed);
            self.size_bytes
                .store(current.saturating_sub(cleared_bytes), Ordering::Relaxed);
        }

        cleared_bytes
    }

    pub fn size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.data.lock().is_empty()
    }
}

impl Default for CacheShard {
    fn default() -> Self {
        Self::new()
    }
}
