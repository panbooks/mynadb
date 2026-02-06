use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use lru::LruCache;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    /// Name of the event (e.g., "parse", "optimize", "execute")
    pub name: String,

    /// When the event started (wall clock time)
    pub timestamp: DateTime<Utc>,

    /// Duration of the event in microseconds
    pub duration_us: u64,

    /// Additional metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTimeline {
    /// Unique query ID
    pub query_id: String,

    /// The SQL query text
    pub query: String,

    /// Database/catalog name
    pub database: String,

    /// When the query started
    pub start_time: DateTime<Utc>,

    /// Total duration in microseconds
    pub total_duration_us: u64,

    /// Timeline of events
    pub events: Vec<TimelineEvent>,

    /// Query result metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_metadata: Option<QueryResultMetadata>,

    /// Error information if query failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultMetadata {
    /// Number of rows returned
    pub row_count: usize,

    /// Number of partitions scanned
    pub partitions_scanned: usize,

    /// Number of files read
    pub files_read: usize,

    /// Bytes scanned
    pub bytes_scanned: u64,
}

/// Builder for tracking query execution timeline
pub struct TimelineBuilder {
    query_id: String,
    query: String,
    database: String,
    start_time: DateTime<Utc>,
    start_instant: Instant,
    events: Vec<TimelineEvent>,
    current_event: Option<(String, Instant)>,
}

impl TimelineBuilder {
    pub fn new(query: String, database: String) -> Self {
        Self {
            query_id: Uuid::new_v4().to_string(),
            query,
            database,
            start_time: Utc::now(),
            start_instant: Instant::now(),
            events: Vec::new(),
            current_event: None,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Start a new event
    pub fn start_event(&mut self, name: impl Into<String>) {
        if let Some((prev_name, prev_start)) = self.current_event.take() {
            // Finish the previous event
            let duration = prev_start.elapsed();
            self.events.push(TimelineEvent {
                name: prev_name,
                timestamp: self.start_time + chrono::Duration::from_std(self.start_instant.elapsed() - duration).unwrap_or_default(),
                duration_us: duration.as_micros() as u64,
                metadata: None,
            });
        }

        self.current_event = Some((name.into(), Instant::now()));
    }

    /// Add an event with explicit duration
    pub fn add_event(&mut self, name: impl Into<String>, duration: Duration) {
        self.events.push(TimelineEvent {
            name: name.into(),
            timestamp: self.start_time + chrono::Duration::from_std(self.start_instant.elapsed() - duration).unwrap_or_default(),
            duration_us: duration.as_micros() as u64,
            metadata: None,
        });
    }

    /// Add an event with metadata
    pub fn add_event_with_metadata(
        &mut self,
        name: impl Into<String>,
        duration: Duration,
        metadata: serde_json::Value,
    ) {
        self.events.push(TimelineEvent {
            name: name.into(),
            timestamp: self.start_time + chrono::Duration::from_std(self.start_instant.elapsed() - duration).unwrap_or_default(),
            duration_us: duration.as_micros() as u64,
            metadata: Some(metadata),
        });
    }

    /// Build the final timeline
    pub fn build(mut self, result_metadata: Option<QueryResultMetadata>, error: Option<String>) -> QueryTimeline {
        // Finish any ongoing event
        if let Some((name, start)) = self.current_event.take() {
            let duration = start.elapsed();
            self.events.push(TimelineEvent {
                name,
                timestamp: self.start_time + chrono::Duration::from_std(self.start_instant.elapsed() - duration).unwrap_or_default(),
                duration_us: duration.as_micros() as u64,
                metadata: None,
            });
        }

        QueryTimeline {
            query_id: self.query_id,
            query: self.query,
            database: self.database,
            start_time: self.start_time,
            total_duration_us: self.start_instant.elapsed().as_micros() as u64,
            events: self.events,
            result_metadata,
            error,
        }
    }
}

/// In-memory storage for query timelines
pub struct TimelineStore {
    timelines: Arc<RwLock<LruCache<String, QueryTimeline>>>,
}

impl TimelineStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            timelines: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity).unwrap()
            ))),
        }
    }

    pub fn insert(&self, timeline: QueryTimeline) {
        let mut timelines = self.timelines.write();
        timelines.put(timeline.query_id.clone(), timeline);
    }

    pub fn get(&self, query_id: &str) -> Option<QueryTimeline> {
        let mut timelines = self.timelines.write();
        timelines.get(query_id).cloned()
    }

    pub fn get_recent(&self, limit: usize) -> Vec<QueryTimeline> {
        let timelines = self.timelines.read();
        timelines.iter()
            .take(limit)
            .map(|(_, timeline)| timeline.clone())
            .collect()
    }

    pub fn clear(&self) {
        let mut timelines = self.timelines.write();
        timelines.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeline_builder() {
        let mut builder = TimelineBuilder::new("SELECT * FROM cpu".to_string(), "mydb".to_string());

        builder.start_event("parse");
        std::thread::sleep(Duration::from_millis(10));

        builder.start_event("optimize");
        std::thread::sleep(Duration::from_millis(20));

        builder.start_event("execute");
        std::thread::sleep(Duration::from_millis(30));

        let timeline = builder.build(Some(QueryResultMetadata {
            row_count: 100,
            partitions_scanned: 2,
            files_read: 5,
            bytes_scanned: 1024,
        }), None);

        assert_eq!(timeline.events.len(), 3);
        assert!(timeline.events[0].name == "parse");
        assert!(timeline.events[1].name == "optimize");
        assert!(timeline.events[2].name == "execute");
        assert!(timeline.total_duration_us > 60000); // At least 60ms
    }

    #[test]
    fn test_timeline_store() {
        let store = TimelineStore::new(10);

        let timeline = QueryTimeline {
            query_id: "test-1".to_string(),
            query: "SELECT 1".to_string(),
            database: "mydb".to_string(),
            start_time: Utc::now(),
            total_duration_us: 1000,
            events: vec![],
            result_metadata: None,
            error: None,
        };

        store.insert(timeline.clone());

        let retrieved = store.get("test-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().query_id, "test-1");
    }
}
