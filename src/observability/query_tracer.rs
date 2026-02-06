use crate::observability::timeline::{TimelineBuilder, TimelineStore, QueryTimeline, QueryResultMetadata};
use crate::observability::slow_query_logger::SlowQueryLogger;
use std::sync::Arc;
use std::time::Instant;
use tracing::{span, Level, Span};

/// Query tracer that integrates timeline tracking, slow query logging, and OpenTelemetry
pub struct QueryTracer {
    timeline_store: Option<Arc<TimelineStore>>,
    slow_query_logger: Option<Arc<SlowQueryLogger>>,
    enabled: bool,
}

impl QueryTracer {
    pub fn new(
        timeline_store: Option<Arc<TimelineStore>>,
        slow_query_logger: Option<Arc<SlowQueryLogger>>,
    ) -> Self {
        let enabled = timeline_store.is_some() || slow_query_logger.is_some();
        Self {
            timeline_store,
            slow_query_logger,
            enabled,
        }
    }

    pub fn disabled() -> Self {
        Self {
            timeline_store: None,
            slow_query_logger: None,
            enabled: false,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Start tracing a query
    pub fn start_query(&self, query: String, database: String) -> QueryTrace {
        let span = span!(
            Level::INFO,
            "query",
            query = %query,
            database = %database,
        );

        let timeline = if self.timeline_store.is_some() {
            Some(TimelineBuilder::new(query.clone(), database.clone()))
        } else {
            None
        };

        QueryTrace {
            span,
            timeline,
            start_instant: Instant::now(),
            query,
            database,
            tracer: self.clone(),
        }
    }

    /// Get recent query timelines
    pub fn get_recent_timelines(&self, limit: usize) -> Vec<QueryTimeline> {
        if let Some(ref store) = self.timeline_store {
            store.get_recent(limit)
        } else {
            Vec::new()
        }
    }

    /// Get a specific query timeline by ID
    pub fn get_timeline(&self, query_id: &str) -> Option<QueryTimeline> {
        if let Some(ref store) = self.timeline_store {
            store.get(query_id)
        } else {
            None
        }
    }

    /// Get recent slow queries
    pub fn get_recent_slow_queries(&self, limit: usize) -> Vec<crate::observability::slow_query_logger::SlowQueryLog> {
        if let Some(ref logger) = self.slow_query_logger {
            logger.get_recent(limit)
        } else {
            Vec::new()
        }
    }

    /// Clear all stored data
    pub fn clear(&self) {
        if let Some(ref store) = self.timeline_store {
            store.clear();
        }
        if let Some(ref logger) = self.slow_query_logger {
            logger.clear();
        }
    }
}

impl Clone for QueryTracer {
    fn clone(&self) -> Self {
        Self {
            timeline_store: self.timeline_store.clone(),
            slow_query_logger: self.slow_query_logger.clone(),
            enabled: self.enabled,
        }
    }
}

/// Represents an active query trace
pub struct QueryTrace {
    span: Span,
    timeline: Option<TimelineBuilder>,
    start_instant: Instant,
    query: String,
    database: String,
    tracer: QueryTracer,
}

impl QueryTrace {
    /// Get the query ID for this trace
    pub fn query_id(&self) -> Option<String> {
        self.timeline.as_ref().map(|t| t.query_id().to_string())
    }

    /// Start a new phase in the query execution
    pub fn start_phase(&mut self, phase: &str) {
        let _enter = self.span.enter();
        let phase_span = span!(parent: &self.span, Level::DEBUG, "phase", phase = phase);
        let _phase_enter = phase_span.enter();

        if let Some(ref mut timeline) = self.timeline {
            timeline.start_event(phase);
        }
    }

    /// Add a completed phase with explicit duration
    pub fn add_phase(&mut self, phase: &str, duration: std::time::Duration) {
        let _enter = self.span.enter();

        if let Some(ref mut timeline) = self.timeline {
            timeline.add_event(phase, duration);
        }
    }

    /// Add metadata to the current trace
    pub fn add_metadata(&mut self, key: &str, value: impl Into<serde_json::Value>) {
        let _enter = self.span.enter();
        // Metadata is recorded in the span
        tracing::debug!(metadata_key = key, metadata_value = ?value.into(), "trace metadata");
    }

    /// Complete the query trace successfully
    pub fn complete(
        self,
        row_count: usize,
        partitions_scanned: usize,
        files_read: usize,
        bytes_scanned: u64,
    ) {
        let duration = self.start_instant.elapsed();
        let duration_ms = duration.as_millis() as u64;

        let _enter = self.span.enter();
        tracing::info!(
            duration_ms = duration_ms,
            row_count = row_count,
            partitions_scanned = partitions_scanned,
            files_read = files_read,
            bytes_scanned = bytes_scanned,
            "Query completed"
        );

        // Record slow query
        if let Some(ref logger) = self.tracer.slow_query_logger {
            logger.log_if_slow(
                self.query.clone(),
                self.database.clone(),
                duration_ms,
                Some(row_count),
                Some(partitions_scanned),
                Some(files_read),
                Some(bytes_scanned),
                None,
            );
        }

        // Store timeline
        if let Some(timeline) = self.timeline {
            if let Some(ref store) = self.tracer.timeline_store {
                let result_metadata = QueryResultMetadata {
                    row_count,
                    partitions_scanned,
                    files_read,
                    bytes_scanned,
                };
                let timeline = timeline.build(Some(result_metadata), None);
                store.insert(timeline);
            }
        }
    }

    /// Complete the query trace with an error
    pub fn complete_with_error(self, error: String) {
        let duration = self.start_instant.elapsed();
        let duration_ms = duration.as_millis() as u64;

        let _enter = self.span.enter();
        tracing::error!(
            duration_ms = duration_ms,
            error = %error,
            "Query failed"
        );

        // Record slow query with error
        if let Some(ref logger) = self.tracer.slow_query_logger {
            logger.log_if_slow(
                self.query.clone(),
                self.database.clone(),
                duration_ms,
                None,
                None,
                None,
                None,
                Some(error.clone()),
            );
        }

        // Store timeline with error
        if let Some(timeline) = self.timeline {
            if let Some(ref store) = self.tracer.timeline_store {
                let timeline = timeline.build(None, Some(error));
                store.insert(timeline);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_tracer() {
        let timeline_store = Arc::new(TimelineStore::new(100));
        let slow_query_logger = Arc::new(SlowQueryLogger::new(100, true));
        let tracer = QueryTracer::new(Some(timeline_store), Some(slow_query_logger));

        let mut trace = tracer.start_query("SELECT * FROM cpu".to_string(), "mydb".to_string());
        trace.start_phase("parse");
        std::thread::sleep(std::time::Duration::from_millis(10));

        trace.start_phase("execute");
        std::thread::sleep(std::time::Duration::from_millis(20));

        trace.complete(100, 2, 5, 1024);

        // Check that timeline was stored
        let timelines = tracer.get_recent_timelines(10);
        assert_eq!(timelines.len(), 1);
        assert!(timelines[0].total_duration_us > 30000); // At least 30ms
    }

    #[test]
    fn test_disabled_tracer() {
        let tracer = QueryTracer::disabled();
        assert!(!tracer.is_enabled());

        let trace = tracer.start_query("SELECT 1".to_string(), "mydb".to_string());
        trace.complete(1, 0, 0, 0);

        let timelines = tracer.get_recent_timelines(10);
        assert_eq!(timelines.len(), 0);
    }
}
