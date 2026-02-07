//! Streaming query subscriptions
//!
//! Enables real-time push-based query result delivery via WebSocket and SSE.
//! Clients subscribe to a SQL query; when new data is written to tables
//! referenced by that query, the query is re-executed and results pushed.

use crate::auth::AuthContext;
use crate::config::StreamingConfig;
use crate::query::query_engine::QueryEngine;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Event emitted when data is written
#[derive(Debug, Clone)]
pub struct WriteEvent {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// A query result update sent to subscribers
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum StreamMessage {
    /// Initial full query result (data is a JSON array string)
    #[serde(rename = "initial")]
    Initial { data: String },
    /// Incremental update (re-executed query result, JSON array string)
    #[serde(rename = "update")]
    Update { data: String },
    /// Error during query execution
    #[serde(rename = "error")]
    Error { message: String },
    /// Heartbeat keepalive
    #[serde(rename = "heartbeat")]
    Heartbeat { timestamp: String },
}

/// Client request to subscribe to a streaming query
#[derive(Debug, Clone, Deserialize)]
pub struct StreamQueryRequest {
    pub database: String,
    pub query: String,
}

/// Internal subscription state
struct Subscription {
    id: Uuid,
    query: String,
    database: String,
    /// Tables referenced by this query (extracted from SQL)
    tables: Vec<String>,
    sender: mpsc::Sender<StreamMessage>,
}

/// Handle returned to the subscriber for receiving updates
pub struct SubscriptionHandle {
    pub id: Uuid,
    pub receiver: mpsc::Receiver<StreamMessage>,
}

/// Info about an active subscription (for management API)
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionInfo {
    pub id: String,
    pub database: String,
    pub query: String,
    pub tables: Vec<String>,
}

/// Manages streaming query subscriptions and dispatches write notifications.
pub struct StreamingQueryManager {
    subscriptions: DashMap<Uuid, Subscription>,
    query_engine: Arc<QueryEngine>,
    config: StreamingConfig,
    total_count: AtomicUsize,
}

impl StreamingQueryManager {
    /// Create a new streaming query manager
    pub fn new(query_engine: Arc<QueryEngine>, config: StreamingConfig) -> Self {
        Self {
            subscriptions: DashMap::new(),
            query_engine,
            config,
            total_count: AtomicUsize::new(0),
        }
    }

    /// Subscribe to a streaming query. Returns a handle for receiving updates.
    pub async fn subscribe(
        &self,
        database: &str,
        sql: &str,
        auth_ctx: &AuthContext,
    ) -> Result<SubscriptionHandle, String> {
        // Check global limit
        let count = self.total_count.load(Ordering::Relaxed);
        if count >= self.config.max_subscriptions {
            return Err(format!(
                "Maximum subscriptions reached ({})",
                self.config.max_subscriptions
            ));
        }

        // Check auth
        if auth_ctx.is_auth_enabled() && !auth_ctx.can_access_database(database) {
            return Err(format!("Access denied to database: {}", database));
        }

        // Extract table references from the query
        let tables = crate::query::sql_utils::extract_table_references(sql, database)
            .into_iter()
            .map(|(_, _, t)| t)
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel(self.config.backpressure_threshold);
        let id = Uuid::new_v4();

        // Execute initial query
        let initial_results = self
            .query_engine
            .execute_query_json_with_db_auth(sql, database, auth_ctx)
            .await;

        match initial_results {
            Ok(json_str) => {
                let _ = tx.send(StreamMessage::Initial { data: json_str }).await;
            }
            Err(e) => {
                let _ = tx
                    .send(StreamMessage::Error {
                        message: e.clone(),
                    })
                    .await;
                return Err(e);
            }
        }

        self.subscriptions.insert(
            id,
            Subscription {
                id,
                query: sql.to_string(),
                database: database.to_string(),
                tables,
                sender: tx,
            },
        );
        self.total_count.fetch_add(1, Ordering::Relaxed);

        debug!(
            "New streaming subscription {}: db={}, query={}",
            id, database, sql
        );

        Ok(SubscriptionHandle { id, receiver: rx })
    }

    /// Remove a subscription
    pub fn unsubscribe(&self, id: &Uuid) -> bool {
        if self.subscriptions.remove(id).is_some() {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            debug!("Subscription removed: {}", id);
            true
        } else {
            false
        }
    }

    /// Called when data is written. Re-executes affected queries and pushes results.
    pub async fn on_data_written(&self, event: WriteEvent) {
        if self.subscriptions.is_empty() {
            return;
        }

        // Collect affected subscription IDs
        let affected: Vec<(Uuid, String, String)> = self
            .subscriptions
            .iter()
            .filter(|entry| {
                let sub = entry.value();
                sub.database == event.catalog
                    && sub.tables.iter().any(|t| t == &event.table)
            })
            .map(|entry| {
                let sub = entry.value();
                (sub.id, sub.database.clone(), sub.query.clone())
            })
            .collect();

        if affected.is_empty() {
            return;
        }

        debug!(
            "Write to {}.{}.{} affects {} subscriptions",
            event.catalog,
            event.schema,
            event.table,
            affected.len()
        );

        for (id, database, query) in affected {
            let result = self
                .query_engine
                .execute_query_json_with_db(&query, &database)
                .await;

            match result {
                Ok(json_str) => {
                    if let Some(sub) = self.subscriptions.get(&id) {
                        // Use try_send for backpressure — if buffer full, skip this update
                        if sub
                            .sender
                            .try_send(StreamMessage::Update { data: json_str })
                            .is_err()
                        {
                            warn!("Subscription {} buffer full, dropping update", id);
                        }
                    }
                }
                Err(e) => {
                    if let Some(sub) = self.subscriptions.get(&id) {
                        let _ = sub.sender.try_send(StreamMessage::Error { message: e });
                    }
                }
            }
        }
    }

    /// List all active subscriptions
    pub fn list_subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions
            .iter()
            .map(|entry| {
                let sub = entry.value();
                SubscriptionInfo {
                    id: sub.id.to_string(),
                    database: sub.database.clone(),
                    query: sub.query.clone(),
                    tables: sub.tables.clone(),
                }
            })
            .collect()
    }

    /// Get the total number of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Remove subscriptions with closed channels (client disconnected)
    pub fn cleanup_closed(&self) {
        let closed: Vec<Uuid> = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().sender.is_closed())
            .map(|entry| *entry.key())
            .collect();

        for id in &closed {
            self.subscriptions.remove(id);
        }

        if !closed.is_empty() {
            let removed = closed.len();
            self.total_count.fetch_sub(removed, Ordering::Relaxed);
            debug!("Cleaned up {} closed subscriptions", removed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_event() {
        let event = WriteEvent {
            catalog: "mydb".to_string(),
            schema: "default".to_string(),
            table: "cpu".to_string(),
        };
        assert_eq!(event.catalog, "mydb");
        assert_eq!(event.table, "cpu");
    }

    #[test]
    fn test_stream_message_serialization() {
        let msg = StreamMessage::Initial {
            data: r#"[{"cpu": 85.2}]"#.to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("initial"));
        assert!(json.contains("85.2"));

        let msg = StreamMessage::Heartbeat {
            timestamp: "2026-02-07T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("heartbeat"));

        let msg = StreamMessage::Error {
            message: "test error".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("error"));
        assert!(json.contains("test error"));
    }

    #[test]
    fn test_stream_query_request_deserialization() {
        let json = r#"{"database": "mydb", "query": "SELECT * FROM cpu"}"#;
        let req: StreamQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.database, "mydb");
        assert_eq!(req.query, "SELECT * FROM cpu");
    }

    #[test]
    fn test_subscription_info_serialization() {
        let info = SubscriptionInfo {
            id: "test-id".to_string(),
            database: "mydb".to_string(),
            query: "SELECT * FROM cpu".to_string(),
            tables: vec!["cpu".to_string()],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("test-id"));
        assert!(json.contains("mydb"));
    }

    #[test]
    fn test_default_config() {
        let config = StreamingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_subscriptions, 10000);
        assert_eq!(config.max_subscriptions_per_connection, 10);
        assert_eq!(config.update_debounce_ms, 100);
        assert_eq!(config.backpressure_threshold, 1000);
        assert_eq!(config.heartbeat_interval_secs, 30);
    }
}
