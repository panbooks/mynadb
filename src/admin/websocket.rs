//! WebSocket handler for real-time events

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::{interval, Duration};

use super::state::AdminState;

/// WebSocket upgrade handler
pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle WebSocket connection
async fn handle_socket(socket: WebSocket, state: AdminState) {
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to admin events
    let mut event_rx = state.subscribe();

    // Send heartbeat every 30 seconds
    let mut heartbeat_interval = interval(Duration::from_secs(30));

    // Spawn task to receive messages from client (for ping/pong)
    let receive_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Close(_) => break,
                Message::Ping(_data) => {
                    // Pong is handled automatically by axum
                    tracing::debug!("Received ping");
                }
                _ => {}
            }
        }
    });

    tracing::debug!("WebSocket client connected");

    // Main event loop
    loop {
        tokio::select! {
            // Broadcast event received
            Ok(event) = event_rx.recv() => {
                // The event payload already contains the full event data
                // Extract fields for the frontend format
                let id = event.payload.get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let timestamp = event.payload.get("timestamp")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let source = event.payload.get("source")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let message = event.payload.get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let data = event.payload.get("data")
                    .cloned()
                    .unwrap_or(json!({}));

                // Format timestamp as ISO string
                let timestamp_str = if timestamp > 0 {
                    let secs = timestamp / 1000;
                    let nsecs = ((timestamp % 1000) * 1_000_000) as u32;
                    chrono::DateTime::from_timestamp(secs, nsecs)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default()
                } else {
                    chrono::Utc::now().to_rfc3339()
                };

                let json = json!({
                    "id": id,
                    "type": event.event_type,
                    "timestamp": timestamp_str,
                    "source": source,
                    "message": message,
                    "payload": data,
                });

                if sender.send(Message::Text(json.to_string())).await.is_err() {
                    break;
                }
            }

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                let heartbeat = json!({
                    "type": "Heartbeat",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });

                if sender.send(Message::Text(heartbeat.to_string())).await.is_err() {
                    break;
                }
            }

            // Client disconnected
            else => break,
        }
    }

    // Clean up
    receive_task.abort();
    tracing::debug!("WebSocket connection closed");
}
