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

use super::state::{AdminEvent, AdminState};

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

    // Main event loop
    loop {
        tokio::select! {
            // Broadcast event received
            Ok(event) = event_rx.recv() => {
                let json = json!({
                    "type": event.event_type,
                    "payload": event.payload,
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                });

                if sender.send(Message::Text(json.to_string())).await.is_err() {
                    break;
                }
            }

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                let heartbeat = json!({
                    "type": "heartbeat",
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
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
