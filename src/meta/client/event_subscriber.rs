//! Event Subscriber for Meta service events
//!
//! This module provides a client for subscribing to Meta service events.
//! It handles reconnection and ensures no events are missed via sequence tracking.

use crate::meta::proto;
use crate::meta::types::EventType;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Event from the Meta service
#[derive(Debug, Clone)]
pub struct MetaEvent {
    pub sequence: u64,
    pub event_type: EventType,
    pub topic: String,
    pub payload: Vec<u8>,
    pub source_node: String,
    pub timestamp: i64,
}

impl From<proto::Event> for MetaEvent {
    fn from(e: proto::Event) -> Self {
        Self {
            sequence: e.sequence,
            event_type: EventType::from(e.event_type),
            topic: e.topic,
            payload: e.payload,
            source_node: e.source_node,
            timestamp: e.timestamp,
        }
    }
}

/// Event handler callback
pub type EventHandler = Box<dyn Fn(MetaEvent) + Send + Sync>;

/// Event subscriber configuration
#[derive(Clone)]
pub struct SubscriberConfig {
    /// Meta service endpoints
    pub endpoints: Vec<String>,
    /// Topics to subscribe to (empty = all topics)
    pub topics: Vec<String>,
    /// Reconnect delay on connection failure
    pub reconnect_delay_ms: u64,
    /// Connection timeout
    pub connect_timeout_ms: u64,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["127.0.0.1:7100".to_string()],
            topics: Vec::new(),
            reconnect_delay_ms: 1000,
            connect_timeout_ms: 5000,
        }
    }
}

/// Event subscriber for Meta service
pub struct EventSubscriber {
    /// Configuration
    config: SubscriberConfig,
    /// Last seen sequence number
    last_sequence: Arc<AtomicU64>,
    /// Shutdown signal
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl EventSubscriber {
    pub fn new(config: SubscriberConfig) -> Self {
        Self {
            config,
            last_sequence: Arc::new(AtomicU64::new(0)),
            shutdown_tx: None,
        }
    }

    /// Start subscribing to events
    ///
    /// Returns a receiver channel for events and a handle to stop the subscription.
    pub fn start(
        &mut self,
        initial_sequence: u64,
    ) -> (mpsc::Receiver<MetaEvent>, EventSubscriberHandle) {
        let (event_tx, event_rx) = mpsc::channel(1000);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        self.last_sequence.store(initial_sequence, Ordering::SeqCst);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let last_sequence = Arc::clone(&self.last_sequence);

        tokio::spawn(async move {
            Self::subscription_loop(config, last_sequence, event_tx, shutdown_rx).await;
        });

        let handle = EventSubscriberHandle {
            last_sequence: Arc::clone(&self.last_sequence),
        };

        (event_rx, handle)
    }

    /// Main subscription loop with reconnection
    async fn subscription_loop(
        config: SubscriberConfig,
        last_sequence: Arc<AtomicU64>,
        event_tx: mpsc::Sender<MetaEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut current_endpoint_idx = 0;

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                info!("Event subscriber shutting down");
                break;
            }

            let endpoint = &config.endpoints[current_endpoint_idx];
            info!("Connecting to Meta service at {}", endpoint);

            match Self::connect_and_subscribe(
                endpoint,
                &config.topics,
                &last_sequence,
                &event_tx,
                &mut shutdown_rx,
                config.connect_timeout_ms,
            )
            .await
            {
                Ok(()) => {
                    info!("Subscription ended gracefully");
                    break;
                }
                Err(e) => {
                    warn!("Subscription error: {}", e);
                    // Try next endpoint
                    current_endpoint_idx = (current_endpoint_idx + 1) % config.endpoints.len();
                    tokio::time::sleep(std::time::Duration::from_millis(config.reconnect_delay_ms))
                        .await;
                }
            }
        }
    }

    /// Connect to a Meta service endpoint and subscribe
    async fn connect_and_subscribe(
        endpoint: &str,
        topics: &[String],
        last_sequence: &Arc<AtomicU64>,
        event_tx: &mpsc::Sender<MetaEvent>,
        shutdown_rx: &mut tokio::sync::oneshot::Receiver<()>,
        connect_timeout_ms: u64,
    ) -> Result<(), String> {
        // Create channel
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", endpoint))
            .map_err(|e| format!("Invalid endpoint: {}", e))?
            .connect_timeout(std::time::Duration::from_millis(connect_timeout_ms))
            .connect()
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        let mut client = proto::meta_service_client::MetaServiceClient::new(channel);

        // Create subscription request
        let request = proto::SubscribeRequest {
            topics: topics.to_vec(),
            start_sequence: last_sequence.load(Ordering::SeqCst),
        };

        info!(
            "Subscribing to topics {:?} from sequence {}",
            topics, request.start_sequence
        );

        // Start streaming
        let response = client
            .subscribe(request)
            .await
            .map_err(|e| format!("Subscribe failed: {}", e))?;

        let mut stream = response.into_inner();

        // Process events
        loop {
            tokio::select! {
                biased;

                // Check shutdown first (biased selection)
                result = &mut *shutdown_rx => {
                    match result {
                        Ok(()) | Err(_) => return Ok(()),
                    }
                }
                event = stream.next() => {
                    match event {
                        Some(Ok(proto_event)) => {
                            let seq = proto_event.sequence;
                            let meta_event = MetaEvent::from(proto_event);

                            debug!("Received event: seq={}, type={:?}, topic={}",
                                seq, meta_event.event_type, meta_event.topic);

                            // Update last sequence
                            last_sequence.store(seq + 1, Ordering::SeqCst);

                            // Send to handler
                            if event_tx.send(meta_event).await.is_err() {
                                warn!("Event receiver dropped");
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => {
                            return Err(format!("Stream error: {}", e));
                        }
                        None => {
                            return Err("Stream ended".to_string());
                        }
                    }
                }
            }
        }
    }

    /// Get the last seen sequence number
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }
}

impl Drop for EventSubscriber {
    fn drop(&mut self) {
        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Handle for managing the event subscription
pub struct EventSubscriberHandle {
    last_sequence: Arc<AtomicU64>,
}

impl EventSubscriberHandle {
    /// Get the last seen sequence number
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }
}
