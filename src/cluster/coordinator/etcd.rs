//! etcd-based coordinator implementation
//!
//! Provides distributed coordination using etcd as the backend.

use super::super::config::CoordinatorConfig;
use super::interface::{Coordinator, CoordinatorType, Subscription, WatchEvent, WatchEventType};
use async_trait::async_trait;
use etcd_client::{
    Client, ConnectOptions, DeleteOptions, EventType, GetOptions, PutOptions, WatchOptions,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// etcd-based coordinator
pub struct EtcdCoordinator {
    /// etcd client
    client: Client,
    /// Configuration
    config: CoordinatorConfig,
    /// Active leases (key -> lease_id)
    leases: Arc<RwLock<HashMap<String, i64>>>,
    /// Lock holders (key -> holder)
    lock_holders: Arc<RwLock<HashMap<String, String>>>,
}

impl EtcdCoordinator {
    /// Create a new etcd coordinator
    pub async fn new(config: &CoordinatorConfig) -> Result<Self, String> {
        let mut connect_options = ConnectOptions::new()
            .with_connect_timeout(Duration::from_secs(config.connect_timeout_secs));

        // Add authentication if configured
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            connect_options = connect_options.with_user(username, password);
        }

        info!("Connecting to etcd at {:?}", config.endpoints);

        let client = Client::connect(&config.endpoints, Some(connect_options))
            .await
            .map_err(|e| format!("Failed to connect to etcd: {}", e))?;

        info!("Connected to etcd successfully");

        Ok(Self {
            client,
            config: config.clone(),
            leases: Arc::new(RwLock::new(HashMap::new())),
            lock_holders: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get the full lock key
    fn lock_key(&self, key: &str) -> String {
        format!("{}{}", self.config.lock_prefix, key)
    }

    /// Create a lease with the given TTL
    async fn create_lease(&self, ttl_secs: i64) -> Result<i64, String> {
        let lease_response = self
            .client
            .lease_client()
            .grant(ttl_secs, None)
            .await
            .map_err(|e| format!("Failed to create lease: {}", e))?;

        Ok(lease_response.id())
    }

    /// Start a keep-alive loop for a lease
    fn start_keep_alive(&self, lease_id: i64, interval: Duration) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                match client.lease_client().keep_alive(lease_id).await {
                    Ok((mut keeper, _)) => {
                        if let Err(e) = keeper.keep_alive().await {
                            warn!("Lease keep-alive failed: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to create keep-alive stream: {}", e);
                        break;
                    }
                }
            }
        })
    }
}

#[async_trait]
impl Coordinator for EtcdCoordinator {
    fn coordinator_type(&self) -> CoordinatorType {
        CoordinatorType::Etcd
    }

    async fn is_connected(&self) -> bool {
        // Try a simple operation to check connectivity
        self.client.clone().status().await.is_ok()
    }

    // ==================== Key-Value Operations ====================

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let response = self
            .client
            .kv_client()
            .get(key.as_bytes().to_vec(), None)
            .await
            .map_err(|e| format!("etcd get failed: {}", e))?;

        Ok(response.kvs().first().map(|kv| kv.value().to_vec()))
    }

    async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        let options = GetOptions::new().with_prefix();
        let response = self
            .client
            .kv_client()
            .get(prefix.as_bytes().to_vec(), Some(options))
            .await
            .map_err(|e| format!("etcd get_prefix failed: {}", e))?;

        Ok(response
            .kvs()
            .iter()
            .map(|kv| {
                (
                    kv.key_str().unwrap_or_default().to_string(),
                    kv.value().to_vec(),
                )
            })
            .collect())
    }

    async fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.client
            .kv_client()
            .put(key.as_bytes().to_vec(), value.to_vec(), None)
            .await
            .map_err(|e| format!("etcd put failed: {}", e))?;
        Ok(())
    }

    async fn put_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<i64, String> {
        let lease_id = self.create_lease(ttl.as_secs() as i64).await?;
        let options = PutOptions::new().with_lease(lease_id);
        self.client
            .kv_client()
            .put(key.as_bytes().to_vec(), value.to_vec(), Some(options))
            .await
            .map_err(|e| format!("etcd put_with_ttl failed: {}", e))?;
        Ok(lease_id)
    }

    async fn delete(&self, key: &str) -> Result<bool, String> {
        let response = self
            .client
            .kv_client()
            .delete(key.as_bytes().to_vec(), None)
            .await
            .map_err(|e| format!("etcd delete failed: {}", e))?;
        Ok(response.deleted() > 0)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<i64, String> {
        let options = DeleteOptions::new().with_prefix();
        let response = self
            .client
            .kv_client()
            .delete(prefix.as_bytes().to_vec(), Some(options))
            .await
            .map_err(|e| format!("etcd delete_prefix failed: {}", e))?;
        Ok(response.deleted())
    }

    // ==================== Distributed Locks ====================

    async fn try_lock(&self, key: &str, holder: &str, ttl: Duration) -> Result<bool, String> {
        let lock_key = self.lock_key(key);

        // Create a lease for the lock
        let lease_id = self.create_lease(ttl.as_secs() as i64).await?;

        // Try to acquire the lock using a transaction
        let lock_value = holder.as_bytes().to_vec();
        let put_options = PutOptions::new().with_lease(lease_id);

        // Check if key doesn't exist, then put
        let response = self
            .client
            .kv_client()
            .get(lock_key.as_bytes().to_vec(), None)
            .await
            .map_err(|e| format!("Failed to check lock: {}", e))?;

        if response.kvs().is_empty() {
            // Lock is not held, try to acquire
            self.client
                .kv_client()
                .put(lock_key.as_bytes().to_vec(), lock_value, Some(put_options))
                .await
                .map_err(|e| format!("Failed to acquire lock: {}", e))?;

            // Store the lease for renewal
            self.leases.write().insert(lock_key.clone(), lease_id);
            self.lock_holders
                .write()
                .insert(lock_key, holder.to_string());

            debug!("Lock acquired: key={}, holder={}", key, holder);
            Ok(true)
        } else {
            // Lock is already held
            // Revoke the lease we created
            let _ = self.client.lease_client().revoke(lease_id).await;
            debug!("Lock already held: key={}", key);
            Ok(false)
        }
    }

    async fn lock(
        &self,
        key: &str,
        holder: &str,
        ttl: Duration,
        timeout: Duration,
    ) -> Result<bool, String> {
        let start = std::time::Instant::now();
        let retry_interval = Duration::from_millis(100);

        while start.elapsed() < timeout {
            if self.try_lock(key, holder, ttl).await? {
                return Ok(true);
            }
            tokio::time::sleep(retry_interval).await;
        }

        Ok(false)
    }

    async fn unlock(&self, key: &str, holder: &str) -> Result<bool, String> {
        let lock_key = self.lock_key(key);

        // Check if we hold the lock
        let current_holder = self.lock_holders.read().get(&lock_key).cloned();
        if current_holder.as_deref() != Some(holder) {
            debug!("Cannot unlock: not the holder");
            return Ok(false);
        }

        // Delete the lock key
        let deleted = self.delete(&lock_key).await?;

        if deleted {
            // Get and remove the lease ID before awaiting
            let lease_id = self.leases.write().remove(&lock_key);

            // Revoke the lease
            if let Some(lid) = lease_id {
                let _ = self.client.lease_client().revoke(lid).await;
            }
            self.lock_holders.write().remove(&lock_key);
            debug!("Lock released: key={}, holder={}", key, holder);
        }

        Ok(deleted)
    }

    async fn renew_lock(&self, key: &str, holder: &str, _ttl: Duration) -> Result<bool, String> {
        let lock_key = self.lock_key(key);

        // Check if we hold the lock and get the lease_id before awaiting
        let (is_holder, lease_id) = {
            let current_holder = self.lock_holders.read().get(&lock_key).cloned();
            let lease_id = self.leases.read().get(&lock_key).copied();
            (current_holder.as_deref() == Some(holder), lease_id)
        };

        if !is_holder {
            return Ok(false);
        }

        // Keep the lease alive
        if let Some(lid) = lease_id {
            self.keep_alive(lid).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn is_lock_held(&self, key: &str, holder: &str) -> Result<bool, String> {
        let lock_key = self.lock_key(key);
        match self.get(&lock_key).await? {
            Some(value) => {
                let current_holder = String::from_utf8_lossy(&value);
                Ok(current_holder == holder)
            }
            None => Ok(false),
        }
    }

    // ==================== Watch ====================

    async fn watch(&self, key: &str) -> Result<Subscription, String> {
        let (tx, rx) = mpsc::channel(100);
        let key = key.to_string();
        let mut client = self.client.clone();

        let handle = tokio::spawn(async move {
            match client.watch(key.as_bytes().to_vec(), None).await {
                Ok((mut watcher, mut stream)) => {
                    while let Some(response) = stream.message().await.ok().flatten() {
                        for event in response.events() {
                            let watch_event = WatchEvent {
                                event_type: match event.event_type() {
                                    EventType::Put => WatchEventType::Put,
                                    EventType::Delete => WatchEventType::Delete,
                                },
                                key: event
                                    .kv()
                                    .map(|kv| kv.key_str().unwrap_or_default().to_string())
                                    .unwrap_or_default(),
                                value: event.kv().map(|kv| kv.value().to_vec()),
                                prev_value: event.prev_kv().map(|kv| kv.value().to_vec()),
                                mod_revision: event.kv().map(|kv| kv.mod_revision()).unwrap_or(0),
                            };
                            if tx.send(watch_event).await.is_err() {
                                break;
                            }
                        }
                    }
                    let _ = watcher.cancel().await;
                }
                Err(e) => {
                    error!("Watch failed: {}", e);
                }
            }
        });

        Ok(Subscription::new(rx, Some(handle)))
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<Subscription, String> {
        let (tx, rx) = mpsc::channel(100);
        let prefix = prefix.to_string();
        let mut client = self.client.clone();

        let handle = tokio::spawn(async move {
            let options = WatchOptions::new().with_prefix();
            match client
                .watch(prefix.as_bytes().to_vec(), Some(options))
                .await
            {
                Ok((mut watcher, mut stream)) => {
                    while let Some(response) = stream.message().await.ok().flatten() {
                        for event in response.events() {
                            let watch_event = WatchEvent {
                                event_type: match event.event_type() {
                                    EventType::Put => WatchEventType::Put,
                                    EventType::Delete => WatchEventType::Delete,
                                },
                                key: event
                                    .kv()
                                    .map(|kv| kv.key_str().unwrap_or_default().to_string())
                                    .unwrap_or_default(),
                                value: event.kv().map(|kv| kv.value().to_vec()),
                                prev_value: event.prev_kv().map(|kv| kv.value().to_vec()),
                                mod_revision: event.kv().map(|kv| kv.mod_revision()).unwrap_or(0),
                            };
                            if tx.send(watch_event).await.is_err() {
                                break;
                            }
                        }
                    }
                    let _ = watcher.cancel().await;
                }
                Err(e) => {
                    error!("Watch prefix failed: {}", e);
                }
            }
        });

        Ok(Subscription::new(rx, Some(handle)))
    }

    // ==================== Pub/Sub ====================

    async fn publish(&self, channel: &str, message: &[u8]) -> Result<(), String> {
        // etcd doesn't have native pub/sub, so we use a key with TTL
        // Subscribers watch the key
        let key = format!(
            "{}{}/{}",
            self.config.cache_prefix,
            channel,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        );
        self.put_with_ttl(&key, message, Duration::from_secs(60))
            .await?;
        Ok(())
    }

    async fn subscribe(&self, channel: &str) -> Result<Subscription, String> {
        let prefix = format!("{}{}/", self.config.cache_prefix, channel);
        self.watch_prefix(&prefix).await
    }

    // ==================== Leader Election ====================

    async fn campaign(
        &self,
        election_key: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<i64, String> {
        let key = format!("{}{}", self.config.lock_prefix, election_key);

        // Create a lease
        let lease_id = self.create_lease(ttl.as_secs() as i64).await?;

        // Start keep-alive
        let interval = Duration::from_secs(ttl.as_secs() / 3);
        let _handle = self.start_keep_alive(lease_id, interval);

        // Try to put our node ID with the lease
        let put_options = PutOptions::new().with_lease(lease_id);

        loop {
            // Check if there's a current leader
            let response = self
                .client
                .kv_client()
                .get(key.as_bytes().to_vec(), None)
                .await
                .map_err(|e| format!("Failed to check leader: {}", e))?;

            if response.kvs().is_empty() {
                // No leader, try to become one
                self.client
                    .kv_client()
                    .put(
                        key.as_bytes().to_vec(),
                        node_id.as_bytes().to_vec(),
                        Some(put_options.clone()),
                    )
                    .await
                    .map_err(|e| format!("Failed to campaign: {}", e))?;

                // Verify we became leader
                let verify = self
                    .client
                    .kv_client()
                    .get(key.as_bytes().to_vec(), None)
                    .await
                    .map_err(|e| format!("Failed to verify leadership: {}", e))?;

                if let Some(kv) = verify.kvs().first() {
                    if kv.value() == node_id.as_bytes() {
                        info!("Elected as leader: node_id={}", node_id);
                        return Ok(lease_id);
                    }
                }
            }

            // Not leader, wait and retry
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn resign(&self, election_key: &str, lease_id: i64) -> Result<(), String> {
        let key = format!("{}{}", self.config.lock_prefix, election_key);

        // Delete the leadership key
        self.delete(&key).await?;

        // Revoke the lease
        self.client
            .lease_client()
            .revoke(lease_id)
            .await
            .map_err(|e| format!("Failed to revoke lease: {}", e))?;

        info!("Resigned from leadership");
        Ok(())
    }

    async fn get_leader(&self, election_key: &str) -> Result<Option<String>, String> {
        let key = format!("{}{}", self.config.lock_prefix, election_key);

        match self.get(&key).await? {
            Some(value) => {
                let leader = String::from_utf8_lossy(&value).to_string();
                Ok(Some(leader))
            }
            None => Ok(None),
        }
    }

    async fn is_leader(&self, election_key: &str, node_id: &str) -> Result<bool, String> {
        match self.get_leader(election_key).await? {
            Some(leader) => Ok(leader == node_id),
            None => Ok(false),
        }
    }

    async fn keep_alive(&self, lease_id: i64) -> Result<(), String> {
        let (mut keeper, _) = self
            .client
            .lease_client()
            .keep_alive(lease_id)
            .await
            .map_err(|e| format!("Failed to create keep-alive: {}", e))?;

        keeper
            .keep_alive()
            .await
            .map_err(|e| format!("Keep-alive failed: {}", e))?;

        Ok(())
    }
}
