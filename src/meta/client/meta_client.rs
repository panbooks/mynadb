//! Client for connecting to Meta service

use crate::meta::proto;
use crate::meta::types::*;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

/// Configuration for MetaClient
#[derive(Debug, Clone)]
pub struct MetaClientConfig {
    /// Meta service endpoints (e.g., ["meta1:7100", "meta2:7100", "meta3:7100"])
    pub endpoints: Vec<String>,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Retry count for failed requests
    pub max_retries: u32,
    /// Retry backoff
    pub retry_backoff: Duration,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["127.0.0.1:7100".to_string()],
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

/// Client for Meta service
pub struct MetaClient {
    /// Configuration
    config: MetaClientConfig,
    /// Current connection (to leader if known)
    client: RwLock<Option<proto::meta_service_client::MetaServiceClient<Channel>>>,
    /// Current endpoint index
    current_endpoint: RwLock<usize>,
    /// Node ID for this client (used as holder for locks)
    node_id: String,
}

impl MetaClient {
    /// Create a new MetaClient
    pub fn new(config: MetaClientConfig, node_id: String) -> Self {
        Self {
            config,
            client: RwLock::new(None),
            current_endpoint: RwLock::new(0),
            node_id,
        }
    }

    /// Create and connect to Meta service
    ///
    /// This is a convenience method that creates a MetaClient and verifies
    /// connectivity by establishing a connection.
    pub async fn connect(endpoints: &[String]) -> Result<Self, String> {
        let config = MetaClientConfig {
            endpoints: endpoints.to_vec(),
            ..Default::default()
        };
        let node_id = format!(
            "client-{}",
            uuid::Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("unknown")
        );
        let client = Self::new(config, node_id);

        // Verify connectivity
        client.get_client().await?;

        Ok(client)
    }

    /// Get or create a connection to Meta service
    async fn get_client(
        &self,
    ) -> Result<proto::meta_service_client::MetaServiceClient<Channel>, String> {
        // Check if we have a cached connection
        {
            let client = self.client.read();
            if let Some(ref c) = *client {
                return Ok(c.clone());
            }
        }

        // Try to connect to each endpoint
        let endpoints = &self.config.endpoints;
        let start_idx = *self.current_endpoint.read();

        for i in 0..endpoints.len() {
            let idx = (start_idx + i) % endpoints.len();
            let addr = &endpoints[idx];

            match self.connect_to(addr).await {
                Ok(client) => {
                    *self.client.write() = Some(client.clone());
                    *self.current_endpoint.write() = idx;
                    info!("Connected to Meta service at {}", addr);
                    return Ok(client);
                }
                Err(e) => {
                    warn!("Failed to connect to Meta at {}: {}", addr, e);
                }
            }
        }

        Err("Failed to connect to any Meta endpoint".to_string())
    }

    /// Connect to a specific endpoint
    async fn connect_to(
        &self,
        addr: &str,
    ) -> Result<proto::meta_service_client::MetaServiceClient<Channel>, String> {
        let endpoint = Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("Invalid endpoint: {}", e))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        Ok(proto::meta_service_client::MetaServiceClient::new(channel))
    }

    /// Clear the cached connection (on error)
    fn clear_connection(&self) {
        *self.client.write() = None;
    }

    /// Execute with retry logic
    async fn with_retry<T, F, Fut>(&self, op_name: &str, f: F) -> Result<T, String>
    where
        F: Fn(proto::meta_service_client::MetaServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        let mut last_error = String::new();

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_backoff * attempt).await;
            }

            let client = match self.get_client().await {
                Ok(c) => c,
                Err(e) => {
                    last_error = e;
                    continue;
                }
            };

            match f(client).await {
                Ok(result) => return Ok(result),
                Err(status) => {
                    last_error = format!("{}: {}", status.code(), status.message());
                    warn!(
                        "Meta {} failed (attempt {}): {}",
                        op_name,
                        attempt + 1,
                        last_error
                    );

                    // Clear connection on certain errors
                    if status.code() == tonic::Code::Unavailable
                        || status.code() == tonic::Code::Internal
                    {
                        self.clear_connection();
                    }
                }
            }
        }

        Err(format!(
            "{} failed after {} retries: {}",
            op_name,
            self.config.max_retries + 1,
            last_error
        ))
    }

    // ========================================================================
    // Node Management
    // ========================================================================

    /// Register this node with Meta service
    pub async fn register_node(&self, info: NodeInfo, lease_ttl_ms: u64) -> Result<u64, String> {
        let proto_info = proto::NodeInfo {
            node_id: info.node_id,
            role: i32::from(info.role),
            state: i32::from(info.state),
            advertise_addr: info.advertise_addr,
            http_addr: info.http_addr.unwrap_or_default(),
            flight_addr: info.flight_addr.unwrap_or_default(),
            registered_at: info.registered_at,
            last_heartbeat: info.last_heartbeat,
            labels: info.labels,
        };

        let request = proto::RegisterNodeRequest {
            info: Some(proto_info),
            lease_ttl_ms,
        };

        let response = self
            .with_retry("register_node", |mut client| {
                let req = request.clone();
                async move { client.register_node(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.lease_id)
        } else {
            Err(response.error)
        }
    }

    /// Send heartbeat
    pub async fn heartbeat(&self, lease_id: u64) -> Result<i64, String> {
        let request = proto::HeartbeatRequest {
            node_id: self.node_id.clone(),
            lease_id,
            timestamp: current_timestamp_ms(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            active_connections: 0,
        };

        let response = self
            .with_retry("heartbeat", |mut client| {
                let req = request.clone();
                async move { client.heartbeat(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.next_heartbeat_ms)
        } else {
            Err(response.error)
        }
    }

    /// Deregister node
    pub async fn deregister_node(&self, lease_id: u64) -> Result<(), String> {
        let request = proto::DeregisterNodeRequest {
            node_id: self.node_id.clone(),
            lease_id,
        };

        let response = self
            .with_retry("deregister_node", |mut client| {
                let req = request.clone();
                async move { client.deregister_node(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(response.error)
        }
    }

    /// List nodes
    pub async fn list_nodes(
        &self,
        role: Option<NodeRole>,
        state: Option<NodeState>,
    ) -> Result<Vec<NodeInfo>, String> {
        let request = proto::ListNodesRequest {
            role_filter: role.map(i32::from).unwrap_or(0),
            state_filter: state.map(i32::from).unwrap_or(0),
        };

        let response = self
            .with_retry("list_nodes", |mut client| {
                let req = request.clone();
                async move { client.list_nodes(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        Ok(response
            .nodes
            .into_iter()
            .map(|n| NodeInfo {
                node_id: n.node_id,
                role: NodeRole::from(n.role),
                state: NodeState::from(n.state),
                advertise_addr: n.advertise_addr,
                http_addr: if n.http_addr.is_empty() {
                    None
                } else {
                    Some(n.http_addr)
                },
                flight_addr: if n.flight_addr.is_empty() {
                    None
                } else {
                    Some(n.flight_addr)
                },
                registered_at: n.registered_at,
                last_heartbeat: n.last_heartbeat,
                labels: n.labels,
            })
            .collect())
    }

    // ========================================================================
    // File Registry
    // ========================================================================

    /// Register files with Meta
    pub async fn register_files(&self, files: Vec<FileInfo>) -> Result<u64, String> {
        let proto_files: Vec<proto::FileInfo> = files
            .into_iter()
            .map(|f| proto::FileInfo {
                path: f.path,
                catalog: f.catalog,
                schema: f.schema,
                table: f.table,
                partition: f.partition,
                level: f.level as u32,
                size_bytes: f.size_bytes,
                row_count: f.row_count,
                min_time: f.min_time,
                max_time: f.max_time,
                created_at: f.created_at,
                created_by: f.created_by,
            })
            .collect();

        let request = proto::RegisterFilesRequest { files: proto_files };

        let response = self
            .with_retry("register_files", |mut client| {
                let req = request.clone();
                async move { client.register_files(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.sequence)
        } else {
            Err(response.error)
        }
    }

    /// Remove files from Meta
    pub async fn remove_files(&self, paths: Vec<String>) -> Result<u64, String> {
        let request = proto::RemoveFilesRequest { paths };

        let response = self
            .with_retry("remove_files", |mut client| {
                let req = request.clone();
                async move { client.remove_files(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.sequence)
        } else {
            Err(response.error)
        }
    }

    /// List files for a partition
    pub async fn list_files_for_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition: &str,
    ) -> Result<Vec<FileInfo>, String> {
        let request = proto::ListFilesRequest {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            partition: partition.to_string(),
            level: 0,
        };

        let response = self
            .with_retry("list_files_for_partition", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .list_files_for_partition(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        Ok(response
            .files
            .into_iter()
            .map(|f| FileInfo {
                path: f.path,
                catalog: f.catalog,
                schema: f.schema,
                table: f.table,
                partition: f.partition,
                level: f.level as u8,
                size_bytes: f.size_bytes,
                row_count: f.row_count,
                min_time: f.min_time,
                max_time: f.max_time,
                created_at: f.created_at,
                created_by: f.created_by,
            })
            .collect())
    }

    /// Get files for query (with time range filtering)
    pub async fn get_files_for_query(
        &self,
        catalog: &str,
        tables: Vec<String>,
        min_time: Option<i64>,
        max_time: Option<i64>,
    ) -> Result<(Vec<FileInfo>, Vec<String>), String> {
        let request = proto::QueryFilesRequest {
            catalog: catalog.to_string(),
            tables,
            min_time: min_time.unwrap_or(0),
            max_time: max_time.unwrap_or(0),
        };

        let response = self
            .with_retry("get_files_for_query", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .get_files_for_query(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        let files: Vec<FileInfo> = response
            .files
            .into_iter()
            .map(|f| FileInfo {
                path: f.path,
                catalog: f.catalog,
                schema: f.schema,
                table: f.table,
                partition: f.partition,
                level: f.level as u8,
                size_bytes: f.size_bytes,
                row_count: f.row_count,
                min_time: f.min_time,
                max_time: f.max_time,
                created_at: f.created_at,
                created_by: f.created_by,
            })
            .collect();

        Ok((files, response.nodes))
    }

    // ========================================================================
    // Distributed Locks
    // ========================================================================

    /// Try to acquire a lock
    pub async fn try_lock(&self, key: &str, ttl_ms: u64) -> Result<Option<u64>, String> {
        let request = proto::TryLockRequest {
            key: key.to_string(),
            holder: self.node_id.clone(),
            ttl_ms,
        };

        let response = self
            .with_retry("try_lock", |mut client| {
                let req = request.clone();
                async move { client.try_lock(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if !response.error.is_empty() {
            return Err(response.error);
        }

        if response.acquired {
            Ok(Some(response.fencing_token))
        } else {
            Ok(None)
        }
    }

    /// Release a lock
    pub async fn unlock(&self, key: &str, fencing_token: u64) -> Result<(), String> {
        let request = proto::UnlockRequest {
            key: key.to_string(),
            holder: self.node_id.clone(),
            fencing_token,
        };

        let response = self
            .with_retry("unlock", |mut client| {
                let req = request.clone();
                async move { client.unlock(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(response.error)
        }
    }

    /// Renew a lock
    pub async fn renew_lock(
        &self,
        key: &str,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Result<i64, String> {
        let request = proto::RenewLockRequest {
            key: key.to_string(),
            holder: self.node_id.clone(),
            fencing_token,
            ttl_ms,
        };

        let response = self
            .with_retry("renew_lock", |mut client| {
                let req = request.clone();
                async move { client.renew_lock(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.expires_at)
        } else {
            Err(response.error)
        }
    }

    /// Get lock info
    pub async fn get_lock(&self, key: &str) -> Result<Option<LockInfo>, String> {
        let request = proto::GetLockRequest {
            key: key.to_string(),
        };

        let response = self
            .with_retry("get_lock", |mut client| {
                let req = request.clone();
                async move { client.get_lock(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.held {
            Ok(Some(LockInfo {
                key: key.to_string(),
                holder: response.holder,
                fencing_token: response.fencing_token,
                acquired_at: 0, // Not returned by API
                expires_at: response.expires_at,
            }))
        } else {
            Ok(None)
        }
    }

    // ========================================================================
    // Leader Election
    // ========================================================================

    /// Campaign for leadership
    pub async fn campaign(&self, election: &str, ttl_ms: u64) -> Result<Option<u64>, String> {
        let request = proto::CampaignRequest {
            election: election.to_string(),
            node_id: self.node_id.clone(),
            lease_ttl_ms: ttl_ms,
        };

        let response = self
            .with_retry("campaign", |mut client| {
                let req = request.clone();
                async move { client.campaign(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if !response.error.is_empty() {
            return Err(response.error);
        }

        if response.elected {
            Ok(Some(response.term))
        } else {
            Ok(None)
        }
    }

    /// Resign from leadership
    pub async fn resign(&self, election: &str, term: u64) -> Result<(), String> {
        let request = proto::ResignRequest {
            election: election.to_string(),
            node_id: self.node_id.clone(),
            term,
        };

        let response = self
            .with_retry("resign", |mut client| {
                let req = request.clone();
                async move { client.resign(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(response.error)
        }
    }

    /// Get current leader
    pub async fn get_leader(&self, election: &str) -> Result<Option<(String, u64)>, String> {
        let request = proto::GetLeaderRequest {
            election: election.to_string(),
        };

        let response = self
            .with_retry("get_leader", |mut client| {
                let req = request.clone();
                async move { client.get_leader(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.has_leader {
            Ok(Some((response.leader, response.term)))
        } else {
            Ok(None)
        }
    }

    // ========================================================================
    // Events
    // ========================================================================

    /// Subscribe to events
    pub async fn subscribe(
        &self,
        topics: Vec<String>,
        start_sequence: u64,
    ) -> Result<mpsc::Receiver<Event>, String> {
        let mut client = self.get_client().await?;

        let request = proto::SubscribeRequest {
            topics,
            start_sequence,
        };

        let mut stream = client
            .subscribe(request)
            .await
            .map_err(|e| format!("Subscribe failed: {}", e))?
            .into_inner();

        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            while let Ok(Some(proto_event)) = stream.message().await {
                let event = Event {
                    sequence: proto_event.sequence,
                    event_type: EventType::from(proto_event.event_type),
                    topic: proto_event.topic,
                    payload: proto_event.payload,
                    timestamp: proto_event.timestamp,
                    source_node: proto_event.source_node,
                };

                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    /// Publish an event
    pub async fn publish(
        &self,
        topic: &str,
        event_type: EventType,
        payload: Vec<u8>,
    ) -> Result<u64, String> {
        let request = proto::PublishRequest {
            topic: topic.to_string(),
            event_type: i32::from(event_type),
            payload,
        };

        let response = self
            .with_retry("publish", |mut client| {
                let req = request.clone();
                async move { client.publish(req).await.map(|r| r.into_inner()) }
            })
            .await?;

        if response.success {
            Ok(response.sequence)
        } else {
            Err(response.error)
        }
    }

    // ========================================================================
    // Cluster Status
    // ========================================================================

    /// Get cluster status
    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, String> {
        let response = self
            .with_retry("get_cluster_status", |mut client| async move {
                client
                    .get_cluster_status(proto::GetClusterStatusRequest {})
                    .await
                    .map(|r| r.into_inner())
            })
            .await?;

        Ok(ClusterStatus {
            meta_leader: response.meta_leader,
            meta_nodes: response.meta_nodes,
            router_nodes: response.router_nodes,
            data_nodes: response.data_nodes,
            compactor_nodes: response.compactor_nodes,
            healthy: response.healthy,
        })
    }

    // ========================================================================
    // Partition Replica Management
    // ========================================================================

    /// Get replica assignment for a partition
    pub async fn get_partition_replica(
        &self,
        partition_key: &str,
    ) -> Result<Option<PartitionReplica>, String> {
        let request = proto::GetPartitionReplicaRequest {
            partition_key: partition_key.to_string(),
        };

        let response = self
            .with_retry("get_partition_replica", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .get_partition_replica(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        if !response.error.is_empty() {
            return Err(response.error);
        }

        if response.found {
            response
                .replica
                .map(|r| proto_to_partition_replica(&r))
                .ok_or_else(|| "Replica found but missing from response".to_string())
                .map(Some)
        } else {
            Ok(None)
        }
    }

    /// List all partition replicas, optionally filtered by leader
    pub async fn list_partition_replicas(
        &self,
        leader_node_id: Option<&str>,
    ) -> Result<Vec<PartitionReplica>, String> {
        let request = proto::ListPartitionReplicasRequest {
            leader_node_id: leader_node_id.unwrap_or("").to_string(),
        };

        let response = self
            .with_retry("list_partition_replicas", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .list_partition_replicas(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        Ok(response
            .replicas
            .into_iter()
            .map(|r| proto_to_partition_replica(&r))
            .collect())
    }

    /// Auto-assign replica for a new partition (first write)
    pub async fn auto_assign_replica(
        &self,
        partition_key: &str,
    ) -> Result<PartitionReplica, String> {
        let request = proto::AutoAssignReplicaRequest {
            partition_key: partition_key.to_string(),
        };

        let response = self
            .with_retry("auto_assign_replica", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .auto_assign_replica(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        if response.success {
            response
                .replica
                .map(|r| proto_to_partition_replica(&r))
                .ok_or_else(|| "Replica assigned but missing from response".to_string())
        } else {
            Err(response.error)
        }
    }

    /// Promote standby to leader (failover)
    pub async fn promote_standby(
        &self,
        partition_key: &str,
    ) -> Result<(String, String), String> {
        let request = proto::PromoteStandbyRequest {
            partition_key: partition_key.to_string(),
        };

        let response = self
            .with_retry("promote_standby", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .promote_standby(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        if response.success {
            Ok((response.new_leader_id, response.old_leader_id))
        } else {
            Err(response.error)
        }
    }

    /// Update replica state (called by leader to report replication progress)
    pub async fn update_replica_state(
        &self,
        partition_key: &str,
        state: ReplicaState,
        last_replicated_seq: u64,
        replication_lag_ms: u64,
    ) -> Result<(), String> {
        let request = proto::UpdateReplicaStateRequest {
            partition_key: partition_key.to_string(),
            state: i32::from(state),
            last_replicated_seq,
            replication_lag_ms,
        };

        let response = self
            .with_retry("update_replica_state", |mut client| {
                let req = request.clone();
                async move {
                    client
                        .update_replica_state(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(response.error)
        }
    }
}

/// Helper function to convert proto PartitionReplicaInfo to PartitionReplica
fn proto_to_partition_replica(r: &proto::PartitionReplicaInfo) -> PartitionReplica {
    PartitionReplica {
        partition_key: r.partition_key.clone(),
        leader_node_id: r.leader_node_id.clone(),
        standby_node_ids: r.standby_node_ids.clone(),
        replication_factor: r.replication_factor as u8,
        last_replicated_seq: r.last_replicated_seq,
        replication_lag_ms: r.replication_lag_ms,
        state: ReplicaState::from(r.state),
        created_at: r.created_at,
        leader_last_seen: r.leader_last_seen,
    }
}

/// Cluster status information
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub meta_leader: String,
    pub meta_nodes: u32,
    pub router_nodes: u32,
    pub data_nodes: u32,
    pub compactor_nodes: u32,
    pub healthy: bool,
}
