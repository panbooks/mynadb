//! gRPC service implementation for Meta service

use crate::meta::proto;
use crate::meta::raft::TypeConfig;
use crate::meta::service::MetaNode;
use crate::meta::types::*;

/// Convert proto DataNodeMode to internal DataNodeMode
fn convert_proto_mode(proto_mode: i32) -> Option<DataNodeMode> {
    match proto_mode {
        1 => Some(DataNodeMode::Primary),
        2 => Some(DataNodeMode::Standby),
        _ => None, // Unspecified or unknown
    }
}

/// Convert internal DataNodeMode to proto DataNodeMode
fn mode_to_proto(mode: Option<DataNodeMode>) -> i32 {
    match mode {
        Some(DataNodeMode::Primary) => 1, // DATA_NODE_MODE_PRIMARY
        Some(DataNodeMode::Standby) => 2, // DATA_NODE_MODE_STANDBY
        None => 0,                        // DATA_NODE_MODE_UNSPECIFIED
    }
}
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

/// gRPC service implementation
pub struct MetaGrpcService {
    node: Arc<MetaNode>,
}

impl MetaGrpcService {
    pub fn new(node: Arc<MetaNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl proto::meta_service_server::MetaService for MetaGrpcService {
    // ========================================================================
    // Node Management
    // ========================================================================

    async fn register_node(
        &self,
        request: Request<proto::RegisterNodeRequest>,
    ) -> Result<Response<proto::RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let info = req
            .info
            .ok_or_else(|| Status::invalid_argument("Missing node info"))?;

        let node_info = NodeInfo {
            node_id: info.node_id.clone(),
            role: NodeRole::from(info.role),
            state: NodeState::from(info.state),
            advertise_addr: info.advertise_addr,
            http_addr: if info.http_addr.is_empty() {
                None
            } else {
                Some(info.http_addr)
            },
            flight_addr: if info.flight_addr.is_empty() {
                None
            } else {
                Some(info.flight_addr)
            },
            registered_at: info.registered_at,
            last_heartbeat: info.last_heartbeat,
            labels: info.labels,
            mode: convert_proto_mode(info.mode),
            // Node-level standby pairing - assigned by state machine during registration
            backing_standby: None,
            backed_primaries: vec![],
        };

        let node_id = info.node_id.clone();
        match self.node.register_node(node_info, req.lease_ttl_ms).await {
            Ok(lease_id) => {
                // Get the updated node info to retrieve backing_standby assignment
                let (assigned_mode, backing_standby_id, backing_standby_addr) =
                    if let Some(node) = self.node.get_node(&node_id).await {
                        let mode = node.mode.map(|m| m as i32).unwrap_or(0);
                        let standby_id = node.backing_standby.clone().unwrap_or_default();

                        // Get the standby's flight address if assigned
                        let standby_addr = if !standby_id.is_empty() {
                            if let Some(standby) = self.node.get_node(&standby_id).await {
                                standby.flight_addr.unwrap_or_default()
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };
                        (mode, standby_id, standby_addr)
                    } else {
                        (0, String::new(), String::new())
                    };

                Ok(Response::new(proto::RegisterNodeResponse {
                    success: true,
                    error: String::new(),
                    lease_id,
                    assigned_mode,
                    backing_standby_id,
                    backing_standby_addr,
                }))
            }
            Err(e) => Ok(Response::new(proto::RegisterNodeResponse {
                success: false,
                error: e,
                lease_id: 0,
                assigned_mode: 0,
                backing_standby_id: String::new(),
                backing_standby_addr: String::new(),
            })),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .heartbeat(&req.node_id, req.lease_id, req.timestamp)
            .await
        {
            Ok(()) => Ok(Response::new(proto::HeartbeatResponse {
                success: true,
                error: String::new(),
                next_heartbeat_ms: 5000, // 5 seconds
            })),
            Err(e) => Ok(Response::new(proto::HeartbeatResponse {
                success: false,
                error: e,
                next_heartbeat_ms: 1000,
            })),
        }
    }

    async fn deregister_node(
        &self,
        request: Request<proto::DeregisterNodeRequest>,
    ) -> Result<Response<proto::DeregisterNodeResponse>, Status> {
        let req = request.into_inner();

        match self.node.deregister_node(&req.node_id).await {
            Ok(()) => Ok(Response::new(proto::DeregisterNodeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::DeregisterNodeResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn list_nodes(
        &self,
        request: Request<proto::ListNodesRequest>,
    ) -> Result<Response<proto::ListNodesResponse>, Status> {
        let req = request.into_inner();

        let role = if req.role_filter == 0 {
            None
        } else {
            Some(NodeRole::from(req.role_filter))
        };

        let state = if req.state_filter == 0 {
            None
        } else {
            Some(NodeState::from(req.state_filter))
        };

        let nodes = self.node.list_nodes(role, state).await;

        let proto_nodes: Vec<proto::NodeInfo> = nodes
            .into_iter()
            .map(|n| proto::NodeInfo {
                node_id: n.node_id,
                role: i32::from(n.role),
                state: i32::from(n.state),
                advertise_addr: n.advertise_addr,
                http_addr: n.http_addr.unwrap_or_default(),
                flight_addr: n.flight_addr.unwrap_or_default(),
                registered_at: n.registered_at,
                last_heartbeat: n.last_heartbeat,
                labels: n.labels,
                mode: mode_to_proto(n.mode),
            })
            .collect();

        Ok(Response::new(proto::ListNodesResponse {
            nodes: proto_nodes,
        }))
    }

    // ========================================================================
    // Table-Level Routing (Recommended)
    // ========================================================================

    async fn register_table(
        &self,
        request: Request<proto::RegisterTableRequest>,
    ) -> Result<Response<proto::RegisterTableResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .register_table(&req.catalog, &req.schema, &req.table, &req.owner_node_id)
            .await
        {
            Ok(response) => match response {
                MetaResponse::TableRegistered {
                    owner_node_id,
                    version,
                    ..
                } => Ok(Response::new(proto::RegisterTableResponse {
                    success: true,
                    error: String::new(),
                    already_exists: false,
                    current_owner: owner_node_id.clone(),
                    all_owners: vec![owner_node_id],
                    version,
                })),
                MetaResponse::TableAlreadyExists {
                    current_owner,
                    all_owners,
                    ..
                } => Ok(Response::new(proto::RegisterTableResponse {
                    success: true,
                    error: String::new(),
                    already_exists: true,
                    current_owner,
                    all_owners,
                    version: 0,
                })),
                _ => Ok(Response::new(proto::RegisterTableResponse {
                    success: false,
                    error: "Unexpected response".to_string(),
                    already_exists: false,
                    current_owner: String::new(),
                    all_owners: Vec::new(),
                    version: 0,
                })),
            },
            Err(e) => Ok(Response::new(proto::RegisterTableResponse {
                success: false,
                error: e,
                already_exists: false,
                current_owner: String::new(),
                all_owners: Vec::new(),
                version: 0,
            })),
        }
    }

    async fn update_table_stats(
        &self,
        request: Request<proto::UpdateTableStatsRequest>,
    ) -> Result<Response<proto::UpdateTableStatsResponse>, Status> {
        let req = request.into_inner();
        let delta = req.delta.unwrap_or_default();

        let stats_delta = TableStatsDelta {
            partition_count_delta: delta.partition_count_delta,
            bytes_delta: delta.bytes_delta,
            rows_delta: delta.rows_delta,
            files_delta: delta.files_delta,
            newest_partition: if delta.newest_partition.is_empty() {
                None
            } else {
                Some(delta.newest_partition)
            },
            min_time: delta.min_time,
            max_time: delta.max_time,
        };

        match self
            .node
            .update_table_stats(&req.catalog, &req.schema, &req.table, stats_delta)
            .await
        {
            Ok(response) => match response {
                MetaResponse::TableStatsUpdated { version, .. } => {
                    Ok(Response::new(proto::UpdateTableStatsResponse {
                        success: true,
                        error: String::new(),
                        version,
                    }))
                }
                MetaResponse::TableNotFound { .. } => {
                    Ok(Response::new(proto::UpdateTableStatsResponse {
                        success: false,
                        error: "Table not found".to_string(),
                        version: 0,
                    }))
                }
                _ => Ok(Response::new(proto::UpdateTableStatsResponse {
                    success: false,
                    error: "Unexpected response".to_string(),
                    version: 0,
                })),
            },
            Err(e) => Ok(Response::new(proto::UpdateTableStatsResponse {
                success: false,
                error: e,
                version: 0,
            })),
        }
    }

    async fn reassign_table(
        &self,
        request: Request<proto::ReassignTableRequest>,
    ) -> Result<Response<proto::ReassignTableResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .reassign_table(&req.catalog, &req.schema, &req.table, &req.new_owner_node_id)
            .await
        {
            Ok(response) => match response {
                MetaResponse::TableReassigned {
                    old_owner,
                    new_owner,
                    all_owners,
                    version,
                    ..
                } => Ok(Response::new(proto::ReassignTableResponse {
                    success: true,
                    error: String::new(),
                    old_primary: old_owner,
                    new_primary: new_owner,
                    all_owners,
                    version,
                })),
                MetaResponse::TableNotFound { .. } => {
                    Ok(Response::new(proto::ReassignTableResponse {
                        success: false,
                        error: "Table not found".to_string(),
                        old_primary: String::new(),
                        new_primary: String::new(),
                        all_owners: Vec::new(),
                        version: 0,
                    }))
                }
                _ => Ok(Response::new(proto::ReassignTableResponse {
                    success: false,
                    error: "Unexpected response".to_string(),
                    old_primary: String::new(),
                    new_primary: String::new(),
                    all_owners: Vec::new(),
                    version: 0,
                })),
            },
            Err(e) => Ok(Response::new(proto::ReassignTableResponse {
                success: false,
                error: e,
                old_primary: String::new(),
                new_primary: String::new(),
                all_owners: Vec::new(),
                version: 0,
            })),
        }
    }

    async fn remove_table(
        &self,
        request: Request<proto::RemoveTableRequest>,
    ) -> Result<Response<proto::RemoveTableResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .remove_table(&req.catalog, &req.schema, &req.table)
            .await
        {
            Ok(_) => Ok(Response::new(proto::RemoveTableResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::RemoveTableResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn get_table_owner(
        &self,
        request: Request<proto::GetTableOwnerRequest>,
    ) -> Result<Response<proto::GetTableOwnerResponse>, Status> {
        let req = request.into_inner();

        let table_meta = self
            .node
            .get_table_meta(&req.catalog, &req.schema, &req.table)
            .await;

        match table_meta {
            Some(meta) => {
                let primary_owner = meta.primary_owner().unwrap_or("").to_string();
                let all_owners = meta.owner_node_ids.clone();
                Ok(Response::new(proto::GetTableOwnerResponse {
                    found: true,
                    primary_owner_id: primary_owner,
                    all_owner_ids: all_owners.clone(),
                    table_meta: Some(proto::TableMeta {
                        catalog: meta.catalog,
                        schema: meta.schema,
                        table: meta.table,
                        owner_node_ids: all_owners,
                        partition_count: meta.partition_count,
                        total_bytes: meta.total_bytes,
                        total_rows: meta.total_rows,
                        oldest_partition: meta.oldest_partition.unwrap_or_default(),
                        newest_partition: meta.newest_partition.unwrap_or_default(),
                        min_time: meta.min_time,
                        max_time: meta.max_time,
                        created_at: meta.created_at,
                        last_modified: meta.last_modified,
                        version: meta.version,
                    }),
                }))
            }
            None => Ok(Response::new(proto::GetTableOwnerResponse {
                found: false,
                primary_owner_id: String::new(),
                all_owner_ids: Vec::new(),
                table_meta: None,
            })),
        }
    }

    async fn add_table_owner(
        &self,
        request: Request<proto::AddTableOwnerRequest>,
    ) -> Result<Response<proto::AddTableOwnerResponse>, Status> {
        let req = request.into_inner();

        let result = self
            .node
            .add_table_owner(&req.catalog, &req.schema, &req.table, &req.owner_node_id)
            .await;

        match result {
            Ok((already_owner, all_owners, version)) => {
                Ok(Response::new(proto::AddTableOwnerResponse {
                    success: true,
                    error: String::new(),
                    already_owner,
                    all_owners,
                    version,
                }))
            }
            Err(e) => Ok(Response::new(proto::AddTableOwnerResponse {
                success: false,
                error: e,
                already_owner: false,
                all_owners: Vec::new(),
                version: 0,
            })),
        }
    }

    async fn remove_table_owner(
        &self,
        request: Request<proto::RemoveTableOwnerRequest>,
    ) -> Result<Response<proto::RemoveTableOwnerResponse>, Status> {
        let req = request.into_inner();

        let result = self
            .node
            .remove_table_owner(&req.catalog, &req.schema, &req.table, &req.owner_node_id)
            .await;

        match result {
            Ok((all_owners, version)) => Ok(Response::new(proto::RemoveTableOwnerResponse {
                success: true,
                error: String::new(),
                all_owners,
                version,
            })),
            Err(e) => Ok(Response::new(proto::RemoveTableOwnerResponse {
                success: false,
                error: e,
                all_owners: Vec::new(),
                version: 0,
            })),
        }
    }

    async fn list_tables_for_node(
        &self,
        request: Request<proto::ListTablesForNodeRequest>,
    ) -> Result<Response<proto::ListTablesForNodeResponse>, Status> {
        let req = request.into_inner();

        let tables = self.node.list_tables_for_node(&req.node_id).await;

        let proto_tables: Vec<proto::TableMeta> = tables
            .into_iter()
            .map(|m| proto::TableMeta {
                catalog: m.catalog,
                schema: m.schema,
                table: m.table,
                owner_node_ids: m.owner_node_ids,
                partition_count: m.partition_count,
                total_bytes: m.total_bytes,
                total_rows: m.total_rows,
                oldest_partition: m.oldest_partition.unwrap_or_default(),
                newest_partition: m.newest_partition.unwrap_or_default(),
                min_time: m.min_time,
                max_time: m.max_time,
                created_at: m.created_at,
                last_modified: m.last_modified,
                version: m.version,
            })
            .collect();

        Ok(Response::new(proto::ListTablesForNodeResponse {
            tables: proto_tables,
        }))
    }

    async fn list_tables(
        &self,
        request: Request<proto::ListTablesRequest>,
    ) -> Result<Response<proto::ListTablesResponse>, Status> {
        let req = request.into_inner();

        let tables = if !req.catalog.is_empty() && !req.schema.is_empty() {
            self.node
                .list_tables_in_schema(&req.catalog, &req.schema)
                .await
        } else if !req.catalog.is_empty() {
            self.node.list_tables_in_catalog(&req.catalog).await
        } else {
            self.node.list_all_tables().await
        };

        let proto_tables: Vec<proto::TableMeta> = tables
            .into_iter()
            .map(|m| proto::TableMeta {
                catalog: m.catalog,
                schema: m.schema,
                table: m.table,
                owner_node_ids: m.owner_node_ids,
                partition_count: m.partition_count,
                total_bytes: m.total_bytes,
                total_rows: m.total_rows,
                oldest_partition: m.oldest_partition.unwrap_or_default(),
                newest_partition: m.newest_partition.unwrap_or_default(),
                min_time: m.min_time,
                max_time: m.max_time,
                created_at: m.created_at,
                last_modified: m.last_modified,
                version: m.version,
            })
            .collect();

        Ok(Response::new(proto::ListTablesResponse {
            tables: proto_tables,
        }))
    }

    // ========================================================================
    // Distributed Locks
    // ========================================================================

    async fn try_lock(
        &self,
        request: Request<proto::TryLockRequest>,
    ) -> Result<Response<proto::TryLockResponse>, Status> {
        let req = request.into_inner();

        match self.node.try_lock(&req.key, &req.holder, req.ttl_ms).await {
            Ok((acquired, fencing_token)) => {
                if acquired {
                    Ok(Response::new(proto::TryLockResponse {
                        acquired: true,
                        fencing_token,
                        current_holder: String::new(),
                        expires_at: current_timestamp_ms() + req.ttl_ms as i64,
                        error: String::new(),
                    }))
                } else {
                    // Get current lock info
                    let lock_info = self.node.get_lock(&req.key).await;
                    let (holder, exp) = lock_info
                        .map(|l| (l.holder, l.expires_at))
                        .unwrap_or_default();
                    Ok(Response::new(proto::TryLockResponse {
                        acquired: false,
                        fencing_token: 0,
                        current_holder: holder,
                        expires_at: exp,
                        error: String::new(),
                    }))
                }
            }
            Err(e) => Ok(Response::new(proto::TryLockResponse {
                acquired: false,
                fencing_token: 0,
                current_holder: String::new(),
                expires_at: 0,
                error: e,
            })),
        }
    }

    async fn unlock(
        &self,
        request: Request<proto::UnlockRequest>,
    ) -> Result<Response<proto::UnlockResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .unlock(&req.key, &req.holder, req.fencing_token)
            .await
        {
            Ok(()) => Ok(Response::new(proto::UnlockResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::UnlockResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn renew_lock(
        &self,
        request: Request<proto::RenewLockRequest>,
    ) -> Result<Response<proto::RenewLockResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .renew_lock(&req.key, &req.holder, req.fencing_token, req.ttl_ms)
            .await
        {
            Ok(expires_at) => Ok(Response::new(proto::RenewLockResponse {
                success: true,
                expires_at,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::RenewLockResponse {
                success: false,
                expires_at: 0,
                error: e,
            })),
        }
    }

    async fn get_lock(
        &self,
        request: Request<proto::GetLockRequest>,
    ) -> Result<Response<proto::GetLockResponse>, Status> {
        let req = request.into_inner();

        let lock_info = self.node.get_lock(&req.key).await;

        Ok(Response::new(match lock_info {
            Some(lock) => proto::GetLockResponse {
                held: true,
                holder: lock.holder,
                fencing_token: lock.fencing_token,
                expires_at: lock.expires_at,
            },
            None => proto::GetLockResponse {
                held: false,
                holder: String::new(),
                fencing_token: 0,
                expires_at: 0,
            },
        }))
    }

    // ========================================================================
    // Leader Election
    // ========================================================================

    async fn campaign(
        &self,
        request: Request<proto::CampaignRequest>,
    ) -> Result<Response<proto::CampaignResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .campaign(&req.election, &req.node_id, req.lease_ttl_ms)
            .await
        {
            Ok((elected, term)) => {
                if elected {
                    Ok(Response::new(proto::CampaignResponse {
                        elected: true,
                        current_leader: req.node_id,
                        term,
                        error: String::new(),
                    }))
                } else {
                    let leader_info = self.node.get_leader(&req.election).await;
                    let (leader, term) =
                        leader_info.map(|l| (l.leader, l.term)).unwrap_or_default();
                    Ok(Response::new(proto::CampaignResponse {
                        elected: false,
                        current_leader: leader,
                        term,
                        error: String::new(),
                    }))
                }
            }
            Err(e) => Ok(Response::new(proto::CampaignResponse {
                elected: false,
                current_leader: String::new(),
                term: 0,
                error: e,
            })),
        }
    }

    async fn resign(
        &self,
        request: Request<proto::ResignRequest>,
    ) -> Result<Response<proto::ResignResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .resign(&req.election, &req.node_id, req.term)
            .await
        {
            Ok(()) => Ok(Response::new(proto::ResignResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::ResignResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn get_leader(
        &self,
        request: Request<proto::GetLeaderRequest>,
    ) -> Result<Response<proto::GetLeaderResponse>, Status> {
        let req = request.into_inner();

        let leader_info = self.node.get_leader(&req.election).await;

        Ok(Response::new(match leader_info {
            Some(leader) => proto::GetLeaderResponse {
                has_leader: true,
                leader: leader.leader,
                term: leader.term,
            },
            None => proto::GetLeaderResponse {
                has_leader: false,
                leader: String::new(),
                term: 0,
            },
        }))
    }

    // ========================================================================
    // Event System
    // ========================================================================

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<proto::Event, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let node = Arc::clone(&self.node);
        let topics: std::collections::HashSet<String> = req.topics.into_iter().collect();
        let start_sequence = req.start_sequence;

        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            // First, catch up from start_sequence to current
            // This ensures no events are missed if client reconnects
            let mut last_seq = start_sequence;
            let catchup_events = node.get_events_from(start_sequence, 10000).await;

            for event in catchup_events {
                // Filter by topic if specified
                if !topics.is_empty() && !topics.contains(&event.topic) {
                    last_seq = event.sequence + 1;
                    continue;
                }

                let proto_event = proto::Event {
                    sequence: event.sequence,
                    event_type: i32::from(event.event_type),
                    topic: event.topic.clone(),
                    payload: event.payload.clone(),
                    timestamp: event.timestamp,
                    source_node: event.source_node.clone(),
                };

                if tx.send(Ok(proto_event)).await.is_err() {
                    return; // Client disconnected
                }

                last_seq = event.sequence + 1;
            }

            // Now subscribe to real-time events via broadcast channel
            let mut event_rx = node.subscribe_events();

            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        // Skip events we've already sent during catchup
                        if event.sequence < last_seq {
                            continue;
                        }

                        // Filter by topic if specified
                        if !topics.is_empty() && !topics.contains(&event.topic) {
                            continue;
                        }

                        let proto_event = proto::Event {
                            sequence: event.sequence,
                            event_type: i32::from(event.event_type),
                            topic: event.topic,
                            payload: event.payload,
                            timestamp: event.timestamp,
                            source_node: event.source_node,
                        };

                        if tx.send(Ok(proto_event)).await.is_err() {
                            debug!("Client disconnected from event subscription");
                            return; // Client disconnected
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        // We fell behind - log warning but continue
                        // Client should use sequence numbers to detect gaps
                        warn!("Event subscriber lagged by {} events", count);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Channel closed - server shutting down
                        info!("Event broadcast channel closed");
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn publish(
        &self,
        request: Request<proto::PublishRequest>,
    ) -> Result<Response<proto::PublishResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .publish_event(
                &req.topic,
                EventType::from(req.event_type),
                req.payload,
                "external",
            )
            .await
        {
            Ok(sequence) => Ok(Response::new(proto::PublishResponse {
                success: true,
                sequence,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::PublishResponse {
                success: false,
                sequence: 0,
                error: e,
            })),
        }
    }

    // ========================================================================
    // Cluster Status
    // ========================================================================

    async fn get_cluster_status(
        &self,
        _request: Request<proto::GetClusterStatusRequest>,
    ) -> Result<Response<proto::GetClusterStatusResponse>, Status> {
        let nodes = self.node.list_nodes(None, None).await;

        let mut meta_count = 0u32;
        let mut router_count = 0u32;
        let mut data_count = 0u32;
        let mut compactor_count = 0u32;

        let proto_nodes: Vec<proto::NodeInfo> = nodes
            .into_iter()
            .map(|n| {
                match n.role {
                    NodeRole::Meta => meta_count += 1,
                    NodeRole::Router => router_count += 1,
                    NodeRole::Data => data_count += 1,
                    NodeRole::Compactor => compactor_count += 1,
                    NodeRole::Unknown => {}
                }

                proto::NodeInfo {
                    node_id: n.node_id,
                    role: i32::from(n.role),
                    state: i32::from(n.state),
                    advertise_addr: n.advertise_addr,
                    http_addr: n.http_addr.unwrap_or_default(),
                    flight_addr: n.flight_addr.unwrap_or_default(),
                    registered_at: n.registered_at,
                    last_heartbeat: n.last_heartbeat,
                    labels: n.labels,
                    mode: mode_to_proto(n.mode),
                }
            })
            .collect();

        let meta_leader = self
            .node
            .current_leader()
            .await
            .map(|id| id.to_string())
            .unwrap_or_default();

        Ok(Response::new(proto::GetClusterStatusResponse {
            meta_leader,
            meta_nodes: meta_count,
            router_nodes: router_count,
            data_nodes: data_count,
            compactor_nodes: compactor_count,
            healthy: true, // TODO: Implement proper health check
            nodes: proto_nodes,
        }))
    }

    async fn get_raft_status(
        &self,
        _request: Request<proto::GetRaftStatusRequest>,
    ) -> Result<Response<proto::GetRaftStatusResponse>, Status> {
        let metrics = self.node.metrics();

        let role = if metrics.is_leader {
            "leader"
        } else {
            "follower"
        };

        Ok(Response::new(proto::GetRaftStatusResponse {
            node_id: metrics.node_id,
            role: role.to_string(),
            term: metrics.current_term,
            last_log_index: 0,
            commit_index: 0,
            last_applied: 0,
            leader_id: if metrics.is_leader {
                metrics.node_id.to_string()
            } else {
                String::new()
            },
            peers: vec![],
        }))
    }

    // ========================================================================
    // Data Node Mode Management (Cold Standby Architecture)
    // ========================================================================

    // promote_data_node() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    async fn get_data_node_mode(
        &self,
        request: Request<proto::GetDataNodeModeRequest>,
    ) -> Result<Response<proto::GetDataNodeModeResponse>, Status> {
        let req = request.into_inner();

        match self.node.get_data_node_mode(&req.node_id).await {
            Ok(mode) => Ok(Response::new(proto::GetDataNodeModeResponse {
                found: true,
                node_id: req.node_id,
                mode: mode_to_proto(Some(mode)),
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::GetDataNodeModeResponse {
                found: false,
                node_id: req.node_id,
                mode: 0,
                error: e,
            })),
        }
    }

    // ========================================================================
    // Scale-In (Retirement) Operations
    // ========================================================================

    async fn start_retire(
        &self,
        request: Request<proto::StartRetireRequest>,
    ) -> Result<Response<proto::StartRetireResponse>, Status> {
        let req = request.into_inner();

        match self.node.start_retire(&req.node_id).await {
            Ok(partitions_to_reassign) => Ok(Response::new(proto::StartRetireResponse {
                success: true,
                error: String::new(),
                partitions_to_reassign: partitions_to_reassign as u32,
            })),
            Err(e) => Ok(Response::new(proto::StartRetireResponse {
                success: false,
                error: e,
                partitions_to_reassign: 0,
            })),
        }
    }

    async fn complete_retire(
        &self,
        request: Request<proto::CompleteRetireRequest>,
    ) -> Result<Response<proto::CompleteRetireResponse>, Status> {
        let req = request.into_inner();

        match self.node.complete_retire(&req.node_id).await {
            Ok(()) => Ok(Response::new(proto::CompleteRetireResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::CompleteRetireResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn cancel_retire(
        &self,
        request: Request<proto::CancelRetireRequest>,
    ) -> Result<Response<proto::CancelRetireResponse>, Status> {
        let req = request.into_inner();

        match self.node.cancel_retire(&req.node_id).await {
            Ok(()) => Ok(Response::new(proto::CancelRetireResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::CancelRetireResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn get_retire_status(
        &self,
        request: Request<proto::GetRetireStatusRequest>,
    ) -> Result<Response<proto::GetRetireStatusResponse>, Status> {
        let req = request.into_inner();

        match self.node.get_retire_status(&req.node_id).await {
            Ok(status) => {
                let progress = status.progress_percent();
                Ok(Response::new(proto::GetRetireStatusResponse {
                    found: true,
                    node_id: status.node_id,
                    state: i32::from(status.state),
                    started_at: status.started_at,
                    partitions_total: status.partitions_total as u32,
                    partitions_reassigned: status.partitions_reassigned as u32,
                    memtable_rows_remaining: status.memtable_rows_remaining,
                    progress_percent: progress as u32,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(proto::GetRetireStatusResponse {
                found: false,
                node_id: req.node_id,
                state: 0,
                started_at: 0,
                partitions_total: 0,
                partitions_reassigned: 0,
                memtable_rows_remaining: 0,
                progress_percent: 0,
                error: e,
            })),
        }
    }

    async fn reassign_partition_for_retire(
        &self,
        request: Request<proto::ReassignPartitionForRetireRequest>,
    ) -> Result<Response<proto::ReassignPartitionForRetireResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .reassign_partition_for_retire(&req.partition_key, &req.from_node_id, &req.to_node_id)
            .await
        {
            Ok(()) => Ok(Response::new(proto::ReassignPartitionForRetireResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::ReassignPartitionForRetireResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn remove_standby_from_replicas(
        &self,
        request: Request<proto::RemoveStandbyFromReplicasRequest>,
    ) -> Result<Response<proto::RemoveStandbyFromReplicasResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .remove_standby_from_replicas(&req.standby_node_id)
            .await
        {
            Ok(partitions_affected) => {
                Ok(Response::new(proto::RemoveStandbyFromReplicasResponse {
                    success: true,
                    error: String::new(),
                    partitions_affected: partitions_affected as u32,
                }))
            }
            Err(e) => Ok(Response::new(proto::RemoveStandbyFromReplicasResponse {
                success: false,
                error: e,
                partitions_affected: 0,
            })),
        }
    }

    async fn recommend_scale_in(
        &self,
        request: Request<proto::RecommendScaleInRequest>,
    ) -> Result<Response<proto::RecommendScaleInResponse>, Status> {
        let req = request.into_inner();

        match self
            .node
            .recommend_scale_in(req.reduce_primaries as usize, req.reduce_standbys as usize)
            .await
        {
            Ok(recommendation) => Ok(Response::new(proto::RecommendScaleInResponse {
                success: true,
                error: String::new(),
                recommended_primaries: recommendation.recommended_primaries,
                recommended_standbys: recommendation.recommended_standbys,
                reasons: recommendation.reasons,
                warnings: recommendation.warnings,
            })),
            Err(e) => Ok(Response::new(proto::RecommendScaleInResponse {
                success: false,
                error: e,
                recommended_primaries: vec![],
                recommended_standbys: vec![],
                reasons: std::collections::HashMap::new(),
                warnings: vec![],
            })),
        }
    }
}

/// Helper function to convert PartitionReplica to proto
fn partition_replica_to_proto(r: &PartitionReplica) -> proto::PartitionReplicaInfo {
    proto::PartitionReplicaInfo {
        partition_key: r.partition_key.clone(),
        leader_node_id: r.leader_node_id.clone(),
        standby_node_ids: r.standby_node_ids.clone(),
        replication_factor: r.replication_factor as u32,
        state: i32::from(r.state),
        last_replicated_seq: r.last_replicated_seq,
        replication_lag_ms: r.replication_lag_ms,
        created_at: r.created_at,
        leader_last_seen: r.leader_last_seen,
    }
}

/// gRPC service for Raft internal communication
pub struct RaftInternalService {
    node: Arc<MetaNode>,
}

impl RaftInternalService {
    pub fn new(node: Arc<MetaNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl proto::raft_internal_server::RaftInternal for RaftInternalService {
    async fn append_entries(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let req: AppendEntriesRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("Failed to deserialize: {}", e)))?;

        debug!("Received AppendEntries from leader");

        let resp = self
            .node
            .raft()
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let req: InstallSnapshotRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("Failed to deserialize: {}", e)))?;

        info!("Received InstallSnapshot from leader");

        let resp = self
            .node
            .raft()
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("InstallSnapshot failed: {}", e)))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data }))
    }

    async fn vote(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let req: VoteRequest<u64> = bincode::deserialize(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("Failed to deserialize: {}", e)))?;

        debug!(
            "Received Vote request from candidate {}",
            req.vote.leader_id().node_id
        );

        let resp = self
            .node
            .raft()
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data }))
    }

    async fn forward_command(
        &self,
        request: Request<proto::ForwardCommandRequest>,
    ) -> Result<Response<proto::ForwardCommandResponse>, Status> {
        let req = request.into_inner();

        debug!("Received forwarded command from follower");

        match self.node.execute_forwarded_command(req.command_data).await {
            Ok((result, meta_response)) => {
                let response_data = meta_response
                    .map(|r| bincode::serialize(&r).unwrap_or_default())
                    .unwrap_or_default();

                Ok(Response::new(proto::ForwardCommandResponse {
                    success: true,
                    error: String::new(),
                    result,
                    response_data,
                }))
            }
            Err(e) => Ok(Response::new(proto::ForwardCommandResponse {
                success: false,
                error: e,
                result: 0,
                response_data: vec![],
            })),
        }
    }
}

/// Start the Meta gRPC server with both MetaService and RaftInternal services
pub async fn start_grpc_server(
    addr: std::net::SocketAddr,
    meta_service: MetaGrpcService,
    raft_service: RaftInternalService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use proto::meta_service_server::MetaServiceServer;
    use proto::raft_internal_server::RaftInternalServer;

    tracing::info!("Starting Meta gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(MetaServiceServer::new(meta_service))
        .add_service(RaftInternalServer::new(raft_service))
        .serve(addr)
        .await?;

    Ok(())
}
