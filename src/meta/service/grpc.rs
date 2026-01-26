//! gRPC service implementation for Meta service

use crate::meta::proto;
use crate::meta::service::MetaNode;
use crate::meta::types::*;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

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
        let info = req.info.ok_or_else(|| Status::invalid_argument("Missing node info"))?;

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
        };

        match self.node.register_node(node_info, req.lease_ttl_ms).await {
            Ok(lease_id) => Ok(Response::new(proto::RegisterNodeResponse {
                success: true,
                error: String::new(),
                lease_id,
            })),
            Err(e) => Ok(Response::new(proto::RegisterNodeResponse {
                success: false,
                error: e,
                lease_id: 0,
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
            })
            .collect();

        Ok(Response::new(proto::ListNodesResponse { nodes: proto_nodes }))
    }

    // ========================================================================
    // File Registry
    // ========================================================================

    async fn register_files(
        &self,
        request: Request<proto::RegisterFilesRequest>,
    ) -> Result<Response<proto::RegisterFilesResponse>, Status> {
        let req = request.into_inner();

        let files: Vec<FileInfo> = req
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

        match self.node.register_files(files).await {
            Ok(sequence) => Ok(Response::new(proto::RegisterFilesResponse {
                success: true,
                error: String::new(),
                sequence,
            })),
            Err(e) => Ok(Response::new(proto::RegisterFilesResponse {
                success: false,
                error: e,
                sequence: 0,
            })),
        }
    }

    async fn remove_files(
        &self,
        request: Request<proto::RemoveFilesRequest>,
    ) -> Result<Response<proto::RemoveFilesResponse>, Status> {
        let req = request.into_inner();

        match self.node.remove_files(req.paths).await {
            Ok(sequence) => Ok(Response::new(proto::RemoveFilesResponse {
                success: true,
                error: String::new(),
                sequence,
            })),
            Err(e) => Ok(Response::new(proto::RemoveFilesResponse {
                success: false,
                error: e,
                sequence: 0,
            })),
        }
    }

    async fn list_files_for_partition(
        &self,
        request: Request<proto::ListFilesRequest>,
    ) -> Result<Response<proto::ListFilesResponse>, Status> {
        let req = request.into_inner();

        let files = self
            .node
            .list_files_for_partition(&req.catalog, &req.schema, &req.table, &req.partition)
            .await;

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

        Ok(Response::new(proto::ListFilesResponse { files: proto_files }))
    }

    async fn get_files_for_query(
        &self,
        request: Request<proto::QueryFilesRequest>,
    ) -> Result<Response<proto::QueryFilesResponse>, Status> {
        let req = request.into_inner();

        // Get files for all requested tables
        let mut all_files = Vec::new();
        let mut nodes_set = std::collections::HashSet::new();

        for table_ref in &req.tables {
            // Parse "schema.table" format
            let parts: Vec<&str> = table_ref.split('.').collect();
            if parts.len() != 2 {
                continue;
            }

            let files = self
                .node
                .list_files_for_table(&req.catalog, parts[0], parts[1])
                .await;

            for file in files {
                // Filter by time range if specified
                if req.min_time > 0 && file.max_time < req.min_time {
                    continue;
                }
                if req.max_time > 0 && file.min_time > req.max_time {
                    continue;
                }

                nodes_set.insert(file.created_by.clone());
                all_files.push(proto::FileInfo {
                    path: file.path,
                    catalog: file.catalog,
                    schema: file.schema,
                    table: file.table,
                    partition: file.partition,
                    level: file.level as u32,
                    size_bytes: file.size_bytes,
                    row_count: file.row_count,
                    min_time: file.min_time,
                    max_time: file.max_time,
                    created_at: file.created_at,
                    created_by: file.created_by,
                });
            }
        }

        Ok(Response::new(proto::QueryFilesResponse {
            files: all_files,
            nodes: nodes_set.into_iter().collect(),
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
                    let (leader, term) = leader_info
                        .map(|l| (l.leader, l.term))
                        .unwrap_or_default();
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
        let mut last_seq = req.start_sequence;

        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

            loop {
                interval.tick().await;

                // Get events from last sequence
                let events = node.get_events_from(last_seq, 100).await;

                for event in events {
                    // Filter by topic if specified
                    if !topics.is_empty() && !topics.contains(&event.topic) {
                        last_seq = event.sequence + 1;
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
                        return; // Client disconnected
                    }

                    last_seq = event.sequence + 1;
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
}

/// gRPC service for Raft internal communication (stub for single-node mode)
pub struct RaftInternalService {
    #[allow(dead_code)]
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
        _request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        // Stub - full Raft integration to be implemented
        Err(Status::unimplemented(
            "Raft internal communication not yet implemented",
        ))
    }

    async fn install_snapshot(
        &self,
        _request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        // Stub - full Raft integration to be implemented
        Err(Status::unimplemented(
            "Raft internal communication not yet implemented",
        ))
    }

    async fn vote(
        &self,
        _request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        // Stub - full Raft integration to be implemented
        Err(Status::unimplemented(
            "Raft internal communication not yet implemented",
        ))
    }
}

/// Start the Meta gRPC server
pub async fn start_grpc_server(
    addr: std::net::SocketAddr,
    service: MetaGrpcService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use proto::meta_service_server::MetaServiceServer;

    tracing::info!("Starting Meta gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(MetaServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
