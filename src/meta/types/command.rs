//! MetaCommand and MetaResponse types

use serde::{Deserialize, Serialize};

use super::{
    EventType, NodeInfo, NodeState, ScaleInRecommendation, TableStatsDelta,
};

/// Commands that can be applied to the Raft state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    // Node operations
    RegisterNode {
        info: NodeInfo,
        lease_id: u64,
        lease_ttl_ms: u64,
    },
    Heartbeat {
        node_id: String,
        lease_id: u64,
        timestamp: i64,
    },
    DeregisterNode {
        node_id: String,
    },
    UpdateNodeState {
        node_id: String,
        state: NodeState,
    },

    // ========================================================================
    // Table-Level Operations (Recommended for routing)
    // ========================================================================
    /// Register a new table with its owner node
    ///
    /// Called when a Data node first receives data for a new table.
    /// The node that first writes to a table becomes its owner.
    RegisterTable {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Add an additional owner to a table (for rerouted writes)
    ///
    /// Called when a write is rerouted to a fallback node after primary failure.
    /// The new node becomes a secondary owner with the specified TTL.
    AddTableOwner {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        /// TTL in milliseconds for this ownership (0 = use default)
        ttl_ms: u64,
    },

    /// Refresh the TTL for an existing table owner
    ///
    /// Called periodically by data nodes to keep their ownership alive.
    /// If the owner is not found, this is a no-op.
    RefreshOwnerTTL {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        /// New TTL in milliseconds (0 = use current TTL)
        ttl_ms: u64,
    },

    /// Cleanup expired owner entries across all tables
    ///
    /// Called periodically by the Meta leader to remove stale owners.
    /// Returns the number of expired entries removed.
    CleanupExpiredOwners,

    /// Remove an owner from a table
    ///
    /// Called when a node leaves the cluster or during ownership consolidation.
    RemoveTableOwner {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Update table statistics after flush
    ///
    /// Called periodically (not per-flush) to sync aggregated stats.
    /// Data nodes batch stats locally and sync every few minutes.
    UpdateTableStats {
        catalog: String,
        schema: String,
        table: String,
        delta: TableStatsDelta,
    },

    /// Reassign a table to a different owner node
    ///
    /// Used for:
    /// - Load balancing (move hot tables)
    /// - Node failure recovery
    /// - Manual rebalancing
    ReassignTable {
        catalog: String,
        schema: String,
        table: String,
        new_owner_node_id: String,
    },

    /// Remove a table from the registry
    ///
    /// Called when a table is dropped or all data is deleted.
    RemoveTable {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Write Distribution Configuration
    // ========================================================================
    /// Set distribution config for catalog/schema/table
    ///
    /// Configures how writes are distributed across primary nodes.
    /// percent=0 means single node (default), percent=100 means all nodes.
    SetDistributionConfig {
        catalog: String,
        schema: String,
        table: String,
        percent: u8,
    },

    /// Delete distribution config
    ///
    /// Removes distribution config, reverting to default (single node).
    DeleteDistributionConfig {
        catalog: String,
        schema: String,
        table: String,
    },

    // Lock operations
    AcquireLock {
        key: String,
        holder: String,
        ttl_ms: u64,
    },
    ReleaseLock {
        key: String,
        holder: String,
        fencing_token: u64,
    },
    RenewLock {
        key: String,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    },

    // Leader election
    Campaign {
        election: String,
        node_id: String,
        ttl_ms: u64,
    },
    Resign {
        election: String,
        node_id: String,
        term: u64,
    },

    // Events
    PublishEvent {
        topic: String,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: String,
    },

    // Lease management
    ExpireLeases,

    // PromoteDataNode removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    // UpdateDataNodeMode removed - No-Promotion architecture
    // Mode is set at registration time and never changes dynamically

    // Scale-in (retirement) operations
    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins partition reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    StartRetire {
        node_id: String,
    },

    /// Mark a node as fully drained (scale-in complete)
    ///
    /// Called when:
    /// - All tables have been reassigned
    /// - Memtable has been flushed to S3
    /// - Node is safe to stop
    CompleteRetire {
        node_id: String,
    },

    /// Cancel a node retirement (scale-in abort)
    ///
    /// Restores the node to Active state.
    /// Only works if retirement hasn't completed.
    CancelRetire {
        node_id: String,
    },

    /// Update retirement status (internal)
    ///
    /// Updates progress information during retirement.
    UpdateRetireStatus {
        node_id: String,
        tables_reassigned: usize,
        memtable_rows_remaining: u64,
    },

    // Recovery operations (failover without promotion)
    /// Trigger Standby to flush a crashed Primary's replicated data
    ///
    /// When a Primary crashes (No-Promotion architecture):
    /// 1. Find Standby backing the crashed Primary
    /// 2. Standby flushes all replicated memtable data to Parquet
    /// 3. This ensures no data loss before partition redistribution
    TriggerStandbyFlush {
        standby_id: String,
        crashed_primary_id: String,
    },

    /// Assign a spare Standby to back specific Primaries (node-level pairing)
    ///
    /// When an active Standby crashes:
    /// 1. Find spare Standby from pool
    /// 2. Assign it to back the affected Primaries
    /// 3. Start replication from Primaries to new Standby
    AssignStandbyToPrimaries {
        standby_id: String,
        primary_ids: Vec<String>,
    },

    /// Mark a Primary as unprotected (no Standby backup)
    ///
    /// Used when:
    /// - A Standby crashes and no spare is available
    /// - Primary has no replication target
    /// - Emits warning based on unprotected_write_policy
    MarkPrimaryUnprotected {
        primary_id: String,
        reason: String,
    },

    /// Redistribute tables from a crashed Primary to remaining Primaries
    ///
    /// Called during Primary crash recovery (No-Promotion architecture):
    /// 1. Get all tables owned by crashed Primary
    /// 2. Distribute evenly to remaining active Primaries
    /// 3. Update table ownership
    RedistributeTables {
        crashed_primary_id: String,
        target_primaries: Vec<String>,
    },
}

/// Response from applying a MetaCommand
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaResponse {
    Success,
    NodeRegistered {
        lease_id: u64,
    },

    /// Leases expired and nodes deregistered
    LeasesExpired {
        expired_nodes: Vec<String>,
    },

    // ========================================================================
    // Table-Level Responses
    // ========================================================================
    /// Table registered successfully
    TableRegistered {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        version: u64,
    },

    /// Table already exists (idempotent registration)
    TableAlreadyExists {
        catalog: String,
        schema: String,
        table: String,
        current_owner: String,
        /// All owner nodes (for multi-node tables)
        all_owners: Vec<String>,
    },

    /// Table stats updated successfully
    TableStatsUpdated {
        catalog: String,
        schema: String,
        table: String,
        version: u64,
    },

    /// Table reassigned to new primary owner
    TableReassigned {
        catalog: String,
        schema: String,
        table: String,
        old_owner: String,
        new_owner: String,
        /// All owner nodes after reassignment
        all_owners: Vec<String>,
        version: u64,
    },

    /// Additional owner added to table (for rerouted writes)
    TableOwnerAdded {
        catalog: String,
        schema: String,
        table: String,
        new_owner: String,
        /// TTL in milliseconds for this ownership
        ttl_ms: u64,
        /// Expiration timestamp
        expires_at: i64,
        /// All owner nodes after addition
        all_owners: Vec<String>,
        version: u64,
    },

    /// Owner removed from table
    TableOwnerRemoved {
        catalog: String,
        schema: String,
        table: String,
        removed_owner: String,
        /// Remaining owner nodes
        all_owners: Vec<String>,
        version: u64,
    },

    /// Node is already an owner of the table
    TableOwnerExists {
        catalog: String,
        schema: String,
        table: String,
        node_id: String,
        /// TTL was refreshed
        ttl_refreshed: bool,
        /// New expiration timestamp (if refreshed)
        expires_at: i64,
        all_owners: Vec<String>,
    },

    /// Owner TTL refreshed successfully
    OwnerTTLRefreshed {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        expires_at: i64,
    },

    /// Owner not found for TTL refresh
    OwnerNotFound {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Expired owners cleaned up
    ExpiredOwnersCleaned {
        /// Number of expired owner entries removed
        removed_count: usize,
        /// Tables that lost all owners and were removed
        orphaned_tables: Vec<String>,
    },

    /// Table removed successfully
    TableRemoved {
        catalog: String,
        schema: String,
        table: String,
    },

    /// Table not found
    TableNotFound {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Distribution Config Responses
    // ========================================================================
    /// Distribution config set successfully
    DistributionConfigSet {
        catalog: String,
        schema: String,
        table: String,
        percent: u8,
        version: u64,
    },

    /// Distribution config deleted successfully
    DistributionConfigDeleted {
        catalog: String,
        schema: String,
        table: String,
    },

    /// Distribution config not found (when deleting)
    DistributionConfigNotFound {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Lock Responses
    // ========================================================================
    LockAcquired {
        fencing_token: u64,
    },
    LockNotAcquired {
        current_holder: String,
        expires_at: i64,
    },
    LockReleased,
    LockRenewed {
        expires_at: i64,
    },
    LeaderElected {
        term: u64,
    },
    LeaderExists {
        current_leader: String,
        term: u64,
    },
    EventPublished {
        sequence: u64,
    },
    NoDataNodesAvailable,
    /// Data node not found
    DataNodeNotFound {
        node_id: String,
    },

    // Scale-in (retirement) responses
    /// Node retirement started successfully
    RetireStarted {
        node_id: String,
        tables_to_reassign: usize,
    },
    /// Node retirement completed, safe to stop
    RetireCompleted {
        node_id: String,
    },
    /// Node retirement cancelled, restored to Active
    RetireCancelled {
        node_id: String,
    },
    /// Retirement status updated
    RetireStatusUpdated {
        node_id: String,
        progress_percent: u8,
    },
    /// Table reassigned from retiring node
    TableReassignedForRetire {
        table_key: String,
        from_node_id: String,
        to_node_id: String,
    },
    /// Cannot retire node - no other nodes to take tables
    CannotRetireNoAlternative {
        node_id: String,
        reason: String,
    },
    /// Node is not in a retirable state
    NodeNotRetirable {
        node_id: String,
        current_state: NodeState,
    },
    /// Scale-in recommendation generated
    ScaleInRecommended {
        recommendation: ScaleInRecommendation,
    },

    // Recovery responses
    /// Standby flush triggered for crashed Primary
    StandbyFlushTriggered {
        standby_id: String,
        crashed_primary_id: String,
    },

    /// Spare Standby assigned to Primaries (node-level pairing)
    StandbyAssignedToPrimaries {
        standby_id: String,
        primary_ids: Vec<String>,
    },

    /// Primary marked as unprotected (no Standby backup)
    PrimaryMarkedUnprotected {
        primary_id: String,
    },

    /// Tables redistributed from crashed Primary
    TablesRedistributed {
        crashed_primary_id: String,
        tables_count: usize,
        target_primaries: Vec<String>,
    },

    /// No spare Standby available
    NoSpareStandbyAvailable,

    Error {
        message: String,
    },
}
