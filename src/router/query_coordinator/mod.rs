//! Query Coordinator - Coordinates distributed queries across Data nodes
//!
//! The QueryCoordinator is responsible for:
//! - Parsing SQL to extract table references and time ranges
//! - Determining which Data nodes to query based on partition-level routing
//! - Fan-out queries to multiple Data nodes via Arrow Flight
//! - Aggregating results from partial queries
//!
//! ## Partition-Level Routing (NEW - Hive Partition Architecture)
//!
//! With node_id as a Hive partition key, queries are routed based on:
//! 1. Extract table references and date filters from SQL
//! 2. Look up partition cache: (table, date) → [node_ids]
//! 3. Route query ONLY to nodes that have data for the requested partitions
//!
//! Benefits:
//! - **60-90% reduction in query fan-out** (only query relevant nodes)
//! - **Cache affinity preserved** (writer = reader)
//! - **Zero Meta storage** for routing (vs 16.8MB+ for file-level)
//! - **Automatic pruning** via DataFusion's Hive partition support
//!
//! ## Fallback to Table-Level Routing
//!
//! When no date filter is present or partition cache is empty, falls back to
//! table-level routing (query all table owners).

mod aggregation;
mod execution;
mod planning;
mod routing;
mod sql_rewrite;
mod types;

#[cfg(test)]
mod tests;

pub use types::{DistributedQueryPlan, PartitionRoutingResult, QueryResult, TableRef, TimeRange};

use super::shard_cache::{DataNodeInfo, ShardCache};
use crate::meta::client::MetaClient;
use crate::router::client::DataNodePool;
use crate::storage::storage::Storage;
use std::sync::Arc;

/// Query Coordinator for distributed query execution
pub struct QueryCoordinator {
    /// Shard cache for partition→node mapping
    pub(super) shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    pub(super) data_node_pool: Arc<DataNodePool>,

    /// Max parallel fan-out
    pub(super) max_parallel_fanout: usize,

    /// Optional Meta client for file-based routing
    /// When available, enables distributed parallel queries with node affinity
    pub(super) meta_client: Option<Arc<MetaClient>>,

    /// Optional Storage for S3 LIST-based partition discovery
    /// When partition cache misses, we can discover `node_id=*` subdirectories
    /// via S3 LIST to avoid full table-level broadcast.
    pub(super) storage: Option<Storage>,
}

impl QueryCoordinator {
    pub fn new(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: None,
            storage: None,
        }
    }

    /// Create with MetaClient for file-based distributed queries
    pub fn with_meta_client(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        meta_client: Arc<MetaClient>,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: Some(meta_client),
            storage: None,
        }
    }

    /// Create with Storage for S3 LIST-based partition discovery
    pub fn with_storage(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        storage: Storage,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: None,
            storage: Some(storage),
        }
    }

    /// Create with both MetaClient and Storage
    pub fn with_meta_and_storage(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        meta_client: Arc<MetaClient>,
        storage: Storage,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: Some(meta_client),
            storage: Some(storage),
        }
    }

    /// Set the MetaClient (can be called after creation)
    pub fn set_meta_client(&mut self, meta_client: Arc<MetaClient>) {
        self.meta_client = Some(meta_client);
    }

    /// Set the Storage (can be called after creation)
    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = Some(storage);
    }
}
