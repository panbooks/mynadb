//! Query Planning - Distributed query plan generation
//!
//! This module handles planning distributed queries, including:
//! - Extracting table references and time ranges from SQL
//! - Partition-level routing (when date filters are present)
//! - Table-level routing fallback

use super::types::{DistributedQueryPlan, TableRef, TimeRange};
use super::QueryCoordinator;
use crate::query::sql_utils::{extract_table_references, extract_time_range};
use std::collections::HashMap;
use tracing::{debug, info};

impl QueryCoordinator {
    /// Plan a distributed query
    pub(super) async fn plan_distributed_query(
        &self,
        catalog: &str,
        sql: &str,
    ) -> Result<DistributedQueryPlan, String> {
        // Extract table references from SQL
        let tables: Vec<TableRef> = extract_table_references(sql, catalog)
            .iter()
            .map(|(_, schema, table)| TableRef {
                schema: schema.clone(),
                table: table.clone(),
            })
            .collect();

        info!("Extracted {} table references: {:?}", tables.len(), tables);

        // Extract time range for partition pruning
        let time_range = extract_time_range(sql).map(|tr| TimeRange {
            min_time: tr.start_timestamp,
            max_time: tr.end_timestamp,
            start_date: tr.start_date,
            end_date: tr.end_date,
        });

        // Detect aggregation/ordering that needs consolidation
        let sql_upper = sql.to_uppercase();
        let needs_aggregation = [
            "GROUP BY", "ORDER BY", "LIMIT", " COUNT(", " SUM(", " AVG(", " MIN(", " MAX(",
        ]
        .iter()
        .any(|kw| sql_upper.contains(kw));

        // Get all queryable nodes
        let all_primary_nodes = self.shard_cache.get_queryable_primary_nodes();

        // ====================================================================
        // PARTITION-LEVEL ROUTING (Hive Partition Architecture)
        // ====================================================================
        // Try partition-level routing first (when date filters are present).
        // This reduces query fan-out by 60-90% by only querying nodes that
        // actually have data for the requested partitions.
        //
        // With partition assignments, each node receives SQL rewritten to only
        // query its assigned partitions (cache affinity, no duplicates).

        if let Some(routing_result) = self
            .try_partition_routing(catalog, &tables, &time_range, &all_primary_nodes)
            .await
        {
            // Partition routing succeeded - use these nodes with assignments
            let target_nodes: Vec<_> = routing_result
                .target_nodes
                .into_iter()
                .take(self.max_parallel_fanout)
                .collect();

            // Filter partition assignments to only include nodes we're targeting
            let target_node_ids: std::collections::HashSet<_> =
                target_nodes.iter().map(|n| n.node_id.as_str()).collect();
            let partition_assignments: HashMap<String, Vec<String>> = routing_result
                .partition_assignments
                .into_iter()
                .filter(|(node_id, _)| target_node_ids.contains(node_id.as_str()))
                .collect();

            info!(
                "Partition-level routing: {} nodes for {} tables with {} partition assignments",
                target_nodes.len(),
                tables.len(),
                partition_assignments.values().map(|v| v.len()).sum::<usize>()
            );

            return Ok(DistributedQueryPlan {
                sql: sql.to_string(),
                catalog: catalog.to_string(),
                tables,
                time_range,
                target_nodes,
                needs_aggregation,
                table_owners: None, // Partition routing doesn't use table_owners
                partition_assignments: Some(partition_assignments),
            });
        }

        // ====================================================================
        // FALLBACK: TABLE-LEVEL ROUTING
        // ====================================================================
        // No date filters or partition cache miss - fall back to table-level routing

        debug!("Falling back to table-level routing");

        // Resolve target nodes for query routing
        let (table_owners, target_nodes) = self
            .resolve_query_targets(catalog, &tables, &all_primary_nodes)
            .await;

        // Limit parallel fan-out
        let target_nodes: Vec<_> = target_nodes
            .into_iter()
            .take(self.max_parallel_fanout)
            .collect();

        info!(
            "Table-level routing: {} nodes, tables={:?}, aggregation={}",
            target_nodes.len(),
            tables,
            needs_aggregation,
        );

        Ok(DistributedQueryPlan {
            sql: sql.to_string(),
            catalog: catalog.to_string(),
            tables,
            time_range,
            target_nodes,
            needs_aggregation,
            table_owners,
            partition_assignments: None, // Table-level routing doesn't use partition assignments
        })
    }
}
