// API Response wrapper
export interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: string
  message?: string
}

// Node types
export type NodeRole = 'Unknown' | 'Meta' | 'Router' | 'Data' | 'Compactor'
export type NodeState = 'Unknown' | 'Starting' | 'Active' | 'Draining' | 'Inactive' | 'Retiring' | 'Drained'
export type DataNodeMode = 'Primary' | 'Standby'

export interface NodeInfo {
  id: string
  role: NodeRole
  state: NodeState
  advertise_addr: string
  http_addr?: string
  flight_addr?: string
  labels: Record<string, string>
  zone?: string
  mode?: DataNodeMode
  paired_node_id?: string
  last_heartbeat?: string
  tables_owned?: number
  registered_at?: string
}

export interface NodeSummary {
  id: string
  role: NodeRole
  state: NodeState
  advertise_addr: string
  zone?: string
  mode?: DataNodeMode
  tables_owned?: number
  last_heartbeat?: string
}

export interface NodeDetail extends NodeInfo {
  health?: NodeHealth
  tables?: TableSummary[]
  stats?: NodeStats
}

export interface NodeHealth {
  is_healthy: boolean
  last_heartbeat: string
  heartbeat_age_ms: number
  message?: string
}

export interface NodeStats {
  memory_used_bytes?: number
  memory_total_bytes?: number
  cache_hits?: number
  cache_misses?: number
  active_queries?: number
  compaction_status?: string
}

// Cluster types
export interface ClusterStatus {
  healthy: boolean
  meta_leader?: string
  meta_term?: number
  node_counts: NodeCounts
  table_count: number
  partition_count: number
  warnings: string[]
}

export interface NodeCounts {
  meta: RoleCounts
  router: RoleCounts
  data: DataNodeCounts
  compactor?: RoleCounts
}

export interface RoleCounts {
  total: number
  active: number
  inactive: number
}

export interface DataNodeCounts {
  total: number
  active: number
  inactive: number
  primary: number
  standby: number
  retiring: number
  drained: number
}

// Table types
export interface TableMeta {
  catalog: string
  schema: string
  table: string
  key: string
  owner_node_ids: string[]
  partition_count: number
  total_bytes: number
  total_rows: number
}

export interface TableSummary {
  key: string
  catalog: string
  schema: string
  table: string
  owner_node_id?: string
  partition_count: number
  total_bytes: number
  total_rows: number
}

export interface ReassignTableRequest {
  target_node_id: string
}

export interface ReassignTableResponse {
  success: boolean
  message: string
  old_owner?: string
  new_owner: string
}

// Scale-in types
export type ScaleInStrategy = 'LeastLoaded' | 'FirstIn' | 'LastIn' | 'HaUnit'

export interface ScaleInRecommendation {
  node_id: string
  role: NodeRole
  mode?: DataNodeMode
  tables_owned: number
  reason: string
}

export interface ScaleInPlanRequest {
  node_count: number
  strategy: ScaleInStrategy
  specific_node_ids?: string[]
  dry_run?: boolean
}

export interface ScaleInPlan {
  id: string
  status: ScaleInPlanStatus
  phases: ScaleInPhase[]
  nodes_to_retire: string[]
  tables_to_migrate: number
  estimated_duration_ms?: number
  warnings: string[]
  created_at: string
}

export type ScaleInPlanStatus = 'Pending' | 'InProgress' | 'Completed' | 'Failed' | 'Cancelled'

export interface ScaleInPhase {
  phase_number: number
  description: string
  operations: ScaleInOperation[]
  status: ScaleInPhaseStatus
}

export type ScaleInPhaseStatus = 'Pending' | 'InProgress' | 'Completed' | 'Failed'

export interface ScaleInOperation {
  type: ScaleInOperationType
  node_id?: string
  table_key?: string
  target_node_id?: string
  status: ScaleInOperationStatus
  message?: string
}

export type ScaleInOperationType =
  | 'MigrateTable'
  | 'FlushMemtable'
  | 'StartRetire'
  | 'CompleteRetire'
  | 'RemoveStandbyFromTables'
  | 'WaitForDrain'

export type ScaleInOperationStatus = 'Pending' | 'InProgress' | 'Completed' | 'Failed' | 'Skipped'

export interface ScaleInResult {
  plan_id: string
  success: boolean
  nodes_retired: string[]
  tables_migrated: number
  duration_ms: number
  errors: string[]
}

// Event types - must match backend EventType enum
export type EventType =
  | 'NodeJoined'
  | 'NodeLeft'
  | 'NodeStateChanged'
  | 'LeaderChanged'
  | 'TableReassigned'
  | 'TableOwnerAdded'
  | 'TableOwnerRemoved'
  | 'TableOwnerExpired'
  | 'CacheInvalidate'
  | 'SchemaChanged'
  | 'FlushForRecovery'
  | 'DistributionConfigChanged'
  | 'TablesCacheRefresh'
  | 'Heartbeat'
  | 'Unknown'

export interface AdminEvent {
  id: string
  type: EventType
  timestamp: string
  source?: string
  payload: Record<string, unknown>
  message: string
}

// Meta cluster types
export interface MetaClusterStatus {
  leader_id?: string
  term: number
  commit_index: number
  peers: MetaPeer[]
  is_healthy: boolean
}

export interface MetaPeer {
  id: string
  address: string
  is_leader: boolean
  is_healthy: boolean
  last_heartbeat?: string
}

// Distribution config types
export interface DistributionConfig {
  scope: DistributionScope
  catalog?: string
  schema?: string
  table?: string
  replication_factor: number
  placement_strategy: PlacementStrategy
}

export type DistributionScope = 'Global' | 'Catalog' | 'Schema' | 'Table'
export type PlacementStrategy = 'RoundRobin' | 'LeastLoaded' | 'ZoneAware'

// Request/Response types for filters
export interface NodeFilter {
  role?: NodeRole
  state?: NodeState
  mode?: DataNodeMode
}

export interface TableFilter {
  catalog?: string
  schema?: string
  owner?: string
  search?: string
  page?: number
  page_size?: number
}

export interface EventFilter {
  type?: EventType
  source?: string
  since?: string
  limit?: number
}

// Pagination
export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}
