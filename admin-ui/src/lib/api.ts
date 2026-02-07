import type {
  ApiResponse,
  ClusterStatus,
  NodeSummary,
  NodeDetail,
  TableSummary,
  ReassignTableRequest,
  ReassignTableResponse,
  ScaleInRecommendation,
  ScaleInPlanRequest,
  ScaleInPlan,
  ScaleInResult,
  AdminEvent,
  MetaClusterStatus,
  DistributionConfig,
  NodeFilter,
  TableFilter,
  EventFilter,
  EventStats,
  PaginatedResponse,
  QueryRequest,
  QueryResult,
  QueryExplain,
  QueryAnalysis,
  SavedQuery,
  QueryHistoryEntry,
  StorageInfo,
  CatalogInfo,
  SchemaInfo,
  TableBasicInfo,
  TableStorageInfo,
  ParquetFileDetails,
} from './types'

const API_BASE = '/admin/api/v1'

async function fetchApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const url = `${API_BASE}${endpoint}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })

  if (!response.ok) {
    const error = await response.text()
    throw new Error(error || `HTTP ${response.status}`)
  }

  const data: ApiResponse<T> = await response.json()
  if (!data.success) {
    throw new Error(data.error || data.message || 'Unknown error')
  }

  return data.data as T
}

// Cluster API
export async function fetchClusterStatus(): Promise<ClusterStatus> {
  return fetchApi<ClusterStatus>('/cluster/status')
}

// Nodes API
export async function fetchNodes(filter?: NodeFilter): Promise<NodeSummary[]> {
  const params = new URLSearchParams()
  if (filter?.role) params.set('role', filter.role)
  if (filter?.state) params.set('state', filter.state)
  if (filter?.mode) params.set('mode', filter.mode)

  const query = params.toString()
  return fetchApi<NodeSummary[]>(`/nodes${query ? `?${query}` : ''}`)
}

export async function fetchNode(nodeId: string): Promise<NodeDetail> {
  return fetchApi<NodeDetail>(`/nodes/${encodeURIComponent(nodeId)}`)
}

export async function retireNode(nodeId: string): Promise<{ success: boolean; message: string }> {
  return fetchApi<{ success: boolean; message: string }>(`/nodes/${encodeURIComponent(nodeId)}/retire`, {
    method: 'POST',
  })
}

export async function getRetireStatus(nodeId: string): Promise<{ status: string; progress: number }> {
  return fetchApi<{ status: string; progress: number }>(`/nodes/${encodeURIComponent(nodeId)}/retire/status`)
}

export async function cancelRetire(nodeId: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/nodes/${encodeURIComponent(nodeId)}/retire/cancel`, {
    method: 'POST',
  })
}

export async function flushNode(nodeId: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/nodes/${encodeURIComponent(nodeId)}/flush`, {
    method: 'POST',
  })
}

export async function compactNode(nodeId: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/nodes/${encodeURIComponent(nodeId)}/compact`, {
    method: 'POST',
  })
}

export async function clearNodeCache(nodeId: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/nodes/${encodeURIComponent(nodeId)}/cache/clear`, {
    method: 'POST',
  })
}

export async function fetchNodeStats(nodeId: string): Promise<Record<string, unknown>> {
  return fetchApi<Record<string, unknown>>(`/nodes/${encodeURIComponent(nodeId)}/stats`)
}

// Tables API
export async function fetchTables(filter?: TableFilter): Promise<PaginatedResponse<TableSummary>> {
  const params = new URLSearchParams()
  if (filter?.catalog) params.set('catalog', filter.catalog)
  if (filter?.schema) params.set('schema', filter.schema)
  if (filter?.owner) params.set('owner', filter.owner)
  if (filter?.search) params.set('search', filter.search)
  if (filter?.page) params.set('page', filter.page.toString())
  if (filter?.page_size) params.set('page_size', filter.page_size.toString())

  const query = params.toString()
  return fetchApi<PaginatedResponse<TableSummary>>(`/tables${query ? `?${query}` : ''}`)
}

export async function reassignTable(
  tableKey: string,
  request: ReassignTableRequest
): Promise<ReassignTableResponse> {
  return fetchApi<ReassignTableResponse>(`/tables/${encodeURIComponent(tableKey)}/reassign`, {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

// Scale-in API
export async function fetchScaleInRecommendations(count: number): Promise<ScaleInRecommendation[]> {
  return fetchApi<ScaleInRecommendation[]>(`/scale-in/recommend?count=${count}`)
}

export async function createScaleInPlan(request: ScaleInPlanRequest): Promise<ScaleInPlan> {
  return fetchApi<ScaleInPlan>('/scale-in/plan', {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

export async function executeScaleIn(planId: string): Promise<ScaleInResult> {
  return fetchApi<ScaleInResult>('/scale-in/execute', {
    method: 'POST',
    body: JSON.stringify({ plan_id: planId }),
  })
}

export async function fetchScaleInStatus(planId: string): Promise<ScaleInPlan> {
  return fetchApi<ScaleInPlan>(`/scale-in/status/${encodeURIComponent(planId)}`)
}

export async function cancelScaleIn(planId: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/scale-in/cancel/${encodeURIComponent(planId)}`, {
    method: 'POST',
  })
}

// Events API
interface EventsResponse {
  events: AdminEvent[]
  total: number
  cache_size: number
  cache_max: number
}

export async function fetchEvents(filter?: EventFilter): Promise<AdminEvent[]> {
  const params = new URLSearchParams()
  if (filter?.type) params.set('type', filter.type)
  if (filter?.source) params.set('source', filter.source)
  if (filter?.since) params.set('since', filter.since)
  if (filter?.search) params.set('search', filter.search)
  if (filter?.limit) params.set('limit', filter.limit.toString())

  const query = params.toString()
  const response = await fetchApi<EventsResponse>(`/events${query ? `?${query}` : ''}`)
  return response.events
}

export async function fetchEventSources(): Promise<string[]> {
  return fetchApi<string[]>('/events/sources')
}

export async function fetchEventStats(): Promise<EventStats> {
  return fetchApi<EventStats>('/events/stats')
}

// WebSocket for real-time events
export function createEventWebSocket(
  onEvent: (event: AdminEvent) => void,
  onError?: (error: Event) => void,
  onClose?: () => void
): WebSocket {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const wsUrl = `${protocol}//${window.location.host}/admin/api/v1/events/ws`

  const ws = new WebSocket(wsUrl)

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data)
      // Skip heartbeat events
      if (data.type === 'Heartbeat') {
        return
      }
      // Ensure the event has all required fields
      const adminEvent: AdminEvent = {
        id: data.id || `evt-${Date.now()}`,
        type: data.type || 'Unknown',
        timestamp: data.timestamp || new Date().toISOString(),
        source: data.source,
        payload: data.payload || {},
        message: data.message || '',
      }
      onEvent(adminEvent)
    } catch (err) {
      console.error('Failed to parse WebSocket message:', event.data, err)
    }
  }

  ws.onerror = (event) => {
    console.error('WebSocket error:', event)
    onError?.(event)
  }

  ws.onclose = () => {
    onClose?.()
  }

  return ws
}

// Meta API
export async function fetchMetaStatus(): Promise<MetaClusterStatus> {
  return fetchApi<MetaClusterStatus>('/meta/status')
}

// Distribution Config API
export async function fetchDistributionConfigs(): Promise<DistributionConfig[]> {
  return fetchApi<DistributionConfig[]>('/distribution')
}

export async function setDistributionConfig(config: DistributionConfig): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>('/distribution', {
    method: 'POST',
    body: JSON.stringify(config),
  })
}

// Query Console API
export async function executeQuery(request: QueryRequest): Promise<QueryResult> {
  return fetchApi<QueryResult>('/query/execute', {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

export async function explainQuery(catalog: string, sql: string): Promise<QueryExplain> {
  const params = new URLSearchParams({ sql })
  return fetchApi<QueryExplain>(`/query/explain/${encodeURIComponent(catalog)}?${params}`)
}

export async function analyzeQuery(catalog: string, sql: string): Promise<QueryAnalysis> {
  const params = new URLSearchParams({ sql })
  return fetchApi<QueryAnalysis>(`/query/analyze/${encodeURIComponent(catalog)}?${params}`)
}

export async function fetchCatalogs(): Promise<string[]> {
  return fetchApi<string[]>('/query/catalogs')
}

export async function fetchSchemas(catalog: string): Promise<string[]> {
  return fetchApi<string[]>(`/query/schemas/${encodeURIComponent(catalog)}`)
}

export async function fetchTableColumns(catalog: string, schema: string, table: string): Promise<{ name: string; data_type: string }[]> {
  return fetchApi<{ name: string; data_type: string }[]>(
    `/query/columns/${encodeURIComponent(catalog)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`
  )
}

export async function fetchSavedQueries(): Promise<SavedQuery[]> {
  return fetchApi<SavedQuery[]>('/query/saved')
}

export async function saveQuery(query: { name: string; sql: string; catalog: string }): Promise<SavedQuery> {
  return fetchApi<SavedQuery>('/query/saved', {
    method: 'POST',
    body: JSON.stringify(query),
  })
}

export async function deleteSavedQuery(id: string): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>(`/query/saved/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  })
}

export async function fetchQueryHistory(limit?: number): Promise<QueryHistoryEntry[]> {
  const params = limit ? `?limit=${limit}` : ''
  return fetchApi<QueryHistoryEntry[]>(`/query/history${params}`)
}

export async function clearQueryHistory(): Promise<{ success: boolean }> {
  return fetchApi<{ success: boolean }>('/query/history', {
    method: 'DELETE',
  })
}

// Storage Browser API
export async function fetchStorageInfo(): Promise<StorageInfo> {
  return fetchApi<StorageInfo>('/storage/info')
}

export async function fetchStorageCatalogs(): Promise<CatalogInfo[]> {
  return fetchApi<CatalogInfo[]>('/storage/catalogs')
}

export async function fetchStorageSchemas(catalog: string): Promise<SchemaInfo[]> {
  return fetchApi<SchemaInfo[]>(`/storage/catalogs/${encodeURIComponent(catalog)}/schemas`)
}

export async function fetchStorageTables(catalog: string, schema: string): Promise<TableBasicInfo[]> {
  return fetchApi<TableBasicInfo[]>(
    `/storage/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables`
  )
}

export async function fetchStorageTable(catalog: string, schema: string, table: string): Promise<TableStorageInfo> {
  return fetchApi<TableStorageInfo>(
    `/storage/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`
  )
}

export async function fetchParquetDetails(path: string): Promise<ParquetFileDetails> {
  const params = new URLSearchParams({ path })
  return fetchApi<ParquetFileDetails>(`/storage/file?${params}`)
}

export async function deletePartition(catalog: string, schema: string, table: string, date: string): Promise<{ success: boolean; deleted_files: number; freed_bytes: number }> {
  return fetchApi<{ success: boolean; deleted_files: number; freed_bytes: number }>(
    `/storage/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/partitions/${encodeURIComponent(date)}`,
    { method: 'DELETE' }
  )
}

export async function triggerCompaction(catalog: string, schema?: string, table?: string): Promise<{ success: boolean; message: string }> {
  let endpoint = `/storage/compact/${encodeURIComponent(catalog)}`
  if (schema) endpoint += `/${encodeURIComponent(schema)}`
  if (table) endpoint += `/${encodeURIComponent(table)}`
  return fetchApi<{ success: boolean; message: string }>(endpoint, { method: 'POST' })
}
