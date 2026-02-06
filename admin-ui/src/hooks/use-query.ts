import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  executeQuery,
  explainQuery,
  analyzeQuery,
  fetchCatalogs,
  fetchSchemas,
  fetchTableColumns,
  fetchSavedQueries,
  saveQuery,
  deleteSavedQuery,
  fetchQueryHistory,
  clearQueryHistory,
} from '@/lib/api'
import type { QueryRequest, SavedQuery } from '@/lib/types'

export function useCatalogs() {
  return useQuery({
    queryKey: ['catalogs'],
    queryFn: fetchCatalogs,
    staleTime: 30000,
  })
}

export function useSchemas(catalog: string | undefined) {
  return useQuery({
    queryKey: ['schemas', catalog],
    queryFn: () => fetchSchemas(catalog!),
    enabled: !!catalog,
    staleTime: 30000,
  })
}

export function useTableColumns(catalog: string | undefined, schema: string | undefined, table: string | undefined) {
  return useQuery({
    queryKey: ['columns', catalog, schema, table],
    queryFn: () => fetchTableColumns(catalog!, schema!, table!),
    enabled: !!catalog && !!schema && !!table,
    staleTime: 30000,
  })
}

export function useExecuteQuery() {
  return useMutation({
    mutationFn: (request: QueryRequest) => executeQuery(request),
  })
}

export function useExplainQuery() {
  return useMutation({
    mutationFn: ({ catalog, sql }: { catalog: string; sql: string }) => explainQuery(catalog, sql),
  })
}

export function useAnalyzeQuery() {
  return useMutation({
    mutationFn: ({ catalog, sql }: { catalog: string; sql: string }) => analyzeQuery(catalog, sql),
  })
}

export function useSavedQueries() {
  return useQuery({
    queryKey: ['saved-queries'],
    queryFn: fetchSavedQueries,
    staleTime: 10000,
  })
}

export function useSaveQuery() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (query: { name: string; sql: string; catalog: string }) => saveQuery(query),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['saved-queries'] })
    },
  })
}

export function useDeleteSavedQuery() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => deleteSavedQuery(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['saved-queries'] })
    },
  })
}

export function useQueryHistory(limit?: number) {
  return useQuery({
    queryKey: ['query-history', limit],
    queryFn: () => fetchQueryHistory(limit),
    staleTime: 5000,
  })
}

export function useClearQueryHistory() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: clearQueryHistory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['query-history'] })
    },
  })
}

// Local storage for query history when backend doesn't support it
const LOCAL_HISTORY_KEY = 'mynadb-query-history'
const LOCAL_SAVED_KEY = 'mynadb-saved-queries'
const MAX_HISTORY = 100

export function getLocalQueryHistory(): QueryHistoryEntry[] {
  try {
    const data = localStorage.getItem(LOCAL_HISTORY_KEY)
    return data ? JSON.parse(data) : []
  } catch {
    return []
  }
}

export function addLocalQueryHistory(entry: Omit<QueryHistoryEntry, 'id'>): void {
  const history = getLocalQueryHistory()
  const newEntry = {
    ...entry,
    id: `local-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
  }
  history.unshift(newEntry)
  if (history.length > MAX_HISTORY) {
    history.pop()
  }
  localStorage.setItem(LOCAL_HISTORY_KEY, JSON.stringify(history))
}

export function clearLocalQueryHistory(): void {
  localStorage.removeItem(LOCAL_HISTORY_KEY)
}

export function getLocalSavedQueries(): SavedQuery[] {
  try {
    const data = localStorage.getItem(LOCAL_SAVED_KEY)
    return data ? JSON.parse(data) : []
  } catch {
    return []
  }
}

export function addLocalSavedQuery(query: { name: string; sql: string; catalog: string }): SavedQuery {
  const saved = getLocalSavedQueries()
  const now = new Date().toISOString()
  const newQuery: SavedQuery = {
    ...query,
    id: `local-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    created_at: now,
    updated_at: now,
  }
  saved.unshift(newQuery)
  localStorage.setItem(LOCAL_SAVED_KEY, JSON.stringify(saved))
  return newQuery
}

export function deleteLocalSavedQuery(id: string): void {
  const saved = getLocalSavedQueries().filter((q) => q.id !== id)
  localStorage.setItem(LOCAL_SAVED_KEY, JSON.stringify(saved))
}

interface QueryHistoryEntry {
  id: string
  sql: string
  catalog: string
  executed_at: string
  execution_time_ms: number
  row_count: number
  status: 'success' | 'error'
  error?: string
}
