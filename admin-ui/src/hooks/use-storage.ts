import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchStorageInfo,
  fetchStorageCatalogs,
  fetchStorageSchemas,
  fetchStorageTables,
  fetchStorageTable,
  fetchParquetDetails,
  deletePartition,
  triggerCompaction,
} from '@/lib/api'

export function useStorageInfo() {
  return useQuery({
    queryKey: ['storage-info'],
    queryFn: fetchStorageInfo,
    staleTime: 30000,
  })
}

export function useStorageCatalogs() {
  return useQuery({
    queryKey: ['storage-catalogs'],
    queryFn: fetchStorageCatalogs,
    staleTime: 30000,
  })
}

export function useStorageSchemas(catalog: string | undefined) {
  return useQuery({
    queryKey: ['storage-schemas', catalog],
    queryFn: () => fetchStorageSchemas(catalog!),
    enabled: !!catalog,
    staleTime: 30000,
  })
}

export function useStorageTables(catalog: string | undefined, schema: string | undefined) {
  return useQuery({
    queryKey: ['storage-tables', catalog, schema],
    queryFn: () => fetchStorageTables(catalog!, schema!),
    enabled: !!catalog && !!schema,
    staleTime: 30000,
  })
}

export function useStorageTable(catalog: string | undefined, schema: string | undefined, table: string | undefined) {
  return useQuery({
    queryKey: ['storage-table', catalog, schema, table],
    queryFn: () => fetchStorageTable(catalog!, schema!, table!),
    enabled: !!catalog && !!schema && !!table,
    staleTime: 10000,
  })
}

export function useParquetDetails(path: string | undefined) {
  return useQuery({
    queryKey: ['parquet-details', path],
    queryFn: () => fetchParquetDetails(path!),
    enabled: !!path,
    staleTime: 60000,
  })
}

export function useDeletePartition() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ catalog, schema, table, date }: { catalog: string; schema: string; table: string; date: string }) =>
      deletePartition(catalog, schema, table, date),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['storage-table', variables.catalog, variables.schema, variables.table] })
      queryClient.invalidateQueries({ queryKey: ['storage-schemas', variables.catalog] })
      queryClient.invalidateQueries({ queryKey: ['storage-catalogs'] })
      queryClient.invalidateQueries({ queryKey: ['storage-info'] })
    },
  })
}

export function useTriggerCompaction() {
  return useMutation({
    mutationFn: ({ catalog, schema, table }: { catalog: string; schema?: string; table?: string }) =>
      triggerCompaction(catalog, schema, table),
  })
}
