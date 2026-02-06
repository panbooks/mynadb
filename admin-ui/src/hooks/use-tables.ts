import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchTables, reassignTable } from '@/lib/api'
import type { TableFilter, ReassignTableRequest } from '@/lib/types'

export function useTables(filter?: TableFilter, refetchInterval = 0) {
  return useQuery({
    queryKey: ['tables', filter],
    queryFn: () => fetchTables(filter),
    refetchInterval: refetchInterval || undefined,
  })
}

export function useReassignTable() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ tableKey, request }: { tableKey: string; request: ReassignTableRequest }) =>
      reassignTable(tableKey, request),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tables'] })
      queryClient.invalidateQueries({ queryKey: ['nodes'] })
    },
  })
}
