import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchNodes,
  fetchNode,
  retireNode,
  cancelRetire,
  flushNode,
  compactNode,
  clearNodeCache,
  fetchNodeStats,
  getRetireStatus,
} from '@/lib/api'
import type { NodeFilter } from '@/lib/types'

export function useNodes(filter?: NodeFilter, refetchInterval = 5000) {
  return useQuery({
    queryKey: ['nodes', filter],
    queryFn: () => fetchNodes(filter),
    refetchInterval,
  })
}

export function useNode(nodeId: string) {
  return useQuery({
    queryKey: ['node', nodeId],
    queryFn: () => fetchNode(nodeId),
    enabled: !!nodeId,
  })
}

export function useNodeStats(nodeId: string) {
  return useQuery({
    queryKey: ['node-stats', nodeId],
    queryFn: () => fetchNodeStats(nodeId),
    enabled: !!nodeId,
    refetchInterval: 10000,
  })
}

export function useRetireStatus(nodeId: string, enabled = false) {
  return useQuery({
    queryKey: ['retire-status', nodeId],
    queryFn: () => getRetireStatus(nodeId),
    enabled: enabled && !!nodeId,
    refetchInterval: 2000,
  })
}

export function useRetireNode() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (nodeId: string) => retireNode(nodeId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['nodes'] })
      queryClient.invalidateQueries({ queryKey: ['cluster-status'] })
    },
  })
}

export function useCancelRetire() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (nodeId: string) => cancelRetire(nodeId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['nodes'] })
      queryClient.invalidateQueries({ queryKey: ['cluster-status'] })
    },
  })
}

export function useFlushNode() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (nodeId: string) => flushNode(nodeId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['node-stats'] })
    },
  })
}

export function useCompactNode() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (nodeId: string) => compactNode(nodeId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['node-stats'] })
    },
  })
}

export function useClearNodeCache() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (nodeId: string) => clearNodeCache(nodeId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['node-stats'] })
    },
  })
}
