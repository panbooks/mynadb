import { useQuery } from '@tanstack/react-query'
import { fetchClusterStatus, fetchMetaStatus } from '@/lib/api'

export function useClusterStatus(refetchInterval = 5000) {
  return useQuery({
    queryKey: ['cluster-status'],
    queryFn: fetchClusterStatus,
    refetchInterval,
  })
}

export function useMetaStatus(refetchInterval = 5000) {
  return useQuery({
    queryKey: ['meta-status'],
    queryFn: fetchMetaStatus,
    refetchInterval,
  })
}
