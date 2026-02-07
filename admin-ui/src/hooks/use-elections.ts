import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchElections, campaignElection, resignElection } from '@/lib/api'

export function useElections(refetchInterval = 10000) {
  return useQuery({
    queryKey: ['elections'],
    queryFn: fetchElections,
    refetchInterval,
  })
}

export function useCampaign() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ election, ttlMs }: { election: string; ttlMs?: number }) =>
      campaignElection(election, ttlMs),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['elections'] })
    },
  })
}

export function useResign() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (election: string) => resignElection(election),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['elections'] })
    },
  })
}
