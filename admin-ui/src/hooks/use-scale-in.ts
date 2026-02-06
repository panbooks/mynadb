import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchScaleInRecommendations,
  createScaleInPlan,
  executeScaleIn,
  fetchScaleInStatus,
  cancelScaleIn,
} from '@/lib/api'
import type { ScaleInPlanRequest } from '@/lib/types'

export function useScaleInRecommendations(count: number, enabled = true) {
  return useQuery({
    queryKey: ['scale-in-recommendations', count],
    queryFn: () => fetchScaleInRecommendations(count),
    enabled: enabled && count > 0,
  })
}

export function useScaleInStatus(planId: string | null, enabled = false) {
  return useQuery({
    queryKey: ['scale-in-status', planId],
    queryFn: () => fetchScaleInStatus(planId!),
    enabled: enabled && !!planId,
    refetchInterval: 2000,
  })
}

export function useCreateScaleInPlan() {
  return useMutation({
    mutationFn: (request: ScaleInPlanRequest) => createScaleInPlan(request),
  })
}

export function useExecuteScaleIn() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (planId: string) => executeScaleIn(planId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['nodes'] })
      queryClient.invalidateQueries({ queryKey: ['cluster-status'] })
    },
  })
}

export function useCancelScaleIn() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (planId: string) => cancelScaleIn(planId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scale-in-status'] })
    },
  })
}
