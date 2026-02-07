import { useMutation } from '@tanstack/react-query'
import { prewarmCache, clearCache } from '@/lib/api'

export function usePrewarmCache() {
  return useMutation({
    mutationFn: (catalog: string) => prewarmCache(catalog),
  })
}

export function useClearCache() {
  return useMutation({
    mutationFn: clearCache,
  })
}
