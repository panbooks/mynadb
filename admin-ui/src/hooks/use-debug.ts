import { useMutation } from '@tanstack/react-query'
import { prewarmCache, clearCache, generateTestData } from '@/lib/api'
import type { GenerateDataRequest } from '@/lib/api'

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

export function useGenerateTestData() {
  return useMutation({
    mutationFn: (request: GenerateDataRequest) => generateTestData(request),
  })
}
