import { useQuery } from '@tanstack/react-query'
import { useEffect, useState, useCallback, useRef } from 'react'
import { fetchEvents, fetchEventSources, fetchEventStats, createEventWebSocket } from '@/lib/api'
import type { EventFilter, AdminEvent } from '@/lib/types'

export function useEvents(filter?: EventFilter) {
  return useQuery({
    queryKey: ['events', filter],
    queryFn: () => fetchEvents(filter),
  })
}

export function useEventSources() {
  return useQuery({
    queryKey: ['event-sources'],
    queryFn: fetchEventSources,
    staleTime: 30000, // Cache for 30 seconds
  })
}

export function useEventStats() {
  return useQuery({
    queryKey: ['event-stats'],
    queryFn: fetchEventStats,
    refetchInterval: 10000, // Refresh every 10 seconds
  })
}

const MAX_REALTIME_EVENTS = 200

export function useEventStream() {
  const [events, setEvents] = useState<AdminEvent[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const [isPaused, setIsPaused] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [missedCount, setMissedCount] = useState(0)
  const wsRef = useRef<WebSocket | null>(null)
  const isPausedRef = useRef(isPaused)

  // Keep ref in sync with state
  useEffect(() => {
    isPausedRef.current = isPaused
  }, [isPaused])

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return
    }

    wsRef.current = createEventWebSocket(
      (event) => {
        if (isPausedRef.current) {
          setMissedCount((prev) => prev + 1)
          return
        }
        setEvents((prev) => [event, ...prev].slice(0, MAX_REALTIME_EVENTS))
      },
      () => {
        setError('WebSocket connection error')
        setIsConnected(false)
      },
      () => {
        setIsConnected(false)
        // Auto-reconnect after 3 seconds
        setTimeout(connect, 3000)
      }
    )

    wsRef.current.onopen = () => {
      setIsConnected(true)
      setError(null)
    }
  }, [])

  const disconnect = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close()
      wsRef.current = null
    }
  }, [])

  const clearEvents = useCallback(() => {
    setEvents([])
  }, [])

  const pause = useCallback(() => {
    setIsPaused(true)
    setMissedCount(0)
  }, [])

  const resume = useCallback(() => {
    setIsPaused(false)
    setMissedCount(0)
  }, [])

  useEffect(() => {
    connect()
    return () => disconnect()
  }, [connect, disconnect])

  return {
    events,
    isConnected,
    isPaused,
    missedCount,
    error,
    connect,
    disconnect,
    clearEvents,
    pause,
    resume,
  }
}
