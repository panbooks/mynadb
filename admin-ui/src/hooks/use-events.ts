import { useQuery } from '@tanstack/react-query'
import { useEffect, useState, useCallback, useRef } from 'react'
import { fetchEvents, createEventWebSocket } from '@/lib/api'
import type { EventFilter, AdminEvent } from '@/lib/types'

export function useEvents(filter?: EventFilter) {
  return useQuery({
    queryKey: ['events', filter],
    queryFn: () => fetchEvents(filter),
  })
}

export function useEventStream() {
  const [events, setEvents] = useState<AdminEvent[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const wsRef = useRef<WebSocket | null>(null)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return
    }

    wsRef.current = createEventWebSocket(
      (event) => {
        setEvents((prev) => [event, ...prev].slice(0, 100)) // Keep last 100 events
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

  useEffect(() => {
    connect()
    return () => disconnect()
  }, [connect, disconnect])

  return {
    events,
    isConnected,
    error,
    connect,
    disconnect,
    clearEvents,
  }
}
