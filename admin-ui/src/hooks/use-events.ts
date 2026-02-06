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

// Critical event types that trigger notifications
const CRITICAL_EVENT_TYPES = new Set([
  'NodeLeft',
  'FlushForRecovery',
  'TableOwnerExpired',
])

// Request browser notification permission
export function requestNotificationPermission(): Promise<NotificationPermission> {
  if (!('Notification' in window)) {
    return Promise.resolve('denied' as NotificationPermission)
  }
  return Notification.requestPermission()
}

// Show browser notification
function showNotification(event: AdminEvent) {
  if (!('Notification' in window) || Notification.permission !== 'granted') {
    return
  }

  const notification = new Notification(`MynaDB: ${event.type}`, {
    body: event.message || 'New event occurred',
    icon: '/favicon.ico',
    tag: event.id, // Prevent duplicate notifications
  })

  // Auto-close after 5 seconds
  setTimeout(() => notification.close(), 5000)
}

export function useEventStream(options?: { enableNotifications?: boolean }) {
  const [events, setEvents] = useState<AdminEvent[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const [isPaused, setIsPaused] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [missedCount, setMissedCount] = useState(0)
  const [notificationsEnabled, setNotificationsEnabled] = useState(
    options?.enableNotifications ?? false
  )
  const wsRef = useRef<WebSocket | null>(null)
  const isPausedRef = useRef(isPaused)
  const notificationsEnabledRef = useRef(notificationsEnabled)

  // Keep refs in sync with state
  useEffect(() => {
    isPausedRef.current = isPaused
  }, [isPaused])

  useEffect(() => {
    notificationsEnabledRef.current = notificationsEnabled
  }, [notificationsEnabled])

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return
    }

    wsRef.current = createEventWebSocket(
      (event) => {
        // Show notification for critical events
        if (notificationsEnabledRef.current && CRITICAL_EVENT_TYPES.has(event.type)) {
          showNotification(event)
        }

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

  const toggleNotifications = useCallback(async () => {
    if (!notificationsEnabled) {
      const permission = await requestNotificationPermission()
      if (permission === 'granted') {
        setNotificationsEnabled(true)
      }
    } else {
      setNotificationsEnabled(false)
    }
  }, [notificationsEnabled])

  return {
    events,
    isConnected,
    isPaused,
    missedCount,
    notificationsEnabled,
    error,
    connect,
    disconnect,
    clearEvents,
    pause,
    resume,
    toggleNotifications,
  }
}
