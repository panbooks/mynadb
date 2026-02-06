import { useState } from 'react'
import {
  Activity,
  Wifi,
  WifiOff,
  Trash2,
  RefreshCw,
  Filter,
  ChevronRight,
  Server,
  Table2,
  Database,
  AlertTriangle,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useEventStream, useEvents } from '@/hooks/use-events'
import { formatRelativeTime } from '@/lib/utils'
import type { AdminEvent, EventType } from '@/lib/types'

const eventTypeIcons: Record<string, React.ComponentType<{ className?: string }>> = {
  NodeJoined: Server,
  NodeLeft: Server,
  NodeStateChanged: Server,
  LeaderChanged: Database,
  TableReassigned: Table2,
  TableOwnerAdded: Table2,
  TableOwnerRemoved: Table2,
  TableOwnerExpired: Table2,
  CacheInvalidate: Database,
  SchemaChanged: Table2,
  FlushForRecovery: Activity,
  DistributionConfigChanged: Database,
  TablesCacheRefresh: Database,
  Unknown: AlertTriangle,
}

export function EventsPage() {
  const [typeFilter, setTypeFilter] = useState<EventType | 'all'>('all')
  const [selectedEvent, setSelectedEvent] = useState<AdminEvent | null>(null)

  const { events: realtimeEvents, isConnected, clearEvents, connect } = useEventStream()
  const { data: historicalEvents, refetch, isFetching } = useEvents({
    type: typeFilter === 'all' ? undefined : typeFilter,
    limit: 50,
  })

  // Combine realtime and historical events, deduplicate by ID
  const allEvents = [...realtimeEvents, ...(historicalEvents ?? [])]
  const uniqueEvents = allEvents.filter(
    (event, index, self) => index === self.findIndex((e) => e.id === event.id)
  )

  const filteredEvents =
    typeFilter === 'all'
      ? uniqueEvents
      : uniqueEvents.filter((e) => e.type === typeFilter)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Events</h1>
          <p className="text-muted-foreground">
            Real-time cluster event stream
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* Connection Status */}
          <Badge variant={isConnected ? 'success' : 'destructive'}>
            {isConnected ? (
              <>
                <Wifi className="mr-1 h-3 w-3" />
                Connected
              </>
            ) : (
              <>
                <WifiOff className="mr-1 h-3 w-3" />
                Disconnected
              </>
            )}
          </Badge>

          {!isConnected && (
            <Button variant="outline" size="sm" onClick={connect}>
              <RefreshCw className="mr-2 h-4 w-4" />
              Reconnect
            </Button>
          )}

          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
          >
            <RefreshCw className={`mr-2 h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </Button>

          <Button variant="outline" size="sm" onClick={clearEvents}>
            <Trash2 className="mr-2 h-4 w-4" />
            Clear
          </Button>
        </div>
      </div>

      {/* Filters */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Filter className="h-4 w-4" />
            Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4">
            <div className="w-64">
              <Select
                value={typeFilter}
                onValueChange={(v) => setTypeFilter(v as EventType | 'all')}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Event Type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Events</SelectItem>
                  <SelectItem value="NodeJoined">Node Joined</SelectItem>
                  <SelectItem value="NodeLeft">Node Left</SelectItem>
                  <SelectItem value="NodeStateChanged">Node State Changed</SelectItem>
                  <SelectItem value="LeaderChanged">Leader Changed</SelectItem>
                  <SelectItem value="TableReassigned">Table Reassigned</SelectItem>
                  <SelectItem value="TableOwnerAdded">Table Owner Added</SelectItem>
                  <SelectItem value="TableOwnerRemoved">Table Owner Removed</SelectItem>
                  <SelectItem value="TableOwnerExpired">Table Owner Expired</SelectItem>
                  <SelectItem value="FlushForRecovery">Flush for Recovery</SelectItem>
                  <SelectItem value="CacheInvalidate">Cache Invalidate</SelectItem>
                  <SelectItem value="SchemaChanged">Schema Changed</SelectItem>
                  <SelectItem value="DistributionConfigChanged">Distribution Config Changed</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Events List */}
      <div className="grid gap-6 lg:grid-cols-2">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Event Stream
            </CardTitle>
            <CardDescription>
              {filteredEvents.length} event(s)
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0">
            <ScrollArea className="h-[500px]">
              {filteredEvents.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 px-6">
                  <Activity className="h-12 w-12 text-muted-foreground mb-4" />
                  <p className="text-lg font-medium">No events</p>
                  <p className="text-sm text-muted-foreground">
                    Events will appear here as they occur
                  </p>
                </div>
              ) : (
                <div className="divide-y">
                  {filteredEvents.map((event) => (
                    <EventRow
                      key={event.id}
                      event={event}
                      isSelected={selectedEvent?.id === event.id}
                      onClick={() => setSelectedEvent(event)}
                    />
                  ))}
                </div>
              )}
            </ScrollArea>
          </CardContent>
        </Card>

        {/* Event Details */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle className="text-base">Event Details</CardTitle>
          </CardHeader>
          <CardContent>
            {selectedEvent ? (
              <EventDetails event={selectedEvent} />
            ) : (
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <ChevronRight className="h-12 w-12 text-muted-foreground mb-4" />
                <p className="text-muted-foreground">
                  Select an event to view details
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

interface EventRowProps {
  event: AdminEvent
  isSelected: boolean
  onClick: () => void
}

function EventRow({ event, isSelected, onClick }: EventRowProps) {
  const Icon = eventTypeIcons[event.type] || Activity

  return (
    <div
      onClick={onClick}
      className={`flex items-center gap-3 p-4 cursor-pointer transition-colors ${
        isSelected ? 'bg-accent' : 'hover:bg-accent/50'
      }`}
    >
      <div
        className={`flex h-8 w-8 items-center justify-center rounded-full ${getEventTypeColor(
          event.type
        )}`}
      >
        <Icon className="h-4 w-4" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <p className="font-medium text-sm">{event.type}</p>
          <Badge variant="outline" className="text-xs">
            {formatRelativeTime(event.timestamp)}
          </Badge>
        </div>
        <p className="text-xs text-muted-foreground truncate">{event.message}</p>
      </div>
      <ChevronRight className="h-4 w-4 text-muted-foreground" />
    </div>
  )
}

interface EventDetailsProps {
  event: AdminEvent
}

function EventDetails({ event }: EventDetailsProps) {
  const Icon = eventTypeIcons[event.type] || Activity

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div
          className={`flex h-10 w-10 items-center justify-center rounded-full ${getEventTypeColor(
            event.type
          )}`}
        >
          <Icon className="h-5 w-5" />
        </div>
        <div>
          <h3 className="font-semibold">{event.type}</h3>
          <p className="text-sm text-muted-foreground">
            {new Date(event.timestamp).toLocaleString()}
          </p>
        </div>
      </div>

      {/* Message */}
      <div>
        <h4 className="text-sm font-medium text-muted-foreground mb-1">Message</h4>
        <p className="text-sm">{event.message}</p>
      </div>

      {/* Source */}
      {event.source && (
        <div>
          <h4 className="text-sm font-medium text-muted-foreground mb-1">Source</h4>
          <code className="text-sm bg-muted px-2 py-1 rounded">{event.source}</code>
        </div>
      )}

      {/* Payload */}
      {event.payload && Object.keys(event.payload).length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-muted-foreground mb-2">Payload</h4>
          <pre className="text-xs bg-muted p-3 rounded-lg overflow-auto max-h-48">
            {JSON.stringify(event.payload, null, 2)}
          </pre>
        </div>
      )}

      {/* Event ID */}
      <div>
        <h4 className="text-sm font-medium text-muted-foreground mb-1">Event ID</h4>
        <code className="text-xs bg-muted px-2 py-1 rounded">{event.id}</code>
      </div>
    </div>
  )
}

function getEventTypeColor(type: EventType): string {
  switch (type) {
    case 'NodeJoined':
      return 'bg-green-100 text-green-600 dark:bg-green-900 dark:text-green-400'
    case 'NodeLeft':
      return 'bg-red-100 text-red-600 dark:bg-red-900 dark:text-red-400'
    case 'NodeStateChanged':
      return 'bg-blue-100 text-blue-600 dark:bg-blue-900 dark:text-blue-400'
    case 'LeaderChanged':
      return 'bg-amber-100 text-amber-600 dark:bg-amber-900 dark:text-amber-400'
    case 'TableReassigned':
    case 'TableOwnerAdded':
    case 'TableOwnerRemoved':
      return 'bg-purple-100 text-purple-600 dark:bg-purple-900 dark:text-purple-400'
    case 'TableOwnerExpired':
      return 'bg-orange-100 text-orange-600 dark:bg-orange-900 dark:text-orange-400'
    case 'FlushForRecovery':
      return 'bg-yellow-100 text-yellow-600 dark:bg-yellow-900 dark:text-yellow-400'
    case 'CacheInvalidate':
    case 'TablesCacheRefresh':
      return 'bg-cyan-100 text-cyan-600 dark:bg-cyan-900 dark:text-cyan-400'
    case 'SchemaChanged':
    case 'DistributionConfigChanged':
      return 'bg-indigo-100 text-indigo-600 dark:bg-indigo-900 dark:text-indigo-400'
    case 'Unknown':
      return 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'
    default:
      return 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'
  }
}
