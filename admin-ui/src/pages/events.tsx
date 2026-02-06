import { useState, useMemo, useRef, useEffect } from 'react'
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
  Search,
  Clock,
  BarChart3,
  Pause,
  Play,
  Download,
  ArrowDownToLine,
  X,
  Bell,
  BellOff,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useEventStream, useEvents, useEventSources, useEventStats } from '@/hooks/use-events'
import { formatRelativeTime } from '@/lib/utils'
import type { AdminEvent, EventType } from '@/lib/types'

// Helper to download file
function downloadFile(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

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

const TIME_RANGES = [
  { value: 'all', label: 'All Time' },
  { value: '1h', label: 'Last Hour' },
  { value: '24h', label: 'Last 24 Hours' },
  { value: '7d', label: 'Last 7 Days' },
]

const EVENT_TYPES = [
  { value: 'NodeJoined', label: 'Node Joined' },
  { value: 'NodeLeft', label: 'Node Left' },
  { value: 'NodeStateChanged', label: 'Node State Changed' },
  { value: 'LeaderChanged', label: 'Leader Changed' },
  { value: 'TableReassigned', label: 'Table Reassigned' },
  { value: 'TableOwnerAdded', label: 'Table Owner Added' },
  { value: 'TableOwnerRemoved', label: 'Table Owner Removed' },
  { value: 'TableOwnerExpired', label: 'Table Owner Expired' },
  { value: 'FlushForRecovery', label: 'Flush for Recovery' },
  { value: 'CacheInvalidate', label: 'Cache Invalidate' },
  { value: 'SchemaChanged', label: 'Schema Changed' },
  { value: 'DistributionConfigChanged', label: 'Distribution Config' },
]

export function EventsPage() {
  const [typeFilter, setTypeFilter] = useState<string>('all')
  const [timeRange, setTimeRange] = useState<string>('all')
  const [sourceFilter, setSourceFilter] = useState<string>('all')
  const [searchQuery, setSearchQuery] = useState<string>('')
  const [selectedEvent, setSelectedEvent] = useState<AdminEvent | null>(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const scrollRef = useRef<HTMLDivElement>(null)

  const {
    events: realtimeEvents,
    isConnected,
    isPaused,
    missedCount,
    notificationsEnabled,
    clearEvents,
    connect,
    pause,
    resume,
    toggleNotifications,
  } = useEventStream()
  const { data: sources } = useEventSources()
  const { data: stats } = useEventStats()

  // Check if any filters are active
  const hasActiveFilters = typeFilter !== 'all' || timeRange !== 'all' ||
    sourceFilter !== 'all' || searchQuery !== ''

  // Clear all filters
  const clearFilters = () => {
    setTypeFilter('all')
    setTimeRange('all')
    setSourceFilter('all')
    setSearchQuery('')
  }

  // Export events to JSON
  const exportToJson = () => {
    const data = JSON.stringify(filteredEvents, null, 2)
    downloadFile(data, 'events.json', 'application/json')
  }

  // Export events to CSV
  const exportToCsv = () => {
    const headers = ['ID', 'Type', 'Timestamp', 'Source', 'Message']
    const rows = filteredEvents.map(e => [
      e.id,
      e.type,
      e.timestamp,
      e.source || '',
      `"${(e.message || '').replace(/"/g, '""')}"`,
    ])
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n')
    downloadFile(csv, 'events.csv', 'text/csv')
  }

  // Auto-scroll to top when new events arrive
  useEffect(() => {
    if (autoScroll && scrollRef.current && realtimeEvents.length > 0) {
      scrollRef.current.scrollTop = 0
    }
  }, [realtimeEvents, autoScroll])

  // Build filter for API
  const apiFilter = useMemo(() => ({
    type: typeFilter !== 'all' ? typeFilter : undefined,
    since: timeRange !== 'all' ? timeRange : undefined,
    source: sourceFilter !== 'all' ? sourceFilter : undefined,
    search: searchQuery || undefined,
    limit: 100,
  }), [typeFilter, timeRange, sourceFilter, searchQuery])

  const { data: historicalEvents, refetch, isFetching } = useEvents(apiFilter)

  // Combine realtime and historical events, deduplicate by ID
  const allEvents = useMemo(() => {
    const combined = [...realtimeEvents, ...(historicalEvents ?? [])]
    const seen = new Set<string>()
    return combined.filter((event) => {
      if (seen.has(event.id)) return false
      seen.add(event.id)
      return true
    })
  }, [realtimeEvents, historicalEvents])

  // Apply client-side filters for real-time events
  const filteredEvents = useMemo(() => {
    return allEvents.filter((e) => {
      if (typeFilter !== 'all' && e.type !== typeFilter) return false
      if (sourceFilter !== 'all' && e.source !== sourceFilter) return false
      if (searchQuery) {
        const query = searchQuery.toLowerCase()
        const matchesMessage = e.message?.toLowerCase().includes(query)
        const matchesType = e.type?.toLowerCase().includes(query)
        const matchesPayload = JSON.stringify(e.payload).toLowerCase().includes(query)
        if (!matchesMessage && !matchesType && !matchesPayload) return false
      }
      return true
    })
  }, [allEvents, typeFilter, sourceFilter, searchQuery])

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

          {/* Paused indicator */}
          {isPaused && (
            <Badge variant="warning">
              <Pause className="mr-1 h-3 w-3" />
              Paused {missedCount > 0 && `(${missedCount} missed)`}
            </Badge>
          )}

          {!isConnected && (
            <Button variant="outline" size="sm" onClick={connect}>
              <RefreshCw className="mr-2 h-4 w-4" />
              Reconnect
            </Button>
          )}

          {/* Pause/Resume button */}
          {isConnected && (
            <Button
              variant={isPaused ? 'default' : 'outline'}
              size="sm"
              onClick={isPaused ? resume : pause}
            >
              {isPaused ? (
                <>
                  <Play className="mr-2 h-4 w-4" />
                  Resume
                </>
              ) : (
                <>
                  <Pause className="mr-2 h-4 w-4" />
                  Pause
                </>
              )}
            </Button>
          )}

          {/* Auto-scroll toggle */}
          <Button
            variant={autoScroll ? 'default' : 'outline'}
            size="sm"
            onClick={() => setAutoScroll(!autoScroll)}
            title={autoScroll ? 'Auto-scroll enabled' : 'Auto-scroll disabled'}
          >
            <ArrowDownToLine className="h-4 w-4" />
          </Button>

          {/* Notifications toggle */}
          <Button
            variant={notificationsEnabled ? 'default' : 'outline'}
            size="sm"
            onClick={toggleNotifications}
            title={notificationsEnabled ? 'Notifications enabled' : 'Enable notifications'}
          >
            {notificationsEnabled ? (
              <Bell className="h-4 w-4" />
            ) : (
              <BellOff className="h-4 w-4" />
            )}
          </Button>

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

          {/* Export dropdown */}
          <Select onValueChange={(v) => v === 'json' ? exportToJson() : exportToCsv()}>
            <SelectTrigger className="w-[110px]">
              <Download className="mr-2 h-4 w-4" />
              Export
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="json">Export JSON</SelectItem>
              <SelectItem value="csv">Export CSV</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Events</CardTitle>
              <BarChart3 className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.total}</div>
              <p className="text-xs text-muted-foreground">in cache</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Last Hour</CardTitle>
              <Clock className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.events_last_hour}</div>
              <p className="text-xs text-muted-foreground">events</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Sources</CardTitle>
              <Server className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{sources?.length ?? 0}</div>
              <p className="text-xs text-muted-foreground">nodes</p>
            </CardContent>
          </Card>
          {/* Event Type Breakdown */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">By Type</CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent className="pt-0">
              <div className="space-y-1 max-h-24 overflow-y-auto">
                {Object.entries(stats.by_type)
                  .sort(([, a], [, b]) => b - a)
                  .slice(0, 5)
                  .map(([type, count]) => (
                    <div key={type} className="flex items-center justify-between text-xs">
                      <span className="truncate text-muted-foreground">{type}</span>
                      <span className="font-medium">{count}</span>
                    </div>
                  ))}
                {Object.keys(stats.by_type).length > 5 && (
                  <p className="text-xs text-muted-foreground">
                    +{Object.keys(stats.by_type).length - 5} more
                  </p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Filters */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Filter className="h-4 w-4" />
            Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-4">
            {/* Event Type Filter */}
            <div className="w-48">
              <label className="text-xs text-muted-foreground mb-1 block">Event Type</label>
              <Select value={typeFilter} onValueChange={setTypeFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="All Types" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Types</SelectItem>
                  {EVENT_TYPES.map((type) => (
                    <SelectItem key={type.value} value={type.value}>
                      {type.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Time Range Filter */}
            <div className="w-40">
              <label className="text-xs text-muted-foreground mb-1 block">Time Range</label>
              <Select value={timeRange} onValueChange={setTimeRange}>
                <SelectTrigger>
                  <SelectValue placeholder="All Time" />
                </SelectTrigger>
                <SelectContent>
                  {TIME_RANGES.map((range) => (
                    <SelectItem key={range.value} value={range.value}>
                      {range.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Source Node Filter */}
            <div className="w-48">
              <label className="text-xs text-muted-foreground mb-1 block">Source Node</label>
              <Select value={sourceFilter} onValueChange={setSourceFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="All Sources" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Sources</SelectItem>
                  {sources?.map((source) => (
                    <SelectItem key={source} value={source}>
                      {source}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Search Input */}
            <div className="flex-1 min-w-[200px]">
              <label className="text-xs text-muted-foreground mb-1 block">Search</label>
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search messages, payloads..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-8"
                />
              </div>
            </div>

            {/* Clear Filters */}
            {hasActiveFilters && (
              <div className="flex items-end">
                <Button variant="ghost" size="sm" onClick={clearFilters}>
                  <X className="mr-2 h-4 w-4" />
                  Clear Filters
                </Button>
              </div>
            )}
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
              {typeFilter !== 'all' && ` (filtered by ${typeFilter})`}
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0">
            <ScrollArea className="h-[500px]" ref={scrollRef}>
              {filteredEvents.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 px-6">
                  <Activity className="h-12 w-12 text-muted-foreground mb-4" />
                  <p className="text-lg font-medium">No events</p>
                  <p className="text-sm text-muted-foreground">
                    {searchQuery || typeFilter !== 'all' || sourceFilter !== 'all'
                      ? 'Try adjusting your filters'
                      : 'Events will appear here as they occur'}
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
