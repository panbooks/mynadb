import { useState } from 'react'
import {
  Bug,
  Cpu,
  MemoryStick,
  Trash2,
  Flame,
  RefreshCw,
  Database,
  CheckCircle2,
  Loader2,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { getHeapProfileUrl, getCpuProfileUrl } from '@/lib/api'
import { usePrewarmCache, useClearCache, useGenerateTestData } from '@/hooks/use-debug'
import { useCatalogs } from '@/hooks/use-query'

export function DebugPage() {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold">Debug Tools</h1>
        <p className="text-muted-foreground">
          Profiling, cache management, and test data generation
        </p>
      </div>

      <GenerateTestDataCard />

      <div className="grid gap-6 md:grid-cols-2">
        <ProfilingCard />
        <CacheManagementCard />
      </div>
    </div>
  )
}

// ============ Data type definitions ============

const DATA_TYPES = [
  {
    id: 'cpu',
    label: 'CPU Metrics',
    description: 'CPU usage, load averages across 5 hosts',
    sources: 5,
  },
  {
    id: 'memory',
    label: 'Memory Metrics',
    description: 'Memory & swap usage across 5 hosts',
    sources: 5,
  },
  {
    id: 'disk',
    label: 'Disk I/O',
    description: 'Disk throughput, IOPS across 5 hosts x 2 devices',
    sources: 10,
  },
  {
    id: 'http',
    label: 'HTTP Requests',
    description: 'Request latency, status codes from 3 services',
    sources: 3,
  },
  {
    id: 'iot',
    label: 'IoT Sensors',
    description: 'Temperature, humidity, pressure from 8 sensors',
    sources: 8,
  },
  {
    id: 'app',
    label: 'App Events',
    description: 'Response times, errors, active users from 3 services',
    sources: 3,
  },
] as const

const POINTS_OPTIONS = [
  { value: '10', label: '10 pts' },
  { value: '50', label: '50 pts' },
  { value: '100', label: '100 pts' },
  { value: '500', label: '500 pts' },
  { value: '1000', label: '1,000 pts' },
]

function GenerateTestDataCard() {
  const [selectedTypes, setSelectedTypes] = useState<string[]>(['cpu', 'http'])
  const [pointsPerSource, setPointsPerSource] = useState('100')
  const [date, setDate] = useState(() => new Date().toISOString().split('T')[0])
  const [database, setDatabase] = useState('testdb')
  const mutation = useGenerateTestData()

  const allSelected = selectedTypes.length === DATA_TYPES.length
  const noneSelected = selectedTypes.length === 0

  const toggleType = (typeId: string) => {
    setSelectedTypes((prev) =>
      prev.includes(typeId) ? prev.filter((t) => t !== typeId) : [...prev, typeId]
    )
  }

  const toggleAll = () => {
    if (allSelected) {
      setSelectedTypes([])
    } else {
      setSelectedTypes(DATA_TYPES.map((t) => t.id))
    }
  }

  const totalLines = selectedTypes.reduce((sum, typeId) => {
    const dt = DATA_TYPES.find((t) => t.id === typeId)
    return sum + (dt ? dt.sources * parseInt(pointsPerSource) : 0)
  }, 0)

  const handleGenerate = () => {
    mutation.mutate({
      types: selectedTypes,
      points_per_source: parseInt(pointsPerSource),
      date,
      database,
    })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Database className="h-4 w-4" />
          Generate Test Data
        </CardTitle>
        <CardDescription>
          Write realistic time-series data to test and explore the database
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        {/* Data type checkboxes */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <Label>Data Types</Label>
            <button
              onClick={toggleAll}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {allSelected ? 'Deselect All' : 'Select All'}
            </button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {DATA_TYPES.map((dt) => (
              <label
                key={dt.id}
                className={`flex items-start gap-3 rounded-lg border p-3 cursor-pointer transition-colors ${
                  selectedTypes.includes(dt.id)
                    ? 'border-primary bg-primary/5'
                    : 'border-border hover:bg-accent/50'
                }`}
              >
                <Checkbox
                  checked={selectedTypes.includes(dt.id)}
                  onCheckedChange={() => toggleType(dt.id)}
                  className="mt-0.5"
                />
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium">{dt.label}</div>
                  <div className="text-xs text-muted-foreground">{dt.description}</div>
                </div>
              </label>
            ))}
          </div>
        </div>

        {/* Settings row */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div>
            <Label>Database</Label>
            <Input
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              placeholder="testdb"
              className="mt-1"
            />
          </div>
          <div>
            <Label>Date</Label>
            <Input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              className="mt-1"
            />
          </div>
          <div>
            <Label>Points per Source</Label>
            <Select value={pointsPerSource} onValueChange={setPointsPerSource}>
              <SelectTrigger className="mt-1">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {POINTS_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Generate button + estimate */}
        <div className="flex items-center gap-4">
          <Button
            onClick={handleGenerate}
            disabled={noneSelected || mutation.isPending || !database}
          >
            {mutation.isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Database className="mr-2 h-4 w-4" />
            )}
            {mutation.isPending ? 'Generating...' : 'Generate Data'}
          </Button>
          <span className="text-sm text-muted-foreground">
            {totalLines.toLocaleString()} lines to <code className="text-xs bg-muted px-1 py-0.5 rounded">{database}</code>
          </span>
        </div>

        {/* Result */}
        {mutation.isSuccess && (
          <div className="rounded-lg border border-green-200 bg-green-50 dark:border-green-900 dark:bg-green-950 p-4">
            <div className="flex items-center gap-2 mb-2">
              <CheckCircle2 className="h-4 w-4 text-green-600" />
              <span className="font-medium text-green-800 dark:text-green-200">
                {mutation.data.total_points.toLocaleString()} points written to {mutation.data.database}
              </span>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm text-green-700 dark:text-green-300">
              {mutation.data.results.map((r) => (
                <span key={r.type}>
                  {r.type}: {r.points_written.toLocaleString()}
                </span>
              ))}
            </div>
            <p className="mt-2 text-xs text-green-600 dark:text-green-400">
              Date: {mutation.data.date} — Try querying in the{' '}
              <a href="/admin/query" className="underline">
                Query Console
              </a>
            </p>
          </div>
        )}

        {mutation.isError && (
          <div className="rounded-lg border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950 p-3">
            <p className="text-sm text-red-700 dark:text-red-300">
              {(mutation.error as Error).message}
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function ProfilingCard() {
  const [duration, setDuration] = useState(10)
  const [format, setFormat] = useState('flamegraph')

  const handleHeapProfile = () => {
    const url = getHeapProfileUrl(duration, format)
    window.open(url, '_blank')
  }

  const handleCpuProfile = () => {
    const url = getCpuProfileUrl(duration, format)
    window.open(url, '_blank')
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Bug className="h-4 w-4" />
          Profiling
        </CardTitle>
        <CardDescription>
          Generate heap and CPU profiles from a data node
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Label>Duration (seconds)</Label>
            <Input
              type="number"
              min={1}
              max={300}
              value={duration}
              onChange={(e) => setDuration(parseInt(e.target.value) || 10)}
              className="mt-1"
            />
          </div>
          <div>
            <Label>Format</Label>
            <Select value={format} onValueChange={setFormat}>
              <SelectTrigger className="mt-1">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="flamegraph">Flamegraph</SelectItem>
                <SelectItem value="protobuf">Protobuf</SelectItem>
                <SelectItem value="text">Text</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="flex gap-3">
          <Button onClick={handleHeapProfile} variant="outline" className="flex-1">
            <MemoryStick className="mr-2 h-4 w-4" />
            Heap Profile
          </Button>
          <Button onClick={handleCpuProfile} variant="outline" className="flex-1">
            <Cpu className="mr-2 h-4 w-4" />
            CPU Profile
          </Button>
        </div>

        <p className="text-xs text-muted-foreground">
          Profile data will be downloaded from the first available data node.
          CPU profiling runs for the specified duration.
        </p>
      </CardContent>
    </Card>
  )
}

function CacheManagementCard() {
  const { data: catalogs } = useCatalogs()
  const [selectedCatalog, setSelectedCatalog] = useState('')
  const [showClearDialog, setShowClearDialog] = useState(false)
  const prewarmMutation = usePrewarmCache()
  const clearMutation = useClearCache()

  const handlePrewarm = () => {
    if (selectedCatalog) {
      prewarmMutation.mutate(selectedCatalog)
    }
  }

  const handleClear = () => {
    clearMutation.mutate(undefined, {
      onSuccess: () => setShowClearDialog(false),
    })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Flame className="h-4 w-4" />
          Cache Management
        </CardTitle>
        <CardDescription>
          Prewarm or clear data node caches
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Prewarm */}
        <div className="space-y-2">
          <Label>Prewarm Cache</Label>
          <div className="flex gap-2">
            <Select value={selectedCatalog} onValueChange={setSelectedCatalog}>
              <SelectTrigger className="flex-1">
                <SelectValue placeholder="Select catalog" />
              </SelectTrigger>
              <SelectContent>
                {catalogs?.map((catalog) => (
                  <SelectItem key={catalog} value={catalog}>
                    {catalog}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              onClick={handlePrewarm}
              disabled={!selectedCatalog || prewarmMutation.isPending}
            >
              <RefreshCw className={`mr-2 h-4 w-4 ${prewarmMutation.isPending ? 'animate-spin' : ''}`} />
              Prewarm
            </Button>
          </div>
          {prewarmMutation.isSuccess && (
            <p className="text-sm text-green-600">{prewarmMutation.data.message}</p>
          )}
          {prewarmMutation.isError && (
            <p className="text-sm text-red-600">{(prewarmMutation.error as Error).message}</p>
          )}
        </div>

        {/* Clear */}
        <div className="space-y-2">
          <Label>Clear All Caches</Label>
          <Dialog open={showClearDialog} onOpenChange={setShowClearDialog}>
            <DialogTrigger asChild>
              <Button variant="destructive" className="w-full">
                <Trash2 className="mr-2 h-4 w-4" />
                Clear All Caches
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Clear all caches?</DialogTitle>
                <DialogDescription>
                  This will clear all metadata and query caches on the data node.
                  Subsequent queries may be slower until caches are rebuilt.
                </DialogDescription>
              </DialogHeader>
              <DialogFooter>
                <Button variant="outline" onClick={() => setShowClearDialog(false)}>
                  Cancel
                </Button>
                <Button
                  variant="destructive"
                  onClick={handleClear}
                  disabled={clearMutation.isPending}
                >
                  {clearMutation.isPending ? 'Clearing...' : 'Clear'}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
          {clearMutation.isSuccess && (
            <p className="text-sm text-green-600">{clearMutation.data.message}</p>
          )}
          {clearMutation.isError && (
            <p className="text-sm text-red-600">{(clearMutation.error as Error).message}</p>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
