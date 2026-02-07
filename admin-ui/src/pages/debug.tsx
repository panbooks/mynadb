import { useState } from 'react'
import {
  Bug,
  Cpu,
  MemoryStick,
  Trash2,
  Flame,
  RefreshCw,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
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
import { usePrewarmCache, useClearCache } from '@/hooks/use-debug'
import { useCatalogs } from '@/hooks/use-query'

export function DebugPage() {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold">Debug Tools</h1>
        <p className="text-muted-foreground">
          Profiling and cache management tools
        </p>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <ProfilingCard />
        <CacheManagementCard />
      </div>
    </div>
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
