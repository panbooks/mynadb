import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft,
  Server,
  Activity,
  HardDrive,
  Trash2,
  RefreshCw,
  Play,
  Square,
  AlertTriangle,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { Skeleton } from '@/components/ui/skeleton'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Progress } from '@/components/ui/progress'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import {
  useNode,
  useNodeStats,
  useRetireNode,
  useCancelRetire,
  useFlushNode,
  useCompactNode,
  useClearNodeCache,
  useRetireStatus,
} from '@/hooks/use-nodes'
import { formatBytes, formatRelativeTime } from '@/lib/utils'
import { useState } from 'react'
import type { NodeState } from '@/lib/types'

export function NodeDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const nodeId = decodeURIComponent(id || '')

  const { data: node, isLoading, error } = useNode(nodeId)
  const { data: stats } = useNodeStats(nodeId)
  const isRetiring = node?.state === 'Retiring' || node?.state === 'Draining'
  const { data: retireStatus } = useRetireStatus(nodeId, isRetiring)

  const retireNode = useRetireNode()
  const cancelRetire = useCancelRetire()
  const flushNode = useFlushNode()
  const compactNode = useCompactNode()
  const clearCache = useClearNodeCache()

  const [showRetireDialog, setShowRetireDialog] = useState(false)

  if (isLoading) {
    return <NodeDetailSkeleton />
  }

  if (error || !node) {
    return (
      <Alert variant="destructive">
        <AlertTriangle className="h-4 w-4" />
        <AlertTitle>Error</AlertTitle>
        <AlertDescription>
          {error?.message || 'Node not found'}
        </AlertDescription>
      </Alert>
    )
  }

  const handleRetire = async () => {
    await retireNode.mutateAsync(nodeId)
    setShowRetireDialog(false)
  }

  const handleCancelRetire = async () => {
    await cancelRetire.mutateAsync(nodeId)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="icon" onClick={() => navigate('/nodes')}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex-1">
          <h1 className="text-2xl font-bold">{node.id}</h1>
          <p className="text-muted-foreground">{node.advertise_addr}</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline">{node.role}</Badge>
          <Badge variant={getStateVariant(node.state)}>{node.state}</Badge>
          {node.mode && (
            <Badge variant={node.mode === 'Primary' ? 'default' : 'secondary'}>
              {node.mode}
            </Badge>
          )}
        </div>
      </div>

      {/* Retirement Progress */}
      {isRetiring && retireStatus && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Activity className="h-4 w-4 animate-pulse text-amber-500" />
              Retirement in Progress
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span>{retireStatus.status}</span>
                <span>{retireStatus.progress}%</span>
              </div>
              <Progress value={retireStatus.progress} />
              <Button
                variant="outline"
                size="sm"
                onClick={handleCancelRetire}
                disabled={cancelRetire.isPending}
              >
                <Square className="mr-2 h-4 w-4" />
                Cancel Retirement
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Node Info */}
      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Server className="h-4 w-4" />
              Node Information
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm text-muted-foreground">Role</p>
                <p className="font-medium">{node.role}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">State</p>
                <p className="font-medium">{node.state}</p>
              </div>
              {node.mode && (
                <div>
                  <p className="text-sm text-muted-foreground">Mode</p>
                  <p className="font-medium">{node.mode}</p>
                </div>
              )}
              {node.zone && (
                <div>
                  <p className="text-sm text-muted-foreground">Zone</p>
                  <p className="font-medium">{node.zone}</p>
                </div>
              )}
              {node.tables_owned !== undefined && (
                <div>
                  <p className="text-sm text-muted-foreground">Tables Owned</p>
                  <p className="font-medium">{node.tables_owned}</p>
                </div>
              )}
              {node.paired_node_id && (
                <div>
                  <p className="text-sm text-muted-foreground">Paired Node</p>
                  <p className="font-medium truncate" title={node.paired_node_id}>
                    {node.paired_node_id}
                  </p>
                </div>
              )}
            </div>

            <Separator />

            <div>
              <p className="text-sm text-muted-foreground">Addresses</p>
              <div className="mt-2 space-y-1">
                <p className="text-sm">
                  <span className="text-muted-foreground">Advertise:</span>{' '}
                  <code className="bg-muted px-1 rounded">{node.advertise_addr}</code>
                </p>
                {node.http_addr && (
                  <p className="text-sm">
                    <span className="text-muted-foreground">HTTP:</span>{' '}
                    <code className="bg-muted px-1 rounded">{node.http_addr}</code>
                  </p>
                )}
                {node.flight_addr && (
                  <p className="text-sm">
                    <span className="text-muted-foreground">Flight:</span>{' '}
                    <code className="bg-muted px-1 rounded">{node.flight_addr}</code>
                  </p>
                )}
              </div>
            </div>

            {node.registered_at && (
              <>
                <Separator />
                <div>
                  <p className="text-sm text-muted-foreground">Registered</p>
                  <p className="font-medium">{formatRelativeTime(node.registered_at)}</p>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Health Status
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {node.health ? (
              <>
                <div className="flex items-center gap-2">
                  <div
                    className={`h-3 w-3 rounded-full ${
                      node.health.is_healthy ? 'bg-green-500' : 'bg-red-500'
                    }`}
                  />
                  <span className="font-medium">
                    {node.health.is_healthy ? 'Healthy' : 'Unhealthy'}
                  </span>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Last Heartbeat</p>
                  <p className="font-medium">
                    {formatRelativeTime(node.health.last_heartbeat)}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    ({node.health.heartbeat_age_ms}ms ago)
                  </p>
                </div>
                {node.health.message && (
                  <div>
                    <p className="text-sm text-muted-foreground">Message</p>
                    <p className="text-sm">{node.health.message}</p>
                  </div>
                )}
              </>
            ) : (
              <p className="text-muted-foreground">No health data available</p>
            )}

            {stats && (
              <>
                <Separator />
                <div className="grid grid-cols-2 gap-4">
                  {stats.memory_used_bytes !== undefined && stats.memory_total_bytes !== undefined && (
                    <div>
                      <p className="text-sm text-muted-foreground">Memory</p>
                      <p className="font-medium">
                        {formatBytes(stats.memory_used_bytes as number)} /{' '}
                        {formatBytes(stats.memory_total_bytes as number)}
                      </p>
                    </div>
                  )}
                  {stats.cache_hits !== undefined && (
                    <div>
                      <p className="text-sm text-muted-foreground">Cache Hit Rate</p>
                      <p className="font-medium">
                        {stats.cache_misses
                          ? (
                              ((stats.cache_hits as number) /
                                ((stats.cache_hits as number) + (stats.cache_misses as number))) *
                              100
                            ).toFixed(1)
                          : 100}
                        %
                      </p>
                    </div>
                  )}
                  {stats.active_queries !== undefined && (
                    <div>
                      <p className="text-sm text-muted-foreground">Active Queries</p>
                      <p className="font-medium">{stats.active_queries as number}</p>
                    </div>
                  )}
                  {stats.compaction_status !== undefined && (
                    <div>
                      <p className="text-sm text-muted-foreground">Compaction</p>
                      <p className="font-medium">{String(stats.compaction_status)}</p>
                    </div>
                  )}
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Actions */}
      {node.role === 'Data' && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Node Actions</CardTitle>
            <CardDescription>
              Perform operations on this data node
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                onClick={() => flushNode.mutate(nodeId)}
                disabled={flushNode.isPending}
              >
                <HardDrive className="mr-2 h-4 w-4" />
                Flush Memtable
              </Button>

              <Button
                variant="outline"
                onClick={() => compactNode.mutate(nodeId)}
                disabled={compactNode.isPending}
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Trigger Compaction
              </Button>

              <Button
                variant="outline"
                onClick={() => clearCache.mutate(nodeId)}
                disabled={clearCache.isPending}
              >
                <Trash2 className="mr-2 h-4 w-4" />
                Clear Cache
              </Button>

              {!isRetiring && node.state === 'Active' && (
                <Dialog open={showRetireDialog} onOpenChange={setShowRetireDialog}>
                  <DialogTrigger asChild>
                    <Button variant="destructive">
                      <Play className="mr-2 h-4 w-4" />
                      Start Retirement
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Retire Node</DialogTitle>
                      <DialogDescription>
                        This will start the retirement process for node {node.id}.
                        Tables will be migrated to other nodes and the memtable will be flushed.
                      </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                      <Button variant="outline" onClick={() => setShowRetireDialog(false)}>
                        Cancel
                      </Button>
                      <Button
                        variant="destructive"
                        onClick={handleRetire}
                        disabled={retireNode.isPending}
                      >
                        {retireNode.isPending ? 'Starting...' : 'Start Retirement'}
                      </Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Labels */}
      {node.labels && Object.keys(node.labels).length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Labels</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {Object.entries(node.labels).map(([key, value]) => (
                <Badge key={key} variant="outline">
                  {key}: {value}
                </Badge>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

function NodeDetailSkeleton() {
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Skeleton className="h-10 w-10" />
        <div className="flex-1">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-32 mt-2" />
        </div>
      </div>
      <div className="grid gap-6 md:grid-cols-2">
        {[...Array(2)].map((_, i) => (
          <Card key={i}>
            <CardHeader>
              <Skeleton className="h-6 w-32" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-32 w-full" />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}

function getStateVariant(state: NodeState): 'success' | 'destructive' | 'warning' | 'secondary' {
  switch (state) {
    case 'Active':
      return 'success'
    case 'Inactive':
    case 'Drained':
      return 'destructive'
    case 'Draining':
    case 'Retiring':
      return 'warning'
    default:
      return 'secondary'
  }
}
