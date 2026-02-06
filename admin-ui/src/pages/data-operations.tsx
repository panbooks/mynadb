import { useState } from 'react'
import {
  HardDrive,
  RefreshCw,
  Trash2,
  Play,
  Server,
  CheckCircle2,
  XCircle,
  Loader2,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useNodes, useFlushNode, useCompactNode, useClearNodeCache } from '@/hooks/use-nodes'
import { formatRelativeTime } from '@/lib/utils'
import type { NodeState } from '@/lib/types'

export function DataOperationsPage() {
  const { data: dataNodes, isLoading, refetch, isFetching } = useNodes({
    role: 'Data',
  })

  const flushNode = useFlushNode()
  const compactNode = useCompactNode()
  const clearCache = useClearNodeCache()

  const [operatingNode, setOperatingNode] = useState<string | null>(null)
  const [operation, setOperation] = useState<string | null>(null)

  const handleFlush = async (nodeId: string) => {
    setOperatingNode(nodeId)
    setOperation('flush')
    try {
      await flushNode.mutateAsync(nodeId)
    } finally {
      setOperatingNode(null)
      setOperation(null)
    }
  }

  const handleCompact = async (nodeId: string) => {
    setOperatingNode(nodeId)
    setOperation('compact')
    try {
      await compactNode.mutateAsync(nodeId)
    } finally {
      setOperatingNode(null)
      setOperation(null)
    }
  }

  const handleClearCache = async (nodeId: string) => {
    setOperatingNode(nodeId)
    setOperation('clear-cache')
    try {
      await clearCache.mutateAsync(nodeId)
    } finally {
      setOperatingNode(null)
      setOperation(null)
    }
  }

  const handleBulkFlush = async () => {
    const activeNodes = dataNodes?.filter((n) => n.state === 'Active') ?? []
    for (const node of activeNodes) {
      await handleFlush(node.id)
    }
  }

  const handleBulkCompact = async () => {
    const activeNodes = dataNodes?.filter((n) => n.state === 'Active') ?? []
    for (const node of activeNodes) {
      await handleCompact(node.id)
    }
  }

  const activeCount = dataNodes?.filter((n) => n.state === 'Active').length ?? 0
  const inactiveCount = dataNodes?.filter((n) => n.state === 'Inactive').length ?? 0
  const retiringCount = dataNodes?.filter((n) => n.state === 'Retiring').length ?? 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Data Operations</h1>
          <p className="text-muted-foreground">
            Manage data node operations
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => refetch()}
          disabled={isFetching}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {/* Summary Cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Active Nodes</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">{activeCount}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Retiring Nodes</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-amber-600">{retiringCount}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Inactive Nodes</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-600">{inactiveCount}</div>
          </CardContent>
        </Card>
      </div>

      {/* Bulk Operations */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Bulk Operations</CardTitle>
          <CardDescription>
            Perform operations on all active data nodes
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex gap-2">
            <Button
              variant="outline"
              onClick={handleBulkFlush}
              disabled={!!operatingNode || activeCount === 0}
            >
              <HardDrive className="mr-2 h-4 w-4" />
              Flush All Memtables
            </Button>
            <Button
              variant="outline"
              onClick={handleBulkCompact}
              disabled={!!operatingNode || activeCount === 0}
            >
              <RefreshCw className="mr-2 h-4 w-4" />
              Compact All Nodes
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Node List */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Server className="h-4 w-4" />
            Data Nodes
          </CardTitle>
          <CardDescription>
            Individual node operations and status
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">
              {[...Array(5)].map((_, i) => (
                <Skeleton key={i} className="h-16 w-full" />
              ))}
            </div>
          ) : dataNodes && dataNodes.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Node</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Mode</TableHead>
                  <TableHead>Tables</TableHead>
                  <TableHead>Last Heartbeat</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dataNodes.map((node) => {
                  const isOperating = operatingNode === node.id
                  const isActive = node.state === 'Active'

                  return (
                    <TableRow key={node.id}>
                      <TableCell>
                        <div>
                          <code className="text-sm">{node.id.slice(0, 12)}...</code>
                          <p className="text-xs text-muted-foreground">
                            {node.advertise_addr}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant={getStateVariant(node.state)}>
                          {node.state === 'Active' && (
                            <CheckCircle2 className="mr-1 h-3 w-3" />
                          )}
                          {node.state === 'Inactive' && (
                            <XCircle className="mr-1 h-3 w-3" />
                          )}
                          {node.state}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {node.mode && (
                          <Badge
                            variant={node.mode === 'Primary' ? 'default' : 'secondary'}
                          >
                            {node.mode}
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell>{node.tables_owned ?? '-'}</TableCell>
                      <TableCell>
                        {node.last_heartbeat
                          ? formatRelativeTime(node.last_heartbeat)
                          : '-'}
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleFlush(node.id)}
                            disabled={!isActive || isOperating}
                            title="Flush Memtable"
                          >
                            {isOperating && operation === 'flush' ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <HardDrive className="h-4 w-4" />
                            )}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleCompact(node.id)}
                            disabled={!isActive || isOperating}
                            title="Trigger Compaction"
                          >
                            {isOperating && operation === 'compact' ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <RefreshCw className="h-4 w-4" />
                            )}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => handleClearCache(node.id)}
                            disabled={!isActive || isOperating}
                            title="Clear Cache"
                          >
                            {isOperating && operation === 'clear-cache' ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <Trash2 className="h-4 w-4" />
                            )}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          ) : (
            <div className="flex flex-col items-center justify-center py-12">
              <Server className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-lg font-medium">No data nodes found</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Retiring Nodes Alert */}
      {retiringCount > 0 && (
        <Alert variant="warning">
          <Play className="h-4 w-4" />
          <AlertTitle>Nodes Retiring</AlertTitle>
          <AlertDescription>
            {retiringCount} node(s) are currently being retired. Tables are being
            migrated to other nodes.
          </AlertDescription>
        </Alert>
      )}
    </div>
  )
}

function getStateVariant(state: NodeState): 'success' | 'destructive' | 'warning' | 'secondary' {
  switch (state) {
    case 'Active':
      return 'success'
    case 'Inactive':
      return 'destructive'
    case 'Draining':
    case 'Retiring':
      return 'warning'
    default:
      return 'secondary'
  }
}
