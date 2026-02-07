import {
  Network,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Activity,
  Trash2,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useNodes } from '@/hooks/use-nodes'
import { formatRelativeTime } from '@/lib/utils'
import type { NodeState } from '@/lib/types'

export function RouterOperationsPage() {
  const { data: routerNodes, isLoading, refetch, isFetching } = useNodes({
    role: 'Router',
  })

  const activeCount = routerNodes?.filter((n) => n.state === 'Active').length ?? 0
  const inactiveCount = routerNodes?.filter((n) => n.state === 'Inactive').length ?? 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Router Operations</h1>
          <p className="text-muted-foreground">
            Manage router node status and operations
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
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Active Routers</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <div className="text-2xl font-bold text-green-600">{activeCount}</div>
              <CheckCircle2 className="h-5 w-5 text-green-600" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Inactive Routers</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <div className="text-2xl font-bold text-red-600">{inactiveCount}</div>
              {inactiveCount > 0 && <XCircle className="h-5 w-5 text-red-600" />}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Router Functions Overview */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Activity className="h-4 w-4" />
            Router Functions
          </CardTitle>
          <CardDescription>
            Router nodes handle query routing and connection management
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-3">
            <div className="border rounded-lg p-4">
              <h4 className="font-medium mb-2">Query Routing</h4>
              <p className="text-sm text-muted-foreground">
                Routes SQL queries to appropriate data nodes based on table ownership
              </p>
            </div>
            <div className="border rounded-lg p-4">
              <h4 className="font-medium mb-2">Connection Pooling</h4>
              <p className="text-sm text-muted-foreground">
                Maintains connection pools to data nodes for efficient query execution
              </p>
            </div>
            <div className="border rounded-lg p-4">
              <h4 className="font-medium mb-2">Cache Management</h4>
              <p className="text-sm text-muted-foreground">
                Caches metadata and routing information for fast lookups
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Router List */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Network className="h-4 w-4" />
            Router Nodes
          </CardTitle>
          <CardDescription>
            Status of all router nodes in the cluster
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">
              {[...Array(3)].map((_, i) => (
                <Skeleton key={i} className="h-16 w-full" />
              ))}
            </div>
          ) : routerNodes && routerNodes.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Node</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Zone</TableHead>
                  <TableHead>Last Heartbeat</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {routerNodes.map((node) => (
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
                    <TableCell>{node.zone ?? '-'}</TableCell>
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
                          disabled={node.state !== 'Active'}
                          title="Invalidate Cache"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="flex flex-col items-center justify-center py-12">
              <Network className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-lg font-medium">No router nodes found</p>
              <p className="text-sm text-muted-foreground">
                Router nodes have not been registered yet
              </p>
            </div>
          )}
        </CardContent>
      </Card>
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
