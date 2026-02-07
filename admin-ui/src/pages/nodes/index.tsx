import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Server, RefreshCw, Filter, ChevronRight } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { useNodes } from '@/hooks/use-nodes'
import { formatRelativeTime } from '@/lib/utils'
import type { NodeRole, NodeState, DataNodeMode, NodeSummary } from '@/lib/types'

export function NodesPage() {
  const [roleFilter, setRoleFilter] = useState<NodeRole | 'all'>('all')
  const [stateFilter, setStateFilter] = useState<NodeState | 'all'>('all')
  const [modeFilter, setModeFilter] = useState<DataNodeMode | 'all'>('all')
  const [searchQuery, setSearchQuery] = useState('')

  const { data: nodes, isLoading, refetch, isFetching } = useNodes({
    role: roleFilter === 'all' ? undefined : roleFilter,
    state: stateFilter === 'all' ? undefined : stateFilter,
    mode: modeFilter === 'all' ? undefined : modeFilter,
  })

  const filteredNodes = nodes?.filter(node =>
    searchQuery === '' ||
    node.id.toLowerCase().includes(searchQuery.toLowerCase()) ||
    node.advertise_addr.toLowerCase().includes(searchQuery.toLowerCase())
  )

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Nodes</h1>
          <p className="text-muted-foreground">
            Manage cluster nodes
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

      {/* Filters */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Filter className="h-4 w-4" />
            Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-4">
            <div>
              <Input
                placeholder="Search by ID or address..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            <div>
              <Select
                value={roleFilter}
                onValueChange={(v) => setRoleFilter(v as NodeRole | 'all')}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Role" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Roles</SelectItem>
                  <SelectItem value="Meta">Meta</SelectItem>
                  <SelectItem value="Router">Router</SelectItem>
                  <SelectItem value="Data">Data</SelectItem>
                  <SelectItem value="Compactor">Compactor</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Select
                value={stateFilter}
                onValueChange={(v) => setStateFilter(v as NodeState | 'all')}
              >
                <SelectTrigger>
                  <SelectValue placeholder="State" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All States</SelectItem>
                  <SelectItem value="Active">Active</SelectItem>
                  <SelectItem value="Inactive">Inactive</SelectItem>
                  <SelectItem value="Draining">Draining</SelectItem>
                  <SelectItem value="Retiring">Retiring</SelectItem>
                  <SelectItem value="Drained">Drained</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Select
                value={modeFilter}
                onValueChange={(v) => setModeFilter(v as DataNodeMode | 'all')}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Mode" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Modes</SelectItem>
                  <SelectItem value="Primary">Primary</SelectItem>
                  <SelectItem value="Standby">Standby</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Node List */}
      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {[...Array(6)].map((_, i) => (
            <Card key={i}>
              <CardContent className="pt-6">
                <Skeleton className="h-6 w-32 mb-2" />
                <Skeleton className="h-4 w-24 mb-4" />
                <Skeleton className="h-4 w-full" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        <>
          <div className="text-sm text-muted-foreground">
            Showing {filteredNodes?.length ?? 0} of {nodes?.length ?? 0} nodes
          </div>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {filteredNodes?.map((node) => (
              <NodeCard key={node.id} node={node} />
            ))}
          </div>
        </>
      )}

      {filteredNodes?.length === 0 && !isLoading && (
        <div className="flex flex-col items-center justify-center py-12">
          <Server className="h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-lg font-medium">No nodes found</p>
          <p className="text-sm text-muted-foreground">
            Try adjusting your filters
          </p>
        </div>
      )}
    </div>
  )
}

function NodeCard({ node }: { node: NodeSummary }) {
  return (
    <Link to={`/nodes/${encodeURIComponent(node.id)}`}>
      <Card className="cursor-pointer transition-colors hover:bg-accent/50">
        <CardContent className="pt-6">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
                <Server className="h-5 w-5 text-primary" />
              </div>
              <div>
                <p className="font-medium truncate max-w-[180px]" title={node.id}>
                  {node.id}
                </p>
                <p className="text-sm text-muted-foreground">
                  {node.advertise_addr}
                </p>
              </div>
            </div>
            <ChevronRight className="h-5 w-5 text-muted-foreground" />
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            <Badge variant="outline">{node.role}</Badge>
            <Badge variant={getStateVariant(node.state)}>{node.state}</Badge>
            {node.mode && (
              <Badge variant={node.mode === 'Primary' ? 'default' : 'secondary'}>
                {node.mode}
              </Badge>
            )}
          </div>

          <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
            {node.zone && (
              <div>
                <p className="text-muted-foreground">Zone</p>
                <p className="font-medium">{node.zone}</p>
              </div>
            )}
            {node.tables_owned !== undefined && (
              <div>
                <p className="text-muted-foreground">Tables</p>
                <p className="font-medium">{node.tables_owned}</p>
              </div>
            )}
            {node.last_heartbeat && (
              <div className="col-span-2">
                <p className="text-muted-foreground">Last Heartbeat</p>
                <p className="font-medium">{formatRelativeTime(node.last_heartbeat)}</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </Link>
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
