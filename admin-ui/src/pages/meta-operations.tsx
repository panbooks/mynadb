import { useState } from 'react'
import {
  Database,
  Crown,
  RefreshCw,
  Settings,
  Plus,
  CheckCircle2,
  XCircle,
  Vote,
  Play,
  LogOut,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useMetaStatus } from '@/hooks/use-cluster'
import { useElections, useCampaign, useResign } from '@/hooks/use-elections'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchDistributionConfigs, setDistributionConfig } from '@/lib/api'
import { formatRelativeTime } from '@/lib/utils'
import type { DistributionConfig, DistributionScope, PlacementStrategy } from '@/lib/types'

export function MetaOperationsPage() {
  const { data: metaStatus, isLoading, refetch, isFetching } = useMetaStatus()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Meta Operations</h1>
          <p className="text-muted-foreground">
            Manage meta cluster and distribution configuration
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

      {/* Meta Cluster Status */}
      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Database className="h-4 w-4" />
              Meta Cluster Status
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-3">
                <Skeleton className="h-6 w-32" />
                <Skeleton className="h-4 w-24" />
              </div>
            ) : metaStatus ? (
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <div
                    className={`h-3 w-3 rounded-full ${
                      metaStatus.is_healthy ? 'bg-green-500' : 'bg-red-500'
                    }`}
                  />
                  <span className="font-medium">
                    {metaStatus.is_healthy ? 'Healthy' : 'Unhealthy'}
                  </span>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Term</p>
                    <p className="text-2xl font-bold">{metaStatus.term}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Commit Index</p>
                    <p className="text-2xl font-bold">{metaStatus.commit_index}</p>
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground">No data available</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Crown className="h-4 w-4" />
              Current Leader
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-12 w-full" />
            ) : metaStatus?.leader_id ? (
              <div className="flex items-center gap-3 p-3 bg-muted rounded-lg">
                <Crown className="h-5 w-5 text-amber-500" />
                <div>
                  <code className="text-sm font-medium">{metaStatus.leader_id}</code>
                  <p className="text-xs text-muted-foreground">
                    Elected in term {metaStatus.term}
                  </p>
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground">No leader elected</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Peer List */}
      {metaStatus?.peers && metaStatus.peers.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Meta Peers</CardTitle>
            <CardDescription>
              All nodes in the meta cluster
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Address</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Health</TableHead>
                  <TableHead>Last Heartbeat</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {metaStatus.peers.map((peer) => (
                  <TableRow key={peer.id}>
                    <TableCell className="font-mono text-sm">{peer.id}</TableCell>
                    <TableCell>
                      <code className="text-sm bg-muted px-1 rounded">
                        {peer.address}
                      </code>
                    </TableCell>
                    <TableCell>
                      {peer.is_leader ? (
                        <Badge variant="default">
                          <Crown className="mr-1 h-3 w-3" />
                          Leader
                        </Badge>
                      ) : (
                        <Badge variant="secondary">Follower</Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      {peer.is_healthy ? (
                        <Badge variant="success">
                          <CheckCircle2 className="mr-1 h-3 w-3" />
                          Healthy
                        </Badge>
                      ) : (
                        <Badge variant="destructive">
                          <XCircle className="mr-1 h-3 w-3" />
                          Unhealthy
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      {peer.last_heartbeat
                        ? formatRelativeTime(peer.last_heartbeat)
                        : '-'}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Elections */}
      <ElectionsSection />

      {/* Distribution Config */}
      <DistributionConfigSection />
    </div>
  )
}

function ElectionsSection() {
  const { data: elections, isLoading } = useElections()
  const campaignMutation = useCampaign()
  const resignMutation = useResign()
  const [confirmAction, setConfirmAction] = useState<{ type: 'campaign' | 'resign'; election: string } | null>(null)

  const handleConfirm = () => {
    if (!confirmAction) return
    if (confirmAction.type === 'campaign') {
      campaignMutation.mutate(
        { election: confirmAction.election },
        { onSettled: () => setConfirmAction(null) }
      )
    } else {
      resignMutation.mutate(
        confirmAction.election,
        { onSettled: () => setConfirmAction(null) }
      )
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Vote className="h-4 w-4" />
          Leader Elections
        </CardTitle>
        <CardDescription>
          Background task leader elections (compactor, retention)
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[...Array(2)].map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : elections && elections.length > 0 ? (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Election</TableHead>
                  <TableHead>Leader</TableHead>
                  <TableHead>Term</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {elections.map((election) => (
                  <TableRow key={election.name}>
                    <TableCell className="font-medium capitalize">
                      {election.name}
                    </TableCell>
                    <TableCell>
                      {election.leader ? (
                        <code className="text-sm bg-muted px-1 rounded">
                          {election.leader}
                        </code>
                      ) : (
                        <span className="text-muted-foreground">None</span>
                      )}
                    </TableCell>
                    <TableCell>{election.term}</TableCell>
                    <TableCell>
                      {election.has_leader ? (
                        <Badge variant="success">
                          <CheckCircle2 className="mr-1 h-3 w-3" />
                          Active
                        </Badge>
                      ) : (
                        <Badge variant="secondary">
                          <XCircle className="mr-1 h-3 w-3" />
                          No Leader
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => setConfirmAction({ type: 'campaign', election: election.name })}
                        >
                          <Play className="mr-1 h-3 w-3" />
                          Campaign
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => setConfirmAction({ type: 'resign', election: election.name })}
                          disabled={!election.has_leader}
                        >
                          <LogOut className="mr-1 h-3 w-3" />
                          Resign
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {/* Confirmation Dialog */}
            <Dialog open={!!confirmAction} onOpenChange={(open) => !open && setConfirmAction(null)}>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>
                    {confirmAction?.type === 'campaign'
                      ? 'Campaign for Leadership'
                      : 'Resign Leadership'}
                  </DialogTitle>
                  <DialogDescription>
                    {confirmAction?.type === 'campaign'
                      ? `This will make the admin node campaign to become the leader for the "${confirmAction.election}" election. This is primarily useful for testing.`
                      : `This will force the current leader to resign from the "${confirmAction?.election}" election. A new leader will be elected automatically.`}
                  </DialogDescription>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setConfirmAction(null)}>
                    Cancel
                  </Button>
                  <Button
                    onClick={handleConfirm}
                    disabled={campaignMutation.isPending || resignMutation.isPending}
                  >
                    {(campaignMutation.isPending || resignMutation.isPending) ? 'Processing...' : 'Confirm'}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>

            {(campaignMutation.isError || resignMutation.isError) && (
              <p className="mt-2 text-sm text-red-600">
                {((campaignMutation.error || resignMutation.error) as Error)?.message}
              </p>
            )}
          </>
        ) : (
          <p className="text-muted-foreground text-center py-8">
            No elections found
          </p>
        )}
      </CardContent>
    </Card>
  )
}

function DistributionConfigSection() {
  const queryClient = useQueryClient()
  const [showAddDialog, setShowAddDialog] = useState(false)

  const { data: configs, isLoading } = useQuery({
    queryKey: ['distribution-configs'],
    queryFn: fetchDistributionConfigs,
  })

  const setConfig = useMutation({
    mutationFn: setDistributionConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['distribution-configs'] })
      setShowAddDialog(false)
    },
  })

  const [newConfig, setNewConfig] = useState<Partial<DistributionConfig>>({
    scope: 'Global',
    replication_factor: 2,
    placement_strategy: 'RoundRobin',
  })

  const handleSave = () => {
    if (newConfig.replication_factor && newConfig.placement_strategy) {
      setConfig.mutate(newConfig as DistributionConfig)
    }
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-base flex items-center gap-2">
            <Settings className="h-4 w-4" />
            Distribution Configuration
          </CardTitle>
          <CardDescription>
            Configure how tables are distributed across nodes
          </CardDescription>
        </div>
        <Dialog open={showAddDialog} onOpenChange={setShowAddDialog}>
          <DialogTrigger asChild>
            <Button size="sm">
              <Plus className="mr-2 h-4 w-4" />
              Add Config
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Add Distribution Config</DialogTitle>
              <DialogDescription>
                Configure distribution settings for a scope
              </DialogDescription>
            </DialogHeader>

            <div className="space-y-4 py-4">
              <div>
                <Label>Scope</Label>
                <Select
                  value={newConfig.scope}
                  onValueChange={(v) =>
                    setNewConfig({ ...newConfig, scope: v as DistributionScope })
                  }
                >
                  <SelectTrigger className="mt-1">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Global">Global</SelectItem>
                    <SelectItem value="Catalog">Catalog</SelectItem>
                    <SelectItem value="Schema">Schema</SelectItem>
                    <SelectItem value="Table">Table</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {newConfig.scope === 'Catalog' && (
                <div>
                  <Label>Catalog Name</Label>
                  <Input
                    value={newConfig.catalog || ''}
                    onChange={(e) =>
                      setNewConfig({ ...newConfig, catalog: e.target.value })
                    }
                    className="mt-1"
                  />
                </div>
              )}

              {(newConfig.scope === 'Schema' || newConfig.scope === 'Table') && (
                <>
                  <div>
                    <Label>Catalog Name</Label>
                    <Input
                      value={newConfig.catalog || ''}
                      onChange={(e) =>
                        setNewConfig({ ...newConfig, catalog: e.target.value })
                      }
                      className="mt-1"
                    />
                  </div>
                  <div>
                    <Label>Schema Name</Label>
                    <Input
                      value={newConfig.schema || ''}
                      onChange={(e) =>
                        setNewConfig({ ...newConfig, schema: e.target.value })
                      }
                      className="mt-1"
                    />
                  </div>
                </>
              )}

              {newConfig.scope === 'Table' && (
                <div>
                  <Label>Table Name</Label>
                  <Input
                    value={newConfig.table || ''}
                    onChange={(e) =>
                      setNewConfig({ ...newConfig, table: e.target.value })
                    }
                    className="mt-1"
                  />
                </div>
              )}

              <div>
                <Label>Replication Factor</Label>
                <Input
                  type="number"
                  min={1}
                  max={5}
                  value={newConfig.replication_factor}
                  onChange={(e) =>
                    setNewConfig({
                      ...newConfig,
                      replication_factor: parseInt(e.target.value) || 1,
                    })
                  }
                  className="mt-1"
                />
              </div>

              <div>
                <Label>Placement Strategy</Label>
                <Select
                  value={newConfig.placement_strategy}
                  onValueChange={(v) =>
                    setNewConfig({
                      ...newConfig,
                      placement_strategy: v as PlacementStrategy,
                    })
                  }
                >
                  <SelectTrigger className="mt-1">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="RoundRobin">Round Robin</SelectItem>
                    <SelectItem value="LeastLoaded">Least Loaded</SelectItem>
                    <SelectItem value="ZoneAware">Zone Aware</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <DialogFooter>
              <Button variant="outline" onClick={() => setShowAddDialog(false)}>
                Cancel
              </Button>
              <Button onClick={handleSave} disabled={setConfig.isPending}>
                {setConfig.isPending ? 'Saving...' : 'Save'}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[...Array(3)].map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : configs && configs.length > 0 ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Scope</TableHead>
                <TableHead>Target</TableHead>
                <TableHead>Replication Factor</TableHead>
                <TableHead>Strategy</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {configs.map((config, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <Badge variant="outline">{config.scope}</Badge>
                  </TableCell>
                  <TableCell className="font-mono text-sm">
                    {config.scope === 'Global'
                      ? '-'
                      : [config.catalog, config.schema, config.table]
                          .filter(Boolean)
                          .join('.')}
                  </TableCell>
                  <TableCell>{config.replication_factor}</TableCell>
                  <TableCell>{config.placement_strategy}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <p className="text-muted-foreground text-center py-8">
            No distribution configs found
          </p>
        )}
      </CardContent>
    </Card>
  )
}
