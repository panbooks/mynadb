import { Link } from 'react-router-dom'
import {
  Server,
  Table2,
  ArrowDownToLine,
  AlertTriangle,
  Database,
  Activity,
  CheckCircle2,
  XCircle,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Skeleton } from '@/components/ui/skeleton'
import { useClusterStatus } from '@/hooks/use-cluster'
import { formatNumber } from '@/lib/utils'

export function Dashboard() {
  const { data: status, isLoading, error } = useClusterStatus()

  if (isLoading) {
    return <DashboardSkeleton />
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <XCircle className="h-4 w-4" />
        <AlertTitle>Error</AlertTitle>
        <AlertDescription>
          Failed to load cluster status: {error.message}
        </AlertDescription>
      </Alert>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Dashboard</h1>
          <p className="text-muted-foreground">
            MynaDB Cluster Overview
          </p>
        </div>
        <Badge
          variant={status?.healthy ? 'success' : 'destructive'}
          className="text-base px-4 py-1"
        >
          {status?.healthy ? (
            <>
              <CheckCircle2 className="mr-2 h-4 w-4" />
              Healthy
            </>
          ) : (
            <>
              <XCircle className="mr-2 h-4 w-4" />
              Degraded
            </>
          )}
        </Badge>
      </div>

      {/* Warnings */}
      {status?.warnings && status.warnings.length > 0 && (
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Cluster Warnings</AlertTitle>
          <AlertDescription>
            <ul className="mt-2 list-disc list-inside space-y-1">
              {status.warnings.map((warning, i) => (
                <li key={i}>{warning}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      )}

      {/* Meta Leader Info */}
      {status?.meta_leader && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base font-medium flex items-center gap-2">
              <Database className="h-4 w-4" />
              Meta Leader
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-4">
              <code className="text-sm bg-muted px-2 py-1 rounded">
                {status.meta_leader}
              </code>
              {status.meta_term !== undefined && (
                <span className="text-sm text-muted-foreground">
                  Term: {status.meta_term}
                </span>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Node Counts */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Meta Nodes</CardTitle>
            <Database className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {status?.node_counts.meta.active}/{status?.node_counts.meta.total}
            </div>
            <p className="text-xs text-muted-foreground">
              {status?.node_counts.meta.inactive} inactive
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Router Nodes</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {status?.node_counts.router.active}/{status?.node_counts.router.total}
            </div>
            <p className="text-xs text-muted-foreground">
              {status?.node_counts.router.inactive} inactive
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Data Nodes</CardTitle>
            <Server className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {status?.node_counts.data.active}/{status?.node_counts.data.total}
            </div>
            <p className="text-xs text-muted-foreground">
              {status?.node_counts.data.primary} primary, {status?.node_counts.data.standby} standby
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Tables</CardTitle>
            <Table2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatNumber(status?.table_count ?? 0)}
            </div>
            <p className="text-xs text-muted-foreground">
              {formatNumber(status?.partition_count ?? 0)} partitions
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Data Node Details */}
      {status?.node_counts.data && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Data Node Breakdown</CardTitle>
            <CardDescription>
              Status of all data nodes in the cluster
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-4">
              <div className="flex items-center gap-2">
                <div className="h-3 w-3 rounded-full bg-green-500" />
                <span className="text-sm">
                  Primary: {status.node_counts.data.primary}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <div className="h-3 w-3 rounded-full bg-blue-500" />
                <span className="text-sm">
                  Standby: {status.node_counts.data.standby}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <div className="h-3 w-3 rounded-full bg-amber-500" />
                <span className="text-sm">
                  Retiring: {status.node_counts.data.retiring}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <div className="h-3 w-3 rounded-full bg-gray-500" />
                <span className="text-sm">
                  Drained: {status.node_counts.data.drained}
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Quick Actions */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Server className="h-4 w-4" />
              Manage Nodes
            </CardTitle>
            <CardDescription>
              View and manage cluster nodes
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button asChild className="w-full">
              <Link to="/nodes">View Nodes</Link>
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Table2 className="h-4 w-4" />
              Browse Tables
            </CardTitle>
            <CardDescription>
              View and manage tables
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button asChild className="w-full">
              <Link to="/tables">View Tables</Link>
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <ArrowDownToLine className="h-4 w-4" />
              Scale Operations
            </CardTitle>
            <CardDescription>
              Plan and execute scale-in operations
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button asChild className="w-full">
              <Link to="/scale-in">Scale In</Link>
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function DashboardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <Skeleton className="h-9 w-48" />
          <Skeleton className="h-4 w-32 mt-2" />
        </div>
        <Skeleton className="h-8 w-24" />
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {[...Array(4)].map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-2">
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-16" />
              <Skeleton className="h-3 w-20 mt-2" />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
