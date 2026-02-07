import {
  Settings,
  Server,
  Network,
  RefreshCw,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useQuery } from '@tanstack/react-query'
import { fetchConfig } from '@/lib/api'

export function SettingsPage() {
  const { data: config, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['config'],
    queryFn: fetchConfig,
    staleTime: 30000,
  })

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Settings</h1>
          <p className="text-muted-foreground">
            Cluster and admin node configuration
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

      <div className="grid gap-6 md:grid-cols-2">
        {/* Admin Node Configuration */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Settings className="h-4 w-4" />
              Admin Node Configuration
            </CardTitle>
            <CardDescription>
              Current admin node settings
            </CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-3">
                <Skeleton className="h-6 w-full" />
                <Skeleton className="h-6 w-full" />
                <Skeleton className="h-6 w-full" />
              </div>
            ) : config ? (
              <div className="space-y-4">
                <div className="flex justify-between items-center py-2 border-b">
                  <span className="text-sm text-muted-foreground">HTTP Host</span>
                  <code className="text-sm bg-muted px-2 py-0.5 rounded">
                    {config.admin.http_host}
                  </code>
                </div>
                <div className="flex justify-between items-center py-2 border-b">
                  <span className="text-sm text-muted-foreground">HTTP Port</span>
                  <code className="text-sm bg-muted px-2 py-0.5 rounded">
                    {config.admin.http_port}
                  </code>
                </div>
                <div className="py-2">
                  <span className="text-sm text-muted-foreground">Meta Endpoints</span>
                  <div className="mt-2 space-y-1">
                    {config.admin.meta_endpoints.map((endpoint, i) => (
                      <div key={i} className="flex items-center gap-2">
                        <Network className="h-3 w-3 text-muted-foreground" />
                        <code className="text-sm bg-muted px-2 py-0.5 rounded">
                          {endpoint}
                        </code>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground">No configuration data available</p>
            )}
          </CardContent>
        </Card>

        {/* Cluster Overview */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Server className="h-4 w-4" />
              Cluster Overview
            </CardTitle>
            <CardDescription>
              Node counts by role
            </CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-3">
                <Skeleton className="h-6 w-full" />
                <Skeleton className="h-6 w-full" />
              </div>
            ) : config ? (
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-muted rounded-lg text-center">
                  <p className="text-2xl font-bold">{config.node_counts.meta}</p>
                  <p className="text-sm text-muted-foreground">Meta Nodes</p>
                </div>
                <div className="p-3 bg-muted rounded-lg text-center">
                  <p className="text-2xl font-bold">{config.node_counts.router}</p>
                  <p className="text-sm text-muted-foreground">Router Nodes</p>
                </div>
                <div className="p-3 bg-muted rounded-lg text-center">
                  <p className="text-2xl font-bold">{config.node_counts.data}</p>
                  <p className="text-sm text-muted-foreground">Data Nodes</p>
                </div>
                <div className="p-3 bg-muted rounded-lg text-center">
                  <p className="text-2xl font-bold">{config.node_counts.compactor}</p>
                  <p className="text-sm text-muted-foreground">Compactor Nodes</p>
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground">No cluster data available</p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
