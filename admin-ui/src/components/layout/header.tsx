import { useQuery } from '@tanstack/react-query'
import { RefreshCw, Moon, Sun } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { fetchClusterStatus } from '@/lib/api'
import { useEffect, useState } from 'react'

interface HeaderProps {
  title?: string
}

export function Header({ title }: HeaderProps) {
  const [isDark, setIsDark] = useState(false)

  const { data: clusterStatus, refetch, isFetching } = useQuery({
    queryKey: ['cluster-status'],
    queryFn: fetchClusterStatus,
    refetchInterval: 5000,
  })

  useEffect(() => {
    const root = window.document.documentElement
    if (isDark) {
      root.classList.add('dark')
    } else {
      root.classList.remove('dark')
    }
  }, [isDark])

  return (
    <header className="flex h-16 items-center justify-between border-b bg-card px-6">
      <div className="flex items-center gap-4">
        {title && <h2 className="text-lg font-semibold">{title}</h2>}
      </div>

      <div className="flex items-center gap-4">
        {/* Cluster Status */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Cluster:</span>
          {clusterStatus ? (
            <Badge variant={clusterStatus.healthy ? 'success' : 'destructive'}>
              {clusterStatus.healthy ? 'Healthy' : 'Degraded'}
            </Badge>
          ) : (
            <Badge variant="secondary">Unknown</Badge>
          )}
        </div>

        {/* Refresh Button */}
        <Button
          variant="ghost"
          size="icon"
          onClick={() => refetch()}
          disabled={isFetching}
        >
          <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>

        {/* Theme Toggle */}
        <Button
          variant="ghost"
          size="icon"
          onClick={() => setIsDark(!isDark)}
        >
          {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </Button>
      </div>
    </header>
  )
}
