import { useState } from 'react'
import { Table2, Search, RefreshCw, ArrowRightLeft } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Label } from '@/components/ui/label'
import { useTables, useReassignTable } from '@/hooks/use-tables'
import { useNodes } from '@/hooks/use-nodes'
import { useCatalogs, useSchemas } from '@/hooks/use-query'
import { formatBytes, formatNumber } from '@/lib/utils'
import type { TableSummary } from '@/lib/types'

export function TablesPage() {
  const [search, setSearch] = useState('')
  const [catalogFilter, setCatalogFilter] = useState<string>('all')
  const [schemaFilter, setSchemaFilter] = useState<string>('all')
  const [page, setPage] = useState(1)
  const pageSize = 20

  const { data: tablesResponse, isLoading, refetch, isFetching } = useTables({
    search: search || undefined,
    catalog: catalogFilter === 'all' ? undefined : catalogFilter,
    schema: schemaFilter === 'all' ? undefined : schemaFilter,
    page,
    page_size: pageSize,
  })

  const tables = tablesResponse?.items ?? []
  const totalPages = tablesResponse?.total_pages ?? 1

  const { data: catalogs } = useCatalogs()
  const { data: schemas } = useSchemas(catalogFilter === 'all' ? undefined : catalogFilter)

  const [selectedTable, setSelectedTable] = useState<TableSummary | null>(null)
  const [showReassignDialog, setShowReassignDialog] = useState(false)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Tables</h1>
          <p className="text-muted-foreground">
            Manage database tables
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
            <Search className="h-4 w-4" />
            Search & Filter
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-4">
            <div className="md:col-span-2">
              <Input
                placeholder="Search by table name..."
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value)
                  setPage(1)
                }}
              />
            </div>
            <div>
              <Select
                value={catalogFilter}
                onValueChange={(v) => {
                  setCatalogFilter(v)
                  setSchemaFilter('all')
                  setPage(1)
                }}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Catalog" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Catalogs</SelectItem>
                  {(catalogs ?? []).map((catalog) => (
                    <SelectItem key={catalog} value={catalog}>
                      {catalog}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Select
                value={schemaFilter}
                onValueChange={(v) => {
                  setSchemaFilter(v)
                  setPage(1)
                }}
                disabled={catalogFilter === 'all'}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Schema" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Schemas</SelectItem>
                  {(schemas ?? []).map((schema) => (
                    <SelectItem key={schema} value={schema}>
                      {schema}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Table List */}
      <Card>
        <CardContent className="pt-6">
          {isLoading ? (
            <div className="space-y-4">
              {[...Array(5)].map((_, i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : tables.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12">
              <Table2 className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-lg font-medium">No tables found</p>
              <p className="text-sm text-muted-foreground">
                Try adjusting your search or filters
              </p>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Table</TableHead>
                  <TableHead>Owner</TableHead>
                  <TableHead className="text-right">Partitions</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  <TableHead className="text-right">Rows</TableHead>
                  <TableHead className="w-[100px]">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tables.map((table) => (
                  <TableRow key={table.key}>
                    <TableCell>
                      <div>
                        <p className="font-medium">{table.table}</p>
                        <p className="text-xs text-muted-foreground">
                          {table.catalog}.{table.schema}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      {table.owner_node_id ? (
                        <Badge variant="outline" className="font-mono text-xs">
                          {table.owner_node_id.slice(0, 12)}...
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      {formatNumber(table.partition_count)}
                    </TableCell>
                    <TableCell className="text-right">
                      {formatBytes(table.total_bytes)}
                    </TableCell>
                    <TableCell className="text-right">
                      {formatNumber(table.total_rows)}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => {
                          setSelectedTable(table)
                          setShowReassignDialog(true)
                        }}
                      >
                        <ArrowRightLeft className="h-4 w-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4">
              <p className="text-sm text-muted-foreground">
                Page {page} of {totalPages}
              </p>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page === 1}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Reassign Dialog */}
      <ReassignDialog
        table={selectedTable}
        open={showReassignDialog}
        onOpenChange={(open) => {
          setShowReassignDialog(open)
          if (!open) setSelectedTable(null)
        }}
        onSuccess={() => {
          setShowReassignDialog(false)
          setSelectedTable(null)
          refetch()
        }}
      />
    </div>
  )
}

interface ReassignDialogProps {
  table: TableSummary | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onSuccess: () => void
}

function ReassignDialog({ table, open, onOpenChange, onSuccess }: ReassignDialogProps) {
  const [targetNodeId, setTargetNodeId] = useState('')
  const { data: nodes } = useNodes({ role: 'Data', state: 'Active' })
  const reassignTable = useReassignTable()

  const availableNodes = nodes?.filter((n) => n.id !== table?.owner_node_id) ?? []

  const handleReassign = async () => {
    if (!table || !targetNodeId) return

    await reassignTable.mutateAsync({
      tableKey: table.key,
      request: { target_node_id: targetNodeId },
    })

    setTargetNodeId('')
    onSuccess()
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Reassign Table</DialogTitle>
          <DialogDescription>
            Move table <strong>{table?.table}</strong> to a different data node.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div>
            <Label>Current Owner</Label>
            <p className="text-sm text-muted-foreground font-mono mt-1">
              {table?.owner_node_id || 'None'}
            </p>
          </div>

          <div>
            <Label htmlFor="target-node">Target Node</Label>
            <Select value={targetNodeId} onValueChange={setTargetNodeId}>
              <SelectTrigger id="target-node" className="mt-1">
                <SelectValue placeholder="Select a node" />
              </SelectTrigger>
              <SelectContent>
                {availableNodes.map((node) => (
                  <SelectItem key={node.id} value={node.id}>
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-xs">{node.id.slice(0, 12)}...</span>
                      {node.mode && (
                        <Badge variant="outline" className="text-xs">
                          {node.mode}
                        </Badge>
                      )}
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleReassign}
            disabled={!targetNodeId || reassignTable.isPending}
          >
            {reassignTable.isPending ? 'Reassigning...' : 'Reassign'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
