import { useState } from 'react'
import {
  Database,
  Folder,
  FolderOpen,
  File,
  HardDrive,
  ChevronRight,
  ChevronDown,
  RefreshCw,
  Trash2,
  FileStack,
  Calendar,
  Loader2,
  Search,
  Layers,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { ScrollArea } from '@/components/ui/scroll-area'
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
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  useStorageInfo,
  useStorageCatalogs,
  useStorageSchemas,
  useStorageTable,
  useParquetDetails,
  useDeletePartition,
  useTriggerCompaction,
} from '@/hooks/use-storage'
import { formatBytes, formatNumber } from '@/lib/utils'
import type { CatalogInfo, SchemaInfo, TableStorageInfo, PartitionInfo, FileInfo, ParquetFileDetails } from '@/lib/types'

interface TreeNode {
  type: 'catalog' | 'schema' | 'table' | 'partition' | 'file'
  name: string
  catalog?: string
  schema?: string
  table?: string
  partition?: string
  data?: CatalogInfo | SchemaInfo | TableStorageInfo | PartitionInfo | FileInfo
}

export function StoragePage() {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set())
  const [selectedNode, setSelectedNode] = useState<TreeNode | null>(null)
  const [selectedCatalog, setSelectedCatalog] = useState<string | undefined>()
  const [selectedSchema, setSelectedSchema] = useState<string | undefined>()
  const [selectedTable, setSelectedTable] = useState<string | undefined>()
  const [selectedFile, setSelectedFile] = useState<string | undefined>()
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<{ catalog: string; schema: string; table: string; date: string } | null>(null)
  const [search, setSearch] = useState('')

  const { data: storageInfo, isLoading: storageLoading, refetch: refetchStorage } = useStorageInfo()
  const { data: catalogs, isLoading: catalogsLoading, refetch: refetchCatalogs } = useStorageCatalogs()
  const { data: schemas } = useStorageSchemas(selectedCatalog)
  const { data: tableInfo } = useStorageTable(selectedCatalog, selectedSchema, selectedTable)
  const { data: fileDetails } = useParquetDetails(selectedFile)

  const deletePartition = useDeletePartition()
  const triggerCompaction = useTriggerCompaction()

  const toggleNode = (nodeKey: string) => {
    const newExpanded = new Set(expandedNodes)
    if (newExpanded.has(nodeKey)) {
      newExpanded.delete(nodeKey)
    } else {
      newExpanded.add(nodeKey)
    }
    setExpandedNodes(newExpanded)
  }

  const handleNodeClick = (node: TreeNode) => {
    setSelectedNode(node)

    if (node.type === 'catalog') {
      setSelectedCatalog(node.name)
      setSelectedSchema(undefined)
      setSelectedTable(undefined)
      setSelectedFile(undefined)
    } else if (node.type === 'schema') {
      setSelectedCatalog(node.catalog)
      setSelectedSchema(node.name)
      setSelectedTable(undefined)
      setSelectedFile(undefined)
    } else if (node.type === 'table') {
      setSelectedCatalog(node.catalog)
      setSelectedSchema(node.schema)
      setSelectedTable(node.name)
      setSelectedFile(undefined)
    } else if (node.type === 'file') {
      const file = node.data as FileInfo
      setSelectedFile(file.path)
    }
  }

  const handleDeletePartition = async () => {
    if (!deleteTarget) return

    await deletePartition.mutateAsync(deleteTarget)
    setShowDeleteDialog(false)
    setDeleteTarget(null)
  }

  const handleCompact = async (catalog: string, schema?: string, table?: string) => {
    await triggerCompaction.mutateAsync({ catalog, schema, table })
  }

  const filteredCatalogs = catalogs?.filter((c) =>
    c.name.toLowerCase().includes(search.toLowerCase())
  )

  const isLoading = storageLoading || catalogsLoading

  return (
    <div className="flex h-[calc(100vh-4rem)] gap-4">
      {/* Tree Navigation */}
      <div className="w-80 flex flex-col border-r">
        <div className="p-4 border-b space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold">Storage Browser</h2>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => {
                refetchStorage()
                refetchCatalogs()
              }}
            >
              <RefreshCw className="h-4 w-4" />
            </Button>
          </div>

          <div className="relative">
            <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search catalogs..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8"
            />
          </div>

          {storageInfo && (
            <div className="space-y-2 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Backend:</span>
                <Badge variant="outline">{storageInfo.backend}</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Total Size:</span>
                <span className="font-medium">{formatBytes(storageInfo.total_size_bytes)}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Files:</span>
                <span className="font-medium">{formatNumber(storageInfo.file_count)}</span>
              </div>
              {storageInfo.bucket && (
                <div className="flex items-center justify-between">
                  <span className="text-muted-foreground">Bucket:</span>
                  <span className="font-mono text-xs">{storageInfo.bucket}</span>
                </div>
              )}
            </div>
          )}
        </div>

        <ScrollArea className="flex-1">
          {isLoading ? (
            <div className="p-4 space-y-2">
              {[...Array(5)].map((_, i) => (
                <Skeleton key={i} className="h-8 w-full" />
              ))}
            </div>
          ) : filteredCatalogs && filteredCatalogs.length > 0 ? (
            <div className="p-2">
              {filteredCatalogs.map((catalog) => (
                <CatalogNode
                  key={catalog.name}
                  catalog={catalog}
                  schemas={selectedCatalog === catalog.name ? schemas : undefined}
                  tableInfo={tableInfo}
                  expandedNodes={expandedNodes}
                  selectedNode={selectedNode}
                  onToggle={toggleNode}
                  onClick={handleNodeClick}
                  onSelectCatalog={() => setSelectedCatalog(catalog.name)}
                  onSelectSchema={(schema) => {
                    setSelectedCatalog(catalog.name)
                    setSelectedSchema(schema)
                  }}
                  onSelectTable={(schema, table) => {
                    setSelectedCatalog(catalog.name)
                    setSelectedSchema(schema)
                    setSelectedTable(table)
                  }}
                />
              ))}
            </div>
          ) : (
            <div className="p-4 text-center text-sm text-muted-foreground">
              <Database className="h-8 w-8 mx-auto mb-2 opacity-50" />
              No catalogs found
            </div>
          )}
        </ScrollArea>
      </div>

      {/* Details Panel */}
      <div className="flex-1 overflow-auto">
        {!selectedNode ? (
          <div className="flex items-center justify-center h-full text-muted-foreground">
            <div className="text-center">
              <HardDrive className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>Select a catalog, schema, or table to view details</p>
            </div>
          </div>
        ) : selectedNode.type === 'catalog' ? (
          <CatalogDetails
            catalog={catalogs?.find((c) => c.name === selectedNode.name)}
            onCompact={() => handleCompact(selectedNode.name)}
            isCompacting={triggerCompaction.isPending}
          />
        ) : selectedNode.type === 'schema' ? (
          <SchemaDetails
            schema={schemas?.find((s) => s.name === selectedNode.name)}
            catalog={selectedNode.catalog!}
            onCompact={() => handleCompact(selectedNode.catalog!, selectedNode.name)}
            isCompacting={triggerCompaction.isPending}
          />
        ) : selectedNode.type === 'table' ? (
          <TableDetails
            tableInfo={tableInfo}
            catalog={selectedNode.catalog!}
            schema={selectedNode.schema!}
            onCompact={() => handleCompact(selectedNode.catalog!, selectedNode.schema!, selectedNode.name)}
            onDeletePartition={(date) => {
              setDeleteTarget({
                catalog: selectedNode.catalog!,
                schema: selectedNode.schema!,
                table: selectedNode.name,
                date,
              })
              setShowDeleteDialog(true)
            }}
            isCompacting={triggerCompaction.isPending}
          />
        ) : selectedNode.type === 'file' ? (
          <FileDetails details={fileDetails} isLoading={!fileDetails && !!selectedFile} />
        ) : null}
      </div>

      {/* Delete Confirmation Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Partition</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete the partition <strong>{deleteTarget?.date}</strong>?
              This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDeletePartition}
              disabled={deletePartition.isPending}
            >
              {deletePartition.isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

interface CatalogNodeProps {
  catalog: CatalogInfo
  schemas?: SchemaInfo[]
  tableInfo?: TableStorageInfo
  expandedNodes: Set<string>
  selectedNode: TreeNode | null
  onToggle: (key: string) => void
  onClick: (node: TreeNode) => void
  onSelectCatalog: () => void
  onSelectSchema: (schema: string) => void
  onSelectTable: (schema: string, table: string) => void
}

function CatalogNode({
  catalog,
  schemas,
  tableInfo,
  expandedNodes,
  selectedNode,
  onToggle,
  onClick,
  onSelectCatalog,
  onSelectSchema,
  onSelectTable,
}: CatalogNodeProps) {
  const isExpanded = expandedNodes.has(catalog.name)
  const isSelected = selectedNode?.type === 'catalog' && selectedNode.name === catalog.name

  return (
    <div>
      <div
        className={`flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-accent ${
          isSelected ? 'bg-accent' : ''
        }`}
        onClick={() => {
          onSelectCatalog()
          onClick({ type: 'catalog', name: catalog.name, data: catalog })
          onToggle(catalog.name)
        }}
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        <Database className="h-4 w-4 text-blue-500" />
        <span className="flex-1 truncate font-medium">{catalog.name}</span>
        <Badge variant="secondary" className="text-xs">
          {formatBytes(catalog.total_size_bytes)}
        </Badge>
      </div>

      {isExpanded && schemas && (
        <div className="ml-4 border-l pl-2 mt-1 space-y-1">
          {schemas.map((schema) => (
            <SchemaNode
              key={schema.name}
              schema={schema}
              catalog={catalog.name}
              tableInfo={tableInfo}
              expandedNodes={expandedNodes}
              selectedNode={selectedNode}
              onToggle={onToggle}
              onClick={onClick}
              onSelectSchema={onSelectSchema}
              onSelectTable={onSelectTable}
            />
          ))}
        </div>
      )}
    </div>
  )
}

interface SchemaNodeProps {
  schema: SchemaInfo
  catalog: string
  tableInfo?: TableStorageInfo
  expandedNodes: Set<string>
  selectedNode: TreeNode | null
  onToggle: (key: string) => void
  onClick: (node: TreeNode) => void
  onSelectSchema: (schema: string) => void
  onSelectTable: (schema: string, table: string) => void
}

function SchemaNode({
  schema,
  catalog,
  tableInfo,
  expandedNodes,
  selectedNode,
  onToggle,
  onClick,
  onSelectSchema,
  onSelectTable,
}: SchemaNodeProps) {
  const nodeKey = `${catalog}/${schema.name}`
  const isExpanded = expandedNodes.has(nodeKey)
  const isSelected = selectedNode?.type === 'schema' && selectedNode.name === schema.name && selectedNode.catalog === catalog

  return (
    <div>
      <div
        className={`flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-accent ${
          isSelected ? 'bg-accent' : ''
        }`}
        onClick={() => {
          onSelectSchema(schema.name)
          onClick({ type: 'schema', name: schema.name, catalog, data: schema })
          onToggle(nodeKey)
        }}
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        {isExpanded ? (
          <FolderOpen className="h-4 w-4 text-yellow-500" />
        ) : (
          <Folder className="h-4 w-4 text-yellow-500" />
        )}
        <span className="flex-1 truncate">{schema.name}</span>
        <span className="text-xs text-muted-foreground">{schema.tables} tables</span>
      </div>

      {isExpanded && tableInfo && tableInfo.schema === schema.name && (
        <div className="ml-4 border-l pl-2 mt-1 space-y-1">
          <TableNode
            table={tableInfo}
            catalog={catalog}
            schema={schema.name}
            expandedNodes={expandedNodes}
            selectedNode={selectedNode}
            onToggle={onToggle}
            onClick={onClick}
            onSelectTable={onSelectTable}
          />
        </div>
      )}
    </div>
  )
}

interface TableNodeProps {
  table: TableStorageInfo
  catalog: string
  schema: string
  expandedNodes: Set<string>
  selectedNode: TreeNode | null
  onToggle: (key: string) => void
  onClick: (node: TreeNode) => void
  onSelectTable: (schema: string, table: string) => void
}

function TableNode({
  table,
  catalog,
  schema,
  expandedNodes,
  selectedNode,
  onToggle,
  onClick,
  onSelectTable,
}: TableNodeProps) {
  const nodeKey = `${catalog}/${schema}/${table.name}`
  const isExpanded = expandedNodes.has(nodeKey)
  const isSelected = selectedNode?.type === 'table' && selectedNode.name === table.name

  return (
    <div>
      <div
        className={`flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-accent ${
          isSelected ? 'bg-accent' : ''
        }`}
        onClick={() => {
          onSelectTable(schema, table.name)
          onClick({ type: 'table', name: table.name, catalog, schema, data: table })
          onToggle(nodeKey)
        }}
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        <FileStack className="h-4 w-4 text-green-500" />
        <span className="flex-1 truncate">{table.name}</span>
        <span className="text-xs text-muted-foreground">{table.partitions.length} parts</span>
      </div>

      {isExpanded && table.partitions && (
        <div className="ml-4 border-l pl-2 mt-1 space-y-1">
          {table.partitions.map((partition) => (
            <PartitionNode
              key={partition.date}
              partition={partition}
              catalog={catalog}
              schema={schema}
              table={table.name}
              expandedNodes={expandedNodes}
              selectedNode={selectedNode}
              onToggle={onToggle}
              onClick={onClick}
            />
          ))}
        </div>
      )}
    </div>
  )
}

interface PartitionNodeProps {
  partition: PartitionInfo
  catalog: string
  schema: string
  table: string
  expandedNodes: Set<string>
  selectedNode: TreeNode | null
  onToggle: (key: string) => void
  onClick: (node: TreeNode) => void
}

function PartitionNode({
  partition,
  catalog,
  schema,
  table,
  expandedNodes,
  selectedNode,
  onToggle,
  onClick,
}: PartitionNodeProps) {
  const nodeKey = `${catalog}/${schema}/${table}/${partition.date}`
  const isExpanded = expandedNodes.has(nodeKey)
  const isSelected = selectedNode?.type === 'partition' && selectedNode.partition === partition.date

  return (
    <div>
      <div
        className={`flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-accent ${
          isSelected ? 'bg-accent' : ''
        }`}
        onClick={() => {
          onClick({ type: 'partition', name: partition.date, catalog, schema, table, partition: partition.date, data: partition })
          onToggle(nodeKey)
        }}
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        <Calendar className="h-4 w-4 text-purple-500" />
        <span className="flex-1 truncate font-mono text-sm">{partition.date}</span>
        <span className="text-xs text-muted-foreground">{partition.files.length} files</span>
      </div>

      {isExpanded && partition.files && (
        <div className="ml-4 border-l pl-2 mt-1 space-y-1">
          {partition.files.map((file) => (
            <div
              key={file.path}
              className={`flex items-center gap-2 px-2 py-1 rounded-lg cursor-pointer hover:bg-accent ${
                selectedNode?.type === 'file' && (selectedNode.data as FileInfo)?.path === file.path ? 'bg-accent' : ''
              }`}
              onClick={() => onClick({ type: 'file', name: file.name, catalog, schema, table, data: file })}
            >
              <File className="h-4 w-4 text-gray-500" />
              <span className="flex-1 truncate font-mono text-xs">{file.name}</span>
              <Badge variant={file.level === 0 ? 'default' : 'secondary'} className="text-xs">
                L{file.level}
              </Badge>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

interface CatalogDetailsProps {
  catalog?: CatalogInfo
  onCompact: () => void
  isCompacting: boolean
}

function CatalogDetails({ catalog, onCompact, isCompacting }: CatalogDetailsProps) {
  if (!catalog) return <Skeleton className="h-full w-full" />

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Database className="h-6 w-6 text-blue-500" />
            {catalog.name}
          </h1>
          <p className="text-muted-foreground">Catalog Overview</p>
        </div>
        <Button onClick={onCompact} disabled={isCompacting}>
          {isCompacting ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Compacting...
            </>
          ) : (
            <>
              <Layers className="h-4 w-4 mr-2" />
              Compact All
            </>
          )}
        </Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Schemas</p>
            <p className="text-3xl font-bold">{formatNumber(catalog.schemas)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Tables</p>
            <p className="text-3xl font-bold">{formatNumber(catalog.tables)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Partitions</p>
            <p className="text-3xl font-bold">{formatNumber(catalog.partitions)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Total Size</p>
            <p className="text-3xl font-bold">{formatBytes(catalog.total_size_bytes)}</p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

interface SchemaDetailsProps {
  schema?: SchemaInfo
  catalog: string
  onCompact: () => void
  isCompacting: boolean
}

function SchemaDetails({ schema, catalog, onCompact, isCompacting }: SchemaDetailsProps) {
  if (!schema) return <Skeleton className="h-full w-full" />

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Folder className="h-6 w-6 text-yellow-500" />
            {schema.name}
          </h1>
          <p className="text-muted-foreground">{catalog} / Schema</p>
        </div>
        <Button onClick={onCompact} disabled={isCompacting}>
          {isCompacting ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Compacting...
            </>
          ) : (
            <>
              <Layers className="h-4 w-4 mr-2" />
              Compact Schema
            </>
          )}
        </Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Tables</p>
            <p className="text-3xl font-bold">{formatNumber(schema.tables)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Partitions</p>
            <p className="text-3xl font-bold">{formatNumber(schema.partitions)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Total Size</p>
            <p className="text-3xl font-bold">{formatBytes(schema.total_size_bytes)}</p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

interface TableDetailsProps {
  tableInfo?: TableStorageInfo
  catalog: string
  schema: string
  onCompact: () => void
  onDeletePartition: (date: string) => void
  isCompacting: boolean
}

function TableDetails({ tableInfo, catalog, schema, onCompact, onDeletePartition, isCompacting }: TableDetailsProps) {
  if (!tableInfo) return <Skeleton className="h-full w-full" />

  // Calculate level distribution
  const levelDistribution: Record<number, { count: number; size: number }> = {}
  tableInfo.partitions.forEach((partition) => {
    partition.files.forEach((file) => {
      if (!levelDistribution[file.level]) {
        levelDistribution[file.level] = { count: 0, size: 0 }
      }
      levelDistribution[file.level].count++
      levelDistribution[file.level].size += file.size_bytes
    })
  })

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <FileStack className="h-6 w-6 text-green-500" />
            {tableInfo.name}
          </h1>
          <p className="text-muted-foreground">{catalog}.{schema}</p>
        </div>
        <Button onClick={onCompact} disabled={isCompacting}>
          {isCompacting ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Compacting...
            </>
          ) : (
            <>
              <Layers className="h-4 w-4 mr-2" />
              Compact Table
            </>
          )}
        </Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Partitions</p>
            <p className="text-3xl font-bold">{formatNumber(tableInfo.partitions.length)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Files</p>
            <p className="text-3xl font-bold">{formatNumber(tableInfo.file_count)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Total Rows</p>
            <p className="text-3xl font-bold">{formatNumber(tableInfo.total_rows)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Total Size</p>
            <p className="text-3xl font-bold">{formatBytes(tableInfo.total_size_bytes)}</p>
          </CardContent>
        </Card>
      </div>

      {/* Compaction Level Distribution */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Compaction Level Distribution</CardTitle>
          <CardDescription>Files grouped by compaction level (L0 = recent, higher = more compacted)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {Object.entries(levelDistribution)
              .sort(([a], [b]) => Number(a) - Number(b))
              .map(([level, data]) => (
                <div key={level} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span className="flex items-center gap-2">
                      <Badge variant={level === '0' ? 'default' : 'secondary'}>L{level}</Badge>
                      <span className="text-muted-foreground">{data.count} files</span>
                    </span>
                    <span>{formatBytes(data.size)}</span>
                  </div>
                  <Progress
                    value={(data.size / tableInfo.total_size_bytes) * 100}
                    className="h-2"
                  />
                </div>
              ))}
          </div>
        </CardContent>
      </Card>

      {/* Partitions Table */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Partitions</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead className="text-right">Files</TableHead>
                <TableHead className="text-right">Rows</TableHead>
                <TableHead className="text-right">Size</TableHead>
                <TableHead className="w-[100px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tableInfo.partitions.map((partition) => (
                <TableRow key={partition.date}>
                  <TableCell className="font-mono">{partition.date}</TableCell>
                  <TableCell className="text-right">{partition.files.length}</TableCell>
                  <TableCell className="text-right">{formatNumber(partition.row_count)}</TableCell>
                  <TableCell className="text-right">{formatBytes(partition.size_bytes)}</TableCell>
                  <TableCell>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-destructive hover:text-destructive"
                            onClick={() => onDeletePartition(partition.date)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Delete partition</TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}

interface FileDetailsProps {
  details?: ParquetFileDetails
  isLoading: boolean
}

function FileDetails({ details, isLoading }: FileDetailsProps) {
  if (isLoading) {
    return (
      <div className="p-6 space-y-4">
        <Skeleton className="h-8 w-1/3" />
        <Skeleton className="h-32 w-full" />
        <Skeleton className="h-48 w-full" />
      </div>
    )
  }

  if (!details) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        <div className="text-center">
          <File className="h-12 w-12 mx-auto mb-4 opacity-50" />
          <p>Select a file to view details</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <File className="h-6 w-6 text-gray-500" />
          Parquet File Details
        </h1>
        <p className="text-muted-foreground font-mono text-sm mt-1 break-all">{details.path}</p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Row Count</p>
            <p className="text-3xl font-bold">{formatNumber(details.row_count)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Row Groups</p>
            <p className="text-3xl font-bold">{formatNumber(details.row_groups)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">File Size</p>
            <p className="text-3xl font-bold">{formatBytes(details.file_size_bytes)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">Compression</p>
            <p className="text-3xl font-bold">{details.compression}</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Schema ({details.schema.length} columns)</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Column</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Nullable</TableHead>
                {details.statistics && (
                  <>
                    <TableHead>Min</TableHead>
                    <TableHead>Max</TableHead>
                    <TableHead>Nulls</TableHead>
                  </>
                )}
              </TableRow>
            </TableHeader>
            <TableBody>
              {details.schema.map((col) => (
                <TableRow key={col.name}>
                  <TableCell className="font-mono">{col.name}</TableCell>
                  <TableCell>
                    <Badge variant="outline">{col.data_type}</Badge>
                  </TableCell>
                  <TableCell>{col.nullable ? 'Yes' : 'No'}</TableCell>
                  {details.statistics && (
                    <>
                      <TableCell className="font-mono text-xs">
                        {formatStatValue(details.statistics.min_values[col.name])}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {formatStatValue(details.statistics.max_values[col.name])}
                      </TableCell>
                      <TableCell>
                        {formatNumber(details.statistics.null_counts[col.name] || 0)}
                      </TableCell>
                    </>
                  )}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}

function formatStatValue(value: unknown): string {
  if (value === null || value === undefined) return '-'
  if (typeof value === 'number') return formatNumber(value)
  if (typeof value === 'string') {
    return value.length > 30 ? value.substring(0, 30) + '...' : value
  }
  return String(value)
}
