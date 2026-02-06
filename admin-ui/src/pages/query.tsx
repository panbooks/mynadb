import { useState, useCallback, useRef, useEffect } from 'react'
import Editor, { OnMount } from '@monaco-editor/react'
import {
  Play,
  FileText,
  Clock,
  Save,
  Trash2,
  Download,
  Search,
  Loader2,
  AlertCircle,
  CheckCircle2,
  Table2,
  BarChart3,
  Copy,
  X,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Label } from '@/components/ui/label'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
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
import { Alert, AlertDescription } from '@/components/ui/alert'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import {
  useCatalogs,
  useExecuteQuery,
  useExplainQuery,
  useAnalyzeQuery,
  getLocalQueryHistory,
  addLocalQueryHistory,
  clearLocalQueryHistory,
  getLocalSavedQueries,
  addLocalSavedQuery,
  deleteLocalSavedQuery,
} from '@/hooks/use-query'
import { formatDuration, formatRelativeTime } from '@/lib/utils'
import type { QueryResult, QueryExplain, QueryAnalysis, SavedQuery } from '@/lib/types'
import type * as Monaco from 'monaco-editor'

interface QueryHistoryEntry {
  id: string
  sql: string
  catalog: string
  executed_at: string
  execution_time_ms: number
  row_count: number
  status: 'success' | 'error'
  error?: string
}

const DEFAULT_SQL = `-- Welcome to MynaDB Query Console
-- Select a catalog and start writing SQL

SELECT * FROM your_table LIMIT 100;`

const SQL_TEMPLATES = [
  { name: 'Show Tables', sql: 'SHOW TABLES' },
  { name: 'Describe Table', sql: 'DESCRIBE table_name' },
  { name: 'Count Rows', sql: 'SELECT COUNT(*) FROM table_name' },
  { name: 'Time Bucket', sql: `SELECT
  time_bucket('1 hour', time) as bucket,
  COUNT(*) as count,
  AVG(value) as avg_value
FROM table_name
WHERE time > now() - INTERVAL '24 hours'
GROUP BY bucket
ORDER BY bucket DESC` },
  { name: 'Latest Values', sql: `SELECT *
FROM table_name
ORDER BY time DESC
LIMIT 100` },
]

export function QueryPage() {
  const [sql, setSql] = useState(DEFAULT_SQL)
  const [selectedCatalog, setSelectedCatalog] = useState<string>('')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [explain, setExplain] = useState<QueryExplain | null>(null)
  const [analysis, setAnalysis] = useState<QueryAnalysis | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [history, setHistory] = useState<QueryHistoryEntry[]>(() => getLocalQueryHistory())
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(() => getLocalSavedQueries())
  const [showSaveDialog, setShowSaveDialog] = useState(false)
  const [queryName, setQueryName] = useState('')
  const [activeResultTab, setActiveResultTab] = useState('results')
  const [sidebarTab, setSidebarTab] = useState<'history' | 'saved'>('history')
  const [historySearch, setHistorySearch] = useState('')

  const editorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null)
  const { data: catalogs, isLoading: catalogsLoading } = useCatalogs()
  const executeQuery = useExecuteQuery()
  const explainQuery = useExplainQuery()
  const analyzeQuery = useAnalyzeQuery()

  // Set default catalog when catalogs are loaded
  useEffect(() => {
    if (catalogs && catalogs.length > 0 && !selectedCatalog) {
      setSelectedCatalog(catalogs[0])
    }
  }, [catalogs, selectedCatalog])

  const handleEditorMount: OnMount = (editor) => {
    editorRef.current = editor

    // Add keyboard shortcuts
    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [
        // Ctrl+Enter or Cmd+Enter
        (window.navigator.platform.includes('Mac') ? 2048 : 2048) | 3, // CtrlCmd + Enter
      ],
      run: () => handleRunQuery(),
    })

    editor.addAction({
      id: 'explain-query',
      label: 'Explain Query',
      keybindings: [
        (window.navigator.platform.includes('Mac') ? 2048 : 2048) | 1024 | 35, // CtrlCmd + Shift + E
      ],
      run: () => handleExplain(),
    })
  }

  const handleRunQuery = useCallback(async () => {
    if (!selectedCatalog || !sql.trim()) return

    setError(null)
    setResult(null)
    setExplain(null)
    setAnalysis(null)
    setActiveResultTab('results')

    const startTime = Date.now()

    try {
      const queryResult = await executeQuery.mutateAsync({
        sql: sql.trim(),
        catalog: selectedCatalog,
        limit: 1000,
      })

      setResult(queryResult)

      // Add to history
      const historyEntry: QueryHistoryEntry = {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        sql: sql.trim(),
        catalog: selectedCatalog,
        executed_at: new Date().toISOString(),
        execution_time_ms: queryResult.execution_time_ms,
        row_count: queryResult.row_count,
        status: 'success',
      }
      addLocalQueryHistory(historyEntry)
      setHistory(getLocalQueryHistory())
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Query failed'
      setError(errorMessage)

      // Add failed query to history
      const historyEntry: QueryHistoryEntry = {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        sql: sql.trim(),
        catalog: selectedCatalog,
        executed_at: new Date().toISOString(),
        execution_time_ms: Date.now() - startTime,
        row_count: 0,
        status: 'error',
        error: errorMessage,
      }
      addLocalQueryHistory(historyEntry)
      setHistory(getLocalQueryHistory())
    }
  }, [sql, selectedCatalog, executeQuery])

  const handleExplain = useCallback(async () => {
    if (!selectedCatalog || !sql.trim()) return

    setError(null)
    setActiveResultTab('explain')

    try {
      const [explainResult, analysisResult] = await Promise.all([
        explainQuery.mutateAsync({ catalog: selectedCatalog, sql: sql.trim() }),
        analyzeQuery.mutateAsync({ catalog: selectedCatalog, sql: sql.trim() }),
      ])

      setExplain(explainResult)
      setAnalysis(analysisResult)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Explain failed')
    }
  }, [sql, selectedCatalog, explainQuery, analyzeQuery])

  const handleSaveQuery = () => {
    if (!queryName.trim() || !sql.trim() || !selectedCatalog) return

    addLocalSavedQuery({
      name: queryName.trim(),
      sql: sql.trim(),
      catalog: selectedCatalog,
    })

    setSavedQueries(getLocalSavedQueries())
    setQueryName('')
    setShowSaveDialog(false)
  }

  const handleDeleteSavedQuery = (id: string) => {
    deleteLocalSavedQuery(id)
    setSavedQueries(getLocalSavedQueries())
  }

  const handleLoadQuery = (query: SavedQuery | QueryHistoryEntry) => {
    setSql(query.sql)
    setSelectedCatalog(query.catalog)
  }

  const handleClearHistory = () => {
    clearLocalQueryHistory()
    setHistory([])
  }

  const handleExportResults = (format: 'csv' | 'json') => {
    if (!result) return

    let content: string
    let mimeType: string
    let extension: string

    if (format === 'csv') {
      const headers = result.columns.map((c) => c.name).join(',')
      const rows = result.rows.map((row) =>
        result.columns.map((col) => {
          const val = row[col.name]
          if (val === null || val === undefined) return ''
          if (typeof val === 'string' && (val.includes(',') || val.includes('"') || val.includes('\n'))) {
            return `"${val.replace(/"/g, '""')}"`
          }
          return String(val)
        }).join(',')
      ).join('\n')
      content = `${headers}\n${rows}`
      mimeType = 'text/csv'
      extension = 'csv'
    } else {
      content = JSON.stringify(result.rows, null, 2)
      mimeType = 'application/json'
      extension = 'json'
    }

    const blob = new Blob([content], { type: mimeType })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `query-results-${Date.now()}.${extension}`
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleCopyResults = () => {
    if (!result) return
    const content = JSON.stringify(result.rows, null, 2)
    navigator.clipboard.writeText(content)
  }

  const filteredHistory = history.filter((h) =>
    h.sql.toLowerCase().includes(historySearch.toLowerCase()) ||
    h.catalog.toLowerCase().includes(historySearch.toLowerCase())
  )

  const isLoading = executeQuery.isPending || explainQuery.isPending || analyzeQuery.isPending

  return (
    <div className="flex h-[calc(100vh-4rem)] gap-4">
      {/* Main Query Area */}
      <div className="flex flex-1 flex-col space-y-4 min-w-0">
        {/* Toolbar */}
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <Label htmlFor="catalog" className="text-sm font-medium">
              Catalog:
            </Label>
            <Select value={selectedCatalog} onValueChange={setSelectedCatalog}>
              <SelectTrigger id="catalog" className="w-[180px]">
                <SelectValue placeholder="Select catalog" />
              </SelectTrigger>
              <SelectContent>
                {catalogsLoading ? (
                  <SelectItem value="loading" disabled>
                    Loading...
                  </SelectItem>
                ) : catalogs && catalogs.length > 0 ? (
                  catalogs.map((catalog) => (
                    <SelectItem key={catalog} value={catalog}>
                      {catalog}
                    </SelectItem>
                  ))
                ) : (
                  <SelectItem value="none" disabled>
                    No catalogs found
                  </SelectItem>
                )}
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center gap-2">
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    onClick={handleRunQuery}
                    disabled={isLoading || !selectedCatalog}
                    className="gap-2"
                  >
                    {executeQuery.isPending ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                    Run
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Ctrl+Enter / Cmd+Enter</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>

            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="outline"
                    onClick={handleExplain}
                    disabled={isLoading || !selectedCatalog}
                    className="gap-2"
                  >
                    <FileText className="h-4 w-4" />
                    Explain
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>View query plan and analysis</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>

            <Button
              variant="outline"
              onClick={() => setShowSaveDialog(true)}
              disabled={!sql.trim() || !selectedCatalog}
              className="gap-2"
            >
              <Save className="h-4 w-4" />
              Save
            </Button>
          </div>

          <Select onValueChange={(val) => setSql(SQL_TEMPLATES.find((t) => t.name === val)?.sql || sql)}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Templates..." />
            </SelectTrigger>
            <SelectContent>
              {SQL_TEMPLATES.map((template) => (
                <SelectItem key={template.name} value={template.name}>
                  {template.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Editor */}
        <Card className="flex-1 overflow-hidden">
          <div className="h-[300px] border-b">
            <Editor
              height="100%"
              defaultLanguage="sql"
              value={sql}
              onChange={(value) => setSql(value || '')}
              onMount={handleEditorMount}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 14,
                lineNumbers: 'on',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                automaticLayout: true,
                tabSize: 2,
                suggest: {
                  showKeywords: true,
                },
              }}
            />
          </div>

          {/* Results Area */}
          <div className="flex-1 overflow-hidden">
            {error && (
              <Alert variant="destructive" className="m-4">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}

            <Tabs value={activeResultTab} onValueChange={setActiveResultTab} className="h-full">
              <div className="flex items-center justify-between border-b px-4">
                <TabsList className="h-10">
                  <TabsTrigger value="results" className="gap-2">
                    <Table2 className="h-4 w-4" />
                    Results
                    {result && (
                      <Badge variant="secondary" className="ml-1">
                        {result.row_count}
                      </Badge>
                    )}
                  </TabsTrigger>
                  <TabsTrigger value="explain" className="gap-2">
                    <BarChart3 className="h-4 w-4" />
                    Explain
                  </TabsTrigger>
                </TabsList>

                {result && activeResultTab === 'results' && (
                  <div className="flex items-center gap-2 py-2">
                    <Badge variant="outline">
                      {formatDuration(result.execution_time_ms)}
                    </Badge>
                    <Button variant="ghost" size="sm" onClick={handleCopyResults}>
                      <Copy className="h-4 w-4" />
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => handleExportResults('csv')}>
                      <Download className="h-4 w-4 mr-1" />
                      CSV
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => handleExportResults('json')}>
                      <Download className="h-4 w-4 mr-1" />
                      JSON
                    </Button>
                  </div>
                )}
              </div>

              <TabsContent value="results" className="h-[calc(100%-3rem)] m-0 p-0">
                {executeQuery.isPending ? (
                  <div className="flex items-center justify-center h-full">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : result ? (
                  <ScrollArea className="h-full">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          {result.columns.map((col) => (
                            <TableHead key={col.name} className="whitespace-nowrap">
                              <div className="flex flex-col">
                                <span>{col.name}</span>
                                <span className="text-xs text-muted-foreground font-normal">
                                  {col.data_type}
                                </span>
                              </div>
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {result.rows.map((row, i) => (
                          <TableRow key={i}>
                            {result.columns.map((col) => (
                              <TableCell key={col.name} className="font-mono text-sm">
                                {formatCellValue(row[col.name])}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                    {result.truncated && (
                      <div className="p-4 text-center text-sm text-muted-foreground">
                        Results truncated. Showing first 1000 rows.
                      </div>
                    )}
                  </ScrollArea>
                ) : (
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    Run a query to see results
                  </div>
                )}
              </TabsContent>

              <TabsContent value="explain" className="h-[calc(100%-3rem)] m-0 p-0">
                {explainQuery.isPending || analyzeQuery.isPending ? (
                  <div className="flex items-center justify-center h-full">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : explain || analysis ? (
                  <ScrollArea className="h-full p-4">
                    <div className="space-y-6">
                      {analysis && (
                        <div className="space-y-4">
                          <h3 className="text-lg font-semibold">Query Analysis</h3>
                          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <Card>
                              <CardContent className="pt-4">
                                <p className="text-sm text-muted-foreground">Tables</p>
                                <p className="text-2xl font-bold">{analysis.tables.length}</p>
                                <p className="text-xs text-muted-foreground truncate">
                                  {analysis.tables.join(', ') || 'None'}
                                </p>
                              </CardContent>
                            </Card>
                            <Card>
                              <CardContent className="pt-4">
                                <p className="text-sm text-muted-foreground">Partitions</p>
                                <p className="text-2xl font-bold">{analysis.partitions_to_scan}</p>
                              </CardContent>
                            </Card>
                            <Card>
                              <CardContent className="pt-4">
                                <p className="text-sm text-muted-foreground">Files</p>
                                <p className="text-2xl font-bold">{analysis.files_to_scan}</p>
                              </CardContent>
                            </Card>
                            <Card>
                              <CardContent className="pt-4">
                                <p className="text-sm text-muted-foreground">Optimizations</p>
                                <p className="text-2xl font-bold">{analysis.optimizations.length}</p>
                              </CardContent>
                            </Card>
                          </div>

                          {analysis.optimizations.length > 0 && (
                            <div>
                              <h4 className="font-medium mb-2">Applied Optimizations</h4>
                              <div className="flex flex-wrap gap-2">
                                {analysis.optimizations.map((opt, i) => (
                                  <Badge key={i} variant="secondary">
                                    {opt}
                                  </Badge>
                                ))}
                              </div>
                            </div>
                          )}

                          <div>
                            <h4 className="font-medium mb-2">Summary</h4>
                            <p className="text-sm text-muted-foreground">{analysis.plan_summary}</p>
                          </div>
                        </div>
                      )}

                      {explain && (
                        <div className="space-y-4">
                          <h3 className="text-lg font-semibold">Execution Plan</h3>

                          <div>
                            <h4 className="font-medium mb-2">Logical Plan</h4>
                            <pre className="bg-muted p-4 rounded-lg text-sm overflow-x-auto">
                              {explain.logical_plan}
                            </pre>
                          </div>

                          <div>
                            <h4 className="font-medium mb-2">Physical Plan</h4>
                            <pre className="bg-muted p-4 rounded-lg text-sm overflow-x-auto">
                              {explain.physical_plan}
                            </pre>
                          </div>
                        </div>
                      )}
                    </div>
                  </ScrollArea>
                ) : (
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    Click "Explain" to analyze your query
                  </div>
                )}
              </TabsContent>
            </Tabs>
          </div>
        </Card>
      </div>

      {/* Sidebar - History & Saved Queries */}
      <div className="w-80 flex flex-col border-l">
        <Tabs value={sidebarTab} onValueChange={(v) => setSidebarTab(v as 'history' | 'saved')} className="flex-1 flex flex-col">
          <TabsList className="grid w-full grid-cols-2 rounded-none border-b">
            <TabsTrigger value="history" className="gap-2">
              <Clock className="h-4 w-4" />
              History
            </TabsTrigger>
            <TabsTrigger value="saved" className="gap-2">
              <Save className="h-4 w-4" />
              Saved
            </TabsTrigger>
          </TabsList>

          <TabsContent value="history" className="flex-1 m-0 flex flex-col">
            <div className="p-2 border-b">
              <div className="relative">
                <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search history..."
                  value={historySearch}
                  onChange={(e) => setHistorySearch(e.target.value)}
                  className="pl-8"
                />
              </div>
            </div>
            <ScrollArea className="flex-1">
              {filteredHistory.length === 0 ? (
                <div className="p-4 text-center text-sm text-muted-foreground">
                  No query history
                </div>
              ) : (
                <div className="space-y-1 p-2">
                  {filteredHistory.map((entry) => (
                    <div
                      key={entry.id}
                      className="p-2 rounded-lg hover:bg-accent cursor-pointer group"
                      onClick={() => handleLoadQuery(entry)}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {entry.status === 'success' ? (
                            <CheckCircle2 className="h-3 w-3 text-green-500" />
                          ) : (
                            <AlertCircle className="h-3 w-3 text-red-500" />
                          )}
                          <Badge variant="outline" className="text-xs">
                            {entry.catalog}
                          </Badge>
                        </div>
                        <span className="text-xs text-muted-foreground">
                          {formatRelativeTime(entry.executed_at)}
                        </span>
                      </div>
                      <p className="text-sm font-mono mt-1 truncate">{entry.sql}</p>
                      <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
                        <span>{entry.row_count} rows</span>
                        <span>{formatDuration(entry.execution_time_ms)}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </ScrollArea>
            {history.length > 0 && (
              <div className="p-2 border-t">
                <Button
                  variant="ghost"
                  size="sm"
                  className="w-full text-muted-foreground"
                  onClick={handleClearHistory}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  Clear History
                </Button>
              </div>
            )}
          </TabsContent>

          <TabsContent value="saved" className="flex-1 m-0 flex flex-col">
            <ScrollArea className="flex-1">
              {savedQueries.length === 0 ? (
                <div className="p-4 text-center text-sm text-muted-foreground">
                  No saved queries
                </div>
              ) : (
                <div className="space-y-1 p-2">
                  {savedQueries.map((query) => (
                    <div
                      key={query.id}
                      className="p-2 rounded-lg hover:bg-accent cursor-pointer group"
                      onClick={() => handleLoadQuery(query)}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-medium">{query.name}</span>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 opacity-0 group-hover:opacity-100"
                          onClick={(e) => {
                            e.stopPropagation()
                            handleDeleteSavedQuery(query.id)
                          }}
                        >
                          <X className="h-3 w-3" />
                        </Button>
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <Badge variant="outline" className="text-xs">
                          {query.catalog}
                        </Badge>
                        <span className="text-xs text-muted-foreground">
                          {formatRelativeTime(query.updated_at)}
                        </span>
                      </div>
                      <p className="text-sm font-mono mt-1 truncate text-muted-foreground">
                        {query.sql}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </ScrollArea>
          </TabsContent>
        </Tabs>
      </div>

      {/* Save Query Dialog */}
      <Dialog open={showSaveDialog} onOpenChange={setShowSaveDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Save Query</DialogTitle>
            <DialogDescription>
              Give your query a name to save it for later use.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Label htmlFor="query-name">Name</Label>
            <Input
              id="query-name"
              value={queryName}
              onChange={(e) => setQueryName(e.target.value)}
              placeholder="My Query"
              className="mt-2"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSaveDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleSaveQuery} disabled={!queryName.trim()}>
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
