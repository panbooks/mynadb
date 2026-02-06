import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { MainLayout } from '@/components/layout/main-layout'
import { Dashboard } from '@/pages/dashboard'
import { NodesPage } from '@/pages/nodes'
import { NodeDetailPage } from '@/pages/nodes/[id]'
import { TablesPage } from '@/pages/tables'
import { ScaleInPage } from '@/pages/scale-in'
import { EventsPage } from '@/pages/events'
import { MetaOperationsPage } from '@/pages/meta-operations'
import { DataOperationsPage } from '@/pages/data-operations'
import { RouterOperationsPage } from '@/pages/router-operations'
import { QueryPage } from '@/pages/query'
import { StoragePage } from '@/pages/storage'

function App() {
  return (
    <BrowserRouter basename="/admin">
      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={<Dashboard />} />
          <Route path="query" element={<QueryPage />} />
          <Route path="storage" element={<StoragePage />} />
          <Route path="nodes" element={<NodesPage />} />
          <Route path="nodes/:id" element={<NodeDetailPage />} />
          <Route path="tables" element={<TablesPage />} />
          <Route path="scale-in" element={<ScaleInPage />} />
          <Route path="events" element={<EventsPage />} />
          <Route path="meta-operations" element={<MetaOperationsPage />} />
          <Route path="data-operations" element={<DataOperationsPage />} />
          <Route path="router-operations" element={<RouterOperationsPage />} />
          <Route path="*" element={<NotFound />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

function NotFound() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center">
        <h1 className="text-4xl font-bold">404</h1>
        <p className="mt-2 text-muted-foreground">Page not found</p>
      </div>
    </div>
  )
}

export default App
