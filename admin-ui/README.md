# MynaDB Admin Dashboard

A modern React-based administration dashboard for managing MynaDB clusters. This dashboard provides a web interface to monitor cluster health, manage nodes, browse tables, and perform scale-in operations.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Development](#development)
- [Production Build](#production-build)
- [Architecture](#architecture)
- [Pages & Features](#pages--features)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Features

### Core Features
- **Cluster Overview**: Real-time health monitoring, node counts, and warnings
- **Node Management**: View, filter, and manage cluster nodes with detailed status
- **Table Browser**: Search, filter, and reassign tables across nodes
- **Scale-In Wizard**: Safely reduce cluster size with a guided 3-step process
- **Operations Pages**: Dedicated views for Meta, Data, and Router operations
- **Real-time Events**: WebSocket-powered event stream with filtering
- **Dark/Light Mode**: Theme toggle for comfortable viewing
- **Auto-refresh**: Automatic data refresh every 5 seconds

### New Features (v1.0)
- **💻 SQL Query Console**: Interactive SQL editor powered by Monaco Editor
  - Execute queries with real-time results (CSV/JSON export)
  - EXPLAIN and ANALYZE query plans
  - Query history and saved queries (LocalStorage)
  - Keyboard shortcuts (`Ctrl+Enter` to run, `Ctrl+Shift+E` to explain)
  - SQL templates for common operations

- **📁 Storage Browser**: File system explorer for Parquet data
  - Tree navigation (Catalog → Schema → Table → Partition → File)
  - Storage statistics and size tracking
  - Partition management (view, delete)
  - Compaction trigger at any level (catalog/schema/table)
  - Parquet file inspector (schema, statistics, min/max values)

## Prerequisites

- **Node.js 18+** (required for Vite build)
- **npm 8+** or yarn
- **MynaDB cluster** running (meta + router + data nodes)
- **MynaDB admin server** running

### Installing Node.js 18+

If you have an older Node.js version, you can upgrade using `n`:

```bash
# Install n package manager
npm install -g n

# Install Node.js 20 (LTS)
n 20

# Verify version
node --version  # Should show v20.x.x
```

## Quick Start

### 1. Start the MynaDB Cluster

```bash
cd /path/to/mynadb-admin

# Start the development cluster (requires goreman)
./dev/start-cluster.sh
```

This starts:
- 3 Meta nodes (ports 6000, 6100, 6200)
- 1 Router node (port 7000)
- 2 Data nodes (ports 5000, 5100)

### 2. Start the Admin Server

In a new terminal:

```bash
cd /path/to/mynadb-admin

# Start admin server
./target/debug/mynadb -c ./dev/configs/cluster/admin.toml admin start
```

The admin server runs on `http://localhost:18080`.

### 3. Build and Access the Dashboard

```bash
cd admin-ui

# Install dependencies
npm install

# Build for production
npm run build
```

Access the dashboard at: **http://localhost:18080/admin**

## Development

For active development with hot-reload:

```bash
cd admin-ui

# Install dependencies
npm install

# Start development server
npm run dev
```

The dev server runs on `http://localhost:3000` with API proxy to `http://localhost:18080`.

### Development URLs

| Service | URL |
|---------|-----|
| Dev Server (Vite) | http://localhost:3000/admin |
| Admin API | http://localhost:18080/admin/api/v1 |
| Router (write/query) | http://localhost:7000 |

### Code Structure

```
admin-ui/
├── src/
│   ├── main.tsx              # Entry point
│   ├── App.tsx               # Root component with router
│   ├── index.css             # Tailwind imports & CSS variables
│   ├── lib/
│   │   ├── api.ts            # API client functions
│   │   ├── types.ts          # TypeScript type definitions
│   │   └── utils.ts          # Utility functions (cn, formatBytes, etc.)
│   ├── hooks/
│   │   ├── use-cluster.ts    # Cluster status hooks
│   │   ├── use-nodes.ts      # Node management hooks
│   │   ├── use-tables.ts     # Table management hooks
│   │   ├── use-scale-in.ts   # Scale-in operation hooks
│   │   ├── use-events.ts     # Event stream hooks (WebSocket)
│   │   ├── use-query.ts      # Query execution hooks ⭐ NEW
│   │   └── use-storage.ts    # Storage browser hooks ⭐ NEW
│   ├── components/
│   │   ├── ui/               # shadcn/ui components (button, card, etc.)
│   │   └── layout/           # Layout components (sidebar, header)
│   └── pages/
│       ├── dashboard.tsx     # Main dashboard
│       ├── query.tsx         # SQL Query Console ⭐ NEW
│       ├── storage.tsx       # Storage Browser ⭐ NEW
│       ├── nodes/
│       │   ├── index.tsx     # Nodes list
│       │   └── [id].tsx      # Node detail
│       ├── tables.tsx        # Tables list & reassignment
│       ├── scale-in.tsx      # Scale-in wizard
│       ├── events.tsx        # Event stream
│       ├── meta-operations.tsx
│       ├── data-operations.tsx
│       └── router-operations.tsx
├── package.json
├── vite.config.ts
├── tailwind.config.js
└── tsconfig.json
```

## Production Build

```bash
cd admin-ui

# Install dependencies (if not already done)
npm install

# Build for production
npm run build

# Output is in dist/
ls -la dist/
```

The built files are automatically served by the admin server from `admin-ui/dist/`.

### Build Output

```
dist/
├── index.html          # Main HTML (served for all SPA routes)
├── favicon.svg         # App icon
└── assets/
    ├── index-*.js      # Main application bundle (~172KB)
    ├── vendor-*.js     # React, Router, etc. (~164KB)
    ├── ui-*.js         # UI components (~85KB)
    ├── charts-*.js     # Recharts (lazy loaded)
    └── index-*.css     # Tailwind CSS (~34KB)
```

## Architecture

### Tech Stack

| Layer | Technology |
|-------|------------|
| Framework | React 18 |
| Language | TypeScript |
| Build Tool | Vite 5 |
| Styling | Tailwind CSS |
| UI Components | shadcn/ui (Radix primitives) |
| Data Fetching | TanStack Query v5 |
| Routing | React Router v6 |
| Icons | Lucide React |
| Charts | Recharts |

### Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   React UI      │────▶│  Admin Server   │────▶│  Meta Service   │
│  (Browser)      │◀────│  (Rust/Axum)    │◀────│  (Raft/gRPC)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               │ Proxy requests
                               ▼
                        ┌─────────────────┐
                        │   Data Nodes    │
                        │  (Operations)   │
                        └─────────────────┘
```

### State Management

- **Server State**: TanStack Query with automatic caching and refetching
- **UI State**: React useState/useReducer for local component state
- **Auto-refresh**: 5-second polling interval for cluster status

## Pages & Features

### Dashboard (`/admin`)

The main overview page showing:
- Cluster health status (Healthy/Degraded badge)
- Meta leader info with Raft term
- Node counts by role (Meta, Router, Data)
- Data node breakdown (Primary/Standby/Retiring)
- Total tables and partitions
- Warnings banner (if any)
- Quick action cards for navigation

### Nodes (`/admin/nodes`)

List and manage cluster nodes:
- **Filters**: Role (Meta/Router/Data), State (Active/Inactive/Retiring), Mode (Primary/Standby)
- **Search**: By node ID or address
- **Node Cards**: ID, address, role, state, mode, zone, tables owned, last heartbeat
- Click any node to view details

### Node Detail (`/admin/nodes/:id`)

Detailed node information:
- Full node metadata and addresses
- Health status with heartbeat tracking
- Memory and cache statistics
- For Data nodes:
  - Flush memtable action
  - Trigger compaction action
  - Clear cache action
  - Start retirement (with confirmation dialog)
- Labels display

### Tables (`/admin/tables`)

Browse and manage tables:
- **Search**: By table name
- **Filter**: By catalog
- **Pagination**: Navigate large table lists
- **Table Info**: Name, catalog.schema, owner node, partitions, size, rows
- **Reassign**: Move table to a different data node

### Scale-In Wizard (`/admin/scale-in`)

Safely reduce cluster size in 3 steps:

1. **Configure**
   - Number of nodes to retire
   - Selection strategy (Least Loaded, First In, Last In, HA Unit)
   - Optional: Select specific nodes manually

2. **Preview Plan**
   - Nodes to be retired
   - Tables to migrate
   - Estimated duration
   - Warnings (if any)
   - Execution phases breakdown

3. **Execute**
   - Real-time progress tracking
   - Per-phase status
   - Operation-level progress
   - Cancel option

### Meta Operations (`/admin/meta-operations`)

Manage the meta cluster:
- Meta cluster health status
- Current leader with Raft term
- Commit index
- Peer list with health status
- Distribution configuration management

### Data Operations (`/admin/data-operations`)

Manage data nodes:
- Active/Retiring/Inactive counts
- Bulk operations (flush all, compact all)
- Per-node operations table
- Individual flush, compact, cache clear buttons

### Router Operations (`/admin/router-operations`)

Monitor router nodes:
- Active/Inactive router counts
- Router function overview
- Router node list with status

### Events (`/admin/events`)

Real-time event monitoring:
- WebSocket connection status
- Event type filtering
- Event list with timestamps
- Event detail panel
- Auto-reconnect on disconnect

### Query Console (`/admin/query`) ⭐ NEW

Interactive SQL query interface:
- **Monaco Editor**: Professional SQL editor with syntax highlighting
- **Catalog Selection**: Query any catalog in your cluster
- **Execute Queries**: Run SQL and see results in table format
- **Export Results**: Download as CSV or JSON
- **EXPLAIN/ANALYZE**: View query plans and optimization details
- **Query History**: Last 100 queries with status (success/error)
- **Saved Queries**: Bookmark frequently used queries
- **Keyboard Shortcuts**:
  - `Ctrl+Enter` / `Cmd+Enter` - Run query
  - `Ctrl+Shift+E` / `Cmd+Shift+E` - Explain query
- **SQL Templates**: Common patterns (SHOW TABLES, time bucketing, etc.)

**Features:**
- Column schema display (name + data type)
- Execution time tracking
- Row count display
- Copy to clipboard
- Multi-tab results (Results, Explain)
- Search history
- LocalStorage persistence

### Storage Browser (`/admin/storage`) ⭐ NEW

File system explorer for Parquet storage:
- **Tree Navigation**: Browse Catalog → Schema → Table → Partition → File hierarchy
- **Storage Overview**: Backend type (S3/Local), total size, file count
- **Statistics Cards**: Per-level metrics (schemas, tables, partitions, size)
- **Compaction Management**:
  - Trigger at catalog, schema, or table level
  - View compaction level distribution (L0, L1, L2+)
  - Visual progress bars for file distribution
- **Partition Management**:
  - View all date partitions for a table
  - Delete old partitions with confirmation
  - See files per partition
- **Parquet Inspector**:
  - File metadata (row count, row groups, compression)
  - Column schema with data types
  - Min/Max statistics per column
  - Null count tracking
- **Search**: Filter catalogs by name

**Use Cases:**
- Monitor storage growth and distribution
- Clean up old partitions manually
- Trigger compaction for tables with many L0 files
- Inspect Parquet file quality (compression, row groups)
- Verify schema after writes

## API Reference

All API endpoints are under `/admin/api/v1/`.

### Cluster

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/status` | GET | Cluster health, node counts, warnings |

### Nodes

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/nodes` | GET | List nodes (filterable by role, state, mode) |
| `/nodes/:id` | GET | Get node details |
| `/nodes/:id/retire` | POST | Start node retirement |
| `/nodes/:id/retire/status` | GET | Get retirement progress |
| `/nodes/:id/retire/cancel` | POST | Cancel retirement |
| `/nodes/:id/flush` | POST | Flush node memtable |
| `/nodes/:id/compact` | POST | Trigger compaction |
| `/nodes/:id/cache/clear` | POST | Clear node cache |

### Tables

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tables` | GET | List tables (paginated, filterable) |
| `/tables/:key/reassign` | POST | Reassign table to new node |

### Scale-In

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/scale-in/recommend` | GET | Get node recommendations |
| `/scale-in/plan` | POST | Generate scale-in plan |
| `/scale-in/execute` | POST | Execute scale-in plan |
| `/scale-in/status/:id` | GET | Get execution status |
| `/scale-in/cancel/:id` | POST | Cancel scale-in |

### Events

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/events` | GET | List historical events |
| `/events/ws` | WebSocket | Real-time event stream |

### Meta

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/meta/status` | GET | Meta cluster status |
| `/distribution` | GET | List distribution configs |
| `/distribution` | POST | Set distribution config |

### Query Console ⭐ NEW

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/query/execute` | POST | Execute SQL query |
| `/query/explain/:catalog` | GET | Get query execution plan |
| `/query/analyze/:catalog` | GET | Analyze query (partitions, files) |
| `/query/catalogs` | GET | List available catalogs |
| `/query/schemas/:catalog` | GET | List schemas in catalog |
| `/query/columns/:catalog/:schema/:table` | GET | Get table columns |
| `/query/saved` | GET | List saved queries (optional) |
| `/query/saved` | POST | Save a query (optional) |
| `/query/saved/:id` | DELETE | Delete saved query (optional) |
| `/query/history` | GET | Get query history (optional) |

### Storage Browser ⭐ NEW

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/storage/info` | GET | Storage backend information |
| `/storage/catalogs` | GET | List catalogs with statistics |
| `/storage/catalogs/:catalog/schemas` | GET | List schemas with statistics |
| `/storage/catalogs/:catalog/schemas/:schema/tables/:table` | GET | Get table storage details |
| `/storage/file?path=...` | GET | Get Parquet file details |
| `/storage/catalogs/:catalog/schemas/:schema/tables/:table/partitions/:date` | DELETE | Delete partition |
| `/storage/compact/:catalog[/:schema[/:table]]` | POST | Trigger compaction |

**Note**: Saved queries and history endpoints are optional. The frontend uses LocalStorage as a fallback if these endpoints are not implemented.

## Configuration

### Admin Server Config (`admin.toml`)

```toml
[admin]
http_host = "127.0.0.1"
http_port = 18080
meta_endpoints = ["127.0.0.1:6001", "127.0.0.1:6101", "127.0.0.1:6201"]
```

### Vite Config (`vite.config.ts`)

Key settings:
- `base: '/admin/'` - Base path for all assets
- `server.proxy` - API proxy for development
- `build.rollupOptions.manualChunks` - Code splitting

### Environment Variables

For development, you can create `.env.local`:

```env
# API base URL (optional, defaults to /admin/api/v1)
VITE_API_BASE=/admin/api/v1
```

## Troubleshooting

### Common Issues

#### "Failed to connect to Meta" error

**Cause**: Admin server can't reach meta nodes.

**Solution**:
1. Ensure meta nodes are running: `curl http://localhost:6000/health`
2. Check meta_endpoints in admin.toml use gRPC ports (6001, 6101, 6201)
3. Don't use `http://` prefix for meta_endpoints

#### Assets return 404

**Cause**: Frontend not built or wrong base path.

**Solution**:
```bash
cd admin-ui
npm run build
# Verify dist/ exists with assets/
ls -la dist/
```

#### Node.js version error

**Cause**: Node.js < 18 doesn't support required features.

**Solution**:
```bash
npm install -g n
n 20
hash -r  # Refresh shell path
node --version  # Should be v20.x
```

#### WebSocket connection fails

**Cause**: WebSocket endpoint not available or blocked.

**Solution**:
1. Check admin server is running
2. Check browser console for CORS errors
3. Ensure `/admin/api/v1/events/ws` endpoint is accessible

#### Blank page after build

**Cause**: JavaScript errors or wrong base path.

**Solution**:
1. Open browser DevTools (F12) → Console tab
2. Check for errors
3. Verify `vite.config.ts` has `base: '/admin/'`
4. Rebuild: `npm run build`

### Debug Tips

1. **Check API directly**:
   ```bash
   curl http://localhost:18080/admin/api/v1/cluster/status
   ```

2. **Check static file serving**:
   ```bash
   curl -I http://localhost:18080/admin
   curl -I http://localhost:18080/admin/assets/index-*.js
   ```

3. **View admin server logs**: Watch the terminal where admin server is running

4. **Enable React Query DevTools**: Uncomment in `main.tsx` for debugging queries

## Documentation

Comprehensive guides for the new features:

- **📘 User Guide**: `/docs/admin-ui-query-storage-guide.md`
  - Complete feature documentation
  - Step-by-step tutorials
  - Best practices
  - Troubleshooting
  - Backend API specifications

- **📋 Quick Reference**: `/docs/admin-ui-quick-reference.md`
  - Cheat sheet for common tasks
  - Keyboard shortcuts
  - SQL templates
  - API endpoints summary
  - Performance tips

## Dependencies

New dependencies added in v1.0:

```json
{
  "@monaco-editor/react": "^4.6.0"  // SQL Editor component
}
```

Total bundle size impact:
- Monaco Editor: ~2.5 MB (lazy-loaded only on Query Console page)
- No impact on other pages

## License

Part of the MynaDB project.
