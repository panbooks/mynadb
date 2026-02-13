# MynaDB Documentation Website

A modern, responsive documentation website for MynaDB - The Voice of Your Machines.

## Structure

```
website/
├── index.html              # Landing page
├── css/
│   └── style.css           # Main stylesheet
├── js/
│   └── main.js             # JavaScript functionality
├── images/                 # Image assets
└── docs/
    ├── index.html          # Documentation home
    ├── quickstart.html     # Quick start guide
    ├── architecture.html   # Architecture overview
    ├── configuration.html  # Configuration reference
    ├── api.html            # HTTP API reference
    ├── flight-api.html     # Arrow Flight API reference
    ├── examples.html       # Query examples
    ├── guides.html         # All guides
    ├── sql-queries.html    # SQL query guide
    ├── line-protocol.html  # InfluxDB Line Protocol guide
    ├── data-model.html     # Data model (catalog.schema.table)
    ├── storage.html        # Storage layer details
    ├── compaction.html     # LSM compaction guide
    ├── retention.html      # Retention policies
    ├── high-availability.html  # HA and replication
    ├── installation.html   # Installation guide
    ├── caching.html        # Multi-level caching
    ├── mysql-protocol.html # MySQL protocol support
    ├── object-store.html   # S3/MinIO/OSS support
    ├── otlp.html           # OpenTelemetry (OTLP) ingestion
    ├── iceberg.html        # Apache Iceberg integration
    ├── grafana.html        # Grafana integration
    ├── continuous-aggregates.html # Continuous aggregates & downsampling
    ├── streaming.html      # Streaming queries (WebSocket/SSE)
    ├── security.html       # Authentication, RBAC & security
    ├── forecasting.html    # Forecasting & geospatial functions
    ├── admin-console.html  # Admin console
    ├── admin-guide.html    # Admin dashboard guide
    ├── admin-operations.html # Cluster operations guide
    ├── monitoring.html     # Monitoring & observability
    ├── cluster-getting-started.html # Cluster getting started guide
    ├── cluster-architecture.html    # Cluster architecture deep dive
    ├── cluster-scaling.html         # Cluster scaling operations
    ├── faq.html            # Frequently asked questions
    └── troubleshooting.html # Troubleshooting guide
```

## Running Locally

### Option 1: Python HTTP Server

```bash
cd website
python3 -m http.server 8080
```

Then open http://localhost:8080

### Option 2: Node.js

```bash
npx serve website
```

### Option 3: Nginx/Apache

Configure your web server to serve the `website/` directory.

## Features

- **Modern Design**: Dark theme with gradient accents
- **Responsive**: Works on desktop, tablet, and mobile
- **Interactive**: Copy-to-clipboard, smooth scrolling
- **Comprehensive**: Quick start, architecture, API reference, examples
- **Search-friendly**: Proper meta tags and structure

## Customization

### Colors

Edit CSS variables in `css/style.css`:

```css
:root {
  --primary: #6366f1;      /* Primary brand color */
  --secondary: #0ea5e9;    /* Secondary color */
  --accent: #10b981;       /* Accent/success color */
  --bg-primary: #0f172a;   /* Main background */
  --text-primary: #f8fafc; /* Main text color */
}
```

### Adding Pages

1. Copy an existing page as a template
2. Update the content
3. Add navigation links in the sidebar
4. Update cross-references

## Pages

| Page | Description |
|------|-------------|
| Landing | Hero section, features, quick start |
| Documentation Index | Overview and navigation |
| Quick Start | 5-minute getting started guide |
| Architecture | System design and components |
| Configuration | All config.toml options |
| API Reference | HTTP endpoints documentation |
| Flight API | Arrow Flight gRPC API |
| Examples | Ready-to-use SQL queries |
| Guides | Step-by-step tutorials |
| SQL Queries | Complete SQL reference |
| Line Protocol | InfluxDB line protocol syntax |
| Data Model | Catalog.Schema.Table structure |
| Storage | Parquet storage layer |
| Compaction | LSM multi-level compaction |
| Retention | Data retention policies |
| High Availability | Replication and failover |
| Installation | Build and deployment |
| Caching | Metadata, query, and file caching |
| MySQL Protocol | MySQL wire protocol support |
| Object Store | S3, MinIO, Aliyun OSS |
| OpenTelemetry (OTLP) | Native OTLP ingestion for metrics, logs, traces |
| Apache Iceberg | REST Catalog API for multi-engine analytics |
| Grafana Integration | InfluxDB-compatible datasource and dashboard |
| Continuous Aggregates | Materialized views with auto-refresh |
| Streaming Queries | Real-time subscriptions via WebSocket/SSE |
| Security & RBAC | Authentication, roles, TLS, audit logging |
| Forecasting & Geospatial | Built-in forecasting and geospatial UDFs |
| Admin Console | Web-based SQL editor and cluster management |
| Admin Dashboard Guide | Getting started tour, navigation, and key concepts |
| Cluster Operations | Node management, tables, scale-in, events, and debug tools |
| Monitoring | Prometheus metrics and observability |
| Cluster Getting Started | Step-by-step cluster setup guide |
| Cluster Architecture | Meta, Router, Data node internals |
| Cluster Scaling | Scale-out, scale-in, and failover |
| FAQ | Frequently asked questions |
| Troubleshooting | Common issues and solutions |

## Technologies

- **HTML5** - Semantic markup
- **CSS3** - Custom properties, flexbox, grid
- **JavaScript** - Vanilla JS for interactivity
- **Fonts** - Inter (sans) + JetBrains Mono (code)

## Browser Support

- Chrome/Edge 80+
- Firefox 75+
- Safari 13+

## License

Same as MynaDB main project.
