# Stats Service Documentation Website

A modern, responsive documentation website for Stats Service, inspired by InfluxDB, GreptimeDB, and DuckDB documentation sites.

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
    ├── examples.html       # Query examples
    ├── guides.html         # All guides
    ├── sql-queries.html    # SQL query guide
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
| Examples | Ready-to-use SQL queries |
| Guides | Step-by-step tutorials |
| SQL Queries | Complete SQL reference |
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

Same as Stats Service main project.
