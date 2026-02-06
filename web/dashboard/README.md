# MynaDB Interactive Dashboard

An AI-generated interactive dashboard for visualizing MynaDB CPU metrics.

## Features

- **Real-time Data Fetching**: Auto-refreshes from MynaDB API
- **Multiple Chart Types**: Bar, stacked bar, line, and doughnut charts
- **Time Series Visualization**: CPU trends over time
- **Host Filtering**: Filter metrics by specific host
- **Alerting Thresholds**: Visual indicators for warning (>60%) and critical (>80%) CPU usage
- **Dark/Light Theme**: Toggle between themes with persistence
- **Export Options**: CSV, JSON, PNG export and clipboard copy
- **Responsive Design**: Works on desktop and mobile

## Quick Start

1. **Start MynaDB server:**
   ```bash
   ./target/release/mynadb
   ```

2. **Write some test data:**
   ```bash
   curl -X POST http://localhost:7000/write/testdb -d \
     'cpu,host=web01,region=us-east,service=prod user=45.2,idle=42.1,system=12.7'
   ```

3. **Start the dashboard server:**
   ```bash
   cd web/dashboard
   python3 -m http.server 8080
   ```

4. **Open in browser:**
   ```
   http://localhost:8080
   ```

## File Structure

```
web/dashboard/
├── index.html          # Main dashboard HTML
├── css/
│   └── styles.css      # Dark/light theme styles
├── js/
│   ├── api.js          # MynaDB API client
│   ├── charts.js       # Chart.js configurations
│   ├── export.js       # Export functionality
│   └── app.js          # Main application logic
└── README.md           # This file
```

## API Endpoints Used

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Server health check |
| `/query/:db` | POST | SQL query execution |
| `/stats` | GET | Cache statistics |

## Configuration

Edit `js/api.js` to change:
- `baseUrl`: MynaDB server URL (default: `http://localhost:7000`)
- `database`: Database name (default: `testdb`)
- `schema`: Schema name (default: `prod`)
- `table`: Table name (default: `cpu`)

Edit `js/charts.js` to change alerting thresholds:
```javascript
const THRESHOLDS = {
    critical: 80,  // Red alert
    warning: 60    // Yellow warning
};
```

## Browser Support

- Chrome 80+
- Firefox 75+
- Safari 13+
- Edge 80+

## Credits

Generated with Claude Code (claude.ai/code)
