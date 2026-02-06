/**
 * MynaDB Dashboard - Main Application
 * Orchestrates data fetching, chart updates, and UI interactions
 */

// Application state
const App = {
    refreshInterval: null,
    refreshRate: 30000, // 30 seconds
    isConnected: false,
    selectedHost: 'all',
    timeRangeMinutes: 15, // Default 15 minutes
    bucketSecs: 30, // Default 30 seconds
    currentData: null,
    timeSeriesData: null,

    /**
     * Initialize the dashboard
     */
    async init() {
        console.log('Initializing MynaDB Dashboard...');

        // Initialize chart defaults
        Charts.init();

        // Setup event listeners
        this.setupEventListeners();

        // Load saved theme
        this.loadTheme();

        // Initial data load
        await this.refresh();

        // Start auto-refresh
        this.startAutoRefresh();

        console.log('Dashboard initialized successfully!');
    },

    /**
     * Setup UI event listeners
     */
    setupEventListeners() {
        // Host filter
        const hostFilter = document.getElementById('hostFilter');
        if (hostFilter) {
            hostFilter.addEventListener('change', (e) => {
                this.selectedHost = e.target.value;
                if (this.timeSeriesData) {
                    Charts.updateLineChart(this.timeSeriesData, this.selectedHost);
                }
            });
        }

        // Refresh rate selector
        const refreshRate = document.getElementById('refreshRate');
        if (refreshRate) {
            refreshRate.addEventListener('change', (e) => {
                this.refreshRate = parseInt(e.target.value) * 1000;
                this.startAutoRefresh();
                showToast(`Refresh rate: ${e.target.value}s`, 'success');
            });
        }

        // Time range selector
        const timeRange = document.getElementById('timeRange');
        if (timeRange) {
            timeRange.addEventListener('change', (e) => {
                this.timeRangeMinutes = parseInt(e.target.value);
                this.refreshTimeSeries();
                showToast(`Time range: ${e.target.options[e.target.selectedIndex].text}`, 'success');
            });
        }

        // Bucket size selector
        const bucketSize = document.getElementById('bucketSize');
        if (bucketSize) {
            bucketSize.addEventListener('change', (e) => {
                this.bucketSecs = parseInt(e.target.value);
                this.refreshTimeSeries();
                showToast(`Bucket size: ${e.target.options[e.target.selectedIndex].text}`, 'success');
            });
        }

        // Manual refresh button
        const refreshBtn = document.getElementById('refreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refresh());
        }

        // Theme toggle
        const themeBtn = document.getElementById('themeToggle');
        if (themeBtn) {
            themeBtn.addEventListener('click', () => this.toggleTheme());
        }

        // Export buttons
        document.getElementById('exportCSV')?.addEventListener('click', () => {
            if (this.currentData) Export.toCSV(this.currentData);
        });

        document.getElementById('exportJSON')?.addEventListener('click', () => {
            if (this.currentData) Export.toJSON(this.currentData);
        });

        document.getElementById('exportPNG')?.addEventListener('click', () => {
            Export.toPNG('barChart', 'mynadb_cpu_metrics.png');
        });

        document.getElementById('copyData')?.addEventListener('click', () => {
            if (this.currentData) Export.copyToClipboard(this.currentData);
        });
    },

    /**
     * Refresh dashboard data
     */
    async refresh() {
        this.updateStatus('loading');

        try {
            // Check server health
            const isHealthy = await API.checkHealth();
            if (!isHealthy) {
                throw new Error('Server not responding');
            }

            // Fetch aggregated metrics
            const rawMetrics = await API.getAggregatedMetrics();

            if (!rawMetrics || rawMetrics.length === 0) {
                // Try to write sample data if empty
                this.updateStatus('disconnected');
                this.showNoData();
                return;
            }

            // Process data
            this.currentData = Charts.processData(rawMetrics);

            // Fetch time series with configured bucket size and time range
            this.timeSeriesData = await API.getTimeSeries(null, this.bucketSecs, this.timeRangeMinutes);

            // Update host filter options
            this.updateHostFilter();

            // Update all charts
            Charts.updateAll(this.currentData, this.timeSeriesData, this.selectedHost);

            // Update connection status
            this.updateStatus('connected');

            // Update last refresh time
            this.updateLastRefresh();

        } catch (error) {
            console.error('Refresh error:', error);
            this.updateStatus('disconnected');
            showToast(`Error: ${error.message}`, 'error');
        }
    },

    /**
     * Refresh only the time series chart (for time range/bucket changes)
     */
    async refreshTimeSeries() {
        try {
            this.timeSeriesData = await API.getTimeSeries(
                this.selectedHost === 'all' ? null : this.selectedHost,
                this.bucketSecs,
                this.timeRangeMinutes
            );

            if (this.timeSeriesData && this.timeSeriesData.length > 0) {
                Charts.updateLineChart(this.timeSeriesData, this.selectedHost);
            }
        } catch (error) {
            console.error('Time series refresh error:', error);
            showToast('Failed to refresh time series', 'error');
        }
    },

    /**
     * Update host filter dropdown
     */
    updateHostFilter() {
        const hostFilter = document.getElementById('hostFilter');
        if (!hostFilter || !this.currentData) return;

        const currentValue = hostFilter.value;

        hostFilter.innerHTML = '<option value="all">All Hosts</option>';
        this.currentData.hosts.forEach(host => {
            const option = document.createElement('option');
            option.value = host;
            option.textContent = host;
            hostFilter.appendChild(option);
        });

        // Restore selection if still valid
        if (this.currentData.hosts.includes(currentValue)) {
            hostFilter.value = currentValue;
        }
    },

    /**
     * Update connection status indicator
     */
    updateStatus(status) {
        const dot = document.getElementById('statusDot');
        const text = document.getElementById('statusText');

        if (dot && text) {
            dot.className = `status-dot ${status === 'connected' ? 'connected' : 'disconnected'}`;
            text.textContent = status === 'connected' ? 'Connected' : status === 'loading' ? 'Loading...' : 'Disconnected';
        }

        this.isConnected = status === 'connected';
    },

    /**
     * Update last refresh timestamp
     */
    updateLastRefresh() {
        const el = document.getElementById('lastRefresh');
        if (el) {
            el.textContent = new Date().toLocaleTimeString();
        }
    },

    /**
     * Show no data message
     */
    showNoData() {
        const tbody = document.getElementById('summaryTableBody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" style="text-align: center; padding: 30px;">
                        No data available. Write some data to MynaDB first.<br>
                        <code style="font-size: 0.8rem; color: #888;">
                            curl -X POST http://localhost:7000/write/testdb -d 'cpu,host=server01,service=prod user=50.0,idle=40.0,system=10.0'
                        </code>
                    </td>
                </tr>
            `;
        }
    },

    /**
     * Start auto-refresh timer
     */
    startAutoRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }

        if (this.refreshRate > 0) {
            this.refreshInterval = setInterval(() => this.refresh(), this.refreshRate);
        }
    },

    /**
     * Stop auto-refresh
     */
    stopAutoRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    },

    /**
     * Toggle dark/light theme
     */
    toggleTheme() {
        document.body.classList.toggle('light-theme');
        const isLight = document.body.classList.contains('light-theme');
        localStorage.setItem('mynadb-theme', isLight ? 'light' : 'dark');

        // Update theme button icon
        const btn = document.getElementById('themeToggle');
        if (btn) {
            btn.textContent = isLight ? '🌙' : '☀️';
        }

        // Re-initialize chart colors
        Charts.init();

        showToast(`Theme: ${isLight ? 'Light' : 'Dark'}`, 'success');
    },

    /**
     * Load saved theme preference
     */
    loadTheme() {
        const saved = localStorage.getItem('mynadb-theme');
        if (saved === 'light') {
            document.body.classList.add('light-theme');
            const btn = document.getElementById('themeToggle');
            if (btn) btn.textContent = '🌙';
        }
    }
};

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => App.init());

// Export App globally
window.App = App;
