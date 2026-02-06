/**
 * Export functionality for MynaDB Dashboard
 * Supports CSV and PNG export
 */

const Export = {
    /**
     * Export data to CSV format
     * @param {Object} data - Chart data object
     * @param {string} filename - Output filename
     */
    toCSV(data, filename = 'mynadb_metrics.csv') {
        const headers = ['Host', 'Region', 'Avg User %', 'Avg Idle %', 'Avg System %', 'Status'];
        const rows = data.hosts.map((host, i) => {
            const level = Charts.getStatusLevel(data.avgUser[i]);
            return [
                host,
                data.regions[i],
                data.avgUser[i].toFixed(2),
                data.avgIdle[i].toFixed(2),
                data.avgSystem[i].toFixed(2),
                level.charAt(0).toUpperCase() + level.slice(1)
            ];
        });

        const csvContent = [
            headers.join(','),
            ...rows.map(row => row.join(','))
        ].join('\n');

        this.downloadFile(csvContent, filename, 'text/csv');
        showToast('CSV exported successfully!', 'success');
    },

    /**
     * Export time series data to CSV
     * @param {Array} timeSeries - Time series data
     * @param {string} filename - Output filename
     */
    timeSeriesCSV(timeSeries, filename = 'mynadb_timeseries.csv') {
        if (!timeSeries || timeSeries.length === 0) {
            showToast('No time series data available', 'warning');
            return;
        }

        const headers = ['Timestamp', 'Host', 'User %', 'Idle %', 'System %'];
        const rows = timeSeries.map(d => [
            new Date(d.time / 1000000).toISOString(),
            d.host,
            (d.user_cpu || 0).toFixed(2),
            (d.idle || 0).toFixed(2),
            (d.system || 0).toFixed(2)
        ]);

        const csvContent = [
            headers.join(','),
            ...rows.map(row => row.join(','))
        ].join('\n');

        this.downloadFile(csvContent, filename, 'text/csv');
        showToast('Time series CSV exported!', 'success');
    },

    /**
     * Export chart to PNG image
     * @param {string} chartId - Canvas element ID
     * @param {string} filename - Output filename
     */
    toPNG(chartId, filename = 'mynadb_chart.png') {
        const canvas = document.getElementById(chartId);
        if (!canvas) {
            showToast('Chart not found', 'error');
            return;
        }

        // Create a temporary canvas with white background
        const tempCanvas = document.createElement('canvas');
        const ctx = tempCanvas.getContext('2d');
        tempCanvas.width = canvas.width;
        tempCanvas.height = canvas.height;

        // Fill with background color
        ctx.fillStyle = '#1a1a2e';
        ctx.fillRect(0, 0, tempCanvas.width, tempCanvas.height);

        // Draw the chart
        ctx.drawImage(canvas, 0, 0);

        // Convert to PNG and download
        const link = document.createElement('a');
        link.download = filename;
        link.href = tempCanvas.toDataURL('image/png');
        link.click();

        showToast('Chart exported as PNG!', 'success');
    },

    /**
     * Export all charts to PNG
     */
    allChartsToPNG() {
        const chartIds = ['barChart', 'stackedChart', 'lineChart'];
        chartIds.forEach((id, index) => {
            setTimeout(() => {
                this.toPNG(id, `mynadb_${id}.png`);
            }, index * 500); // Stagger downloads
        });
    },

    /**
     * Export dashboard summary as JSON
     * @param {Object} data - Dashboard data
     */
    toJSON(data, filename = 'mynadb_metrics.json') {
        const exportData = {
            exportedAt: new Date().toISOString(),
            thresholds: Charts.THRESHOLDS,
            metrics: data.hosts.map((host, i) => ({
                host,
                region: data.regions[i],
                avgUser: data.avgUser[i],
                avgIdle: data.avgIdle[i],
                avgSystem: data.avgSystem[i],
                status: Charts.getStatusLevel(data.avgUser[i])
            }))
        };

        const jsonContent = JSON.stringify(exportData, null, 2);
        this.downloadFile(jsonContent, filename, 'application/json');
        showToast('JSON exported successfully!', 'success');
    },

    /**
     * Helper to download a file
     */
    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    },

    /**
     * Copy data to clipboard
     */
    async copyToClipboard(data) {
        const text = data.hosts.map((host, i) =>
            `${host} (${data.regions[i]}): User=${data.avgUser[i].toFixed(1)}%, Idle=${data.avgIdle[i].toFixed(1)}%, System=${data.avgSystem[i].toFixed(1)}%`
        ).join('\n');

        try {
            await navigator.clipboard.writeText(text);
            showToast('Copied to clipboard!', 'success');
        } catch (err) {
            showToast('Failed to copy', 'error');
        }
    }
};

/**
 * Show toast notification
 */
function showToast(message, type = 'success') {
    // Remove existing toasts
    document.querySelectorAll('.toast').forEach(t => t.remove());

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);

    setTimeout(() => toast.remove(), 3000);
}

// Make showToast globally available
window.showToast = showToast;
window.Export = Export;
