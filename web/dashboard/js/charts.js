/**
 * MynaDB Dashboard Charts
 * Handles chart creation and updates using Chart.js
 */

// Alerting thresholds
const THRESHOLDS = {
    critical: 80,
    warning: 60
};

// Color schemes
const COLORS = {
    user: {
        bg: 'rgba(255, 107, 107, 0.7)',
        border: 'rgba(255, 107, 107, 1)'
    },
    idle: {
        bg: 'rgba(107, 203, 119, 0.7)',
        border: 'rgba(107, 203, 119, 1)'
    },
    system: {
        bg: 'rgba(0, 217, 255, 0.7)',
        border: 'rgba(0, 217, 255, 1)'
    },
    warning: {
        bg: 'rgba(255, 217, 61, 0.7)',
        border: 'rgba(255, 217, 61, 1)'
    }
};

// Chart instances storage
const charts = {
    bar: null,
    stacked: null,
    line: null,
    doughnuts: {}
};

// Current data storage
let currentData = {
    hosts: [],
    regions: [],
    avgUser: [],
    avgIdle: [],
    avgSystem: [],
    timeSeries: []
};

/**
 * Get status level based on CPU usage
 */
function getStatusLevel(value) {
    if (value >= THRESHOLDS.critical) return 'critical';
    if (value >= THRESHOLDS.warning) return 'warning';
    return 'normal';
}

/**
 * Get status text based on level
 */
function getStatusText(level) {
    switch (level) {
        case 'critical': return 'Critical';
        case 'warning': return 'Warning';
        default: return 'Normal';
    }
}

/**
 * Initialize Chart.js defaults
 */
function initChartDefaults() {
    Chart.defaults.color = getComputedStyle(document.documentElement)
        .getPropertyValue('--text-primary').trim() || '#e0e0e0';
    Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
}

/**
 * Create or update bar chart
 */
function updateBarChart(data) {
    const ctx = document.getElementById('barChart');
    if (!ctx) return;

    const labels = data.hosts.map((h, i) => `${h} (${data.regions[i]})`);

    if (charts.bar) {
        charts.bar.data.labels = labels;
        charts.bar.data.datasets[0].data = data.avgUser;
        charts.bar.data.datasets[1].data = data.avgIdle;
        charts.bar.data.datasets[2].data = data.avgSystem;
        charts.bar.update('none');
    } else {
        charts.bar = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'User %',
                        data: data.avgUser,
                        backgroundColor: COLORS.user.bg,
                        borderColor: COLORS.user.border,
                        borderWidth: 2
                    },
                    {
                        label: 'Idle %',
                        data: data.avgIdle,
                        backgroundColor: COLORS.idle.bg,
                        borderColor: COLORS.idle.border,
                        borderWidth: 2
                    },
                    {
                        label: 'System %',
                        data: data.avgSystem,
                        backgroundColor: COLORS.system.bg,
                        borderColor: COLORS.system.border,
                        borderWidth: 2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(2)}%`
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        title: { display: true, text: 'CPU %' },
                        grid: { color: 'rgba(255, 255, 255, 0.1)' }
                    },
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.1)' }
                    }
                }
            }
        });
    }
}

/**
 * Create or update stacked bar chart
 */
function updateStackedChart(data) {
    const ctx = document.getElementById('stackedChart');
    if (!ctx) return;

    const labels = data.hosts.map((h, i) => `${h} (${data.regions[i]})`);

    if (charts.stacked) {
        charts.stacked.data.labels = labels;
        charts.stacked.data.datasets[0].data = data.avgUser;
        charts.stacked.data.datasets[1].data = data.avgSystem;
        charts.stacked.data.datasets[2].data = data.avgIdle;
        charts.stacked.update('none');
    } else {
        charts.stacked = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'User %',
                        data: data.avgUser,
                        backgroundColor: COLORS.user.bg,
                        borderColor: COLORS.user.border,
                        borderWidth: 1
                    },
                    {
                        label: 'System %',
                        data: data.avgSystem,
                        backgroundColor: COLORS.system.bg,
                        borderColor: COLORS.system.border,
                        borderWidth: 1
                    },
                    {
                        label: 'Idle %',
                        data: data.avgIdle,
                        backgroundColor: COLORS.idle.bg,
                        borderColor: COLORS.idle.border,
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(2)}%`
                        }
                    }
                },
                scales: {
                    x: { stacked: true, grid: { color: 'rgba(255, 255, 255, 0.1)' } },
                    y: {
                        stacked: true,
                        max: 100,
                        title: { display: true, text: 'CPU %' },
                        grid: { color: 'rgba(255, 255, 255, 0.1)' }
                    }
                }
            }
        });
    }
}

/**
 * Create or update time series line chart
 */
function updateLineChart(timeSeries, selectedHost = 'all') {
    const ctx = document.getElementById('lineChart');
    if (!ctx) return;

    // Process time series data
    let filteredData = timeSeries;
    if (selectedHost !== 'all') {
        filteredData = timeSeries.filter(d => d.host === selectedHost);
    }

    // Group by host
    const hostData = {};
    filteredData.forEach(d => {
        if (!hostData[d.host]) {
            hostData[d.host] = { times: [], values: [] };
        }
        // Use bucket (time_bucket result) or time field
        const timestamp = d.bucket || d.time;
        hostData[d.host].times.push(new Date(timestamp / 1000000)); // Convert nanoseconds to milliseconds
        hostData[d.host].values.push(d.user_cpu);
    });

    // Create datasets
    const datasets = Object.entries(hostData).map(([host, data], index) => {
        const colors = [COLORS.user.border, COLORS.idle.border, COLORS.system.border];
        return {
            label: host,
            data: data.values,
            borderColor: colors[index % colors.length],
            backgroundColor: colors[index % colors.length].replace('1)', '0.1)'),
            tension: 0.4,
            fill: true
        };
    });

    // Get all unique timestamps sorted (use bucket or time)
    const allTimes = [...new Set(filteredData.map(d => d.bucket || d.time))].sort();
    const labels = allTimes.map(t => {
        const date = new Date(t / 1000000);
        return date.toLocaleTimeString();
    });

    if (charts.line) {
        charts.line.data.labels = labels;
        charts.line.data.datasets = datasets;
        charts.line.update('none');
    } else {
        charts.line = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: ctx => `${ctx.dataset.label}: ${ctx.raw.toFixed(2)}%`
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        title: { display: true, text: 'CPU User %' },
                        grid: { color: 'rgba(255, 255, 255, 0.1)' }
                    },
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.1)' }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
    }
}

/**
 * Create or update doughnut charts for each host
 */
function updateDoughnutCharts(data) {
    const doughnutOptions = {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
            legend: {
                position: 'bottom',
                labels: { padding: 10, usePointStyle: true, font: { size: 11 } }
            },
            tooltip: {
                callbacks: {
                    label: ctx => `${ctx.label}: ${ctx.raw.toFixed(2)}%`
                }
            }
        },
        cutout: '65%'
    };

    data.hosts.forEach((host, index) => {
        const canvasId = `doughnut${index + 1}`;
        const ctx = document.getElementById(canvasId);
        if (!ctx) return;

        const chartData = {
            labels: ['User', 'Idle', 'System'],
            datasets: [{
                data: [data.avgUser[index], data.avgIdle[index], data.avgSystem[index]],
                backgroundColor: [COLORS.user.bg, COLORS.idle.bg, COLORS.system.bg],
                borderColor: [COLORS.user.border, COLORS.idle.border, COLORS.system.border],
                borderWidth: 2
            }]
        };

        if (charts.doughnuts[canvasId]) {
            charts.doughnuts[canvasId].data = chartData;
            charts.doughnuts[canvasId].update('none');
        } else {
            charts.doughnuts[canvasId] = new Chart(ctx, {
                type: 'doughnut',
                data: chartData,
                options: doughnutOptions
            });
        }

        // Update status indicator
        const statusEl = document.getElementById(`status${index + 1}`);
        if (statusEl) {
            const level = getStatusLevel(data.avgUser[index]);
            statusEl.className = `host-status ${level}`;
            statusEl.textContent = getStatusText(level);
        }

        // Update metric value
        const valueEl = document.getElementById(`value${index + 1}`);
        if (valueEl) {
            const level = getStatusLevel(data.avgUser[index]);
            valueEl.className = `metric-value ${level}`;
            valueEl.textContent = `${data.avgUser[index].toFixed(1)}%`;
        }

        // Update host title
        const titleEl = document.getElementById(`hostTitle${index + 1}`);
        if (titleEl) {
            titleEl.textContent = `${host} (${data.regions[index]})`;
        }
    });
}

/**
 * Update summary table
 */
function updateTable(data) {
    const tbody = document.getElementById('summaryTableBody');
    if (!tbody) return;

    tbody.innerHTML = data.hosts.map((host, i) => {
        const level = getStatusLevel(data.avgUser[i]);
        return `
            <tr>
                <td>${host}</td>
                <td>${data.regions[i]}</td>
                <td class="${level}">${data.avgUser[i].toFixed(2)}</td>
                <td>${data.avgIdle[i].toFixed(2)}</td>
                <td>${data.avgSystem[i].toFixed(2)}</td>
                <td><span class="badge ${level}">${getStatusText(level)}</span></td>
            </tr>
        `;
    }).join('');
}

/**
 * Update all charts with new data
 */
function updateAllCharts(data, timeSeries = null, selectedHost = 'all') {
    currentData = data;
    if (timeSeries) currentData.timeSeries = timeSeries;

    updateBarChart(data);
    updateStackedChart(data);
    updateDoughnutCharts(data);
    updateTable(data);

    if (timeSeries && timeSeries.length > 0) {
        updateLineChart(timeSeries, selectedHost);
    }
}

/**
 * Process raw API response into chart data format
 */
function processMetricsData(rawData) {
    return {
        hosts: rawData.map(d => d.host),
        regions: rawData.map(d => d.region),
        avgUser: rawData.map(d => d.avg_user || 0),
        avgIdle: rawData.map(d => d.avg_idle || 0),
        avgSystem: rawData.map(d => d.avg_system || 0)
    };
}

// Export functions
window.Charts = {
    init: initChartDefaults,
    updateAll: updateAllCharts,
    processData: processMetricsData,
    updateLineChart,
    getStatusLevel,
    THRESHOLDS,
    COLORS
};
