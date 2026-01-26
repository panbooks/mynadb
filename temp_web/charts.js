// CPU Metrics Data from MynaDB query
const data = {
    hosts: ['db01', 'web01', 'web02'],
    regions: ['us-east', 'us-east', 'us-west'],
    avgUser: [73.23, 48.77, 28.50],
    avgIdle: [17.60, 38.93, 62.10],
    avgSystem: [9.17, 12.30, 9.40]
};

// Color schemes
const colors = {
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
    }
};

// Chart.js global defaults
Chart.defaults.color = '#e0e0e0';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

// 1. Bar Chart - CPU Usage by Host
const barCtx = document.getElementById('barChart').getContext('2d');
new Chart(barCtx, {
    type: 'bar',
    data: {
        labels: data.hosts.map((h, i) => `${h} (${data.regions[i]})`),
        datasets: [
            {
                label: 'User %',
                data: data.avgUser,
                backgroundColor: colors.user.bg,
                borderColor: colors.user.border,
                borderWidth: 2
            },
            {
                label: 'Idle %',
                data: data.avgIdle,
                backgroundColor: colors.idle.bg,
                borderColor: colors.idle.border,
                borderWidth: 2
            },
            {
                label: 'System %',
                data: data.avgSystem,
                backgroundColor: colors.system.bg,
                borderColor: colors.system.border,
                borderWidth: 2
            }
        ]
    },
    options: {
        responsive: true,
        plugins: {
            legend: {
                position: 'top',
            },
            tooltip: {
                callbacks: {
                    label: function(context) {
                        return `${context.dataset.label}: ${context.raw.toFixed(2)}%`;
                    }
                }
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                max: 100,
                title: {
                    display: true,
                    text: 'CPU %'
                },
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)'
                }
            },
            x: {
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)'
                }
            }
        }
    }
});

// 2. Stacked Bar Chart
const stackedCtx = document.getElementById('stackedChart').getContext('2d');
new Chart(stackedCtx, {
    type: 'bar',
    data: {
        labels: data.hosts.map((h, i) => `${h} (${data.regions[i]})`),
        datasets: [
            {
                label: 'User %',
                data: data.avgUser,
                backgroundColor: colors.user.bg,
                borderColor: colors.user.border,
                borderWidth: 1
            },
            {
                label: 'System %',
                data: data.avgSystem,
                backgroundColor: colors.system.bg,
                borderColor: colors.system.border,
                borderWidth: 1
            },
            {
                label: 'Idle %',
                data: data.avgIdle,
                backgroundColor: colors.idle.bg,
                borderColor: colors.idle.border,
                borderWidth: 1
            }
        ]
    },
    options: {
        responsive: true,
        plugins: {
            legend: {
                position: 'top',
            },
            tooltip: {
                callbacks: {
                    label: function(context) {
                        return `${context.dataset.label}: ${context.raw.toFixed(2)}%`;
                    }
                }
            }
        },
        scales: {
            x: {
                stacked: true,
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)'
                }
            },
            y: {
                stacked: true,
                max: 100,
                title: {
                    display: true,
                    text: 'CPU %'
                },
                grid: {
                    color: 'rgba(255, 255, 255, 0.1)'
                }
            }
        }
    }
});

// 3. Doughnut Charts for each host
const doughnutOptions = {
    responsive: true,
    plugins: {
        legend: {
            position: 'bottom',
            labels: {
                padding: 15,
                usePointStyle: true
            }
        },
        tooltip: {
            callbacks: {
                label: function(context) {
                    return `${context.label}: ${context.raw.toFixed(2)}%`;
                }
            }
        }
    },
    cutout: '60%'
};

// db01
new Chart(document.getElementById('doughnut1').getContext('2d'), {
    type: 'doughnut',
    data: {
        labels: ['User', 'Idle', 'System'],
        datasets: [{
            data: [data.avgUser[0], data.avgIdle[0], data.avgSystem[0]],
            backgroundColor: [colors.user.bg, colors.idle.bg, colors.system.bg],
            borderColor: [colors.user.border, colors.idle.border, colors.system.border],
            borderWidth: 2
        }]
    },
    options: doughnutOptions
});

// web01
new Chart(document.getElementById('doughnut2').getContext('2d'), {
    type: 'doughnut',
    data: {
        labels: ['User', 'Idle', 'System'],
        datasets: [{
            data: [data.avgUser[1], data.avgIdle[1], data.avgSystem[1]],
            backgroundColor: [colors.user.bg, colors.idle.bg, colors.system.bg],
            borderColor: [colors.user.border, colors.idle.border, colors.system.border],
            borderWidth: 2
        }]
    },
    options: doughnutOptions
});

// web02
new Chart(document.getElementById('doughnut3').getContext('2d'), {
    type: 'doughnut',
    data: {
        labels: ['User', 'Idle', 'System'],
        datasets: [{
            data: [data.avgUser[2], data.avgIdle[2], data.avgSystem[2]],
            backgroundColor: [colors.user.bg, colors.idle.bg, colors.system.bg],
            borderColor: [colors.user.border, colors.idle.border, colors.system.border],
            borderWidth: 2
        }]
    },
    options: doughnutOptions
});

console.log('MynaDB CPU Metrics Dashboard loaded successfully!');
console.log('Data:', data);
