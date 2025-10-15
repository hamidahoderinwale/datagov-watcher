/**
 * Data Visualization JavaScript
 * Handles all chart rendering and data loading for the data visualization page
 */

// Global variables
let chartInstances = {};
let chartData = {};

// Initialize the page
document.addEventListener('DOMContentLoaded', function() {
    initializeDataVisualization();
});

function initializeDataVisualization() {
    console.log('Initializing data visualization...');
    
    // Load all chart data
    loadAllCharts();
    
    // Set up event listeners
    setupEventListeners();
}

function loadAllCharts() {
    // Load trends chart
    loadTrendsData(30, 'all');
    
    // Load status distribution chart
    loadStatusData('all');
    
    // Load agency distribution chart
    loadAgencyDistributionData('top20');
    
    // Load agencies performance chart
    loadAgenciesData('top10');
    
    // Load changes chart
    loadChangesData(30);
    
    // Load quality chart
    loadQualityData(30);
}

function setupEventListeners() {
    // Time period controls
    const timeControls = document.querySelectorAll('.time-control');
    timeControls.forEach(control => {
        control.addEventListener('change', function() {
            const period = this.value;
            loadTrendsData(period, 'all');
            loadChangesData(period);
            loadQualityData(period);
        });
    });
    
    // Data source controls
    const sourceControls = document.querySelectorAll('.source-control');
    sourceControls.forEach(control => {
        control.addEventListener('change', function() {
            const source = this.value;
            loadTrendsData(30, source);
            loadStatusData(source);
        });
    });
    
    // Agency filter controls
    const agencyControls = document.querySelectorAll('.agency-control');
    agencyControls.forEach(control => {
        control.addEventListener('change', function() {
            const filter = this.value;
            loadAgencyDistributionData(filter);
            loadAgenciesData(filter);
        });
    });
}

// Load trends data
async function loadTrendsData(timePeriod, dataSource) {
    try {
        const response = await fetch(`/api/analytics/trends?period=${timePeriod}&metric=datasets`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.trends = data;
        renderTrendsChart();
    } catch (error) {
        console.error('Error loading trends data:', error);
        // Use fallback data
        chartData.trends = getFallbackTrendsData();
        renderTrendsChart();
    }
}

// Load status data
async function loadStatusData(dataSource) {
    try {
        const response = await fetch(`/api/analytics/status-distribution`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.status = data;
        renderStatusChart();
    } catch (error) {
        console.error('Error loading status data:', error);
        // Use fallback data
        chartData.status = getFallbackStatusData();
        renderStatusChart();
    }
}

// Load agency distribution data
async function loadAgencyDistributionData(agencyFilter) {
    try {
        const response = await fetch(`/api/analytics/agencies?filter=${agencyFilter}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.agencyDistribution = data;
        renderAgencyDistributionChart();
    } catch (error) {
        console.error('Error loading agency distribution data:', error);
        // Use fallback data
        chartData.agencyDistribution = getFallbackAgencyDistributionData();
        renderAgencyDistributionChart();
    }
}

// Load agencies data
async function loadAgenciesData(agencyFilter) {
    try {
        const response = await fetch(`/api/analytics/agencies?filter=${agencyFilter}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.agencies = data;
        renderAgenciesChart();
    } catch (error) {
        console.error('Error loading agencies data:', error);
        // Use fallback data
        chartData.agencies = getFallbackAgenciesData();
        renderAgenciesChart();
    }
}

// Load changes data
async function loadChangesData(timePeriod) {
    try {
        const response = await fetch(`/api/analytics/changes?period=${timePeriod}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.changes = data;
        renderChangesChart();
    } catch (error) {
        console.error('Error loading changes data:', error);
        // Use fallback data
        chartData.changes = getFallbackChangesData();
        renderChangesChart();
    }
}

// Load quality data
async function loadQualityData(timePeriod) {
    try {
        const response = await fetch(`/api/analytics/quality?period=${timePeriod}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        chartData.quality = data;
        renderQualityChart();
    } catch (error) {
        console.error('Error loading quality data:', error);
        // Use fallback data
        chartData.quality = getFallbackQualityData();
        renderQualityChart();
    }
}

// Render trends chart
function renderTrendsChart() {
    const ctx = document.getElementById('trends-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.trends) {
        chartInstances.trends.destroy();
    }
    
    const data = chartData.trends || getFallbackTrendsData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="trends-canvas"></canvas>';
    const canvas = document.getElementById('trends-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.trends = new Chart(chartCtx, {
        type: 'line',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Total Datasets',
                data: data.total || [],
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                tension: 0.4,
                fill: true
            }, {
                label: 'Available Datasets',
                data: data.available || [],
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4,
                fill: true
            }, {
                label: 'Unavailable Datasets',
                data: data.unavailable || [],
                borderColor: '#ef4444',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: '#e5e7eb'
                    }
                },
                x: {
                    grid: {
                        color: '#e5e7eb'
                    }
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('trends', data.stats);
}

// Render status chart
function renderStatusChart() {
    const ctx = document.getElementById('status-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.status) {
        chartInstances.status.destroy();
    }
    
    const data = chartData.status || getFallbackStatusData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="status-canvas"></canvas>';
    const canvas = document.getElementById('status-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.status = new Chart(chartCtx, {
        type: 'doughnut',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Status',
                data: data.datasets?.[0]?.data || [],
                backgroundColor: data.datasets?.[0]?.backgroundColor || [],
                borderColor: data.datasets?.[0]?.borderColor || '#1f2937',
                borderWidth: data.datasets?.[0]?.borderWidth || 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'right'
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('status', data.stats);
}

// Render agency distribution chart
function renderAgencyDistributionChart() {
    const ctx = document.getElementById('agency-distribution-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.agencyDistribution) {
        chartInstances.agencyDistribution.destroy();
    }
    
    const data = chartData.agencyDistribution || getFallbackAgencyDistributionData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="agency-distribution-canvas"></canvas>';
    const canvas = document.getElementById('agency-distribution-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.agencyDistribution = new Chart(chartCtx, {
        type: 'doughnut',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Datasets',
                data: data.datasets?.[0]?.data || [],
                backgroundColor: data.datasets?.[0]?.backgroundColor || [],
                borderColor: data.datasets?.[0]?.borderColor || '#1f2937',
                borderWidth: data.datasets?.[0]?.borderWidth || 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'right',
                    labels: {
                        usePointStyle: true,
                        padding: 15,
                        font: {
                            size: 12
                        }
                    }
                },
                title: {
                    display: true,
                    text: 'Dataset Distribution by Agency',
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('agency-distribution', data.summary);
}

// Render agencies chart
function renderAgenciesChart() {
    const ctx = document.getElementById('agencies-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.agencies) {
        chartInstances.agencies.destroy();
    }
    
    const data = chartData.agencies || getFallbackAgenciesData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="agencies-canvas"></canvas>';
    const canvas = document.getElementById('agencies-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.agencies = new Chart(chartCtx, {
        type: 'bar',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Dataset Count',
                data: data.datasets?.[0]?.data || [],
                backgroundColor: '#3b82f6',
                borderColor: '#2563eb',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: '#e5e7eb'
                    }
                },
                x: {
                    grid: {
                        color: '#e5e7eb'
                    }
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('agencies', data.stats);
}

// Render changes chart
function renderChangesChart() {
    const ctx = document.getElementById('changes-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.changes) {
        chartInstances.changes.destroy();
    }
    
    const data = chartData.changes || getFallbackChangesData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="changes-canvas"></canvas>';
    const canvas = document.getElementById('changes-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.changes = new Chart(chartCtx, {
        type: 'line',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Changes',
                data: data.datasets?.[0]?.data || [],
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: '#e5e7eb'
                    }
                },
                x: {
                    grid: {
                        color: '#e5e7eb'
                    }
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('changes', data.stats);
}

// Render quality chart
function renderQualityChart() {
    const ctx = document.getElementById('quality-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (chartInstances.quality) {
        chartInstances.quality.destroy();
    }
    
    const data = chartData.quality || getFallbackQualityData();
    
    // Create canvas element
    ctx.innerHTML = '<canvas id="quality-canvas"></canvas>';
    const canvas = document.getElementById('quality-canvas');
    const chartCtx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    chartInstances.quality = new Chart(chartCtx, {
        type: 'line',
        data: {
            labels: data.labels || [],
            datasets: [{
                label: 'Quality Score',
                data: data.datasets?.[0]?.data || [],
                borderColor: '#8b5cf6',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: {
                        color: '#e5e7eb'
                    }
                },
                x: {
                    grid: {
                        color: '#e5e7eb'
                    }
                }
            }
        }
    });
    
    // Update stats
    updateChartStats('quality', data.stats);
}

// Update chart statistics
function updateChartStats(chartId, stats) {
    const statsElement = document.getElementById(`${chartId}-stats`);
    if (!statsElement || !stats) return;
    
    let statsHtml = '<div class="chart-stats-grid">';
    
    for (const [key, value] of Object.entries(stats)) {
        const label = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        statsHtml += `
            <div class="chart-stat">
                <div class="stat-label">${label}</div>
                <div class="stat-value">${value}</div>
            </div>
        `;
    }
    
    statsHtml += '</div>';
    statsElement.innerHTML = statsHtml;
}

// Toggle chart type
function toggleChart(chartId, type) {
    // Update button states
    const buttons = document.querySelectorAll(`[onclick*="toggleChart('${chartId}'"]`);
    buttons.forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
    
    // Update chart type
    if (chartInstances[chartId]) {
        chartInstances[chartId].config.type = type;
        chartInstances[chartId].update();
    }
}

// Export chart data
function exportChartData(format) {
    console.log(`Exporting chart data as ${format}`);
    // Implementation for exporting chart data
    alert(`Exporting chart data as ${format.toUpperCase()}...`);
}

// Error handling for missing data
function handleMissingData(dataType) {
    console.warn(`No ${dataType} data available`);
    return {
        labels: [],
        datasets: [],
        error: `No ${dataType} data available. Please check data sources.`
    };
}

function getFallbackStatusData() {
    return handleMissingData('status distribution');
}

function getFallbackAgencyDistributionData() {
    return handleMissingData('agency distribution');
}

function getFallbackAgenciesData() {
    return handleMissingData('agencies');
}

function getFallbackChangesData() {
    return handleMissingData('changes');
}

function getFallbackQualityData() {
    return handleMissingData('quality metrics');
}
