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

// Fallback data functions
function getFallbackTrendsData() {
    const labels = [];
    const total = [];
    const available = [];
    const unavailable = [];
    
    // Generate 30 days of sample data
    for (let i = 29; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        labels.push(date.toISOString().split('T')[0]);
        
        const baseTotal = 1000 + Math.random() * 100;
        const baseAvailable = baseTotal * (0.85 + Math.random() * 0.1);
        const baseUnavailable = baseTotal - baseAvailable;
        
        total.push(Math.round(baseTotal));
        available.push(Math.round(baseAvailable));
        unavailable.push(Math.round(baseUnavailable));
    }
    
    return {
        labels: labels,
        total: total,
        available: available,
        unavailable: unavailable,
        stats: {
            total_datasets: Math.max(...total),
            avg_available: Math.round(available.reduce((a, b) => a + b, 0) / available.length),
            avg_unavailable: Math.round(unavailable.reduce((a, b) => a + b, 0) / unavailable.length)
        }
    };
}

function getFallbackStatusData() {
    return {
        labels: ['Available', 'Unavailable', 'Unknown'],
        datasets: [{
            label: 'Status',
            data: [850, 120, 30],
            backgroundColor: ['#10b981', '#ef4444', '#6b7280'],
            borderColor: '#1f2937',
            borderWidth: 1
        }],
        stats: {
            total_datasets: 1000,
            available_percentage: 85,
            unavailable_percentage: 12,
            unknown_percentage: 3
        }
    };
}

function getFallbackAgencyDistributionData() {
    return {
        labels: ['Department of Health', 'Department of Education', 'Department of Transportation', 'Department of Energy', 'Department of Commerce'],
        datasets: [{
            label: 'Datasets',
            data: [125, 98, 76, 54, 43],
            backgroundColor: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'],
            borderColor: '#1f2937',
            borderWidth: 1
        }],
        summary: {
            total_agencies: 5,
            total_datasets: 396,
            total_available: 350,
            total_unavailable: 46
        }
    };
}

function getFallbackAgenciesData() {
    return {
        labels: ['Agency A', 'Agency B', 'Agency C', 'Agency D', 'Agency E'],
        datasets: [{
            label: 'Datasets',
            data: [45, 32, 28, 19, 15],
            backgroundColor: '#3b82f6',
            borderColor: '#2563eb',
            borderWidth: 1
        }],
        stats: {
            total_agencies: 5,
            total_datasets: 139
        }
    };
}

function getFallbackChangesData() {
    const labels = [];
    const data = [];
    
    // Generate 30 days of sample data
    for (let i = 29; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        labels.push(date.toISOString().split('T')[0]);
        data.push(Math.round(Math.random() * 50 + 10));
    }
    
    return {
        labels: labels,
        datasets: [{
            label: 'Changes',
            data: data,
            borderColor: '#f59e0b',
            backgroundColor: 'rgba(245, 158, 11, 0.1)',
            tension: 0.4,
            fill: true
        }],
        stats: {
            total_changes: data.reduce((a, b) => a + b, 0),
            avg_daily_changes: Math.round(data.reduce((a, b) => a + b, 0) / data.length)
        }
    };
}

function getFallbackQualityData() {
    const labels = [];
    const data = [];
    
    // Generate 30 days of sample data
    for (let i = 29; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        labels.push(date.toISOString().split('T')[0]);
        data.push(Math.round(70 + Math.random() * 20));
    }
    
    return {
        labels: labels,
        datasets: [{
            label: 'Quality Score',
            data: data,
            borderColor: '#8b5cf6',
            backgroundColor: 'rgba(139, 92, 246, 0.1)',
            tension: 0.4,
            fill: true
        }],
        stats: {
            avg_quality: Math.round(data.reduce((a, b) => a + b, 0) / data.length),
            min_quality: Math.min(...data),
            max_quality: Math.max(...data)
        }
    };
}
