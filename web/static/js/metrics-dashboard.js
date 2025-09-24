// Metrics Dashboard JavaScript

let trendsChart = null;
let yearlyChart = null;

async function loadSystemMetrics() {
    try {
        const response = await fetch('/api/enhanced/metrics/overview');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const metrics = await response.json();
        populateSystemMetrics(metrics);
        
        // Load charts
        loadTrendsChart(metrics.monthly_trends);
        loadYearlyChart(metrics.yearly_trends);
        
        // Populate agency leaderboard
        populateAgencyLeaderboard(metrics.top_agencies_by_churn);
        
        // Populate agency tiles
        populateAgencyTiles(metrics.top_agencies_by_churn);
        
    } catch (error) {
        console.error('Error loading system metrics:', error);
        showError('Failed to load system metrics');
    }
}

function populateSystemMetrics(metrics) {
    // Update KPI cards with null checks
    const updateElement = (id, value) => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        } else {
            console.warn(`Element with id '${id}' not found`);
        }
    };
    
    updateElement('total-datasets', metrics.total_datasets?.toLocaleString() || '0');
    updateElement('active-datasets', metrics.active_datasets?.toLocaleString() || '0');
    updateElement('vanished-datasets', metrics.vanished_datasets?.toLocaleString() || '0');
    updateElement('new-this-year', metrics.new_datasets_this_year?.toLocaleString() || '0');
    updateElement('median-lifespan', (metrics.median_lifespan_days || 0) + ' days');
    updateElement('avg-volatility', (metrics.avg_volatility || 0).toFixed(2));
    
    // Update trend indicators (placeholder)
    updateTrendIndicator('total-trend', '+5.2%');
    updateTrendIndicator('active-trend', '+3.1%');
    updateTrendIndicator('vanished-trend', '+1.8%');
    updateTrendIndicator('new-trend', '+12.5%');
    updateTrendIndicator('lifespan-trend', '+2.3%');
    updateTrendIndicator('volatility-trend', '-0.8%');
}

function updateTrendIndicator(elementId, trend) {
    const element = document.getElementById(elementId);
    if (!element) {
        console.warn(`Element with id '${elementId}' not found`);
        return;
    }
    
    const trendText = element.querySelector('.trend-text');
    const trendIcon = element.querySelector('.trend-icon');
    
    if (!trendText) {
        console.warn(`Trend text element not found in '${elementId}'`);
        return;
    }
    
    trendText.textContent = trend;
    
    // Update trend icon based on positive/negative (if icon exists)
    if (trendIcon) {
        if (trend.startsWith('+')) {
            trendIcon.textContent = 'UP';
            trendText.style.color = '#16a34a';
        } else if (trend.startsWith('-')) {
            trendIcon.textContent = 'DOWN';
            trendText.style.color = '#dc2626';
        } else {
            trendIcon.textContent = 'STABLE';
            trendText.style.color = '#6b7280';
        }
    } else {
        // If no icon, just set the text color
        if (trend.startsWith('+')) {
            trendText.style.color = '#16a34a';
        } else if (trend.startsWith('-')) {
            trendText.style.color = '#dc2626';
        } else {
            trendText.style.color = '#6b7280';
        }
    }
}

function loadTrendsChart(monthlyTrends) {
    const chartElement = document.getElementById('trends-chart');
    if (!chartElement) {
        console.warn('Trends chart element not found');
        return;
    }
    
    const ctx = chartElement.getContext('2d');
    
    // Destroy existing chart
    if (trendsChart) {
        trendsChart.destroy();
    }
    
    // Prepare data
    const labels = monthlyTrends.map(trend => trend.month);
    const activeData = monthlyTrends.map(trend => trend.active_datasets || 0);
    const vanishedData = monthlyTrends.map(trend => trend.vanished_datasets || 0);
    
    trendsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Active Datasets',
                    data: activeData,
                    borderColor: '#16a34a',
                    backgroundColor: 'rgba(22, 163, 74, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Vanished Datasets',
                    data: vanishedData,
                    borderColor: '#dc2626',
                    backgroundColor: 'rgba(220, 38, 38, 0.1)',
                    tension: 0.4,
                    fill: true
                }
            ]
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
}

function loadYearlyChart(yearlyTrends) {
    const chartElement = document.getElementById('yearly-chart');
    if (!chartElement) {
        console.warn('Yearly chart element not found');
        return;
    }
    
    const ctx = chartElement.getContext('2d');
    
    // Destroy existing chart
    if (yearlyChart) {
        yearlyChart.destroy();
    }
    
    // Prepare data
    const labels = yearlyTrends.map(trend => trend.year);
    const data = yearlyTrends.map(trend => trend.new_datasets);
    
    yearlyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'New Datasets',
                data: data,
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
}

function populateAgencyLeaderboard(agencies) {
    const tbody = document.querySelector('#agency-leaderboard tbody');
    if (!tbody) {
        console.warn('Agency leaderboard tbody not found');
        return;
    }
    
    tbody.innerHTML = '';
    
    agencies.forEach((agency, index) => {
        const row = document.createElement('tr');
        
        // Determine vanished rate class
        let vanishedRateClass = 'low';
        if (agency.vanished_rate > 0.3) vanishedRateClass = 'high';
        else if (agency.vanished_rate > 0.1) vanishedRateClass = 'medium';
        
        row.innerHTML = `
            <td class="rank-cell">${index + 1}</td>
            <td class="agency-cell">
                <a href="/agencies/${encodeURIComponent(agency.agency)}">${agency.agency}</a>
            </td>
            <td>${agency.total_datasets.toLocaleString()}</td>
            <td>${agency.vanished_datasets.toLocaleString()}</td>
            <td class="vanished-rate ${vanishedRateClass}">${(agency.vanished_rate * 100).toFixed(1)}%</td>
            <td>${agency.avg_volatility.toFixed(2)}</td>
            <td>
                <a href="/agencies/${encodeURIComponent(agency.agency)}" class="action-btn">View</a>
            </td>
        `;
        tbody.appendChild(row);
    });
}

function populateAgencyTiles(agencies) {
    const container = document.getElementById('agency-tiles');
    if (!container) {
        console.warn('Agency tiles container not found');
        return;
    }
    
    container.innerHTML = '';
    
    agencies.slice(0, 6).forEach(agency => {
        const tile = document.createElement('div');
        tile.className = 'agency-tile';
        tile.onclick = () => window.location.href = `/agencies/${encodeURIComponent(agency.agency)}`;
        
        // Calculate volatility percentage for the bar
        const volatilityPercent = Math.min(agency.avg_volatility * 100, 100);
        
        tile.innerHTML = `
            <h3>${agency.agency}</h3>
            <p>${agency.total_datasets.toLocaleString()} datasets</p>
            <div class="tile-metrics">
                <div class="tile-metric">
                    <div class="tile-metric-value">${agency.active_datasets.toLocaleString()}</div>
                    <div class="tile-metric-label">Active</div>
                </div>
                <div class="tile-metric">
                    <div class="tile-metric-value">${agency.vanished_datasets.toLocaleString()}</div>
                    <div class="tile-metric-label">Vanished</div>
                </div>
            </div>
            <div class="volatility-bar">
                <div class="volatility-fill" style="width: ${volatilityPercent}%"></div>
            </div>
        `;
        
        container.appendChild(tile);
    });
}

function showError(message) {
    // Create error notification
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-notification';
    errorDiv.textContent = message;
    errorDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #fef2f2;
        color: #dc2626;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #fecaca;
        z-index: 1000;
    `;
    
    document.body.appendChild(errorDiv);
    
    // Remove after 5 seconds
    setTimeout(() => {
        errorDiv.remove();
    }, 5000);
}
