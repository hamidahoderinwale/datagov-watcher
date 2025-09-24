// Agency Page JavaScript

let currentAgencyName = null;
let trendsChart = null;

async function loadAgencyData(agencyName) {
    currentAgencyName = agencyName;
    
    try {
        // Load agency metrics
        const response = await fetch(`/api/agencies/${encodeURIComponent(agencyName)}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const metrics = await response.json();
        populateAgencyMetrics(metrics);
        
        // Load trends chart
        loadTrendsChart(metrics.monthly_trends);
        
    } catch (error) {
        console.error('Error loading agency data:', error);
        showError('Failed to load agency data');
    }
}

function populateAgencyMetrics(metrics) {
    // Update header
    const updateElement = (id, value) => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        } else {
            console.warn(`Element with id '${id}' not found`);
        }
    };
    
    updateElement('agency-name', metrics.agency_name || 'Unknown Agency');
    updateElement('total-datasets', (metrics.total_datasets || 0).toLocaleString());
    updateElement('active-datasets', (metrics.active_datasets || 0).toLocaleString());
    updateElement('vanished-datasets', (metrics.vanished_datasets || 0).toLocaleString());
    updateElement('avg-volatility', (metrics.avg_volatility || 0).toFixed(2));
    updateElement('license-stability', ((metrics.license_stability || 0) * 100).toFixed(0) + '%');
    updateElement('median-lifespan', (metrics.median_lifespan_days || 0) + ' days');
    
    // Populate volatile datasets table
    populateVolatileTable(metrics.most_volatile_datasets || []);
    
    // Populate at-risk datasets table
    populateAtRiskTable(metrics.most_at_risk_datasets || []);
}

function populateVolatileTable(datasets) {
    const tbody = document.querySelector('#volatile-table tbody');
    if (!tbody) {
        console.warn('Volatile table tbody not found');
        return;
    }
    
    tbody.innerHTML = '';
    
    datasets.forEach((dataset, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><a href="/datasets/${dataset.dataset_id}" class="dataset-link">${dataset.title || dataset.dataset_id}</a></td>
            <td>${dataset.volatility.toFixed(2)}</td>
            <td><span class="status-badge active">Active</span></td>
            <td><a href="/datasets/${dataset.dataset_id}" class="action-btn">View</a></td>
        `;
        tbody.appendChild(row);
    });
}

function populateAtRiskTable(datasets) {
    const tbody = document.querySelector('#at-risk-table tbody');
    if (!tbody) {
        console.warn('At-risk table tbody not found');
        return;
    }
    
    tbody.innerHTML = '';
    
    if (datasets.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td colspan="4" style="text-align: center; color: #6b7280; font-style: italic;">
                No at-risk datasets identified
            </td>
        `;
        tbody.appendChild(row);
        return;
    }
    
    datasets.forEach((dataset, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><a href="/datasets/${dataset.dataset_id}" class="dataset-link">${dataset.title || dataset.dataset_id}</a></td>
            <td>${dataset.risk_score.toFixed(2)}</td>
            <td>${dataset.last_seen}</td>
            <td><a href="/datasets/${dataset.dataset_id}" class="action-btn">View</a></td>
        `;
        tbody.appendChild(row);
    });
}

function loadTrendsChart(monthlyTrends) {
    const ctx = document.getElementById('trends-chart').getContext('2d');
    
    // Destroy existing chart
    if (trendsChart) {
        trendsChart.destroy();
    }
    
    // Prepare data
    const labels = monthlyTrends.map(trend => trend.month);
    const data = monthlyTrends.map(trend => trend.new_datasets);
    
    trendsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'New Datasets',
                data: data,
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                tension: 0.4,
                fill: true
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

function switchTab(tabName) {
    // Remove active class from all tabs and panels
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));
    
    // Add active class to selected tab and panel
    document.querySelector(`[onclick="switchTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`${tabName}-tab`).classList.add('active');
}

// Search and filter functionality
document.getElementById('dataset-search').addEventListener('input', function() {
    filterDatasets();
});

document.getElementById('status-filter').addEventListener('change', function() {
    filterDatasets();
});

function filterDatasets() {
    const searchTerm = document.getElementById('dataset-search').value.toLowerCase();
    const statusFilter = document.getElementById('status-filter').value;
    
    const rows = document.querySelectorAll('#all-datasets-table tbody tr');
    
    rows.forEach(row => {
        const datasetName = row.cells[0].textContent.toLowerCase();
        const status = row.cells[1].textContent.toLowerCase();
        
        const matchesSearch = datasetName.includes(searchTerm);
        const matchesStatus = !statusFilter || status.includes(statusFilter);
        
        if (matchesSearch && matchesStatus) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
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
