// Live Monitor JavaScript
let autoRefreshInterval;
let isAutoRefreshActive = true;

// Load live monitor data
async function loadLiveMonitorData() {
    try {
        // Load monitoring status
        await loadMonitoringStatus();
        
        // Load monitoring statistics
        await loadMonitoringStats();
        
        // Load recent monitoring data
        await loadRecentMonitoringData();
        
        // Load activity feed
        await loadActivityFeed();
        
    } catch (error) {
        console.error('Error loading live monitor data:', error);
        showError('Failed to load live monitor data');
    }
}

// Load monitoring status
async function loadMonitoringStatus() {
    try {
        const response = await fetch('/api/monitoring/status');
        const data = await response.json();
        
        // Update monitoring status
        const statusElement = document.getElementById('monitoring-status');
        const detailsElement = document.getElementById('monitoring-details');
        
        if (data.running) {
            statusElement.textContent = 'Running';
            statusElement.className = 'status-indicator running';
            detailsElement.textContent = 'Monitoring system is active';
        } else {
            statusElement.textContent = 'Stopped';
            statusElement.className = 'status-indicator stopped';
            detailsElement.textContent = 'Monitoring system is inactive';
        }
        
        // Update other status values
        updateElement('total-checks', data.recent_activity?.total_checks || '0');
        updateElement('success-rate', data.success_rates?.overall ? `${(data.success_rates.overall * 100).toFixed(1)}%` : '0%');
        updateElement('last-update', new Date().toLocaleTimeString());
        
    } catch (error) {
        console.error('Error loading monitoring status:', error);
        updateElement('monitoring-status', 'Error');
        updateElement('monitoring-details', 'Failed to load status');
    }
}

// Load monitoring statistics
async function loadMonitoringStats() {
    try {
        const response = await fetch('/api/monitoring/stats');
        const data = await response.json();
        
        // Update status breakdown
        updateStatusBreakdown(data);
        
        // Update response times
        updateResponseTimes(data);
        
        // Update error codes
        updateErrorCodes(data);
        
    } catch (error) {
        console.error('Error loading monitoring stats:', error);
    }
}

// Update status breakdown
function updateStatusBreakdown(data) {
    const container = document.getElementById('status-breakdown');
    container.innerHTML = '';
    
    if (data.monitoring_status) {
        Object.entries(data.monitoring_status).forEach(([status, count]) => {
            const item = document.createElement('div');
            item.className = 'stat-item';
            item.innerHTML = `
                <span class="stat-label">${status}</span>
                <span class="stat-value">${count.toLocaleString()}</span>
            `;
            container.appendChild(item);
        });
    } else {
        container.innerHTML = '<div class="loading">No status data available</div>';
    }
}

// Update response times
function updateResponseTimes(data) {
    const container = document.getElementById('response-times');
    container.innerHTML = '';
    
    if (data.response_times) {
        Object.entries(data.response_times).forEach(([range, count]) => {
            const item = document.createElement('div');
            item.className = 'stat-item';
            item.innerHTML = `
                <span class="stat-label">${range}</span>
                <span class="stat-value">${count.toLocaleString()}</span>
            `;
            container.appendChild(item);
        });
    } else {
        container.innerHTML = '<div class="loading">No response time data available</div>';
    }
}

// Update error codes
function updateErrorCodes(data) {
    const container = document.getElementById('error-codes');
    container.innerHTML = '';
    
    if (data.status_codes) {
        Object.entries(data.status_codes).forEach(([code, count]) => {
            const item = document.createElement('div');
            item.className = 'stat-item';
            item.innerHTML = `
                <span class="stat-label">${code}</span>
                <span class="stat-value">${count.toLocaleString()}</span>
            `;
            container.appendChild(item);
        });
    } else {
        container.innerHTML = '<div class="loading">No error code data available</div>';
    }
}

// Load recent monitoring data
async function loadRecentMonitoringData() {
    try {
        const response = await fetch('/api/datasets?limit=20');
        const data = await response.json();
        
        const tbody = document.querySelector('#monitoring-table tbody');
        tbody.innerHTML = '';
        
        if (data.datasets && data.datasets.length > 0) {
            data.datasets.forEach(dataset => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${formatDatasetId(dataset.dataset_id)}</td>
                    <td>${formatTitle(dataset.title)}</td>
                    <td>${formatAgency(dataset.agency)}</td>
                    <td><span class="status-badge ${dataset.availability || 'unknown'}">${(dataset.availability || 'unknown').toUpperCase()}</span></td>
                    <td>${formatResponseTime(dataset.response_time_ms)}</td>
                    <td>${formatDate(dataset.last_checked)}</td>
                `;
                tbody.appendChild(row);
            });
        } else {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">No monitoring data available</td></tr>';
        }
        
    } catch (error) {
        console.error('Error loading recent monitoring data:', error);
        const tbody = document.querySelector('#monitoring-table tbody');
        tbody.innerHTML = '<tr><td colspan="6" class="loading">Error loading data</td></tr>';
    }
}

// Load activity feed
async function loadActivityFeed() {
    try {
        // For now, create some sample activity items
        // In a real implementation, this would fetch from an activity log API
        const container = document.getElementById('activity-feed');
        container.innerHTML = '';
        
        const activities = [
            { icon: 'success', message: 'Dataset monitoring check completed', time: '2 minutes ago' },
            { icon: 'error', message: 'Dataset unavailable: HTTP 404', time: '5 minutes ago' },
            { icon: 'warning', message: 'High response time detected', time: '8 minutes ago' },
            { icon: 'success', message: 'New dataset discovered', time: '12 minutes ago' },
            { icon: 'success', message: 'Monitoring system started', time: '15 minutes ago' }
        ];
        
        activities.forEach(activity => {
            const item = document.createElement('div');
            item.className = 'activity-item';
            item.innerHTML = `
                <div class="activity-icon ${activity.icon}">${getActivityIcon(activity.icon)}</div>
                <div class="activity-content">
                    <div class="activity-message">${activity.message}</div>
                    <div class="activity-time">${activity.time}</div>
                </div>
            `;
            container.appendChild(item);
        });
        
    } catch (error) {
        console.error('Error loading activity feed:', error);
        const container = document.getElementById('activity-feed');
        container.innerHTML = '<div class="loading">Error loading activity feed</div>';
    }
}

// Get activity icon
function getActivityIcon(type) {
    switch (type) {
        case 'success': return '✓';
        case 'error': return '✗';
        case 'warning': return 'WARNING';
        default: return '•';
    }
}

// Start monitoring
async function startMonitoring() {
    try {
        const response = await fetch('/api/monitoring/start', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (response.ok) {
            showSuccess('Monitoring started successfully');
            loadMonitoringStatus();
        } else {
            const error = await response.json();
            showError(`Failed to start monitoring: ${error.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        console.error('Error starting monitoring:', error);
        showError('Failed to start monitoring');
    }
}

// Stop monitoring
async function stopMonitoring() {
    try {
        const response = await fetch('/api/stop_monitoring', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (response.ok) {
            showSuccess('Monitoring stopped successfully');
            loadMonitoringStatus();
        } else {
            const error = await response.json();
            showError(`Failed to stop monitoring: ${error.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        console.error('Error stopping monitoring:', error);
        showError('Failed to stop monitoring');
    }
}

// Initialize monitoring
async function initMonitoring() {
    try {
        const response = await fetch('/api/monitoring/init', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (response.ok) {
            showSuccess('Monitoring system initialized successfully');
            loadMonitoringStatus();
        } else {
            const error = await response.json();
            showError(`Failed to initialize monitoring: ${error.error || 'Unknown error'}`);
        }
        
    } catch (error) {
        console.error('Error initializing monitoring:', error);
        showError('Failed to initialize monitoring');
    }
}

// Start auto-refresh
function startAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
    
    autoRefreshInterval = setInterval(() => {
        if (isAutoRefreshActive) {
            loadLiveMonitorData();
        }
    }, 30000); // Refresh every 30 seconds
}

// Stop auto-refresh
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// Toggle auto-refresh
function toggleAutoRefresh() {
    isAutoRefreshActive = !isAutoRefreshActive;
    const button = document.getElementById('toggle-refresh');
    
    if (isAutoRefreshActive) {
        startAutoRefresh();
        button.textContent = 'Stop Auto-refresh';
        button.className = 'btn btn-secondary btn-sm';
    } else {
        stopAutoRefresh();
        button.textContent = 'Start Auto-refresh';
        button.className = 'btn btn-primary btn-sm';
    }
}

// Utility functions
function updateElement(id, value) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = value;
    }
}

function formatDatasetId(id) {
    return id ? id.substring(0, 8) + '...' : 'N/A';
}

function formatTitle(title) {
    return title ? (title.length > 50 ? title.substring(0, 50) + '...' : title) : 'N/A';
}

function formatAgency(agency) {
    return agency || 'N/A';
}

function formatResponseTime(time) {
    if (!time) return 'N/A';
    return time < 1000 ? `${time}ms` : `${(time / 1000).toFixed(1)}s`;
}

function formatDate(date) {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
}

function showSuccess(message) {
    // Simple success notification
    console.log('Success:', message);
    // In a real implementation, you might show a toast notification
}

function showError(message) {
    // Simple error notification
    console.error('Error:', message);
    // In a real implementation, you might show a toast notification
}

// Clean up on page unload
window.addEventListener('beforeunload', () => {
    stopAutoRefresh();
});


