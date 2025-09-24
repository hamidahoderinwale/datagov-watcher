// Management Dashboard JavaScript

let refreshInterval;

// Load system status
async function loadSystemStatus() {
    try {
        // Load discovery status
        const discoveryResponse = await fetch('/api/discovery/status');
        const discoveryData = await discoveryResponse.json();
        updateDiscoveryStatus(discoveryData);
        
        // Load monitoring status
        const monitoringResponse = await fetch('/api/monitoring/status');
        const monitoringData = await monitoringResponse.json();
        updateMonitoringStatus(monitoringData);
        
        
    } catch (error) {
        console.error('Error loading system status:', error);
        showError('Failed to load system status');
    }
}

// Update discovery status display
function updateDiscoveryStatus(data) {
    const indicator = document.getElementById('discovery-indicator');
    const details = document.getElementById('discovery-details');
    
    if (data.total_datasets > 0) {
        indicator.textContent = 'Active';
        indicator.className = 'status-indicator running';
        details.textContent = `${data.total_datasets.toLocaleString()} datasets discovered`;
    } else {
        indicator.textContent = 'Inactive';
        indicator.className = 'status-indicator stopped';
        details.textContent = 'No datasets discovered yet';
    }
}

// Update monitoring status display
function updateMonitoringStatus(data) {
    const indicator = document.getElementById('monitoring-indicator');
    const details = document.getElementById('monitoring-details');
    
    if (data.running) {
        indicator.textContent = 'Running';
        indicator.className = 'status-indicator running';
        const totalScheduled = Object.values(data.schedule_summary || {}).reduce((sum, priority) => sum + (priority.total || 0), 0);
        details.textContent = `${totalScheduled.toLocaleString()} datasets scheduled`;
    } else {
        indicator.textContent = 'Stopped';
        indicator.className = 'status-indicator stopped';
        details.textContent = 'Monitoring not active';
    }
}


// Load discovery status
async function loadDiscoveryStatus() {
    try {
        const response = await fetch('/api/discovery/status');
        const data = await response.json();
        
        // Update stats
        document.getElementById('total-datasets').textContent = data.total_datasets?.toLocaleString() || '0';
        document.getElementById('recent-discoveries').textContent = data.recent_discoveries?.toLocaleString() || '0';
        document.getElementById('active-sources').textContent = Object.keys(data.source_breakdown || {}).length;
        
        // Update source breakdown
        updateSourceBreakdown(data.source_breakdown || {});
        
    } catch (error) {
        console.error('Error loading discovery status:', error);
        showError('Failed to load discovery status');
    }
}

// Update source breakdown display
function updateSourceBreakdown(sources) {
    const container = document.getElementById('source-breakdown');
    container.innerHTML = '';
    
    if (Object.keys(sources).length === 0) {
        container.innerHTML = '<p class="text-gray-500">No source data available</p>';
        return;
    }
    
    for (const [source, count] of Object.entries(sources)) {
        const item = document.createElement('div');
        item.className = 'source-item';
        item.innerHTML = `
            <span class="source-name">${source}</span>
            <span class="source-count">${count.toLocaleString()}</span>
        `;
        container.appendChild(item);
    }
}

// Load monitoring status
async function loadMonitoringStatus() {
    try {
        const response = await fetch('/api/monitoring/status');
        const data = await response.json();
        
        // Update monitoring priorities
        updateMonitoringPriorities(data.schedule_summary || {});
        
        // Update monitoring stats
        updateMonitoringStats(data);
        
    } catch (error) {
        console.error('Error loading monitoring status:', error);
        showError('Failed to load monitoring status');
    }
}

// Update monitoring priorities display
function updateMonitoringPriorities(priorities) {
    const container = document.getElementById('monitoring-priorities');
    container.innerHTML = '';
    
    const config = {
        critical: { frequency: 1, color: 'red' },
        high: { frequency: 6, color: 'orange' },
        medium: { frequency: 24, color: 'blue' },
        low: { frequency: 168, color: 'green' }
    };
    
    for (const [priority, data] of Object.entries(priorities)) {
        const priorityConfig = config[priority] || { frequency: 24, color: 'gray' };
        
        const card = document.createElement('div');
        card.className = 'priority-card';
        card.innerHTML = `
            <div class="priority-header">
                <span class="priority-name">${priority}</span>
                <span class="priority-frequency">Every ${priorityConfig.frequency}h</span>
            </div>
            <div class="priority-stats">
                <div class="priority-stat">
                    <div class="priority-stat-value">${data.total || 0}</div>
                    <div class="priority-stat-label">Total</div>
                </div>
                <div class="priority-stat">
                    <div class="priority-stat-value">${data.due || 0}</div>
                    <div class="priority-stat-label">Due</div>
                </div>
            </div>
        `;
        container.appendChild(card);
    }
}

// Update monitoring stats display
function updateMonitoringStats(data) {
    const container = document.getElementById('monitoring-stats');
    container.innerHTML = '';
    
    if (data.recent_activity) {
        const activityDiv = document.createElement('div');
        activityDiv.innerHTML = `
            <h4>Recent Activity (Last Hour)</h4>
            <div class="activity-summary">
                ${Object.entries(data.recent_activity).map(([priority, count]) => 
                    `<span class="activity-type monitoring">${priority}: ${count} checks</span>`
                ).join(' ')}
            </div>
        `;
        container.appendChild(activityDiv);
    }
    
    if (data.success_rates) {
        const successDiv = document.createElement('div');
        successDiv.innerHTML = `
            <h4>Success Rates (Last 24h)</h4>
            <div class="success-rates">
                ${Object.entries(data.success_rates).map(([priority, rates]) => 
                    `<span class="activity-type monitoring">${priority}: ${(rates.success_rate * 100).toFixed(1)}%</span>`
                ).join(' ')}
            </div>
        `;
        container.appendChild(successDiv);
    }
}

// Load activity log
async function loadActivityLog() {
    try {
        // This would typically fetch from a logs API
        // For now, we'll show a placeholder
        const container = document.getElementById('activity-log');
        container.innerHTML = `
            <div class="activity-item">
                <div class="activity-message">System initialized</div>
                <div class="activity-time">${new Date().toLocaleString()}</div>
            </div>
        `;
    } catch (error) {
        console.error('Error loading activity log:', error);
    }
}

// Start discovery
async function startDiscovery() {
    try {
        const button = event.target;
        button.disabled = true;
        button.textContent = 'Starting...';
        
        const response = await fetch('/api/discovery/start', { method: 'POST' });
        const data = await response.json();
        
        if (data.status === 'started') {
            showSuccess('Discovery started successfully');
            setTimeout(() => {
                loadDiscoveryStatus();
            }, 2000);
        } else {
            showError('Failed to start discovery: ' + data.error);
        }
        
    } catch (error) {
        console.error('Error starting discovery:', error);
        showError('Failed to start discovery');
    } finally {
        const button = event.target;
        button.disabled = false;
        button.textContent = 'Start Discovery';
    }
}

// Initialize monitoring
async function initMonitoring() {
    try {
        const button = event.target;
        button.disabled = true;
        button.textContent = 'Initializing...';
        
        const response = await fetch('/api/monitoring/init', { method: 'POST' });
        const data = await response.json();
        
        if (data.status === 'initializing') {
            showSuccess('Monitoring initialization started');
            setTimeout(() => {
                loadMonitoringStatus();
            }, 3000);
        } else {
            showError('Failed to initialize monitoring: ' + data.error);
        }
        
    } catch (error) {
        console.error('Error initializing monitoring:', error);
        showError('Failed to initialize monitoring');
    } finally {
        const button = event.target;
        button.disabled = false;
        button.textContent = 'Initialize Schedule';
    }
}

// Start monitoring
async function startMonitoring() {
    try {
        const button = event.target;
        button.disabled = true;
        button.textContent = 'Starting...';
        
        const response = await fetch('/api/monitoring/start', { method: 'POST' });
        const data = await response.json();
        
        if (data.status === 'started') {
            showSuccess('Monitoring started successfully');
            setTimeout(() => {
                loadMonitoringStatus();
            }, 2000);
        } else {
            showError('Failed to start monitoring: ' + data.error);
        }
        
    } catch (error) {
        console.error('Error starting monitoring:', error);
        showError('Failed to start monitoring');
    } finally {
        const button = event.target;
        button.disabled = false;
        button.textContent = 'Start Monitoring';
    }
}

// Refresh discovery status
function refreshDiscoveryStatus() {
    loadDiscoveryStatus();
    showSuccess('Discovery status refreshed');
}

// Refresh monitoring status
function refreshMonitoringStatus() {
    loadMonitoringStatus();
    showSuccess('Monitoring status refreshed');
}

// Refresh activity log
function refreshActivityLog() {
    loadActivityLog();
    showSuccess('Activity log refreshed');
}

// Clear activity log
function clearActivityLog() {
    const container = document.getElementById('activity-log');
    container.innerHTML = '<div class="activity-item"><div class="activity-message">Log cleared</div><div class="activity-time">' + new Date().toLocaleString() + '</div></div>';
    showSuccess('Activity log cleared');
}

// Save configuration
async function saveConfiguration() {
    try {
        const config = {
            discovery_interval: parseInt(document.getElementById('discovery-interval').value),
            critical_frequency: parseInt(document.getElementById('critical-frequency').value),
            high_frequency: parseInt(document.getElementById('high-frequency').value),
            medium_frequency: parseInt(document.getElementById('medium-frequency').value),
            low_frequency: parseInt(document.getElementById('low-frequency').value)
        };
        
        // This would typically save to a configuration API
        localStorage.setItem('monitoring_config', JSON.stringify(config));
        showSuccess('Configuration saved successfully');
        
    } catch (error) {
        console.error('Error saving configuration:', error);
        showError('Failed to save configuration');
    }
}

// Reset configuration
function resetConfiguration() {
    document.getElementById('discovery-interval').value = 24;
    document.getElementById('critical-frequency').value = 1;
    document.getElementById('high-frequency').value = 6;
    document.getElementById('medium-frequency').value = 24;
    document.getElementById('low-frequency').value = 168;
    showSuccess('Configuration reset to defaults');
}

// Show success message
function showSuccess(message) {
    showNotification(message, 'success');
}

// Show error message
function showError(message) {
    showNotification(message, 'error');
}

// Show notification
function showNotification(message, type) {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        color: white;
        font-weight: 500;
        z-index: 1000;
        animation: slideIn 0.3s ease;
    `;
    
    if (type === 'success') {
        notification.style.backgroundColor = '#10b981';
    } else if (type === 'error') {
        notification.style.backgroundColor = '#ef4444';
    }
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

// Add CSS for notification animation
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
`;
document.head.appendChild(style);


