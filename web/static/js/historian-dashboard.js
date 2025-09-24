/**
 * Historian Dashboard JavaScript
 * Handles data loading and UI updates for the main dashboard
 */

class HistorianDashboard {
    constructor() {
        this.init();
    }

    init() {
        this.loadNavigation();
        this.loadSystemStats();
        this.loadRecentActivity();
        this.loadSystemStatus();
        
        // Refresh data every 30 seconds
        setInterval(() => {
            this.loadSystemStats();
            this.loadRecentActivity();
            this.loadSystemStatus();
        }, 30000);
    }

    async loadNavigation() {
        try {
            const response = await fetch('/historian_navigation.html');
            const html = await response.text();
            document.getElementById('historian-nav-container').innerHTML = html;
        } catch (error) {
            console.error('Error loading navigation:', error);
        }
    }

    async loadSystemStats() {
        try {
            const response = await fetch('/api/system/status');
            const data = await response.json();
            
            if (data.error) {
                console.error('Error loading system stats:', data.error);
                return;
            }

            // Update stats cards
            this.updateElement('total-datasets', data.total_datasets || 0);
            this.updateElement('total-snapshots', data.total_snapshots || 0);
            this.updateElement('total-events', data.total_diffs || 0);
            
            // Load vanished datasets count
            this.loadVanishedCount();
            
        } catch (error) {
            console.error('Error loading system stats:', error);
        }
    }

    async loadVanishedCount() {
        try {
            const response = await fetch('/api/vanished-datasets/stats');
            const data = await response.json();
            
            if (data.error) {
                console.error('Error loading vanished stats:', data.error);
                return;
            }

            this.updateElement('vanished-datasets', data.total_vanished || 0);
        } catch (error) {
            console.error('Error loading vanished count:', error);
        }
    }

    async loadRecentActivity() {
        try {
            const response = await fetch('/api/changes?limit=5');
            const data = await response.json();
            
            if (data.error) {
                console.error('Error loading recent activity:', data.error);
                this.showError('recent-activity', 'Failed to load recent activity');
                return;
            }

            this.renderRecentActivity(data.changes || []);
        } catch (error) {
            console.error('Error loading recent activity:', error);
            this.showError('recent-activity', 'Failed to load recent activity');
        }
    }

    renderRecentActivity(changes) {
        const container = document.getElementById('recent-activity');
        
        if (!changes || changes.length === 0) {
            container.innerHTML = '<div class="loading">No recent activity</div>';
            return;
        }

        const html = changes.map(change => `
            <div class="activity-item">
                <div class="activity-icon">${this.getActivityIcon(change.type)}</div>
                <div class="activity-content">
                    <div class="activity-title">${change.title || 'Unknown Dataset'}</div>
                    <div class="activity-description">${change.description || change.type}</div>
                </div>
                <div class="activity-time">${this.formatTime(change.date)}</div>
            </div>
        `).join('');

        container.innerHTML = html;
    }

    getActivityIcon(type) {
        const icons = {
            'content_change': 'CONTENT',
            'availability_change': 'WARNING',
            'license_change': 'LICENSE',
            'schema_change': 'SCHEMA',
            'vanished': 'VANISHED',
            'new': '✨'
        };
        return icons[type] || 'CHANGE';
    }

    async loadSystemStatus() {
        try {
            const response = await fetch('/api/health');
            const data = await response.json();
            
            if (data.error) {
                console.error('Error loading system status:', data.error);
                return;
            }

            // Update monitoring status
            const statusElement = document.getElementById('monitoring-status');
            statusElement.textContent = data.monitoring_status || 'Unknown';
            statusElement.className = `status-value ${data.monitoring_status || 'unknown'}`;

            // Update last updated time
            this.updateElement('last-updated', this.formatTime(data.timestamp));

            // Update health score
            this.updateElement('health-score', `${data.health_score || 0}%`);

        } catch (error) {
            console.error('Error loading system status:', error);
        }
    }

    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        }
    }

    showError(containerId, message) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `<div class="loading" style="color: var(--event-high);">${message}</div>`;
        }
    }

    formatTime(timestamp) {
        if (!timestamp) return 'Unknown';
        
        try {
            const date = new Date(timestamp);
            const now = new Date();
            const diffMs = now - date;
            const diffMins = Math.floor(diffMs / 60000);
            const diffHours = Math.floor(diffMs / 3600000);
            const diffDays = Math.floor(diffMs / 86400000);

            if (diffMins < 1) return 'Just now';
            if (diffMins < 60) return `${diffMins}m ago`;
            if (diffHours < 24) return `${diffHours}h ago`;
            if (diffDays < 7) return `${diffDays}d ago`;
            
            return date.toLocaleDateString();
        } catch (error) {
            return 'Unknown';
        }
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new HistorianDashboard();
});
