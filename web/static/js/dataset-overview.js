// Dataset Overview JavaScript - Dataset State Historian

class DatasetOverview {
    constructor() {
        this.datasetId = this.getDatasetIdFromUrl();
        this.dataset = null;
        this.timeline = [];
        this.changes = [];
        this.postmortem = null;
        this.currentTab = 'timeline';
        
        this.init();
    }
    
    getDatasetIdFromUrl() {
        const path = window.location.pathname;
        const match = path.match(/\/datasets\/([^\/]+)/);
        return match ? match[1] : null;
    }
    
    init() {
        if (!this.datasetId) {
            this.showError('No dataset ID provided');
            return;
        }
        
        this.setupEventListeners();
        this.loadDataset();
    }
    
    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const tabName = e.target.getAttribute('onclick').match(/showTab\('([^']+)'\)/)[1];
                this.showTab(tabName);
            });
        });
    }
    
    async loadDataset() {
        try {
            this.showLoading(true);
            
            // Load dataset details
            const response = await fetch(`/api/dataset/${this.datasetId}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.dataset = data;
            this.timeline = data.timeline || [];
            this.changes = data.diffs || [];
            
            this.renderDatasetHeader();
            this.renderQuickStats();
            this.loadTimeline();
            this.loadChanges();
            
        } catch (error) {
            console.error('Error loading dataset:', error);
            this.showError('Failed to load dataset: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    renderDatasetHeader() {
        if (!this.dataset) return;
        
        const dataset = this.dataset;
        
        document.getElementById('datasetTitle').textContent = dataset.title || 'Untitled Dataset';
        document.getElementById('datasetAgency').textContent = dataset.agency || 'Unknown Agency';
        
        const status = this.getDatasetStatus(dataset);
        const statusElement = document.getElementById('datasetStatus');
        statusElement.textContent = status;
        statusElement.className = `status-pill ${status}`;
        
        document.getElementById('datasetLicense').textContent = dataset.license || 'Unknown';
        
        if (this.timeline.length > 0) {
            const firstSeen = new Date(this.timeline[0].date).toLocaleDateString();
            const lastSeen = new Date(this.timeline[this.timeline.length - 1].date).toLocaleDateString();
            
            document.getElementById('datasetFirstSeen').textContent = firstSeen;
            document.getElementById('datasetLastSeen').textContent = lastSeen;
        }
    }
    
    renderQuickStats() {
        if (!this.dataset) return;
        
        const snapshots = this.timeline.length;
        const licenseFlips = this.countLicenseFlips();
        const events = this.changes.length;
        const volatility = this.calculateVolatility();
        
        document.getElementById('snapshotsCount').textContent = snapshots;
        document.getElementById('licenseFlips').textContent = licenseFlips;
        document.getElementById('eventsCount').textContent = events;
        document.getElementById('volatilityScore').textContent = volatility.toFixed(2);
    }
    
    getDatasetStatus(dataset) {
        if (dataset.availability === 'available') {
            if (dataset.total_diffs > 0) {
                return 'active';
            } else {
                return 'stable';
            }
        } else if (dataset.availability === 'partially_available') {
            return 'degraded';
        } else {
            return 'vanished';
        }
    }
    
    countLicenseFlips() {
        let flips = 0;
        let lastLicense = null;
        
        for (const snapshot of this.timeline) {
            const license = snapshot.license || 'Unknown';
            if (lastLicense && lastLicense !== license) {
                flips++;
            }
            lastLicense = license;
        }
        
        return flips;
    }
    
    calculateVolatility() {
        if (this.timeline.length < 2) return 0;
        
        let changes = 0;
        let totalFields = 0;
        
        for (let i = 1; i < this.timeline.length; i++) {
            const prev = this.timeline[i - 1];
            const curr = this.timeline[i];
            
            // Count field changes
            const fields = ['title', 'agency', 'license', 'row_count', 'column_count'];
            for (const field of fields) {
                totalFields++;
                if (prev[field] !== curr[field]) {
                    changes++;
                }
            }
        }
        
        return totalFields > 0 ? changes / totalFields : 0;
    }
    
    async loadTimeline() {
        try {
            const days = document.getElementById('timelineDays')?.value || '30';
            const response = await fetch(`/api/timeline/chromogram/${this.datasetId}?days=${days}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.renderChromogram(data);
            
        } catch (error) {
            console.error('Error loading timeline:', error);
            document.getElementById('chromogramTimeline').innerHTML = 
                `<div class="error">Failed to load timeline: ${error.message}</div>`;
        }
    }
    
    renderChromogram(data) {
        const container = document.getElementById('chromogramTimeline');
        
        if (!data.bands || data.bands.length === 0) {
            container.innerHTML = '<div class="text-center">No timeline data available</div>';
            return;
        }
        
        let html = `
            <div class="chromogram-header">
                <h4 class="chromogram-title">Dataset Evolution Timeline</h4>
                <div class="chromogram-legend">
                    <span>● Changed</span>
                    <span>○ Unchanged</span>
                </div>
            </div>
            <div class="chromogram-grid">
                <div class="chromogram-fields">
        `;
        
        // Render field labels
        for (const band of data.bands) {
            for (const field of band.fields) {
                html += `<div class="chromogram-field">${field}</div>`;
            }
        }
        
        html += `
                </div>
                <div class="chromogram-timeline-rows">
        `;
        
        // Render timeline rows
        for (const band of data.bands) {
            for (const field of band.fields) {
                html += `<div class="chromogram-timeline-row">`;
                
                // Render cells for this field
                const fieldCells = data.cells.filter(cell => cell.field === field);
                for (const cell of fieldCells) {
                    const changed = cell.changed ? 'changed' : 'unchanged';
                    const color = this.getFieldColor(field, cell.value);
                    html += `<div class="chromogram-cell ${changed}" style="background-color: ${color}" 
                             onclick="datasetOverview.showFieldDiff('${field}')" 
                             title="${cell.date}: ${cell.value}"></div>`;
                }
                
                html += `</div>`;
            }
        }
        
        html += `
                </div>
            </div>
        `;
        
        container.innerHTML = html;
    }
    
    getFieldColor(field, value) {
        // Generate consistent colors based on field name and value
        const colors = {
            'title': '#006D77',
            'agency': '#2A9D8F',
            'license': '#FFB400',
            'row_count': '#D7263D',
            'column_count': '#8B5CF6'
        };
        
        return colors[field] || '#6A6F73';
    }
    
    async loadChanges() {
        try {
            const response = await fetch(`/api/events/${this.datasetId}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.changes = data.events || [];
            this.renderChanges();
            
        } catch (error) {
            console.error('Error loading changes:', error);
            document.getElementById('changesList').innerHTML = 
                `<div class="error">Failed to load changes: ${error.message}</div>`;
        }
    }
    
    renderChanges() {
        const container = document.getElementById('changesList');
        
        if (this.changes.length === 0) {
            container.innerHTML = '<div class="text-center">No changes recorded</div>';
            return;
        }
        
        let html = '';
        
        for (const change of this.changes) {
            const severity = change.severity || 'low';
            const date = new Date(change.date).toLocaleDateString();
            
            html += `
                <div class="change-item">
                    <div class="change-date">${date}</div>
                    <div class="change-type">${change.event_type || 'Unknown'}</div>
                    <div class="change-severity ${severity}">${severity}</div>
                    <div class="change-description">${change.description || 'No description'}</div>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    filterChanges() {
        const severityFilter = document.getElementById('severityFilter')?.value || '';
        const eventTypeFilter = document.getElementById('eventTypeFilter')?.value || '';
        
        const filteredChanges = this.changes.filter(change => {
            if (severityFilter && change.severity !== severityFilter) {
                return false;
            }
            if (eventTypeFilter && change.event_type !== eventTypeFilter) {
                return false;
            }
            return true;
        });
        
        this.renderFilteredChanges(filteredChanges);
    }
    
    renderFilteredChanges(changes) {
        const container = document.getElementById('changesList');
        
        if (changes.length === 0) {
            container.innerHTML = '<div class="text-center">No changes match the selected filters</div>';
            return;
        }
        
        let html = '';
        
        for (const change of changes) {
            const severity = change.severity || 'low';
            const date = new Date(change.date).toLocaleDateString();
            
            html += `
                <div class="change-item">
                    <div class="change-date">${date}</div>
                    <div class="change-type">${change.event_type || 'Unknown'}</div>
                    <div class="change-severity ${severity}">${severity}</div>
                    <div class="change-description">${change.description || 'No description'}</div>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    async generatePostmortem() {
        try {
            this.showLoading(true);
            
            const response = await fetch(`/api/postmortem/${this.datasetId}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.postmortem = data;
            this.renderPostmortem();
            
        } catch (error) {
            console.error('Error generating postmortem:', error);
            document.getElementById('postmortemContent').innerHTML = 
                `<div class="error">Failed to generate post-mortem: ${error.message}</div>`;
        } finally {
            this.showLoading(false);
        }
    }
    
    renderPostmortem() {
        const container = document.getElementById('postmortemContent');
        
        if (!this.postmortem) {
            container.innerHTML = '<div class="text-center">No post-mortem data available</div>';
            return;
        }
        
        const dataset = this.postmortem.dataset || {};
        const timeline = this.postmortem.timeline || [];
        const conclusion = this.postmortem.conclusion || {};
        
        let html = `
            <div class="postmortem-section">
                <h4>Dataset Information</h4>
                <p><strong>Title:</strong> ${dataset.title || 'Unknown'}</p>
                <p><strong>Agency:</strong> ${dataset.agency || 'Unknown'}</p>
                <p><strong>Status:</strong> <span class="status-pill ${dataset.status || 'unknown'}">${dataset.status || 'Unknown'}</span></p>
                <p><strong>Last Seen:</strong> ${dataset.last_seen || 'Unknown'}</p>
            </div>
        `;
        
        if (timeline.length > 0) {
            html += `
                <div class="postmortem-section">
                    <h4>Key Changes Before Disappearance</h4>
                    <div class="postmortem-timeline">
            `;
            
            for (const event of timeline) {
                html += `
                    <div class="postmortem-timeline-item">
                        <div class="postmortem-timeline-date">${new Date(event.date).toLocaleDateString()}</div>
                        <div class="postmortem-timeline-event">${event.event_type || 'Unknown Event'}</div>
                        <div class="postmortem-timeline-description">${event.description || 'No description'}</div>
                    </div>
                `;
            }
            
            html += `
                    </div>
                </div>
            `;
        }
        
        if (conclusion.suspected_cause || conclusion.archived_copies) {
            html += `
                <div class="postmortem-conclusion">
                    <h5>Analysis Conclusion</h5>
                    <p><strong>Suspected Cause:</strong> ${conclusion.suspected_cause || 'Unknown'}</p>
                    <p><strong>Archived Copies:</strong> ${conclusion.archived_copies || 'None available'}</p>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    showTab(tabName) {
        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        
        // Show selected tab
        document.getElementById(`${tabName}-tab`).classList.add('active');
        document.querySelector(`[onclick="showTab('${tabName}')"]`).classList.add('active');
        
        this.currentTab = tabName;
        
        // Load tab-specific data
        if (tabName === 'timeline') {
            this.loadTimeline();
        } else if (tabName === 'changes') {
            this.loadChanges();
        } else if (tabName === 'postmortem') {
            this.generatePostmortem();
        }
    }
    
    async showFieldDiff(field) {
        try {
            const response = await fetch(`/api/field-history/${this.datasetId}/${field}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.renderFieldDiff(field, data.history);
            
        } catch (error) {
            console.error('Error loading field diff:', error);
            this.showError('Failed to load field history: ' + error.message);
        }
    }
    
    renderFieldDiff(field, history) {
        const modal = document.getElementById('fieldDiffModal');
        const title = document.getElementById('fieldDiffTitle');
        const content = document.getElementById('fieldDiffContent');
        
        title.textContent = `Field: ${field}`;
        
        if (history.length === 0) {
            content.innerHTML = '<div class="text-center">No history available for this field</div>';
        } else {
            let html = '<div class="field-diff-timeline">';
            
            for (const item of history) {
                html += `
                    <div class="field-diff-period">
                        <div class="field-diff-date">${new Date(item.date).toLocaleDateString()}</div>
                        <div class="field-diff-value ${item.changed ? 'field-diff-changed' : ''}">${item.value || 'N/A'}</div>
                    </div>
                `;
            }
            
            html += '</div>';
            content.innerHTML = html;
        }
        
        modal.style.display = 'block';
    }
    
    closeFieldDiffModal() {
        const modal = document.getElementById('fieldDiffModal');
        modal.style.display = 'none';
    }
    
    showLoading(show) {
        const indicator = document.getElementById('loadingIndicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }
    
    showError(message) {
        console.error(message);
        // You could show a toast notification here
    }
}

// Global functions for HTML onclick handlers
function showTab(tabName) {
    if (window.datasetOverview) {
        datasetOverview.showTab(tabName);
    }
}

function updateTimeline() {
    if (window.datasetOverview) {
        datasetOverview.loadTimeline();
    }
}

function filterChanges() {
    if (window.datasetOverview) {
        datasetOverview.filterChanges();
    }
}

function loadChanges() {
    if (window.datasetOverview) {
        datasetOverview.loadChanges();
    }
}

function generatePostmortem() {
    if (window.datasetOverview) {
        datasetOverview.generatePostmortem();
    }
}

function exportDataset() {
    // Implement export functionality
    console.log('Export dataset');
}

function exportPostmortem() {
    // Implement PDF export functionality
    console.log('Export postmortem as PDF');
}

function refreshDataset() {
    if (window.datasetOverview) {
        datasetOverview.loadDataset();
    }
}

function closeFieldDiffModal() {
    if (window.datasetOverview) {
        datasetOverview.closeFieldDiffModal();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.datasetOverview = new DatasetOverview();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('fieldDiffModal');
    if (event.target === modal) {
        closeFieldDiffModal();
    }
});
