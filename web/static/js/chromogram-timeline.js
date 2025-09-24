// Chromogram Timeline JavaScript - Dataset State Historian

class ChromogramTimeline {
    constructor() {
        this.datasets = [];
        this.selectedDataset = null;
        this.timelineData = null;
        this.currentTimeRange = '30';
        this.currentFieldFilter = 'all';
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.loadDatasets();
    }
    
    setupEventListeners() {
        // Dataset selection
        const datasetSelect = document.getElementById('datasetSelect');
        if (datasetSelect) {
            datasetSelect.addEventListener('change', () => {
                this.loadDatasetTimeline();
            });
        }
        
        // Time range selection
        const timeRange = document.getElementById('timeRange');
        if (timeRange) {
            timeRange.addEventListener('change', () => {
                this.updateTimeline();
            });
        }
        
        // Field filter
        const fieldFilter = document.getElementById('fieldFilter');
        if (fieldFilter) {
            fieldFilter.addEventListener('change', () => {
                this.filterFields();
            });
        }
    }
    
    async loadDatasets() {
        try {
            this.showLoading(true);
            
            const response = await fetch('/api/datasets');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.datasets = data;
            this.populateDatasetSelect();
            
        } catch (error) {
            console.error('Error loading datasets:', error);
            this.showError('Failed to load datasets: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    populateDatasetSelect() {
        const select = document.getElementById('datasetSelect');
        if (!select) return;
        
        // Clear existing options except the first one
        select.innerHTML = '<option value="">Select a dataset...</option>';
        
        // Add dataset options
        this.datasets.forEach(dataset => {
            const option = document.createElement('option');
            option.value = dataset.dataset_id;
            option.textContent = `${dataset.title || 'Untitled'} (${dataset.agency || 'Unknown'})`;
            select.appendChild(option);
        });
    }
    
    async loadDatasetTimeline() {
        const datasetId = document.getElementById('datasetSelect')?.value;
        if (!datasetId) {
            this.clearTimeline();
            return;
        }
        
        this.selectedDataset = datasetId;
        await this.updateTimeline();
    }
    
    async updateTimeline() {
        if (!this.selectedDataset) return;
        
        try {
            this.showLoading(true);
            
            const timeRange = document.getElementById('timeRange')?.value || '30';
            this.currentTimeRange = timeRange;
            
            const response = await fetch(`/api/timeline/chromogram/${this.selectedDataset}?days=${timeRange}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.timelineData = data;
            this.renderChromogram();
            this.renderEventTimeline();
            this.updateTimelineInfo();
            
        } catch (error) {
            console.error('Error loading timeline:', error);
            this.showError('Failed to load timeline: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    renderChromogram() {
        const container = document.getElementById('chromogramTimeline');
        if (!container || !this.timelineData) return;
        
        if (!this.timelineData.bands || this.timelineData.bands.length === 0) {
            container.innerHTML = '<div class="text-center">No timeline data available</div>';
            return;
        }
        
        let html = '<div class="chromogram-grid">';
        
        // Render field labels
        html += '<div class="chromogram-fields">';
        for (const band of this.timelineData.bands) {
            for (const field of band.fields) {
                if (this.shouldShowField(field)) {
                    html += `<div class="chromogram-field">${field}</div>`;
                }
            }
        }
        html += '</div>';
        
        // Render timeline rows
        html += '<div class="chromogram-timeline-rows">';
        for (const band of this.timelineData.bands) {
            for (const field of band.fields) {
                if (this.shouldShowField(field)) {
                    html += this.renderTimelineRow(field, band);
                }
            }
        }
        html += '</div>';
        
        html += '</div>';
        container.innerHTML = html;
    }
    
    renderTimelineRow(field, band) {
        const fieldCells = this.timelineData.cells.filter(cell => cell.field === field);
        const color = this.getFieldColor(field);
        
        let html = `<div class="chromogram-timeline-row">`;
        
        // Render cells for this field
        for (const cell of fieldCells) {
            const changed = cell.changed ? 'changed' : 'unchanged';
            const opacity = cell.changed ? 1 : 0.3;
            
            html += `
                <div class="chromogram-cell ${changed}" 
                     style="background-color: ${color}; opacity: ${opacity};"
                     onclick="chromogramTimeline.showCellDetail('${field}', '${cell.date}', '${cell.value}')"
                     title="${cell.date}: ${cell.value}">
                </div>
            `;
        }
        
        // Add event markers
        const fieldEvents = this.timelineData.events.filter(event => 
            event.field === field || event.description.toLowerCase().includes(field.toLowerCase())
        );
        
        for (const event of fieldEvents) {
            const eventIndex = this.getEventIndex(event.date);
            if (eventIndex >= 0) {
                const severity = event.severity || 'low';
                const icon = this.getEventIcon(event.event_type);
                
                html += `
                    <div class="event-marker ${severity}" 
                         style="left: ${eventIndex * 18}px;"
                         onclick="chromogramTimeline.showEventDetail('${event.date}', '${event.event_type}')"
                         title="${event.description}">
                        ${icon}
                    </div>
                `;
            }
        }
        
        html += `</div>`;
        return html;
    }
    
    shouldShowField(field) {
        if (this.currentFieldFilter === 'all') return true;
        if (this.currentFieldFilter === 'metadata') {
            return ['title', 'agency', 'license'].includes(field);
        }
        if (this.currentFieldFilter === 'content') {
            return ['row_count', 'column_count', 'file_size'].includes(field);
        }
        return true;
    }
    
    getFieldColor(field) {
        const colors = {
            'title': '#006D77',
            'agency': '#2A9D8F',
            'license': '#FFB400',
            'row_count': '#D7263D',
            'column_count': '#8B5CF6',
            'file_size': '#9C27B0',
            'url': '#607D8B',
            'description': '#795548'
        };
        
        return colors[field] || '#6A6F73';
    }
    
    getEventIcon(eventType) {
        const icons = {
            'VANISHED': '👻',
            'LICENSE_CHANGED': '🔒',
            'SCHEMA_SHRINK': '✂️',
            'CONTENT_DRIFT': '',
            'URL_CHANGED': '🔗',
            'TITLE_CHANGED': '📝'
        };
        
        return icons[eventType] || '⚠️';
    }
    
    getEventIndex(eventDate) {
        if (!this.timelineData.cells) return -1;
        
        const uniqueDates = [...new Set(this.timelineData.cells.map(cell => cell.date))].sort();
        return uniqueDates.indexOf(eventDate);
    }
    
    renderEventTimeline() {
        const container = document.getElementById('eventTimeline');
        if (!container || !this.timelineData) return;
        
        const events = this.timelineData.events || [];
        
        if (events.length === 0) {
            container.innerHTML = '<div class="text-center">No events recorded</div>';
            return;
        }
        
        let html = '';
        
        for (const event of events) {
            const severity = event.severity || 'low';
            const icon = this.getEventIcon(event.event_type);
            const date = new Date(event.date).toLocaleDateString();
            
            html += `
                <div class="event-item" onclick="chromogramTimeline.showEventDetail('${event.date}', '${event.event_type}')">
                    <div class="event-date">${date}</div>
                    <div class="event-icon ${severity}">${icon}</div>
                    <div class="event-details">
                        <div class="event-type">${event.event_type || 'Unknown Event'}</div>
                        <div class="event-description">${event.description || 'No description'}</div>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    updateTimelineInfo() {
        const title = document.getElementById('timelineTitle');
        const info = document.getElementById('timelineInfo');
        
        if (!this.selectedDataset || !this.timelineData) {
            title.textContent = 'Chromogram Timeline';
            info.textContent = 'Select a dataset to view timeline';
            return;
        }
        
        const dataset = this.datasets.find(d => d.dataset_id === this.selectedDataset);
        const datasetName = dataset ? dataset.title : 'Unknown Dataset';
        
        title.textContent = `Chromogram Timeline - ${datasetName}`;
        
        const bands = this.timelineData.bands || [];
        const cells = this.timelineData.cells || [];
        const events = this.timelineData.events || [];
        
        const fieldCount = bands.reduce((sum, band) => sum + band.fields.length, 0);
        const changedCells = cells.filter(cell => cell.changed).length;
        const totalCells = cells.length;
        
        info.innerHTML = `
            <div class="timeline-stats">
                <div class="timeline-stat">
                    <span class="timeline-stat-value">${fieldCount}</span>
                    <span>Fields</span>
                </div>
                <div class="timeline-stat">
                    <span class="timeline-stat-value">${changedCells}/${totalCells}</span>
                    <span>Changes</span>
                </div>
                <div class="timeline-stat">
                    <span class="timeline-stat-value">${events.length}</span>
                    <span>Events</span>
                </div>
            </div>
        `;
    }
    
    filterFields() {
        const fieldFilter = document.getElementById('fieldFilter')?.value || 'all';
        this.currentFieldFilter = fieldFilter;
        
        if (this.timelineData) {
            this.renderChromogram();
        }
    }
    
    showCellDetail(field, date, value) {
        const modal = document.getElementById('cellDetailModal');
        const title = document.getElementById('cellDetailTitle');
        const content = document.getElementById('cellDetailContent');
        
        title.textContent = `${field} - ${new Date(date).toLocaleDateString()}`;
        
        content.innerHTML = `
            <div class="cell-detail-field">
                <h5>Field</h5>
                <div class="cell-detail-value">${field}</div>
            </div>
            <div class="cell-detail-field">
                <h5>Date</h5>
                <div class="cell-detail-value">${new Date(date).toLocaleString()}</div>
            </div>
            <div class="cell-detail-field">
                <h5>Value</h5>
                <div class="cell-detail-value cell-detail-changed">${value || 'N/A'}</div>
            </div>
        `;
        
        modal.style.display = 'block';
    }
    
    showEventDetail(date, eventType) {
        const modal = document.getElementById('cellDetailModal');
        const title = document.getElementById('cellDetailTitle');
        const content = document.getElementById('cellDetailContent');
        
        title.textContent = `Event - ${new Date(date).toLocaleDateString()}`;
        
        const event = this.timelineData.events.find(e => e.date === date && e.event_type === eventType);
        
        content.innerHTML = `
            <div class="cell-detail-field">
                <h5>Event Type</h5>
                <div class="cell-detail-value">${eventType}</div>
            </div>
            <div class="cell-detail-field">
                <h5>Date</h5>
                <div class="cell-detail-value">${new Date(date).toLocaleString()}</div>
            </div>
            <div class="cell-detail-field">
                <h5>Description</h5>
                <div class="cell-detail-value">${event?.description || 'No description available'}</div>
            </div>
            <div class="cell-detail-field">
                <h5>Severity</h5>
                <div class="cell-detail-value">${event?.severity || 'Unknown'}</div>
            </div>
        `;
        
        modal.style.display = 'block';
    }
    
    closeCellDetailModal() {
        const modal = document.getElementById('cellDetailModal');
        modal.style.display = 'none';
    }
    
    clearTimeline() {
        document.getElementById('chromogramTimeline').innerHTML = 
            '<div class="loading">Select a dataset to view timeline</div>';
        document.getElementById('eventTimeline').innerHTML = 
            '<div class="loading">Select a dataset to view events</div>';
        document.getElementById('timelineTitle').textContent = 'Chromogram Timeline';
        document.getElementById('timelineInfo').textContent = 'Select a dataset to view timeline';
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
function loadDatasetTimeline() {
    if (window.chromogramTimeline) {
        chromogramTimeline.loadDatasetTimeline();
    }
}

function updateTimeline() {
    if (window.chromogramTimeline) {
        chromogramTimeline.updateTimeline();
    }
}

function filterFields() {
    if (window.chromogramTimeline) {
        chromogramTimeline.filterFields();
    }
}

function exportTimeline() {
    // Implement export functionality
    console.log('Export timeline');
}

function closeCellDetailModal() {
    if (window.chromogramTimeline) {
        chromogramTimeline.closeCellDetailModal();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.chromogramTimeline = new ChromogramTimeline();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('cellDetailModal');
    if (event.target === modal) {
        closeCellDetailModal();
    }
});
