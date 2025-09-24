// Change Log JavaScript - Dataset State Historian

class ChangeLog {
    constructor() {
        this.changes = [];
        this.filteredChanges = [];
        this.datasets = [];
        this.currentPage = 1;
        this.itemsPerPage = 20;
        this.sortBy = 'date';
        this.sortOrder = 'desc';
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.loadDatasets();
        this.loadChanges();
    }
    
    setupEventListeners() {
        // Filter changes
        const filters = ['datasetFilter', 'eventTypeFilter', 'severityFilter', 'dateRangeFilter'];
        filters.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', () => {
                    this.filterChanges();
                });
            }
        });
        
        // Sort changes
        const sortElements = ['sortBy', 'sortOrder'];
        sortElements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', () => {
                    this.sortChanges();
                });
            }
        });
    }
    
    async loadDatasets() {
        try {
            const response = await fetch('/api/datasets');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.datasets = data;
            this.populateDatasetFilter();
            
        } catch (error) {
            console.error('Error loading datasets:', error);
        }
    }
    
    populateDatasetFilter() {
        const select = document.getElementById('datasetFilter');
        if (!select) return;
        
        select.innerHTML = '<option value="">All Datasets</option>';
        
        this.datasets.forEach(dataset => {
            const option = document.createElement('option');
            option.value = dataset.dataset_id;
            option.textContent = `${dataset.title || 'Untitled'} (${dataset.agency || 'Unknown'})`;
            select.appendChild(option);
        });
    }
    
    async loadChanges() {
        try {
            this.showLoading(true);
            
            const response = await fetch('/api/events/normalized');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.changes = data.events || [];
            this.filteredChanges = [...this.changes];
            this.renderChanges();
            this.renderChangeSummary();
            
        } catch (error) {
            console.error('Error loading changes:', error);
            this.showError('Failed to load changes: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    filterChanges() {
        const datasetFilter = document.getElementById('datasetFilter')?.value || '';
        const eventTypeFilter = document.getElementById('eventTypeFilter')?.value || '';
        const severityFilter = document.getElementById('severityFilter')?.value || '';
        const dateRangeFilter = document.getElementById('dateRangeFilter')?.value || 'all';
        
        this.filteredChanges = this.changes.filter(change => {
            // Dataset filter
            if (datasetFilter && change.dataset_id !== datasetFilter) {
                return false;
            }
            
            // Event type filter
            if (eventTypeFilter && change.event_type !== eventTypeFilter) {
                return false;
            }
            
            // Severity filter
            if (severityFilter && change.severity !== severityFilter) {
                return false;
            }
            
            // Date range filter
            if (dateRangeFilter !== 'all') {
                const hours = parseInt(dateRangeFilter);
                const cutoffDate = new Date();
                cutoffDate.setHours(cutoffDate.getHours() - hours);
                const changeDate = new Date(change.date);
                
                if (changeDate < cutoffDate) {
                    return false;
                }
            }
            
            return true;
        });
        
        this.currentPage = 1;
        this.renderChanges();
        this.renderChangeSummary();
    }
    
    sortChanges() {
        this.sortBy = document.getElementById('sortBy')?.value || 'date';
        this.sortOrder = document.getElementById('sortOrder')?.value || 'desc';
        
        this.filteredChanges.sort((a, b) => {
            let aValue, bValue;
            
            switch (this.sortBy) {
                case 'date':
                    aValue = new Date(a.date);
                    bValue = new Date(b.date);
                    break;
                case 'severity':
                    const severityOrder = { 'high': 3, 'medium': 2, 'low': 1 };
                    aValue = severityOrder[a.severity] || 0;
                    bValue = severityOrder[b.severity] || 0;
                    break;
                case 'type':
                    aValue = a.event_type || '';
                    bValue = b.event_type || '';
                    break;
                case 'dataset':
                    aValue = a.dataset_title || '';
                    bValue = b.dataset_title || '';
                    break;
                default:
                    aValue = a[this.sortBy] || '';
                    bValue = b[this.sortBy] || '';
            }
            
            if (aValue < bValue) return this.sortOrder === 'asc' ? -1 : 1;
            if (aValue > bValue) return this.sortOrder === 'asc' ? 1 : -1;
            return 0;
        });
        
        this.renderChanges();
    }
    
    renderChanges() {
        const container = document.getElementById('changeLogBody');
        if (!container) return;
        
        if (this.filteredChanges.length === 0) {
            container.innerHTML = '<div class="text-center">No changes found</div>';
            return;
        }
        
        // Calculate pagination
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = Math.min(startIndex + this.itemsPerPage, this.filteredChanges.length);
        const pageChanges = this.filteredChanges.slice(startIndex, endIndex);
        
        let html = '';
        
        for (const change of pageChanges) {
            const severity = change.severity || 'low';
            const date = new Date(change.date).toLocaleDateString();
            const time = new Date(change.date).toLocaleTimeString();
            const eventType = change.event_type || 'Unknown';
            const datasetTitle = change.dataset_title || change.dataset_id || 'Unknown Dataset';
            const description = change.description || 'No description available';
            
            html += `
                <div class="change-row ${severity}-severity" 
                     onclick="changeLog.showChangeDetail('${change.id || change.date}')"
                     title="${date} ${time}: ${eventType}">
                    <div class="col-date">${date}</div>
                    <div class="col-type">
                        <div class="event-type">
                            <span class="event-type-icon ${eventType.toLowerCase().replace('_', '-')}">${this.getEventIcon(eventType)}</span>
                            <span>${eventType}</span>
                        </div>
                    </div>
                    <div class="col-severity">
                        <span class="severity-indicator ${severity}">${severity}</span>
                    </div>
                    <div class="col-dataset">${datasetTitle}</div>
                    <div class="col-description">${description}</div>
                    <div class="col-actions">
                        <button class="action-btn" onclick="event.stopPropagation(); changeLog.showChangeDetail('${change.id || change.date}')">
                            View
                        </button>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
        this.renderPagination();
    }
    
    renderPagination() {
        const pagination = document.getElementById('changePagination');
        if (!pagination) return;
        
        const totalPages = Math.ceil(this.filteredChanges.length / this.itemsPerPage);
        
        if (totalPages <= 1) {
            pagination.innerHTML = '';
            return;
        }
        
        let html = '';
        
        // Previous button
        html += `<button ${this.currentPage === 1 ? 'disabled' : ''} onclick="changeLog.goToPage(${this.currentPage - 1})">Previous</button>`;
        
        // Page numbers
        const startPage = Math.max(1, this.currentPage - 2);
        const endPage = Math.min(totalPages, this.currentPage + 2);
        
        if (startPage > 1) {
            html += `<button onclick="changeLog.goToPage(1)">1</button>`;
            if (startPage > 2) {
                html += `<span>...</span>`;
            }
        }
        
        for (let i = startPage; i <= endPage; i++) {
            const active = i === this.currentPage ? 'active' : '';
            html += `<button class="${active}" onclick="changeLog.goToPage(${i})">${i}</button>`;
        }
        
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                html += `<span>...</span>`;
            }
            html += `<button onclick="changeLog.goToPage(${totalPages})">${totalPages}</button>`;
        }
        
        // Next button
        html += `<button ${this.currentPage === totalPages ? 'disabled' : ''} onclick="changeLog.goToPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = html;
    }
    
    goToPage(page) {
        const totalPages = Math.ceil(this.filteredChanges.length / this.itemsPerPage);
        if (page >= 1 && page <= totalPages) {
            this.currentPage = page;
            this.renderChanges();
        }
    }
    
    renderChangeSummary() {
        const totalChanges = this.filteredChanges.length;
        const highSeverityChanges = this.filteredChanges.filter(c => c.severity === 'high').length;
        const vanishedDatasets = this.filteredChanges.filter(c => c.event_type === 'VANISHED').length;
        
        // Calculate recent changes (last 24 hours)
        const cutoffDate = new Date();
        cutoffDate.setHours(cutoffDate.getHours() - 24);
        const recentChanges = this.filteredChanges.filter(c => new Date(c.date) > cutoffDate).length;
        
        document.getElementById('totalChanges').textContent = totalChanges;
        document.getElementById('highSeverityChanges').textContent = highSeverityChanges;
        document.getElementById('vanishedDatasets').textContent = vanishedDatasets;
        document.getElementById('recentChanges').textContent = recentChanges;
        
        // Update change info
        document.getElementById('changeInfo').innerHTML = `
            <div class="change-stats">
                <div class="change-stat-item">
                    <span class="change-stat-value">${totalChanges}</span>
                    <span>Total Changes</span>
                </div>
                <div class="change-stat-item">
                    <span class="change-stat-value">${this.currentPage}</span>
                    <span>Page</span>
                </div>
            </div>
        `;
    }
    
    getEventIcon(eventType) {
        const icons = {
            'VANISHED': 'VANISHED',
            'LICENSE_CHANGED': 'LICENSE',
            'SCHEMA_SHRINK': 'SCHEMA',
            'CONTENT_DRIFT': 'CONTENT',
            'URL_CHANGED': 'URL',
            'TITLE_CHANGED': 'TITLE',
            'SCHEMA_EXPAND': 'SCHEMA',
            'CONTENT_GROWTH': 'GROWTH',
            'AVAILABILITY_CHANGE': 'WARNING'
        };
        
        return icons[eventType] || 'WARNING';
    }
    
    showChangeDetail(changeId) {
        const change = this.filteredChanges.find(c => (c.id || c.date) === changeId);
        if (!change) return;
        
        const modal = document.getElementById('changeDetailModal');
        const title = document.getElementById('changeDetailTitle');
        const content = document.getElementById('changeDetailContent');
        
        title.textContent = `Change Details - ${change.event_type || 'Unknown Event'}`;
        
        const date = new Date(change.date).toLocaleString();
        const severity = change.severity || 'unknown';
        const eventType = change.event_type || 'Unknown';
        const datasetTitle = change.dataset_title || change.dataset_id || 'Unknown Dataset';
        const description = change.description || 'No description available';
        
        content.innerHTML = `
            <div class="change-detail-section">
                <h5>Event Information</h5>
                <p><strong>Date:</strong> ${date}</p>
                <p><strong>Event Type:</strong> ${eventType}</p>
                <p><strong>Severity:</strong> <span class="severity-indicator ${severity}">${severity}</span></p>
                <p><strong>Dataset:</strong> ${datasetTitle}</p>
                <p><strong>Description:</strong> ${description}</p>
            </div>
            
            ${change.old_value || change.new_value ? `
                <div class="change-detail-section">
                    <h5>Change Details</h5>
                    ${change.old_value ? `<p><strong>Previous Value:</strong> ${change.old_value}</p>` : ''}
                    ${change.new_value ? `<p><strong>New Value:</strong> ${change.new_value}</p>` : ''}
                </div>
            ` : ''}
            
            ${change.metadata ? `
                <div class="change-detail-section">
                    <h5>Additional Information</h5>
                    <pre>${JSON.stringify(change.metadata, null, 2)}</pre>
                </div>
            ` : ''}
        `;
        
        modal.style.display = 'block';
    }
    
    closeChangeDetailModal() {
        const modal = document.getElementById('changeDetailModal');
        modal.style.display = 'none';
    }
    
    clearFilters() {
        document.getElementById('datasetFilter').value = '';
        document.getElementById('eventTypeFilter').value = '';
        document.getElementById('severityFilter').value = '';
        document.getElementById('dateRangeFilter').value = '24';
        
        this.filteredChanges = [...this.changes];
        this.currentPage = 1;
        this.renderChanges();
        this.renderChangeSummary();
    }
    
    showLoading(show) {
        const indicator = document.getElementById('loadingIndicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }
    
    showError(message) {
        console.error(message);
        const container = document.getElementById('changeLogBody');
        if (container) {
            container.innerHTML = `<div class="error text-center">${message}</div>`;
        }
    }
}

// Global functions for HTML onclick handlers
function filterChanges() {
    if (window.changeLog) {
        changeLog.filterChanges();
    }
}

function sortChanges() {
    if (window.changeLog) {
        changeLog.sortChanges();
    }
}

function loadChanges() {
    if (window.changeLog) {
        changeLog.loadChanges();
    }
}

function clearFilters() {
    if (window.changeLog) {
        changeLog.clearFilters();
    }
}

function exportChanges() {
    // Implement export functionality
    console.log('Export changes');
}

function closeChangeDetailModal() {
    if (window.changeLog) {
        changeLog.closeChangeDetailModal();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.changeLog = new ChangeLog();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('changeDetailModal');
    if (event.target === modal) {
        closeChangeDetailModal();
    }
});
