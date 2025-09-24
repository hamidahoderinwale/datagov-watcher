// Field Diff Panel JavaScript - Dataset State Historian

class FieldDiffPanel {
    constructor() {
        this.datasets = [];
        this.selectedDataset = null;
        this.selectedField = null;
        this.fieldHistory = [];
        this.fieldFields = [];
        
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
                this.loadDatasetFields();
            });
        }
        
        // Field selection
        const fieldSelect = document.getElementById('fieldSelect');
        if (fieldSelect) {
            fieldSelect.addEventListener('change', () => {
                this.loadFieldHistory();
            });
        }
        
        // Date inputs
        const fromDate = document.getElementById('fromDate');
        const toDate = document.getElementById('toDate');
        
        if (fromDate) {
            fromDate.addEventListener('change', () => {
                this.updateDiffComparison();
            });
        }
        
        if (toDate) {
            toDate.addEventListener('change', () => {
                this.updateDiffComparison();
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
        
        select.innerHTML = '<option value="">Select a dataset...</option>';
        
        this.datasets.forEach(dataset => {
            const option = document.createElement('option');
            option.value = dataset.dataset_id;
            option.textContent = `${dataset.title || 'Untitled'} (${dataset.agency || 'Unknown'})`;
            select.appendChild(option);
        });
    }
    
    async loadDatasetFields() {
        const datasetId = document.getElementById('datasetSelect')?.value;
        if (!datasetId) {
            this.clearFieldSelection();
            return;
        }
        
        this.selectedDataset = datasetId;
        
        try {
            this.showLoading(true);
            
            // Load dataset timeline to get available fields
            const response = await fetch(`/api/dataset/${datasetId}/timeline`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.extractFields(data.timeline);
            this.populateFieldSelect();
            
        } catch (error) {
            console.error('Error loading dataset fields:', error);
            this.showError('Failed to load dataset fields: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    extractFields(timeline) {
        const fields = new Set();
        
        for (const snapshot of timeline) {
            Object.keys(snapshot).forEach(key => {
                if (key !== 'date' && snapshot[key] !== null && snapshot[key] !== undefined) {
                    fields.add(key);
                }
            });
        }
        
        this.fieldFields = Array.from(fields).sort();
    }
    
    populateFieldSelect() {
        const select = document.getElementById('fieldSelect');
        if (!select) return;
        
        select.innerHTML = '<option value="">Select a field...</option>';
        
        this.fieldFields.forEach(field => {
            const option = document.createElement('option');
            option.value = field;
            option.textContent = this.formatFieldName(field);
            select.appendChild(option);
        });
    }
    
    formatFieldName(field) {
        return field
            .replace(/_/g, ' ')
            .replace(/\b\w/g, l => l.toUpperCase());
    }
    
    async loadFieldHistory() {
        const field = document.getElementById('fieldSelect')?.value;
        if (!field || !this.selectedDataset) {
            this.clearFieldHistory();
            return;
        }
        
        this.selectedField = field;
        
        try {
            this.showLoading(true);
            
            const response = await fetch(`/api/field-history/${this.selectedDataset}/${field}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.fieldHistory = data.history || [];
            this.renderFieldHistory();
            this.updateFieldInfo();
            
        } catch (error) {
            console.error('Error loading field history:', error);
            this.showError('Failed to load field history: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    renderFieldHistory() {
        const container = document.getElementById('fieldHistoryTimeline');
        if (!container) return;
        
        if (this.fieldHistory.length === 0) {
            container.innerHTML = '<div class="text-center">No history available for this field</div>';
            return;
        }
        
        let html = '';
        let lastValue = null;
        
        for (const item of this.fieldHistory) {
            const changed = lastValue !== null && lastValue !== item.value;
            const changeClass = changed ? 'changed' : 'unchanged';
            const valueClass = changed ? 'changed' : 'unchanged';
            
            const date = new Date(item.date).toLocaleDateString();
            const time = new Date(item.date).toLocaleTimeString();
            
            html += `
                <div class="field-history-item ${changeClass}" 
                     onclick="fieldDiffPanel.showDiffDetail('${item.date}', '${item.value}')"
                     title="${date} ${time}: ${item.value || 'N/A'}">
                    <div class="field-history-date">${date}</div>
                    <div class="field-history-value ${valueClass}">${item.value || 'N/A'}</div>
                    <div class="field-history-change-indicator ${changeClass}">
                        ${changed ? '●' : '○'}
                    </div>
                </div>
            `;
            
            lastValue = item.value;
        }
        
        container.innerHTML = html;
    }
    
    updateFieldInfo() {
        const title = document.getElementById('fieldHistoryTitle');
        const info = document.getElementById('fieldInfo');
        
        if (!this.selectedField) {
            title.textContent = 'Field History';
            info.textContent = 'Select a field to view history';
            return;
        }
        
        const fieldName = this.formatFieldName(this.selectedField);
        title.textContent = `Field History - ${fieldName}`;
        
        const totalChanges = this.fieldHistory.filter((item, index) => 
            index > 0 && item.value !== this.fieldHistory[index - 1].value
        ).length;
        
        const firstValue = this.fieldHistory[0]?.value || 'N/A';
        const lastValue = this.fieldHistory[this.fieldHistory.length - 1]?.value || 'N/A';
        
        info.innerHTML = `
            <div class="field-stats">
                <div class="field-stat">
                    <span class="field-stat-value">${this.fieldHistory.length}</span>
                    <span>Snapshots</span>
                </div>
                <div class="field-stat">
                    <span class="field-stat-value">${totalChanges}</span>
                    <span>Changes</span>
                </div>
                <div class="field-stat">
                    <span class="field-stat-value">${firstValue}</span>
                    <span>First Value</span>
                </div>
                <div class="field-stat">
                    <span class="field-stat-value">${lastValue}</span>
                    <span>Last Value</span>
                </div>
            </div>
        `;
    }
    
    updateDiffComparison() {
        const fromDate = document.getElementById('fromDate')?.value;
        const toDate = document.getElementById('toDate')?.value;
        
        if (!fromDate || !toDate) {
            document.getElementById('diffComparison').innerHTML = 
                '<div class="loading">Select both dates to compare</div>';
            return;
        }
        
        this.compareDates();
    }
    
    async compareDates() {
        const fromDate = document.getElementById('fromDate')?.value;
        const toDate = document.getElementById('toDate')?.value;
        
        if (!fromDate || !toDate || !this.selectedDataset || !this.selectedField) {
            return;
        }
        
        try {
            this.showLoading(true);
            
            // Find the closest snapshots to the selected dates
            const fromSnapshot = this.findClosestSnapshot(fromDate);
            const toSnapshot = this.findClosestSnapshot(toDate);
            
            if (!fromSnapshot || !toSnapshot) {
                throw new Error('No snapshots found for the selected dates');
            }
            
            this.renderDiffComparison(fromSnapshot, toSnapshot);
            
        } catch (error) {
            console.error('Error comparing dates:', error);
            this.showError('Failed to compare dates: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    findClosestSnapshot(targetDate) {
        const target = new Date(targetDate);
        let closest = null;
        let minDiff = Infinity;
        
        for (const item of this.fieldHistory) {
            const itemDate = new Date(item.date);
            const diff = Math.abs(itemDate - target);
            
            if (diff < minDiff) {
                minDiff = diff;
                closest = item;
            }
        }
        
        return closest;
    }
    
    renderDiffComparison(fromSnapshot, toSnapshot) {
        const container = document.getElementById('diffComparison');
        
        const fromDate = new Date(fromSnapshot.date).toLocaleDateString();
        const toDate = new Date(toSnapshot.date).toLocaleDateString();
        const fromValue = fromSnapshot.value || 'N/A';
        const toValue = toSnapshot.value || 'N/A';
        const changed = fromValue !== toValue;
        
        container.innerHTML = `
            <div class="diff-comparison-header">
                <div class="diff-comparison-date">${fromDate}</div>
                <div class="diff-comparison-date">${toDate}</div>
            </div>
            <div class="diff-comparison-content">
                <div class="diff-comparison-side from">
                    <div class="diff-comparison-value ${changed ? 'changed' : 'unchanged'}">${fromValue}</div>
                </div>
                <div class="diff-comparison-side to">
                    <div class="diff-comparison-value ${changed ? 'changed' : 'unchanged'}">${toValue}</div>
                </div>
            </div>
        `;
    }
    
    showDiffDetail(date, value) {
        const modal = document.getElementById('diffDetailModal');
        const title = document.getElementById('diffDetailTitle');
        const content = document.getElementById('diffDetailContent');
        
        title.textContent = `${this.formatFieldName(this.selectedField)} - ${new Date(date).toLocaleDateString()}`;
        
        const item = this.fieldHistory.find(h => h.date === date);
        const index = this.fieldHistory.findIndex(h => h.date === date);
        const prevItem = index > 0 ? this.fieldHistory[index - 1] : null;
        const nextItem = index < this.fieldHistory.length - 1 ? this.fieldHistory[index + 1] : null;
        
        content.innerHTML = `
            <div class="diff-detail-section">
                <h5>Current Value</h5>
                <div class="diff-detail-timeline">
                    <div class="diff-detail-period">
                        <div class="diff-detail-date">${new Date(date).toLocaleString()}</div>
                        <div class="diff-detail-value changed">${value || 'N/A'}</div>
                    </div>
                </div>
            </div>
            
            ${prevItem ? `
                <div class="diff-detail-section">
                    <h5>Previous Value</h5>
                    <div class="diff-detail-timeline">
                        <div class="diff-detail-period">
                            <div class="diff-detail-date">${new Date(prevItem.date).toLocaleString()}</div>
                            <div class="diff-detail-value ${prevItem.value !== value ? 'changed' : 'unchanged'}">${prevItem.value || 'N/A'}</div>
                        </div>
                    </div>
                </div>
            ` : ''}
            
            ${nextItem ? `
                <div class="diff-detail-section">
                    <h5>Next Value</h5>
                    <div class="diff-detail-timeline">
                        <div class="diff-detail-period">
                            <div class="diff-detail-date">${new Date(nextItem.date).toLocaleString()}</div>
                            <div class="diff-detail-value ${nextItem.value !== value ? 'changed' : 'unchanged'}">${nextItem.value || 'N/A'}</div>
                        </div>
                    </div>
                </div>
            ` : ''}
        `;
        
        modal.style.display = 'block';
    }
    
    closeDiffDetailModal() {
        const modal = document.getElementById('diffDetailModal');
        modal.style.display = 'none';
    }
    
    clearFieldSelection() {
        document.getElementById('fieldSelect').innerHTML = '<option value="">Select a field...</option>';
        this.clearFieldHistory();
    }
    
    clearFieldHistory() {
        document.getElementById('fieldHistoryTimeline').innerHTML = 
            '<div class="loading">Select a field to view history</div>';
        document.getElementById('fieldHistoryTitle').textContent = 'Field History';
        document.getElementById('fieldInfo').textContent = 'Select a field to view history';
        document.getElementById('diffComparison').innerHTML = 
            '<div class="loading">Select dates to compare</div>';
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
function loadDatasetFields() {
    if (window.fieldDiffPanel) {
        fieldDiffPanel.loadDatasetFields();
    }
}

function loadFieldHistory() {
    if (window.fieldDiffPanel) {
        fieldDiffPanel.loadFieldHistory();
    }
}

function updateDiffComparison() {
    if (window.fieldDiffPanel) {
        fieldDiffPanel.updateDiffComparison();
    }
}

function compareDates() {
    if (window.fieldDiffPanel) {
        fieldDiffPanel.compareDates();
    }
}

function exportFieldDiff() {
    // Implement export functionality
    console.log('Export field diff');
}

function closeDiffDetailModal() {
    if (window.fieldDiffPanel) {
        fieldDiffPanel.closeDiffDetailModal();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.fieldDiffPanel = new FieldDiffPanel();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('diffDetailModal');
    if (event.target === modal) {
        closeDiffDetailModal();
    }
});
