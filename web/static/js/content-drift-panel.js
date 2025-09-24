// Content Drift Panel JavaScript - Dataset State Historian

class ContentDriftPanel {
    constructor() {
        this.datasets = [];
        this.selectedDataset = null;
        this.driftData = null;
        this.charts = {};
        this.currentTimeRange = '30';
        this.currentDriftType = 'all';
        
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
                this.loadDatasetDrift();
            });
        }
        
        // Time range selection
        const timeRange = document.getElementById('timeRange');
        if (timeRange) {
            timeRange.addEventListener('change', () => {
                this.updateDriftAnalysis();
            });
        }
        
        // Drift type selection
        const driftType = document.getElementById('driftType');
        if (driftType) {
            driftType.addEventListener('change', () => {
                this.updateDriftAnalysis();
            });
        }
        
        // Severity filter
        const severityFilter = document.getElementById('severityFilter');
        if (severityFilter) {
            severityFilter.addEventListener('change', () => {
                this.filterDriftEvents();
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
    
    async loadDatasetDrift() {
        const datasetId = document.getElementById('datasetSelect')?.value;
        if (!datasetId) {
            this.clearDriftAnalysis();
            return;
        }
        
        this.selectedDataset = datasetId;
        await this.updateDriftAnalysis();
    }
    
    async updateDriftAnalysis() {
        if (!this.selectedDataset) return;
        
        try {
            this.showLoading(true);
            
            const timeRange = document.getElementById('timeRange')?.value || '30';
            const driftType = document.getElementById('driftType')?.value || 'all';
            
            this.currentTimeRange = timeRange;
            this.currentDriftType = driftType;
            
            // Load dataset timeline data
            const response = await fetch(`/api/dataset/${this.selectedDataset}/timeline`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.driftData = data;
            this.renderDriftSummary();
            this.renderCharts();
            this.renderSchemaEvolution();
            this.renderDriftEvents();
            
        } catch (error) {
            console.error('Error loading drift analysis:', error);
            this.showError('Failed to load drift analysis: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    renderDriftSummary() {
        if (!this.driftData || !this.driftData.timeline) return;
        
        const timeline = this.driftData.timeline;
        const totalSnapshots = timeline.length;
        
        // Calculate average similarity (mock calculation)
        const avgSimilarity = this.calculateAverageSimilarity(timeline);
        
        // Calculate max drift (mock calculation)
        const maxDrift = this.calculateMaxDrift(timeline);
        
        // Count drift events (mock calculation)
        const driftEvents = this.countDriftEvents(timeline);
        
        document.getElementById('totalSnapshots').textContent = totalSnapshots;
        document.getElementById('avgSimilarity').textContent = avgSimilarity.toFixed(2);
        document.getElementById('maxDrift').textContent = maxDrift.toFixed(2);
        document.getElementById('driftEvents').textContent = driftEvents;
        
        // Update drift info
        const dataset = this.datasets.find(d => d.dataset_id === this.selectedDataset);
        const datasetName = dataset ? dataset.title : 'Unknown Dataset';
        
        document.getElementById('driftInfo').innerHTML = `
            <div class="drift-stats">
                <div class="drift-stat-item">
                    <span class="drift-stat-value">${datasetName}</span>
                    <span>Dataset</span>
                </div>
                <div class="drift-stat-item">
                    <span class="drift-stat-value">${this.currentTimeRange} days</span>
                    <span>Time Range</span>
                </div>
            </div>
        `;
    }
    
    calculateAverageSimilarity(timeline) {
        // Mock calculation - in real implementation, this would use actual similarity scores
        if (timeline.length < 2) return 1.0;
        
        let totalSimilarity = 0;
        let comparisons = 0;
        
        for (let i = 1; i < timeline.length; i++) {
            const prev = timeline[i - 1];
            const curr = timeline[i];
            
            // Simple similarity based on row count changes
            const rowChange = Math.abs((curr.row_count || 0) - (prev.row_count || 0));
            const maxRows = Math.max(curr.row_count || 0, prev.row_count || 0);
            const similarity = maxRows > 0 ? 1 - (rowChange / maxRows) : 1;
            
            totalSimilarity += similarity;
            comparisons++;
        }
        
        return comparisons > 0 ? totalSimilarity / comparisons : 1.0;
    }
    
    calculateMaxDrift(timeline) {
        if (timeline.length < 2) return 0;
        
        let maxDrift = 0;
        
        for (let i = 1; i < timeline.length; i++) {
            const prev = timeline[i - 1];
            const curr = timeline[i];
            
            // Calculate drift based on multiple factors
            const rowDrift = this.calculateRowDrift(prev, curr);
            const columnDrift = this.calculateColumnDrift(prev, curr);
            const totalDrift = (rowDrift + columnDrift) / 2;
            
            maxDrift = Math.max(maxDrift, totalDrift);
        }
        
        return maxDrift;
    }
    
    calculateRowDrift(prev, curr) {
        const prevRows = prev.row_count || 0;
        const currRows = curr.row_count || 0;
        
        if (prevRows === 0 && currRows === 0) return 0;
        if (prevRows === 0) return 1;
        
        return Math.abs(currRows - prevRows) / Math.max(prevRows, 1);
    }
    
    calculateColumnDrift(prev, curr) {
        const prevCols = prev.column_count || 0;
        const currCols = curr.column_count || 0;
        
        if (prevCols === 0 && currCols === 0) return 0;
        if (prevCols === 0) return 1;
        
        return Math.abs(currCols - prevCols) / Math.max(prevCols, 1);
    }
    
    countDriftEvents(timeline) {
        let events = 0;
        
        for (let i = 1; i < timeline.length; i++) {
            const prev = timeline[i - 1];
            const curr = timeline[i];
            
            const rowDrift = this.calculateRowDrift(prev, curr);
            const columnDrift = this.calculateColumnDrift(prev, curr);
            
            // Count as drift event if significant change
            if (rowDrift > 0.1 || columnDrift > 0.1) {
                events++;
            }
        }
        
        return events;
    }
    
    renderCharts() {
        if (!this.driftData || !this.driftData.timeline) return;
        
        this.renderRowCountChart();
        this.renderSimilarityChart();
    }
    
    renderRowCountChart() {
        const ctx = document.getElementById('rowCountChart');
        if (!ctx) return;
        
        // Destroy existing chart
        if (this.charts.rowCount) {
            this.charts.rowCount.destroy();
        }
        
        const timeline = this.driftData.timeline;
        const labels = timeline.map(item => new Date(item.date).toLocaleDateString());
        const data = timeline.map(item => item.row_count || 0);
        
        this.charts.rowCount = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Row Count',
                    data: data,
                    borderColor: '#006D77',
                    backgroundColor: 'rgba(0, 109, 119, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
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
                            color: '#E0E0E0'
                        }
                    },
                    x: {
                        grid: {
                            color: '#E0E0E0'
                        }
                    }
                }
            }
        });
    }
    
    renderSimilarityChart() {
        const ctx = document.getElementById('similarityChart');
        if (!ctx) return;
        
        // Destroy existing chart
        if (this.charts.similarity) {
            this.charts.similarity.destroy();
        }
        
        const timeline = this.driftData.timeline;
        const labels = timeline.map(item => new Date(item.date).toLocaleDateString());
        const data = this.calculateSimilarityScores(timeline);
        
        this.charts.similarity = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Similarity Score',
                    data: data,
                    borderColor: '#2A9D8F',
                    backgroundColor: 'rgba(42, 157, 143, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
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
                        min: 0,
                        max: 1,
                        grid: {
                            color: '#E0E0E0'
                        }
                    },
                    x: {
                        grid: {
                            color: '#E0E0E0'
                        }
                    }
                }
            }
        });
    }
    
    calculateSimilarityScores(timeline) {
        const scores = [];
        
        for (let i = 0; i < timeline.length; i++) {
            if (i === 0) {
                scores.push(1.0);
            } else {
                const prev = timeline[i - 1];
                const curr = timeline[i];
                
                const rowDrift = this.calculateRowDrift(prev, curr);
                const columnDrift = this.calculateColumnDrift(prev, curr);
                const similarity = 1 - Math.max(rowDrift, columnDrift);
                
                scores.push(Math.max(0, similarity));
            }
        }
        
        return scores;
    }
    
    renderSchemaEvolution() {
        const container = document.getElementById('schemaEvolution');
        if (!container || !this.driftData || !this.driftData.timeline) return;
        
        const timeline = this.driftData.timeline;
        
        if (timeline.length === 0) {
            container.innerHTML = '<div class="text-center">No schema data available</div>';
            return;
        }
        
        let html = '<div class="schema-timeline">';
        
        for (let i = 0; i < timeline.length; i++) {
            const snapshot = timeline[i];
            const prevSnapshot = i > 0 ? timeline[i - 1] : null;
            
            const date = new Date(snapshot.date).toLocaleDateString();
            const columns = this.extractColumns(snapshot);
            const prevColumns = prevSnapshot ? this.extractColumns(prevSnapshot) : [];
            
            const columnChanges = this.calculateColumnChanges(prevColumns, columns);
            
            html += `
                <div class="schema-snapshot">
                    <div class="schema-date">${date}</div>
                    <div class="schema-columns">
                        ${columns.map(col => `
                            <div class="schema-column ${columnChanges[col] || ''}">${col}</div>
                        `).join('')}
                    </div>
                    <div class="schema-stats">
                        <span>${columns.length} columns</span>
                        ${Object.keys(columnChanges).length > 0 ? `
                            <span>${Object.values(columnChanges).filter(c => c === 'added').length} added</span>
                            <span>${Object.values(columnChanges).filter(c => c === 'removed').length} removed</span>
                        ` : ''}
                    </div>
                </div>
            `;
        }
        
        html += '</div>';
        container.innerHTML = html;
    }
    
    extractColumns(snapshot) {
        // Mock column extraction - in real implementation, this would parse schema data
        const mockColumns = ['id', 'name', 'description', 'created_at', 'updated_at'];
        return mockColumns.slice(0, snapshot.column_count || 0);
    }
    
    calculateColumnChanges(prevColumns, currColumns) {
        const changes = {};
        
        // Find added columns
        currColumns.forEach(col => {
            if (!prevColumns.includes(col)) {
                changes[col] = 'added';
            }
        });
        
        // Find removed columns
        prevColumns.forEach(col => {
            if (!currColumns.includes(col)) {
                changes[col] = 'removed';
            }
        });
        
        return changes;
    }
    
    renderDriftEvents() {
        const container = document.getElementById('driftEventsTimeline');
        if (!container || !this.driftData || !this.driftData.timeline) return;
        
        const timeline = this.driftData.timeline;
        const events = this.generateDriftEvents(timeline);
        
        if (events.length === 0) {
            container.innerHTML = '<div class="text-center">No drift events recorded</div>';
            return;
        }
        
        let html = '';
        
        for (const event of events) {
            const severity = event.severity || 'low';
            const icon = this.getEventIcon(event.type);
            const date = new Date(event.date).toLocaleDateString();
            
            html += `
                <div class="drift-event" onclick="contentDriftPanel.showEventDetail('${event.date}', '${event.type}')">
                    <div class="drift-event-date">${date}</div>
                    <div class="drift-event-icon ${severity}">${icon}</div>
                    <div class="drift-event-details">
                        <div class="drift-event-type">${event.type}</div>
                        <div class="drift-event-description">${event.description}</div>
                        <div class="drift-event-metrics">
                            <div class="drift-event-metric">
                                <span>Drift:</span>
                                <span class="drift-event-metric-value">${event.drift.toFixed(2)}</span>
                            </div>
                            <div class="drift-event-metric">
                                <span>Similarity:</span>
                                <span class="drift-event-metric-value">${event.similarity.toFixed(2)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    generateDriftEvents(timeline) {
        const events = [];
        
        for (let i = 1; i < timeline.length; i++) {
            const prev = timeline[i - 1];
            const curr = timeline[i];
            
            const rowDrift = this.calculateRowDrift(prev, curr);
            const columnDrift = this.calculateColumnDrift(prev, curr);
            const totalDrift = Math.max(rowDrift, columnDrift);
            
            if (totalDrift > 0.1) {
                const severity = totalDrift > 0.5 ? 'high' : totalDrift > 0.2 ? 'medium' : 'low';
                const type = rowDrift > columnDrift ? 'Row Count Drift' : 'Schema Drift';
                const description = `Significant change detected: ${totalDrift.toFixed(2)} drift score`;
                
                events.push({
                    date: curr.date,
                    type: type,
                    description: description,
                    severity: severity,
                    drift: totalDrift,
                    similarity: 1 - totalDrift
                });
            }
        }
        
        return events.sort((a, b) => new Date(b.date) - new Date(a.date));
    }
    
    getEventIcon(type) {
        const icons = {
            'Row Count Drift': '',
            'Schema Drift': '',
            'Content Drift': '',
            'Structure Change': '🏗️'
        };
        
        return icons[type] || '⚠️';
    }
    
    filterDriftEvents() {
        const severityFilter = document.getElementById('severityFilter')?.value || '';
        
        if (!this.driftData || !this.driftData.timeline) return;
        
        const timeline = this.driftData.timeline;
        const allEvents = this.generateDriftEvents(timeline);
        
        const filteredEvents = severityFilter ? 
            allEvents.filter(event => event.severity === severityFilter) : 
            allEvents;
        
        this.renderFilteredDriftEvents(filteredEvents);
    }
    
    renderFilteredDriftEvents(events) {
        const container = document.getElementById('driftEventsTimeline');
        if (!container) return;
        
        if (events.length === 0) {
            container.innerHTML = '<div class="text-center">No events match the selected filter</div>';
            return;
        }
        
        let html = '';
        
        for (const event of events) {
            const severity = event.severity || 'low';
            const icon = this.getEventIcon(event.type);
            const date = new Date(event.date).toLocaleDateString();
            
            html += `
                <div class="drift-event" onclick="contentDriftPanel.showEventDetail('${event.date}', '${event.type}')">
                    <div class="drift-event-date">${date}</div>
                    <div class="drift-event-icon ${severity}">${icon}</div>
                    <div class="drift-event-details">
                        <div class="drift-event-type">${event.type}</div>
                        <div class="drift-event-description">${event.description}</div>
                        <div class="drift-event-metrics">
                            <div class="drift-event-metric">
                                <span>Drift:</span>
                                <span class="drift-event-metric-value">${event.drift.toFixed(2)}</span>
                            </div>
                            <div class="drift-event-metric">
                                <span>Similarity:</span>
                                <span class="drift-event-metric-value">${event.similarity.toFixed(2)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        container.innerHTML = html;
    }
    
    showEventDetail(date, type) {
        const modal = document.getElementById('eventDetailModal');
        const title = document.getElementById('eventDetailTitle');
        const content = document.getElementById('eventDetailContent');
        
        title.textContent = `Event Details - ${type}`;
        
        const event = this.generateDriftEvents(this.driftData.timeline)
            .find(e => e.date === date && e.type === type);
        
        if (event) {
            content.innerHTML = `
                <div class="event-detail-section">
                    <h5>Event Information</h5>
                    <p><strong>Date:</strong> ${new Date(event.date).toLocaleString()}</p>
                    <p><strong>Type:</strong> ${event.type}</p>
                    <p><strong>Severity:</strong> <span class="similarity-indicator ${event.severity}">${event.severity}</span></p>
                    <p><strong>Description:</strong> ${event.description}</p>
                </div>
                
                <div class="event-detail-section">
                    <h5>Metrics</h5>
                    <p><strong>Drift Score:</strong> ${event.drift.toFixed(3)}</p>
                    <p><strong>Similarity Score:</strong> ${event.similarity.toFixed(3)}</p>
                </div>
            `;
        } else {
            content.innerHTML = '<div class="text-center">Event details not available</div>';
        }
        
        modal.style.display = 'block';
    }
    
    closeEventDetailModal() {
        const modal = document.getElementById('eventDetailModal');
        modal.style.display = 'none';
    }
    
    toggleChartType(chartType) {
        // Implement chart type toggling (line vs bar, etc.)
        console.log(`Toggle chart type for ${chartType}`);
    }
    
    clearDriftAnalysis() {
        document.getElementById('totalSnapshots').textContent = '-';
        document.getElementById('avgSimilarity').textContent = '-';
        document.getElementById('maxDrift').textContent = '-';
        document.getElementById('driftEvents').textContent = '-';
        document.getElementById('driftInfo').textContent = 'Select a dataset to view drift analysis';
        
        // Clear charts
        Object.values(this.charts).forEach(chart => {
            if (chart) chart.destroy();
        });
        this.charts = {};
        
        // Clear other content
        document.getElementById('schemaEvolution').innerHTML = 
            '<div class="loading">Select a dataset to view schema evolution</div>';
        document.getElementById('driftEventsTimeline').innerHTML = 
            '<div class="loading">Select a dataset to view drift events</div>';
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
function loadDatasetDrift() {
    if (window.contentDriftPanel) {
        contentDriftPanel.loadDatasetDrift();
    }
}

function updateDriftAnalysis() {
    if (window.contentDriftPanel) {
        contentDriftPanel.updateDriftAnalysis();
    }
}

function filterDriftEvents() {
    if (window.contentDriftPanel) {
        contentDriftPanel.filterDriftEvents();
    }
}

function exportDriftReport() {
    // Implement export functionality
    console.log('Export drift report');
}

function toggleChartType(chartType) {
    if (window.contentDriftPanel) {
        contentDriftPanel.toggleChartType(chartType);
    }
}

function closeEventDetailModal() {
    if (window.contentDriftPanel) {
        contentDriftPanel.closeEventDetailModal();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.contentDriftPanel = new ContentDriftPanel();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('eventDetailModal');
    if (event.target === modal) {
        closeEventDetailModal();
    }
});
