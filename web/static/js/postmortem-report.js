// Post-mortem Report JavaScript - Dataset State Historian

class PostmortemReport {
    constructor() {
        this.vanishedDatasets = [];
        this.selectedDataset = null;
        this.postmortemData = null;
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.loadVanishedDatasets();
    }
    
    setupEventListeners() {
        // Dataset selection
        const datasetSelect = document.getElementById('datasetSelect');
        if (datasetSelect) {
            datasetSelect.addEventListener('change', () => {
                this.loadPostmortem();
            });
        }
    }
    
    async loadVanishedDatasets() {
        try {
            this.showLoading(true);
            
            const response = await fetch('/api/vanished-datasets');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.vanishedDatasets = data.vanished_datasets || [];
            this.populateDatasetSelect();
            
        } catch (error) {
            console.error('Error loading vanished datasets:', error);
            this.showError('Failed to load vanished datasets: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    populateDatasetSelect() {
        const select = document.getElementById('datasetSelect');
        if (!select) return;
        
        select.innerHTML = '<option value="">Select a vanished dataset...</option>';
        
        this.vanishedDatasets.forEach(dataset => {
            const option = document.createElement('option');
            option.value = dataset.dataset_id;
            option.textContent = `${dataset.title || 'Untitled'} (${dataset.agency || 'Unknown'}) - Last seen: ${dataset.last_seen_date || 'Unknown'}`;
            select.appendChild(option);
        });
    }
    
    async loadPostmortem() {
        const datasetId = document.getElementById('datasetSelect')?.value;
        if (!datasetId) {
            this.clearPostmortem();
            return;
        }
        
        this.selectedDataset = datasetId;
        await this.generatePostmortem();
    }
    
    async generatePostmortem() {
        if (!this.selectedDataset) return;
        
        try {
            this.showLoading(true);
            
            const response = await fetch(`/api/postmortem/${this.selectedDataset}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.postmortemData = data;
            this.renderPostmortem();
            
        } catch (error) {
            console.error('Error generating postmortem:', error);
            this.showError('Failed to generate post-mortem: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    renderPostmortem() {
        const container = document.getElementById('postmortemReport');
        if (!container || !this.postmortemData) return;
        
        const dataset = this.postmortemData.dataset || {};
        const timeline = this.postmortemData.timeline || [];
        const analysis = this.postmortemData.analysis || {};
        const conclusion = this.postmortemData.conclusion || {};
        
        const lastSeen = dataset.last_seen || 'Unknown';
        const status = dataset.status || 'vanished';
        const agency = dataset.agency || 'Unknown Agency';
        const title = dataset.title || 'Untitled Dataset';
        
        let html = `
            <div class="postmortem-header">
                <h1 class="postmortem-title">Post-mortem Analysis Report</h1>
                <p class="postmortem-subtitle">Dataset: ${title}</p>
                <div class="postmortem-meta">
                    <span>Generated: ${new Date().toLocaleDateString()}</span>
                    <span>Agency: ${agency}</span>
                    <span>Last Seen: ${lastSeen}</span>
                </div>
            </div>
            
            <div class="postmortem-section">
                <h2>Dataset Information</h2>
                <div class="dataset-info">
                    <div class="dataset-info-grid">
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Dataset ID</div>
                            <div class="dataset-info-value">${dataset.dataset_id || 'Unknown'}</div>
                        </div>
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Title</div>
                            <div class="dataset-info-value">${title}</div>
                        </div>
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Agency</div>
                            <div class="dataset-info-value">${agency}</div>
                        </div>
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Status</div>
                            <div class="dataset-info-value">
                                <span class="dataset-status ${status}">${status}</span>
                            </div>
                        </div>
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Last Seen</div>
                            <div class="dataset-info-value">${lastSeen}</div>
                        </div>
                        <div class="dataset-info-item">
                            <div class="dataset-info-label">Original URL</div>
                            <div class="dataset-info-value">
                                <a href="${dataset.original_url || '#'}" target="_blank">${dataset.original_url || 'Not available'}</a>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        if (timeline.length > 0) {
            html += `
                <div class="postmortem-section">
                    <h2>Timeline of Events</h2>
                    <div class="timeline-section">
                        <p>Key changes and events leading to the dataset's disappearance:</p>
            `;
            
            for (const event of timeline) {
                const severity = event.severity || 'low';
                const date = new Date(event.date).toLocaleDateString();
                
                html += `
                    <div class="timeline-item">
                        <div class="timeline-date">${date}</div>
                        <div class="timeline-content">
                            <div class="timeline-event">${event.event_type || 'Unknown Event'}</div>
                            <div class="timeline-description">${event.description || 'No description available'}</div>
                            <div class="timeline-severity ${severity}">${severity}</div>
                        </div>
                    </div>
                `;
            }
            
            html += `
                    </div>
                </div>
            `;
        }
        
        if (analysis) {
            html += `
                <div class="postmortem-section">
                    <h2>Analysis</h2>
                    <div class="analysis-section">
                        <div class="analysis-grid">
                            <div class="analysis-item">
                                <div class="analysis-value">${analysis.total_snapshots || 0}</div>
                                <div class="analysis-label">Total Snapshots</div>
                                <div class="analysis-description">Number of times this dataset was monitored</div>
                            </div>
                            <div class="analysis-item">
                                <div class="analysis-value">${analysis.changes_detected || 0}</div>
                                <div class="analysis-label">Changes Detected</div>
                                <div class="analysis-description">Total number of changes recorded</div>
                            </div>
                            <div class="analysis-item">
                                <div class="analysis-value">${analysis.volatility_score || 0}</div>
                                <div class="analysis-label">Volatility Score</div>
                                <div class="analysis-description">Measure of how frequently the dataset changed</div>
                            </div>
                            <div class="analysis-item">
                                <div class="analysis-value">${analysis.days_active || 0}</div>
                                <div class="analysis-label">Days Active</div>
                                <div class="analysis-description">Number of days the dataset was monitored</div>
                            </div>
                        </div>
                        
                        <h3>Pattern Analysis</h3>
                        <p>${analysis.pattern_analysis || 'No pattern analysis available.'}</p>
                        
                        <h3>Key Metrics</h3>
                        <ul>
                            <li><strong>Average Similarity:</strong> ${analysis.avg_similarity || 'N/A'}</li>
                            <li><strong>Max Drift:</strong> ${analysis.max_drift || 'N/A'}</li>
                            <li><strong>Schema Changes:</strong> ${analysis.schema_changes || 0}</li>
                            <li><strong>Content Changes:</strong> ${analysis.content_changes || 0}</li>
                        </ul>
                    </div>
                </div>
            `;
        }
        
        if (conclusion) {
            html += `
                <div class="postmortem-section">
                    <h2>Conclusion</h2>
                    <div class="conclusion-section">
                        <div class="conclusion-item">
                            <div class="conclusion-label">Suspected Cause</div>
                            <div class="conclusion-value">${conclusion.suspected_cause || 'Unable to determine the exact cause of disappearance.'}</div>
                        </div>
                        
                        <div class="conclusion-item">
                            <div class="conclusion-label">Impact Assessment</div>
                            <div class="conclusion-value">${conclusion.impact_assessment || 'The impact of this dataset\'s disappearance could not be assessed.'}</div>
                        </div>
                        
                        <div class="conclusion-item">
                            <div class="conclusion-label">Recommendations</div>
                            <div class="conclusion-value">${conclusion.recommendations || 'No specific recommendations available.'}</div>
                        </div>
                        
                        <div class="conclusion-item">
                            <div class="conclusion-label">Prevention Measures</div>
                            <div class="conclusion-value">${conclusion.prevention_measures || 'Consider implementing monitoring alerts for similar datasets.'}</div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Archive Information
        const archiveSources = this.postmortemData.archive_sources || [];
        if (archiveSources.length > 0) {
            html += `
                <div class="postmortem-section">
                    <h2>Archive Information</h2>
                    <div class="archive-info">
                        <p>The following archival sources may contain copies of this dataset:</p>
                        <div class="archive-links">
            `;
            
            for (const source of archiveSources) {
                html += `
                    <a href="${source.url}" target="_blank" class="archive-link">
                        <span>${source.name || 'Archive'}</span>
                        <span>→</span>
                    </a>
                `;
            }
            
            html += `
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Footer
        html += `
            <div class="postmortem-section">
                <div style="text-align: center; color: var(--text-muted); font-size: var(--text-sm); border-top: 1px solid var(--border-light); padding-top: var(--space-4);">
                    <p>This report was generated by the Dataset State Historian on ${new Date().toLocaleString()}</p>
                    <p>For questions or additional analysis, please contact the system administrator.</p>
                </div>
            </div>
        `;
        
        container.innerHTML = html;
        container.classList.add('loaded');
    }
    
    clearPostmortem() {
        const container = document.getElementById('postmortemReport');
        if (container) {
            container.innerHTML = '<div class="loading">Select a dataset to generate post-mortem report</div>';
            container.classList.remove('loaded');
        }
    }
    
    showLoading(show) {
        const indicator = document.getElementById('loadingIndicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }
    
    showError(message) {
        console.error(message);
        const container = document.getElementById('postmortemReport');
        if (container) {
            container.innerHTML = `<div class="error text-center">${message}</div>`;
        }
    }
}

// Global functions for HTML onclick handlers
function loadPostmortem() {
    if (window.postmortemReport) {
        postmortemReport.loadPostmortem();
    }
}

function generatePostmortem() {
    if (window.postmortemReport) {
        postmortemReport.generatePostmortem();
    }
}

function exportPostmortem() {
    // Implement PDF export functionality
    console.log('Export post-mortem as PDF');
    
    // This would typically use a library like jsPDF or Puppeteer
    // to generate a PDF from the current post-mortem report
    const reportElement = document.getElementById('postmortemReport');
    if (reportElement) {
        // Basic print functionality as fallback
        window.print();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.postmortemReport = new PostmortemReport();
});
