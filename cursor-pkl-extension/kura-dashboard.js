/**
 * Enhanced Kura Dashboard JavaScript
 * Provides hierarchical clustering visualization, UMAP plots, and advanced analytics
 */

class KuraDashboard {
    constructor() {
        this.sessions = [];
        this.clusters = [];
        this.umapData = [];
        this.selectedSessions = new Set();
        this.currentColorBy = 'intent';
        this.clusterTree = null;
        this.umapPlot = null;
        
        this.initializeEventListeners();
        this.loadData();
    }

    initializeEventListeners() {
        // Header controls
        document.getElementById('refresh-btn').addEventListener('click', () => this.loadData());
        document.getElementById('export-btn').addEventListener('click', () => this.exportData());
        document.getElementById('search-btn').addEventListener('click', () => this.performSearch());
        document.getElementById('global-search').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.performSearch();
        });

        // Cluster tree controls
        document.getElementById('expand-all-btn').addEventListener('click', () => this.expandAllClusters());
        document.getElementById('collapse-all-btn').addEventListener('click', () => this.collapseAllClusters());

        // UMAP controls
        document.getElementById('color-by-select').addEventListener('change', (e) => {
            this.currentColorBy = e.target.value;
            this.updateUMAPColors();
        });
        document.getElementById('reset-zoom-btn').addEventListener('click', () => this.resetUMAPZoom());
        document.getElementById('fullscreen-btn').addEventListener('click', () => this.toggleFullscreen());

        // Pattern insights
        document.getElementById('insight-type-select').addEventListener('change', (e) => {
            this.updatePatternInsights(e.target.value);
        });

        // Quick actions
        document.getElementById('generate-notebook-btn').addEventListener('click', () => this.showNotebookModal());
        document.getElementById('export-cluster-btn').addEventListener('click', () => this.exportSelectedCluster());
        document.getElementById('create-procedure-btn').addEventListener('click', () => this.showProcedureModal());
        document.getElementById('share-insights-btn').addEventListener('click', () => this.shareInsights());

        // Session details
        document.getElementById('close-details-btn').addEventListener('click', () => this.closeSessionDetails());

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => this.handleKeyboardShortcuts(e));
    }

    async loadData() {
        this.showLoading(true);
        
        try {
            // Load PKL sessions and run Kura analysis
            const response = await fetch('/api/sessions/analyze-with-kura', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    test_mode: true, // Use test mode for now
                    include_dashboard_data: true
                })
            });

            if (!response.ok) {
                throw new Error('Failed to load Kura analysis');
            }

            const data = await response.json();
            
            if (data.success) {
                this.sessions = data.sessions || [];
                this.clusters = data.clusters || [];
                this.umapData = data.umap_coordinates || [];
                
                this.renderClusterTree();
                this.renderUMAPPlot();
                this.updateStatistics();
                this.updatePatternInsights('success_patterns');
            } else {
                throw new Error(data.error || 'Unknown error');
            }
        } catch (error) {
            console.error('Error loading data:', error);
            this.showError('Failed to load Kura analysis data: ' + error.message);
            
            // Load mock data for demonstration
            this.loadMockData();
        } finally {
            this.showLoading(false);
        }
    }

    loadMockData() {
        // Mock data for demonstration
        this.sessions = [
            {
                id: 'session_001',
                intent: 'explore',
                outcome: 'success',
                currentFile: 'data_analysis.ipynb',
                timestamp: '2024-01-15T10:30:00Z',
                confidence: 0.92,
                summary: 'Customer dataset analysis and pattern identification'
            },
            {
                id: 'session_002', 
                intent: 'debug',
                outcome: 'stuck',
                currentFile: 'model_training.py',
                timestamp: '2024-01-15T14:20:00Z',
                confidence: 0.65,
                summary: 'Machine learning model debugging session'
            },
            {
                id: 'session_003',
                intent: 'implement',
                outcome: 'success',
                currentFile: 'visualization.py',
                timestamp: '2024-01-16T09:15:00Z',
                confidence: 0.88,
                summary: 'Interactive dashboard implementation'
            }
        ];

        this.clusters = [
            {
                id: 'cluster_explore',
                name: 'Data Exploration Tasks',
                sessions: ['session_001'],
                size: 1,
                success_rate: 0.92
            },
            {
                id: 'cluster_debug',
                name: 'Debugging Sessions',
                sessions: ['session_002'],
                size: 1,
                success_rate: 0.65
            },
            {
                id: 'cluster_implement',
                name: 'Implementation Tasks',
                sessions: ['session_003'],
                size: 1,
                success_rate: 0.88
            }
        ];

        this.umapData = [
            { id: 'session_001', x: 0.2, y: 0.7, cluster: 'explore', intent: 'explore', outcome: 'success' },
            { id: 'session_002', x: 0.8, y: 0.3, cluster: 'debug', intent: 'debug', outcome: 'stuck' },
            { id: 'session_003', x: 0.5, y: 0.9, cluster: 'implement', intent: 'implement', outcome: 'success' }
        ];

        this.renderClusterTree();
        this.renderUMAPPlot();
        this.updateStatistics();
        this.updatePatternInsights('success_patterns');
    }

    renderClusterTree() {
        const container = document.getElementById('cluster-tree');
        container.innerHTML = '';

        // Create hierarchical structure
        const hierarchicalClusters = this.buildHierarchy();
        
        // Render tree
        const treeHtml = this.renderClusterNode(hierarchicalClusters, 0);
        container.innerHTML = treeHtml;

        // Add event listeners
        container.querySelectorAll('.cluster-node').forEach(node => {
            node.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectCluster(node.dataset.clusterId);
            });
        });

        container.querySelectorAll('.cluster-toggle').forEach(toggle => {
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleCluster(toggle.parentElement);
            });
        });
    }

    buildHierarchy() {
        // Build hierarchical cluster structure
        // This is a simplified version - in real implementation, this would come from Kura
        return {
            id: 'root',
            name: 'All Sessions',
            size: this.sessions.length,
            children: [
                {
                    id: 'data_science',
                    name: 'Data Science Workflows',
                    size: 2,
                    children: [
                        {
                            id: 'cluster_explore',
                            name: 'Data Exploration',
                            size: 1,
                            sessions: ['session_001']
                        },
                        {
                            id: 'cluster_implement',
                            name: 'Implementation',
                            size: 1,
                            sessions: ['session_003']
                        }
                    ]
                },
                {
                    id: 'problem_solving',
                    name: 'Problem Solving',
                    size: 1,
                    children: [
                        {
                            id: 'cluster_debug',
                            name: 'Debugging',
                            size: 1,
                            sessions: ['session_002']
                        }
                    ]
                }
            ]
        };
    }

    renderClusterNode(node, depth) {
        const indent = '  '.repeat(depth);
        const hasChildren = node.children && node.children.length > 0;
        const icon = hasChildren ? '📁' : '📄';
        const toggle = hasChildren ? '▼' : '';
        
        let html = `
            <div class="cluster-node" data-cluster-id="${node.id}" style="margin-left: ${depth * 16}px">
                <span class="cluster-toggle">${toggle}</span>
                <span class="cluster-icon">${icon}</span>
                <span class="cluster-label">${node.name}</span>
                <span class="cluster-count">(${node.size})</span>
            </div>
        `;

        if (hasChildren) {
            html += `<div class="cluster-children" data-cluster-id="${node.id}">`;
            for (const child of node.children) {
                html += this.renderClusterNode(child, depth + 1);
            }
            html += '</div>';
        }

        return html;
    }

    renderUMAPPlot() {
        const container = document.getElementById('umap-plot');
        
        // Prepare data for Plotly
        const traces = this.prepareUMAPTraces();
        
        const layout = {
            title: {
                text: 'Session Relationships (UMAP Projection)',
                font: { size: 16 }
            },
            xaxis: {
                title: 'UMAP Dimension 1',
                showgrid: true,
                zeroline: false
            },
            yaxis: {
                title: 'UMAP Dimension 2', 
                showgrid: true,
                zeroline: false
            },
            hovermode: 'closest',
            showlegend: true,
            legend: {
                x: 1,
                y: 1,
                xanchor: 'left',
                yanchor: 'top'
            },
            margin: { t: 50, r: 150, b: 50, l: 50 },
            dragmode: 'select'
        };

        const config = {
            displayModeBar: true,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d'],
            displaylogo: false,
            responsive: true
        };

        Plotly.newPlot(container, traces, layout, config);

        // Handle selection events
        container.on('plotly_selected', (eventData) => {
            this.handleUMAPSelection(eventData);
        });

        container.on('plotly_click', (eventData) => {
            this.handleUMAPClick(eventData);
        });

        this.umapPlot = container;
    }

    prepareUMAPTraces() {
        const colorMap = this.getColorMap();
        const groupedData = {};

        // Group data by the current color-by attribute
        this.umapData.forEach(point => {
            const groupKey = point[this.currentColorBy] || 'unknown';
            if (!groupedData[groupKey]) {
                groupedData[groupKey] = {
                    x: [],
                    y: [],
                    text: [],
                    ids: [],
                    color: colorMap[groupKey] || '#999999'
                };
            }

            groupedData[groupKey].x.push(point.x);
            groupedData[groupKey].y.push(point.y);
            groupedData[groupKey].text.push(this.formatHoverText(point));
            groupedData[groupKey].ids.push(point.id);
        });

        // Create traces
        const traces = [];
        Object.entries(groupedData).forEach(([groupKey, data]) => {
            traces.push({
                x: data.x,
                y: data.y,
                mode: 'markers',
                type: 'scatter',
                name: groupKey,
                text: data.text,
                ids: data.ids,
                hovertemplate: '%{text}<extra></extra>',
                marker: {
                    color: data.color,
                    size: 10,
                    opacity: 0.7,
                    line: {
                        width: 1,
                        color: 'white'
                    }
                }
            });
        });

        return traces;
    }

    getColorMap() {
        const colorMaps = {
            intent: {
                'explore': '#10b981',
                'debug': '#ef4444', 
                'implement': '#2563eb',
                'refactor': '#f59e0b'
            },
            outcome: {
                'success': '#10b981',
                'stuck': '#ef4444',
                'in-progress': '#f59e0b'
            },
            file_type: {
                '.ipynb': '#ff6b35',
                '.py': '#3776ab',
                '.js': '#f7df1e',
                '.ts': '#3178c6'
            },
            cluster: {
                'explore': '#10b981',
                'debug': '#ef4444',
                'implement': '#2563eb'
            }
        };

        return colorMaps[this.currentColorBy] || {};
    }

    formatHoverText(point) {
        const session = this.sessions.find(s => s.id === point.id);
        if (!session) return point.id;

        return `
            <b>${session.id}</b><br>
            Intent: ${session.intent}<br>
            Outcome: ${session.outcome}<br>
            File: ${session.currentFile}<br>
            Confidence: ${(session.confidence * 100).toFixed(1)}%<br>
            ${session.summary || ''}
        `.trim();
    }

    updateStatistics() {
        // Update status bar
        document.getElementById('session-count').textContent = `${this.sessions.length} sessions loaded`;
        document.getElementById('cluster-count').textContent = `${this.clusters.length} clusters`;
        document.getElementById('selected-count').textContent = `${this.selectedSessions.size} selected`;
        document.getElementById('last-updated').textContent = `Updated ${new Date().toLocaleTimeString()}`;

        // Update cluster statistics
        const statsContainer = document.getElementById('cluster-stats-content');
        const stats = this.calculateStatistics();
        
        statsContainer.innerHTML = `
            <div class="stat-item">
                <span class="stat-label">Success Rate</span>
                <span class="stat-value">${(stats.successRate * 100).toFixed(1)}%</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Avg Duration</span>
                <span class="stat-value">${stats.avgDuration}min</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Most Common</span>
                <span class="stat-value">${stats.mostCommonIntent}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">File Types</span>
                <span class="stat-value">${stats.fileTypes}</span>
            </div>
        `;
    }

    calculateStatistics() {
        const successful = this.sessions.filter(s => s.outcome === 'success').length;
        const successRate = this.sessions.length > 0 ? successful / this.sessions.length : 0;
        
        const intentCounts = {};
        const fileTypes = new Set();
        
        this.sessions.forEach(session => {
            intentCounts[session.intent] = (intentCounts[session.intent] || 0) + 1;
            if (session.currentFile) {
                const ext = session.currentFile.split('.').pop();
                fileTypes.add('.' + ext);
            }
        });

        const mostCommonIntent = Object.entries(intentCounts)
            .sort(([,a], [,b]) => b - a)[0]?.[0] || 'none';

        return {
            successRate,
            avgDuration: 15, // Mock data
            mostCommonIntent,
            fileTypes: fileTypes.size
        };
    }

    updatePatternInsights(type) {
        const container = document.getElementById('pattern-insights');
        
        const insights = this.generateInsights(type);
        
        container.innerHTML = insights.map(insight => `
            <div class="insight-card">
                <div class="insight-title">${insight.title}</div>
                <div class="insight-description">${insight.description}</div>
                <div class="insight-metrics">
                    ${insight.metrics.map(metric => `
                        <div class="metric">
                            <div class="metric-value">${metric.value}</div>
                            <div class="metric-label">${metric.label}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');
    }

    generateInsights(type) {
        switch (type) {
            case 'success_patterns':
                return [
                    {
                        title: 'High Success Rate Pattern',
                        description: 'Data exploration sessions have the highest success rate',
                        metrics: [
                            { value: '92%', label: 'Success Rate' },
                            { value: '1', label: 'Sessions' },
                            { value: '12min', label: 'Avg Duration' }
                        ]
                    },
                    {
                        title: 'Implementation Efficiency',
                        description: 'Implementation tasks complete quickly when well-planned',
                        metrics: [
                            { value: '88%', label: 'Success Rate' },
                            { value: '1', label: 'Sessions' },
                            { value: '15min', label: 'Avg Duration' }
                        ]
                    }
                ];
            case 'failure_patterns':
                return [
                    {
                        title: 'Debugging Challenges',
                        description: 'Machine learning debugging sessions often get stuck',
                        metrics: [
                            { value: '35%', label: 'Stuck Rate' },
                            { value: '1', label: 'Sessions' },
                            { value: '25min', label: 'Avg Duration' }
                        ]
                    }
                ];
            case 'temporal_patterns':
                return [
                    {
                        title: 'Morning Productivity',
                        description: 'Sessions started in the morning have higher success rates',
                        metrics: [
                            { value: '90%', label: 'Morning Success' },
                            { value: '2', label: 'Sessions' },
                            { value: '14min', label: 'Avg Duration' }
                        ]
                    }
                ];
            case 'file_patterns':
                return [
                    {
                        title: 'Jupyter Notebook Success',
                        description: 'Jupyter notebooks are associated with successful exploration',
                        metrics: [
                            { value: '92%', label: 'Success Rate' },
                            { value: '1', label: 'Sessions' },
                            { value: '.ipynb', label: 'File Type' }
                        ]
                    }
                ];
            default:
                return [];
        }
    }

    // Event Handlers
    selectCluster(clusterId) {
        // Update UI selection
        document.querySelectorAll('.cluster-node').forEach(node => {
            node.classList.remove('selected');
        });
        
        const selectedNode = document.querySelector(`[data-cluster-id="${clusterId}"]`);
        if (selectedNode) {
            selectedNode.classList.add('selected');
        }

        // Update selected sessions
        const cluster = this.clusters.find(c => c.id === clusterId);
        if (cluster) {
            this.selectedSessions.clear();
            cluster.sessions.forEach(sessionId => {
                this.selectedSessions.add(sessionId);
            });
            this.updateStatistics();
            this.highlightSelectedSessions();
        }
    }

    toggleCluster(node) {
        const children = node.nextElementSibling;
        const toggle = node.querySelector('.cluster-toggle');
        
        if (children && children.classList.contains('cluster-children')) {
            const isExpanded = children.style.display !== 'none';
            children.style.display = isExpanded ? 'none' : 'block';
            toggle.textContent = isExpanded ? '▶' : '▼';
        }
    }

    handleUMAPSelection(eventData) {
        if (!eventData || !eventData.points) return;

        this.selectedSessions.clear();
        eventData.points.forEach(point => {
            this.selectedSessions.add(point.id);
        });

        this.updateStatistics();
        this.showSelectionInfo(eventData.points);
    }

    handleUMAPClick(eventData) {
        if (!eventData || !eventData.points || eventData.points.length === 0) return;

        const sessionId = eventData.points[0].id;
        this.showSessionDetails(sessionId);
    }

    showSessionDetails(sessionId) {
        const session = this.sessions.find(s => s.id === sessionId);
        if (!session) return;

        const container = document.getElementById('session-details');
        container.innerHTML = `
            <div class="session-card">
                <div class="session-header">
                    <span class="session-id">${session.id}</span>
                    <span class="session-timestamp">${new Date(session.timestamp).toLocaleString()}</span>
                </div>
                <div class="session-meta">
                    <div class="meta-item">
                        <div class="meta-label">Intent</div>
                        <div class="meta-value">${session.intent}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Outcome</div>
                        <div class="meta-value">${session.outcome}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">File</div>
                        <div class="meta-value">${session.currentFile}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Confidence</div>
                        <div class="meta-value">${(session.confidence * 100).toFixed(1)}%</div>
                    </div>
                </div>
                <div class="session-content">
                    ${session.summary || 'No summary available'}
                </div>
            </div>
        `;
    }

    showSelectionInfo(points) {
        const container = document.getElementById('selection-content');
        container.innerHTML = `
            <p><strong>${points.length} sessions selected</strong></p>
            <ul>
                ${points.slice(0, 5).map(point => `
                    <li>${point.id} (${point.data.name || 'Unknown'})</li>
                `).join('')}
                ${points.length > 5 ? `<li>... and ${points.length - 5} more</li>` : ''}
            </ul>
        `;
    }

    // Modal Functions
    showNotebookModal() {
        const modal = document.getElementById('notebook-modal');
        modal.classList.add('active');
    }

    showProcedureModal() {
        const modal = document.getElementById('procedure-modal');
        const sessionsList = document.getElementById('selected-sessions-list');
        
        // Populate selected sessions
        sessionsList.innerHTML = Array.from(this.selectedSessions).map(sessionId => {
            const session = this.sessions.find(s => s.id === sessionId);
            return `<div class="selected-session-item">${sessionId}: ${session?.summary || 'No summary'}</div>`;
        }).join('') || '<div class="selected-session-item">No sessions selected</div>';
        
        modal.classList.add('active');
    }

    // Utility Functions
    showLoading(show) {
        const overlay = document.getElementById('loading-overlay');
        overlay.style.display = show ? 'flex' : 'none';
    }

    showError(message) {
        // Simple error display - could be enhanced with a proper notification system
        alert('Error: ' + message);
    }

    performSearch() {
        const query = document.getElementById('global-search').value.toLowerCase();
        if (!query) return;

        // Filter sessions based on search query
        const filteredSessions = this.sessions.filter(session => 
            session.id.toLowerCase().includes(query) ||
            session.intent.toLowerCase().includes(query) ||
            session.currentFile.toLowerCase().includes(query) ||
            (session.summary && session.summary.toLowerCase().includes(query))
        );

        // Update UI to show filtered results
        this.selectedSessions.clear();
        filteredSessions.forEach(session => {
            this.selectedSessions.add(session.id);
        });

        this.updateStatistics();
        this.highlightSelectedSessions();
    }

    highlightSelectedSessions() {
        // Update UMAP plot to highlight selected sessions
        if (this.umapPlot) {
            const update = {
                'marker.size': this.umapData.map(point => 
                    this.selectedSessions.has(point.id) ? 15 : 10
                ),
                'marker.line.width': this.umapData.map(point =>
                    this.selectedSessions.has(point.id) ? 3 : 1
                )
            };

            Plotly.restyle(this.umapPlot, update);
        }
    }

    handleKeyboardShortcuts(e) {
        if (e.ctrlKey || e.metaKey) {
            switch (e.key) {
                case 'k':
                    e.preventDefault();
                    document.getElementById('global-search').focus();
                    break;
                case 'r':
                    e.preventDefault();
                    this.loadData();
                    break;
                case 'e':
                    e.preventDefault();
                    this.exportData();
                    break;
            }
        }
    }

    // Additional methods would go here...
    updateUMAPColors() {
        this.renderUMAPPlot();
    }

    resetUMAPZoom() {
        if (this.umapPlot) {
            Plotly.relayout(this.umapPlot, {
                'xaxis.autorange': true,
                'yaxis.autorange': true
            });
        }
    }

    toggleFullscreen() {
        const container = document.getElementById('umap-plot');
        if (document.fullscreenElement) {
            document.exitFullscreen();
        } else {
            container.requestFullscreen();
        }
    }

    expandAllClusters() {
        document.querySelectorAll('.cluster-children').forEach(children => {
            children.style.display = 'block';
        });
        document.querySelectorAll('.cluster-toggle').forEach(toggle => {
            toggle.textContent = '▼';
        });
    }

    collapseAllClusters() {
        document.querySelectorAll('.cluster-children').forEach(children => {
            children.style.display = 'none';
        });
        document.querySelectorAll('.cluster-toggle').forEach(toggle => {
            toggle.textContent = '▶';
        });
    }

    closeSessionDetails() {
        document.getElementById('session-details').innerHTML = '<p>Select a session to view details</p>';
    }

    exportData() {
        // Export current analysis data
        const data = {
            sessions: this.sessions,
            clusters: this.clusters,
            umapData: this.umapData,
            selectedSessions: Array.from(this.selectedSessions),
            exportTime: new Date().toISOString()
        };

        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `kura-analysis-${new Date().toISOString().slice(0, 10)}.json`;
        a.click();
        URL.revokeObjectURL(url);
    }

    exportSelectedCluster() {
        // Export selected cluster data
        console.log('Exporting selected cluster...');
    }

    shareInsights() {
        // Generate shareable URL with current state
        const state = {
            colorBy: this.currentColorBy,
            selectedSessions: Array.from(this.selectedSessions)
        };
        
        const url = new URL(window.location);
        url.hash = btoa(JSON.stringify(state));
        
        navigator.clipboard.writeText(url.toString()).then(() => {
            alert('Shareable URL copied to clipboard!');
        });
    }
}

// Global functions for modal handling
function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function generateNotebook() {
    console.log('Generating notebook...');
    closeModal('notebook-modal');
}

function createProcedure() {
    console.log('Creating procedure...');
    closeModal('procedure-modal');
}

// Initialize dashboard
function initializeKuraDashboard() {
    window.kuraDashboard = new KuraDashboard();
}
