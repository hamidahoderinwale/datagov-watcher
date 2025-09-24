/**
 * Kura API Endpoint for PKL Extension
 * Provides REST API for Kura analysis integration
 */

const express = require('express');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs').promises;
const cors = require('cors');

class KuraAPIServer {
    constructor(port = 3001) {
        this.app = express();
        this.port = port;
        this.setupMiddleware();
        this.setupRoutes();
    }

    setupMiddleware() {
        this.app.use(cors());
        this.app.use(express.json());
        this.app.use(express.static('.')); // Serve static files
    }

    setupRoutes() {
        // Health check
        this.app.get('/api/health', (req, res) => {
            res.json({ status: 'ok', timestamp: new Date().toISOString() });
        });

        // Get all sessions
        this.app.get('/api/sessions', async (req, res) => {
            try {
                const sessions = await this.loadSessions();
                res.json({ success: true, sessions });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // Analyze sessions with Kura
        this.app.post('/api/sessions/analyze-with-kura', async (req, res) => {
            try {
                const { test_mode = true, include_dashboard_data = false } = req.body;
                
                console.log('🔬 Starting Kura analysis...');
                const result = await this.runKuraAnalysis(test_mode, include_dashboard_data);
                
                res.json({
                    success: true,
                    ...result,
                    analysis_time: new Date().toISOString()
                });
            } catch (error) {
                console.error('Kura analysis failed:', error);
                res.status(500).json({ 
                    success: false, 
                    error: error.message,
                    fallback_data: this.getMockKuraData()
                });
            }
        });

        // Get specific cluster data
        this.app.get('/api/clusters/:clusterId', async (req, res) => {
            try {
                const { clusterId } = req.params;
                const clusterData = await this.getClusterData(clusterId);
                res.json({ success: true, cluster: clusterData });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // Generate procedure template
        this.app.post('/api/procedures/generate', async (req, res) => {
            try {
                const { sessions, name, description, category } = req.body;
                const procedure = await this.generateProcedureTemplate(sessions, name, description, category);
                res.json({ success: true, procedure });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // Generate analysis notebook
        this.app.post('/api/notebooks/generate', async (req, res) => {
            try {
                const { sessions, template_type, title, options } = req.body;
                const notebook = await this.generateAnalysisNotebook(sessions, template_type, title, options);
                res.json({ success: true, notebook_path: notebook });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // Export data
        this.app.post('/api/export', async (req, res) => {
            try {
                const { format, data, filename } = req.body;
                const exportPath = await this.exportData(format, data, filename);
                res.json({ success: true, export_path: exportPath });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // Serve the enhanced dashboard
        this.app.get('/dashboard/enhanced', (req, res) => {
            res.sendFile(path.join(__dirname, 'kura-enhanced-dashboard.html'));
        });
    }

    async loadSessions() {
        try {
            // Try to load from existing exports
            const exportsDir = path.join(__dirname, 'exports');
            const files = await fs.readdir(exportsDir);
            const latestExport = files
                .filter(f => f.startsWith('cursor-history-export-'))
                .sort()
                .pop();

            if (latestExport) {
                const data = await fs.readFile(path.join(exportsDir, latestExport), 'utf8');
                const exportData = JSON.parse(data);
                return exportData.sessions || [];
            }

            // Fallback to mock data
            return this.getMockSessions();
        } catch (error) {
            console.warn('Could not load sessions, using mock data:', error.message);
            return this.getMockSessions();
        }
    }

    async runKuraAnalysis(testMode = true, includeDashboardData = false) {
        if (testMode) {
            // Return mock Kura analysis data for testing
            console.log('🧪 Running in test mode - returning mock Kura data');
            return this.getMockKuraData();
        }

        try {
            // Load sessions
            const sessions = await this.loadSessions();
            
            // Create temporary sessions file
            const tempSessionsFile = path.join(__dirname, 'temp_sessions.json');
            await fs.writeFile(tempSessionsFile, JSON.stringify(sessions, null, 2));

            // Run Kura bridge
            const pythonScript = path.join(__dirname, 'kura_bridge.py');
            const outputDir = path.join(__dirname, 'kura_output');

            const result = await this.runPythonScript(pythonScript, [
                '--sessions-file', tempSessionsFile,
                '--output-dir', outputDir,
                '--cache-dir', path.join(__dirname, 'kura_cache')
            ]);

            // Load results
            const resultsFile = path.join(outputDir, 'kura_analysis_results.json');
            const dashboardFile = path.join(outputDir, 'dashboard_data.json');

            const analysisResults = JSON.parse(await fs.readFile(resultsFile, 'utf8'));
            let dashboardData = {};
            
            if (includeDashboardData) {
                try {
                    dashboardData = JSON.parse(await fs.readFile(dashboardFile, 'utf8'));
                } catch (e) {
                    console.warn('Could not load dashboard data:', e.message);
                }
            }

            // Cleanup
            await fs.unlink(tempSessionsFile).catch(() => {});

            return {
                sessions,
                clusters: analysisResults.clusters || [],
                hierarchical_clusters: analysisResults.reduced_clusters || [],
                umap_coordinates: analysisResults.projected_clusters || [],
                summaries: analysisResults.summaries || [],
                dashboard_data: dashboardData,
                total_conversations: analysisResults.total_conversations || 0,
                mock_mode: analysisResults.mock_mode || false
            };

        } catch (error) {
            console.error('Real Kura analysis failed:', error);
            throw new Error(`Kura analysis failed: ${error.message}`);
        }
    }

    runPythonScript(scriptPath, args) {
        return new Promise((resolve, reject) => {
            const venvPath = path.join(__dirname, 'kura_env', 'bin', 'python');
            const pythonPath = require('fs').existsSync(venvPath) ? venvPath : 'python3';
            
            const process = spawn(pythonPath, [scriptPath, ...args], {
                cwd: __dirname,
                env: { ...process.env, PYTHONPATH: __dirname }
            });

            let stdout = '';
            let stderr = '';

            process.stdout.on('data', (data) => {
                stdout += data.toString();
                console.log('Kura:', data.toString().trim());
            });

            process.stderr.on('data', (data) => {
                stderr += data.toString();
                console.error('Kura Error:', data.toString().trim());
            });

            process.on('close', (code) => {
                if (code === 0) {
                    resolve({ stdout, stderr });
                } else {
                    reject(new Error(`Python script failed with code ${code}: ${stderr}`));
                }
            });

            process.on('error', (error) => {
                reject(new Error(`Failed to start Python script: ${error.message}`));
            });
        });
    }

    getMockSessions() {
        return [
            {
                id: 'session_001',
                timestamp: '2024-01-15T10:30:00Z',
                intent: 'explore',
                outcome: 'success',
                confidence: 0.92,
                currentFile: 'data_analysis.ipynb',
                summary: 'Customer dataset analysis and pattern identification'
            },
            {
                id: 'session_002',
                timestamp: '2024-01-15T14:20:00Z',
                intent: 'debug',
                outcome: 'stuck',
                confidence: 0.65,
                currentFile: 'model_training.py',
                summary: 'Machine learning model debugging session'
            },
            {
                id: 'session_003',
                timestamp: '2024-01-16T09:15:00Z',
                intent: 'implement',
                outcome: 'success',
                confidence: 0.88,
                currentFile: 'visualization.py',
                summary: 'Interactive dashboard implementation'
            }
        ];
    }

    getMockKuraData() {
        return {
            sessions: this.getMockSessions(),
            clusters: [
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
            ],
            hierarchical_clusters: [
                {
                    id: 'meta_data_science',
                    name: 'Data Science Workflows',
                    children: ['cluster_explore', 'cluster_implement'],
                    total_size: 2
                },
                {
                    id: 'meta_debugging',
                    name: 'Problem Solving',
                    children: ['cluster_debug'],
                    total_size: 1
                }
            ],
            umap_coordinates: [
                { id: 'session_001', x: 0.2, y: 0.7, cluster: 'explore', intent: 'explore', outcome: 'success' },
                { id: 'session_002', x: 0.8, y: 0.3, cluster: 'debug', intent: 'debug', outcome: 'stuck' },
                { id: 'session_003', x: 0.5, y: 0.9, cluster: 'implement', intent: 'implement', outcome: 'success' }
            ],
            summaries: [
                {
                    id: 'session_001',
                    summary: 'Session about explore task in data_analysis.ipynb',
                    intent: 'explore',
                    outcome: 'success',
                    confidence: 0.92
                },
                {
                    id: 'session_002',
                    summary: 'Session about debug task in model_training.py',
                    intent: 'debug',
                    outcome: 'stuck',
                    confidence: 0.65
                },
                {
                    id: 'session_003',
                    summary: 'Session about implement task in visualization.py',
                    intent: 'implement',
                    outcome: 'success',
                    confidence: 0.88
                }
            ],
            total_conversations: 3,
            mock_mode: true
        };
    }

    async getClusterData(clusterId) {
        // Implementation for getting specific cluster data
        const mockData = this.getMockKuraData();
        return mockData.clusters.find(c => c.id === clusterId) || null;
    }

    async generateProcedureTemplate(sessions, name, description, category) {
        // Generate a procedure template based on successful sessions
        const template = {
            id: `proc_${Date.now()}`,
            name,
            description,
            category,
            sessions: sessions,
            steps: [
                {
                    sequence: 1,
                    action: 'code',
                    description: 'Initialize analysis environment',
                    codePattern: 'import pandas as pd\nimport numpy as np\nimport matplotlib.pyplot as plt'
                },
                {
                    sequence: 2,
                    action: 'analysis',
                    description: 'Load and inspect data',
                    codePattern: 'df = pd.read_csv("{{dataset_path}}")\nprint(df.head())\nprint(df.info())'
                },
                {
                    sequence: 3,
                    action: 'visualization',
                    description: 'Create initial visualizations',
                    codePattern: 'df.hist(figsize=(12, 8))\nplt.tight_layout()\nplt.show()'
                }
            ],
            success_rate: 0.85,
            avg_duration: 15,
            created_at: new Date().toISOString()
        };

        // Save template
        const templatesDir = path.join(__dirname, 'procedure_templates');
        await fs.mkdir(templatesDir, { recursive: true });
        await fs.writeFile(
            path.join(templatesDir, `${template.id}.json`),
            JSON.stringify(template, null, 2)
        );

        return template;
    }

    async generateAnalysisNotebook(sessions, templateType, title, options) {
        // Generate a Jupyter notebook for analysis
        const notebook = {
            cells: [
                {
                    cell_type: 'markdown',
                    metadata: {},
                    source: [
                        `# ${title}\n`,
                        `\n`,
                        `**Generated**: ${new Date().toISOString()}\n`,
                        `**Sessions analyzed**: ${sessions.length}\n`,
                        `**Template**: ${templateType}\n`,
                        `\n`,
                        `This notebook was automatically generated from PKL Extension session analysis.\n`
                    ]
                },
                {
                    cell_type: 'code',
                    execution_count: null,
                    metadata: {},
                    outputs: [],
                    source: [
                        '# Import required libraries\n',
                        'import pandas as pd\n',
                        'import numpy as np\n',
                        'import matplotlib.pyplot as plt\n',
                        'import seaborn as sns\n',
                        '\n',
                        '# Set up plotting\n',
                        'plt.style.use("default")\n',
                        'sns.set_palette("husl")\n'
                    ]
                },
                {
                    cell_type: 'markdown',
                    metadata: {},
                    source: [
                        `## Session Analysis\n`,
                        `\n`,
                        `Analysis of ${sessions.length} sessions from PKL Extension.\n`
                    ]
                }
            ],
            metadata: {
                kernelspec: {
                    display_name: 'Python 3',
                    language: 'python',
                    name: 'python3'
                },
                language_info: {
                    name: 'python',
                    version: '3.8.0'
                }
            },
            nbformat: 4,
            nbformat_minor: 4
        };

        // Save notebook
        const notebooksDir = path.join(__dirname, 'generated_notebooks');
        await fs.mkdir(notebooksDir, { recursive: true });
        
        const filename = `${title.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.ipynb`;
        const notebookPath = path.join(notebooksDir, filename);
        
        await fs.writeFile(notebookPath, JSON.stringify(notebook, null, 2));

        // Try to open in Cursor
        try {
            const { spawn } = require('child_process');
            spawn('open', ['-a', 'Cursor', notebookPath], { detached: true });
        } catch (error) {
            console.warn('Could not open notebook in Cursor:', error.message);
        }

        return notebookPath;
    }

    async exportData(format, data, filename) {
        const exportsDir = path.join(__dirname, 'exports');
        await fs.mkdir(exportsDir, { recursive: true });

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const exportFilename = filename || `kura-export-${timestamp}`;

        let exportPath;
        let content;

        switch (format) {
            case 'json':
                exportPath = path.join(exportsDir, `${exportFilename}.json`);
                content = JSON.stringify(data, null, 2);
                break;
            case 'csv':
                exportPath = path.join(exportsDir, `${exportFilename}.csv`);
                content = this.convertToCSV(data);
                break;
            case 'markdown':
                exportPath = path.join(exportsDir, `${exportFilename}.md`);
                content = this.convertToMarkdown(data);
                break;
            default:
                throw new Error(`Unsupported export format: ${format}`);
        }

        await fs.writeFile(exportPath, content);
        return exportPath;
    }

    convertToCSV(data) {
        // Simple CSV conversion for sessions data
        if (!data.sessions || !Array.isArray(data.sessions)) {
            return 'No session data available';
        }

        const headers = ['id', 'timestamp', 'intent', 'outcome', 'confidence', 'currentFile', 'summary'];
        const rows = data.sessions.map(session => 
            headers.map(header => `"${(session[header] || '').toString().replace(/"/g, '""')}"`).join(',')
        );

        return [headers.join(','), ...rows].join('\n');
    }

    convertToMarkdown(data) {
        // Convert analysis data to markdown report
        let markdown = `# Kura Analysis Report\n\n`;
        markdown += `**Generated**: ${new Date().toISOString()}\n\n`;

        if (data.sessions) {
            markdown += `## Sessions Summary\n\n`;
            markdown += `Total sessions: ${data.sessions.length}\n\n`;
            
            markdown += `| Session ID | Intent | Outcome | File | Confidence |\n`;
            markdown += `|------------|--------|---------|------|------------|\n`;
            
            data.sessions.forEach(session => {
                markdown += `| ${session.id} | ${session.intent} | ${session.outcome} | ${session.currentFile} | ${(session.confidence * 100).toFixed(1)}% |\n`;
            });
        }

        if (data.clusters) {
            markdown += `\n## Clusters\n\n`;
            data.clusters.forEach(cluster => {
                markdown += `### ${cluster.name}\n\n`;
                markdown += `- **Sessions**: ${cluster.size}\n`;
                markdown += `- **Success Rate**: ${(cluster.success_rate * 100).toFixed(1)}%\n\n`;
            });
        }

        return markdown;
    }

    start() {
        this.app.listen(this.port, () => {
            console.log(`🚀 Kura API Server running on http://localhost:${this.port}`);
            console.log(`📊 Enhanced Dashboard: http://localhost:${this.port}/dashboard/enhanced`);
        });
    }
}

// Start server if run directly
if (require.main === module) {
    const server = new KuraAPIServer();
    server.start();
}

module.exports = KuraAPIServer;
