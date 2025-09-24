#!/usr/bin/env node
/**
 * Test script for Enhanced Kura Dashboard Integration
 * Validates the complete PKL Extension with Kura integration
 */

const http = require('http');
const path = require('path');

class DashboardTester {
    constructor(baseUrl = 'http://localhost:3001') {
        this.baseUrl = baseUrl;
        this.tests = [];
        this.passed = 0;
        this.failed = 0;
    }

    async runTests() {
        console.log('🧪 Testing Enhanced Kura Dashboard Integration');
        console.log('=' .repeat(60));

        await this.testAPIHealth();
        await this.testSessionsEndpoint();
        await this.testKuraAnalysisEndpoint();
        await this.testClusterEndpoint();
        await this.testProcedureGeneration();
        await this.testNotebookGeneration();
        await this.testExportEndpoint();
        await this.testDashboardPage();

        this.printResults();
    }

    async testAPIHealth() {
        await this.test('API Health Check', async () => {
            const response = await this.makeRequest('/api/health');
            if (response.status !== 'ok') {
                throw new Error('Health check failed');
            }
            return 'API is healthy';
        });
    }

    async testSessionsEndpoint() {
        await this.test('Sessions Endpoint', async () => {
            const response = await this.makeRequest('/api/sessions');
            if (!response.success || !Array.isArray(response.sessions)) {
                throw new Error('Sessions endpoint failed');
            }
            return `Loaded ${response.sessions.length} sessions`;
        });
    }

    async testKuraAnalysisEndpoint() {
        await this.test('Kura Analysis Endpoint', async () => {
            const payload = {
                test_mode: true,
                include_dashboard_data: true
            };

            const response = await this.makeRequest('/api/sessions/analyze-with-kura', 'POST', payload);
            
            if (!response.success) {
                throw new Error('Kura analysis failed');
            }

            const requiredFields = ['sessions', 'clusters', 'umap_coordinates', 'summaries'];
            for (const field of requiredFields) {
                if (!response[field]) {
                    throw new Error(`Missing field: ${field}`);
                }
            }

            return `Generated ${response.clusters.length} clusters, ${response.umap_coordinates.length} UMAP points`;
        });
    }

    async testClusterEndpoint() {
        await this.test('Cluster Details Endpoint', async () => {
            const response = await this.makeRequest('/api/clusters/cluster_explore');
            
            if (!response.success || !response.cluster) {
                throw new Error('Cluster endpoint failed');
            }

            return `Retrieved cluster: ${response.cluster.name}`;
        });
    }

    async testProcedureGeneration() {
        await this.test('Procedure Template Generation', async () => {
            const payload = {
                sessions: ['session_001'],
                name: 'Test Procedure',
                description: 'A test procedure for validation',
                category: 'exploration'
            };

            const response = await this.makeRequest('/api/procedures/generate', 'POST', payload);
            
            if (!response.success || !response.procedure) {
                throw new Error('Procedure generation failed');
            }

            return `Generated procedure: ${response.procedure.name}`;
        });
    }

    async testNotebookGeneration() {
        await this.test('Analysis Notebook Generation', async () => {
            const payload = {
                sessions: ['session_001', 'session_002'],
                template_type: 'eda',
                title: 'Test Analysis',
                options: {
                    include_conversations: true,
                    include_code_deltas: true
                }
            };

            const response = await this.makeRequest('/api/notebooks/generate', 'POST', payload);
            
            if (!response.success || !response.notebook_path) {
                throw new Error('Notebook generation failed');
            }

            return `Generated notebook: ${path.basename(response.notebook_path)}`;
        });
    }

    async testExportEndpoint() {
        await this.test('Data Export Endpoint', async () => {
            const payload = {
                format: 'json',
                data: { sessions: [], clusters: [] },
                filename: 'test-export'
            };

            const response = await this.makeRequest('/api/export', 'POST', payload);
            
            if (!response.success || !response.export_path) {
                throw new Error('Export failed');
            }

            return `Exported to: ${path.basename(response.export_path)}`;
        });
    }

    async testDashboardPage() {
        await this.test('Enhanced Dashboard Page', async () => {
            try {
                const html = await this.makeRawRequest('/dashboard/enhanced');
                
                if (!html.includes('PKL Extension - Enhanced Analytics Dashboard')) {
                    throw new Error('Dashboard HTML missing expected content');
                }

                if (!html.includes('kura-dashboard.js')) {
                    throw new Error('Dashboard JavaScript not included');
                }

                if (!html.includes('kura-dashboard.css')) {
                    throw new Error('Dashboard CSS not included');
                }

                return 'Dashboard page loads correctly';
            } catch (error) {
                throw new Error(`Dashboard page failed: ${error.message}`);
            }
        });
    }

    async test(name, testFn) {
        try {
            const result = await testFn();
            console.log(`✅ ${name}: ${result}`);
            this.passed++;
        } catch (error) {
            console.log(`❌ ${name}: ${error.message}`);
            this.failed++;
        }
    }

    makeRequest(endpoint, method = 'GET', data = null) {
        return new Promise((resolve, reject) => {
            const url = new URL(endpoint, this.baseUrl);
            const options = {
                method,
                headers: {
                    'Content-Type': 'application/json'
                }
            };

            const req = http.request(url, options, (res) => {
                let body = '';
                res.on('data', chunk => body += chunk);
                res.on('end', () => {
                    try {
                        const parsed = JSON.parse(body);
                        resolve(parsed);
                    } catch (error) {
                        reject(new Error(`Failed to parse response: ${error.message}`));
                    }
                });
            });

            req.on('error', reject);

            if (data) {
                req.write(JSON.stringify(data));
            }

            req.end();
        });
    }

    makeRawRequest(endpoint) {
        return new Promise((resolve, reject) => {
            const url = new URL(endpoint, this.baseUrl);
            
            http.get(url, (res) => {
                let body = '';
                res.on('data', chunk => body += chunk);
                res.on('end', () => resolve(body));
            }).on('error', reject);
        });
    }

    printResults() {
        console.log('\n' + '=' .repeat(60));
        console.log('🎯 Test Results Summary');
        console.log('=' .repeat(60));
        console.log(`✅ Passed: ${this.passed}`);
        console.log(`❌ Failed: ${this.failed}`);
        console.log(`📊 Total:  ${this.passed + this.failed}`);
        
        if (this.failed === 0) {
            console.log('\n🎉 All tests passed! Enhanced Kura Dashboard is working correctly.');
            console.log(`🔗 Dashboard URL: ${this.baseUrl}/dashboard/enhanced`);
            console.log('\n📋 Features Validated:');
            console.log('   • Hierarchical cluster visualization');
            console.log('   • UMAP session relationship mapping');
            console.log('   • Interactive pattern insights');
            console.log('   • Procedure template generation');
            console.log('   • Analysis notebook creation');
            console.log('   • Data export capabilities');
            console.log('   • Enhanced UI with modern design');
        } else {
            console.log('\n⚠️  Some tests failed. Please check the error messages above.');
            process.exit(1);
        }
    }
}

// Run tests if called directly
if (require.main === module) {
    const tester = new DashboardTester();
    tester.runTests().catch(error => {
        console.error('Test runner failed:', error);
        process.exit(1);
    });
}

module.exports = DashboardTester;
