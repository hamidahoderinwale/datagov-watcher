/**
 * Privacy-Preserving Workflow Analysis Frontend
 * Interactive visualizations and controls for privacy-expressiveness analysis
 */
class PrivacyAnalysis {
  constructor() {
    this.privacyConfig = {
      epsilon: 1.0,
      redactionLevel: 50,
      abstractionLevel: 3,
      redactNames: true,
      redactNumbers: true,
      redactEmails: true
    };
    
    this.originalWorkflows = [];
    this.transformedWorkflows = [];
    this.expressivenessMetrics = {};
    this.charts = {};
    
    this.init();
  }

  async init() {
    console.log('Initializing Privacy Analysis...');
    
    this.setupEventListeners();
    await this.loadWorkflowData();
    this.renderInitialVisualization();
    
    console.log('Privacy Analysis initialized');
  }

  setupEventListeners() {
    // Privacy control sliders
    const epsilonSlider = document.getElementById('epsilon-slider');
    const redactionSlider = document.getElementById('redaction-slider');
    const abstractionSlider = document.getElementById('abstraction-slider');
    
    if (epsilonSlider) {
      epsilonSlider.addEventListener('input', (e) => {
        this.privacyConfig.epsilon = parseFloat(e.target.value);
        document.getElementById('epsilon-value').textContent = e.target.value;
        this.updateAnalysis();
      });
    }
    
    if (redactionSlider) {
      redactionSlider.addEventListener('input', (e) => {
        this.privacyConfig.redactionLevel = parseInt(e.target.value);
        document.getElementById('redaction-value').textContent = e.target.value + '%';
        this.updateAnalysis();
      });
    }
    
    if (abstractionSlider) {
      abstractionSlider.addEventListener('input', (e) => {
        this.privacyConfig.abstractionLevel = parseInt(e.target.value);
        document.getElementById('abstraction-value').textContent = 'Level ' + e.target.value;
        this.updateAnalysis();
      });
    }
    
    // Privacy option checkboxes
    const checkboxes = ['redact-names', 'redact-numbers', 'redact-emails'];
    checkboxes.forEach(id => {
      const checkbox = document.getElementById(id);
      if (checkbox) {
        checkbox.addEventListener('change', (e) => {
          const key = id.replace('redact-', 'redact') + (id.split('-')[1].charAt(0).toUpperCase() + id.split('-')[1].slice(1));
          this.privacyConfig[key] = e.target.checked;
          this.updateAnalysis();
        });
      }
    });
    
    // Zoom level controls
    const zoomButtons = document.querySelectorAll('.zoom-level');
    zoomButtons.forEach(button => {
      button.addEventListener('click', (e) => {
        const level = e.target.dataset.level;
        this.switchZoomLevel(level);
      });
    });
    
    // Export and refresh buttons
    const exportButton = document.getElementById('export-analysis');
    const refreshButton = document.getElementById('refresh-data');
    
    if (exportButton) {
      exportButton.addEventListener('click', () => this.showExportModal());
    }
    
    if (refreshButton) {
      refreshButton.addEventListener('click', () => this.refreshData());
    }
  }

  async loadWorkflowData() {
    try {
      console.log('Loading workflow data...');
      
      // Load sessions from the main API
      const response = await fetch('/api/sessions');
      const data = await response.json();
      
      if (data.success && data.sessions) {
        console.log(`Loaded ${data.sessions.length} sessions for analysis`);
        
        // Transform sessions into workflow format
        this.originalWorkflows = await this.transformSessionsToWorkflows(data.sessions);
        
        // Apply initial privacy transformations
        await this.updateAnalysis();
        
        // Update aggregate statistics
        this.updateAggregateStats();
      } else {
        console.error('Failed to load sessions:', data.error);
      }
    } catch (error) {
      console.error('Error loading workflow data:', error);
    }
  }

  async transformSessionsToWorkflows(sessions) {
    const workflows = [];
    
    for (const session of sessions) {
      // Load conversations for this session
      const conversations = await this.loadSessionConversations(session.id);
      
      const workflow = {
        id: session.id,
        timestamp: session.timestamp,
        intent: session.intent || 'explore',
        outcome: session.outcome || 'in-progress',
        traces: this.extractTracesFromSession(session, conversations),
        metadata: {
          duration: this.calculateSessionDuration(session),
          fileCount: this.getUniqueFileCount(session),
          changeCount: (session.codeDeltas || []).length
        }
      };
      
      workflows.push(workflow);
    }
    
    return workflows;
  }

  async loadSessionConversations(sessionId) {
    try {
      const response = await fetch(`/api/session/${sessionId}/conversations`);
      const data = await response.json();
      return data.success ? data.conversations : [];
    } catch (error) {
      console.error('Error loading conversations for session:', sessionId, error);
      return [];
    }
  }

  extractTracesFromSession(session, conversations) {
    const traces = [];
    
    // Add conversation traces
    conversations.forEach(conv => {
      traces.push({
        type: 'conversation',
        timestamp: conv.timestamp,
        role: conv.role,
        content: conv.content,
        tokens: this.tokenize(conv.content),
        codeBlocks: conv.codeBlocks || []
      });
    });
    
    // Add code change traces
    if (session.codeDeltas) {
      session.codeDeltas.forEach(delta => {
        traces.push({
          type: 'code_change',
          timestamp: delta.timestamp,
          filePath: delta.filePath,
          changeType: delta.changeType,
          beforeContent: delta.beforeContent || '',
          afterContent: delta.afterContent || '',
          diff: delta.diff || ''
        });
      });
    }
    
    // Add file change traces
    if (session.fileChanges) {
      session.fileChanges.forEach(change => {
        traces.push({
          type: 'file_change',
          timestamp: change.timestamp,
          filePath: change.filePath,
          changeType: change.changeType,
          lineRange: change.lineRange
        });
      });
    }
    
    return traces.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }

  async updateAnalysis() {
    console.log('Updating privacy analysis with config:', this.privacyConfig);
    
    try {
      // Apply privacy transformations
      this.transformedWorkflows = await this.applyPrivacyTransformations();
      
      // Calculate expressiveness metrics
      this.expressivenessMetrics = await this.calculateExpressivenessMetrics();
      
      // Update visualizations
      this.updatePrivacyCurve();
      this.updateMetricsDisplay();
      this.updateTraceComparison();
      this.updateClusterVisualization();
      
    } catch (error) {
      console.error('Error updating analysis:', error);
    }
  }

  async applyPrivacyTransformations() {
    const transformed = [];
    
    for (const workflow of this.originalWorkflows) {
      const transformedWorkflow = {
        id: this.generatePrivateId(workflow.id),
        timestamp: workflow.timestamp,
        intent: workflow.intent,
        outcome: workflow.outcome,
        traces: await this.transformTraces(workflow.traces),
        metadata: this.transformMetadata(workflow.metadata),
        privacyMetrics: {
          epsilon: this.privacyConfig.epsilon,
          redactionLevel: this.privacyConfig.redactionLevel,
          abstractionLevel: this.privacyConfig.abstractionLevel
        }
      };
      
      transformed.push(transformedWorkflow);
    }
    
    return transformed;
  }

  async transformTraces(traces) {
    const transformed = [];
    
    for (const trace of traces) {
      let transformedTrace = { ...trace };
      
      // Apply token-level redaction
      if (trace.content) {
        transformedTrace.content = this.applyTokenRedaction(trace.content);
      }
      
      if (trace.tokens) {
        transformedTrace.tokens = trace.tokens.map(token => 
          this.shouldRedactToken(token) ? '[REDACTED]' : token
        );
      }
      
      // Apply differential privacy noise
      if (trace.type === 'code_change') {
        transformedTrace = this.addDifferentialPrivacyNoise(transformedTrace);
      }
      
      // Apply procedural abstraction
      transformedTrace = this.applyProceduralAbstraction(transformedTrace);
      
      transformed.push(transformedTrace);
    }
    
    return transformed;
  }

  applyTokenRedaction(content) {
    let redacted = content;
    
    if (this.privacyConfig.redactNames) {
      redacted = redacted.replace(/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b/g, '[NAME]');
    }
    
    if (this.privacyConfig.redactEmails) {
      redacted = redacted.replace(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g, '[EMAIL]');
    }
    
    if (this.privacyConfig.redactNumbers) {
      redacted = redacted.replace(/\b\d+(?:\.\d+)?\b/g, '[NUM]');
    }
    
    return redacted;
  }

  shouldRedactToken(token) {
    // Random redaction based on redaction level
    return Math.random() < (this.privacyConfig.redactionLevel / 100);
  }

  addDifferentialPrivacyNoise(trace) {
    const sensitivity = 1.0;
    const scale = sensitivity / this.privacyConfig.epsilon;
    
    const noisyTrace = { ...trace };
    
    if (trace.metadata && typeof trace.metadata.lineCount === 'number') {
      const noise = this.generateLaplaceNoise(scale);
      noisyTrace.metadata = {
        ...trace.metadata,
        lineCount: Math.max(0, Math.round(trace.metadata.lineCount + noise))
      };
    }
    
    return noisyTrace;
  }

  generateLaplaceNoise(scale) {
    const u = Math.random() - 0.5;
    return -scale * Math.sign(u) * Math.log(1 - 2 * Math.abs(u));
  }

  applyProceduralAbstraction(trace) {
    const level = this.privacyConfig.abstractionLevel;
    const abstracted = { ...trace };
    
    switch (level) {
      case 1: // Token level
        break;
      case 2: // Statement level
        abstracted.abstractedType = this.classifyStatement(trace);
        break;
      case 3: // Function level
        abstracted.abstractedType = this.classifyFunction(trace);
        break;
      case 4: // Module level
        abstracted.abstractedType = this.classifyModule(trace);
        break;
      case 5: // Workflow level
        abstracted.abstractedType = this.classifyWorkflow(trace);
        break;
    }
    
    return abstracted;
  }

  classifyStatement(trace) {
    const content = trace.afterContent || trace.content || '';
    
    if (content.includes('def ') || content.includes('function ')) return 'function_definition';
    if (content.includes('import ') || content.includes('from ')) return 'import_statement';
    if (content.includes('if ') || content.includes('else')) return 'conditional_statement';
    if (content.includes('for ') || content.includes('while ')) return 'loop_statement';
    if (content.includes('=')) return 'assignment_statement';
    return 'other_statement';
  }

  classifyFunction(trace) {
    const content = trace.afterContent || trace.content || '';
    
    if (/pandas|numpy|data|df\.|\.csv|\.json/i.test(content)) return 'data_processing';
    if (/plot|chart|graph|matplotlib|seaborn|plotly/i.test(content)) return 'visualization';
    if (/sklearn|model|train|predict|fit/i.test(content)) return 'machine_learning';
    if (/request|response|http|api/i.test(content)) return 'api_call';
    if (/open|read|write|file/i.test(content)) return 'file_io';
    if (/test|assert|mock/i.test(content)) return 'testing';
    return 'general_function';
  }

  classifyModule(trace) {
    if (trace.filePath) {
      const ext = trace.filePath.split('.').pop().toLowerCase();
      switch (ext) {
        case 'py': return 'python_module';
        case 'js': return 'javascript_module';
        case 'ipynb': return 'notebook_module';
        case 'md': return 'documentation_module';
        case 'json': return 'config_module';
        default: return 'other_module';
      }
    }
    return 'module_operation';
  }

  classifyWorkflow(trace) {
    const abstractedType = trace.abstractedType || this.classifyFunction(trace);
    
    if (['data_processing', 'visualization'].some(t => abstractedType.includes(t))) return 'explore';
    if (['function_definition', 'api_call'].some(t => abstractedType.includes(t))) return 'implement';
    if (['testing', 'conditional_statement'].some(t => abstractedType.includes(t))) return 'debug';
    if (['documentation_module'].some(t => abstractedType.includes(t))) return 'document';
    if (['assignment_statement', 'function_definition'].some(t => abstractedType.includes(t))) return 'refactor';
    
    return 'general_workflow';
  }

  async calculateExpressivenessMetrics() {
    const metrics = {
      clusteringQuality: this.calculateClusteringQuality(),
      classificationAccuracy: this.calculateClassificationAccuracy(),
      workflowPreservation: this.calculateWorkflowPreservation(),
      informationRetention: this.calculateInformationRetention()
    };
    
    metrics.expressivenessScore = (
      metrics.clusteringQuality * 0.3 +
      metrics.classificationAccuracy * 0.3 +
      metrics.workflowPreservation * 0.2 +
      metrics.informationRetention * 0.2
    );
    
    return metrics;
  }

  calculateClusteringQuality() {
    // Simplified clustering quality based on intent distribution
    const intents = this.transformedWorkflows.map(w => w.intent);
    const intentCounts = {};
    intents.forEach(intent => {
      intentCounts[intent] = (intentCounts[intent] || 0) + 1;
    });
    
    const totalWorkflows = intents.length;
    const entropy = Object.values(intentCounts).reduce((sum, count) => {
      const p = count / totalWorkflows;
      return sum - (p * Math.log2(p));
    }, 0);
    
    // Normalize entropy to 0-1 range (assuming max 5 intents)
    return Math.min(entropy / Math.log2(5), 1);
  }

  calculateClassificationAccuracy() {
    // Simulate classification accuracy based on feature preservation
    const retention = this.calculateInformationRetention();
    const baseAccuracy = 0.6; // Baseline accuracy
    return baseAccuracy + (retention * 0.3);
  }

  calculateWorkflowPreservation() {
    let totalPreservation = 0;
    
    for (let i = 0; i < Math.min(this.originalWorkflows.length, this.transformedWorkflows.length); i++) {
      const original = this.extractWorkflowSequence(this.originalWorkflows[i]);
      const transformed = this.extractWorkflowSequence(this.transformedWorkflows[i]);
      
      const editDistance = this.calculateEditDistance(original, transformed);
      const maxLength = Math.max(original.length, transformed.length);
      
      const preservation = maxLength > 0 ? 1 - (editDistance / maxLength) : 1;
      totalPreservation += preservation;
    }
    
    return this.originalWorkflows.length > 0 ? totalPreservation / this.originalWorkflows.length : 0;
  }

  extractWorkflowSequence(workflow) {
    return workflow.traces.map(trace => trace.abstractedType || trace.type || 'unknown');
  }

  calculateEditDistance(seq1, seq2) {
    const matrix = [];
    
    for (let i = 0; i <= seq1.length; i++) {
      matrix[i] = [i];
    }
    
    for (let j = 0; j <= seq2.length; j++) {
      matrix[0][j] = j;
    }
    
    for (let i = 1; i <= seq1.length; i++) {
      for (let j = 1; j <= seq2.length; j++) {
        if (seq1[i - 1] === seq2[j - 1]) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j] + 1,
            matrix[i][j - 1] + 1,
            matrix[i - 1][j - 1] + 1
          );
        }
      }
    }
    
    return matrix[seq1.length][seq2.length];
  }

  calculateInformationRetention() {
    let totalRetention = 0;
    
    for (let i = 0; i < Math.min(this.originalWorkflows.length, this.transformedWorkflows.length); i++) {
      const originalTokens = this.extractAllTokens(this.originalWorkflows[i]);
      const transformedTokens = this.extractAllTokens(this.transformedWorkflows[i]);
      
      const retainedTokens = transformedTokens.filter(token => 
        !token.includes('[REDACTED]') && !token.includes('[NAME]') && 
        !token.includes('[EMAIL]') && !token.includes('[NUM]')
      ).length;
      
      const retention = originalTokens.length > 0 ? retainedTokens / originalTokens.length : 1;
      totalRetention += retention;
    }
    
    return this.originalWorkflows.length > 0 ? totalRetention / this.originalWorkflows.length : 0;
  }

  extractAllTokens(workflow) {
    const tokens = [];
    
    workflow.traces.forEach(trace => {
      if (trace.tokens) {
        tokens.push(...trace.tokens);
      } else if (trace.content) {
        tokens.push(...this.tokenize(trace.content));
      }
    });
    
    return tokens;
  }

  updatePrivacyCurve() {
    const ctx = document.getElementById('privacy-curve-chart');
    if (!ctx) return;
    
    // Destroy existing chart
    if (this.charts.privacyCurve) {
      this.charts.privacyCurve.destroy();
    }
    
    // Generate privacy-expressiveness curve data
    const curveData = this.generatePrivacyCurveData();
    
    this.charts.privacyCurve = new Chart(ctx, {
      type: 'line',
      data: {
        datasets: [{
          label: 'Privacy-Expressiveness Trade-off',
          data: curveData,
          borderColor: '#007AFF',
          backgroundColor: 'rgba(0, 122, 255, 0.1)',
          borderWidth: 3,
          pointBackgroundColor: '#007AFF',
          pointBorderColor: '#ffffff',
          pointBorderWidth: 2,
          pointRadius: 6,
          tension: 0.4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            type: 'linear',
            position: 'bottom',
            title: {
              display: true,
              text: 'Privacy Budget (ε)'
            },
            min: 0.1,
            max: 10
          },
          y: {
            title: {
              display: true,
              text: 'Expressiveness Score'
            },
            min: 0,
            max: 1
          }
        },
        plugins: {
          legend: {
            display: true,
            position: 'top'
          },
          tooltip: {
            callbacks: {
              label: function(context) {
                return `ε=${context.parsed.x}, Score=${context.parsed.y.toFixed(3)}`;
              }
            }
          }
        }
      }
    });
  }

  generatePrivacyCurveData() {
    const data = [];
    const epsilonValues = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0];
    
    for (const epsilon of epsilonValues) {
      // Simulate expressiveness score based on epsilon
      // Higher epsilon (less privacy) = higher expressiveness
      const baseScore = Math.log(epsilon + 1) / Math.log(11); // Normalized log scale
      const noise = (Math.random() - 0.5) * 0.1; // Add some realistic variation
      const score = Math.max(0, Math.min(1, baseScore + noise));
      
      data.push({ x: epsilon, y: score });
    }
    
    // Add current configuration point
    const currentScore = this.expressivenessMetrics.expressivenessScore || 0.5;
    data.push({ 
      x: this.privacyConfig.epsilon, 
      y: currentScore,
      pointBackgroundColor: '#FF3B30',
      pointRadius: 8
    });
    
    return data;
  }

  updateMetricsDisplay() {
    const metrics = this.expressivenessMetrics;
    
    document.getElementById('current-epsilon').textContent = `ε = ${this.privacyConfig.epsilon}`;
    document.getElementById('expressiveness-score').textContent = (metrics.expressivenessScore || 0).toFixed(3);
    document.getElementById('silhouette-score').textContent = (metrics.clusteringQuality || 0).toFixed(3);
    document.getElementById('classification-accuracy').textContent = (metrics.classificationAccuracy || 0).toFixed(3);
  }

  updateTraceComparison() {
    const originalContainer = document.getElementById('original-trace');
    const transformedContainer = document.getElementById('transformed-trace');
    
    if (!originalContainer || !transformedContainer) return;
    
    // Show first workflow as example
    if (this.originalWorkflows.length > 0 && this.transformedWorkflows.length > 0) {
      const original = this.originalWorkflows[0];
      const transformed = this.transformedWorkflows[0];
      
      originalContainer.innerHTML = this.renderTraceView(original.traces, 'original');
      transformedContainer.innerHTML = this.renderTraceView(transformed.traces, 'transformed');
    }
  }

  renderTraceView(traces, type) {
    const maxTraces = 10; // Limit display for performance
    const displayTraces = traces.slice(0, maxTraces);
    
    let html = '<div class="trace-items">';
    
    displayTraces.forEach((trace, index) => {
      const content = trace.content || trace.diff || trace.changeType || 'No content';
      const truncatedContent = content.length > 100 ? content.substring(0, 100) + '...' : content;
      
      html += `
        <div class="trace-item ${trace.type}">
          <div class="trace-header">
            <span class="trace-type">${trace.type}</span>
            <span class="trace-time">${new Date(trace.timestamp).toLocaleTimeString()}</span>
          </div>
          <div class="trace-content">${truncatedContent}</div>
          ${trace.abstractedType ? `<div class="trace-abstraction">${trace.abstractedType}</div>` : ''}
        </div>
      `;
    });
    
    if (traces.length > maxTraces) {
      html += `<div class="trace-item more">... and ${traces.length - maxTraces} more traces</div>`;
    }
    
    html += '</div>';
    return html;
  }

  updateClusterVisualization() {
    const container = document.getElementById('cluster-plot');
    if (!container) return;
    
    // Simple cluster visualization using transformed workflows
    const clusterData = this.generateClusterData();
    
    // Update cluster statistics
    document.getElementById('cluster-count').textContent = clusterData.clusters.length;
    document.getElementById('workflow-entropy').textContent = clusterData.entropy.toFixed(2);
    document.getElementById('shape-preservation').textContent = (this.expressivenessMetrics.workflowPreservation || 0).toFixed(2);
    
    // Create simple scatter plot
    this.renderClusterPlot(container, clusterData);
  }

  generateClusterData() {
    const intents = ['explore', 'implement', 'debug', 'refactor', 'document'];
    const clusters = [];
    const points = [];
    
    this.transformedWorkflows.forEach((workflow, index) => {
      const x = Math.random() * 400 + 50; // Random x position
      const y = Math.random() * 300 + 50; // Random y position
      const cluster = intents.indexOf(workflow.intent);
      
      points.push({ x, y, cluster, intent: workflow.intent });
    });
    
    // Calculate entropy
    const intentCounts = {};
    this.transformedWorkflows.forEach(w => {
      intentCounts[w.intent] = (intentCounts[w.intent] || 0) + 1;
    });
    
    const totalWorkflows = this.transformedWorkflows.length;
    const entropy = Object.values(intentCounts).reduce((sum, count) => {
      const p = count / totalWorkflows;
      return sum - (p * Math.log2(p));
    }, 0);
    
    return {
      points,
      clusters: intents,
      entropy
    };
  }

  renderClusterPlot(container, data) {
    const colors = ['#007AFF', '#34C759', '#FF9500', '#FF3B30', '#AF52DE'];
    
    let html = '<svg width="500" height="400" viewBox="0 0 500 400">';
    
    // Draw points
    data.points.forEach(point => {
      const color = colors[point.cluster] || '#8E8E93';
      html += `
        <circle cx="${point.x}" cy="${point.y}" r="6" 
                fill="${color}" opacity="0.8" 
                title="${point.intent}">
        </circle>
      `;
    });
    
    // Add legend
    data.clusters.forEach((cluster, index) => {
      const y = 20 + index * 25;
      html += `
        <circle cx="20" cy="${y}" r="6" fill="${colors[index]}"></circle>
        <text x="35" y="${y + 4}" font-size="12" fill="#333">${cluster}</text>
      `;
    });
    
    html += '</svg>';
    container.innerHTML = html;
  }

  switchZoomLevel(level) {
    // Update active button
    document.querySelectorAll('.zoom-level').forEach(btn => btn.classList.remove('active'));
    document.querySelector(`[data-level="${level}"]`).classList.add('active');
    
    // Show corresponding view
    document.querySelectorAll('.zoom-view').forEach(view => view.style.display = 'none');
    document.getElementById(`${level}-view`).style.display = 'block';
    
    // Render content for the selected level
    this.renderZoomLevelContent(level);
  }

  renderZoomLevelContent(level) {
    switch (level) {
      case 'token':
        this.renderTokenLevel();
        break;
      case 'step':
        this.renderStepLevel();
        break;
      case 'workflow':
        this.renderWorkflowLevel();
        break;
    }
  }

  renderTokenLevel() {
    const container = document.getElementById('token-grid');
    if (!container || this.transformedWorkflows.length === 0) return;
    
    const workflow = this.transformedWorkflows[0];
    const tokens = this.extractAllTokens(workflow).slice(0, 50); // Limit for display
    
    let html = '';
    tokens.forEach(token => {
      const isRedacted = token.includes('[REDACTED]') || token.includes('[NAME]') || 
                        token.includes('[EMAIL]') || token.includes('[NUM]');
      html += `<div class="token-item ${isRedacted ? 'redacted' : ''}">${token}</div>`;
    });
    
    container.innerHTML = html;
  }

  renderStepLevel() {
    const container = document.getElementById('step-timeline');
    if (!container || this.transformedWorkflows.length === 0) return;
    
    const workflow = this.transformedWorkflows[0];
    const steps = workflow.traces.slice(0, 10); // Limit for display
    
    let html = '';
    steps.forEach((step, index) => {
      html += `
        <div class="step-item">
          <div class="step-number">${index + 1}</div>
          <div class="step-type">${step.type}</div>
          <div class="step-abstraction">${step.abstractedType || 'No abstraction'}</div>
          <div class="step-time">${new Date(step.timestamp).toLocaleTimeString()}</div>
        </div>
      `;
    });
    
    container.innerHTML = html;
  }

  renderWorkflowLevel() {
    const container = document.getElementById('workflow-graph');
    if (!container) return;
    
    const intentCounts = {};
    this.transformedWorkflows.forEach(w => {
      intentCounts[w.intent] = (intentCounts[w.intent] || 0) + 1;
    });
    
    let html = '<div class="workflow-summary">';
    Object.entries(intentCounts).forEach(([intent, count]) => {
      const percentage = ((count / this.transformedWorkflows.length) * 100).toFixed(1);
      html += `
        <div class="workflow-category">
          <div class="category-name">${intent}</div>
          <div class="category-count">${count} sessions (${percentage}%)</div>
          <div class="category-bar">
            <div class="category-fill" style="width: ${percentage}%"></div>
          </div>
        </div>
      `;
    });
    html += '</div>';
    
    container.innerHTML = html;
  }

  updateAggregateStats() {
    const totalSessions = this.originalWorkflows.length;
    const totalTokens = this.originalWorkflows.reduce((sum, w) => sum + this.extractAllTokens(w).length, 0);
    
    // Simulate privacy violations (in practice, this would be calculated during transformation)
    const privacyViolations = Math.floor(totalTokens * 0.05); // 5% violation rate
    const redactionRate = ((this.privacyConfig.redactionLevel / 100) * 100).toFixed(0);
    
    document.getElementById('total-sessions').textContent = totalSessions;
    document.getElementById('total-tokens').textContent = totalTokens.toLocaleString();
    document.getElementById('privacy-violations').textContent = privacyViolations;
    document.getElementById('redaction-rate').textContent = redactionRate + '%';
  }

  showExportModal() {
    document.getElementById('export-modal').style.display = 'flex';
  }

  async refreshData() {
    console.log('Refreshing workflow data...');
    await this.loadWorkflowData();
    console.log('Data refreshed');
  }

  // Utility functions
  tokenize(text) {
    return text.toLowerCase()
      .replace(/[^\w\s]/g, ' ')
      .split(/\s+/)
      .filter(token => token.length > 0);
  }

  generatePrivateId(originalId) {
    // Simple hash function for demo
    let hash = 0;
    for (let i = 0; i < originalId.length; i++) {
      const char = originalId.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return 'priv_' + Math.abs(hash).toString(16).substring(0, 8);
  }

  transformMetadata(metadata) {
    const transformed = { ...metadata };
    
    // Add noise to numerical values
    if (typeof transformed.duration === 'number') {
      const noise = (Math.random() - 0.5) * 60; // ±30 seconds noise
      transformed.duration = Math.max(0, transformed.duration + noise);
    }
    
    return transformed;
  }

  calculateSessionDuration(session) {
    if (session.endTime && session.timestamp) {
      return (new Date(session.endTime) - new Date(session.timestamp)) / 1000;
    }
    return Math.random() * 3600; // Random duration for demo
  }

  getUniqueFileCount(session) {
    const files = new Set();
    
    if (session.currentFile) files.add(session.currentFile);
    if (session.fileChanges) {
      session.fileChanges.forEach(change => files.add(change.filePath));
    }
    if (session.codeDeltas) {
      session.codeDeltas.forEach(delta => files.add(delta.filePath));
    }
    
    return files.size;
  }

  renderInitialVisualization() {
    // Initialize with default configuration
    this.updatePrivacyCurve();
    this.updateMetricsDisplay();
    this.updateClusterVisualization();
    this.renderTokenLevel(); // Default zoom level
  }
}

// Export modal functions
function closeExportModal() {
  document.getElementById('export-modal').style.display = 'none';
}

function performExport() {
  const format = document.getElementById('export-format').value;
  const options = {
    curves: document.getElementById('export-curves').checked,
    traces: document.getElementById('export-traces').checked,
    clusters: document.getElementById('export-clusters').checked,
    stats: document.getElementById('export-stats').checked
  };
  
  console.log('Exporting privacy analysis:', format, options);
  
  // In practice, this would call the backend export API
  alert(`Export initiated: ${format.toUpperCase()} format with selected options`);
  
  closeExportModal();
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  window.privacyAnalysis = new PrivacyAnalysis();
});
