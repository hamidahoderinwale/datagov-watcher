const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

/**
 * Privacy-Preserving Workflow Analysis Service
 * Implements differential privacy, token redaction, and procedural abstraction
 * for analyzing Cursor coding workflows while protecting sensitive data
 */
class PrivacyService {
  constructor() {
    this.privacyConfig = {
      epsilon: 1.0,                    // Differential privacy budget
      redactionLevel: 0.5,             // Token redaction percentage (0-1)
      abstractionLevel: 3,             // Procedural abstraction level (1-5)
      redactNames: true,
      redactNumbers: true,
      redactEmails: true
    };
    
    this.sensitivePatterns = {
      names: /\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b/g,
      emails: /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g,
      numbers: /\b\d+(?:\.\d+)?\b/g,
      ipAddresses: /\b(?:\d{1,3}\.){3}\d{1,3}\b/g,
      urls: /https?:\/\/[^\s]+/g,
      filePaths: /(?:[a-zA-Z]:)?[\\\/](?:[^\\\/\n]+[\\\/])*[^\\\/\n]*/g
    };
    
    this.abstractionLevels = {
      1: 'token',      // Individual tokens/words
      2: 'statement',  // Code statements
      3: 'function',   // Function-level operations
      4: 'module',     // Module-level changes
      5: 'workflow'    // High-level workflow patterns
    };
  }

  /**
   * Update privacy configuration
   */
  updateConfig(config) {
    this.privacyConfig = { ...this.privacyConfig, ...config };
  }

  /**
   * Collect and export Cursor workflow data
   */
  async collectWorkflowData(sessions) {
    const workflows = [];
    
    for (const session of sessions) {
      const workflow = {
        id: session.id,
        timestamp: session.timestamp,
        intent: session.intent,
        outcome: session.outcome,
        traces: await this.extractTraces(session),
        metadata: {
          duration: this.calculateDuration(session),
          fileCount: this.getUniqueFiles(session).length,
          changeCount: session.codeDeltas?.length || 0
        }
      };
      
      workflows.push(workflow);
    }
    
    return workflows;
  }

  /**
   * Extract detailed traces from session data
   */
  async extractTraces(session) {
    const traces = [];
    
    // Extract conversation traces
    if (session.conversations) {
      for (const conv of session.conversations) {
        traces.push({
          type: 'conversation',
          timestamp: conv.timestamp,
          role: conv.role,
          content: conv.content,
          tokens: this.tokenize(conv.content),
          codeBlocks: conv.codeBlocks || []
        });
      }
    }
    
    // Extract code change traces
    if (session.codeDeltas) {
      for (const delta of session.codeDeltas) {
        traces.push({
          type: 'code_change',
          timestamp: delta.timestamp,
          filePath: delta.filePath,
          changeType: delta.changeType,
          beforeContent: delta.beforeContent,
          afterContent: delta.afterContent,
          diff: delta.diff
        });
      }
    }
    
    // Extract file change traces
    if (session.fileChanges) {
      for (const change of session.fileChanges) {
        traces.push({
          type: 'file_change',
          timestamp: change.timestamp,
          filePath: change.filePath,
          changeType: change.changeType,
          lineRange: change.lineRange
        });
      }
    }
    
    return traces.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }

  /**
   * Apply privacy transformations to workflow data
   */
  async applyPrivacyTransformations(workflows) {
    const transformedWorkflows = [];
    
    for (const workflow of workflows) {
      const transformed = {
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
      
      transformedWorkflows.push(transformed);
    }
    
    return transformedWorkflows;
  }

  /**
   * Transform individual traces with privacy protection
   */
  async transformTraces(traces) {
    const transformedTraces = [];
    
    for (const trace of traces) {
      let transformed = { ...trace };
      
      // Apply token-level redaction
      if (trace.content) {
        transformed.content = this.applyTokenRedaction(trace.content);
      }
      
      if (trace.tokens) {
        transformed.tokens = trace.tokens.map(token => 
          this.shouldRedactToken(token) ? '[REDACTED]' : token
        );
      }
      
      // Apply differential privacy noise
      if (trace.type === 'code_change') {
        transformed = this.addDifferentialPrivacyNoise(transformed);
      }
      
      // Apply procedural abstraction
      transformed = this.applyProceduralAbstraction(transformed);
      
      transformedTraces.push(transformed);
    }
    
    return transformedTraces;
  }

  /**
   * Apply token-level redaction based on sensitivity patterns
   */
  applyTokenRedaction(content) {
    let redactedContent = content;
    
    if (this.privacyConfig.redactNames) {
      redactedContent = redactedContent.replace(this.sensitivePatterns.names, '[NAME]');
    }
    
    if (this.privacyConfig.redactEmails) {
      redactedContent = redactedContent.replace(this.sensitivePatterns.emails, '[EMAIL]');
    }
    
    if (this.privacyConfig.redactNumbers) {
      redactedContent = redactedContent.replace(this.sensitivePatterns.numbers, '[NUM]');
    }
    
    // Redact IP addresses and URLs
    redactedContent = redactedContent.replace(this.sensitivePatterns.ipAddresses, '[IP]');
    redactedContent = redactedContent.replace(this.sensitivePatterns.urls, '[URL]');
    redactedContent = redactedContent.replace(this.sensitivePatterns.filePaths, '[PATH]');
    
    return redactedContent;
  }

  /**
   * Determine if a token should be redacted based on redaction level
   */
  shouldRedactToken(token) {
    // Always redact if it matches sensitive patterns
    for (const pattern of Object.values(this.sensitivePatterns)) {
      if (pattern.test && pattern.test(token)) {
        return true;
      }
    }
    
    // Random redaction based on redaction level
    return Math.random() < this.privacyConfig.redactionLevel;
  }

  /**
   * Add calibrated noise for differential privacy
   */
  addDifferentialPrivacyNoise(trace) {
    const sensitivity = 1.0; // Sensitivity of the query
    const scale = sensitivity / this.privacyConfig.epsilon;
    
    // Add Laplace noise to numerical values
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

  /**
   * Generate Laplace noise for differential privacy
   */
  generateLaplaceNoise(scale) {
    const u = Math.random() - 0.5;
    return -scale * Math.sign(u) * Math.log(1 - 2 * Math.abs(u));
  }

  /**
   * Apply procedural abstraction to collapse into higher-level categories
   */
  applyProceduralAbstraction(trace) {
    const level = this.privacyConfig.abstractionLevel;
    const abstractedTrace = { ...trace };
    
    switch (level) {
      case 1: // Token level - minimal abstraction
        break;
        
      case 2: // Statement level
        if (trace.type === 'code_change') {
          abstractedTrace.abstractedType = this.classifyCodeStatement(trace);
        }
        break;
        
      case 3: // Function level
        if (trace.type === 'code_change') {
          abstractedTrace.abstractedType = this.classifyFunctionOperation(trace);
        }
        break;
        
      case 4: // Module level
        abstractedTrace.abstractedType = this.classifyModuleChange(trace);
        break;
        
      case 5: // Workflow level
        abstractedTrace.abstractedType = this.classifyWorkflowPattern(trace);
        break;
    }
    
    return abstractedTrace;
  }

  /**
   * Classify code statements for abstraction
   */
  classifyCodeStatement(trace) {
    const content = trace.afterContent || trace.content || '';
    
    if (content.includes('def ') || content.includes('function ')) {
      return 'function_definition';
    } else if (content.includes('import ') || content.includes('from ')) {
      return 'import_statement';
    } else if (content.includes('if ') || content.includes('else')) {
      return 'conditional_statement';
    } else if (content.includes('for ') || content.includes('while ')) {
      return 'loop_statement';
    } else if (content.includes('=')) {
      return 'assignment_statement';
    } else {
      return 'other_statement';
    }
  }

  /**
   * Classify function-level operations
   */
  classifyFunctionOperation(trace) {
    const patterns = {
      'data_processing': /pandas|numpy|data|df\.|\.csv|\.json/i,
      'visualization': /plot|chart|graph|matplotlib|seaborn|plotly/i,
      'machine_learning': /sklearn|model|train|predict|fit/i,
      'api_call': /request|response|http|api/i,
      'file_io': /open|read|write|file/i,
      'testing': /test|assert|mock/i
    };
    
    const content = trace.afterContent || trace.content || '';
    
    for (const [category, pattern] of Object.entries(patterns)) {
      if (pattern.test(content)) {
        return category;
      }
    }
    
    return 'general_function';
  }

  /**
   * Classify module-level changes
   */
  classifyModuleChange(trace) {
    if (trace.type === 'file_change') {
      const ext = path.extname(trace.filePath || '').toLowerCase();
      
      switch (ext) {
        case '.py': return 'python_module';
        case '.js': return 'javascript_module';
        case '.ipynb': return 'notebook_module';
        case '.md': return 'documentation_module';
        case '.json': return 'config_module';
        default: return 'other_module';
      }
    }
    
    return 'module_operation';
  }

  /**
   * Classify high-level workflow patterns
   */
  classifyWorkflowPattern(trace) {
    // Map to high-level workflow categories
    const workflowPatterns = {
      'explore': ['data_processing', 'visualization'],
      'implement': ['function_definition', 'api_call'],
      'debug': ['testing', 'conditional_statement'],
      'document': ['documentation_module', 'comment'],
      'refactor': ['assignment_statement', 'function_definition']
    };
    
    const abstractedType = trace.abstractedType || this.classifyFunctionOperation(trace);
    
    for (const [pattern, types] of Object.entries(workflowPatterns)) {
      if (types.some(type => abstractedType.includes(type))) {
        return pattern;
      }
    }
    
    return 'general_workflow';
  }

  /**
   * Measure expressiveness of transformed data
   */
  async measureExpressiveness(originalWorkflows, transformedWorkflows) {
    const metrics = {
      clusteringQuality: await this.calculateClusteringQuality(transformedWorkflows),
      classificationAccuracy: await this.calculateClassificationAccuracy(originalWorkflows, transformedWorkflows),
      workflowPreservation: await this.calculateWorkflowPreservation(originalWorkflows, transformedWorkflows),
      informationRetention: this.calculateInformationRetention(originalWorkflows, transformedWorkflows)
    };
    
    // Overall expressiveness score (weighted average)
    metrics.expressivenessScore = (
      metrics.clusteringQuality * 0.3 +
      metrics.classificationAccuracy * 0.3 +
      metrics.workflowPreservation * 0.2 +
      metrics.informationRetention * 0.2
    );
    
    return metrics;
  }

  /**
   * Calculate clustering quality using Silhouette coefficient
   */
  async calculateClusteringQuality(workflows) {
    // Extract features for clustering
    const features = workflows.map(w => this.extractFeatureVector(w));
    
    // Perform k-means clustering
    const clusters = this.performClustering(features, 5);
    
    // Calculate Silhouette coefficient
    return this.calculateSilhouetteCoefficient(features, clusters);
  }

  /**
   * Extract feature vector from workflow for clustering
   */
  extractFeatureVector(workflow) {
    const features = [];
    
    // Intent features
    const intents = ['explore', 'implement', 'debug', 'refactor', 'document'];
    intents.forEach(intent => {
      features.push(workflow.intent === intent ? 1 : 0);
    });
    
    // Outcome features
    const outcomes = ['success', 'stuck', 'in-progress'];
    outcomes.forEach(outcome => {
      features.push(workflow.outcome === outcome ? 1 : 0);
    });
    
    // Metadata features (normalized)
    features.push(Math.min(workflow.metadata.duration / 3600, 1)); // Duration in hours, capped at 1
    features.push(Math.min(workflow.metadata.fileCount / 10, 1));   // File count, capped at 10
    features.push(Math.min(workflow.metadata.changeCount / 100, 1)); // Change count, capped at 100
    
    // Trace type distribution
    const traceTypes = ['conversation', 'code_change', 'file_change'];
    traceTypes.forEach(type => {
      const count = workflow.traces.filter(t => t.type === type).length;
      features.push(Math.min(count / workflow.traces.length, 1));
    });
    
    return features;
  }

  /**
   * Perform simple k-means clustering
   */
  performClustering(features, k) {
    // Initialize centroids randomly
    const centroids = [];
    for (let i = 0; i < k; i++) {
      centroids.push(features[Math.floor(Math.random() * features.length)]);
    }
    
    let clusters = new Array(features.length);
    let converged = false;
    let iterations = 0;
    
    while (!converged && iterations < 100) {
      const newClusters = new Array(features.length);
      
      // Assign points to closest centroid
      for (let i = 0; i < features.length; i++) {
        let minDistance = Infinity;
        let closestCentroid = 0;
        
        for (let j = 0; j < k; j++) {
          const distance = this.euclideanDistance(features[i], centroids[j]);
          if (distance < minDistance) {
            minDistance = distance;
            closestCentroid = j;
          }
        }
        
        newClusters[i] = closestCentroid;
      }
      
      // Check for convergence
      converged = clusters.every((cluster, i) => cluster === newClusters[i]);
      clusters = newClusters;
      
      // Update centroids
      for (let j = 0; j < k; j++) {
        const clusterPoints = features.filter((_, i) => clusters[i] === j);
        if (clusterPoints.length > 0) {
          centroids[j] = this.calculateCentroid(clusterPoints);
        }
      }
      
      iterations++;
    }
    
    return clusters;
  }

  /**
   * Calculate Euclidean distance between two feature vectors
   */
  euclideanDistance(a, b) {
    return Math.sqrt(a.reduce((sum, val, i) => sum + Math.pow(val - b[i], 2), 0));
  }

  /**
   * Calculate centroid of a set of points
   */
  calculateCentroid(points) {
    const dimensions = points[0].length;
    const centroid = new Array(dimensions).fill(0);
    
    for (const point of points) {
      for (let i = 0; i < dimensions; i++) {
        centroid[i] += point[i];
      }
    }
    
    return centroid.map(val => val / points.length);
  }

  /**
   * Calculate Silhouette coefficient for clustering quality
   */
  calculateSilhouetteCoefficient(features, clusters) {
    const silhouetteScores = [];
    
    for (let i = 0; i < features.length; i++) {
      const ownCluster = clusters[i];
      const ownClusterPoints = features.filter((_, j) => clusters[j] === ownCluster && j !== i);
      
      if (ownClusterPoints.length === 0) {
        silhouetteScores.push(0);
        continue;
      }
      
      // Average distance to points in same cluster
      const a = ownClusterPoints.reduce((sum, point) => 
        sum + this.euclideanDistance(features[i], point), 0) / ownClusterPoints.length;
      
      // Average distance to points in nearest cluster
      let minB = Infinity;
      const uniqueClusters = [...new Set(clusters)].filter(c => c !== ownCluster);
      
      for (const otherCluster of uniqueClusters) {
        const otherClusterPoints = features.filter((_, j) => clusters[j] === otherCluster);
        if (otherClusterPoints.length > 0) {
          const b = otherClusterPoints.reduce((sum, point) => 
            sum + this.euclideanDistance(features[i], point), 0) / otherClusterPoints.length;
          minB = Math.min(minB, b);
        }
      }
      
      const silhouette = (minB - a) / Math.max(a, minB);
      silhouetteScores.push(silhouette);
    }
    
    return silhouetteScores.reduce((sum, score) => sum + score, 0) / silhouetteScores.length;
  }

  /**
   * Calculate classification accuracy for task prediction
   */
  async calculateClassificationAccuracy(originalWorkflows, transformedWorkflows) {
    // Use intent classification as ground truth
    const originalIntents = originalWorkflows.map(w => w.intent);
    const transformedFeatures = transformedWorkflows.map(w => this.extractFeatureVector(w));
    
    // Simple majority class classifier for baseline
    const intentCounts = {};
    originalIntents.forEach(intent => {
      intentCounts[intent] = (intentCounts[intent] || 0) + 1;
    });
    
    const majorityClass = Object.keys(intentCounts).reduce((a, b) => 
      intentCounts[a] > intentCounts[b] ? a : b);
    
    const majorityAccuracy = intentCounts[majorityClass] / originalIntents.length;
    
    // For simplicity, return a score based on feature preservation
    // In practice, you'd train a classifier on transformed features
    const featurePreservation = this.calculateInformationRetention(originalWorkflows, transformedWorkflows);
    
    return Math.max(majorityAccuracy * featurePreservation, majorityAccuracy * 0.5);
  }

  /**
   * Calculate workflow shape preservation
   */
  async calculateWorkflowPreservation(originalWorkflows, transformedWorkflows) {
    let totalPreservation = 0;
    
    for (let i = 0; i < originalWorkflows.length; i++) {
      const original = originalWorkflows[i];
      const transformed = transformedWorkflows[i];
      
      // Calculate edit distance between workflow sequences
      const originalSequence = this.extractWorkflowSequence(original);
      const transformedSequence = this.extractWorkflowSequence(transformed);
      
      const editDistance = this.calculateEditDistance(originalSequence, transformedSequence);
      const maxLength = Math.max(originalSequence.length, transformedSequence.length);
      
      const preservation = maxLength > 0 ? 1 - (editDistance / maxLength) : 1;
      totalPreservation += preservation;
    }
    
    return totalPreservation / originalWorkflows.length;
  }

  /**
   * Extract workflow sequence for comparison
   */
  extractWorkflowSequence(workflow) {
    return workflow.traces.map(trace => {
      if (trace.abstractedType) {
        return trace.abstractedType;
      } else if (trace.type) {
        return trace.type;
      } else {
        return 'unknown';
      }
    });
  }

  /**
   * Calculate edit distance between two sequences
   */
  calculateEditDistance(seq1, seq2) {
    const matrix = [];
    
    // Initialize matrix
    for (let i = 0; i <= seq1.length; i++) {
      matrix[i] = [i];
    }
    
    for (let j = 0; j <= seq2.length; j++) {
      matrix[0][j] = j;
    }
    
    // Fill matrix
    for (let i = 1; i <= seq1.length; i++) {
      for (let j = 1; j <= seq2.length; j++) {
        if (seq1[i - 1] === seq2[j - 1]) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j] + 1,     // deletion
            matrix[i][j - 1] + 1,     // insertion
            matrix[i - 1][j - 1] + 1  // substitution
          );
        }
      }
    }
    
    return matrix[seq1.length][seq2.length];
  }

  /**
   * Calculate information retention ratio
   */
  calculateInformationRetention(originalWorkflows, transformedWorkflows) {
    let totalRetention = 0;
    
    for (let i = 0; i < originalWorkflows.length; i++) {
      const original = originalWorkflows[i];
      const transformed = transformedWorkflows[i];
      
      // Calculate token retention
      const originalTokens = this.extractAllTokens(original);
      const transformedTokens = this.extractAllTokens(transformed);
      
      const retainedTokens = transformedTokens.filter(token => 
        !token.includes('[REDACTED]') && !token.includes('[NAME]') && 
        !token.includes('[EMAIL]') && !token.includes('[NUM]')
      ).length;
      
      const retention = originalTokens.length > 0 ? retainedTokens / originalTokens.length : 1;
      totalRetention += retention;
    }
    
    return totalRetention / originalWorkflows.length;
  }

  /**
   * Extract all tokens from a workflow
   */
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

  /**
   * Simple tokenization
   */
  tokenize(text) {
    return text.toLowerCase()
      .replace(/[^\w\s]/g, ' ')
      .split(/\s+/)
      .filter(token => token.length > 0);
  }

  /**
   * Generate privacy-preserving ID
   */
  generatePrivateId(originalId) {
    const hash = crypto.createHash('sha256');
    hash.update(originalId + this.privacyConfig.epsilon);
    return 'priv_' + hash.digest('hex').substring(0, 8);
  }

  /**
   * Transform metadata with privacy protection
   */
  transformMetadata(metadata) {
    const transformed = { ...metadata };
    
    // Add noise to numerical values
    if (typeof transformed.duration === 'number') {
      const noise = this.generateLaplaceNoise(1.0 / this.privacyConfig.epsilon);
      transformed.duration = Math.max(0, transformed.duration + noise);
    }
    
    return transformed;
  }

  /**
   * Calculate session duration
   */
  calculateDuration(session) {
    if (session.endTime && session.timestamp) {
      return (new Date(session.endTime) - new Date(session.timestamp)) / 1000;
    }
    return 0;
  }

  /**
   * Get unique files from session
   */
  getUniqueFiles(session) {
    const files = new Set();
    
    if (session.currentFile) {
      files.add(session.currentFile);
    }
    
    if (session.fileChanges) {
      session.fileChanges.forEach(change => files.add(change.filePath));
    }
    
    if (session.codeDeltas) {
      session.codeDeltas.forEach(delta => files.add(delta.filePath));
    }
    
    return Array.from(files);
  }
}

module.exports = PrivacyService;
