const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const { exec } = require('child_process');
// const NotebookSemanticClassifier = require('./notebook-semantic-classifier'); // Removed during cleanup

/**
 * Real-time monitor for Cursor IDE activity
 * Monitors .ipynb files and logs actual conversations and code changes
 */
class RealMonitor {
  constructor() {
    this.activeSessions = new Map();
    this.watchers = new Map();
    this.cursorDbPath = this.findCursorDatabase();
    this.isMonitoring = false;
    this.lastConversationId = null;
    this.lastSessionId = null;
    // this.semanticClassifier = new NotebookSemanticClassifier(); // Removed during cleanup
  }

  findCursorDatabase() {
    const possiblePaths = [
      path.join(process.env.HOME || '', 'Library/Application Support/Cursor/User/globalStorage'),
      path.join(process.env.HOME || '', 'Library/Application Support/Cursor/User/workspaceStorage'),
      path.join(process.env.HOME || '', 'Library/Application Support/Cursor/logs')
    ];

    for (const basePath of possiblePaths) {
      if (fs.existsSync(basePath)) {
        const dbFiles = this.findSQLiteFiles(basePath);
        if (dbFiles.length > 0) {
          return dbFiles[0];
        }
      }
    }
    return null;
  }

  findSQLiteFiles(dir) {
    const files = [];
    const scanDir = (currentDir) => {
      try {
        const entries = fs.readdirSync(currentDir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = path.join(currentDir, entry.name);
          if (entry.isDirectory()) {
            scanDir(fullPath);
          } else if (entry.name.endsWith('.db') || entry.name.endsWith('.sqlite') || entry.name.endsWith('.sqlite3')) {
            files.push(fullPath);
          }
        }
      } catch (error) {
        // Ignore permission errors
      }
    };
    scanDir(dir);
    return files;
  }

  async startMonitoring() {
    if (this.isMonitoring) return;

    console.log('Starting real-time monitoring...');
    this.isMonitoring = true;

    // Load existing sessions from storage
    await this.loadExistingSessions();

    // Monitor Cursor database for conversation changes
    if (this.cursorDbPath) {
      this.monitorCursorDatabase();
    }

    // Monitor .ipynb files for changes
    this.monitorNotebookFiles();

    // Monitor common development directories
    this.monitorDevelopmentDirectories();

    console.log('Real-time monitoring started');
    console.log(`Loaded ${this.activeSessions.size} existing sessions`);
  }

  async loadExistingSessions() {
    try {
      if (this.dataStorage) {
        const storedSessions = await this.dataStorage.loadSessions();
        
        // Load recent sessions (last 24 hours) into active sessions
        const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        
        for (const session of storedSessions) {
          const sessionTime = new Date(session.timestamp);
          if (sessionTime > oneDayAgo) {
            this.activeSessions.set(session.id, session);
            console.log(`Restored session: ${session.id} (${session.currentFile})`);
          }
        }
      }
    } catch (error) {
      console.error('Error loading existing sessions:', error);
    }
  }

  stopMonitoring() {
    if (!this.isMonitoring) return;

    console.log('🛑 Stopping real-time monitoring...');
    this.isMonitoring = false;

    this.watchers.forEach(watcher => watcher.close());
    this.watchers.clear();

    console.log('Real-time monitoring stopped');
  }

  monitorCursorDatabase() {
    if (!this.cursorDbPath) return;

    const watcher = chokidar.watch(this.cursorDbPath, {
      persistent: true,
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: 1000,
        pollInterval: 100
      }
    });

    watcher.on('change', (filePath) => {
      console.log('Cursor database changed:', filePath);
      this.parseCursorDatabase();
    });

    this.watchers.set('cursor-db', watcher);
  }

  monitorNotebookFiles() {
    const commonDirs = [
      path.join(process.env.HOME || '', 'Desktop'),
      path.join(process.env.HOME || '', 'Documents'),
      path.join(process.env.HOME || '', 'Projects'),
      path.join(process.env.HOME || '', 'Code'),
      path.join(process.env.HOME || '', 'updated_notebooks_for_cursor')
    ];

    commonDirs.forEach(dir => {
      if (fs.existsSync(dir)) {
        this.monitorDirectory(dir);
      }
    });
  }

  monitorDirectory(dirPath) {
    const watcher = chokidar.watch(dirPath, {
      persistent: true,
      ignoreInitial: true,
      ignored: [
        '**/node_modules/**',
        '**/.git/**',
        '**/__pycache__/**',
        '**/.DS_Store',
        '**/Thumbs.db',
        '**/cursor-pkl-extension/**'
      ]
    });

    watcher.on('change', (filePath) => {
      if (filePath.endsWith('.ipynb')) {
        console.log('Notebook changed:', filePath);
        this.handleNotebookChange(filePath);
      }
    });

    watcher.on('add', (filePath) => {
      if (filePath.endsWith('.ipynb')) {
        console.log('New notebook detected:', filePath);
        this.handleNotebookChange(filePath);
      }
    });

    this.watchers.set(dirPath, watcher);
  }

  monitorDevelopmentDirectories() {
    // Monitor additional directories where users might work
    const additionalDirs = [
      path.join(process.env.HOME || '', 'Downloads'),
      path.join(process.env.HOME || '', 'Desktop/Projects'),
      path.join(process.env.HOME || '', 'Documents/Code')
    ];

    additionalDirs.forEach(dir => {
      if (fs.existsSync(dir)) {
        this.monitorDirectory(dir);
      }
    });
  }

  async handleNotebookChange(filePath) {
    try {
      // Check if this is a new session or continuation
      const sessionId = this.getOrCreateSession(filePath);
      
      // Parse the notebook to get changes
      const notebookData = await this.parseNotebook(filePath);
      
      // Extract conversation context
      const conversationContext = this.extractConversationContext(notebookData);
      
      // Log the changes
      await this.logNotebookChanges(sessionId, filePath, notebookData, conversationContext);
      
    } catch (error) {
      console.error('Error handling notebook change:', error);
    }
  }

  getOrCreateSession(filePath) {
    // Check if we already have an active session for this file
    for (const [sessionId, session] of this.activeSessions.entries()) {
      if (session.currentFile === filePath && session.phase === 'IN_PROGRESS') {
        return sessionId;
      }
    }

    // Create new session
    const sessionId = 'session-' + Date.now();
    const now = new Date().toISOString();
    const session = {
      id: sessionId,
      timestamp: now,
      startTime: now,
      endTime: null,
      intent: 'EXPLORE',
      phase: 'IN_PROGRESS',
      outcome: 'IN_PROGRESS',
      confidence: 0.8,
      currentFile: filePath,
      cursorPosition: { line: 1, character: 0 },
      selectedText: '',
      fileChanges: [],
      codeDeltas: [],
      linkedEvents: [],
      privacyMode: false,
      userConsent: true,
      dataRetention: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString(),
      annotations: [],
      lastActivity: now,
      duration: 0
    };

    this.activeSessions.set(sessionId, session);
    console.log('NEW: Session created:', sessionId, 'for file:', filePath);
    
    return sessionId;
  }

  async parseNotebook(filePath) {
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      
      // Check if file is empty or not valid JSON
      if (!content.trim()) {
        console.log('Empty notebook file:', filePath);
        return {
          metadata: {},
          cells: [],
          executionCount: 0,
          lastModified: fs.statSync(filePath).mtime
        };
      }
      
      const notebook = JSON.parse(content);
      
      return {
        metadata: notebook.metadata || {},
        cells: notebook.cells || [],
        executionCount: this.getMaxExecutionCount(notebook.cells || []),
        lastModified: fs.statSync(filePath).mtime
      };
    } catch (error) {
      console.error('Error parsing notebook:', error);
      return {
        metadata: {},
        cells: [],
        executionCount: 0,
        lastModified: fs.statSync(filePath).mtime
      };
    }
  }

  getMaxExecutionCount(cells) {
    let maxCount = 0;
    cells.forEach(cell => {
      if (cell.cell_type === 'code' && cell.execution_count) {
        maxCount = Math.max(maxCount, cell.execution_count);
      }
    });
    return maxCount;
  }

  extractConversationContext(notebookData) {
    if (!notebookData || !notebookData.cells) return null;

    const codeCells = notebookData.cells.filter(cell => cell.cell_type === 'code');
    const markdownCells = notebookData.cells.filter(cell => cell.cell_type === 'markdown');
    
    // Look for conversation patterns in markdown cells
    const conversationCells = markdownCells.filter(cell => {
      const content = Array.isArray(cell.source) ? cell.source.join('') : cell.source || '';
      return content.includes('User:') || content.includes('Assistant:') || 
             content.includes('Question:') || content.includes('Answer:');
    });

    return {
      totalCells: notebookData.cells.length,
      codeCells: codeCells.length,
      markdownCells: markdownCells.length,
      conversationCells: conversationCells.length,
      executionCount: notebookData.executionCount,
      hasConversation: conversationCells.length > 0
    };
  }

  async logNotebookChanges(sessionId, filePath, notebookData, conversationContext) {
    try {
      const session = this.activeSessions.get(sessionId);
      if (!session || !notebookData) return;

      // Create file change event
      const fileChange = {
        id: 'change-' + Date.now(),
        sessionId: sessionId,
        timestamp: new Date().toISOString(),
        filePath: filePath,
        changeType: 'modified',
        beforeSnippet: '',
        afterSnippet: `Notebook updated - ${notebookData.cells.length} cells, ${conversationContext ? conversationContext.codeCells : 0} code cells`,
        lineRange: { start: 1, end: notebookData.cells.length },
        gitHash: ''
      };

      session.fileChanges.push(fileChange);
      session.timestamp = new Date().toISOString();

      // Extract code deltas from code cells
      const codeDeltas = this.extractCodeDeltas(notebookData, sessionId);
      session.codeDeltas.push(...codeDeltas);

      // Extract visualizations from executed cells
      const visualizations = this.extractVisualizations(notebookData, sessionId);
      if (visualizations.length > 0) {
        if (!session.visualizations) session.visualizations = [];
        session.visualizations.push(...visualizations);
      }

      // Update execution states
      const executionStates = this.getExecutionStates(notebookData);
      session.executionStates = executionStates;

      // Perform simple intent classification (simplified version)
      const semanticAnalysis = this.simpleClassify(session);
      session.semanticAnalysis = semanticAnalysis;
      session.intent = semanticAnalysis.primary_intent;
      session.confidence = semanticAnalysis.confidence;

      // Update session activity and duration
      this.updateSessionActivity(sessionId);

      // Save session to persistent storage
      await this.saveSessionToStorage(session);

      console.log('Logged changes for session:', sessionId);
      console.log('   - File:', filePath);
      console.log('   - Cells:', notebookData.cells.length);
      console.log('   - Duration:', this.formatDuration(session.duration));
      console.log('   - Code cells:', conversationContext ? conversationContext.codeCells : 0);
      console.log('   - Visualizations:', visualizations.length);
      console.log('   - Execution states:', executionStates);
      console.log('   - Intent:', session.intent);
      console.log('   - Confidence:', (session.confidence * 100).toFixed(1) + '%');
      console.log('   - Evidence strength:', (semanticAnalysis.confidence_breakdown.evidence_strength * 100).toFixed(1) + '%');
      console.log('   - Signal agreement:', (semanticAnalysis.confidence_breakdown.signal_agreement * 100).toFixed(1) + '%');

    } catch (error) {
      console.error('Error logging notebook changes:', error);
    }
  }

  async saveSessionToStorage(session) {
    try {
      if (this.dataStorage) {
        await this.dataStorage.saveSession(session);
      }
    } catch (error) {
      console.error('Error saving session to storage:', error);
    }
  }

  extractCodeDeltas(notebookData, sessionId) {
    const deltas = [];
    
    if (!notebookData.cells) return deltas;

    notebookData.cells.forEach((cell, index) => {
      if (cell.cell_type === 'code' && cell.source) {
        const content = Array.isArray(cell.source) ? cell.source.join('') : cell.source;
        
        if (content.trim()) {
          // Detect if this looks like a suggestion (common patterns)
          const isSuggestion = this.detectSuggestionPattern(content);
          
          deltas.push({
            id: 'delta-' + Date.now() + '-' + index,
            sessionId: sessionId,
            timestamp: new Date().toISOString(),
            filePath: notebookData.filePath || 'unknown',
            beforeContent: '',
            afterContent: content,
            diff: '+ ' + content,
            changeType: isSuggestion ? 'suggestion' : 'added',
            lineCount: content.split('\n').length,
            cellIndex: index,
            executionCount: cell.execution_count || null,
            isSuggestion: isSuggestion,
            suggestionStatus: isSuggestion ? 'accepted' : 'unknown' // Could be enhanced with more detection
          });
        }
      }
    });

    return deltas;
  }

  detectSuggestionPattern(content) {
    // Common patterns that suggest this might be AI-generated or suggested code
    const suggestionPatterns = [
      /# Generated by|# Suggested by|# AI suggestion/i,
      /# TODO:|# FIXME:|# NOTE:/i,
      /import.*#.*suggestion/i,
      /# Alternative approach/i,
      /# Option \d+/i,
      /# Try this/i
    ];
    
    return suggestionPatterns.some(pattern => pattern.test(content));
  }

  extractVisualizations(notebookData, sessionId) {
    if (!notebookData.cells) return [];

    const visualizations = [];
    
    notebookData.cells.forEach((cell, cellIndex) => {
      if (cell.cell_type === 'code' && cell.outputs) {
        cell.outputs.forEach((output, outputIndex) => {
          if (output.output_type === 'display_data' || output.output_type === 'execute_result') {
            // Check for matplotlib figures
            if (output.data && output.data['image/png']) {
              visualizations.push({
                id: `viz-${sessionId}-${cellIndex}-${outputIndex}`,
                sessionId: sessionId,
                cellIndex: cellIndex,
                outputIndex: outputIndex,
                type: 'matplotlib',
                format: 'png',
                data: output.data['image/png'],
                timestamp: new Date().toISOString(),
                cellSource: cell.source ? (Array.isArray(cell.source) ? cell.source.join('') : cell.source) : ''
              });
            }
            
            // Check for plotly figures
            if (output.data && output.data['application/vnd.plotly.v1+json']) {
              try {
                const plotlyData = JSON.parse(output.data['application/vnd.plotly.v1+json']);
                visualizations.push({
                  id: `viz-${sessionId}-${cellIndex}-${outputIndex}`,
                  sessionId: sessionId,
                  cellIndex: cellIndex,
                  outputIndex: outputIndex,
                  type: 'plotly',
                  format: 'json',
                  data: plotlyData,
                  timestamp: new Date().toISOString(),
                  cellSource: cell.source ? (Array.isArray(cell.source) ? cell.source.join('') : cell.source) : ''
                });
              } catch (error) {
                console.error('Error parsing plotly data:', error);
              }
            }
            
            // Check for text-based plots
            if (output.data && output.data['text/plain']) {
              const text = output.data['text/plain'];
              if (this.isVisualizationText(text)) {
                visualizations.push({
                  id: `viz-${sessionId}-${cellIndex}-${outputIndex}`,
                  sessionId: sessionId,
                  cellIndex: cellIndex,
                  outputIndex: outputIndex,
                  type: 'text_plot',
                  format: 'text',
                  data: text,
                  timestamp: new Date().toISOString(),
                  cellSource: cell.source ? (Array.isArray(cell.source) ? cell.source.join('') : cell.source) : ''
                });
              }
            }
          }
        });
      }
    });

    return visualizations;
  }

  isVisualizationText(text) {
    const vizPatterns = [
      /plot|chart|graph|figure|visualization/i,
      /┌|┐|└|┘|─|│|╭|╮|╯|╰|├|┤|┬|┴|┼/,
      /^\s*[│┌┐└┘─├┤┬┴┼]/,
      /^\s*[█▄▀▐▌▀▄]/,
      /^\s*[\*\-\+]/,
      /^\s*[▲▼◄►]/
    ];
    
    return vizPatterns.some(pattern => pattern.test(text));
  }

  getExecutionStates(notebookData) {
    if (!notebookData.cells) return { not_executed: 0, executed: 0, error: 0 };

    const states = { not_executed: 0, executed: 0, error: 0 };
    
    notebookData.cells.forEach(cell => {
      if (cell.cell_type === 'code') {
        if (!cell.execution_count) {
          states.not_executed++;
        } else if (cell.outputs && cell.outputs.some(output => output.output_type === 'error')) {
          states.error++;
        } else {
          states.executed++;
        }
      }
    });
    
    return states;
  }

  classifyIntent(notebookData, conversationContext) {
    if (!notebookData.cells) return 'EXPLORE';

    const allContent = notebookData.cells
      .map(cell => Array.isArray(cell.source) ? cell.source.join('') : cell.source || '')
      .join(' ')
      .toLowerCase();

    // Data science keywords
    const exploreKeywords = ['analyze', 'explore', 'examine', 'investigate', 'understand', 'visualize', 'plot', 'chart', 'data', 'dataset', 'eda'];
    const implementKeywords = ['implement', 'create', 'build', 'develop', 'write', 'code', 'function', 'class', 'method', 'def ', 'import'];
    const debugKeywords = ['debug', 'fix', 'error', 'bug', 'issue', 'problem', 'troubleshoot', 'why', 'not working', 'traceback'];
    const refactorKeywords = ['refactor', 'optimize', 'improve', 'clean', 'restructure', 'reorganize', 'simplify'];
    const documentKeywords = ['document', 'comment', 'explain', 'describe', 'readme', 'docstring', 'annotation', '# '];

    const scores = {
      EXPLORE: exploreKeywords.filter(k => allContent.includes(k)).length,
      IMPLEMENT: implementKeywords.filter(k => allContent.includes(k)).length,
      DEBUG: debugKeywords.filter(k => allContent.includes(k)).length,
      REFACTOR: refactorKeywords.filter(k => allContent.includes(k)).length,
      DOCUMENT: documentKeywords.filter(k => allContent.includes(k)).length
    };

    const maxScore = Math.max(...Object.values(scores));
    if (maxScore === 0) return 'EXPLORE';

    return Object.keys(scores).find(key => scores[key] === maxScore);
  }

  async parseCursorDatabase() {
    try {
      // This would parse the actual Cursor database
      // For now, we'll simulate conversation detection
      console.log('Parsing Cursor database for conversations...');
      
      // Simulate finding a new conversation
      if (this.lastConversationId !== this.lastSessionId) {
        this.lastConversationId = this.lastSessionId;
        console.log('INFO: New conversation detected in Cursor');
      }
      
    } catch (error) {
      console.error('Error parsing Cursor database:', error);
    }
  }

  getActiveSessions() {
    return Array.from(this.activeSessions.values());
  }

  getSession(sessionId) {
    return this.activeSessions.get(sessionId);
  }

  endSession(sessionId) {
    const session = this.activeSessions.get(sessionId);
    if (session) {
      session.phase = 'COMPLETED';
      session.outcome = this.determineOutcome(session);
      session.timestamp = new Date().toISOString();
      
      console.log('Session ended:', sessionId, 'outcome:', session.outcome);
      return session;
    }
    return null;
  }

  simpleClassify(session) {
    // Simple intent classification based on code patterns and file names
    let intent = 'data_exploration';
    let confidence = 0.5;

    const codeContent = session.codeDeltas?.map(d => d.afterContent).join(' ') || '';
    const fileName = session.currentFile || '';

    // Check for implementation patterns
    if (codeContent.includes('def ') || codeContent.includes('class ') || codeContent.includes('function')) {
      intent = 'implementation';
      confidence = 0.7;
    }

    // Check for debug patterns
    if (codeContent.includes('print(') || codeContent.includes('console.log') || codeContent.includes('debug')) {
      intent = 'debug';
      confidence = 0.8;
    }

    // Check for exploration patterns (default for notebooks)
    if (fileName.includes('.ipynb') || codeContent.includes('import ') || codeContent.includes('plt.') || codeContent.includes('pd.')) {
      intent = 'data_exploration';
      confidence = 0.6;
    }

    return {
      primary_intent: intent,
      confidence: confidence,
      evidence: { code_patterns: true },
      signal_weights: { cell_analysis: 0.4, prompt_analysis: 0.2 },
      confidence_breakdown: { signal_agreement: 1, evidence_strength: confidence },
      all_evidences: [{ intent, confidence, weight: 1.0 }],
      intent_scores: { [intent]: { total_score: confidence, evidence_count: 1 } }
    };
  }

  determineOutcome(session) {
    const hasFileChanges = session.fileChanges.length > 0;
    const hasCodeDeltas = session.codeDeltas.length > 0;
    const sessionDuration = new Date() - new Date(session.timestamp);
    
    if (hasFileChanges && hasCodeDeltas) return 'SUCCESS';
    if (hasFileChanges) return 'IN_PROGRESS';
    if (sessionDuration > 300000) return 'STUCK'; // 5 minutes
    
    return 'IN_PROGRESS';
  }

  /**
   * Update session activity and calculate live duration
   */
  updateSessionActivity(sessionId) {
    const session = this.activeSessions.get(sessionId);
    if (!session) return;

    const now = new Date().toISOString();
    session.lastActivity = now;
    
    // Ensure session has startTime (for legacy sessions)
    if (!session.startTime) {
      session.startTime = session.timestamp;
    }
    
    // Calculate current duration in milliseconds
    const startTime = new Date(session.startTime);
    const currentTime = new Date(now);
    session.duration = currentTime - startTime;
    
    console.log(`Session ${sessionId} duration: ${this.formatDuration(session.duration)}`);
  }

  /**
   * Get live duration for a session
   */
  getLiveDuration(sessionId) {
    const session = this.activeSessions.get(sessionId);
    if (!session) return 0;

    // Handle sessions without startTime (legacy sessions)
    const startTime = session.startTime || session.timestamp;
    if (!startTime) return 0;

    if (session.endTime) {
      // Session ended, return final duration
      return new Date(session.endTime) - new Date(startTime);
    } else {
      // Session active, calculate current duration
      return new Date() - new Date(startTime);
    }
  }

  /**
   * Format duration in human-readable format
   */
  formatDuration(durationMs) {
    if (!durationMs || durationMs < 0) return '0s';
    
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  /**
   * End a session and set final duration
   */
  endSession(sessionId) {
    const session = this.activeSessions.get(sessionId);
    if (!session) return null;

    const now = new Date().toISOString();
    session.endTime = now;
    session.phase = 'COMPLETED';
    session.outcome = this.determineOutcome(session);
    session.duration = new Date(now) - new Date(session.startTime);
    
    console.log(`Session ended: ${sessionId}, duration: ${this.formatDuration(session.duration)}`);
    return session;
  }
}

module.exports = RealMonitor;
