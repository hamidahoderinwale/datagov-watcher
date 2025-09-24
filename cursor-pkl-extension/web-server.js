#!/usr/bin/env node

const express = require('express');
const path = require('path');
const RealMonitor = require('./real-monitor');
const DataStorage = require('./data-storage');
const ExportService = require('./export-service');
const AppleScriptService = require('./applescript-service');
const PrivacyService = require('./privacy-service');
const ProcedurePatternService = require('./procedure-patterns');
const PostMortemService = require('./post-mortem-service');

const app = express();
const port = 3000;

// Initialize components
const realMonitor = new RealMonitor();
const dataStorage = new DataStorage();
const exportService = new ExportService();
const privacyService = new PrivacyService();
const procedureService = new ProcedurePatternService();
const postMortemService = new PostMortemService(dataStorage);

// Connect data storage to real monitor
realMonitor.dataStorage = dataStorage;

// Initialize data storage and run maintenance
async function initializeDataStorage() {
  try {
    // Run data migration
    await dataStorage.migrateData();
    
    // Clean up old data (keep last 30 days)
    await dataStorage.cleanupOldData(30);
    
    // Get data size info
    const dataSize = await dataStorage.getDataSize();
    console.log(`Data storage: ${dataSize.totalSizeMB}MB (${dataSize.stats.totalSessions} sessions)`);
  } catch (error) {
    console.error('Error initializing data storage:', error);
  }
}

// Initialize and start monitoring
initializeDataStorage().then(() => {
  realMonitor.startMonitoring();
});

// Middleware
app.use(express.json());

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  
  // Handle preflight requests
  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
  } else {
    next();
  }
});

app.use(express.static(path.join(__dirname)));

// API Routes
app.get('/api/sessions', async (req, res) => {
  try {
    const activeSessions = realMonitor.getActiveSessions();
    const storedSessions = await dataStorage.loadSessions();
    
    // Combine and deduplicate
    const allSessions = [...activeSessions, ...storedSessions];
    const uniqueSessions = deduplicateSessions(allSessions);
    
    // Sort by timestamp
    uniqueSessions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    res.json({ 
      success: true, 
      sessions: uniqueSessions,
      count: uniqueSessions.length
    });
  } catch (error) {
    console.error('Error getting sessions:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/session/:id', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const session = realMonitor.getSession(sessionId) || await dataStorage.getSession(sessionId);
    
    if (!session) {
      return res.status(404).json({ success: false, error: 'Session not found' });
    }
    
    const conversations = await dataStorage.getConversationsForSession(sessionId);
    const fileChanges = await dataStorage.getFileChangesForSession(sessionId);
    const annotations = await dataStorage.getAnnotationsForSession(sessionId);
    
    res.json({
      success: true,
      session: {
        ...session,
        conversations,
        fileChanges,
        annotations
      }
    });
  } catch (error) {
    console.error('Error getting session details:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/session/:id/annotation', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const { content } = req.body;
    
    const annotation = {
      id: 'annotation-' + Date.now(),
      sessionId: sessionId,
      timestamp: new Date().toISOString(),
      content: content,
      tags: []
    };
    
    await dataStorage.saveAnnotation(annotation);
    
    res.json({ success: true, annotation });
  } catch (error) {
    console.error('Error adding annotation:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Return to context - opens file in Cursor IDE and restores session state (Enhanced)
app.post('/api/session/:id/return-to-context', async (req, res) => {
  try {
    const sessionId = req.params.id;
    
    // Get session from active sessions or storage
    const session = realMonitor.getSession(sessionId) || await dataStorage.getSession(sessionId);
    
    if (!session) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found' 
      });
    }
    
    console.log('INFO: Return to context requested for session:', sessionId);
    console.log('   - File:', session.currentFile);
    console.log('   - Position:', session.cursorPosition);
    
    // Use enhanced AppleScript to restore context in Cursor IDE
    const result = await AppleScriptService.restoreSessionContext({
      currentFile: session.currentFile,
      cursorPosition: session.cursorPosition,
      selectedText: session.selectedText
    });
    
    if (result.success) {
      console.log('SUCCESS: Session context restored in Cursor IDE');
      console.log('   - Method:', result.method);
      res.json({ 
        success: true, 
        message: 'Session context restored in Cursor IDE',
        method: result.method,
        session: {
          id: session.id,
          file: session.currentFile,
          timestamp: session.timestamp,
          position: result.position
        }
      });
    } else {
      console.log('WARNING: Failed to restore session context:', result.error);
      res.status(500).json({ 
        success: false, 
        error: result.error || 'Failed to restore context in Cursor IDE. Please ensure Cursor is accessible.' 
      });
    }
  } catch (error) {
    console.error('Error restoring session context:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const stats = await dataStorage.getStats();
    const activeSessions = realMonitor.getActiveSessions();
    
    res.json({
      success: true,
      stats: {
        ...stats,
        activeSessions: activeSessions.length,
        lastUpdate: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Error getting stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get live durations for all active sessions
app.get('/api/sessions/live-durations', (req, res) => {
  try {
    const activeSessions = realMonitor.getActiveSessions();
    const liveDurations = {};
    
    activeSessions.forEach(session => {
      const duration = realMonitor.getLiveDuration(session.id);
      liveDurations[session.id] = {
        duration: duration,
        formatted: realMonitor.formatDuration(duration),
        startTime: session.startTime,
        lastActivity: session.lastActivity,
        isActive: !session.endTime
      };
    });
    
    res.json({
      success: true,
      durations: liveDurations,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error getting live durations:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get visualizations from a notebook session
app.get('/api/session/:id/visualizations', async (req, res) => {
  try {
    const sessionId = req.params.id;
    
    // Get session from active sessions or storage
    const session = realMonitor.getSession(sessionId) || await dataStorage.getSession(sessionId);
    
    if (!session) {
      return res.status(404).json({ 
        success: false, 
        error: 'Session not found' 
      });
    }

    // Check if session has a notebook file
    if (!session.currentFile || !session.currentFile.endsWith('.ipynb')) {
      return res.json({
        success: true,
        visualizations: [],
        message: 'Session does not contain a notebook file'
      });
    }

    console.log('INFO: Extracting visualizations for session:', sessionId);
    console.log('   - File:', session.currentFile);

    // Extract visualizations from the notebook
    const result = await AppleScriptService.extractNotebookVisualizations(session.currentFile);
    
    if (result.error) {
      console.log('WARNING: Failed to extract visualizations:', result.error);
      return res.status(500).json({
        success: false,
        error: result.error
      });
    }

    console.log('SUCCESS: Extracted', result.total, 'visualizations');
    res.json({
      success: true,
      visualizations: result.visualizations,
      total: result.total,
      file: result.file,
      lastModified: result.lastModified,
      session: {
        id: session.id,
        file: session.currentFile,
        timestamp: session.timestamp
      }
    });
  } catch (error) {
    console.error('Error extracting visualizations:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Get all visualizations from all notebook sessions
app.get('/api/visualizations', async (req, res) => {
  try {
    const allSessions = [
      ...realMonitor.getActiveSessions(),
      ...(await dataStorage.loadSessions())
    ];

    // Filter for notebook sessions and deduplicate
    const notebookSessions = allSessions
      .filter(session => session.currentFile && session.currentFile.endsWith('.ipynb'))
      .reduce((unique, session) => {
        const existing = unique.find(s => s.currentFile === session.currentFile);
        if (!existing || new Date(session.timestamp) > new Date(existing.timestamp)) {
          return [...unique.filter(s => s.currentFile !== session.currentFile), session];
        }
        return unique;
      }, []);

    const allVisualizations = [];
    
    for (const session of notebookSessions) {
      try {
        const result = await AppleScriptService.extractNotebookVisualizations(session.currentFile);
        if (result.visualizations && result.visualizations.length > 0) {
          allVisualizations.push({
            sessionId: session.id,
            file: session.currentFile,
            timestamp: session.timestamp,
            visualizations: result.visualizations,
            total: result.total
          });
        }
      } catch (error) {
        console.log('Skipping visualization extraction for', session.currentFile, ':', error.message);
      }
    }

    console.log('INFO: Found visualizations in', allVisualizations.length, 'notebook sessions');
    res.json({
      success: true,
      sessions: allVisualizations,
      totalSessions: allVisualizations.length,
      totalVisualizations: allVisualizations.reduce((sum, s) => sum + s.total, 0)
    });
  } catch (error) {
    console.error('Error getting all visualizations:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Export API endpoints
app.post('/api/export', async (req, res) => {
  try {
    const options = req.body?.options || {};
    
    // Get all sessions
    const activeSessions = realMonitor.getActiveSessions();
    const storedSessions = await dataStorage.loadSessions();
    const allSessions = [...activeSessions, ...storedSessions];
    const uniqueSessions = deduplicateSessions(allSessions);
    
    // Generate export data
    const exportData = await exportService.generateExportData(uniqueSessions, options);
    
    // Save to file
    const result = await exportService.saveExport(exportData);
    
    res.json({
      success: true,
      export: {
        filename: result.filename,
        size: result.size,
        sessionCount: result.sessionCount,
        downloadUrl: `/api/export/download/${result.filename}`
      }
    });
  } catch (error) {
    console.error('Error creating export:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/export/list', async (req, res) => {
  try {
    const exports = await exportService.getAvailableExports();
    res.json({
      success: true,
      exports: exports.map(exp => ({
        filename: exp.filename,
        size: exp.size,
        created: exp.created,
        modified: exp.modified,
        downloadUrl: `/api/export/download/${exp.filename}`
      }))
    });
  } catch (error) {
    console.error('Error listing exports:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/export/download/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const filePath = path.join(exportService.exportDir, filename);
    
    // Check if file exists
    try {
      await require('fs').promises.access(filePath);
    } catch (error) {
      return res.status(404).json({ success: false, error: 'Export file not found' });
    }
    
    res.download(filePath, filename, (err) => {
      if (err) {
        console.error('Error downloading file:', err);
        res.status(500).json({ success: false, error: 'Download failed' });
      }
    });
  } catch (error) {
    console.error('Error downloading export:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.delete('/api/export/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const success = await exportService.deleteExport(filename);
    
    if (success) {
      res.json({ success: true, message: 'Export deleted successfully' });
    } else {
      res.status(404).json({ success: false, error: 'Export file not found' });
    }
  } catch (error) {
    console.error('Error deleting export:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get conversations for a specific session
app.get('/api/session/:id/conversations', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const conversations = await dataStorage.getConversationsForSession(sessionId);
    res.json({ success: true, conversations, count: conversations.length });
  } catch (error) {
    console.error('Error getting conversations:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get all conversations
app.get('/api/conversations', async (req, res) => {
  try {
    const conversations = await dataStorage.getAllConversations();
    res.json({ success: true, conversations, count: conversations.length });
  } catch (error) {
    console.error('Error getting all conversations:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Save a new conversation
app.post('/api/conversations', async (req, res) => {
  try {
    const conversation = req.body;
    const result = await dataStorage.saveConversation(conversation);
    res.json({ success: true, conversation, result });
  } catch (error) {
    console.error('Error saving conversation:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Serve the live dashboard
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'live-dashboard-clean.html'));
});

// Serve privacy analysis view
app.get('/privacy-analysis', (req, res) => {
  res.sendFile(path.join(__dirname, 'components/privacy-analysis/privacy-analysis.html'));
});

// Serve post-mortem analysis view
app.get('/post-mortem', (req, res) => {
  res.sendFile(path.join(__dirname, 'components/post-mortem/post-mortem.html'));
});

// ============================================================================
// PROCEDURAL PATTERN API ENDPOINTS
// ============================================================================

// Get procedure suggestions for a session
app.get('/api/session/:id/suggestions', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const sessions = await dataStorage.loadSessions();
    const currentSession = sessions.find(s => s.id === sessionId);
    
    if (!currentSession) {
      return res.status(404).json({ success: false, error: 'Session not found' });
    }
    
    // Get all patterns (built-in + discovered)
    const allPatterns = await procedureService.identifyPatternsFromSessions(sessions);
    
    // Get suggestions for current context
    const suggestions = procedureService.getSuggestionsForContext(currentSession, allPatterns);
    
    res.json({
      success: true,
      sessionId,
      suggestions,
      totalPatterns: allPatterns.length
    });
  } catch (error) {
    console.error('Error getting procedure suggestions:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get all available procedure patterns
app.get('/api/procedures/patterns', async (req, res) => {
  try {
    const sessions = await dataStorage.loadSessions();
    const patterns = await procedureService.identifyPatternsFromSessions(sessions);
    
    res.json({
      success: true,
      patterns: patterns.map(p => ({
        id: p.id,
        name: p.name,
        category: p.category,
        description: p.description,
        successRate: p.successRate,
        avgDuration: p.avgDuration,
        parameters: p.parameters
      })),
      builtInCount: procedureService.builtInPatterns.length,
      discoveredCount: patterns.length - procedureService.builtInPatterns.length
    });
  } catch (error) {
    console.error('Error getting procedure patterns:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Execute a procedure pattern
app.post('/api/procedures/execute', async (req, res) => {
  try {
    const { procedureId, parameters, targetFile } = req.body;
    
    if (!procedureId) {
      return res.status(400).json({ success: false, error: 'Procedure ID is required' });
    }
    
    // Get the pattern
    const sessions = await dataStorage.loadSessions();
    const allPatterns = await procedureService.identifyPatternsFromSessions(sessions);
    const pattern = allPatterns.find(p => p.id === procedureId);
    
    if (!pattern) {
      return res.status(404).json({ success: false, error: 'Procedure pattern not found' });
    }
    
    // Generate executable notebook
    const notebook = procedureService.generateExecutableNotebook(pattern, parameters || {});
    
    // Save notebook to file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `${pattern.id}-${timestamp}.ipynb`;
    const notebookPath = path.join(__dirname, 'generated-notebooks', filename);
    
    // Ensure directory exists
    const fs = require('fs');
    const generatedDir = path.join(__dirname, 'generated-notebooks');
    if (!fs.existsSync(generatedDir)) {
      fs.mkdirSync(generatedDir, { recursive: true });
    }
    
    // Write notebook file
    fs.writeFileSync(notebookPath, JSON.stringify(notebook, null, 2));
    
    console.log(`Generated procedure notebook: ${filename}`);
    console.log(`  - Pattern: ${pattern.name}`);
    console.log(`  - Parameters: ${JSON.stringify(parameters)}`);
    console.log(`  - Cells: ${notebook.cells.length}`);
    
    res.json({
      success: true,
      procedureId,
      patternName: pattern.name,
      generatedNotebook: notebookPath,
      filename,
      cellCount: notebook.cells.length,
      parameters: parameters || {}
    });
  } catch (error) {
    console.error('Error executing procedure:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Open generated notebook in Cursor
app.post('/api/cursor/open-notebook', async (req, res) => {
  try {
    const { notebookPath, openInCursor = true } = req.body;
    
    if (!notebookPath) {
      return res.status(400).json({ success: false, error: 'Notebook path is required' });
    }
    
    if (openInCursor) {
      // Use AppleScript service to open in Cursor
      const success = await AppleScriptService.openFileInCursor(notebookPath);
      
      if (success) {
        res.json({
          success: true,
          message: 'Notebook opened in Cursor IDE',
          notebookPath
        });
      } else {
        res.json({
          success: false,
          error: 'Failed to open notebook in Cursor IDE',
          notebookPath
        });
      }
    } else {
      res.json({
        success: true,
        message: 'Notebook generated successfully',
        notebookPath
      });
    }
  } catch (error) {
    console.error('Error opening notebook in Cursor:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get procedure execution history
app.get('/api/procedures/history', async (req, res) => {
  try {
    const fs = require('fs');
    const generatedDir = path.join(__dirname, 'generated-notebooks');
    
    if (!fs.existsSync(generatedDir)) {
      return res.json({ success: true, history: [] });
    }
    
    const files = fs.readdirSync(generatedDir)
      .filter(file => file.endsWith('.ipynb'))
      .map(file => {
        const filePath = path.join(generatedDir, file);
        const stats = fs.statSync(filePath);
        const parts = file.replace('.ipynb', '').split('-');
        const procedureId = parts[0];
        
        return {
          filename: file,
          procedureId,
          createdAt: stats.birthtime,
          size: stats.size,
          path: filePath
        };
      })
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    
    res.json({
      success: true,
      history: files,
      totalGenerated: files.length
    });
  } catch (error) {
    console.error('Error getting procedure history:', error);
    res.status(500).json({ success: false, error: error.message          });
       }
     });

// ============================================================================
// POST-MORTEM ANALYSIS API ENDPOINTS
// ============================================================================

// Get all post-mortems
app.get('/api/post-mortems', async (req, res) => {
  try {
    const postMortems = await postMortemService.loadPostMortems();
    
    res.json({
      success: true,
      postMortems: postMortems.map(pm => ({
        id: pm.id,
        timestamp: pm.timestamp,
        resourceInfo: pm.resourceInfo,
        suspectedCause: pm.suspectedCause.primary,
        status: pm.status,
        recoveryOptionsCount: pm.recoveryOptions.length,
        archivedVersionsCount: pm.archivedVersions.length
      })),
      totalCount: postMortems.length
    });
  } catch (error) {
    console.error('Error getting post-mortems:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get specific post-mortem details
app.get('/api/post-mortem/:id', async (req, res) => {
  try {
    const postMortems = await postMortemService.loadPostMortems();
    const postMortem = postMortems.find(pm => pm.id === req.params.id);
    
    if (!postMortem) {
      return res.status(404).json({ success: false, error: 'Post-mortem not found' });
    }
    
    res.json({
      success: true,
      postMortem
    });
  } catch (error) {
    console.error('Error getting post-mortem:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Create new post-mortem for missing resource
app.post('/api/post-mortem/create', async (req, res) => {
  try {
    const { filePath, lastSeen, size, checksum, url } = req.body;
    
    if (!filePath) {
      return res.status(400).json({ success: false, error: 'File path is required' });
    }
    
    const resourceInfo = {
      filePath,
      lastSeen: lastSeen || new Date().toISOString(),
      size,
      checksum,
      url
    };
    
    const postMortem = await postMortemService.createPostMortem(resourceInfo);
    
    res.json({
      success: true,
      postMortemId: postMortem.id,
      message: 'Post-mortem created successfully',
      suspectedCause: postMortem.suspectedCause.primary,
      recoveryOptions: postMortem.recoveryOptions.length
    });
  } catch (error) {
    console.error('Error creating post-mortem:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Export post-mortem as PDF report
app.post('/api/post-mortem/:id/export-pdf', async (req, res) => {
  try {
    const postMortemId = req.params.id;
    const reportData = await postMortemService.exportPostMortemPDF(postMortemId);
    
    // Generate filename
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `post-mortem-${postMortemId}-${timestamp}.json`;
    
    // For now, export as JSON (would be PDF in full implementation)
    const exportPath = path.join(__dirname, 'exports', filename);
    const fs = require('fs');
    const exportDir = path.join(__dirname, 'exports');
    
    if (!fs.existsSync(exportDir)) {
      fs.mkdirSync(exportDir, { recursive: true });
    }
    
    fs.writeFileSync(exportPath, JSON.stringify(reportData, null, 2));
    
    res.json({
      success: true,
      filename,
      reportData,
      downloadUrl: `/api/export/download/${filename}`
    });
  } catch (error) {
    console.error('Error exporting post-mortem PDF:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Update post-mortem status or add notes
app.put('/api/post-mortem/:id', async (req, res) => {
  try {
    const postMortemId = req.params.id;
    const { status, notes } = req.body;
    
    const postMortems = await postMortemService.loadPostMortems();
    const postMortem = postMortems.find(pm => pm.id === postMortemId);
    
    if (!postMortem) {
      return res.status(404).json({ success: false, error: 'Post-mortem not found' });
    }
    
    // Update fields
    if (status) postMortem.status = status;
    if (notes) postMortem.notes.push({
      timestamp: new Date().toISOString(),
      content: notes
    });
    postMortem.updatedAt = new Date().toISOString();
    
    // Save updated post-mortem
    await postMortemService.savePostMortem(postMortem);
    
    res.json({
      success: true,
      message: 'Post-mortem updated successfully',
      postMortem
    });
  } catch (error) {
    console.error('Error updating post-mortem:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Check for missing files and create post-mortems automatically
app.post('/api/post-mortem/scan-missing', async (req, res) => {
  try {
    const sessions = await dataStorage.loadSessions();
    const uniqueFiles = [...new Set(sessions.map(s => s.currentFile).filter(Boolean))];
    
    const missingFiles = [];
    const fs = require('fs');
    
    for (const filePath of uniqueFiles) {
      try {
        await fs.promises.access(filePath);
      } catch (error) {
        if (error.code === 'ENOENT') {
          missingFiles.push(filePath);
        }
      }
    }
    
    const createdPostMortems = [];
    
    for (const filePath of missingFiles) {
      // Check if post-mortem already exists
      const existingPostMortems = await postMortemService.loadPostMortems();
      const exists = existingPostMortems.some(pm => pm.resourceInfo.filePath === filePath);
      
      if (!exists) {
        const lastSession = sessions
          .filter(s => s.currentFile === filePath)
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
        
        const postMortem = await postMortemService.createPostMortem({
          filePath,
          lastSeen: lastSession ? lastSession.timestamp : new Date().toISOString()
        });
        
        createdPostMortems.push(postMortem.id);
      }
    }
    
    res.json({
      success: true,
      scannedFiles: uniqueFiles.length,
      missingFiles: missingFiles.length,
      newPostMortems: createdPostMortems.length,
      createdIds: createdPostMortems
    });
  } catch (error) {
    console.error('Error scanning for missing files:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Serve static assets
app.use('/assets', express.static(path.join(__dirname, 'assets')));
app.use('/components', express.static(path.join(__dirname, 'components')));

// Utility function to deduplicate sessions
function deduplicateSessions(sessions) {
  const seen = new Map();
  const unique = [];
  
  for (const session of sessions) {
    const key = session.currentFile + '_' + session.intent;
    const existing = seen.get(key);
    
    if (!existing || new Date(session.timestamp) > new Date(existing.timestamp)) {
      if (existing) {
        const index = unique.indexOf(existing);
        if (index > -1) unique.splice(index, 1);
      }
      seen.set(key, session);
      unique.push(session);
    }
  }
  
  return unique;
}

// Privacy Analysis API Endpoints

// Get privacy-transformed workflow data
app.post('/api/privacy/analyze', async (req, res) => {
  try {
    const { config } = req.body;
    
    // Update privacy configuration if provided
    if (config) {
      privacyService.updateConfig(config);
    }
    
    // Get all sessions
    const sessions = await dataStorage.loadSessions();
    
    // Collect workflow data
    const workflows = await privacyService.collectWorkflowData(sessions);
    
    // Apply privacy transformations
    const transformedWorkflows = await privacyService.applyPrivacyTransformations(workflows);
    
    // Measure expressiveness
    const metrics = await privacyService.measureExpressiveness(workflows, transformedWorkflows);
    
    res.json({
      success: true,
      originalWorkflows: workflows,
      transformedWorkflows: transformedWorkflows,
      expressivenessMetrics: metrics,
      privacyConfig: privacyService.privacyConfig
    });
  } catch (error) {
    console.error('Error in privacy analysis:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Update privacy configuration
app.post('/api/privacy/config', async (req, res) => {
  try {
    const { config } = req.body;
    privacyService.updateConfig(config);
    
    res.json({
      success: true,
      config: privacyService.privacyConfig
    });
  } catch (error) {
    console.error('Error updating privacy config:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get privacy analysis statistics
app.get('/api/privacy/stats', async (req, res) => {
  try {
    const sessions = await dataStorage.loadSessions();
    const workflows = await privacyService.collectWorkflowData(sessions);
    
    // Calculate aggregate statistics
    const stats = {
      totalSessions: workflows.length,
      totalTokens: workflows.reduce((sum, w) => {
        return sum + w.traces.reduce((traceSum, trace) => {
          return traceSum + (trace.tokens ? trace.tokens.length : 0);
        }, 0);
      }, 0),
      privacyViolations: Math.floor(Math.random() * 10), // Simulated for demo
      avgRedactionRate: privacyService.privacyConfig.redactionLevel,
      avgExpressionScore: 0.75, // Would be calculated from recent analyses
      clusterCount: 5 // Would be calculated from clustering analysis
    };
    
    res.json({
      success: true,
      stats: stats,
      privacyConfig: privacyService.privacyConfig
    });
  } catch (error) {
    console.error('Error getting privacy stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Export privacy analysis
app.post('/api/privacy/export', async (req, res) => {
  try {
    const { format, options } = req.body;
    
    // Get sessions and perform analysis
    const sessions = await dataStorage.loadSessions();
    const workflows = await privacyService.collectWorkflowData(sessions);
    const transformedWorkflows = await privacyService.applyPrivacyTransformations(workflows);
    const metrics = await privacyService.measureExpressiveness(workflows, transformedWorkflows);
    
    // Create export data
    const exportData = {
      timestamp: new Date().toISOString(),
      privacyConfig: privacyService.privacyConfig,
      workflows: options?.includeWorkflows ? workflows : undefined,
      transformedWorkflows: options?.includeTransformed ? transformedWorkflows : undefined,
      expressivenessMetrics: options?.includeMetrics ? metrics : undefined,
      summary: {
        totalSessions: workflows.length,
        avgExpressionScore: metrics.expressivenessScore,
        privacyBudget: privacyService.privacyConfig.epsilon
      }
    };
    
    // Generate filename
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `privacy-analysis-${timestamp}.${format}`;
    
    // Export using existing export service
    const exportPath = await exportService.exportData(exportData, format, filename);
    
    res.json({
      success: true,
      filename: filename,
      path: exportPath,
      size: JSON.stringify(exportData).length
    });
  } catch (error) {
    console.error('Error exporting privacy analysis:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Initialize conversation monitoring
async function initializeConversationMonitoring() {
  try {
    const { CursorDBParser } = require('./dist/services/cursor-db-parser');
    const cursorDbParser = new CursorDBParser();
    await cursorDbParser.connect();
    
    // Start monitoring for new conversations
    await cursorDbParser.startConversationMonitoring(async (conversations) => {
      console.log(`Processing ${conversations.length} new conversations`);
      for (const conversation of conversations) {
        await dataStorage.saveConversation(conversation);
      }
    });
    
    console.log('Conversation monitoring started');
  } catch (error) {
    console.log('WARNING: Conversation monitoring not available (Cursor DB not accessible):', error.message);
    console.log('INFO: To capture real conversations, ensure Cursor IDE is running and accessible');
    
    // We can still manually add conversations via the API
    console.log('Conversation API endpoints available:');
    console.log('   - POST /api/conversations');
    console.log('   - GET /api/conversations');
    console.log('   - GET /api/session/:id/conversations');
  }
}

// Start server
app.listen(port, async () => {
  console.log('PKL Web Server Started');
  console.log('========================');
  console.log(`Dashboard: http://localhost:${port}`);
  console.log(`API: http://localhost:${port}/api/sessions`);
  console.log('Monitoring .ipynb files...');
  
  // Initialize conversation monitoring
  await initializeConversationMonitoring();
  
  console.log('');
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down server...');
  realMonitor.stopMonitoring();
    process.exit(0);
});
