#!/usr/bin/env node

const fs = require('fs').promises;
const path = require('path');

/**
 * Export Service for Cursor PKL Extension
 * Handles structured data export and download functionality
 */
class ExportService {
  constructor() {
    this.exportDir = path.join(__dirname, 'exports');
    this.ensureExportDir();
  }

  async ensureExportDir() {
    try {
      await fs.mkdir(this.exportDir, { recursive: true });
    } catch (error) {
      console.error('Error creating export directory:', error);
    }
  }

  /**
   * Generate comprehensive export data structure
   */
  async generateExportData(sessions, options = {}) {
    const {
      includeCodeDeltas = true,
      includeFileChanges = true,
      includeConversations = true,
      includeMetadata = true,
      dateRange = null,
      fileFilter = null
    } = options;

    const exportData = {
      metadata: {
        exportTimestamp: new Date().toISOString(),
        version: '1.0.0',
        totalSessions: sessions.length,
        exportOptions: options,
        generatedBy: 'Cursor PKL Extension'
      },
      summary: this.generateSummary(sessions),
      sessions: [],
      statistics: this.generateStatistics(sessions)
    };

    // Filter sessions based on options
    let filteredSessions = sessions;
    
    if (dateRange) {
      filteredSessions = this.filterByDateRange(sessions, dateRange);
    }
    
    if (fileFilter) {
      filteredSessions = this.filterByFile(sessions, fileFilter);
    }

    // Process each session
    for (const session of filteredSessions) {
      const sessionData = {
        id: session.id,
        timestamp: session.timestamp,
        intent: session.intent,
        phase: session.phase,
        outcome: session.outcome,
        confidence: session.confidence,
        currentFile: session.currentFile,
        duration: this.calculateSessionDuration(session),
        tags: session.tags || [],
        cursorPosition: session.cursorPosition,
        selectedText: session.selectedText,
        privacyMode: session.privacyMode,
        userConsent: session.userConsent,
        dataRetention: session.dataRetention,
        linkedEvents: session.linkedEvents || [],
        annotations: session.annotations || []
      };

      if (includeMetadata) {
        sessionData.metadata = {
          createdAt: session.timestamp,
          lastModified: session.timestamp,
          filePath: session.currentFile,
          intent: session.intent,
          outcome: session.outcome
        };
      }

      if (includeFileChanges && session.fileChanges) {
        sessionData.fileChanges = session.fileChanges.map(change => ({
          id: change.id,
          timestamp: change.timestamp,
          filePath: change.filePath,
          changeType: change.changeType,
          beforeSnippet: change.beforeSnippet,
          afterSnippet: change.afterSnippet,
          lineRange: change.lineRange,
          gitHash: change.gitHash
        }));
      }

      if (includeCodeDeltas && session.codeDeltas) {
        sessionData.codeDeltas = session.codeDeltas.map(delta => ({
          id: delta.id,
          timestamp: delta.timestamp,
          filePath: delta.filePath,
          beforeContent: delta.beforeContent,
          afterContent: delta.afterContent,
          diff: delta.diff,
          changeType: delta.changeType,
          lineCount: delta.lineCount,
          cellIndex: delta.cellIndex,
          executionCount: delta.executionCount,
          isSuggestion: delta.isSuggestion,
          suggestionStatus: delta.suggestionStatus
        }));
      }

      if (includeConversations && session.conversations) {
        sessionData.conversations = session.conversations.map(conv => ({
          id: conv.id,
          timestamp: conv.timestamp,
          role: conv.role,
          content: conv.content,
          context: conv.context,
          tokens: conv.tokens
        }));
      }

      // Add visualizations
      if (session.visualizations && session.visualizations.length > 0) {
        sessionData.visualizations = session.visualizations.map(viz => ({
          id: viz.id,
          sessionId: viz.sessionId,
          cellIndex: viz.cellIndex,
          outputIndex: viz.outputIndex,
          type: viz.type,
          format: viz.format,
          data: viz.data,
          timestamp: viz.timestamp,
          cellSource: viz.cellSource
        }));
      }

      // Add execution states
      if (session.executionStates) {
        sessionData.executionStates = session.executionStates;
      }

      // Add semantic analysis
      if (session.semanticAnalysis) {
        sessionData.semanticAnalysis = {
          primary_intent: session.semanticAnalysis.primary_intent,
          confidence: session.semanticAnalysis.confidence,
          evidence: session.semanticAnalysis.evidence,
          signal_weights: session.semanticAnalysis.signal_weights,
          confidence_breakdown: session.semanticAnalysis.confidence_breakdown,
          all_evidences: session.semanticAnalysis.all_evidences || [],
          intent_scores: session.semanticAnalysis.intent_scores || {}
        };
      }

      exportData.sessions.push(sessionData);
    }

    return exportData;
  }

  /**
   * Generate summary statistics
   */
  generateSummary(sessions) {
    const intents = {};
    const outcomes = {};
    const files = {};
    let totalCodeChanges = 0;
    let totalFileChanges = 0;
    let totalConversations = 0;
    let totalVisualizations = 0;
    let totalAnnotations = 0;
    let semanticAnalysisCount = 0;

    sessions.forEach(session => {
      // Intent distribution
      intents[session.intent] = (intents[session.intent] || 0) + 1;
      
      // Outcome distribution
      outcomes[session.outcome] = (outcomes[session.outcome] || 0) + 1;
      
      // File activity
      const fileName = session.currentFile;
      if (!files[fileName]) {
        files[fileName] = {
          sessions: 0,
          codeChanges: 0,
          fileChanges: 0,
          conversations: 0
        };
      }
      files[fileName].sessions++;
      
      // Count changes
      if (session.codeDeltas) {
        totalCodeChanges += session.codeDeltas.length;
        files[fileName].codeChanges += session.codeDeltas.length;
      }
      
      if (session.fileChanges) {
        totalFileChanges += session.fileChanges.length;
        files[fileName].fileChanges += session.fileChanges.length;
      }
      
      if (session.conversations) {
        totalConversations += session.conversations.length;
        files[fileName].conversations += session.conversations.length;
      }

      // Count new fields
      if (session.visualizations) {
        totalVisualizations += session.visualizations.length;
      }

      if (session.annotations) {
        totalAnnotations += session.annotations.length;
      }

      if (session.semanticAnalysis) {
        semanticAnalysisCount++;
      }
    });

    return {
      totalSessions: sessions.length,
      totalCodeChanges,
      totalFileChanges,
      totalConversations,
      totalVisualizations,
      totalAnnotations,
      semanticAnalysisCount,
      intentDistribution: intents,
      outcomeDistribution: outcomes,
      fileActivity: files,
      timeRange: {
        start: sessions.length > 0 ? Math.min(...sessions.map(s => new Date(s.timestamp).getTime())) : null,
        end: sessions.length > 0 ? Math.max(...sessions.map(s => new Date(s.timestamp).getTime())) : null
      }
    };
  }

  /**
   * Generate detailed statistics
   */
  generateStatistics(sessions) {
    const stats = {
      productivity: {
        averageSessionDuration: this.calculateAverageSessionDuration(sessions),
        mostActiveHour: this.findMostActiveHour(sessions),
        mostActiveDay: this.findMostActiveDay(sessions),
        successRate: this.calculateSuccessRate(sessions)
      },
      patterns: {
        commonIntents: this.findCommonPatterns(sessions, 'intent'),
        commonOutcomes: this.findCommonPatterns(sessions, 'outcome'),
        fileActivity: this.analyzeFileActivity(sessions)
      },
      insights: {
        peakProductivity: this.findPeakProductivity(sessions),
        workflowPatterns: this.analyzeWorkflowPatterns(sessions),
        suggestionAdoption: this.analyzeSuggestionAdoption(sessions)
      }
    };

    return stats;
  }

  /**
   * Filter sessions by date range
   */
  filterByDateRange(sessions, dateRange) {
    const { start, end } = dateRange;
    return sessions.filter(session => {
      const sessionDate = new Date(session.timestamp);
      return sessionDate >= new Date(start) && sessionDate <= new Date(end);
    });
  }

  /**
   * Filter sessions by file pattern
   */
  filterByFile(sessions, fileFilter) {
    const pattern = new RegExp(fileFilter, 'i');
    return sessions.filter(session => 
      pattern.test(session.currentFile)
    );
  }

  /**
   * Calculate session duration
   */
  calculateSessionDuration(session) {
    if (session.startTime && session.endTime) {
      return new Date(session.endTime) - new Date(session.startTime);
    }
    return null;
  }

  /**
   * Calculate average session duration
   */
  calculateAverageSessionDuration(sessions) {
    const durations = sessions
      .map(s => this.calculateSessionDuration(s))
      .filter(d => d !== null);
    
    if (durations.length === 0) return 0;
    return durations.reduce((a, b) => a + b, 0) / durations.length;
  }

  /**
   * Find most active hour
   */
  findMostActiveHour(sessions) {
    const hourCounts = {};
    sessions.forEach(session => {
      const hour = new Date(session.timestamp).getHours();
      hourCounts[hour] = (hourCounts[hour] || 0) + 1;
    });
    
    return Object.keys(hourCounts).reduce((a, b) => 
      hourCounts[a] > hourCounts[b] ? a : b
    );
  }

  /**
   * Find most active day
   */
  findMostActiveDay(sessions) {
    const dayCounts = {};
    sessions.forEach(session => {
      const day = new Date(session.timestamp).toDateString();
      dayCounts[day] = (dayCounts[day] || 0) + 1;
    });
    
    return Object.keys(dayCounts).reduce((a, b) => 
      dayCounts[a] > dayCounts[b] ? a : b
    );
  }

  /**
   * Calculate success rate
   */
  calculateSuccessRate(sessions) {
    const outcomes = sessions.map(s => s.outcome);
    const successCount = outcomes.filter(o => o === 'SUCCESS').length;
    return sessions.length > 0 ? (successCount / sessions.length) * 100 : 0;
  }

  /**
   * Find common patterns
   */
  findCommonPatterns(sessions, field) {
    const counts = {};
    sessions.forEach(session => {
      const value = session[field];
      counts[value] = (counts[value] || 0) + 1;
    });
    
    return Object.entries(counts)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 5)
      .map(([key, count]) => ({ [field]: key, count }));
  }

  /**
   * Analyze file activity
   */
  analyzeFileActivity(sessions) {
    const fileStats = {};
    sessions.forEach(session => {
      const file = session.currentFile;
      if (!fileStats[file]) {
        fileStats[file] = {
          sessions: 0,
          totalChanges: 0,
          avgChangesPerSession: 0
        };
      }
      fileStats[file].sessions++;
      fileStats[file].totalChanges += (session.codeDeltas?.length || 0) + (session.fileChanges?.length || 0);
    });

    // Calculate averages
    Object.keys(fileStats).forEach(file => {
      const stats = fileStats[file];
      stats.avgChangesPerSession = stats.totalChanges / stats.sessions;
    });

    return fileStats;
  }

  /**
   * Find peak productivity periods
   */
  findPeakProductivity(sessions) {
    const hourlyActivity = {};
    sessions.forEach(session => {
      const hour = new Date(session.timestamp).getHours();
      if (!hourlyActivity[hour]) {
        hourlyActivity[hour] = { sessions: 0, changes: 0 };
      }
      hourlyActivity[hour].sessions++;
      hourlyActivity[hour].changes += (session.codeDeltas?.length || 0);
    });

    return Object.entries(hourlyActivity)
      .sort(([,a], [,b]) => b.sessions - a.sessions)
      .slice(0, 3)
      .map(([hour, data]) => ({ hour: parseInt(hour), ...data }));
  }

  /**
   * Analyze workflow patterns
   */
  analyzeWorkflowPatterns(sessions) {
    const patterns = {
      exploreToImplement: 0,
      implementToDebug: 0,
      debugToRefactor: 0,
      refactorToDocument: 0
    };

    for (let i = 0; i < sessions.length - 1; i++) {
      const current = sessions[i];
      const next = sessions[i + 1];
      
      if (current.intent === 'EXPLORE' && next.intent === 'IMPLEMENT') {
        patterns.exploreToImplement++;
      } else if (current.intent === 'IMPLEMENT' && next.intent === 'DEBUG') {
        patterns.implementToDebug++;
      } else if (current.intent === 'DEBUG' && next.intent === 'REFACTOR') {
        patterns.debugToRefactor++;
      } else if (current.intent === 'REFACTOR' && next.intent === 'DOCUMENT') {
        patterns.refactorToDocument++;
      }
    }

    return patterns;
  }

  /**
   * Analyze suggestion adoption
   */
  analyzeSuggestionAdoption(sessions) {
    let totalSuggestions = 0;
    let acceptedSuggestions = 0;
    let rejectedSuggestions = 0;

    sessions.forEach(session => {
      if (session.codeDeltas) {
        session.codeDeltas.forEach(delta => {
          if (delta.isSuggestion) {
            totalSuggestions++;
            if (delta.suggestionStatus === 'accepted') {
              acceptedSuggestions++;
            } else if (delta.suggestionStatus === 'rejected') {
              rejectedSuggestions++;
            }
          }
        });
      }
    });

    return {
      totalSuggestions,
      acceptedSuggestions,
      rejectedSuggestions,
      acceptanceRate: totalSuggestions > 0 ? (acceptedSuggestions / totalSuggestions) * 100 : 0
    };
  }

  /**
   * Save export data to file
   */
  async saveExport(exportData, filename = null) {
    if (!filename) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      filename = `cursor-history-export-${timestamp}.json`;
    }

    const filePath = path.join(this.exportDir, filename);
    const jsonData = JSON.stringify(exportData, null, 2);
    
    await fs.writeFile(filePath, jsonData, 'utf8');
    
    return {
      filename,
      filePath,
      size: Buffer.byteLength(jsonData, 'utf8'),
      sessionCount: exportData.sessions.length
    };
  }

  /**
   * Get available exports
   */
  async getAvailableExports() {
    try {
      const files = await fs.readdir(this.exportDir);
      const exports = [];
      
      for (const file of files) {
        if (file.endsWith('.json')) {
          const filePath = path.join(this.exportDir, file);
          const stats = await fs.stat(filePath);
          exports.push({
            filename: file,
            filePath,
            size: stats.size,
            created: stats.birthtime,
            modified: stats.mtime
          });
        }
      }
      
      return exports.sort((a, b) => b.created - a.created);
    } catch (error) {
      console.error('Error reading exports directory:', error);
      return [];
    }
  }

  /**
   * Delete export file
   */
  async deleteExport(filename) {
    try {
      const filePath = path.join(this.exportDir, filename);
      await fs.unlink(filePath);
      return true;
    } catch (error) {
      console.error('Error deleting export:', error);
      return false;
    }
  }
}

module.exports = ExportService;
