/**
 * Post-mortem Service for PKL Extension
 * 
 * Tracks and documents when notebooks, projects, or resources become unavailable,
 * providing forensic analysis and recovery options for lost research assets.
 */

const fs = require('fs').promises;
const path = require('path');
const { nanoid } = require('nanoid');

class PostMortemService {
  constructor(dataStorage) {
    this.dataStorage = dataStorage;
    this.postMortemDir = path.join(process.cwd(), 'data', 'post-mortems');
    this.snapshotDir = path.join(process.cwd(), 'data', 'snapshots');
    this.init();
  }

  async init() {
    // Ensure directories exist
    await fs.mkdir(this.postMortemDir, { recursive: true });
    await fs.mkdir(this.snapshotDir, { recursive: true });
  }

  /**
   * Create a post-mortem when a resource becomes unavailable
   */
  async createPostMortem(resourceInfo) {
    const postMortemId = nanoid();
    const timestamp = new Date().toISOString();
    
    // Get last known snapshot
    const lastSnapshot = await this.getLastSnapshot(resourceInfo.filePath);
    
    // Analyze prior changes leading to disappearance
    const priorDiffs = await this.analyzePriorDiffs(resourceInfo.filePath);
    
    // Determine suspected cause
    const suspectedCause = await this.determineSuspectedCause(resourceInfo);
    
    // Search for archived versions
    const archivedVersions = await this.findArchivedVersions(resourceInfo);
    
    const postMortem = {
      id: postMortemId,
      timestamp,
      resourceInfo: {
        filePath: resourceInfo.filePath,
        fileName: path.basename(resourceInfo.filePath),
        lastSeen: resourceInfo.lastSeen,
        resourceType: this.determineResourceType(resourceInfo.filePath),
        size: resourceInfo.size || null,
        checksum: resourceInfo.checksum || null
      },
      lastSnapshot: lastSnapshot,
      priorDiffs: priorDiffs,
      suspectedCause: suspectedCause,
      archivedVersions: archivedVersions,
      recoveryOptions: await this.generateRecoveryOptions(resourceInfo, archivedVersions),
      forensicAnalysis: await this.performForensicAnalysis(resourceInfo, priorDiffs),
      status: 'active', // active, resolved, archived
      notes: [],
      createdAt: timestamp,
      updatedAt: timestamp
    };
    
    // Save post-mortem
    await this.savePostMortem(postMortem);
    
    console.log(`Post-mortem created: ${postMortemId} for ${resourceInfo.filePath}`);
    
    return postMortem;
  }

  /**
   * Get the last known snapshot of a resource
   */
  async getLastSnapshot(filePath) {
    try {
      const sessions = await this.dataStorage.loadSessions();
      const relevantSessions = sessions.filter(s => s.currentFile === filePath);
      
      if (relevantSessions.length === 0) {
        return null;
      }
      
      // Get most recent session
      const lastSession = relevantSessions.sort((a, b) => 
        new Date(b.timestamp) - new Date(a.timestamp)
      )[0];
      
      return {
        sessionId: lastSession.id,
        timestamp: lastSession.timestamp,
        metadata: {
          intent: lastSession.intent,
          outcome: lastSession.outcome,
          duration: lastSession.duration,
          executionStates: lastSession.executionStates
        },
        schema: await this.extractSchema(lastSession),
        codeDeltas: lastSession.codeDeltas?.slice(-5) || [], // Last 5 changes
        conversations: lastSession.conversations?.slice(-3) || [] // Last 3 conversations
      };
    } catch (error) {
      console.error('Error getting last snapshot:', error);
      return null;
    }
  }

  /**
   * Analyze changes leading up to disappearance
   */
  async analyzePriorDiffs(filePath) {
    try {
      const sessions = await this.dataStorage.loadSessions();
      const relevantSessions = sessions
        .filter(s => s.currentFile === filePath)
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, 10); // Last 10 sessions
      
      const diffs = [];
      
      for (const session of relevantSessions) {
        if (session.codeDeltas && session.codeDeltas.length > 0) {
          diffs.push({
            sessionId: session.id,
            timestamp: session.timestamp,
            changeCount: session.codeDeltas.length,
            changeTypes: [...new Set(session.codeDeltas.map(d => d.changeType))],
            significantChanges: session.codeDeltas.filter(d => d.lineCount > 10),
            lastChange: session.codeDeltas[session.codeDeltas.length - 1]
          });
        }
      }
      
      return {
        totalSessions: relevantSessions.length,
        totalChanges: diffs.reduce((sum, d) => sum + d.changeCount, 0),
        changePattern: this.analyzeChangePattern(diffs),
        recentActivity: diffs.slice(0, 3),
        timeline: diffs
      };
    } catch (error) {
      console.error('Error analyzing prior diffs:', error);
      return { error: error.message };
    }
  }

  /**
   * Determine suspected cause of disappearance
   */
  async determineSuspectedCause(resourceInfo) {
    const causes = [];
    
    // File system checks
    try {
      await fs.access(resourceInfo.filePath);
    } catch (error) {
      if (error.code === 'ENOENT') {
        causes.push({
          type: 'file_not_found',
          confidence: 0.9,
          description: 'File no longer exists at expected location',
          evidence: `ENOENT error for ${resourceInfo.filePath}`
        });
      } else if (error.code === 'EACCES') {
        causes.push({
          type: 'permission_denied',
          confidence: 0.8,
          description: 'File exists but access is denied',
          evidence: `EACCES error for ${resourceInfo.filePath}`
        });
      }
    }
    
    // Directory structure checks
    const parentDir = path.dirname(resourceInfo.filePath);
    try {
      const parentExists = await fs.access(parentDir);
      if (!parentExists) {
        causes.push({
          type: 'directory_moved',
          confidence: 0.7,
          description: 'Parent directory no longer exists',
          evidence: `Parent directory ${parentDir} not found`
        });
      }
    } catch (error) {
      causes.push({
        type: 'directory_moved',
        confidence: 0.7,
        description: 'Parent directory no longer accessible',
        evidence: error.message
      });
    }
    
    // Git repository checks
    if (resourceInfo.filePath.includes('.git') || await this.isInGitRepo(resourceInfo.filePath)) {
      causes.push({
        type: 'repo_moved',
        confidence: 0.6,
        description: 'File may be part of moved/deleted git repository',
        evidence: 'File path suggests git repository involvement'
      });
    }
    
    // Notebook-specific checks
    if (resourceInfo.filePath.endsWith('.ipynb')) {
      causes.push({
        type: 'notebook_cleanup',
        confidence: 0.5,
        description: 'Jupyter notebook may have been cleaned up or moved',
        evidence: 'File is a Jupyter notebook'
      });
    }
    
    // Determine primary cause
    const primaryCause = causes.length > 0 
      ? causes.reduce((max, cause) => cause.confidence > max.confidence ? cause : max)
      : {
          type: 'unknown',
          confidence: 0.1,
          description: 'Unable to determine cause of disappearance',
          evidence: 'No clear indicators found'
        };
    
    return {
      primary: primaryCause,
      all: causes,
      investigationSuggestions: this.generateInvestigationSuggestions(causes)
    };
  }

  /**
   * Find archived versions of the resource
   */
  async findArchivedVersions(resourceInfo) {
    const archivedVersions = [];
    
    // Check local backups/snapshots
    try {
      const snapshotFiles = await fs.readdir(this.snapshotDir);
      const relevantSnapshots = snapshotFiles.filter(file => 
        file.includes(path.basename(resourceInfo.filePath, path.extname(resourceInfo.filePath)))
      );
      
      for (const snapshot of relevantSnapshots) {
        const snapshotPath = path.join(this.snapshotDir, snapshot);
        const stats = await fs.stat(snapshotPath);
        
        archivedVersions.push({
          type: 'local_snapshot',
          location: snapshotPath,
          timestamp: stats.mtime.toISOString(),
          size: stats.size,
          confidence: 0.9
        });
      }
    } catch (error) {
      console.log('No local snapshots found');
    }
    
    // Generate Wayback Machine URLs (placeholder - would need actual API integration)
    if (resourceInfo.url) {
      archivedVersions.push({
        type: 'wayback_machine',
        location: `https://web.archive.org/web/*/${resourceInfo.url}`,
        timestamp: null,
        size: null,
        confidence: 0.6,
        note: 'Check Wayback Machine for archived versions'
      });
    }
    
    // Check git history if applicable
    if (await this.isInGitRepo(resourceInfo.filePath)) {
      archivedVersions.push({
        type: 'git_history',
        location: 'git log --follow -- ' + resourceInfo.filePath,
        timestamp: null,
        size: null,
        confidence: 0.8,
        note: 'Check git history for file movements or deletions'
      });
    }
    
    return archivedVersions;
  }

  /**
   * Generate recovery options
   */
  async generateRecoveryOptions(resourceInfo, archivedVersions) {
    const options = [];
    
    // Local snapshot recovery
    const localSnapshots = archivedVersions.filter(v => v.type === 'local_snapshot');
    if (localSnapshots.length > 0) {
      options.push({
        type: 'restore_from_snapshot',
        description: `Restore from local snapshot (${localSnapshots.length} available)`,
        action: `Copy from ${localSnapshots[0].location}`,
        confidence: 0.9,
        automated: true
      });
    }
    
    // Git recovery
    const gitVersions = archivedVersions.filter(v => v.type === 'git_history');
    if (gitVersions.length > 0) {
      options.push({
        type: 'git_restore',
        description: 'Restore from git history',
        action: `git checkout HEAD~1 -- ${resourceInfo.filePath}`,
        confidence: 0.8,
        automated: false
      });
    }
    
    // Recreation from session data
    const lastSnapshot = await this.getLastSnapshot(resourceInfo.filePath);
    if (lastSnapshot && lastSnapshot.codeDeltas) {
      options.push({
        type: 'recreate_from_deltas',
        description: 'Recreate file from code deltas',
        action: 'Reconstruct content from tracked code changes',
        confidence: 0.7,
        automated: true
      });
    }
    
    // Manual recovery suggestions
    options.push({
      type: 'manual_search',
      description: 'Manual search and recovery',
      action: 'Search file system, backups, and cloud storage',
      confidence: 0.3,
      automated: false
    });
    
    return options.sort((a, b) => b.confidence - a.confidence);
  }

  /**
   * Perform forensic analysis
   */
  async performForensicAnalysis(resourceInfo, priorDiffs) {
    const analysis = {
      timeline: [],
      patterns: [],
      riskFactors: [],
      recommendations: []
    };
    
    // Timeline analysis
    if (priorDiffs.timeline) {
      analysis.timeline = priorDiffs.timeline.map(diff => ({
        timestamp: diff.timestamp,
        event: `${diff.changeCount} changes made`,
        significance: diff.significantChanges.length > 0 ? 'high' : 'low'
      }));
    }
    
    // Pattern analysis
    if (priorDiffs.changePattern) {
      analysis.patterns.push({
        type: 'change_frequency',
        description: `Average ${Math.round(priorDiffs.totalChanges / priorDiffs.totalSessions)} changes per session`,
        implication: priorDiffs.totalChanges > 50 ? 'High activity before disappearance' : 'Normal activity'
      });
    }
    
    // Risk factor analysis
    const recentActivity = priorDiffs.recentActivity || [];
    if (recentActivity.length > 0) {
      const lastActivity = new Date(recentActivity[0].timestamp);
      const daysSinceActivity = (Date.now() - lastActivity.getTime()) / (1000 * 60 * 60 * 24);
      
      if (daysSinceActivity > 30) {
        analysis.riskFactors.push({
          type: 'stale_file',
          description: `No activity for ${Math.round(daysSinceActivity)} days`,
          severity: 'medium'
        });
      }
    }
    
    // Generate recommendations
    analysis.recommendations = [
      'Set up automated backups for critical notebooks',
      'Use version control (git) for important projects',
      'Regular snapshots of work-in-progress files',
      'Monitor file system changes for early detection'
    ];
    
    return analysis;
  }

  /**
   * Save post-mortem to file
   */
  async savePostMortem(postMortem) {
    const filePath = path.join(this.postMortemDir, `${postMortem.id}.json`);
    await fs.writeFile(filePath, JSON.stringify(postMortem, null, 2));
  }

  /**
   * Load all post-mortems
   */
  async loadPostMortems() {
    try {
      const files = await fs.readdir(this.postMortemDir);
      const postMortems = [];
      
      for (const file of files) {
        if (file.endsWith('.json')) {
          const content = await fs.readFile(path.join(this.postMortemDir, file), 'utf8');
          postMortems.push(JSON.parse(content));
        }
      }
      
      return postMortems.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    } catch (error) {
      console.error('Error loading post-mortems:', error);
      return [];
    }
  }

  /**
   * Export post-mortem as PDF report
   */
  async exportPostMortemPDF(postMortemId) {
    // This would require a PDF generation library like puppeteer or jsPDF
    // For now, return structured data that can be used to generate PDF
    
    const postMortems = await this.loadPostMortems();
    const postMortem = postMortems.find(pm => pm.id === postMortemId);
    
    if (!postMortem) {
      throw new Error('Post-mortem not found');
    }
    
    const reportData = {
      title: `Post-mortem Report: ${postMortem.resourceInfo.fileName}`,
      sections: [
        {
          title: 'Executive Summary',
          content: `Resource ${postMortem.resourceInfo.fileName} became unavailable on ${postMortem.timestamp}. Primary suspected cause: ${postMortem.suspectedCause.primary.description}.`
        },
        {
          title: 'Resource Information',
          content: postMortem.resourceInfo
        },
        {
          title: 'Last Known State',
          content: postMortem.lastSnapshot
        },
        {
          title: 'Change History',
          content: postMortem.priorDiffs
        },
        {
          title: 'Forensic Analysis',
          content: postMortem.forensicAnalysis
        },
        {
          title: 'Recovery Options',
          content: postMortem.recoveryOptions
        },
        {
          title: 'Archived Versions',
          content: postMortem.archivedVersions
        }
      ],
      metadata: {
        generated: new Date().toISOString(),
        postMortemId: postMortemId
      }
    };
    
    return reportData;
  }

  // Helper methods
  determineResourceType(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    const typeMap = {
      '.ipynb': 'jupyter_notebook',
      '.py': 'python_script',
      '.js': 'javascript',
      '.ts': 'typescript',
      '.md': 'markdown',
      '.json': 'json_data',
      '.csv': 'csv_data'
    };
    return typeMap[ext] || 'unknown';
  }

  async isInGitRepo(filePath) {
    try {
      const dir = path.dirname(filePath);
      await fs.access(path.join(dir, '.git'));
      return true;
    } catch {
      return false;
    }
  }

  analyzeChangePattern(diffs) {
    if (diffs.length === 0) return null;
    
    const changeFrequency = diffs.length;
    const avgChangesPerSession = diffs.reduce((sum, d) => sum + d.changeCount, 0) / diffs.length;
    const recentActivity = diffs.slice(0, 3);
    
    return {
      frequency: changeFrequency,
      avgChangesPerSession: Math.round(avgChangesPerSession),
      trend: recentActivity.length > 1 ? 'active' : 'declining',
      lastActivity: diffs[0]?.timestamp
    };
  }

  generateInvestigationSuggestions(causes) {
    const suggestions = [];
    
    if (causes.some(c => c.type === 'file_not_found')) {
      suggestions.push('Check if file was moved to a different location');
      suggestions.push('Search for files with similar names in the directory tree');
    }
    
    if (causes.some(c => c.type === 'repo_moved')) {
      suggestions.push('Check git log for file movements or deletions');
      suggestions.push('Look for repository forks or mirrors');
    }
    
    if (causes.some(c => c.type === 'permission_denied')) {
      suggestions.push('Check file and directory permissions');
      suggestions.push('Verify user access rights to the location');
    }
    
    suggestions.push('Check system logs for file system events');
    suggestions.push('Review backup systems for recent copies');
    
    return suggestions;
  }

  async extractSchema(session) {
    // Extract structural information from the last session
    const schema = {
      fileType: this.determineResourceType(session.currentFile || ''),
      cellCount: session.executionStates ? 
        Object.values(session.executionStates).reduce((a, b) => a + b, 0) : 0,
      hasCode: session.codeDeltas && session.codeDeltas.length > 0,
      hasConversations: session.conversations && session.conversations.length > 0,
      complexity: this.calculateComplexity(session)
    };
    
    return schema;
  }

  calculateComplexity(session) {
    let complexity = 0;
    
    if (session.codeDeltas) {
      complexity += session.codeDeltas.length * 0.1;
    }
    
    if (session.conversations) {
      complexity += session.conversations.length * 0.05;
    }
    
    if (session.duration) {
      complexity += Math.min(session.duration / 60000, 60) * 0.01; // Max 60 minutes
    }
    
    return Math.round(complexity * 100) / 100;
  }
}

module.exports = PostMortemService;
