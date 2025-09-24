const fs = require('fs');
const path = require('path');

/**
 * JSON-based data storage manager
 * Handles session data persistence and retrieval
 */
class DataStorage {
  constructor() {
    this.dataDir = path.join(process.env.HOME || '', '.pkl');
    this.sessionsFile = path.join(this.dataDir, 'sessions.json');
    this.conversationsFile = path.join(this.dataDir, 'conversations.json');
    this.fileChangesFile = path.join(this.dataDir, 'file-changes.json');
    this.annotationsFile = path.join(this.dataDir, 'annotations.json');
    
    this.ensureDataDirectory();
  }

  ensureDataDirectory() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  // Session management
  async saveSession(session) {
    try {
      const sessions = await this.loadSessions();
      const existingIndex = sessions.findIndex(s => s.id === session.id);
      
      if (existingIndex >= 0) {
        sessions[existingIndex] = session;
      } else {
        sessions.push(session);
      }
      
      await this.saveToFile(this.sessionsFile, sessions);
      
      // Also save to individual session file for better persistence
      await this.saveIndividualSession(session);
      
      return { success: true };
    } catch (error) {
      console.error('Error saving session:', error);
      return { success: false, error: error.message };
    }
  }

  async saveIndividualSession(session) {
    try {
      const sessionFile = path.join(this.dataDir, 'sessions', `${session.id}.json`);
      const sessionDir = path.dirname(sessionFile);
      
      if (!fs.existsSync(sessionDir)) {
        fs.mkdirSync(sessionDir, { recursive: true });
      }
      
      fs.writeFileSync(sessionFile, JSON.stringify(session, null, 2));
    } catch (error) {
      console.error('Error saving individual session:', error);
    }
  }

  async loadIndividualSession(sessionId) {
    try {
      const sessionFile = path.join(this.dataDir, 'sessions', `${sessionId}.json`);
      if (!fs.existsSync(sessionFile)) {
        return null;
      }
      
      const data = fs.readFileSync(sessionFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.error('Error loading individual session:', error);
      return null;
    }
  }

  async loadAllIndividualSessions() {
    try {
      const sessionsDir = path.join(this.dataDir, 'sessions');
      
      if (!fs.existsSync(sessionsDir)) {
        return [];
      }
      
      const files = fs.readdirSync(sessionsDir);
      const sessions = [];
      
      for (const file of files) {
        if (file.endsWith('.json')) {
          try {
            const sessionData = fs.readFileSync(path.join(sessionsDir, file), 'utf8');
            const session = JSON.parse(sessionData);
            sessions.push(session);
          } catch (error) {
            console.error(`Error loading session file ${file}:`, error);
          }
        }
      }
      
      return sessions;
    } catch (error) {
      console.error('Error loading individual sessions:', error);
      return [];
    }
  }

  async loadSessions() {
    try {
      // Load from main sessions file
      let mainSessions = [];
      if (fs.existsSync(this.sessionsFile)) {
        const data = fs.readFileSync(this.sessionsFile, 'utf8');
        mainSessions = JSON.parse(data);
      }
      
      // Load from individual session files
      const individualSessions = await this.loadAllIndividualSessions();
      
      // Merge and deduplicate sessions
      const allSessions = [...mainSessions, ...individualSessions];
      const uniqueSessions = this.deduplicateSessions(allSessions);
      
      // Sort by timestamp (newest first)
      uniqueSessions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      return uniqueSessions;
    } catch (error) {
      console.error('Error loading sessions:', error);
      return [];
    }
  }

  deduplicateSessions(sessions) {
    const seen = new Map();
    const unique = [];
    
    for (const session of sessions) {
      const key = session.id;
      if (!seen.has(key)) {
        seen.set(key, true);
        unique.push(session);
      }
    }
    
    return unique;
  }

  async getSession(sessionId) {
    try {
      const sessions = await this.loadSessions();
      return sessions.find(s => s.id === sessionId);
    } catch (error) {
      console.error('Error getting session:', error);
      return null;
    }
  }

  // Conversation management
  async saveConversation(conversation) {
    try {
      const conversations = await this.loadConversations();
      const existingIndex = conversations.findIndex(c => c.id === conversation.id);
      
      if (existingIndex >= 0) {
        conversations[existingIndex] = conversation;
      } else {
        conversations.push(conversation);
      }
      
      await this.saveToFile(this.conversationsFile, conversations);
      return { success: true };
    } catch (error) {
      console.error('Error saving conversation:', error);
      return { success: false, error: error.message };
    }
  }

  async loadConversations() {
    try {
      if (!fs.existsSync(this.conversationsFile)) {
        return [];
      }
      
      const data = fs.readFileSync(this.conversationsFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.error('Error loading conversations:', error);
      return [];
    }
  }

  async getAllConversations() {
    return await this.loadConversations();
  }

  async getConversationsForSession(sessionId) {
    try {
      const conversations = await this.loadConversations();
      return conversations.filter(conv => conv.sessionId === sessionId);
    } catch (error) {
      console.error('Error getting conversations for session:', error);
      return [];
    }
  }

  // File changes management
  async saveFileChange(fileChange) {
    try {
      const fileChanges = await this.loadFileChanges();
      const existingIndex = fileChanges.findIndex(fc => fc.id === fileChange.id);
      
      if (existingIndex >= 0) {
        fileChanges[existingIndex] = fileChange;
      } else {
        fileChanges.push(fileChange);
      }
      
      await this.saveToFile(this.fileChangesFile, fileChanges);
      return { success: true };
    } catch (error) {
      console.error('Error saving file change:', error);
      return { success: false, error: error.message };
    }
  }

  async loadFileChanges() {
    try {
      if (!fs.existsSync(this.fileChangesFile)) {
        return [];
      }
      
      const data = fs.readFileSync(this.fileChangesFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.error('Error loading file changes:', error);
      return [];
    }
  }

  async getFileChangesForSession(sessionId) {
    try {
      const fileChanges = await this.loadFileChanges();
      return fileChanges.filter(fc => fc.sessionId === sessionId);
    } catch (error) {
      console.error('Error getting file changes for session:', error);
      return [];
    }
  }

  // Annotations management
  async saveAnnotation(annotation) {
    try {
      const annotations = await this.loadAnnotations();
      const existingIndex = annotations.findIndex(a => a.id === annotation.id);
      
      if (existingIndex >= 0) {
        annotations[existingIndex] = annotation;
      } else {
        annotations.push(annotation);
      }
      
      await this.saveToFile(this.annotationsFile, annotations);
      return { success: true };
    } catch (error) {
      console.error('Error saving annotation:', error);
      return { success: false, error: error.message };
    }
  }

  async loadAnnotations() {
    try {
      if (!fs.existsSync(this.annotationsFile)) {
        return [];
      }
      
      const data = fs.readFileSync(this.annotationsFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.error('Error loading annotations:', error);
      return [];
    }
  }

  async getAnnotationsForSession(sessionId) {
    try {
      const annotations = await this.loadAnnotations();
      return annotations.filter(a => a.sessionId === sessionId);
    } catch (error) {
      console.error('Error getting annotations for session:', error);
      return [];
    }
  }

  // Search functionality
  async searchSessions(query) {
    try {
      const sessions = await this.loadSessions();
      const conversations = await this.loadConversations();
      const fileChanges = await this.loadFileChanges();
      
      const results = sessions.filter(session => {
        const sessionText = JSON.stringify(session).toLowerCase();
        const sessionConversations = conversations.filter(c => c.sessionId === session.id);
        const sessionFileChanges = fileChanges.filter(fc => fc.sessionId === session.id);
        
        const conversationText = sessionConversations.map(c => c.content).join(' ').toLowerCase();
        const fileChangeText = sessionFileChanges.map(fc => fc.filePath).join(' ').toLowerCase();
        
        const allText = sessionText + ' ' + conversationText + ' ' + fileChangeText;
        
        return allText.includes(query.toLowerCase());
      });
      
      return results;
    } catch (error) {
      console.error('Error searching sessions:', error);
      return [];
    }
  }

  // Export functionality
  async exportSession(sessionId, format) {
    try {
      const session = await this.getSession(sessionId);
      if (!session) {
        throw new Error('Session not found');
      }
      
      const conversations = await this.getConversationsForSession(sessionId);
      const fileChanges = await this.getFileChangesForSession(sessionId);
      const annotations = await this.getAnnotationsForSession(sessionId);
      
      const exportData = {
        session,
        conversations,
        fileChanges,
        annotations,
        exportedAt: new Date().toISOString()
      };
      
      let content;
      let filename;
      
      switch (format) {
        case 'json':
          content = JSON.stringify(exportData, null, 2);
          filename = `session-${sessionId}.json`;
          break;
        case 'markdown':
          content = this.generateMarkdownExport(exportData);
          filename = `session-${sessionId}.md`;
          break;
        case 'csv':
          content = this.generateCSVExport(exportData);
          filename = `session-${sessionId}.csv`;
          break;
        default:
          throw new Error('Unsupported export format');
      }
      
      const exportPath = path.join(this.dataDir, 'exports', filename);
      const exportDir = path.dirname(exportPath);
      
      if (!fs.existsSync(exportDir)) {
        fs.mkdirSync(exportDir, { recursive: true });
      }
      
      fs.writeFileSync(exportPath, content);
      
      return { success: true, path: exportPath };
    } catch (error) {
      console.error('Error exporting session:', error);
      return { success: false, error: error.message };
    }
  }

  generateMarkdownExport(data) {
    const { session, conversations, fileChanges, annotations } = data;
    
    let markdown = `# Session: ${session.id}\n\n`;
    markdown += `**Intent:** ${session.intent}\n`;
    markdown += `**Outcome:** ${session.outcome}\n`;
    markdown += `**Timestamp:** ${session.timestamp}\n`;
    markdown += `**File:** ${session.currentFile}\n\n`;
    
    if (conversations.length > 0) {
      markdown += `## Conversation\n\n`;
      conversations.forEach(conv => {
        markdown += `### ${conv.role}\n\n`;
        markdown += `${conv.content}\n\n`;
      });
    }
    
    if (fileChanges.length > 0) {
      markdown += `## File Changes\n\n`;
      fileChanges.forEach(change => {
        markdown += `- **${change.filePath}** (${change.changeType})\n`;
      });
    }
    
    if (annotations.length > 0) {
      markdown += `## Annotations\n\n`;
      annotations.forEach(annotation => {
        markdown += `- ${annotation.content}\n`;
      });
    }
    
    return markdown;
  }

  generateCSVExport(data) {
    const { session, conversations } = data;
    
    let csv = 'Type,Timestamp,Content\n';
    csv += `Session,${session.timestamp},"${session.intent} - ${session.outcome}"\n`;
    
    conversations.forEach(conv => {
      csv += `Conversation,${conv.timestamp},"${conv.role}: ${conv.content.replace(/"/g, '""')}"\n`;
    });
    
    return csv;
  }

  // Utility methods
  async saveToFile(filePath, data) {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  }

  async getStats() {
    try {
      const sessions = await this.loadSessions();
      const conversations = await this.loadConversations();
      const fileChanges = await this.loadFileChanges();
      
      // Calculate comprehensive stats
      const totalSessions = sessions.length;
      const totalChanges = sessions.reduce((sum, s) => 
        sum + (s.codeDeltas?.length || 0) + (s.fileChanges?.length || 0), 0);
      
      // Execution metrics
      const totalExecutionCells = sessions.reduce((sum, s) => 
        sum + (s.executionStates?.executed || 0), 0);
      const avgExecutionCells = totalSessions > 0 ? 
        Math.round(totalExecutionCells / totalSessions) : 0;
      
      // Conversation metrics
      const totalPrompts = conversations.length;
      const avgPromptLength = conversations.length > 0 ? 
        Math.round(conversations.reduce((sum, c) => 
          sum + (c.content?.length || 0), 0) / conversations.length) : 0;
      
      // Session duration metrics
      const completedSessions = sessions.filter(s => s.endTime && s.timestamp);
      const avgSessionDuration = completedSessions.length > 0 ?
        Math.round(completedSessions.reduce((sum, s) => {
          const start = new Date(s.timestamp);
          const end = new Date(s.endTime);
          return sum + (end - start);
        }, 0) / completedSessions.length / 1000 / 60) : 0; // in minutes
      
      // File type distribution
      const notebookSessions = sessions.filter(s => 
        s.currentFile?.endsWith('.ipynb')).length;
      const pythonSessions = sessions.filter(s => 
        s.currentFile?.endsWith('.py')).length;
      
      return {
        totalSessions,
        totalChanges,
        avgExecutionCells,
        avgPromptLength,
        avgSessionDuration,
        notebookSessions,
        pythonSessions,
        totalConversations: conversations.length,
        lastUpdate: new Date().toISOString()
      };
    } catch (error) {
      console.error('Error getting stats:', error);
      return {
        totalSessions: 0,
        totalChanges: 0,
        avgExecutionCells: 0,
        avgPromptLength: 0,
        avgSessionDuration: 0,
        notebookSessions: 0,
        pythonSessions: 0,
        totalConversations: 0,
        lastUpdate: new Date().toISOString()
      };
    }
  }

  // Data cleanup and maintenance
  async cleanupOldData(daysToKeep = 30) {
    try {
      const cutoffDate = new Date(Date.now() - daysToKeep * 24 * 60 * 60 * 1000);
      
      // Clean up old sessions
      const sessions = await this.loadSessions();
      const recentSessions = sessions.filter(session => 
        new Date(session.timestamp) > cutoffDate
      );
      
      if (recentSessions.length !== sessions.length) {
        await this.saveToFile(this.sessionsFile, recentSessions);
        console.log(`🧹 Cleaned up ${sessions.length - recentSessions.length} old sessions`);
      }
      
      // Clean up old individual session files
      const sessionsDir = path.join(this.dataDir, 'sessions');
      if (fs.existsSync(sessionsDir)) {
        const files = fs.readdirSync(sessionsDir);
        let cleanedFiles = 0;
        
        for (const file of files) {
          if (file.endsWith('.json')) {
            try {
              const sessionData = fs.readFileSync(path.join(sessionsDir, file), 'utf8');
              const session = JSON.parse(sessionData);
              
              if (new Date(session.timestamp) <= cutoffDate) {
                fs.unlinkSync(path.join(sessionsDir, file));
                cleanedFiles++;
              }
            } catch (error) {
              console.error(`Error processing session file ${file}:`, error);
            }
          }
        }
        
        if (cleanedFiles > 0) {
          console.log(`🧹 Cleaned up ${cleanedFiles} old session files`);
        }
      }
      
      return { success: true, cleanedSessions: sessions.length - recentSessions.length };
    } catch (error) {
      console.error('Error cleaning up old data:', error);
      return { success: false, error: error.message };
    }
  }

  async migrateData() {
    try {
      // Check if we need to migrate from old format
      const sessions = await this.loadSessions();
      let migrated = 0;
      
      for (const session of sessions) {
        // Ensure session has required fields
        if (!session.id || !session.timestamp) {
          continue;
        }
        
        // Add missing fields with defaults
        const needsMigration = !session.phase || !session.outcome || !session.confidence;
        
        if (needsMigration) {
          session.phase = session.phase || 'IN_PROGRESS';
          session.outcome = session.outcome || 'IN_PROGRESS';
          session.confidence = session.confidence || 0.8;
          session.fileChanges = session.fileChanges || [];
          session.codeDeltas = session.codeDeltas || [];
          session.conversations = session.conversations || [];
          
          await this.saveIndividualSession(session);
          migrated++;
        }
      }
      
      if (migrated > 0) {
        console.log(`Migrated ${migrated} sessions to new format`);
      }
      
      return { success: true, migrated };
    } catch (error) {
      console.error('Error migrating data:', error);
      return { success: false, error: error.message };
    }
  }

  async getDataSize() {
    try {
      const stats = await this.getStats();
      const sessionsDir = path.join(this.dataDir, 'sessions');
      
      let totalSize = 0;
      
      // Calculate main files size
      const mainFiles = [this.sessionsFile, this.conversationsFile, this.fileChangesFile, this.annotationsFile];
      for (const file of mainFiles) {
        if (fs.existsSync(file)) {
          const stat = fs.statSync(file);
          totalSize += stat.size;
        }
      }
      
      // Calculate individual session files size
      if (fs.existsSync(sessionsDir)) {
        const files = fs.readdirSync(sessionsDir);
        for (const file of files) {
          if (file.endsWith('.json')) {
            const stat = fs.statSync(path.join(sessionsDir, file));
            totalSize += stat.size;
          }
        }
      }
      
      return {
        totalSize,
        totalSizeMB: (totalSize / (1024 * 1024)).toFixed(2),
        stats
      };
    } catch (error) {
      console.error('Error calculating data size:', error);
      return { totalSize: 0, totalSizeMB: '0.00', stats: {} };
    }
  }
}

module.exports = DataStorage;
