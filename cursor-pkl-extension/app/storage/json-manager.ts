import fs from 'fs';
import path from 'path';
import { CONFIG } from '../config/constants';
import { PKLSession, ConversationEvent, FileChangeEvent, CodeDelta, LinkedEvent } from '../config/types';

/**
 * JSON-based storage manager for bulk session data
 * Handles efficient storage and retrieval of large datasets
 */
export class JsonManager {
  private dataPath: string;

  constructor(dataPath: string = CONFIG.PKL_DATA_PATH) {
    this.dataPath = dataPath;
    this.initializeDirectories();
  }

  private initializeDirectories(): void {
    // Ensure data directory exists
    if (!fs.existsSync(this.dataPath)) {
      fs.mkdirSync(this.dataPath, { recursive: true });
    }
  }

  async saveSession(session: PKLSession): Promise<void> {
    const sessionFile = path.join(this.dataPath, `session-${session.id}.json`);
    fs.writeFileSync(sessionFile, JSON.stringify(session, null, 2));
  }

  async saveEvents(events: ConversationEvent[]): Promise<void> {
    const eventsFile = path.join(this.dataPath, 'events.json');
    let allEvents: ConversationEvent[] = [];
    
    if (fs.existsSync(eventsFile)) {
      allEvents = JSON.parse(fs.readFileSync(eventsFile, 'utf8'));
    }
    
    allEvents.push(...events);
    fs.writeFileSync(eventsFile, JSON.stringify(allEvents, null, 2));
  }

  async saveFileChanges(changes: FileChangeEvent[]): Promise<void> {
    const changesFile = path.join(this.dataPath, 'file-changes.json');
    let allChanges: FileChangeEvent[] = [];
    
    if (fs.existsSync(changesFile)) {
      allChanges = JSON.parse(fs.readFileSync(changesFile, 'utf8'));
    }
    
    allChanges.push(...changes);
    fs.writeFileSync(changesFile, JSON.stringify(allChanges, null, 2));
  }

  async saveCodeDeltas(deltas: CodeDelta[]): Promise<void> {
    const deltasFile = path.join(this.dataPath, 'code-deltas.json');
    let allDeltas: CodeDelta[] = [];
    
    if (fs.existsSync(deltasFile)) {
      allDeltas = JSON.parse(fs.readFileSync(deltasFile, 'utf8'));
    }
    
    allDeltas.push(...deltas);
    fs.writeFileSync(deltasFile, JSON.stringify(allDeltas, null, 2));
  }

  async saveLinkedEvents(events: LinkedEvent[]): Promise<void> {
    const eventsFile = path.join(this.dataPath, 'linked-events.json');
    let allEvents: LinkedEvent[] = [];
    
    if (fs.existsSync(eventsFile)) {
      allEvents = JSON.parse(fs.readFileSync(eventsFile, 'utf8'));
    }
    
    allEvents.push(...events);
    fs.writeFileSync(eventsFile, JSON.stringify(allEvents, null, 2));
  }

  async getSessionsByDateRange(startDate: Date, endDate: Date): Promise<PKLSession[]> {
    const sessions: PKLSession[] = [];
    const files = fs.readdirSync(this.dataPath).filter(f => f.startsWith('session-') && f.endsWith('.json'));
    
    for (const file of files) {
      try {
        const sessionData = JSON.parse(fs.readFileSync(path.join(this.dataPath, file), 'utf8'));
        const sessionDate = new Date(sessionData.timestamp);
        
        if (sessionDate >= startDate && sessionDate <= endDate) {
          sessions.push(sessionData);
        }
      } catch (error) {
        console.error(`Error reading session file ${file}:`, error);
      }
    }
    
    return sessions.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
  }

  async getEventsBySessionId(sessionId: string): Promise<ConversationEvent[]> {
    const eventsFile = path.join(this.dataPath, 'events.json');
    
    if (!fs.existsSync(eventsFile)) {
      return [];
    }
    
    const allEvents: ConversationEvent[] = JSON.parse(fs.readFileSync(eventsFile, 'utf8'));
    return allEvents.filter(event => event.sessionId === sessionId);
  }

  async getFileChangesBySessionId(sessionId: string): Promise<FileChangeEvent[]> {
    const changesFile = path.join(this.dataPath, 'file-changes.json');
    
    if (!fs.existsSync(changesFile)) {
      return [];
    }
    
    const allChanges: FileChangeEvent[] = JSON.parse(fs.readFileSync(changesFile, 'utf8'));
    return allChanges.filter(change => change.sessionId === sessionId);
  }

  async getCodeDeltasBySessionId(sessionId: string): Promise<CodeDelta[]> {
    const deltasFile = path.join(this.dataPath, 'code-deltas.json');
    
    if (!fs.existsSync(deltasFile)) {
      return [];
    }
    
    const allDeltas: CodeDelta[] = JSON.parse(fs.readFileSync(deltasFile, 'utf8'));
    return allDeltas.filter(delta => delta.sessionId === sessionId);
  }

  async getLinkedEventsBySessionId(sessionId: string): Promise<LinkedEvent[]> {
    const eventsFile = path.join(this.dataPath, 'linked-events.json');
    
    if (!fs.existsSync(eventsFile)) {
      return [];
    }
    
    const allEvents: LinkedEvent[] = JSON.parse(fs.readFileSync(eventsFile, 'utf8'));
    return allEvents.filter(event => event.sessionId === sessionId);
  }

  async exportToJson(tableName: string, outputPath: string): Promise<void> {
    const sourceFile = path.join(this.dataPath, `${tableName}.json`);
    const targetFile = path.join(outputPath, `${tableName}.json`);
    
    if (fs.existsSync(sourceFile)) {
      fs.copyFileSync(sourceFile, targetFile);
    }
  }

  async importFromJson(tableName: string, inputPath: string): Promise<void> {
    const sourceFile = path.join(inputPath, `${tableName}.json`);
    const targetFile = path.join(this.dataPath, `${tableName}.json`);
    
    if (fs.existsSync(sourceFile)) {
      fs.copyFileSync(sourceFile, targetFile);
    }
  }

  close(): void {
    // No cleanup needed for JSON files
  }
}
