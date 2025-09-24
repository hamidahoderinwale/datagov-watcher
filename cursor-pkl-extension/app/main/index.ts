import { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage, dialog } from 'electron';
import path from 'path';
import { CONFIG } from '../config/constants';
// import { SimpleSQLiteManager } from '../storage/simple-sqlite-manager';
import { JsonManager } from '../storage/json-manager';
import { CursorDBParser } from '../services/cursor-db-parser';
import { FileMonitor } from '../services/file-monitor';
import { AppleScriptService } from '../services/applescript-service';
import { PKLSession, SearchFilters, ExportOptions } from '../config/types';

/**
 * Main Electron process for PKL Extension
 * Handles system tray, IPC communication, and data management
 */
class PKLExtension {
  private mainWindow: BrowserWindow | null = null;
  private tray: Tray | null = null;
  private sqliteManager: SimpleSQLiteManager;
  private jsonManager: JsonManager;
  private cursorParser: CursorDBParser;
  private fileMonitor: FileMonitor | null = null;
  private isQuitting = false;
  private updateInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.sqliteManager = new SimpleSQLiteManager();
    this.jsonManager = new JsonManager();
    this.cursorParser = new CursorDBParser();
  }

  async initialize(): Promise<void> {
    await this.setupDatabase();
    await this.setupFileMonitoring();
    this.setupIPC();
    this.createTray();
    this.createWindow();
    this.startPeriodicUpdates();
  }

  private async setupDatabase(): Promise<void> {
    try {
      await this.cursorParser.connect();
      console.log('Connected to Cursor database');
    } catch (error) {
      console.error('Failed to connect to Cursor database:', error);
    }
  }

  private async setupFileMonitoring(): Promise<void> {
    try {
      const workspacePath = process.cwd();
      this.fileMonitor = new FileMonitor(workspacePath);
      
      this.fileMonitor.on('fileChanges', (changes) => {
        this.handleFileChanges(changes);
      });

      this.fileMonitor.on('error', (error) => {
        console.error('File monitor error:', error);
      });

      await this.fileMonitor.startMonitoring();
      console.log('File monitoring started');
    } catch (error) {
      console.error('Failed to start file monitoring:', error);
    }
  }

  private setupIPC(): void {
    // Get sessions - try server first, fallback to local storage
    ipcMain.handle(CONFIG.IPC_CHANNELS.GET_SESSIONS, async () => {
      try {
        // Try to get sessions from the web server first
        try {
          const response = await fetch('http://localhost:3000/api/sessions');
          if (response.ok) {
            const data = await response.json();
            return { success: true, data: data.sessions || [] };
          }
        } catch (serverError) {
          console.log('Server not available, trying standalone monitor...');
          try {
            const response = await fetch('http://localhost:3001/api/sessions');
            if (response.ok) {
              const data = await response.json();
              return { success: true, data: data.sessions || [] };
            }
          } catch (monitorError) {
            console.log('Monitor not available, using local storage...');
          }
        }

        // Fallback to local storage
        const sessions = await this.sqliteManager.getSessions();
        return { success: true, data: sessions };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Get session detail
    ipcMain.handle(CONFIG.IPC_CHANNELS.GET_SESSION_DETAIL, async (_, sessionId: string) => {
      try {
        const session = await this.sqliteManager.getSessionById(sessionId);
        return { success: true, data: session };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Search sessions
    ipcMain.handle(CONFIG.IPC_CHANNELS.SEARCH_SESSIONS, async (_, query: string) => {
      try {
        const sessions = await this.sqliteManager.searchSessions(query);
        return { success: true, data: sessions };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Return to context
    ipcMain.handle(CONFIG.IPC_CHANNELS.RETURN_TO_CONTEXT, async (_, sessionId: string) => {
      try {
        const session = await this.sqliteManager.getSessionById(sessionId);
        if (!session) {
          return { success: false, error: 'Session not found' };
        }

        const success = await AppleScriptService.restoreSessionContext({
          currentFile: session.currentFile,
          cursorPosition: session.cursorPosition,
          selectedText: session.selectedText
        });

        return { success, error: success ? null : 'Failed to restore context' };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Resume session
    ipcMain.handle(CONFIG.IPC_CHANNELS.RESUME_SESSION, async (_, sessionId: string) => {
      try {
        const session = await this.sqliteManager.getSessionById(sessionId);
        if (!session) {
          return { success: false, error: 'Session not found' };
        }

        // This would restore the full session state
        const success = await AppleScriptService.restoreSessionContext(session);
        return { success, error: success ? null : 'Failed to resume session' };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Add annotation
    ipcMain.handle(CONFIG.IPC_CHANNELS.ADD_ANNOTATION, async (_, sessionId: string, content: string) => {
      try {
        const annotation = {
          id: require('nanoid').nanoid(),
          sessionId,
          timestamp: new Date(),
          content,
          tags: []
        };

        await this.sqliteManager.saveAnnotation(annotation);
        return { success: true, data: annotation };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Export session
    ipcMain.handle(CONFIG.IPC_CHANNELS.EXPORT_SESSION, async (_, sessionId: string, options: ExportOptions) => {
      try {
        const session = await this.sqliteManager.getSessionById(sessionId);
        if (!session) {
          return { success: false, error: 'Session not found' };
        }

        const exportPath = await this.exportSession(session, options);
        return { success: true, data: { path: exportPath } };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Export all sessions
    ipcMain.handle(CONFIG.IPC_CHANNELS.EXPORT_ALL, async (_, options: ExportOptions) => {
      try {
        const sessions = await this.sqliteManager.getSessions();
        const exportPath = await this.exportAllSessions(sessions, options);
        return { success: true, data: { path: exportPath } };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Get export list
    ipcMain.handle(CONFIG.IPC_CHANNELS.GET_EXPORTS, async () => {
      try {
        const response = await fetch('http://localhost:3000/api/export/list');
        if (response.ok) {
          const data = await response.json();
          return { success: true, data: data.exports || [] };
        }
        return { success: true, data: [] };
      } catch (error) {
        return { success: true, data: [] };
      }
    });

    // Delete export
    ipcMain.handle(CONFIG.IPC_CHANNELS.DELETE_EXPORT, async (_, filename: string) => {
      try {
        const response = await fetch(`http://localhost:3000/api/export/${filename}`, {
          method: 'DELETE'
        });
        return { success: response.ok };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });

    // Get stats
    ipcMain.handle(CONFIG.IPC_CHANNELS.GET_STATS, async () => {
      try {
        const response = await fetch('http://localhost:3000/api/stats');
        if (response.ok) {
          const data = await response.json();
          return { success: true, data: data.stats };
        }
        return { success: true, data: {} };
      } catch (error) {
        return { success: true, data: {} };
      }
    });

    // Refresh sessions (force update)
    ipcMain.handle(CONFIG.IPC_CHANNELS.REFRESH_SESSIONS, async () => {
      try {
        await this.syncData();
        const sessions = await this.sqliteManager.getSessions();
        return { success: true, data: sessions };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : String(error) };
      }
    });
  }

  private createTray(): void {
    const iconPath = path.join(__dirname, '../../assets/tray-icon.png');
    const icon = nativeImage.createFromPath(iconPath);
    
    this.tray = new Tray(icon);
    this.tray.setToolTip('Cursor PKL Extension');
    
    const contextMenu = Menu.buildFromTemplate([
      {
        label: 'Show Widget',
        click: () => this.toggleWindow()
      },
      {
        label: 'Sync Data',
        click: () => this.syncData()
      },
      { type: 'separator' },
      {
        label: 'Quit',
        click: () => this.quit()
      }
    ]);

    this.tray.setContextMenu(contextMenu);
    this.tray.on('click', () => this.toggleWindow());
  }

  private createWindow(): void {
    this.mainWindow = new BrowserWindow({
      width: CONFIG.WIDGET_EXPANDED_WIDTH,
      height: CONFIG.WIDGET_EXPANDED_HEIGHT,
      minWidth: CONFIG.WIDGET_COLLAPSED_WIDTH,
      minHeight: CONFIG.WIDGET_COLLAPSED_HEIGHT,
      maxHeight: CONFIG.WIDGET_MAX_HEIGHT,
      show: false,
      frame: false,
      resizable: true,
      alwaysOnTop: true,
      skipTaskbar: true,
      webPreferences: {
        nodeIntegration: false,
        contextIsolation: true,
        preload: path.join(__dirname, 'preload.js')
      }
    });

    if (process.env.NODE_ENV === 'development') {
      // In development, load the React app from the dev server
      this.mainWindow.loadURL('http://localhost:3000');
      this.mainWindow.webContents.openDevTools();
    } else {
      // In production, load the built React app
      this.mainWindow.loadFile(path.join(__dirname, '../renderer/index.html'));
    }

    this.mainWindow.on('close', (event) => {
      if (!this.isQuitting) {
        event.preventDefault();
        this.mainWindow?.hide();
      }
    });

    this.mainWindow.on('closed', () => {
      this.mainWindow = null;
    });
  }

  private toggleWindow(): void {
    if (!this.mainWindow) return;

    if (this.mainWindow.isVisible()) {
      this.mainWindow.hide();
    } else {
      this.mainWindow.show();
      this.mainWindow.focus();
    }
  }

  private async syncData(): Promise<void> {
    try {
      const sessions = await this.cursorParser.parseAllSessions();
      
      for (const session of sessions) {
        await this.sqliteManager.saveSession(session);
        await this.jsonManager.saveSession(session);
      }

      console.log(`Synced ${sessions.length} sessions`);
    } catch (error) {
      console.error('Failed to sync data:', error);
    }
  }

  private async handleFileChanges(changes: any[]): Promise<void> {
    // Process file changes and update current session
    console.log(`Processing ${changes.length} file changes`);
    
    // Notify renderer about file changes
    if (this.mainWindow) {
      this.mainWindow.webContents.send(CONFIG.IPC_CHANNELS.FILE_CHANGE, changes);
    }
    
    // Refresh sessions and notify renderer
    try {
      await this.syncData();
      const sessions = await this.sqliteManager.getSessions();
      if (this.mainWindow) {
        this.mainWindow.webContents.send(CONFIG.IPC_CHANNELS.SESSION_UPDATE, sessions);
      }
    } catch (error) {
      console.error('Failed to sync data after file changes:', error);
    }
  }

  private startPeriodicUpdates(): void {
    // Update sessions every 5 seconds
    this.updateInterval = setInterval(async () => {
      try {
        await this.syncData();
        const sessions = await this.sqliteManager.getSessions();
        if (this.mainWindow && this.mainWindow.isVisible()) {
          this.mainWindow.webContents.send(CONFIG.IPC_CHANNELS.SESSION_UPDATE, sessions);
        }
      } catch (error) {
        console.error('Failed to update sessions:', error);
      }
    }, 5000);
  }

  private async exportSession(session: PKLSession, options: ExportOptions): Promise<string> {
    const { format } = options;
    const timestamp = session.timestamp.toISOString().split('T')[0];
    const filename = `session-${session.id}-${timestamp}.${format}`;
    const filePath = path.join(CONFIG.PKL_DATA_PATH, 'exports', filename);

    // Ensure exports directory exists
    const exportsDir = path.dirname(filePath);
    require('fs').mkdirSync(exportsDir, { recursive: true });

    let content: string;

    switch (format) {
      case 'json':
        content = JSON.stringify(session, null, 2);
        break;
      case 'markdown':
        content = this.generateMarkdownExport(session);
        break;
      case 'csv':
        content = this.generateCSVExport(session);
        break;
      default:
        throw new Error(`Unsupported export format: ${format}`);
    }

    require('fs').writeFileSync(filePath, content);
    return filePath;
  }

  private generateMarkdownExport(session: PKLSession): string {
    return `# Session ${session.id}

**Intent:** ${session.intent}
**Phase:** ${session.phase}
**Outcome:** ${session.outcome || 'N/A'}
**Timestamp:** ${session.timestamp.toISOString()}

## Context
- **File:** ${session.currentFile || 'N/A'}
- **Position:** Line ${session.cursorPosition?.line || 'N/A'}, Column ${session.cursorPosition?.character || 'N/A'}

## File Changes
${session.fileChanges.map(change => `- **${change.changeType}:** ${change.filePath}`).join('\n')}

## Annotations
${session.annotations.map(ann => `- ${ann.content}`).join('\n')}
`;
  }

  private generateCSVExport(session: PKLSession): string {
    const headers = ['id', 'timestamp', 'intent', 'phase', 'outcome', 'file', 'changes'];
    const row = [
      session.id,
      session.timestamp.toISOString(),
      session.intent,
      session.phase,
      session.outcome || '',
      session.currentFile || '',
      session.fileChanges.length.toString()
    ];
    
    return [headers.join(','), row.join(',')].join('\n');
  }

  private async exportAllSessions(sessions: PKLSession[], options: ExportOptions): Promise<string> {
    const { format } = options;
    const timestamp = new Date().toISOString().split('T')[0];
    const filename = `all-sessions-${timestamp}.${format}`;
    const filePath = path.join(CONFIG.PKL_DATA_PATH, 'exports', filename);

    // Ensure exports directory exists
    const exportsDir = path.dirname(filePath);
    require('fs').mkdirSync(exportsDir, { recursive: true });

    let content: string;

    switch (format) {
      case 'json':
        content = JSON.stringify(sessions, null, 2);
        break;
      case 'markdown':
        content = this.generateAllSessionsMarkdown(sessions);
        break;
      case 'csv':
        content = this.generateAllSessionsCSV(sessions);
        break;
      default:
        throw new Error(`Unsupported export format: ${format}`);
    }

    require('fs').writeFileSync(filePath, content);
    return filePath;
  }

  private generateAllSessionsMarkdown(sessions: PKLSession[]): string {
    let content = `# All Sessions Export\n\n`;
    content += `**Total Sessions:** ${sessions.length}\n`;
    content += `**Export Date:** ${new Date().toISOString()}\n\n`;

    sessions.forEach((session, index) => {
      content += `## Session ${index + 1}: ${session.id}\n\n`;
      content += `**Intent:** ${session.intent}\n`;
      content += `**Phase:** ${session.phase}\n`;
      content += `**Outcome:** ${session.outcome || 'N/A'}\n`;
      content += `**Timestamp:** ${session.timestamp.toISOString()}\n\n`;
      content += `### Context\n`;
      content += `- **File:** ${session.currentFile || 'N/A'}\n`;
      content += `- **Position:** Line ${session.cursorPosition?.line || 'N/A'}, Column ${session.cursorPosition?.character || 'N/A'}\n\n`;
      content += `### File Changes\n`;
      session.fileChanges.forEach(change => {
        content += `- **${change.changeType}:** ${change.filePath}\n`;
      });
      content += `\n---\n\n`;
    });

    return content;
  }

  private generateAllSessionsCSV(sessions: PKLSession[]): string {
    const headers = ['id', 'timestamp', 'intent', 'phase', 'outcome', 'file', 'changes'];
    const rows = sessions.map(session => [
      session.id,
      session.timestamp.toISOString(),
      session.intent,
      session.phase,
      session.outcome || '',
      session.currentFile || '',
      session.fileChanges.length.toString()
    ]);
    
    return [headers.join(','), ...rows.map(row => row.join(','))].join('\n');
  }

  private quit(): void {
    this.isQuitting = true;
    app.quit();
  }
}

// Initialize the application
const pklExtension = new PKLExtension();

app.whenReady().then(async () => {
  await pklExtension.initialize();
});

app.on('window-all-closed', () => {
  // Keep the app running even when all windows are closed (tray app)
});

app.on('activate', () => {
  // macOS specific: re-create window when dock icon is clicked
});

app.on('before-quit', async () => {
  // Cleanup
  if (pklExtension['fileMonitor']) {
    await pklExtension['fileMonitor'].stopMonitoring();
  }
  if (pklExtension['updateInterval']) {
    clearInterval(pklExtension['updateInterval']);
  }
  pklExtension['sqliteManager'].close();
  pklExtension['jsonManager'].close();
  await pklExtension['cursorParser'].close();
});
