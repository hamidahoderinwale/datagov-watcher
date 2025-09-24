import chokidar from 'chokidar';
import path from 'path';
import fs from 'fs';
import { EventEmitter } from 'events';
import { CONFIG } from '../config/constants';
import { FileChangeEvent } from '../config/types';
import { nanoid } from 'nanoid';

/**
 * File system monitoring service using FSEvents
 * Monitors workspace changes and correlates them with Cursor sessions
 */
export class FileMonitor extends EventEmitter {
  private watcher: chokidar.FSWatcher | null = null;
  private workspacePath: string;
  private isMonitoring: boolean = false;
  private changeBuffer: Map<string, FileChangeEvent> = new Map();
  private bufferTimeout: NodeJS.Timeout | null = null;

  constructor(workspacePath: string) {
    super();
    this.workspacePath = workspacePath;
  }

  async startMonitoring(): Promise<void> {
    if (this.isMonitoring) return;

    try {
      this.watcher = chokidar.watch(this.workspacePath, {
        ignored: [
          /(^|[\/\\])\../, // dotfiles
          /node_modules/,
          /\.git/,
          /\.DS_Store/,
          /\.cursor/,
          /\.vscode/,
          /dist/,
          /build/,
          /coverage/,
          /\.parquet$/,
          /\.db$/
        ],
        persistent: true,
        ignoreInitial: true,
        followSymlinks: false,
        depth: 10
      });

      this.watcher
        .on('add', (filePath) => this.handleFileChange('created', filePath))
        .on('change', (filePath) => this.handleFileChange('modified', filePath))
        .on('unlink', (filePath) => this.handleFileChange('deleted', filePath))
        .on('error', (error) => this.emit('error', error));

      this.isMonitoring = true;
      this.emit('started');
    } catch (error) {
      this.emit('error', error);
      throw error;
    }
  }

  async stopMonitoring(): Promise<void> {
    if (!this.isMonitoring || !this.watcher) return;

    try {
      await this.watcher.close();
      this.watcher = null;
      this.isMonitoring = false;
      
      if (this.bufferTimeout) {
        clearTimeout(this.bufferTimeout);
        this.bufferTimeout = null;
      }
      
      this.emit('stopped');
    } catch (error) {
      this.emit('error', error);
    }
  }

  private handleFileChange(changeType: 'created' | 'modified' | 'deleted', filePath: string): void {
    // Skip non-code files
    if (!this.isCodeFile(filePath)) return;

    const relativePath = path.relative(this.workspacePath, filePath);
    const timestamp = new Date();

    // Buffer changes to avoid spam
    const changeId = `${filePath}-${changeType}`;
    const existingChange = this.changeBuffer.get(changeId);

    if (existingChange) {
      // Update existing change
      existingChange.timestamp = timestamp;
    } else {
      // Create new change
      const change: FileChangeEvent = {
        id: nanoid(),
        sessionId: 'current', // Will be updated when session is created
        timestamp,
        filePath: relativePath,
        changeType,
        lineRange: {
          start: 0,
          end: 0
        }
      };

      this.changeBuffer.set(changeId, change);
    }

    // Debounce changes
    if (this.bufferTimeout) {
      clearTimeout(this.bufferTimeout);
    }

    this.bufferTimeout = setTimeout(() => {
      this.flushChangeBuffer();
    }, 1000); // 1 second debounce
  }

  private flushChangeBuffer(): void {
    if (this.changeBuffer.size === 0) return;

    const changes = Array.from(this.changeBuffer.values());
    this.changeBuffer.clear();

    // Enhance changes with file content
    this.enhanceChangesWithContent(changes).then(enhancedChanges => {
      this.emit('fileChanges', enhancedChanges);
    });
  }

  private async enhanceChangesWithContent(changes: FileChangeEvent[]): Promise<FileChangeEvent[]> {
    const enhancedChanges: FileChangeEvent[] = [];

    for (const change of changes) {
      try {
        const fullPath = path.join(this.workspacePath, change.filePath);
        
        if (change.changeType === 'deleted' || !fs.existsSync(fullPath)) {
          enhancedChanges.push(change);
          continue;
        }

        const stats = fs.statSync(fullPath);
        if (!stats.isFile()) continue;

        // Read file content for code files
        if (this.isCodeFile(change.filePath)) {
          const content = fs.readFileSync(fullPath, 'utf8');
          const lines = content.split('\n');
          
          change.afterSnippet = content;
          change.lineRange = {
            start: 0,
            end: lines.length - 1
          };

          // Try to get git hash if available
          change.gitHash = await this.getGitHash(fullPath);
        }

        enhancedChanges.push(change);
      } catch (error) {
        console.error(`Error enhancing change for ${change.filePath}:`, error);
        enhancedChanges.push(change);
      }
    }

    return enhancedChanges;
  }

  private isCodeFile(filePath: string): boolean {
    const codeExtensions = [
      '.py', '.js', '.ts', '.jsx', '.tsx', '.ipynb', '.md', '.json', '.yaml', '.yml',
      '.html', '.css', '.scss', '.sass', '.less', '.vue', '.svelte', '.go', '.rs',
      '.java', '.cpp', '.c', '.h', '.hpp', '.cs', '.php', '.rb', '.swift', '.kt',
      '.scala', '.clj', '.hs', '.ml', '.fs', '.dart', '.r', '.m', '.mm'
    ];

    const ext = path.extname(filePath).toLowerCase();
    return codeExtensions.includes(ext);
  }

  private async getGitHash(filePath: string): Promise<string | undefined> {
    try {
      const { exec } = require('child_process');
      const { promisify } = require('util');
      const execAsync = promisify(exec);

      const { stdout } = await execAsync(`git log -1 --format="%H" -- "${filePath}"`, {
        cwd: path.dirname(filePath)
      });

      return stdout.trim() || undefined;
    } catch (error) {
      return undefined;
    }
  }

  async getRecentChanges(since: Date): Promise<FileChangeEvent[]> {
    const changes: FileChangeEvent[] = [];
    const cutoffTime = since.getTime();

    // This would typically query a persistent storage of changes
    // For now, we'll return empty array as changes are handled in real-time
    return changes;
  }

  isActive(): boolean {
    return this.isMonitoring;
  }

  getWorkspacePath(): string {
    return this.workspacePath;
  }
}
