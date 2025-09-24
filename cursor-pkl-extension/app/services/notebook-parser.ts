import fs from 'fs';
import path from 'path';
import { nanoid } from 'nanoid';
import { CONFIG } from '../config/constants';
import { PKLSession, ConversationEvent, CodeBlock, Position } from '../config/types';

/**
 * Jupyter notebook parser for Cursor
 * Handles .ipynb files and extracts execution patterns, outputs, and data science workflows
 */
export class NotebookParser {
  private notebookPath: string;

  constructor(notebookPath: string) {
    this.notebookPath = notebookPath;
  }

  /**
   * Parse a Jupyter notebook file and extract session data
   */
  async parseNotebook(): Promise<PKLSession | null> {
    try {
      if (!fs.existsSync(this.notebookPath)) {
        return null;
      }

      const notebookContent = fs.readFileSync(this.notebookPath, 'utf8');
      const notebook = JSON.parse(notebookContent);

      if (!notebook.cells || !Array.isArray(notebook.cells)) {
        return null;
      }

      const session = this.createNotebookSession(notebook);
      return session;
    } catch (error) {
      console.error('Error parsing notebook:', error);
      return null;
    }
  }

  private createNotebookSession(notebook: any): PKLSession {
    const sessionId = nanoid();
    const timestamp = this.extractNotebookTimestamp(notebook);
    
    // Analyze notebook for data science patterns
    const analysis = this.analyzeNotebookPatterns(notebook);
    
    // Extract execution history
    const executionHistory = this.extractExecutionHistory(notebook);
    
    // Create session
    const session: PKLSession = {
      id: sessionId,
      timestamp,
      intent: analysis.intent,
      phase: analysis.phase,
      outcome: analysis.outcome,
      confidence: analysis.confidence,
      currentFile: this.notebookPath,
      cursorPosition: this.extractCursorPosition(notebook),
      selectedText: this.extractSelectedText(notebook),
      fileChanges: [],
      codeDeltas: this.extractCodeDeltas(notebook),
      linkedEvents: this.extractNotebookEvents(notebook),
      privacyMode: false,
      userConsent: true,
      dataRetention: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
      annotations: []
    };

    return session;
  }

  private extractNotebookTimestamp(notebook: any): Date {
    // Try to get creation or modification time
    const stats = fs.statSync(this.notebookPath);
    return stats.mtime;
  }

  private analyzeNotebookPatterns(notebook: any): {
    intent: 'debug' | 'implement' | 'explore' | 'refactor' | 'document';
    phase: 'start' | 'middle' | 'success' | 'stuck';
    outcome?: 'success' | 'stuck' | 'in-progress';
    confidence: number;
  } {
    const allText = this.extractAllText(notebook).toLowerCase();
    
    // Intent classification for data science workflows
    let intent: 'debug' | 'implement' | 'explore' | 'refactor' | 'document' = 'explore';
    let maxScore = 0;

    // Enhanced keywords for data science
    const dataScienceKeywords = {
      explore: ['explore', 'analyze', 'investigate', 'examine', 'visualize', 'plot', 'chart', 'graph', 'eda', 'exploratory'],
      implement: ['implement', 'build', 'create', 'develop', 'train', 'model', 'algorithm', 'pipeline', 'function'],
      debug: ['debug', 'fix', 'error', 'traceback', 'exception', 'bug', 'issue', 'problem', 'troubleshoot'],
      refactor: ['refactor', 'optimize', 'improve', 'clean', 'restructure', 'reorganize', 'refactor'],
      document: ['document', 'explain', 'comment', 'describe', 'docstring', 'markdown', 'note']
    };

    for (const [intentType, keywords] of Object.entries(dataScienceKeywords)) {
      const score = keywords.reduce((acc, keyword) => 
        acc + (allText.includes(keyword.toLowerCase()) ? 1 : 0), 0
      );
      
      if (score > maxScore) {
        maxScore = score;
        intent = intentType as any;
      }
    }

    // Outcome detection based on execution results
    const hasSuccess = this.detectSuccessPatterns(notebook);
    const hasStuck = this.detectStuckPatterns(notebook);
    
    let outcome: 'success' | 'stuck' | 'in-progress' | undefined;
    if (hasSuccess && !hasStuck) {
      outcome = 'success';
    } else if (hasStuck && !hasSuccess) {
      outcome = 'stuck';
    } else if (hasSuccess && hasStuck) {
      outcome = 'in-progress';
    }

    // Phase detection based on cell execution patterns
    const executedCells = this.getExecutedCells(notebook);
    let phase: 'start' | 'middle' | 'success' | 'stuck' = 'start';
    
    if (executedCells.length > 3) {
      phase = 'middle';
    }
    if (outcome === 'success') {
      phase = 'success';
    } else if (outcome === 'stuck') {
      phase = 'stuck';
    }

    return {
      intent,
      phase,
      outcome,
      confidence: Math.min(maxScore / 5, 1) // Normalize to 0-1
    };
  }

  private extractAllText(notebook: any): string {
    let text = '';
    
    for (const cell of notebook.cells || []) {
      if (cell.cell_type === 'code' || cell.cell_type === 'markdown') {
        if (Array.isArray(cell.source)) {
          text += cell.source.join('') + ' ';
        } else if (typeof cell.source === 'string') {
          text += cell.source + ' ';
        }
      }
      
      // Include outputs
      if (cell.outputs) {
        for (const output of cell.outputs) {
          if (output.text) {
            if (Array.isArray(output.text)) {
              text += output.text.join('') + ' ';
            } else {
              text += output.text + ' ';
            }
          }
        }
      }
    }
    
    return text;
  }

  private detectSuccessPatterns(notebook: any): boolean {
    const successPatterns = [
      'success', 'completed', 'finished', 'done', 'no errors',
      'test passed', 'all tests passed', 'model trained', 'accuracy',
      'plot created', 'visualization complete', 'data loaded'
    ];

    for (const cell of notebook.cells || []) {
      if (cell.outputs) {
        for (const output of cell.outputs) {
          const outputText = this.extractOutputText(output).toLowerCase();
          if (successPatterns.some(pattern => outputText.includes(pattern))) {
            return true;
          }
        }
      }
    }
    
    return false;
  }

  private detectStuckPatterns(notebook: any): boolean {
    const stuckPatterns = [
      'error', 'exception', 'traceback', 'failed', 'failed to',
      'not found', 'undefined', 'keyerror', 'valueerror',
      'timeout', 'memory error', 'kernel died'
    ];

    for (const cell of notebook.cells || []) {
      if (cell.outputs) {
        for (const output of cell.outputs) {
          const outputText = this.extractOutputText(output).toLowerCase();
          if (stuckPatterns.some(pattern => outputText.includes(pattern))) {
            return true;
          }
        }
      }
    }
    
    return false;
  }

  private getExecutedCells(notebook: any): any[] {
    return (notebook.cells || []).filter((cell: any) => 
      cell.cell_type === 'code' && cell.outputs && cell.outputs.length > 0
    );
  }

  private extractOutputText(output: any): string {
    if (output.text) {
      if (Array.isArray(output.text)) {
        return output.text.join('');
      }
      return output.text;
    }
    if (output.data && output.data['text/plain']) {
      if (Array.isArray(output.data['text/plain'])) {
        return output.data['text/plain'].join('');
      }
      return output.data['text/plain'];
    }
    return '';
  }

  private extractCursorPosition(notebook: any): Position | undefined {
    // Jupyter notebooks don't have traditional cursor positions
    // Return the last executed cell position
    const executedCells = this.getExecutedCells(notebook);
    if (executedCells.length > 0) {
      const lastCell = executedCells[executedCells.length - 1];
      return {
        line: lastCell.execution_count || 0,
        character: 0
      };
    }
    return undefined;
  }

  private extractSelectedText(notebook: any): string | undefined {
    // Extract the last code cell content as "selected text"
    const codeCells = (notebook.cells || []).filter((cell: any) => cell.cell_type === 'code');
    if (codeCells.length > 0) {
      const lastCell = codeCells[codeCells.length - 1];
      if (Array.isArray(lastCell.source)) {
        return lastCell.source.join('').trim();
      } else if (typeof lastCell.source === 'string') {
        return lastCell.source.trim();
      }
    }
    return undefined;
  }

  private extractCodeDeltas(notebook: any): any[] {
    const deltas: any[] = [];
    const codeCells = (notebook.cells || []).filter((cell: any) => cell.cell_type === 'code');
    
    for (let i = 0; i < codeCells.length; i++) {
      const cell = codeCells[i];
      const cellContent = Array.isArray(cell.source) ? cell.source.join('') : cell.source;
      
      deltas.push({
        id: nanoid(),
        sessionId: 'notebook-session',
        timestamp: new Date(),
        filePath: this.notebookPath,
        beforeContent: '',
        afterContent: cellContent,
        diff: `+ ${cellContent}`,
        changeType: 'added' as const,
        lineCount: cellContent.split('\n').length
      });
    }
    
    return deltas;
  }

  private extractNotebookEvents(notebook: any): any[] {
    const events: any[] = [];
    
    for (const cell of notebook.cells || []) {
      if (cell.cell_type === 'code' && cell.outputs) {
        for (const output of cell.outputs) {
          const outputText = this.extractOutputText(output);
          
          // Detect different types of events
          if (output.output_type === 'execute_result') {
            events.push({
              id: nanoid(),
              sessionId: 'notebook-session',
              timestamp: new Date(),
              type: 'code_run',
              output: outputText,
              tag: 'cell_execution',
              classification: 'success'
            });
          } else if (output.output_type === 'error') {
            events.push({
              id: nanoid(),
              sessionId: 'notebook-session',
              timestamp: new Date(),
              type: 'error',
              output: outputText,
              tag: 'cell_error',
              classification: 'error'
            });
          }
        }
      }
    }
    
    return events;
  }

  /**
   * Extract conversation events from notebook markdown cells
   * These represent prompts and responses in the notebook context
   */
  extractConversationEvents(notebook: any): ConversationEvent[] {
    const events: ConversationEvent[] = [];
    
    for (const cell of notebook.cells || []) {
      if (cell.cell_type === 'markdown') {
        const content = Array.isArray(cell.source) ? cell.source.join('') : cell.source;
        
        // Look for prompt/response patterns in markdown
        if (content.includes('**Prompt:**') || content.includes('**Question:**')) {
          events.push({
            id: nanoid(),
            sessionId: 'notebook-session',
            timestamp: new Date(),
            role: 'user',
            content: content,
            metadata: { cellType: 'markdown', cellIndex: cell.execution_count },
            referencedFiles: this.extractReferencedFiles(content),
            codeBlocks: this.extractCodeBlocks(content)
          });
        } else if (content.includes('**Response:**') || content.includes('**Answer:**')) {
          events.push({
            id: nanoid(),
            sessionId: 'notebook-session',
            timestamp: new Date(),
            role: 'assistant',
            content: content,
            metadata: { cellType: 'markdown', cellIndex: cell.execution_count },
            referencedFiles: this.extractReferencedFiles(content),
            codeBlocks: this.extractCodeBlocks(content)
          });
        }
      }
    }
    
    return events;
  }

  private extractReferencedFiles(content: string): string[] {
    const filePatterns = [
      /\.py\b/g,
      /\.ipynb\b/g,
      /\.csv\b/g,
      /\.json\b/g,
      /\.parquet\b/g,
      /\.pkl\b/g,
      /\.h5\b/g,
      /\.hdf5\b/g
    ];
    
    const files: string[] = [];
    for (const pattern of filePatterns) {
      const matches = content.match(pattern);
      if (matches) {
        files.push(...matches);
      }
    }
    
    return [...new Set(files)]; // Remove duplicates
  }

  private extractCodeBlocks(content: string): CodeBlock[] {
    const codeBlocks: CodeBlock[] = [];
    const codeBlockRegex = /```(\w+)?\n([\s\S]*?)```/g;
    
    let match;
    while ((match = codeBlockRegex.exec(content)) !== null) {
      codeBlocks.push({
        language: match[1] || 'python',
        content: match[2].trim()
      });
    }
    
    return codeBlocks;
  }

  /**
   * Extract execution history from notebook cells
   */
  private extractExecutionHistory(notebook: any): any[] {
    const executionHistory: any[] = [];
    
    if (!notebook.cells) return executionHistory;
    
    for (const cell of notebook.cells) {
      if (cell.cell_type === 'code' && cell.execution_count !== null) {
        executionHistory.push({
          cellIndex: cell.execution_count,
          executionCount: cell.execution_count,
          outputs: cell.outputs || [],
          executionTime: new Date().toISOString(),
          success: this.determineCellSuccess(cell)
        });
      }
    }
    
    return executionHistory;
  }

  /**
   * Determine if a cell execution was successful
   */
  private determineCellSuccess(cell: any): boolean {
    if (!cell.outputs || cell.outputs.length === 0) {
      return true; // No outputs usually means successful execution
    }
    
    // Check for error outputs
    for (const output of cell.outputs) {
      if (output.output_type === 'error') {
        return false;
      }
    }
    
    return true;
  }
}
