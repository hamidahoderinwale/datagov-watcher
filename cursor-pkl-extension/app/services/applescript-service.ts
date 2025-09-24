import { exec } from 'child_process';
import { promisify } from 'util';
import path from 'path';

const execAsync = promisify(exec);

/**
 * AppleScript integration service for Cursor operations
 * Handles file opening, cursor positioning, and context restoration
 */
export class AppleScriptService {
  private static readonly CURSOR_APP_NAME = 'Cursor';

  /**
   * Opens a file in Cursor at a specific line and column
   */
  static async openFileInCursor(filePath: string, line?: number, column?: number): Promise<boolean> {
    try {
      const absolutePath = path.resolve(filePath);
      
      let script = `tell application "${this.CURSOR_APP_NAME}" to activate`;
      
      if (line !== undefined) {
        script += `\ntell application "${this.CURSOR_APP_NAME}" to open "${absolutePath}"`;
        script += `\ntell application "System Events" to tell process "${this.CURSOR_APP_NAME}"`;
        script += `\n\tkeystroke "g" using {command down, shift down}`;
        script += `\n\tkeystroke "${line}"`;
        script += `\n\tkey code 36`; // Enter key
        if (column !== undefined) {
          script += `\n\tkeystroke "${column}"`;
        }
        script += `\nend tell`;
      } else {
        script += `\ntell application "${this.CURSOR_APP_NAME}" to open "${absolutePath}"`;
      }

      await execAsync(`osascript -e '${script}'`);
      return true;
    } catch (error) {
      console.error('Failed to open file in Cursor:', error);
      return false;
    }
  }

  /**
   * Selects text in Cursor
   */
  static async selectTextInCursor(text: string): Promise<boolean> {
    try {
      const script = `
        tell application "${this.CURSOR_APP_NAME}" to activate
        tell application "System Events" to tell process "${this.CURSOR_APP_NAME}"
          keystroke "a" using command down
          keystroke "${text.replace(/"/g, '\\"')}"
        end tell
      `;

      await execAsync(`osascript -e '${script}'`);
      return true;
    } catch (error) {
      console.error('Failed to select text in Cursor:', error);
      return false;
    }
  }

  /**
   * Checks if Cursor is running
   */
  static async isCursorRunning(): Promise<boolean> {
    try {
      const { stdout } = await execAsync(`pgrep -f "${this.CURSOR_APP_NAME}"`);
      return stdout.trim().length > 0;
    } catch (error) {
      return false;
    }
  }

  /**
   * Gets the currently active file in Cursor
   */
  static async getCurrentFile(): Promise<string | null> {
    try {
      const script = `
        tell application "${this.CURSOR_APP_NAME}" to activate
        tell application "System Events" to tell process "${this.CURSOR_APP_NAME}"
          keystroke "p" using command down
          delay 0.5
          keystroke "c" using command down
        end tell
      `;

      await execAsync(`osascript -e '${script}'`);
      
      // Get clipboard content
      const { stdout } = await execAsync('pbpaste');
      return stdout.trim() || null;
    } catch (error) {
      console.error('Failed to get current file:', error);
      return null;
    }
  }

  /**
   * Restores a complete session context
   */
  static async restoreSessionContext(session: {
    currentFile?: string;
    cursorPosition?: { line: number; character: number };
    selectedText?: string;
  }): Promise<boolean> {
    try {
      if (!session.currentFile) return false;

      const success = await this.openFileInCursor(
        session.currentFile,
        session.cursorPosition?.line,
        session.cursorPosition?.character
      );

      if (success && session.selectedText) {
        await this.selectTextInCursor(session.selectedText);
      }

      return success;
    } catch (error) {
      console.error('Failed to restore session context:', error);
      return false;
    }
  }
}
