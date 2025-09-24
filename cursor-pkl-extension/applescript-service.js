const { exec } = require('child_process');
const { promisify } = require('util');
const path = require('path');
const fs = require('fs').promises;

const execAsync = promisify(exec);

/**
 * AppleScript integration service for Cursor operations
 * JavaScript version for web server
 */
class AppleScriptService {
  static CURSOR_APP_NAME = 'Cursor';

  /**
   * Opens a file in Cursor at a specific line and column using multiple approaches
   */
  static async openFileInCursor(filePath, line, column) {
    try {
      const absolutePath = path.resolve(filePath);
      
      // Verify file exists
      try {
        await fs.access(absolutePath);
      } catch (error) {
        console.error('File does not exist:', absolutePath);
        return false;
      }

      console.log(`Opening file: ${absolutePath} at line ${line || 'N/A'}, column ${column || 'N/A'}`);

      // Method 1: Try using Cursor's CLI if available
      try {
        const cursorCommand = line !== undefined 
          ? `cursor "${absolutePath}:${line}:${column || 1}"`
          : `cursor "${absolutePath}"`;
        
        await execAsync(cursorCommand);
        console.log('SUCCESS: Opened file using Cursor CLI');
        return true;
      } catch (cliError) {
        console.log('Cursor CLI not available, trying AppleScript...');
      }

      // Method 2: AppleScript approach with improved reliability
      const script = this.buildAppleScript(absolutePath, line, column);
      await execAsync(`osascript -e '${script}'`);
      console.log('SUCCESS: Opened file using AppleScript');
      return true;

    } catch (error) {
      console.error('All methods failed to open file in Cursor:', error);
      
      // Method 3: Fallback to simple file opening
      try {
        await execAsync(`open -a "${this.CURSOR_APP_NAME}" "${path.resolve(filePath)}"`);
        console.log('FALLBACK: Opened file without position');
        return true;
      } catch (fallbackError) {
        console.error('Even fallback method failed:', fallbackError);
        return false;
      }
    }
  }

  /**
   * Build AppleScript with improved error handling
   */
  static buildAppleScript(filePath, line, column) {
    let script = `
      tell application "${this.CURSOR_APP_NAME}"
        activate
        open POSIX file "${filePath}"
      end tell
    `;

    if (line !== undefined && line > 0) {
      script += `
        delay 1
        tell application "System Events"
          tell process "${this.CURSOR_APP_NAME}"
            keystroke "g" using {command down}
            delay 0.5
            keystroke "${line}"
            delay 0.3
            key code 36
          end tell
        end tell
      `;
    }

    return script.replace(/\n\s+/g, '\n').trim();
  }

  /**
   * Selects text in Cursor
   */
  static async selectTextInCursor(text) {
    try {
      const script = `
        tell application "${this.CURSOR_APP_NAME}" to activate
        tell application "System Events" to tell process "${this.CURSOR_APP_NAME}"
          keystroke "f" using command down
          delay 0.5
          keystroke "${text}"
          delay 0.5
          key code 36
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
  static async isCursorRunning() {
    try {
      const script = `tell application "System Events" to (name of processes) contains "${this.CURSOR_APP_NAME}"`;
      const { stdout } = await execAsync(`osascript -e '${script}'`);
      return stdout.trim() === 'true';
    } catch (error) {
      return false;
    }
  }

  /**
   * Restores a complete session context with enhanced reliability
   */
  static async restoreSessionContext(session) {
    try {
      if (!session.currentFile) {
        console.log('No file path provided for session context restoration');
        return { success: false, error: 'No file path provided' };
      }

      console.log('Restoring session context:', {
        file: session.currentFile,
        line: session.cursorPosition?.line,
        column: session.cursorPosition?.character
      });

      // Check if Cursor is running and start if needed
      const isRunning = await this.isCursorRunning();
      if (!isRunning) {
        console.log('Starting Cursor IDE...');
        try {
          await execAsync(`open -a "${this.CURSOR_APP_NAME}"`);
          // Wait for Cursor to start
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          return { success: false, error: 'Failed to start Cursor IDE' };
        }
      }

      // Open the file at the specific position
      const success = await this.openFileInCursor(
        session.currentFile,
        session.cursorPosition?.line,
        session.cursorPosition?.character
      );

      if (!success) {
        return { success: false, error: 'Failed to open file in Cursor' };
      }

      // Optional: Select text if available
      if (session.selectedText && session.selectedText.trim()) {
        try {
          await new Promise(resolve => setTimeout(resolve, 1500));
          await this.selectTextInCursor(session.selectedText);
        } catch (error) {
          console.log('Text selection failed, but file opened successfully');
        }
      }

      return { 
        success: true, 
        method: 'enhanced_applescript',
        file: session.currentFile,
        position: session.cursorPosition
      };
    } catch (error) {
      console.error('Failed to restore session context:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Extract visualization data from notebook for preview
   */
  static async extractNotebookVisualizations(filePath) {
    try {
      const absolutePath = path.resolve(filePath);
      
      // Verify file exists and is a notebook
      if (!absolutePath.endsWith('.ipynb')) {
        return { visualizations: [], error: 'Not a notebook file' };
      }

      try {
        await fs.access(absolutePath);
      } catch (error) {
        return { visualizations: [], error: 'File not found' };
      }

      // Read and parse notebook
      const notebookContent = await fs.readFile(absolutePath, 'utf8');
      
      // Handle notebooks with extra content after JSON
      let cleanedContent = notebookContent;
      const lastBraceIndex = notebookContent.lastIndexOf('}');
      if (lastBraceIndex !== -1 && lastBraceIndex < notebookContent.length - 1) {
        // There's content after the last brace, truncate it
        cleanedContent = notebookContent.substring(0, lastBraceIndex + 1);
        console.log('Cleaned notebook content (removed trailing text)');
      }
      
      const notebook = JSON.parse(cleanedContent);

      const visualizations = [];
      
      if (notebook.cells) {
        notebook.cells.forEach((cell, index) => {
          if (cell.cell_type === 'code' && cell.outputs) {
            cell.outputs.forEach((output, outputIndex) => {
              // Check for matplotlib/plotly/seaborn visualizations
              if (output.data) {
                // Image outputs (PNG, SVG)
                if (output.data['image/png']) {
                  visualizations.push({
                    type: 'image',
                    format: 'png',
                    data: output.data['image/png'],
                    cellIndex: index,
                    outputIndex: outputIndex,
                    source: cell.source ? cell.source.slice(0, 100).join('').trim() : ''
                  });
                }
                
                if (output.data['image/svg+xml']) {
                  visualizations.push({
                    type: 'image',
                    format: 'svg',
                    data: output.data['image/svg+xml'],
                    cellIndex: index,
                    outputIndex: outputIndex,
                    source: cell.source ? cell.source.slice(0, 100).join('').trim() : ''
                  });
                }

                // Plotly/interactive visualizations
                if (output.data['application/vnd.plotly.v1+json']) {
                  visualizations.push({
                    type: 'plotly',
                    data: output.data['application/vnd.plotly.v1+json'],
                    cellIndex: index,
                    outputIndex: outputIndex,
                    source: cell.source ? cell.source.slice(0, 100).join('').trim() : ''
                  });
                }

                // HTML outputs (might contain charts)
                if (output.data['text/html']) {
                  const htmlContent = Array.isArray(output.data['text/html']) 
                    ? output.data['text/html'].join('') 
                    : output.data['text/html'];
                  
                  if (htmlContent.includes('plotly') || htmlContent.includes('chart') || htmlContent.includes('svg')) {
                    visualizations.push({
                      type: 'html',
                      data: htmlContent,
                      cellIndex: index,
                      outputIndex: outputIndex,
                      source: cell.source ? cell.source.slice(0, 100).join('').trim() : ''
                    });
                  }
                }
              }
            });
          }
        });
      }

      return { 
        visualizations, 
        total: visualizations.length,
        file: absolutePath,
        lastModified: (await fs.stat(absolutePath)).mtime
      };
    } catch (error) {
      console.error('Error extracting visualizations:', error);
      return { visualizations: [], error: error.message };
    }
  }
}

module.exports = AppleScriptService;
