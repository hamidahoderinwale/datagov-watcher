const fs = require('fs');
const path = require('path');

/**
 * Jupyter notebook parser
 * Extracts execution history, cell content, and data science patterns
 */
class JupyterParser {
  constructor() {
    this.dataScienceKeywords = [
      'pandas', 'numpy', 'matplotlib', 'seaborn', 'plotly', 'sklearn', 'tensorflow', 'pytorch',
      'dataframe', 'array', 'plot', 'chart', 'visualize', 'analyze', 'explore', 'model',
      'train', 'predict', 'fit', 'score', 'accuracy', 'precision', 'recall', 'f1',
      'correlation', 'regression', 'classification', 'clustering', 'pca', 'svd'
    ];
  }

  async parseNotebook(filePath) {
    try {
      if (!fs.existsSync(filePath)) {
        throw new Error('Notebook file not found');
      }

      const content = fs.readFileSync(filePath, 'utf8');
      const notebook = JSON.parse(content);

      return {
        metadata: this.extractMetadata(notebook),
        cells: this.extractCells(notebook),
        executionHistory: this.extractExecutionHistory(notebook),
        dataSciencePatterns: this.analyzeDataSciencePatterns(notebook),
        fileInfo: {
          path: filePath,
          size: fs.statSync(filePath).size,
          lastModified: fs.statSync(filePath).mtime
        }
      };
    } catch (error) {
      console.error('Error parsing notebook:', error);
      throw error;
    }
  }

  extractMetadata(notebook) {
    return {
      kernelspec: notebook.metadata?.kernelspec || {},
      language_info: notebook.metadata?.language_info || {},
      cellCount: notebook.cells?.length || 0,
      hasOutputs: notebook.cells?.some(cell => cell.outputs && cell.outputs.length > 0) || false
    };
  }

  extractCells(notebook) {
    if (!notebook.cells) return [];

    return notebook.cells.map((cell, index) => ({
      index,
      cell_type: cell.cell_type,
      source: Array.isArray(cell.source) ? cell.source.join('') : cell.source || '',
      execution_count: cell.execution_count || null,
      outputs: cell.outputs || [],
      metadata: cell.metadata || {},
      execution_state: this.getExecutionState(cell),
      visualizations: this.extractVisualizations(cell),
      hasErrors: this.hasCellErrors(cell)
    }));
  }

  extractExecutionHistory(notebook) {
    if (!notebook.cells) return [];

    const executionHistory = [];
    
    notebook.cells.forEach((cell, index) => {
      if (cell.cell_type === 'code' && cell.execution_count) {
        executionHistory.push({
          cellIndex: index,
          executionCount: cell.execution_count,
          timestamp: this.extractCellTimestamp(cell),
          outputs: cell.outputs || [],
          source: Array.isArray(cell.source) ? cell.source.join('') : cell.source || ''
        });
      }
    });

    return executionHistory.sort((a, b) => a.executionCount - b.executionCount);
  }

  extractCellTimestamp(cell) {
    // Try to extract timestamp from cell metadata
    if (cell.metadata?.execution?.iopub?.timestamp) {
      return new Date(cell.metadata.execution.iopub.timestamp);
    }
    
    // Fallback to current time
    return new Date();
  }

  analyzeDataSciencePatterns(notebook) {
    if (!notebook.cells) return {};

    const allSource = notebook.cells
      .filter(cell => cell.cell_type === 'code')
      .map(cell => Array.isArray(cell.source) ? cell.source.join('') : cell.source || '')
      .join('\n')
      .toLowerCase();

    const patterns = {
      hasDataLoading: this.detectPattern(allSource, ['pd.read_', 'np.load', 'open(', 'csv', 'json']),
      hasVisualization: this.detectPattern(allSource, ['plt.', 'sns.', 'plot', 'chart', 'hist', 'scatter']),
      hasAnalysis: this.detectPattern(allSource, ['describe', 'corr', 'groupby', 'pivot', 'merge']),
      hasMachineLearning: this.detectPattern(allSource, ['sklearn', 'fit', 'predict', 'train_test_split', 'model']),
      hasStatistics: this.detectPattern(allSource, ['mean', 'std', 'median', 'quantile', 't-test', 'chi2']),
      hasDataManipulation: this.detectPattern(allSource, ['drop', 'fillna', 'replace', 'apply', 'transform']),
      libraries: this.extractLibraries(allSource),
      complexity: this.calculateComplexity(allSource)
    };

    return patterns;
  }

  detectPattern(text, keywords) {
    return keywords.some(keyword => text.includes(keyword));
  }

  extractLibraries(text) {
    const importPattern = /(?:import|from)\s+(\w+)/g;
    const libraries = new Set();
    let match;

    while ((match = importPattern.exec(text)) !== null) {
      libraries.add(match[1]);
    }

    return Array.from(libraries);
  }

  calculateComplexity(text) {
    const lines = text.split('\n').length;
    const functions = (text.match(/def\s+\w+/g) || []).length;
    const classes = (text.match(/class\s+\w+/g) || []).length;
    const loops = (text.match(/for\s+|while\s+/g) || []).length;
    const conditionals = (text.match(/if\s+/g) || []).length;

    return {
      lines,
      functions,
      classes,
      loops,
      conditionals,
      score: lines + functions * 2 + classes * 3 + loops + conditionals
    };
  }

  async detectSessionIntent(notebook) {
    try {
      const patterns = this.analyzeDataSciencePatterns(notebook);
      
      if (patterns.hasMachineLearning) return 'IMPLEMENT';
      if (patterns.hasVisualization && patterns.hasAnalysis) return 'EXPLORE';
      if (patterns.hasDataLoading && patterns.hasDataManipulation) return 'IMPLEMENT';
      if (patterns.hasStatistics) return 'EXPLORE';
      
      return 'EXPLORE'; // Default for data science notebooks
    } catch (error) {
      console.error('Error detecting session intent:', error);
      return 'EXPLORE';
    }
  }

  async detectSessionOutcome(notebook) {
    try {
      const executionHistory = this.extractExecutionHistory(notebook);
      const patterns = this.analyzeDataSciencePatterns(notebook);
      
      // Check if notebook has been executed
      const hasExecutedCells = executionHistory.length > 0;
      
      // Check for error outputs
      const hasErrors = executionHistory.some(cell => 
        cell.outputs.some(output => output.output_type === 'error')
      );
      
      // Check for successful outputs
      const hasSuccessfulOutputs = executionHistory.some(cell => 
        cell.outputs.some(output => output.output_type === 'execute_result' || output.output_type === 'display_data')
      );
      
      if (hasErrors && !hasSuccessfulOutputs) return 'stuck';
      if (hasExecutedCells && hasSuccessfulOutputs) return 'success';
      if (hasExecutedCells) return 'in-progress';
      
      return 'in-progress';
    } catch (error) {
      console.error('Error detecting session outcome:', error);
      return 'in-progress';
    }
  }

  async extractCodeBlocks(notebook) {
    if (!notebook.cells) return [];

    const codeBlocks = [];
    
    notebook.cells.forEach((cell, index) => {
      if (cell.cell_type === 'code') {
        const source = Array.isArray(cell.source) ? cell.source.join('') : cell.source || '';
        
        if (source.trim()) {
          codeBlocks.push({
            cellIndex: index,
            language: 'python',
            content: source,
            executionCount: cell.execution_count,
            outputs: cell.outputs || []
          });
        }
      }
    });

    return codeBlocks;
  }

  getExecutionState(cell) {
    if (!cell.execution_count) return 'not_executed';
    if (this.hasCellErrors(cell)) return 'error';
    if (cell.outputs && cell.outputs.length > 0) return 'executed';
    return 'executed';
  }

  hasCellErrors(cell) {
    if (!cell.outputs) return false;
    return cell.outputs.some(output => output.output_type === 'error');
  }

  extractVisualizations(cell) {
    if (!cell.outputs || cell.cell_type !== 'code') return [];

    const visualizations = [];
    
    cell.outputs.forEach((output, outputIndex) => {
      if (output.output_type === 'display_data' || output.output_type === 'execute_result') {
        // Check for matplotlib figures
        if (output.data && output.data['image/png']) {
          visualizations.push({
            type: 'matplotlib',
            format: 'png',
            data: output.data['image/png'],
            outputIndex,
            timestamp: new Date().toISOString()
          });
        }
        
        // Check for plotly figures
        if (output.data && output.data['application/vnd.plotly.v1+json']) {
          const plotlyData = JSON.parse(output.data['application/vnd.plotly.v1+json']);
          visualizations.push({
            type: 'plotly',
            format: 'json',
            data: plotlyData,
            outputIndex,
            timestamp: new Date().toISOString()
          });
        }
        
        // Check for text-based plots (ASCII art, etc.)
        if (output.data && output.data['text/plain']) {
          const text = output.data['text/plain'];
          if (this.isVisualizationText(text)) {
            visualizations.push({
              type: 'text_plot',
              format: 'text',
              data: text,
              outputIndex,
              timestamp: new Date().toISOString()
            });
          }
        }
      }
    });

    return visualizations;
  }

  isVisualizationText(text) {
    // Check for common visualization text patterns
    const vizPatterns = [
      /plot|chart|graph|figure|visualization/i,
      /┌|┐|└|┘|─|│|╭|╮|╯|╰|├|┤|┬|┴|┼/, // Box drawing characters
      /^\s*[│┌┐└┘─├┤┬┴┼]/, // Starts with box drawing
      /^\s*[█▄▀▐▌▀▄]/, // Block characters
      /^\s*[\*\-\+]/, // Bullet patterns
      /^\s*[▲▼◄►]/, // Arrow patterns
    ];
    
    return vizPatterns.some(pattern => pattern.test(text));
  }

  async extractConversationContext(notebook) {
    try {
      const cells = this.extractCells(notebook);
      const codeBlocks = await this.extractCodeBlocks(notebook);
      const patterns = this.analyzeDataSciencePatterns(notebook);
      
      return {
        totalCells: cells.length,
        codeCells: cells.filter(cell => cell.cell_type === 'code').length,
        markdownCells: cells.filter(cell => cell.cell_type === 'markdown').length,
        executedCells: cells.filter(cell => cell.execution_count).length,
        codeBlocks,
        dataSciencePatterns: patterns,
        complexity: patterns.complexity,
        executionStates: this.getExecutionStates(cells),
        visualizationCount: this.getVisualizationCount(cells)
      };
    } catch (error) {
      console.error('Error extracting conversation context:', error);
      return null;
    }
  }

  getExecutionStates(cells) {
    const states = {
      not_executed: 0,
      executed: 0,
      error: 0
    };
    
    cells.forEach(cell => {
      if (cell.cell_type === 'code') {
        states[cell.execution_state]++;
      }
    });
    
    return states;
  }

  getVisualizationCount(cells) {
    let count = 0;
    cells.forEach(cell => {
      if (cell.visualizations) {
        count += cell.visualizations.length;
      }
    });
    return count;
  }
}

module.exports = JupyterParser;
