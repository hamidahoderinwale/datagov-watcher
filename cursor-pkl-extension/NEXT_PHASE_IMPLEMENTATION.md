# PKL Extension - Next Phase Implementation Plan

## **IMMEDIATE PRIORITY: Procedural Pattern Mining**

Based on the comprehensive review, the PKL Extension is functionally complete but missing the **core essence** of procedural knowledge libraries. Here's a focused implementation plan for the most impactful enhancement:

---

## **PHASE 1: Smart Procedure Detection (2-3 weeks)**

### **Goal**: Transform session tracking into procedural knowledge capture

### **Implementation Steps**:

### 1. **Enhanced Pattern Analysis Service**

```typescript
// New file: app/services/procedure-pattern-service.ts
export class ProcedurePatternService {
  
  /**
   * Analyze session sequences to identify recurring procedures
   */
  async identifyProcedures(sessions: PKLSession[]): Promise<ProceduralPattern[]> {
    const patterns = [];
    
    // Group sessions by file type and intent
    const sessionGroups = this.groupSessionsByContext(sessions);
    
    for (const group of sessionGroups) {
      // Extract common sequences
      const sequences = await this.extractActionSequences(group);
      
      // Identify patterns using sequence mining
      const patterns = await this.mineSequencePatterns(sequences);
      
      // Generate executable templates
      const templates = await this.generateTemplates(patterns);
      
      patterns.push(...templates);
    }
    
    return patterns;
  }
  
  private async extractActionSequences(sessions: PKLSession[]): Promise<ActionSequence[]> {
    return sessions.map(session => ({
      sessionId: session.id,
      actions: this.parseNotebookActions(session),
      context: this.extractContext(session),
      outcome: session.outcome
    }));
  }
  
  private parseNotebookActions(session: PKLSession): NotebookAction[] {
    // Parse code deltas into semantic actions
    const actions = [];
    
    for (const delta of session.codeDeltas) {
      const action = this.classifyCodeAction(delta);
      if (action) actions.push(action);
    }
    
    return actions;
  }
  
  private classifyCodeAction(delta: CodeDelta): NotebookAction | null {
    const code = delta.afterContent.toLowerCase();
    
    // Data science action patterns
    if (code.includes('pd.read_') || code.includes('.load') || code.includes('open(')) {
      return { type: 'data_loading', confidence: 0.9, code: delta.afterContent };
    }
    
    if (code.includes('.describe()') || code.includes('.info()') || code.includes('.head()')) {
      return { type: 'data_exploration', confidence: 0.95, code: delta.afterContent };
    }
    
    if (code.includes('.plot') || code.includes('plt.') || code.includes('sns.')) {
      return { type: 'visualization', confidence: 0.9, code: delta.afterContent };
    }
    
    if (code.includes('.fit(') || code.includes('.predict(') || code.includes('model')) {
      return { type: 'modeling', confidence: 0.85, code: delta.afterContent };
    }
    
    // Add more patterns...
    return null;
  }
}
```

### 2. **Procedure Template Generator**

```typescript
// New file: app/services/template-generator.ts
export class TemplateGenerator {
  
  async generateExecutableTemplate(pattern: ProceduralPattern): Promise<ExecutableTemplate> {
    return {
      id: `template-${pattern.id}`,
      name: this.generateTemplateName(pattern),
      description: this.generateDescription(pattern),
      category: pattern.category,
      
      cells: await this.generateNotebookCells(pattern),
      parameters: this.extractParameters(pattern),
      dependencies: this.identifyDependencies(pattern),
      
      sourcePattern: pattern,
      usageStats: { executions: 0, successRate: 0, avgTime: 0 }
    };
  }
  
  private async generateNotebookCells(pattern: ProceduralPattern): Promise<NotebookCell[]> {
    const cells = [];
    
    // Generate markdown introduction
    cells.push({
      cell_type: 'markdown',
      source: [
        `# ${pattern.name}\n`,
        `\n`,
        `**Purpose**: ${pattern.description}\n`,
        `**Success Rate**: ${(pattern.successRate * 100).toFixed(1)}%\n`,
        `**Typical Duration**: ~${pattern.avgDuration}min\n`,
        `\n`,
        `## Parameters\n`,
        ...pattern.steps.map(step => `- ${step.description}\n`)
      ]
    });
    
    // Generate parameterized code cells
    for (const step of pattern.steps) {
      if (step.action === 'code') {
        cells.push({
          cell_type: 'code',
          source: this.parameterizeCode(step.codePattern),
          metadata: {
            step_description: step.description,
            expected_output: step.expectedOutput,
            common_errors: step.commonErrors
          }
        });
      }
    }
    
    return cells;
  }
  
  private parameterizeCode(codePattern: string): string[] {
    // Replace common patterns with parameters
    let parameterized = codePattern
      .replace(/['"][^'"]+\.csv['"]/, '"{{dataset_path}}"')
      .replace(/df\['[^']+'\]/, 'df["{{target_column}}"]')
      .replace(/figsize=\([^)]+\)/, 'figsize={{figure_size}}');
      
    return parameterized.split('\n');
  }
}
```

### 3. **Smart Suggestion Engine**

```typescript
// New file: app/services/suggestion-engine.ts
export class SuggestionEngine {
  
  async getSuggestions(currentSession: PKLSession): Promise<ProcedureSuggestion[]> {
    const context = this.analyzeCurrentContext(currentSession);
    const availablePatterns = await this.loadProcedurePatterns();
    
    const suggestions = [];
    
    for (const pattern of availablePatterns) {
      const relevance = this.calculateRelevance(context, pattern);
      
      if (relevance > 0.7) {
        suggestions.push({
          pattern,
          relevance,
          reason: this.generateReason(context, pattern),
          estimatedTime: pattern.avgDuration,
          parameters: this.suggestParameters(context, pattern)
        });
      }
    }
    
    return suggestions.sort((a, b) => b.relevance - a.relevance);
  }
  
  private analyzeCurrentContext(session: PKLSession): SessionContext {
    return {
      fileType: this.detectFileType(session.currentFile),
      recentActions: this.getRecentActions(session),
      dataCharacteristics: this.analyzeDataContext(session),
      intent: session.intent,
      phase: session.phase
    };
  }
  
  private calculateRelevance(context: SessionContext, pattern: ProceduralPattern): number {
    let score = 0;
    
    // Context matching
    if (pattern.contexts.includes(context.fileType)) score += 0.3;
    if (pattern.category === context.intent) score += 0.4;
    
    // Sequential matching
    const actionMatch = this.calculateActionSequenceMatch(
      context.recentActions, 
      pattern.steps
    );
    score += actionMatch * 0.3;
    
    return Math.min(score, 1.0);
  }
}
```

---

## **PHASE 2: Enhanced UI for Procedure Discovery (1-2 weeks)**

### **New Dashboard Components**:

### 1. **Procedure Suggestions Panel**
```html
<!-- Add to live-dashboard-clean.html -->
<section class="suggestions-panel" id="suggestions-panel">
  <div class="panel-header">
    <h3>Smart Suggestions</h3>
    <span class="suggestion-count">3 procedures available</span>
  </div>
  
  <div class="suggestions-list" id="suggestions-list">
    <!-- Populated by JavaScript -->
  </div>
</section>
```

### 2. **Procedure Execution Modal**
```html
<div class="modal" id="procedureExecutionModal">
  <div class="modal-content procedure-modal">
    <div class="modal-header">
      <h2 id="procedure-title">Execute Procedure</h2>
      <button class="modal-close" onclick="closeProcedureModal()">&times;</button>
    </div>
    
    <div class="modal-body">
      <div class="procedure-info">
        <div class="procedure-meta">
          <span class="success-rate">Success Rate: <strong id="success-rate">0%</strong></span>
          <span class="avg-time">Avg Time: <strong id="avg-time">0min</strong></span>
        </div>
        <p class="procedure-description" id="procedure-description"></p>
      </div>
      
      <div class="procedure-parameters" id="procedure-parameters">
        <!-- Dynamic parameter inputs -->
      </div>
      
      <div class="procedure-preview" id="procedure-preview">
        <h4>Generated Code Preview</h4>
        <pre><code id="code-preview"></code></pre>
      </div>
    </div>
    
    <div class="modal-footer">
      <button class="btn btn-secondary" onclick="closeProcedureModal()">Cancel</button>
      <button class="btn btn-primary" onclick="executeProcedure()">Execute in Cursor</button>
    </div>
  </div>
</div>
```

### 3. **Enhanced JavaScript for Suggestions**
```javascript
// Add to assets/js/dashboard.js
class ProcedureManager {
  constructor() {
    this.suggestions = [];
    this.patterns = [];
  }
  
  async loadSuggestions(sessionId) {
    try {
      const response = await fetch(`/api/session/${sessionId}/suggestions`);
      const data = await response.json();
      
      if (data.success) {
        this.suggestions = data.suggestions;
        this.renderSuggestions();
      }
    } catch (error) {
      console.error('Failed to load suggestions:', error);
    }
  }
  
  renderSuggestions() {
    const container = document.getElementById('suggestions-list');
    if (!container) return;
    
    const suggestionsHtml = this.suggestions.map(suggestion => `
      <div class="suggestion-item" onclick="showProcedureModal('${suggestion.pattern.id}')">
        <div class="suggestion-header">
          <h4 class="suggestion-title">${suggestion.pattern.name}</h4>
          <span class="relevance-score">${(suggestion.relevance * 100).toFixed(0)}% match</span>
        </div>
        <p class="suggestion-reason">${suggestion.reason}</p>
        <div class="suggestion-meta">
          <span class="estimated-time">~${suggestion.estimatedTime}min</span>
          <span class="success-rate">${(suggestion.pattern.successRate * 100).toFixed(0)}% success</span>
        </div>
      </div>
    `).join('');
    
    container.innerHTML = suggestionsHtml;
  }
}

// Add to global functions
async function executeProcedure() {
  const procedureId = document.getElementById('procedureExecutionModal').dataset.procedureId;
  const parameters = collectProcedureParameters();
  
  try {
    const response = await fetch('/api/procedures/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        procedureId,
        parameters,
        targetFile: getCurrentFile()
      })
    });
    
    const result = await response.json();
    
    if (result.success) {
      // Open generated notebook in Cursor
      await fetch('/api/cursor/open-notebook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          notebookPath: result.generatedNotebook,
          openInCursor: true
        })
      });
      
      closeProcedureModal();
      alert('Procedure executed successfully! Check Cursor for the generated notebook.');
    }
  } catch (error) {
    console.error('Procedure execution failed:', error);
    alert('Failed to execute procedure. Please try again.');
  }
}
```

---

## **PHASE 3: Learning and Adaptation (2 weeks)**

### **Implementation**:

1. **Usage Tracking**: Monitor which procedures are used and their success rates
2. **Pattern Evolution**: Update patterns based on user modifications and outcomes
3. **Personalization**: Adapt suggestions based on individual user patterns
4. **Community Learning**: Share anonymized successful patterns (with privacy controls)

---

## **EXPECTED OUTCOMES**

### **Immediate Benefits** (after Phase 1):
- **Automatic discovery** of 5-10 common data science procedures from existing sessions
- **Template generation** for repetitive tasks (EDA, model validation, etc.)
- **Context-aware suggestions** during active coding sessions

### **Medium-term Impact** (after Phase 2):
- **50% reduction** in time for common analysis tasks
- **Standardized workflows** across different projects
- **Knowledge preservation** of successful approaches

### **Long-term Vision** (after Phase 3):
- **Continuous learning** system that improves with usage
- **Community-driven** procedure library
- **Research reproducibility** through standardized, validated procedures

---

This focused implementation plan transforms the PKL Extension from a session tracker into a true **procedural knowledge library** that learns, suggests, and automates the thinking patterns of data scientists and researchers.
