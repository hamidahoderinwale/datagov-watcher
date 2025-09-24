/**
 * Procedural Pattern Detection and Template Generation
 * 
 * This module transforms the PKL Extension from session tracking to true procedural knowledge capture
 * by identifying common data science workflows and generating executable templates.
 */

class ProcedurePatternService {
  constructor() {
    // Built-in data science procedure patterns
    this.builtInPatterns = [
      {
        id: 'quick-eda',
        name: 'Quick Exploratory Data Analysis',
        category: 'exploration',
        description: 'Comprehensive overview of dataset characteristics, distributions, and relationships',
        successRate: 0.92,
        avgDuration: 8,
        contexts: ['csv', 'parquet', 'excel', 'json', 'notebook', 'ipynb'],
        triggers: ['data loading', 'new dataset', 'pd.read_'],
        steps: [
          {
            sequence: 1,
            action: 'code',
            description: 'Load and inspect dataset structure',
            codePattern: `# Dataset Overview
df = pd.read_csv('{{dataset_path}}')
print(f"Dataset shape: {df.shape}")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
df.info()`,
            expectedOutput: 'Dataset dimensions and column information'
          },
          {
            sequence: 2,
            action: 'code',
            description: 'Check for missing values and duplicates',
            codePattern: `# Data Quality Check
print("Missing Values:")
missing_stats = df.isnull().sum()
missing_pct = (missing_stats / len(df)) * 100
quality_df = pd.DataFrame({
    'Missing_Count': missing_stats,
    'Missing_Percentage': missing_pct
})
print(quality_df[quality_df.Missing_Count > 0].sort_values('Missing_Count', ascending=False))

print(f"\\nDuplicate rows: {df.duplicated().sum()}")`,
            expectedOutput: 'Missing value statistics and duplicate count'
          },
          {
            sequence: 3,
            action: 'code',
            description: 'Generate statistical summary',
            codePattern: `# Statistical Summary
print("Numerical Variables Summary:")
numerical_cols = df.select_dtypes(include=[np.number]).columns
if len(numerical_cols) > 0:
    display(df[numerical_cols].describe())

print("\\nCategorical Variables Summary:")
categorical_cols = df.select_dtypes(include=['object', 'category']).columns
if len(categorical_cols) > 0:
    for col in categorical_cols[:5]:  # Show first 5 categorical columns
        print(f"\\n{col}: {df[col].nunique()} unique values")
        print(df[col].value_counts().head())`,
            expectedOutput: 'Descriptive statistics for numerical and categorical variables'
          },
          {
            sequence: 4,
            action: 'code',
            description: 'Create distribution visualizations',
            codePattern: `# Distribution Visualizations
import matplotlib.pyplot as plt
import seaborn as sns

# Set up the plotting style
plt.style.use('default')
sns.set_palette("husl")

# Numerical distributions
numerical_cols = df.select_dtypes(include=[np.number]).columns
if len(numerical_cols) > 0:
    n_cols = min(len(numerical_cols), 4)
    n_rows = (len(numerical_cols) + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 4*n_rows))
    if n_rows == 1:
        axes = [axes] if n_cols == 1 else axes
    else:
        axes = axes.flatten()
    
    for i, col in enumerate(numerical_cols[:12]):  # Limit to 12 columns
        df[col].hist(bins=30, ax=axes[i], alpha=0.7)
        axes[i].set_title(f'Distribution of {col}')
        axes[i].set_xlabel(col)
        axes[i].set_ylabel('Frequency')
    
    # Hide empty subplots
    for i in range(len(numerical_cols), len(axes)):
        axes[i].set_visible(False)
    
    plt.tight_layout()
    plt.show()`,
            expectedOutput: 'Histogram plots showing distributions of numerical variables'
          },
          {
            sequence: 5,
            action: 'code',
            description: 'Correlation analysis',
            codePattern: `# Correlation Analysis
numerical_cols = df.select_dtypes(include=[np.number]).columns
if len(numerical_cols) > 1:
    correlation_matrix = df[numerical_cols].corr()
    
    # Create correlation heatmap
    plt.figure(figsize=(12, 8))
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    sns.heatmap(correlation_matrix, mask=mask, annot=True, cmap='coolwarm', 
                center=0, square=True, linewidths=0.5)
    plt.title('Correlation Matrix Heatmap')
    plt.tight_layout()
    plt.show()
    
    # Find high correlations
    high_corr_pairs = []
    for i in range(len(correlation_matrix.columns)):
        for j in range(i+1, len(correlation_matrix.columns)):
            corr_val = correlation_matrix.iloc[i, j]
            if abs(corr_val) > 0.7:
                high_corr_pairs.append((
                    correlation_matrix.columns[i], 
                    correlation_matrix.columns[j], 
                    corr_val
                ))
    
    if high_corr_pairs:
        print("High correlation pairs (|r| > 0.7):")
        for var1, var2, corr in high_corr_pairs:
            print(f"{var1} - {var2}: {corr:.3f}")
else:
    print("Not enough numerical variables for correlation analysis")`,
            expectedOutput: 'Correlation heatmap and list of highly correlated variable pairs'
          }
        ],
        parameters: [
          {
            name: 'dataset_path',
            type: 'string',
            description: 'Path to the dataset file',
            defaultValue: 'data.csv',
            validation: ['file_exists', 'readable_format']
          },
          {
            name: 'figure_size',
            type: 'tuple',
            description: 'Figure size for plots (width, height)',
            defaultValue: '(12, 8)',
            validation: ['positive_numbers']
          }
        ]
      },
      
      {
        id: 'model-validation',
        name: 'Model Performance Validation',
        category: 'modeling',
        description: 'Comprehensive validation of machine learning model performance with multiple metrics and visualizations',
        successRate: 0.89,
        avgDuration: 12,
        contexts: ['sklearn', 'model', 'prediction'],
        triggers: ['model.fit', 'predict', 'accuracy'],
        steps: [
          {
            sequence: 1,
            action: 'code',
            description: 'Split data and train model',
            codePattern: `# Model Training Setup
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Prepare features and target
X = df.drop('{{target_column}}', axis=1)
y = df['{{target_column}}']

# Split the data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {X_train.shape}")
print(f"Test set: {X_test.shape}")
print(f"Target distribution in train: {y_train.value_counts().to_dict()}")`,
            expectedOutput: 'Data split information and target distribution'
          },
          {
            sequence: 2,
            action: 'code',
            description: 'Train and evaluate model',
            codePattern: `# Model Training and Basic Evaluation
# Assuming model is already defined
model.fit(X_train, y_train)

# Predictions
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, 'predict_proba') else None

# Basic metrics
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, average='weighted')
recall = recall_score(y_test, y_pred, average='weighted')
f1 = f1_score(y_test, y_pred, average='weighted')

print("Model Performance Metrics:")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1-Score: {f1:.4f}")

if y_pred_proba is not None:
    auc_score = roc_auc_score(y_test, y_pred_proba)
    print(f"AUC-ROC: {auc_score:.4f}")`,
            expectedOutput: 'Model performance metrics including accuracy, precision, recall, F1-score, and AUC'
          },
          {
            sequence: 3,
            action: 'code',
            description: 'Generate confusion matrix',
            codePattern: `# Confusion Matrix Visualization
cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
            xticklabels=model.classes_ if hasattr(model, 'classes_') else ['Class 0', 'Class 1'],
            yticklabels=model.classes_ if hasattr(model, 'classes_') else ['Class 0', 'Class 1'])
plt.title('Confusion Matrix')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.show()

# Classification report
print("\\nDetailed Classification Report:")
print(classification_report(y_test, y_pred))`,
            expectedOutput: 'Confusion matrix heatmap and detailed classification report'
          },
          {
            sequence: 4,
            action: 'code',
            description: 'ROC curve and feature importance',
            codePattern: `# ROC Curve (for binary classification)
if y_pred_proba is not None and len(np.unique(y_test)) == 2:
    fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)
    
    plt.figure(figsize=(12, 5))
    
    # ROC Curve
    plt.subplot(1, 2, 1)
    plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {auc_score:.2f})')
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve')
    plt.legend(loc="lower right")
    
    # Feature Importance (if available)
    plt.subplot(1, 2, 2)
    if hasattr(model, 'feature_importances_'):
        feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=True)
        
        plt.barh(range(len(feature_importance)), feature_importance['importance'])
        plt.yticks(range(len(feature_importance)), feature_importance['feature'])
        plt.xlabel('Feature Importance')
        plt.title('Feature Importance')
    else:
        plt.text(0.5, 0.5, 'Feature importance\\nnot available for this model', 
                ha='center', va='center', transform=plt.gca().transAxes)
    
    plt.tight_layout()
    plt.show()`,
            expectedOutput: 'ROC curve with AUC score and feature importance plot'
          }
        ],
        parameters: [
          {
            name: 'target_column',
            type: 'string',
            description: 'Name of the target variable column',
            validation: ['column_exists']
          },
          {
            name: 'test_size',
            type: 'number',
            description: 'Proportion of data to use for testing',
            defaultValue: 0.2,
            validation: ['between_0_and_1']
          }
        ]
      }
    ];
  }

  /**
   * Analyze sessions to identify procedural patterns
   */
  async identifyPatternsFromSessions(sessions) {
    const discoveredPatterns = [];
    
    // Group sessions by similar characteristics
    const sessionGroups = this.groupSessionsByContext(sessions);
    
    for (const group of sessionGroups) {
      // Extract action sequences
      const sequences = this.extractActionSequences(group);
      
      // Find common patterns
      const patterns = this.findCommonSequences(sequences);
      
      // Generate pattern definitions
      for (const pattern of patterns) {
        if (pattern.frequency >= 3 && pattern.successRate >= 0.7) {
          discoveredPatterns.push(this.createPatternDefinition(pattern, group));
        }
      }
    }
    
    return [...this.builtInPatterns, ...discoveredPatterns];
  }

  /**
   * Get suggestions for current session context
   */
  getSuggestionsForContext(currentSession, allPatterns = null) {
    const patterns = allPatterns || this.builtInPatterns;
    const suggestions = [];
    
    const context = this.analyzeSessionContext(currentSession);
    
    for (const pattern of patterns) {
      const relevance = this.calculateRelevance(context, pattern);
      
      if (relevance > 0.5) {
        suggestions.push({
          pattern,
          relevance,
          reason: this.generateSuggestionReason(context, pattern),
          estimatedTime: pattern.avgDuration,
          parameters: this.suggestParameters(context, pattern)
        });
      }
    }
    
    return suggestions.sort((a, b) => b.relevance - a.relevance).slice(0, 5);
  }

  /**
   * Generate executable notebook from pattern
   */
  generateExecutableNotebook(pattern, parameters = {}) {
    const cells = [];
    
    // Add title and description
    cells.push({
      cell_type: 'markdown',
      source: [
        `# ${pattern.name}\n`,
        `\n`,
        `${pattern.description}\n`,
        `\n`,
        `**Generated by PKL Extension** | Success Rate: ${(pattern.successRate * 100).toFixed(1)}% | Est. Time: ~${pattern.avgDuration}min\n`,
        `\n`,
        `---\n`
      ]
    });

    // Add imports cell
    cells.push({
      cell_type: 'code',
      source: [
        '# Required imports\n',
        'import pandas as pd\n',
        'import numpy as np\n',
        'import matplotlib.pyplot as plt\n',
        'import seaborn as sns\n',
        '\n',
        '# Configure display options\n',
        'pd.set_option("display.max_columns", None)\n',
        'plt.style.use("default")\n',
        'sns.set_palette("husl")\n'
      ]
    });

    // Add step cells
    for (const step of pattern.steps) {
      if (step.action === 'code') {
        // Add step description
        cells.push({
          cell_type: 'markdown',
          source: [
            `## Step ${step.sequence}: ${step.description}\n`,
            `\n`,
            `Expected output: ${step.expectedOutput}\n`
          ]
        });

        // Add parameterized code
        const parameterizedCode = this.parameterizeCode(step.codePattern, parameters);
        cells.push({
          cell_type: 'code',
          source: parameterizedCode.split('\n'),
          metadata: {
            step_id: step.sequence,
            expected_output: step.expectedOutput
          }
        });
      }
    }

    // Add conclusion
    cells.push({
      cell_type: 'markdown',
      source: [
        `## Summary\n`,
        `\n`,
        `This notebook was generated using the "${pattern.name}" procedure pattern.\n`,
        `\n`,
        `**Next steps you might consider:**\n`,
        `- Review the results and identify any data quality issues\n`,
        `- Adjust parameters based on your specific use case\n`,
        `- Explore additional analyses based on the insights gained\n`,
        `\n`,
        `*Generated by PKL Extension - Procedural Knowledge Library*\n`
      ]
    });

    return {
      cells,
      metadata: {
        kernelspec: {
          display_name: 'Python 3',
          language: 'python',
          name: 'python3'
        },
        language_info: {
          name: 'python',
          version: '3.8.0'
        },
        pkl_metadata: {
          pattern_id: pattern.id,
          pattern_name: pattern.name,
          generated_at: new Date().toISOString(),
          parameters: parameters
        }
      },
      nbformat: 4,
      nbformat_minor: 4
    };
  }

  // Helper methods
  groupSessionsByContext(sessions) {
    const groups = {};
    
    for (const session of sessions) {
      const key = `${session.intent}_${this.getFileType(session.currentFile)}`;
      if (!groups[key]) groups[key] = [];
      groups[key].push(session);
    }
    
    return Object.values(groups);
  }

  extractActionSequences(sessions) {
    return sessions.map(session => ({
      sessionId: session.id,
      actions: this.parseCodeActions(session.codeDeltas || []),
      outcome: session.outcome,
      duration: session.duration || 0
    }));
  }

  parseCodeActions(codeDeltas) {
    const actions = [];
    
    for (const delta of codeDeltas) {
      const code = (delta.afterContent || '').toLowerCase();
      
      if (code.includes('pd.read_') || code.includes('.read_csv')) {
        actions.push({ type: 'data_loading', confidence: 0.9 });
      } else if (code.includes('.describe()') || code.includes('.info()')) {
        actions.push({ type: 'data_exploration', confidence: 0.95 });
      } else if (code.includes('.plot') || code.includes('plt.') || code.includes('sns.')) {
        actions.push({ type: 'visualization', confidence: 0.9 });
      } else if (code.includes('.fit(') || code.includes('model')) {
        actions.push({ type: 'modeling', confidence: 0.85 });
      }
    }
    
    return actions;
  }

  analyzeSessionContext(session) {
    return {
      intent: session.intent,
      phase: session.phase,
      fileType: this.getFileType(session.currentFile),
      recentActions: this.parseCodeActions(session.codeDeltas || []).slice(-5),
      hasData: this.hasDataLoading(session),
      hasModel: this.hasModeling(session)
    };
  }

  calculateRelevance(context, pattern) {
    let score = 0;
    
    // Intent matching - handle different intent formats
    const normalizedIntent = context.intent?.replace('_', '') || '';
    const normalizedCategory = pattern.category?.replace('_', '') || '';
    
    if (normalizedCategory === normalizedIntent || 
        (context.intent === 'data_exploration' && pattern.category === 'exploration') ||
        (context.intent === 'explore' && pattern.category === 'exploration')) {
      score += 0.4;
    }
    
    // Context matching
    if (pattern.contexts.includes(context.fileType)) score += 0.3;
    
    // Trigger matching - check for data loading patterns
    const hasDataLoading = context.recentActions.some(action => action.type === 'data_loading');
    const hasVisualization = context.recentActions.some(action => action.type === 'visualization');
    
    if (pattern.id === 'quick-eda' && (hasDataLoading || hasVisualization || context.hasData)) {
      score += 0.3;
    }
    
    // File type bonus for notebooks
    if (context.fileType === 'notebook' && pattern.contexts.includes('notebook')) {
      score += 0.2;
    }
    
    return Math.min(score, 1.0);
  }

  generateSuggestionReason(context, pattern) {
    if (pattern.category === context.intent) {
      return `Perfect match for ${context.intent} tasks`;
    } else if (pattern.contexts.includes(context.fileType)) {
      return `Commonly used with ${context.fileType} files`;
    } else {
      return `Suggested based on your recent actions`;
    }
  }

  suggestParameters(context, pattern) {
    const suggestions = {};
    
    for (const param of pattern.parameters) {
      if (param.name === 'dataset_path' && context.hasData) {
        suggestions[param.name] = 'df';  // Assume dataframe is already loaded
      } else if (param.name === 'target_column' && context.hasModel) {
        suggestions[param.name] = 'target';  // Common target column name
      } else {
        suggestions[param.name] = param.defaultValue;
      }
    }
    
    return suggestions;
  }

  parameterizeCode(codePattern, parameters) {
    let parameterized = codePattern;
    
    for (const [key, value] of Object.entries(parameters)) {
      const placeholder = `{{${key}}}`;
      parameterized = parameterized.replace(new RegExp(placeholder, 'g'), value);
    }
    
    return parameterized;
  }

  getFileType(filePath) {
    if (!filePath) return 'unknown';
    const ext = filePath.split('.').pop().toLowerCase();
    return ext === 'ipynb' ? 'notebook' : ext;
  }

  hasDataLoading(session) {
    return (session.codeDeltas || []).some(delta => 
      (delta.afterContent || '').toLowerCase().includes('pd.read_')
    );
  }

  hasModeling(session) {
    return (session.codeDeltas || []).some(delta => 
      (delta.afterContent || '').toLowerCase().includes('model')
    );
  }

  // Additional helper methods for pattern discovery
  findCommonSequences(sequences) {
    // Simplified pattern discovery - in a full implementation, this would use
    // more sophisticated sequence mining algorithms
    const patterns = [];
    const actionCounts = {};
    
    // Count action sequences
    for (const seq of sequences) {
      const actionTypes = seq.actions.map(a => a.type);
      const seqKey = actionTypes.join(' -> ');
      
      if (!actionCounts[seqKey]) {
        actionCounts[seqKey] = {
          sequence: actionTypes,
          frequency: 0,
          outcomes: [],
          durations: []
        };
      }
      
      actionCounts[seqKey].frequency++;
      actionCounts[seqKey].outcomes.push(seq.outcome);
      actionCounts[seqKey].durations.push(seq.duration);
    }
    
    // Convert to patterns
    for (const [seqKey, data] of Object.entries(actionCounts)) {
      if (data.frequency >= 2) { // Minimum frequency threshold
        const successfulOutcomes = data.outcomes.filter(o => o === 'success').length;
        const successRate = successfulOutcomes / data.outcomes.length;
        const avgDuration = data.durations.reduce((a, b) => a + b, 0) / data.durations.length;
        
        patterns.push({
          sequence: data.sequence,
          frequency: data.frequency,
          successRate,
          avgDuration: Math.round(avgDuration / 60000) // Convert to minutes
        });
      }
    }
    
    return patterns;
  }

  createPatternDefinition(pattern, sessionGroup) {
    const actionSequence = pattern.sequence.join(' -> ');
    
    return {
      id: `discovered-${actionSequence.replace(/[^a-z0-9]/gi, '-').toLowerCase()}`,
      name: `${actionSequence} Pattern`,
      category: this.inferCategoryFromActions(pattern.sequence),
      description: `Discovered pattern: ${actionSequence}`,
      successRate: pattern.successRate,
      avgDuration: pattern.avgDuration,
      contexts: [...new Set(sessionGroup.map(s => this.getFileType(s.currentFile)))],
      triggers: pattern.sequence.slice(0, 2), // First two actions as triggers
      steps: this.generateStepsFromSequence(pattern.sequence),
      parameters: []
    };
  }

  inferCategoryFromActions(sequence) {
    if (sequence.includes('data_loading') && sequence.includes('data_exploration')) {
      return 'exploration';
    } else if (sequence.includes('modeling')) {
      return 'modeling';
    } else if (sequence.includes('visualization')) {
      return 'visualization';
    } else {
      return 'analysis';
    }
  }

  generateStepsFromSequence(sequence) {
    return sequence.map((action, index) => ({
      sequence: index + 1,
      action: 'code',
      description: this.getActionDescription(action),
      codePattern: this.getActionCodePattern(action),
      expectedOutput: this.getActionExpectedOutput(action)
    }));
  }

  getActionDescription(action) {
    const descriptions = {
      'data_loading': 'Load dataset from file',
      'data_exploration': 'Explore dataset characteristics',
      'visualization': 'Create data visualizations',
      'modeling': 'Build and train model',
      'validation': 'Validate model performance'
    };
    return descriptions[action] || `Perform ${action} operation`;
  }

  getActionCodePattern(action) {
    const patterns = {
      'data_loading': 'df = pd.read_csv("{{dataset_path}}")',
      'data_exploration': 'df.describe()\ndf.info()',
      'visualization': 'df.plot()\nplt.show()',
      'modeling': 'model.fit(X_train, y_train)',
      'validation': 'model.score(X_test, y_test)'
    };
    return patterns[action] || `# ${action} code here`;
  }

  getActionExpectedOutput(action) {
    const outputs = {
      'data_loading': 'Dataset loaded successfully',
      'data_exploration': 'Dataset summary statistics',
      'visualization': 'Data visualization plots',
      'modeling': 'Trained model object',
      'validation': 'Model performance metrics'
    };
    return outputs[action] || `Output from ${action}`;
  }
}

module.exports = ProcedurePatternService;
