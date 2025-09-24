import { IntentType } from './constants';

/**
 * Core data model interfaces for PKL Extension
 * Defines the structure of sessions, events, and UI state
 */

export interface PKLSession {
  id: string;
  timestamp: Date;
  endTime?: Date;
  intent: IntentType;
  phase: 'start' | 'middle' | 'success' | 'stuck';
  outcome?: 'success' | 'stuck' | 'in-progress';
  confidence: number;
  
  // Context data
  currentFile?: string;
  cursorPosition?: Position;
  selectedText?: string;
  
  // File changes
  fileChanges: FileChangeEvent[];
  codeDeltas: CodeDelta[];
  
  // Events and outcomes
  linkedEvents: LinkedEvent[];
  
  // Metadata
  privacyMode: boolean;
  userConsent: boolean;
  dataRetention: Date;
  annotations: Annotation[];
}

export interface ConversationEvent {
  id: string;
  sessionId: string;
  timestamp: Date;
  role: 'user' | 'assistant';
  content: string;
  metadata?: Record<string, any>;
  referencedFiles: string[];
  codeBlocks: CodeBlock[];
}

export interface FileChangeEvent {
  id: string;
  sessionId: string;
  timestamp: Date;
  filePath: string;
  changeType: 'created' | 'modified' | 'deleted';
  beforeSnippet?: string;
  afterSnippet?: string;
  lineRange: {
    start: number;
    end: number;
  };
  gitHash?: string;
}

export interface CodeDelta {
  id: string;
  sessionId: string;
  timestamp: Date;
  filePath: string;
  beforeContent: string;
  afterContent: string;
  diff: string;
  changeType: 'added' | 'modified' | 'deleted';
  lineCount: number;
}

export interface LinkedEvent {
  id: string;
  sessionId: string;
  timestamp: Date;
  type: 'code_run' | 'test_result' | 'error' | 'success' | 'file_change';
  filePath?: string;
  output?: string;
  tag: string;
  classification: string;
}

export interface Annotation {
  id: string;
  sessionId: string;
  timestamp: Date;
  content: string;
  tags: string[];
}

export interface CodeBlock {
  language: string;
  content: string;
  startLine?: number;
  endLine?: number;
}

export interface Position {
  line: number;
  character: number;
}

export interface SearchFilters {
  intent?: string;
  outcome?: string;
  dateRange?: string;
  filePath?: string;
  query?: string;
}

export interface ExportOptions {
  format: 'json' | 'markdown' | 'csv';
  includeCode: boolean;
  includeMetadata: boolean;
  includeConversations: boolean;
  includeFileChanges: boolean;
  dateRange?: {
    start: Date;
    end: Date;
  };
}

export interface WidgetState {
  isExpanded: boolean;
  currentSession?: PKLSession;
  sessions: PKLSession[];
  searchQuery: string;
  filters: SearchFilters;
  isLoading: boolean;
  error?: string;
}

export interface UIConfig {
  theme: 'light' | 'dark' | 'auto';
  dataCollectionLevel: 'minimal' | 'standard' | 'comprehensive';
  autoLaunch: boolean;
  notifications: boolean;
  spotlightIndexing: boolean;
}

export interface StorageStats {
  totalSessions: number;
  totalEvents: number;
  totalFileChanges: number;
  databaseSize: number;
  lastUpdate: Date;
}
