import { app } from 'electron';
import path from 'path';

/**
 * Application configuration constants
 * Centralized configuration for paths, limits, and feature flags
 */

export const CONFIG = {
  // Data paths
  CURSOR_DB_PATH: path.join(
    process.env.HOME || '',
    'Library/Application Support/Cursor/User/globalStorage'
  ),
  PKL_DATA_PATH: path.join(process.env.HOME || '', '.pkl/data'),
  SQLITE_DB_PATH: path.join(process.env.HOME || '', '.pkl/sessions.db'),
  
  // Performance limits
  MAX_SESSIONS_IN_MEMORY: 1000,
  SEARCH_RESULTS_LIMIT: 100,
  VIRTUAL_SCROLL_ITEM_HEIGHT: 48,
  
  // UI dimensions
  WIDGET_COLLAPSED_WIDTH: 280,
  WIDGET_COLLAPSED_HEIGHT: 48,
  WIDGET_EXPANDED_WIDTH: 320,
  WIDGET_EXPANDED_HEIGHT: 480,
  WIDGET_MAX_HEIGHT: 800,
  
  // Session detection
  SESSION_TIMEOUT_MINUTES: 30,
  FILE_CHANGE_CORRELATION_WINDOW_MS: 120000, // 2 minutes
  
  // Data collection levels
  DATA_COLLECTION_LEVELS: {
    MINIMAL: 'minimal',
    STANDARD: 'standard',
    COMPREHENSIVE: 'comprehensive'
  } as const,
  
  // Intent classification keywords
  INTENT_KEYWORDS: {
    debug: ['fix', 'error', 'traceback', 'fail', 'debug', 'bug', 'issue'],
    implement: ['implement', 'add', 'create', 'build', 'write', 'code'],
    explore: ['explore', 'try', 'can i', 'how do i', 'what is', 'investigate'],
    refactor: ['refactor', 'rename', 'clean', 'optimize', 'improve', 'restructure'],
    document: ['doc', 'comment', 'explain', 'what does', 'describe', 'documentation']
  },
  
  // Outcome detection patterns
  SUCCESS_PATTERNS: [
    'test passed',
    'success',
    'process finished',
    'no errors',
    'completed successfully',
    'build successful'
  ],
  
  STUCK_PATTERNS: [
    'exception',
    'traceback',
    'error',
    'failed',
    'build failed',
    'test failed'
  ],
  
  // Export formats
  EXPORT_FORMATS: {
    JSON: 'json',
    MARKDOWN: 'markdown',
    CSV: 'csv'
  } as const,
  
  // IPC channels
  IPC_CHANNELS: {
    GET_SESSIONS: 'get-sessions',
    GET_SESSION_DETAIL: 'get-session-detail',
    SEARCH_SESSIONS: 'search-sessions',
    RETURN_TO_CONTEXT: 'return-to-context',
    RESUME_SESSION: 'resume-session',
    ADD_ANNOTATION: 'add-annotation',
    EXPORT_SESSION: 'export-session',
    EXPORT_ALL: 'export-all',
    GET_EXPORTS: 'get-exports',
    DELETE_EXPORT: 'delete-export',
    GET_STATS: 'get-stats',
    REFRESH_SESSIONS: 'refresh-sessions',
    SESSION_UPDATE: 'session-update',
    FILE_CHANGE: 'file-change'
  }
} as const;

export type DataCollectionLevel = typeof CONFIG.DATA_COLLECTION_LEVELS[keyof typeof CONFIG.DATA_COLLECTION_LEVELS];
export type ExportFormat = typeof CONFIG.EXPORT_FORMATS[keyof typeof CONFIG.EXPORT_FORMATS];
export type IntentType = keyof typeof CONFIG.INTENT_KEYWORDS;
