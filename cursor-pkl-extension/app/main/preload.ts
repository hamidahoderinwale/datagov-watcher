import { contextBridge, ipcRenderer } from 'electron';
import { CONFIG } from '../config/constants';
import { PKLSession, SearchFilters, ExportOptions } from '../config/types';

/**
 * Preload script for secure IPC communication
 * Exposes safe APIs to the renderer process
 */
const electronAPI = {
  // Session management
  getSessions: (): Promise<{ success: boolean; data?: PKLSession[]; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.GET_SESSIONS),

  getSessionDetail: (sessionId: string): Promise<{ success: boolean; data?: PKLSession; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.GET_SESSION_DETAIL, sessionId),

  searchSessions: (query: string): Promise<{ success: boolean; data?: PKLSession[]; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.SEARCH_SESSIONS, query),

  // Context actions
  returnToContext: (sessionId: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.RETURN_TO_CONTEXT, sessionId),

  resumeSession: (sessionId: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.RESUME_SESSION, sessionId),

  // Annotations
  addAnnotation: (sessionId: string, content: string): Promise<{ success: boolean; data?: any; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.ADD_ANNOTATION, sessionId, content),

  // Export
  exportSession: (sessionId: string, options: ExportOptions): Promise<{ success: boolean; data?: { path: string }; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.EXPORT_SESSION, sessionId, options),

  exportAll: (options: ExportOptions): Promise<{ success: boolean; data?: { path: string }; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.EXPORT_ALL, options),

  getExports: (): Promise<{ success: boolean; data?: any[]; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.GET_EXPORTS),

  deleteExport: (filename: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.DELETE_EXPORT, filename),

  // Stats
  getStats: (): Promise<{ success: boolean; data?: any; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.GET_STATS),

  // Refresh
  refreshSessions: (): Promise<{ success: boolean; data?: PKLSession[]; error?: string }> =>
    ipcRenderer.invoke(CONFIG.IPC_CHANNELS.REFRESH_SESSIONS),

  // Event listeners
  onSessionUpdate: (callback: (sessions: PKLSession[]) => void) => {
    ipcRenderer.on(CONFIG.IPC_CHANNELS.SESSION_UPDATE, (_, sessions) => callback(sessions));
  },

  onFileChange: (callback: (changes: any[]) => void) => {
    ipcRenderer.on(CONFIG.IPC_CHANNELS.FILE_CHANGE, (_, changes) => callback(changes));
  },

  // Remove listeners
  removeAllListeners: (channel: string) => {
    ipcRenderer.removeAllListeners(channel);
  }
};

// Expose the API to the renderer process
contextBridge.exposeInMainWorld('electronAPI', electronAPI);

// Type declaration for the global window object
declare global {
  interface Window {
    electronAPI: typeof electronAPI;
  }
}
