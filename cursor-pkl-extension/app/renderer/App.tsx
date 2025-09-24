import React, { useState, useEffect } from 'react';
import { PKLSession, SearchFilters } from '../config/types';
import { SessionList } from './components/SessionList';
import { SessionDetail } from './components/SessionDetail';
import { SearchBar } from './components/SearchBar';
import { FilterBar } from './components/FilterBar';
import { Header } from './components/Header';
import { LoadingState } from './components/LoadingState';
import { ErrorState } from './components/ErrorState';
import { ExportModal } from './components/ExportModal';
import { CONFIG } from '../config/constants';
import './App.css';

/**
 * Main App component for the PKL Widget
 * Manages state and coordinates between components
 */
export const App: React.FC = () => {
  const [sessions, setSessions] = useState<PKLSession[]>([]);
  const [selectedSession, setSelectedSession] = useState<PKLSession | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [filters, setFilters] = useState<SearchFilters>({});
  const [filteredSessions, setFilteredSessions] = useState<PKLSession[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isExpanded, setIsExpanded] = useState(false);
  const [isExportModalOpen, setIsExportModalOpen] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [exportProgress, setExportProgress] = useState(0);

  useEffect(() => {
    loadSessions();
    setupEventListeners();
  }, []);

  // Filter sessions based on search query and filters
  useEffect(() => {
    let filtered = [...sessions];

    // Apply search query
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(session => 
        session.currentFile?.toLowerCase().includes(query) ||
        session.intent?.toLowerCase().includes(query) ||
        session.outcome?.toLowerCase().includes(query) ||
        session.annotations?.some(ann => ann.content.toLowerCase().includes(query))
      );
    }

    // Apply filters
    if (filters.intent) {
      filtered = filtered.filter(session => session.intent === filters.intent);
    }
    if (filters.outcome) {
      filtered = filtered.filter(session => session.outcome === filters.outcome);
    }
    if (filters.dateRange) {
      const now = new Date();
      const days = filters.dateRange === 'today' ? 1 : 
                   filters.dateRange === 'week' ? 7 : 
                   filters.dateRange === 'month' ? 30 : 365;
      const cutoff = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
      filtered = filtered.filter(session => new Date(session.timestamp) >= cutoff);
    }

    setFilteredSessions(filtered);
  }, [sessions, searchQuery, filters]);

  const loadSessions = async () => {
    try {
      setIsLoading(true);
      setError(null);
      
      const result = await window.electronAPI.getSessions();
      if (result.success && result.data) {
        setSessions(result.data);
      } else {
        setError(result.error || 'Failed to load sessions');
      }
    } catch (err) {
      setError('Failed to load sessions');
    } finally {
      setIsLoading(false);
    }
  };

  const setupEventListeners = () => {
    window.electronAPI.onSessionUpdate((updatedSessions) => {
      setSessions(updatedSessions);
    });

    window.electronAPI.onFileChange((changes) => {
      // Handle file changes - could trigger session updates
      console.log('File changes detected:', changes);
    });
  };

  const handleSearch = async (query: string) => {
    setSearchQuery(query);
    
    if (!query.trim()) {
      loadSessions();
      return;
    }

    try {
      setIsLoading(true);
      const result = await window.electronAPI.searchSessions(query);
      if (result.success && result.data) {
        setSessions(result.data);
      } else {
        setError(result.error || 'Search failed');
      }
    } catch (err) {
      setError('Search failed');
    } finally {
      setIsLoading(false);
    }
  };

  const handleSessionSelect = async (sessionId: string) => {
    try {
      const result = await window.electronAPI.getSessionDetail(sessionId);
      if (result.success && result.data) {
        setSelectedSession(result.data);
        setIsExpanded(true);
      } else {
        setError(result.error || 'Failed to load session details');
      }
    } catch (err) {
      setError('Failed to load session details');
    }
  };

  const handleReturnToContext = async (sessionId: string) => {
    try {
      const result = await window.electronAPI.returnToContext(sessionId);
      if (!result.success) {
        setError(result.error || 'Failed to return to context');
      }
    } catch (err) {
      setError('Failed to return to context');
    }
  };

  const handleResumeSession = async (sessionId: string) => {
    try {
      const result = await window.electronAPI.resumeSession(sessionId);
      if (!result.success) {
        setError(result.error || 'Failed to resume session');
      }
    } catch (err) {
      setError('Failed to resume session');
    }
  };

  const handleAddAnnotation = async (sessionId: string, content: string) => {
    try {
      const result = await window.electronAPI.addAnnotation(sessionId, content);
      if (result.success) {
        // Refresh the selected session
        if (selectedSession?.id === sessionId) {
          handleSessionSelect(sessionId);
        }
      } else {
        setError(result.error || 'Failed to add annotation');
      }
    } catch (err) {
      setError('Failed to add annotation');
    }
  };

  const handleExportSession = async (sessionId: string, format: 'json' | 'markdown' | 'csv') => {
    try {
      const result = await window.electronAPI.exportSession(sessionId, { format, includeCode: true, includeMetadata: true });
      if (result.success) {
        // Show success message or open file location
        console.log('Export successful:', result.data?.path);
      } else {
        setError(result.error || 'Export failed');
      }
    } catch (err) {
      setError('Export failed');
    }
  };

  const handleExportAll = async (options: { format: 'json' | 'markdown' | 'csv'; includeCode: boolean; includeMetadata: boolean; includeConversations: boolean; includeFileChanges: boolean }) => {
    try {
      setIsExporting(true);
      setExportProgress(0);
      
      // Simulate progress
      const progressInterval = setInterval(() => {
        setExportProgress(prev => Math.min(prev + 10, 90));
      }, 200);

      const result = await window.electronAPI.exportAll(options);
      
      clearInterval(progressInterval);
      setExportProgress(100);
      
      if (result.success) {
        console.log('Export all successful:', result.data?.path);
        setTimeout(() => {
          setIsExportModalOpen(false);
          setIsExporting(false);
          setExportProgress(0);
        }, 1000);
      } else {
        setError(result.error || 'Export all failed');
        setIsExporting(false);
        setExportProgress(0);
      }
    } catch (err) {
      setError('Export all failed');
      setIsExporting(false);
      setExportProgress(0);
    }
  };

  const handleRefreshSessions = async () => {
    try {
      setIsLoading(true);
      const result = await window.electronAPI.refreshSessions();
      if (result.success && result.data) {
        setSessions(result.data);
      } else {
        setError(result.error || 'Failed to refresh sessions');
      }
    } catch (err) {
      setError('Failed to refresh sessions');
    } finally {
      setIsLoading(false);
    }
  };

  const handleCloseDetail = () => {
    setSelectedSession(null);
    setIsExpanded(false);
  };

  const handleRetry = () => {
    setError(null);
    loadSessions();
  };

  if (error) {
    return (
      <div className="app">
        <ErrorState error={error} onRetry={handleRetry} />
      </div>
    );
  }

  return (
    <div className={`app ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <Header 
        isExpanded={isExpanded}
        onToggle={() => setIsExpanded(!isExpanded)}
        currentSession={sessions[0]} // Most recent session
        onRefresh={handleRefreshSessions}
        onExport={() => setIsExportModalOpen(true)}
      />
      
      {isExpanded && (
        <>
          <SearchBar 
            query={searchQuery}
            onSearch={handleSearch}
            onClear={() => handleSearch('')}
          />
          
          <FilterBar
            filters={filters}
            onFiltersChange={setFilters}
            sessionCount={filteredSessions.length}
            totalCount={sessions.length}
          />
          
          {isLoading ? (
            <LoadingState />
          ) : (
            <div className="content">
              <SessionList
                sessions={filteredSessions}
                selectedSessionId={selectedSession?.id}
                onSessionSelect={handleSessionSelect}
                onReturnToContext={handleReturnToContext}
                onResumeSession={handleResumeSession}
              />
              
              {selectedSession && (
                <SessionDetail
                  session={selectedSession}
                  onClose={handleCloseDetail}
                  onAddAnnotation={handleAddAnnotation}
                  onExportSession={handleExportSession}
                />
              )}
            </div>
          )}
        </>
      )}
      
      <ExportModal
        isOpen={isExportModalOpen}
        onClose={() => setIsExportModalOpen(false)}
        onExport={handleExportSession}
        onExportAll={handleExportAll}
        isExporting={isExporting}
        exportProgress={exportProgress}
      />
    </div>
  );
};
