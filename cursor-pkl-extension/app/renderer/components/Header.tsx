import React from 'react';
import { PKLSession } from '../../config/types';
import './Header.css';

interface HeaderProps {
  isExpanded: boolean;
  onToggle: () => void;
  currentSession?: PKLSession;
  onRefresh?: () => void;
  onExport?: () => void;
}

/**
 * Header component showing current session status
 * Displays collapsed state with essential info
 */
export const Header: React.FC<HeaderProps> = ({ isExpanded, onToggle, currentSession, onRefresh, onExport }) => {
  const formatTime = (date: Date) => {
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      hour12: false 
    });
  };

  const getStatusDot = () => {
    if (!currentSession) return 'gray';
    if (currentSession.outcome === 'success') return 'green';
    if (currentSession.outcome === 'stuck') return 'red';
    return 'blue';
  };

  return (
    <div className="header" onClick={onToggle}>
      <div className="header-content">
        <div className="header-left">
          <span className="app-title">PKL</span>
          {currentSession && (
            <span className="current-file">
              {currentSession.currentFile?.split('/').pop() || 'No file'}
            </span>
          )}
        </div>
        
        <div className="header-right">
          {currentSession && (
            <>
              <span className="session-time">
                {formatTime(currentSession.timestamp)}
              </span>
              <span className="session-intent">
                {currentSession.intent}
              </span>
            </>
          )}
          {onRefresh && (
            <button 
              className="refresh-btn"
              onClick={(e) => {
                e.stopPropagation();
                onRefresh();
              }}
              title="Refresh sessions"
            >
              ↻
            </button>
          )}
          {onExport && (
            <button 
              className="export-btn"
              onClick={(e) => {
                e.stopPropagation();
                onExport();
              }}
              title="Export sessions"
            >
              ⬇
            </button>
          )}
          <div 
            className={`status-dot ${getStatusDot()}`}
            title={currentSession?.outcome || 'No active session'}
          />
        </div>
      </div>
    </div>
  );
};
