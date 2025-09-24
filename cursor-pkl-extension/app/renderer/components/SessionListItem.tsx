import React from 'react';
import { PKLSession } from '../../config/types';
import './SessionListItem.css';

interface SessionListItemProps {
  session: PKLSession;
  isSelected: boolean;
  onSelect: () => void;
  onReturnToContext: () => void;
  onResumeSession: () => void;
}

/**
 * Individual session list item component
 * Shows session summary with actions
 */
export const SessionListItem: React.FC<SessionListItemProps> = ({
  session,
  isSelected,
  onSelect,
  onReturnToContext,
  onResumeSession
}) => {
  const formatTime = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(diff / 3600000);
    const days = Math.floor(diff / 86400000);

    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m ago`;
    if (hours < 24) return `${hours}h ago`;
    return `${days}d ago`;
  };

  const getOutcomeIcon = () => {
    switch (session.outcome) {
      case 'success': return '✅';
      case 'stuck': return '✖';
      case 'in-progress': return '…';
      default: return '';
    }
  };

  const getOutcomeClass = () => {
    switch (session.outcome) {
      case 'success': return 'success';
      case 'stuck': return 'stuck';
      case 'in-progress': return 'in-progress';
      default: return '';
    }
  };

  const handleContextMenu = (event: React.MouseEvent) => {
    event.preventDefault();
    // Could show context menu with additional actions
  };

  const handleDoubleClick = () => {
    onReturnToContext();
  };

  return (
    <div 
      className={`session-item ${isSelected ? 'selected' : ''}`}
      onClick={onSelect}
      onDoubleClick={handleDoubleClick}
      onContextMenu={handleContextMenu}
    >
      <div className="session-item-content">
        <span className="session-bullet">•</span>
        <span className="session-time">{formatTime(session.timestamp)}</span>
        <span className="session-intent">{session.intent}</span>
        <span className="session-file">{session.currentFile?.split('/').pop() || 'No file'}</span>
        <span className={`session-outcome ${getOutcomeClass()}`}>
          {getOutcomeIcon()}
        </span>
      </div>
      
      {isSelected && (
        <div className="session-actions">
          <button 
            className="action-button return-button"
            onClick={(e) => {
              e.stopPropagation();
              onReturnToContext();
            }}
            title="Return to context"
          >
            Return
          </button>
          <button 
            className="action-button resume-button"
            onClick={(e) => {
              e.stopPropagation();
              onResumeSession();
            }}
            title="Resume session"
          >
            Resume
          </button>
        </div>
      )}
    </div>
  );
};
