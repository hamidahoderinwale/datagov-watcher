import React, { useState } from 'react';
import { PKLSession } from '../../config/types';
import { AnnotationModal } from './AnnotationModal';
import './SessionDetail.css';

interface SessionDetailProps {
  session: PKLSession;
  onClose: () => void;
  onAddAnnotation: (sessionId: string, content: string) => void;
  onExportSession: (sessionId: string, format: 'json' | 'markdown' | 'csv') => void;
}

/**
 * Detailed session view component
 * Shows full session context, file changes, and annotations
 */
export const SessionDetail: React.FC<SessionDetailProps> = ({
  session,
  onClose,
  onAddAnnotation,
  onExportSession
}) => {
  const [isAnnotationModalOpen, setIsAnnotationModalOpen] = useState(false);

  const formatTimestamp = (date: Date) => {
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const handleAddAnnotation = (content: string, tags: string[]) => {
    onAddAnnotation(session.id, content);
    setIsAnnotationModalOpen(false);
  };


  return (
    <div className="session-detail">
      <div className="session-detail-header">
        <div className="session-detail-title">
          <h3>Session: {formatTimestamp(session.timestamp)} {session.currentFile?.split('/').pop() || 'No file'}</h3>
        </div>
        <button className="close-button" onClick={onClose}>
          ×
        </button>
      </div>

      <div className="session-detail-content">
        <div className="session-info">
          <div className="info-row">
            <span className="info-label">Intent:</span>
            <span className="info-value">{session.intent}</span>
          </div>
          <div className="info-row">
            <span className="info-label">Outcome:</span>
            <span className={`info-value outcome-${session.outcome || 'none'}`}>
              {session.outcome || 'Unknown'}
            </span>
          </div>
        </div>

        <div className="conversation">
          <h4>Conversation</h4>
          <div className="conversation-messages">
            <div className="message user-message">
              <span className="message-role">User:</span>
              <span className="message-content">"Why is X failing?"</span>
            </div>
            <div className="message assistant-message">
              <span className="message-role">Assistant:</span>
              <span className="message-content">"Try Y..."</span>
            </div>
          </div>
        </div>

        <div className="session-actions">
          <button 
            className="action-button annotation-button"
            onClick={() => setIsAnnotationModalOpen(true)}
          >
            Add Annotation
          </button>
          <button 
            className="action-button export-button"
            onClick={() => onExportSession(session.id, 'json')}
          >
            Export Session
          </button>
        </div>
      </div>

      <AnnotationModal
        isOpen={isAnnotationModalOpen}
        onClose={() => setIsAnnotationModalOpen(false)}
        onSave={handleAddAnnotation}
        sessionId={session.id}
        existingAnnotations={session.annotations || []}
      />
    </div>
  );
};
