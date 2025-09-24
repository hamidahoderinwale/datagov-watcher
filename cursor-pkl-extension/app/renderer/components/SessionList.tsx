import React from 'react';
import { List, AutoSizer } from 'react-virtualized';
import { PKLSession } from '../../config/types';
import { SessionListItem } from './SessionListItem';
import './SessionList.css';

interface SessionListProps {
  sessions: PKLSession[];
  selectedSessionId?: string;
  onSessionSelect: (sessionId: string) => void;
  onReturnToContext: (sessionId: string) => void;
  onResumeSession: (sessionId: string) => void;
}

/**
 * Virtualized session list component
 * Efficiently renders large numbers of sessions
 */
export const SessionList: React.FC<SessionListProps> = ({
  sessions,
  selectedSessionId,
  onSessionSelect,
  onReturnToContext,
  onResumeSession
}) => {
  const rowRenderer = ({ index, key, style }: any) => {
    const session = sessions[index];
    if (!session) return null;

    return (
      <div key={key} style={style}>
        <SessionListItem
          session={session}
          isSelected={session.id === selectedSessionId}
          onSelect={() => onSessionSelect(session.id)}
          onReturnToContext={() => onReturnToContext(session.id)}
          onResumeSession={() => onResumeSession(session.id)}
        />
      </div>
    );
  };

  if (sessions.length === 0) {
    return (
      <div className="session-list-empty">
        <div className="empty-message">
          <h3>No sessions found</h3>
          <p>Start coding in Cursor to see your sessions here</p>
        </div>
      </div>
    );
  }

  return (
    <div className="session-list">
      <div className="session-list-header">
        <h3>Recent Sessions</h3>
        <span className="session-count">{sessions.length} sessions</span>
      </div>
      
      <div className="session-list-content">
        <AutoSizer>
          {({ height, width }) => (
            <List
              height={height}
              width={width}
              rowCount={sessions.length}
              rowHeight={48}
              rowRenderer={rowRenderer}
              overscanRowCount={5}
            />
          )}
        </AutoSizer>
      </div>
    </div>
  );
};
