import React from 'react';
import './ErrorState.css';

interface ErrorStateProps {
  error: string;
  onRetry: () => void;
  showIcon?: boolean;
  title?: string;
}

/**
 * Error state component with retry functionality
 * Shows when data loading fails
 */
export const ErrorState: React.FC<ErrorStateProps> = ({ 
  error, 
  onRetry, 
  showIcon = true, 
  title = 'Something went wrong' 
}) => {
  return (
    <div className="error-state">
      <div className="error-content">
        {showIcon && <div className="error-icon">⚠</div>}
        <h3>{title}</h3>
        <p className="error-message">{error}</p>
        <div className="error-actions">
          <button className="retry-button" onClick={onRetry}>
            Try Again
          </button>
        </div>
      </div>
    </div>
  );
};
