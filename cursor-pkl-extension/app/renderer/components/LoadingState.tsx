import React from 'react';
import './LoadingState.css';

interface LoadingStateProps {
  message?: string;
  showSpinner?: boolean;
}

/**
 * Loading state component with skeleton placeholders
 * Shows while data is being fetched
 */
export const LoadingState: React.FC<LoadingStateProps> = ({ 
  message = 'Loading sessions...', 
  showSpinner = true 
}) => {
  return (
    <div className="loading-state">
      {showSpinner && (
        <div className="loading-spinner">
          <div className="spinner" />
        </div>
      )}
      <div className="loading-message">{message}</div>
      <div className="loading-skeleton">
        <div className="skeleton-header" />
        <div className="skeleton-item" />
        <div className="skeleton-item" />
        <div className="skeleton-item" />
        <div className="skeleton-item" />
      </div>
    </div>
  );
};
