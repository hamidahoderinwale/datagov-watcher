import React, { useState } from 'react';
import { ExportOptions } from '../../config/types';
import './ExportModal.css';

interface ExportModalProps {
  isOpen: boolean;
  onClose: () => void;
  onExport: (options: ExportOptions) => void;
  onExportAll: (options: ExportOptions) => void;
  isExporting: boolean;
  exportProgress?: number;
}

/**
 * Export modal component
 * Allows users to export sessions in different formats
 */
export const ExportModal: React.FC<ExportModalProps> = ({
  isOpen,
  onClose,
  onExport,
  onExportAll,
  isExporting,
  exportProgress = 0
}) => {
  const [format, setFormat] = useState<'json' | 'markdown' | 'csv'>('json');
  const [includeCode, setIncludeCode] = useState(true);
  const [includeMetadata, setIncludeMetadata] = useState(true);
  const [includeConversations, setIncludeConversations] = useState(true);
  const [includeFileChanges, setIncludeFileChanges] = useState(true);

  if (!isOpen) return null;

  const handleExport = () => {
    onExport({
      format,
      includeCode,
      includeMetadata,
      includeConversations,
      includeFileChanges
    });
  };

  const handleExportAll = () => {
    onExportAll({
      format,
      includeCode,
      includeMetadata,
      includeConversations,
      includeFileChanges
    });
  };

  return (
    <div className="export-modal-overlay" onClick={onClose}>
      <div className="export-modal" onClick={(e) => e.stopPropagation()}>
        <div className="export-modal-header">
          <h3>Export Sessions</h3>
          <button className="close-button" onClick={onClose}>×</button>
        </div>
        
        <div className="export-modal-content">
          <div className="export-format-section">
            <h4>Export Format</h4>
            <div className="format-options">
              <label className="format-option">
                <input
                  type="radio"
                  name="format"
                  value="json"
                  checked={format === 'json'}
                  onChange={(e) => setFormat(e.target.value as 'json')}
                />
                <span>JSON</span>
              </label>
              <label className="format-option">
                <input
                  type="radio"
                  name="format"
                  value="markdown"
                  checked={format === 'markdown'}
                  onChange={(e) => setFormat(e.target.value as 'markdown')}
                />
                <span>Markdown</span>
              </label>
              <label className="format-option">
                <input
                  type="radio"
                  name="format"
                  value="csv"
                  checked={format === 'csv'}
                  onChange={(e) => setFormat(e.target.value as 'csv')}
                />
                <span>CSV</span>
              </label>
            </div>
          </div>

          <div className="export-options-section">
            <h4>Include</h4>
            <div className="option-checkboxes">
              <label className="option-checkbox">
                <input
                  type="checkbox"
                  checked={includeCode}
                  onChange={(e) => setIncludeCode(e.target.checked)}
                />
                <span>Code Deltas</span>
              </label>
              <label className="option-checkbox">
                <input
                  type="checkbox"
                  checked={includeMetadata}
                  onChange={(e) => setIncludeMetadata(e.target.checked)}
                />
                <span>Metadata</span>
              </label>
              <label className="option-checkbox">
                <input
                  type="checkbox"
                  checked={includeConversations}
                  onChange={(e) => setIncludeConversations(e.target.checked)}
                />
                <span>Conversations</span>
              </label>
              <label className="option-checkbox">
                <input
                  type="checkbox"
                  checked={includeFileChanges}
                  onChange={(e) => setIncludeFileChanges(e.target.checked)}
                />
                <span>File Changes</span>
              </label>
            </div>
          </div>

          {isExporting && (
            <div className="export-progress">
              <div className="progress-bar">
                <div 
                  className="progress-fill" 
                  style={{ width: `${exportProgress}%` }}
                />
              </div>
              <span className="progress-text">Exporting... {exportProgress}%</span>
            </div>
          )}
        </div>

        <div className="export-modal-footer">
          <button className="cancel-button" onClick={onClose}>
            Cancel
          </button>
          <button 
            className="export-button" 
            onClick={handleExport}
            disabled={isExporting}
          >
            Export Selected
          </button>
          <button 
            className="export-all-button" 
            onClick={handleExportAll}
            disabled={isExporting}
          >
            Export All
          </button>
        </div>
      </div>
    </div>
  );
};


