import React, { useState } from 'react';
import './AnnotationModal.css';

interface AnnotationModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (content: string, tags: string[]) => void;
  sessionId: string;
  existingAnnotations?: Array<{
    id: string;
    content: string;
    tags: string[];
    timestamp: Date;
  }>;
}

/**
 * Annotation modal component
 * Allows users to add and view annotations for sessions
 */
export const AnnotationModal: React.FC<AnnotationModalProps> = ({
  isOpen,
  onClose,
  onSave,
  sessionId,
  existingAnnotations = []
}) => {
  const [content, setContent] = useState('');
  const [tags, setTags] = useState('');
  const [isEditing, setIsEditing] = useState(false);

  if (!isOpen) return null;

  const handleSave = () => {
    if (!content.trim()) return;
    
    const tagArray = tags.split(',').map(tag => tag.trim()).filter(tag => tag.length > 0);
    onSave(content, tagArray);
    
    setContent('');
    setTags('');
    setIsEditing(false);
  };

  const handleCancel = () => {
    setContent('');
    setTags('');
    setIsEditing(false);
  };

  const formatTime = (date: Date) => {
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
    <div className="annotation-modal-overlay" onClick={onClose}>
      <div className="annotation-modal" onClick={(e) => e.stopPropagation()}>
        <div className="annotation-modal-header">
          <h3>Session Annotations</h3>
          <button className="close-button" onClick={onClose}>×</button>
        </div>
        
        <div className="annotation-modal-content">
          {/* Existing Annotations */}
          {existingAnnotations.length > 0 && (
            <div className="existing-annotations">
              <h4>Previous Annotations</h4>
              <div className="annotations-list">
                {existingAnnotations.map((annotation) => (
                  <div key={annotation.id} className="annotation-item">
                    <div className="annotation-content">{annotation.content}</div>
                    <div className="annotation-meta">
                      <span className="annotation-time">{formatTime(annotation.timestamp)}</span>
                      {annotation.tags.length > 0 && (
                        <div className="annotation-tags">
                          {annotation.tags.map((tag, index) => (
                            <span key={index} className="annotation-tag">{tag}</span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Add New Annotation */}
          <div className="add-annotation">
            <h4>Add Annotation</h4>
            <div className="annotation-form">
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                placeholder="Add your notes about this session..."
                className="annotation-textarea"
                rows={4}
              />
              
              <div className="annotation-tags-input">
                <label htmlFor="tags">Tags (comma-separated):</label>
                <input
                  id="tags"
                  type="text"
                  value={tags}
                  onChange={(e) => setTags(e.target.value)}
                  placeholder="e.g., bug, feature, important"
                  className="tags-input"
                />
              </div>
            </div>
          </div>
        </div>

        <div className="annotation-modal-footer">
          <button className="cancel-button" onClick={handleCancel}>
            Cancel
          </button>
          <button 
            className="save-button" 
            onClick={handleSave}
            disabled={!content.trim()}
          >
            Save Annotation
          </button>
        </div>
      </div>
    </div>
  );
};


