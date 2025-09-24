import React, { useState, useRef, useEffect } from 'react';
import './SearchBar.css';

interface SearchBarProps {
  query: string;
  onSearch: (query: string) => void;
  onClear: () => void;
}

/**
 * Search bar component with keyboard shortcuts
 * Supports Cmd+K shortcut for focus
 */
export const SearchBar: React.FC<SearchBarProps> = ({ query, onSearch, onClear }) => {
  const [localQuery, setLocalQuery] = useState(query);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.metaKey && event.key === 'k') {
        event.preventDefault();
        inputRef.current?.focus();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  useEffect(() => {
    setLocalQuery(query);
  }, [query]);

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setLocalQuery(value);
    onSearch(value);
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Escape') {
      onClear();
      inputRef.current?.blur();
    }
  };

  const handleClear = () => {
    setLocalQuery('');
    onClear();
    inputRef.current?.focus();
  };

  return (
    <div className="search-bar">
      <div className="search-input-container">
        <input
          ref={inputRef}
          type="text"
          className="search-input"
          placeholder="Search sessions (⌘K)"
          value={localQuery}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
        />
        {localQuery && (
          <button 
            className="search-clear"
            onClick={handleClear}
            title="Clear search"
          >
            ×
          </button>
        )}
      </div>
    </div>
  );
};
