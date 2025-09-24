import React from 'react';
import { SearchFilters } from '../../config/types';
import './FilterBar.css';

interface FilterBarProps {
  filters: SearchFilters;
  onFiltersChange: (filters: SearchFilters) => void;
  sessionCount: number;
  totalCount: number;
}

/**
 * Filter bar component for advanced session filtering
 * Provides filters for intent, outcome, and date range
 */
export const FilterBar: React.FC<FilterBarProps> = ({
  filters,
  onFiltersChange,
  sessionCount,
  totalCount
}) => {
  const handleIntentChange = (intent: string) => {
    onFiltersChange({
      ...filters,
      intent: intent === 'all' ? undefined : intent
    });
  };

  const handleOutcomeChange = (outcome: string) => {
    onFiltersChange({
      ...filters,
      outcome: outcome === 'all' ? undefined : outcome
    });
  };

  const handleDateRangeChange = (dateRange: string) => {
    onFiltersChange({
      ...filters,
      dateRange: dateRange === 'all' ? undefined : dateRange
    });
  };

  const clearFilters = () => {
    onFiltersChange({});
  };

  const hasActiveFilters = filters.intent || filters.outcome || filters.dateRange;

  return (
    <div className="filter-bar">
      <div className="filter-controls">
        <div className="filter-group">
          <label htmlFor="intent-filter">Intent:</label>
          <select
            id="intent-filter"
            value={filters.intent || 'all'}
            onChange={(e) => handleIntentChange(e.target.value)}
            className="filter-select"
          >
            <option value="all">All Intents</option>
            <option value="debug">Debug</option>
            <option value="implement">Implement</option>
            <option value="explore">Explore</option>
            <option value="refactor">Refactor</option>
            <option value="document">Document</option>
          </select>
        </div>

        <div className="filter-group">
          <label htmlFor="outcome-filter">Outcome:</label>
          <select
            id="outcome-filter"
            value={filters.outcome || 'all'}
            onChange={(e) => handleOutcomeChange(e.target.value)}
            className="filter-select"
          >
            <option value="all">All Outcomes</option>
            <option value="success">Success</option>
            <option value="in-progress">In Progress</option>
            <option value="stuck">Stuck</option>
          </select>
        </div>

        <div className="filter-group">
          <label htmlFor="date-filter">Date:</label>
          <select
            id="date-filter"
            value={filters.dateRange || 'all'}
            onChange={(e) => handleDateRangeChange(e.target.value)}
            className="filter-select"
          >
            <option value="all">All Time</option>
            <option value="today">Today</option>
            <option value="week">This Week</option>
            <option value="month">This Month</option>
          </select>
        </div>

        {hasActiveFilters && (
          <button 
            className="clear-filters-btn"
            onClick={clearFilters}
            title="Clear all filters"
          >
            Clear Filters
          </button>
        )}
      </div>

      <div className="filter-results">
        <span className="results-count">
          {sessionCount} of {totalCount} sessions
        </span>
      </div>
    </div>
  );
};


