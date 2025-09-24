# JavaScript Architecture

This directory contains the organized JavaScript architecture for the PKL Dashboard.

## File Structure

```
assets/js/
├── README.md      # This file - JavaScript architecture documentation
├── loading.js     # Loading utilities and FOUC prevention
└── dashboard.js   # Main dashboard functionality
```

## JavaScript Files Overview

### 1. `loading.js`
**Purpose**: Handle initial page loading and prevent FOUC

**Functions**:
- `checkStylesheetsLoaded()`: Track CSS loading progress
- `hideLoadingOverlay()`: Remove loading spinner when ready
- Window load event fallback

**Loading**: Loaded in HTML head before main dashboard script

### 2. `dashboard.js`
**Purpose**: Main application logic and UI management

**Classes**:
- `Dashboard`: Main application class

**Key Methods**:
- `init()`: Initialize dashboard
- `loadData()`: Fetch session and stats data
- `updateStats()`: Update statistics display
- `startLiveDurationUpdates()`: Real-time duration tracking
- `render()`: Render UI components
- `setupEventListeners()`: Event binding

**Global Functions**:
- `showSessionDetail()`: Display session modal
- `closeSessionDetail()`: Hide session modal
- `returnToContext()`: Open file in Cursor IDE
- `exportSession()`: Export session data
- `switchView()`: Toggle between views
- `refreshData()`: Reload dashboard data

## Architecture Principles

### 1. **Separation of Concerns**
- Loading utilities separate from application logic
- CSS class-based state management (no inline styles)
- API communication abstracted into methods

### 2. **State Management**
```javascript
// CSS classes for dynamic states
button.classList.add('btn-success');    // Success state
button.classList.add('btn-error');      // Error state
button.classList.add('btn-loading');    // Loading state
```

### 3. **Event-Driven Updates**
- Real-time duration updates every 5 seconds
- Automatic data refresh mechanisms
- User interaction feedback

### 4. **Error Handling**
- Graceful API error handling
- User-friendly error messages
- Fallback states for missing data

### 5. **Performance Optimization**
- Parallel API calls for data loading
- Efficient DOM updates
- Interval-based live updates

## API Integration

### Endpoints Used
- `GET /api/sessions`: Session data
- `GET /api/stats`: Dashboard statistics
- `GET /api/sessions/live-durations`: Real-time durations
- `GET /api/session/:id/conversations`: Session conversations
- `POST /api/session/:id/return-to-context`: Context restoration

### Data Flow
1. Initial load: Sessions + Stats in parallel
2. Live updates: Duration polling every 5 seconds
3. Modal data: Conversations loaded on demand
4. User actions: Context restoration, exports

## Component Organization

### Dashboard Class Structure
```javascript
class Dashboard {
  constructor()           // Initialize properties
  async init()           // Setup and first load
  async loadData()       // Fetch API data
  updateStats()          // Update statistics
  render()               // Render UI
  setupEventListeners()  // Event binding
  
  // Live duration methods
  startLiveDurationUpdates()
  updateLiveDurations()
  updateDurationDisplays()
  
  // View management
  renderSessionsView()
  renderNotebooksView()
  renderEmptyState()
  
  // Chart rendering
  renderCharts()
  renderActivityChart()
  renderIntentChart()
}
```

### Global Functions
Functions available globally for HTML event handlers:
- Modal management
- Context restoration
- Data export
- View switching

## State Management

### Application State
```javascript
this.sessions = [];        // Session data
this.stats = null;         // Statistics data
this.liveDurations = {};   // Real-time durations
this.currentView = 'sessions'; // Active view
this.charts = {};          // Chart instances
```

### UI State Management
- CSS classes for visual states
- DOM attributes for data binding
- Event listeners for interactions

## Error Handling Strategy

### API Errors
```javascript
try {
  const response = await fetch('/api/endpoint');
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  // Handle success
} catch (error) {
  console.error('Error:', error);
  this.showError('User-friendly message');
}
```

### User Feedback
- Loading states during API calls
- Success/error visual feedback
- Informative error messages
- Graceful fallbacks

## Performance Considerations

### Efficient Updates
- Parallel API calls where possible
- Selective DOM updates
- Debounced event handlers

### Memory Management
- Cleanup intervals on page unload
- Chart instance management
- Event listener cleanup

## Browser Compatibility

- Modern JavaScript (ES6+)
- Async/await syntax
- Fetch API
- DOM manipulation methods
- CSS class manipulation
