// Dashboard JavaScript functionality
class Dashboard {
  constructor() {
    this.sessions = [];
    this.stats = null;
    this.visualizations = [];
    this.currentView = 'sessions';
    this.charts = {};
    this.liveDurations = {};
    this.durationUpdateInterval = null;
    this.init();
  }

  async init() {
    await this.loadData();
    this.setupEventListeners();
    this.render();
    // Delay chart rendering to ensure DOM is ready
    setTimeout(() => this.renderCharts(), 100);
    // Start live duration updates
    this.startLiveDurationUpdates();
    // Setup search functionality
    this.setupSearch();
  }

  async loadData() {
    try {
      console.log('Loading data...');
      
      // Load sessions and stats in parallel
      const [sessionsResponse, statsResponse] = await Promise.all([
        fetch('/api/sessions'),
        fetch('/api/stats')
      ]);
      
      if (!sessionsResponse.ok || !statsResponse.ok) {
        throw new Error(`HTTP error! sessions: ${sessionsResponse.status}, stats: ${statsResponse.status}`);
      }
      
      const [sessionsData, statsData] = await Promise.all([
        sessionsResponse.json(),
        statsResponse.json()
      ]);
      
      console.log('Data loaded:', { sessions: sessionsData, stats: statsData });
      
      if (sessionsData.success && statsData.success) {
        this.sessions = sessionsData.sessions;
        this.stats = statsData.stats;
        console.log('Sessions loaded:', this.sessions.length);
        console.log('Stats loaded:', this.stats);
        this.updateStats();
      } else {
        console.error('Failed to load data:', { sessions: sessionsData.error, stats: statsData.error });
      }
    } catch (error) {
      console.error('Error loading data:', error);
      // Show user-friendly message
      this.showError('Failed to load session data. Please check if the server is running.');
    }
  }

  updateStats() {
    // Use stats from API if available, otherwise calculate from sessions
    if (this.stats) {
      console.log('Updating stats from API:', this.stats);
      
      // Update compact stats
      this.updateStatCard('total-sessions', this.stats.totalSessions || 0);
      this.updateStatCard('total-changes', this.stats.totalChanges || 0);
      this.updateStatCard('avg-duration', this.formatDuration(this.stats.avgSessionDuration * 60 || 0));
      this.updateStatCard('active-sessions', this.stats.activeSessions || 0);
      this.updateStatCard('total-conversations', this.stats.totalConversations || 0);
    } else {
      // Fallback to calculating from sessions data
      const totalSessions = this.sessions.length;
      const activeSessions = this.sessions.filter(s => s.outcome === 'in-progress').length;
      const totalChanges = this.sessions.reduce((sum, s) => sum + (s.codeDeltas?.length || 0), 0);
      
      console.log('Updating stats from sessions:', {
        totalSessions,
        activeSessions,
        totalChanges
      });

      // Update compact stats
      this.updateStatCard('total-sessions', totalSessions);
      this.updateStatCard('total-changes', totalChanges);
      this.updateStatCard('avg-duration', '0m');
      this.updateStatCard('active-sessions', activeSessions);
      this.updateStatCard('total-conversations', 0);
    }
  }

  updateStatCard(id, value) {
    const element = document.getElementById(id);
    if (element) {
      // Format certain values for better display
      let displayValue = value;
      
      if (id === 'avg-prompt-length' && value > 1000) {
        displayValue = (value / 1000).toFixed(1) + 'k';
      } else if (id === 'total-changes' && value > 1000) {
        displayValue = (value / 1000).toFixed(1) + 'k';
      } else if (typeof value === 'number' && value !== Math.floor(value)) {
        displayValue = value.toFixed(1);
      }
      
      element.textContent = displayValue;
    }
  }

  showError(message) {
    const container = document.getElementById('sessions-list');
    if (container) {
      container.innerHTML = `
        <div class="error-state">
          <div class="error-icon">!</div>
          <h3>Error Loading Data</h3>
          <p>${message}</p>
          <button class="btn btn-primary" onclick="window.dashboard?.refreshData()">Retry</button>
        </div>
      `;
    }
  }

  // Live Duration Methods
  startLiveDurationUpdates() {
    // Update durations every 5 seconds
    this.durationUpdateInterval = setInterval(async () => {
      await this.updateLiveDurations();
    }, 5000);
    
    // Initial load
    this.updateLiveDurations();
  }

  async updateLiveDurations() {
    try {
      const response = await fetch('/api/sessions/live-durations');
      const data = await response.json();
      
      if (data.success) {
        this.liveDurations = data.durations;
        this.updateDurationDisplays();
      }
    } catch (error) {
      console.error('Error updating live durations:', error);
    }
  }

  updateDurationDisplays() {
    // Update duration displays in session list
    document.querySelectorAll('[data-session-id]').forEach(element => {
      const sessionId = element.getAttribute('data-session-id');
      const durationElement = element.querySelector('.session-duration');
      
      if (durationElement && this.liveDurations[sessionId]) {
        const duration = this.liveDurations[sessionId];
        durationElement.textContent = duration.formatted;
        durationElement.setAttribute('title', `Started: ${new Date(duration.startTime).toLocaleString()}`);
        
        // Add visual indicator for active sessions
        if (duration.isActive) {
          durationElement.classList.add('live-duration');
        }
      }
    });

    // Update duration in session detail modal if open
    const modal = document.getElementById('sessionDetailModal');
    if (modal && modal.style.display === 'block') {
      const sessionId = modal.getAttribute('data-session-id');
      if (sessionId && this.liveDurations[sessionId]) {
        const durationElement = modal.querySelector('.modal-duration');
        if (durationElement) {
          durationElement.textContent = this.liveDurations[sessionId].formatted;
        }
      }
    }
  }

  formatDuration(durationMs) {
    if (!durationMs || durationMs < 0) return '0s';
    
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  stopLiveDurationUpdates() {
    if (this.durationUpdateInterval) {
      clearInterval(this.durationUpdateInterval);
      this.durationUpdateInterval = null;
    }
  }

  setupEventListeners() {
    // View toggle buttons
    const sessionsBtn = document.getElementById('sessions-view-btn');
    const notebooksBtn = document.getElementById('notebooks-view-btn');
    const visualizationsBtn = document.getElementById('visualizations-view-btn');
    const refreshBtn = document.getElementById('refresh-btn');
    
    if (sessionsBtn) {
      sessionsBtn.addEventListener('click', () => {
        console.log('Switching to sessions view');
        this.switchView('sessions');
      });
    }
    
    if (notebooksBtn) {
      notebooksBtn.addEventListener('click', () => {
        console.log('Switching to notebooks view');
        this.switchView('notebooks');
      });
    }

    if (visualizationsBtn) {
      visualizationsBtn.addEventListener('click', () => {
        console.log('Switching to visualizations view');
        this.switchView('visualizations');
      });
    }
    
    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => {
        console.log('Refreshing data');
        this.refreshData();
      });
    }
  }

  switchView(view) {
    this.currentView = view;
    
    // Update button states
    document.querySelectorAll('.view-btn').forEach(btn => btn.classList.remove('active'));
    document.getElementById(`${view}-view-btn`)?.classList.add('active');
    
    // Update content
    if (view === 'sessions') {
      this.renderSessionsView();
    } else if (view === 'notebooks') {
      this.renderNotebooksView();
    } else if (view === 'visualizations') {
      this.renderVisualizationsView();
    }
  }

  render() {
    if (this.sessions.length === 0) {
      this.renderEmptyState();
    } else {
      // Switch to notebooks view by default as requested
      this.currentView = 'notebooks';
      this.renderNotebooksView();
    }
    
    // Mark dashboard as loaded to prevent FOUC
    setTimeout(() => {
      const dashboard = document.querySelector('.dashboard');
      if (dashboard) {
        dashboard.classList.add('loaded');
      }
    }, 100); // Small delay to ensure stylesheets are applied
  }

  renderEmptyState() {
    const container = document.getElementById('sessions-list');
    if (container) {
      container.innerHTML = `
        <div class="empty-state">
          <div class="empty-state-icon">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
              <polyline points="14,2 14,8 20,8"></polyline>
              <line x1="16" y1="13" x2="8" y2="13"></line>
              <line x1="16" y1="17" x2="8" y2="17"></line>
              <polyline points="10,9 9,9 8,9"></polyline>
            </svg>
          </div>
          <h3 class="empty-state-title">No Active Sessions</h3>
          <p class="empty-state-description">
            Open a .ipynb file in Cursor IDE to start tracking your coding sessions and AI interactions.
          </p>
          <div class="empty-state-actions">
            <button class="btn btn-primary" onclick="refreshData()">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <polyline points="23 4 23 10 17 10"></polyline>
                <polyline points="1 20 1 14 7 14"></polyline>
                <path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"></path>
              </svg>
              Refresh
            </button>
            <button class="btn btn-secondary" onclick="openHelp()">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"></circle>
                <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path>
                <line x1="12" y1="17" x2="12.01" y2="17"></line>
              </svg>
              Help
            </button>
          </div>
        </div>
      `;
    }
  }

  renderSessionsView() {
    const container = document.getElementById('sessions-list');
    if (!container) return;

    if (this.sessions.length === 0) {
      this.renderEmptyState();
      return;
    }

    const sessionsHtml = this.sessions.map(session => this.renderSessionItem(session)).join('');
    container.innerHTML = `
      <div class="sessions-header">
        <h2 class="sessions-title">Session History</h2>
        <p class="sessions-subtitle">${this.sessions.length} sessions found</p>
      </div>
      <div class="sessions-content">
        ${sessionsHtml}
      </div>
    `;
  }

  renderSessionItem(session) {
    const fileName = session.currentFile ? session.currentFile.split('/').pop() : 'Unknown file';
    const time = new Date(session.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    const changes = (session.codeDeltas?.length || 0) + (session.fileChanges?.length || 0);
    const intent = session.intent || 'explore';
    const outcome = session.outcome || 'in-progress';
    
    // Get live duration if available
    const liveDuration = this.liveDurations[session.id];
    const durationDisplay = liveDuration ? this.formatDuration(liveDuration.duration) : '0s';

    // Compact format matching plan wireframe: dot 12:30  debug  main.py   checkmark
    return `
      <div class="session-item" data-session-id="${session.id}" onclick="showSessionDetail('${session.id}')">
        <div class="session-main">
          <span class="session-time">${time}</span>
          <span class="session-intent ${intent}">${intent}</span>
          <span class="session-file">${fileName}</span>
        </div>
        <div class="session-status">
          <span class="session-outcome ${outcome}">${this.getOutcomeIcon(outcome)}</span>
          <span class="session-changes">${changes}</span>
        </div>
      </div>
    `;
  }

  getOutcomeIcon(outcome) {
    switch (outcome) {
      case 'success': return 'OK';
      case 'stuck': return 'X';
      case 'in-progress': return '*';
      default: return '?';
    }
  }
          <div class="session-stat">
            <span>${session.codeDeltas?.length || 0} code deltas</span>
          </div>
          <div class="session-stat">
            <span>${session.fileChanges?.length || 0} file changes</span>
          </div>
        </div>
      </div>
    `;
  }

  renderNotebooksView() {
    const container = document.getElementById('sessions-list');
    if (!container) return;

    if (this.sessions.length === 0) {
      this.renderEmptyState();
      return;
    }

    const notebooks = this.groupSessionsByNotebook();
    const notebooksHtml = Object.entries(notebooks).map(([notebook, sessions]) => 
      this.renderNotebookCard(notebook, sessions)
    ).join('');

    container.innerHTML = `
      <div class="sessions-header">
        <h2 class="sessions-title">Notebook Grid</h2>
        <p class="sessions-subtitle">${Object.keys(notebooks).length} notebooks found</p>
      </div>
      <div class="notebooks-grid">
        ${notebooksHtml}
      </div>
    `;
  }

  groupSessionsByNotebook() {
    const groups = {};
    this.sessions.forEach(session => {
      const notebook = session.currentFile || 'Unknown';
      if (!groups[notebook]) {
        groups[notebook] = [];
      }
      groups[notebook].push(session);
    });
    return groups;
  }

  renderNotebookCard(notebook, sessions) {
    const totalChanges = sessions.reduce((sum, s) => sum + (s.codeDeltas?.length || 0) + (s.fileChanges?.length || 0), 0);
    const activeSessions = sessions.filter(s => s.outcome === 'IN_PROGRESS').length;
    const completedSessions = sessions.filter(s => s.outcome === 'COMPLETED').length;

    return `
      <div class="notebook-card">
        <div class="notebook-header">
          <h3 class="notebook-title">${notebook.split('/').pop()}</h3>
          <div class="notebook-stats">
            <span class="notebook-stat">${sessions.length} sessions</span>
            <span class="notebook-stat">${totalChanges} changes</span>
          </div>
        </div>
        <div class="notebook-content">
          <div class="notebook-sessions">
            <div class="session-summary">
              <span class="session-count active">${activeSessions} active</span>
              <span class="session-count completed">${completedSessions} completed</span>
            </div>
            <div class="recent-sessions">
              <h4>Recent Sessions</h4>
              ${sessions.slice(0, 3).map(s => `
                <div class="recent-session" onclick="showSessionDetail('${s.id}')">
                  <span class="recent-session-time">${new Date(s.timestamp).toLocaleTimeString()}</span>
                  <span class="recent-session-intent ${this.getIntentClass(s.intent)}">${this.getDisplayIntent(s.intent)}</span>
                </div>
              `).join('')}
            </div>
          </div>
        </div>
      </div>
    `;
  }

  getIntentClass(intent) {
    const intentMap = {
      'EXPLORE': 'explore',
      'data_exploration': 'explore',
      'IMPLEMENT': 'implement',
      'implementation': 'implement',
      'DEBUG': 'debug',
      'debug': 'debug'
    };
    return intentMap[intent] || 'unknown';
  }

  getOutcomeClass(outcome) {
    const outcomeMap = {
      'COMPLETED': 'completed',
      'IN_PROGRESS': 'in-progress',
      'FAILED': 'failed'
    };
    return outcomeMap[outcome] || 'unknown';
  }

  getDisplayIntent(intent) {
    const intentMap = {
      'EXPLORE': 'Explore',
      'data_exploration': 'Explore',
      'IMPLEMENT': 'Implement',
      'implementation': 'Implement',
      'DEBUG': 'Debug',
      'debug': 'Debug'
    };
    return intentMap[intent] || 'Unknown';
  }

  getDisplayOutcome(outcome) {
    const outcomeMap = {
      'COMPLETED': 'Completed',
      'IN_PROGRESS': 'In Progress',
      'FAILED': 'Failed'
    };
    return outcomeMap[outcome] || 'Unknown';
  }

  async refreshData() {
    await this.loadData();
    this.render();
    this.renderCharts();
  }

  renderCharts() {
    if (this.sessions.length === 0) {
      console.log('No sessions to render charts');
      return;
    }
    console.log('Rendering charts with', this.sessions.length, 'sessions');
    this.renderActivityChart();
    this.renderIntentChart();
  }

  renderActivityChart() {
    const ctx = document.getElementById('activityChart');
    if (!ctx) return;

    // Destroy existing chart if it exists
    if (this.charts.activity) {
      this.charts.activity.destroy();
      this.charts.activity = null;
    }

    // Group sessions by date
    const sessionsByDate = {};
    this.sessions.forEach(session => {
      const date = new Date(session.timestamp).toDateString();
      if (!sessionsByDate[date]) {
        sessionsByDate[date] = { sessions: 0, changes: 0 };
      }
      sessionsByDate[date].sessions++;
      sessionsByDate[date].changes += (session.codeDeltas?.length || 0) + (session.fileChanges?.length || 0);
    });

    const dates = Object.keys(sessionsByDate).sort();
    const sessionCounts = dates.map(date => sessionsByDate[date].sessions);
    const changeCounts = dates.map(date => sessionsByDate[date].changes);

    this.charts.activity = new Chart(ctx, {
      type: 'line',
      data: {
        labels: dates,
        datasets: [{
          label: 'Sessions',
          data: sessionCounts,
          borderColor: 'rgb(0, 122, 255)',
          backgroundColor: 'rgba(0, 122, 255, 0.1)',
          tension: 0.4,
          yAxisID: 'y'
        }, {
          label: 'Changes',
          data: changeCounts,
          borderColor: 'rgb(52, 199, 89)',
          backgroundColor: 'rgba(52, 199, 89, 0.1)',
          tension: 0.4,
          yAxisID: 'y1'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            type: 'linear',
            display: true,
            position: 'left',
            title: {
              display: true,
              text: 'Sessions'
            }
          },
          y1: {
            type: 'linear',
            display: true,
            position: 'right',
            title: {
              display: true,
              text: 'Changes'
            },
            grid: {
              drawOnChartArea: false,
            },
          }
        },
        plugins: {
          legend: {
            display: true,
            position: 'top'
          }
        }
      }
    });
  }

  renderIntentChart() {
    const ctx = document.getElementById('intentChart');
    if (!ctx) return;

    // Destroy existing chart if it exists
    if (this.charts.intent) {
      this.charts.intent.destroy();
      this.charts.intent = null;
    }

    // Count sessions by intent
    const intentCounts = {};
    this.sessions.forEach(session => {
      const intent = session.intent || 'Unknown';
      intentCounts[intent] = (intentCounts[intent] || 0) + 1;
    });

    const intents = Object.keys(intentCounts);
    const counts = Object.values(intentCounts);
    const colors = [
      'rgb(0, 122, 255)',    // Blue for EXPLORE
      'rgb(52, 199, 89)',    // Green for IMPLEMENT
      'rgb(255, 149, 0)',    // Orange for DEBUG
      'rgb(255, 59, 48)',    // Red for other
      'rgb(175, 82, 222)'    // Purple for other
    ];

    this.charts.intent = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: intents,
        datasets: [{
          data: counts,
          backgroundColor: colors.slice(0, intents.length),
          borderWidth: 2,
          borderColor: '#ffffff'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: true,
            position: 'bottom'
          }
        }
      }
    });
  }

  // Visualization methods
  async loadVisualizations() {
    try {
      console.log('Loading visualizations...');
      const response = await fetch('/api/visualizations');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.success) {
        this.visualizations = data.sessions || [];
        console.log('Visualizations loaded:', this.visualizations.length, 'sessions with visualizations');
      } else {
        console.error('Failed to load visualizations:', data.error);
      }
    } catch (error) {
      console.error('Error loading visualizations:', error);
      this.showError('Failed to load visualizations. Please check if the server is running.');
    }
  }

  async renderVisualizationsView() {
    console.log('Rendering visualizations view');
    
    // Load visualizations if not already loaded
    if (this.visualizations.length === 0) {
      await this.loadVisualizations();
    }

    const container = document.getElementById('sessions-list');
    
    if (this.visualizations.length === 0) {
      container.innerHTML = `
        <div class="no-visualizations">
          <div class="no-visualizations-icon">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
              <circle cx="8.5" cy="8.5" r="1.5"></circle>
              <polyline points="21,15 16,10 5,21"></polyline>
            </svg>
          </div>
          <h3>No Visualizations Found</h3>
          <p>No charts or plots found in your notebook sessions. Create some visualizations in your Jupyter notebooks to see them here.</p>
          <button class="btn btn-primary" onclick="refreshData()">Refresh Data</button>
        </div>
      `;
      return;
    }

    const visualizationsHtml = this.visualizations.map(session => {
      const fileName = session.file.split('/').pop();
      const sessionTime = new Date(session.timestamp).toLocaleString();
      
      const visualizationsCards = session.visualizations.map((viz, index) => {
        const typeClass = viz.type;
        let previewHtml = '';
        
        if (viz.type === 'image' && viz.format === 'png') {
          previewHtml = `<img src="data:image/png;base64,${viz.data}" alt="Visualization ${index + 1}">`;
        } else if (viz.type === 'image' && viz.format === 'svg') {
          previewHtml = Array.isArray(viz.data) ? viz.data.join('') : viz.data;
        } else if (viz.type === 'html') {
          previewHtml = `<div class="html-content">${viz.data}</div>`;
        } else if (viz.type === 'plotly') {
          previewHtml = `<div class="html-content"><pre>${JSON.stringify(viz.data, null, 2).slice(0, 500)}...</pre></div>`;
        }
        
        return `
          <div class="visualization-card">
            <div class="visualization-header">
              <div class="visualization-title">
                Visualization ${index + 1}
                <span class="visualization-type-badge ${typeClass}">${viz.type.toUpperCase()}</span>
              </div>
              <div class="visualization-meta">
                <span>Cell ${viz.cellIndex + 1}</span>
                <span>Output ${viz.outputIndex + 1}</span>
              </div>
            </div>
            <div class="visualization-preview">
              ${previewHtml}
            </div>
            ${viz.source ? `
              <div class="visualization-source">
                <strong>Source Code:</strong>
                ${viz.source}
              </div>
            ` : ''}
            <div class="visualization-actions">
              <button class="btn btn-sm btn-secondary" onclick="showSessionDetail('${session.sessionId}')">
                View Session
              </button>
              <button class="btn btn-sm btn-secondary" onclick="returnToContext('${session.sessionId}')">
                Open in Cursor
              </button>
            </div>
          </div>
        `;
      }).join('');
      
      return `
        <div class="notebook-section">
          <div class="notebook-header">
            <h3 class="notebook-title">${fileName}</h3>
            <div class="notebook-meta">
              <span>${sessionTime}</span>
              <span>${session.total} visualizations</span>
            </div>
          </div>
          <div class="visualizations-grid">
            ${visualizationsCards}
          </div>
        </div>
      `;
    }).join('');

    container.innerHTML = visualizationsHtml;
  }

  setupSearch() {
    const searchInput = document.getElementById('search-input');
    const intentFilter = document.getElementById('intent-filter');
    const outcomeFilter = document.getElementById('outcome-filter');
    
    if (searchInput) {
      // Search input with debouncing
      let searchTimeout;
      searchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
          this.filterSessions();
        }, 300);
      });
      
      // Keyboard shortcut (Cmd+K)
      document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
          e.preventDefault();
          searchInput.focus();
        }
      });
    }
    
    if (intentFilter) {
      intentFilter.addEventListener('change', () => this.filterSessions());
    }
    
    if (outcomeFilter) {
      outcomeFilter.addEventListener('change', () => this.filterSessions());
    }
  }

  filterSessions() {
    const searchInput = document.getElementById('search-input');
    const intentFilter = document.getElementById('intent-filter');
    const outcomeFilter = document.getElementById('outcome-filter');
    
    const searchTerm = searchInput?.value.toLowerCase() || '';
    const intentFilter_value = intentFilter?.value || '';
    const outcomeFilter_value = outcomeFilter?.value || '';
    
    let filteredSessions = this.sessions;
    
    // Apply text search
    if (searchTerm) {
      filteredSessions = filteredSessions.filter(session => {
        const fileName = session.currentFile ? session.currentFile.split('/').pop().toLowerCase() : '';
        const intent = (session.intent || '').toLowerCase();
        const sessionId = session.id.toLowerCase();
        
        return fileName.includes(searchTerm) || 
               intent.includes(searchTerm) || 
               sessionId.includes(searchTerm);
      });
    }
    
    // Apply intent filter
    if (intentFilter_value) {
      filteredSessions = filteredSessions.filter(session => session.intent === intentFilter_value);
    }
    
    // Apply outcome filter
    if (outcomeFilter_value) {
      filteredSessions = filteredSessions.filter(session => session.outcome === outcomeFilter_value);
    }
    
    // Re-render sessions with filtered results
    this.renderFilteredSessions(filteredSessions);
  }

  renderFilteredSessions(filteredSessions) {
    const container = document.getElementById('sessions-list');
    if (!container) return;
    
    if (filteredSessions.length === 0) {
      container.innerHTML = `
        <div class="empty-state">
          <div class="empty-state-icon">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="11" cy="11" r="8"></circle>
              <path d="m21 21-4.35-4.35"></path>
            </svg>
          </div>
          <h3>No sessions found</h3>
          <p>Try adjusting your search criteria or filters.</p>
          <button class="btn btn-secondary" onclick="clearSearch()">Clear Search</button>
        </div>
      `;
      return;
    }
    
    const sessionsHtml = filteredSessions.map(session => this.renderSessionItem(session)).join('');
    container.innerHTML = `
      <div class="sessions-header">
        <h2 class="sessions-title">Session History</h2>
        <p class="sessions-subtitle">${filteredSessions.length} of ${this.sessions.length} sessions</p>
      </div>
      <div class="sessions-content">
        ${sessionsHtml}
      </div>
    `;
  }
}

// Global functions for backward compatibility
function refreshData() {
  if (window.dashboard) {
    window.dashboard.refreshData();
  }
}

function openHelp() {
  alert('Help: Open a .ipynb file in Cursor IDE to start tracking sessions.');
}

function clearSearch() {
  const searchInput = document.getElementById('search-input');
  const intentFilter = document.getElementById('intent-filter');
  const outcomeFilter = document.getElementById('outcome-filter');
  
  if (searchInput) searchInput.value = '';
  if (intentFilter) intentFilter.value = '';
  if (outcomeFilter) outcomeFilter.value = '';
  
  if (window.dashboard) {
    window.dashboard.filterSessions();
  }
}

function showSessionDetail(sessionId) {
  if (!window.dashboard) return;
  
  const session = window.dashboard.sessions.find(s => s.id === sessionId);
  if (!session) {
    console.error('Session not found:', sessionId);
    return;
  }
  
  // Show session detail modal
  const modal = document.getElementById('sessionDetailModal');
  const title = document.getElementById('session-detail-title');
  const body = document.getElementById('session-detail-body');
  
  if (!modal || !title || !body) {
    console.error('Modal elements not found');
    return;
  }
  
  const time = new Date(session.timestamp).toLocaleString();
  const changes = (session.codeDeltas?.length || 0) + (session.fileChanges?.length || 0);
  const fileName = session.currentFile ? session.currentFile.split('/').pop() : 'Unknown';
  
  title.textContent = `Session: ${fileName} - ${time}`;
  
  // Fetch conversations for this session
  let conversationsHtml = '<div class="loading">Loading conversations...</div>';
  
  body.innerHTML = `
    <div class="session-detail-section">
      <h4>Overview</h4>
      <div class="detail-grid">
        <div class="detail-item">
          <label>Session ID:</label>
          <span>${session.id}</span>
        </div>
        <div class="detail-item">
          <label>File:</label>
          <span>${session.currentFile || 'Unknown'}</span>
        </div>
        <div class="detail-item">
          <label>Intent:</label>
          <span class="session-intent ${session.intent?.toLowerCase()}">${window.dashboard.getDisplayIntent(session.intent)}</span>
        </div>
        <div class="detail-item">
          <label>Outcome:</label>
          <span class="session-outcome ${window.dashboard.getOutcomeClass(session.outcome || session.phase)}">${window.dashboard.getDisplayOutcome(session.outcome || session.phase)}</span>
        </div>
        <div class="detail-item">
          <label>Duration:</label>
          <span class="modal-duration">${session.duration ? this.formatDuration(session.duration) : 'Calculating...'}</span>
        </div>
      </div>
    </div>
    
    <div class="session-detail-section">
      <h4>Statistics</h4>
      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-value">${changes}</div>
          <div class="stat-label">Total Changes</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${session.codeDeltas?.length || 0}</div>
          <div class="stat-label">Code Deltas</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${session.fileChanges?.length || 0}</div>
          <div class="stat-label">File Changes</div>
        </div>
      </div>
    </div>
    
    <div class="session-detail-section">
      <h4>Conversations</h4>
      <div id="session-conversations">
        ${conversationsHtml}
      </div>
    </div>
    
    <div class="session-detail-section">
      <h4>Code Changes</h4>
      <div class="code-changes">
        ${session.codeDeltas?.slice(0, 5).map(delta => `
          <div class="code-delta">
            <div class="delta-header">
              <span class="delta-type">${delta.changeType}</span>
              <span class="delta-lines">${delta.lineCount} lines</span>
            </div>
            <pre class="delta-content">${delta.afterContent.slice(0, 200)}${delta.afterContent.length > 200 ? '...' : ''}</pre>
          </div>
        `).join('') || '<p>No code changes recorded</p>'}
        ${(session.codeDeltas?.length || 0) > 5 ? `<p class="more-changes">... and ${(session.codeDeltas?.length || 0) - 5} more changes</p>` : ''}
      </div>
    </div>
    
    <div class="session-actions">
      <button class="btn btn-primary" onclick="returnToContext('${session.id}')">Return to Context</button>
      <button class="btn btn-secondary" onclick="exportSession('${session.id}')">Export Session</button>
      <button class="btn btn-secondary" onclick="closeSessionDetail()">Close</button>
    </div>
  `;
  
  modal.setAttribute('data-session-id', sessionId);
  modal.style.display = 'block';
  
  // Load conversations asynchronously
  loadSessionConversations(sessionId);
}

async function loadSessionConversations(sessionId) {
  try {
    const response = await fetch(`/api/session/${sessionId}/conversations`);
    const data = await response.json();
    
    const conversationsContainer = document.getElementById('session-conversations');
    if (!conversationsContainer) return;
    
    if (data.success && data.conversations.length > 0) {
      const conversationsHtml = data.conversations.map(conv => `
        <div class="conversation-item">
          <div class="conversation-header">
            <span class="conversation-role ${conv.role}">${conv.role === 'user' ? 'User' : 'Assistant'}</span>
            <span class="conversation-time">${new Date(conv.timestamp).toLocaleString()}</span>
          </div>
          <div class="conversation-content">${conv.content}</div>
          ${conv.codeBlocks && conv.codeBlocks.length > 0 ? `
            <div class="conversation-code-blocks">
              ${conv.codeBlocks.map(code => `
                <pre class="code-block"><code>${code}</code></pre>
              `).join('')}
            </div>
          ` : ''}
          ${conv.referencedFiles && conv.referencedFiles.length > 0 ? `
            <div class="conversation-files">
              <small>Referenced files: ${conv.referencedFiles.map(f => f.split('/').pop()).join(', ')}</small>
            </div>
          ` : ''}
        </div>
      `).join('');
      
      conversationsContainer.innerHTML = conversationsHtml;
    } else {
      conversationsContainer.innerHTML = '<div class="no-conversations">No conversations found for this session.</div>';
    }
  } catch (error) {
    console.error('Error loading conversations:', error);
    const conversationsContainer = document.getElementById('session-conversations');
    if (conversationsContainer) {
      conversationsContainer.innerHTML = '<div class="error">Failed to load conversations.</div>';
    }
  }
}

function switchView(view) {
  if (window.dashboard) {
    window.dashboard.switchView(view);
  }
}

function closeSessionDetail() {
  const modal = document.getElementById('sessionDetailModal');
  if (modal) {
    modal.style.display = 'none';
  }
}

async function returnToContext(sessionId) {
  try {
    console.log('Return to context for session:', sessionId);
    
    // Show loading state
    const button = event.target;
    const originalText = button.textContent;
    button.textContent = 'Opening...';
    button.disabled = true;
    
    // Make API call to restore context
    const response = await fetch(`/api/session/${sessionId}/return-to-context`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const result = await response.json();
    
    if (result.success) {
      console.log('Session context restored successfully');
      
      // Show success feedback
      button.textContent = 'Opened in Cursor';
      button.classList.add('btn-success');
      
      // Reset button after delay
      setTimeout(() => {
        button.textContent = originalText;
        button.disabled = false;
        button.classList.remove('btn-success');
      }, 2000);
      
      // Optionally close the modal
      setTimeout(() => {
        closeSessionDetail();
      }, 1500);
      
    } else {
      console.error('Failed to restore context:', result.error);
      
      // Show error feedback
      button.textContent = 'Failed to Open';
      button.classList.add('btn-error');
      
      // Reset button after delay
      setTimeout(() => {
        button.textContent = originalText;
        button.disabled = false;
        button.classList.remove('btn-error');
      }, 2000);
      
      // Show user-friendly error message
      alert(`Failed to open in Cursor IDE: ${result.error}`);
    }
  } catch (error) {
    console.error('Error calling return to context API:', error);
    
    // Reset button
    const button = event.target;
    button.textContent = 'Return to Context';
    button.disabled = false;
    button.classList.remove('btn-success', 'btn-error', 'btn-loading');
    
    alert('Error: Could not connect to the server. Please ensure the PKL server is running.');
  }
}

function exportSession(sessionId) {
  // Export specific session
  fetch(`/api/export`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      options: { sessionId: sessionId }
    })
  })
  .then(response => response.json())
  .then(data => {
    if (data.success) {
      window.open(data.export.downloadUrl, '_blank');
    } else {
      alert('Export failed: ' + data.error);
    }
  })
  .catch(error => {
    console.error('Export error:', error);
    alert('Export failed');
  });
}

function formatDuration(duration) {
  if (typeof duration === 'number') {
    const minutes = Math.floor(duration / 60000);
    const seconds = Math.floor((duration % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
  }
  return duration;
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  // Prevent duplicate initialization
  if (window.dashboard) {
    console.log('Dashboard already initialized');
    return;
  }
  
  console.log('DOM loaded, initializing dashboard...');
  window.dashboard = new Dashboard();
  console.log('Dashboard initialized:', window.dashboard);
});
