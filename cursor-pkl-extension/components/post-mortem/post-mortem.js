/**
 * Post-mortem Analysis Interface
 * 
 * Manages the UI for forensic analysis of missing or unavailable resources
 */

class PostMortemManager {
  constructor() {
    this.postMortems = [];
    this.currentPostMortem = null;
    this.timelineChart = null;
    this.init();
  }

  async init() {
    await this.loadPostMortems();
    this.setupEventListeners();
    this.updateStats();
  }

  setupEventListeners() {
    // Filter change handlers
    const statusFilter = document.getElementById('status-filter');
    const causeFilter = document.getElementById('cause-filter');
    
    if (statusFilter) {
      statusFilter.addEventListener('change', () => this.filterPostMortems());
    }
    
    if (causeFilter) {
      causeFilter.addEventListener('change', () => this.filterPostMortems());
    }
  }

  async loadPostMortems() {
    try {
      const response = await fetch('/api/post-mortems');
      const data = await response.json();
      
      if (data.success) {
        this.postMortems = data.postMortems;
        this.renderPostMortems();
      } else {
        this.showError('Failed to load post-mortems');
      }
    } catch (error) {
      console.error('Error loading post-mortems:', error);
      this.showError('Failed to connect to server');
    }
  }

  renderPostMortems(filteredPostMortems = null) {
    const container = document.getElementById('post-mortems-list');
    const postMortems = filteredPostMortems || this.postMortems;
    
    if (postMortems.length === 0) {
      container.innerHTML = `
        <div class="empty-state">
          <div class="empty-state-icon">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path>
              <line x1="12" y1="9" x2="12" y2="13"></line>
              <line x1="12" y1="17" x2="12.01" y2="17"></line>
            </svg>
          </div>
          <h3>No Post-mortems Found</h3>
          <p>No missing or unavailable resources have been detected.</p>
          <button class="btn btn-primary" onclick="scanForMissing()">Scan for Missing Files</button>
        </div>
      `;
      return;
    }

    const postMortemsHtml = postMortems.map(pm => this.renderPostMortemItem(pm)).join('');
    container.innerHTML = postMortemsHtml;
  }

  renderPostMortemItem(postMortem) {
    const timestamp = new Date(postMortem.timestamp).toLocaleString();
    const fileName = postMortem.resourceInfo.fileName;
    const cause = postMortem.suspectedCause.type.replace(/_/g, ' ');
    const confidence = Math.round(postMortem.suspectedCause.confidence * 100);
    
    return `
      <div class="post-mortem-item" onclick="showPostMortemDetail('${postMortem.id}')">
        <div class="post-mortem-icon ${postMortem.status}">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            ${this.getStatusIcon(postMortem.status)}
          </svg>
        </div>
        
        <div class="post-mortem-content">
          <div class="post-mortem-title">${fileName}</div>
          <div class="post-mortem-meta">
            <span class="post-mortem-cause">${cause}</span>
            <span class="post-mortem-timestamp">${timestamp}</span>
          </div>
        </div>
        
        <div class="post-mortem-stats">
          <div class="post-mortem-stat">
            <div class="post-mortem-stat-value">${confidence}%</div>
            <div class="post-mortem-stat-label">Confidence</div>
          </div>
          <div class="post-mortem-stat">
            <div class="post-mortem-stat-value">${postMortem.recoveryOptionsCount}</div>
            <div class="post-mortem-stat-label">Recovery</div>
          </div>
          <div class="post-mortem-stat">
            <div class="post-mortem-stat-value">${postMortem.archivedVersionsCount}</div>
            <div class="post-mortem-stat-label">Archives</div>
          </div>
        </div>
      </div>
    `;
  }

  getStatusIcon(status) {
    switch (status) {
      case 'active':
        return `<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path>
                <line x1="12" y1="9" x2="12" y2="13"></line>
                <line x1="12" y1="17" x2="12.01" y2="17"></line>`;
      case 'resolved':
        return `<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                <polyline points="22,4 12,14.01 9,11.01"></polyline>`;
      case 'archived':
        return `<polyline points="3,6 5,6 21,6"></polyline>
                <path d="M19,6v14a2,2 0 0,1 -2,2H7a2,2 0 0,1 -2,-2V6m3,0V4a2,2 0 0,1 2,-2h4a2,2 0 0,1 2,2v2"></path>`;
      default:
        return `<circle cx="12" cy="12" r="10"></circle>
                <line x1="12" y1="8" x2="12" y2="12"></line>
                <line x1="12" y1="16" x2="12.01" y2="16"></line>`;
    }
  }

  filterPostMortems() {
    const statusFilter = document.getElementById('status-filter').value;
    const causeFilter = document.getElementById('cause-filter').value;
    
    let filtered = this.postMortems;
    
    if (statusFilter) {
      filtered = filtered.filter(pm => pm.status === statusFilter);
    }
    
    if (causeFilter) {
      filtered = filtered.filter(pm => pm.suspectedCause.type === causeFilter);
    }
    
    this.renderPostMortems(filtered);
  }

  updateStats() {
    const totalElement = document.getElementById('total-post-mortems');
    const activeElement = document.getElementById('active-cases');
    const resolvedElement = document.getElementById('resolved-cases');
    
    if (totalElement) totalElement.textContent = this.postMortems.length;
    if (activeElement) activeElement.textContent = this.postMortems.filter(pm => pm.status === 'active').length;
    if (resolvedElement) resolvedElement.textContent = this.postMortems.filter(pm => pm.status === 'resolved').length;
  }

  async showPostMortemDetail(postMortemId) {
    try {
      const response = await fetch(`/api/post-mortem/${postMortemId}`);
      const data = await response.json();
      
      if (data.success) {
        this.currentPostMortem = data.postMortem;
        this.renderPostMortemDetail(data.postMortem);
        document.getElementById('postMortemModal').style.display = 'block';
      } else {
        this.showError('Failed to load post-mortem details');
      }
    } catch (error) {
      console.error('Error loading post-mortem detail:', error);
      this.showError('Failed to connect to server');
    }
  }

  renderPostMortemDetail(postMortem) {
    // Update modal title
    document.getElementById('post-mortem-title').textContent = 
      `Post-mortem: ${postMortem.resourceInfo.fileName}`;
    
    // Render overview tab
    this.renderOverviewTab(postMortem);
    this.renderForensicsTab(postMortem);
    this.renderRecoveryTab(postMortem);
    this.renderTimelineTab(postMortem);
  }

  renderOverviewTab(postMortem) {
    const resourceInfo = document.getElementById('resource-info');
    const suspectedCause = document.getElementById('suspected-cause');
    const lastSnapshot = document.getElementById('last-snapshot');
    
    // Resource information
    resourceInfo.innerHTML = `
      <h4>Resource Information</h4>
      <div class="resource-details">
        <div class="resource-detail">
          <div class="resource-detail-label">File Path</div>
          <div class="resource-detail-value">${postMortem.resourceInfo.filePath}</div>
        </div>
        <div class="resource-detail">
          <div class="resource-detail-label">File Name</div>
          <div class="resource-detail-value">${postMortem.resourceInfo.fileName}</div>
        </div>
        <div class="resource-detail">
          <div class="resource-detail-label">Resource Type</div>
          <div class="resource-detail-value">${postMortem.resourceInfo.resourceType}</div>
        </div>
        <div class="resource-detail">
          <div class="resource-detail-label">Last Seen</div>
          <div class="resource-detail-value">${new Date(postMortem.resourceInfo.lastSeen).toLocaleString()}</div>
        </div>
        ${postMortem.resourceInfo.size ? `
        <div class="resource-detail">
          <div class="resource-detail-label">Size</div>
          <div class="resource-detail-value">${this.formatFileSize(postMortem.resourceInfo.size)}</div>
        </div>` : ''}
      </div>
    `;
    
    // Suspected cause
    const cause = postMortem.suspectedCause.primary;
    const confidence = Math.round(cause.confidence * 100);
    
    suspectedCause.innerHTML = `
      <h4>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path>
          <line x1="12" y1="9" x2="12" y2="13"></line>
          <line x1="12" y1="17" x2="12.01" y2="17"></line>
        </svg>
        Suspected Cause
        <span class="cause-confidence">${confidence}% confidence</span>
      </h4>
      <p><strong>${cause.type.replace(/_/g, ' ').toUpperCase()}</strong></p>
      <p>${cause.description}</p>
      <p><strong>Evidence:</strong> ${cause.evidence}</p>
    `;
    
    // Last snapshot
    if (postMortem.lastSnapshot) {
      const snapshot = postMortem.lastSnapshot;
      lastSnapshot.innerHTML = `
        <h4>Last Known State</h4>
        <div class="resource-details">
          <div class="resource-detail">
            <div class="resource-detail-label">Session ID</div>
            <div class="resource-detail-value">${snapshot.sessionId}</div>
          </div>
          <div class="resource-detail">
            <div class="resource-detail-label">Timestamp</div>
            <div class="resource-detail-value">${new Date(snapshot.timestamp).toLocaleString()}</div>
          </div>
          <div class="resource-detail">
            <div class="resource-detail-label">Intent</div>
            <div class="resource-detail-value">${snapshot.metadata.intent}</div>
          </div>
          <div class="resource-detail">
            <div class="resource-detail-label">Outcome</div>
            <div class="resource-detail-value">${snapshot.metadata.outcome}</div>
          </div>
          <div class="resource-detail">
            <div class="resource-detail-label">Code Changes</div>
            <div class="resource-detail-value">${snapshot.codeDeltas.length}</div>
          </div>
        </div>
      `;
    } else {
      lastSnapshot.innerHTML = `
        <h4>Last Known State</h4>
        <p>No snapshot information available.</p>
      `;
    }
  }

  renderForensicsTab(postMortem) {
    const forensicAnalysis = document.getElementById('forensic-analysis');
    const priorDiffs = document.getElementById('prior-diffs');
    
    // Forensic analysis
    if (postMortem.forensicAnalysis) {
      const analysis = postMortem.forensicAnalysis;
      forensicAnalysis.innerHTML = `
        <h4>Forensic Analysis</h4>
        ${analysis.patterns.map(pattern => `
          <div class="analysis-pattern">
            <h5>${pattern.type.replace(/_/g, ' ').toUpperCase()}</h5>
            <p>${pattern.description}</p>
            <p><em>${pattern.implication}</em></p>
          </div>
        `).join('')}
        
        ${analysis.riskFactors.length > 0 ? `
          <h5>Risk Factors</h5>
          ${analysis.riskFactors.map(risk => `
            <div class="risk-factor ${risk.severity}">
              <strong>${risk.type.replace(/_/g, ' ')}</strong>: ${risk.description}
            </div>
          `).join('')}
        ` : ''}
        
        <h5>Recommendations</h5>
        <ul>
          ${analysis.recommendations.map(rec => `<li>${rec}</li>`).join('')}
        </ul>
      `;
    }
    
    // Prior diffs
    if (postMortem.priorDiffs && postMortem.priorDiffs.timeline) {
      const diffs = postMortem.priorDiffs;
      priorDiffs.innerHTML = `
        <h4>Change History</h4>
        <div class="diff-summary">
          <p><strong>Total Sessions:</strong> ${diffs.totalSessions}</p>
          <p><strong>Total Changes:</strong> ${diffs.totalChanges}</p>
          ${diffs.changePattern ? `
            <p><strong>Change Pattern:</strong> ${diffs.changePattern.trend} (${diffs.changePattern.avgChangesPerSession} avg changes/session)</p>
          ` : ''}
        </div>
        
        <h5>Recent Activity</h5>
        <div class="timeline-list">
          ${diffs.recentActivity.map(activity => `
            <div class="timeline-item">
              <div class="timeline-time">${new Date(activity.timestamp).toLocaleString()}</div>
              <div class="timeline-content">
                <strong>Session ${activity.sessionId.slice(-8)}</strong>: ${activity.changeCount} changes
                ${activity.significantChanges.length > 0 ? ` (${activity.significantChanges.length} significant)` : ''}
              </div>
            </div>
          `).join('')}
        </div>
      `;
    }
  }

  renderRecoveryTab(postMortem) {
    const recoveryOptions = document.getElementById('recovery-options');
    const archivedVersions = document.getElementById('archived-versions');
    
    // Recovery options
    if (postMortem.recoveryOptions) {
      recoveryOptions.innerHTML = `
        <h4>Recovery Options</h4>
        ${postMortem.recoveryOptions.map(option => {
          const confidence = Math.round(option.confidence * 100);
          const confidenceClass = confidence > 70 ? 'high-confidence' : '';
          
          return `
            <div class="recovery-option ${confidenceClass}">
              <div class="recovery-option-content">
                <div class="recovery-option-title">${option.description}</div>
                <div class="recovery-option-description">${option.action}</div>
              </div>
              <div class="recovery-confidence">${confidence}%</div>
            </div>
          `;
        }).join('')}
      `;
    }
    
    // Archived versions
    if (postMortem.archivedVersions) {
      archivedVersions.innerHTML = `
        <h4>Archived Versions</h4>
        ${postMortem.archivedVersions.length > 0 ? `
          ${postMortem.archivedVersions.map(version => `
            <div class="archived-version">
              <div class="version-type">${version.type.replace(/_/g, ' ').toUpperCase()}</div>
              <div class="version-location">${version.location}</div>
              ${version.timestamp ? `<div class="version-timestamp">${new Date(version.timestamp).toLocaleString()}</div>` : ''}
              ${version.note ? `<div class="version-note">${version.note}</div>` : ''}
            </div>
          `).join('')}
        ` : '<p>No archived versions found.</p>'}
      `;
    }
  }

  renderTimelineTab(postMortem) {
    // Render timeline chart using Chart.js
    const canvas = document.getElementById('timelineChart');
    const ctx = canvas.getContext('2d');
    
    if (this.timelineChart) {
      this.timelineChart.destroy();
    }
    
    if (postMortem.forensicAnalysis && postMortem.forensicAnalysis.timeline) {
      const timeline = postMortem.forensicAnalysis.timeline;
      
      this.timelineChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: timeline.map(item => new Date(item.timestamp).toLocaleDateString()),
          datasets: [{
            label: 'Activity Level',
            data: timeline.map(item => item.significance === 'high' ? 2 : 1),
            borderColor: 'rgb(0, 122, 255)',
            backgroundColor: 'rgba(0, 122, 255, 0.1)',
            tension: 0.1,
            fill: true
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              beginAtZero: true,
              max: 3,
              ticks: {
                callback: function(value) {
                  const labels = ['', 'Low Activity', 'High Activity', ''];
                  return labels[value] || '';
                }
              }
            }
          },
          plugins: {
            title: {
              display: true,
              text: 'Activity Timeline Leading to Disappearance'
            }
          }
        }
      });
    } else {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#666';
      ctx.font = '16px Arial';
      ctx.textAlign = 'center';
      ctx.fillText('No timeline data available', canvas.width / 2, canvas.height / 2);
    }
  }

  formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  showError(message) {
    // Simple error display - could be enhanced with toast notifications
    alert('Error: ' + message);
  }
}

// Global functions for HTML onclick handlers
function showPostMortemDetail(postMortemId) {
  if (window.postMortemManager) {
    window.postMortemManager.showPostMortemDetail(postMortemId);
  }
}

function closePostMortemModal() {
  document.getElementById('postMortemModal').style.display = 'none';
}

function switchTab(tabName) {
  // Hide all tab panes
  document.querySelectorAll('.tab-pane').forEach(pane => {
    pane.classList.remove('active');
  });
  
  // Remove active class from all tab buttons
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.remove('active');
  });
  
  // Show selected tab pane
  document.getElementById(`${tabName}-tab`).classList.add('active');
  
  // Add active class to clicked button
  event.target.classList.add('active');
}

async function scanForMissing() {
  try {
    const response = await fetch('/api/post-mortem/scan-missing', {
      method: 'POST'
    });
    
    const data = await response.json();
    
    if (data.success) {
      alert(`Scan complete!\nScanned: ${data.scannedFiles} files\nMissing: ${data.missingFiles} files\nNew post-mortems created: ${data.newPostMortems}`);
      
      if (data.newPostMortems > 0) {
        window.postMortemManager.loadPostMortems();
      }
    } else {
      alert('Scan failed: ' + data.error);
    }
  } catch (error) {
    console.error('Error scanning for missing files:', error);
    alert('Scan failed: Could not connect to server');
  }
}

function createPostMortem() {
  document.getElementById('createPostMortemModal').style.display = 'block';
}

function closeCreateModal() {
  document.getElementById('createPostMortemModal').style.display = 'none';
  document.getElementById('create-post-mortem-form').reset();
}

async function submitCreatePostMortem() {
  const form = document.getElementById('create-post-mortem-form');
  const formData = new FormData(form);
  
  const data = {
    filePath: formData.get('filePath'),
    lastSeen: formData.get('lastSeen'),
    url: formData.get('url'),
    size: formData.get('size') ? parseInt(formData.get('size')) : null
  };
  
  if (!data.filePath) {
    alert('File path is required');
    return;
  }
  
  try {
    const response = await fetch('/api/post-mortem/create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    });
    
    const result = await response.json();
    
    if (result.success) {
      alert(`Post-mortem created successfully!\nSuspected cause: ${result.suspectedCause.description}\nRecovery options: ${result.recoveryOptions}`);
      closeCreateModal();
      window.postMortemManager.loadPostMortems();
    } else {
      alert('Failed to create post-mortem: ' + result.error);
    }
  } catch (error) {
    console.error('Error creating post-mortem:', error);
    alert('Failed to create post-mortem: Could not connect to server');
  }
}

async function exportPostMortemPDF() {
  if (!window.postMortemManager.currentPostMortem) return;
  
  try {
    const response = await fetch(`/api/post-mortem/${window.postMortemManager.currentPostMortem.id}/export-pdf`, {
      method: 'POST'
    });
    
    const data = await response.json();
    
    if (data.success) {
      alert(`Post-mortem report exported successfully!\nFile: ${data.filename}`);
      // Could open download link: window.open(data.downloadUrl);
    } else {
      alert('Export failed: ' + data.error);
    }
  } catch (error) {
    console.error('Error exporting post-mortem:', error);
    alert('Export failed: Could not connect to server');
  }
}

async function updatePostMortemStatus() {
  if (!window.postMortemManager.currentPostMortem) return;
  
  const newStatus = prompt('Enter new status (active, resolved, archived):', 
    window.postMortemManager.currentPostMortem.status);
  
  if (!newStatus) return;
  
  const notes = prompt('Add notes (optional):');
  
  try {
    const response = await fetch(`/api/post-mortem/${window.postMortemManager.currentPostMortem.id}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        status: newStatus,
        notes: notes
      })
    });
    
    const data = await response.json();
    
    if (data.success) {
      alert('Post-mortem updated successfully!');
      closePostMortemModal();
      window.postMortemManager.loadPostMortems();
    } else {
      alert('Update failed: ' + data.error);
    }
  } catch (error) {
    console.error('Error updating post-mortem:', error);
    alert('Update failed: Could not connect to server');
  }
}

// Initialize the post-mortem manager when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  window.postMortemManager = new PostMortemManager();
});
