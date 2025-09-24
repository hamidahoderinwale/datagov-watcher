// Dataset Profile JavaScript

let currentDatasetId = null;

async function loadDatasetProfile(datasetId) {
    currentDatasetId = datasetId;
    
    try {
        // Load dataset profile
        const response = await fetch(`/api/enhanced/datasets/${datasetId}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const profile = await response.json();
        populateDatasetProfile(profile);
        
        // Load timeline embed
        loadTimelineEmbed(datasetId);
        
        // Load context
        loadDatasetContext(datasetId);
        
    } catch (error) {
        console.error('Error loading dataset profile:', error);
        showError('Failed to load dataset profile');
    }
}

function populateDatasetProfile(profile) {
    // Update header
    document.getElementById('dataset-title').textContent = profile.title;
    document.getElementById('dataset-agency').textContent = profile.agency;
    document.getElementById('dataset-status').textContent = profile.status;
    document.getElementById('dataset-license').textContent = profile.license;
    document.getElementById('dataset-url').href = profile.url;
    
    // Update stats
    document.getElementById('snapshot-count').textContent = profile.snapshot_count;
    document.getElementById('volatility-score').textContent = profile.volatility_score.toFixed(2);
    document.getElementById('license-flips').textContent = profile.license_flip_count;
    document.getElementById('schema-churn').textContent = profile.schema_churn_count;
    document.getElementById('content-drift').textContent = profile.content_drift_score.toFixed(2);
    document.getElementById('schema-stability').textContent = (profile.schema_stability * 100).toFixed(0) + '%';
    
    // Update schema table
    populateSchemaTable(profile.schema_columns);
    
    // Update event chips
    populateEventChips(profile);
    
    // Update related datasets
    populateRelatedDatasets(profile.related_datasets);
}

function populateSchemaTable(columns) {
    const tbody = document.querySelector('#schema-table tbody');
    tbody.innerHTML = '';
    
    columns.forEach(column => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${column.name}</td>
            <td>${column.type}</td>
            <td>
                <div class="stability-bar">
                    <div class="stability-fill" style="width: ${column.stability * 100}%"></div>
                </div>
                <span class="stability-text">${(column.stability * 100).toFixed(0)}%</span>
            </td>
        `;
        tbody.appendChild(row);
    });
}

function populateEventChips(profile) {
    const container = document.getElementById('event-chips');
    container.innerHTML = '';
    
    // Create sample events based on profile data
    const events = [];
    
    if (profile.license_flip_count > 0) {
        events.push({
            type: 'LICENSE_CHANGED',
            severity: 'medium',
            count: profile.license_flip_count
        });
    }
    
    if (profile.schema_churn_count > 0) {
        events.push({
            type: 'SCHEMA_CHANGED',
            severity: 'high',
            count: profile.schema_churn_count
        });
    }
    
    if (profile.content_drift_score > 0.5) {
        events.push({
            type: 'CONTENT_DRIFT',
            severity: 'high',
            count: 1
        });
    }
    
    if (profile.status === 'vanished') {
        events.push({
            type: 'VANISHED',
            severity: 'critical',
            count: 1
        });
    }
    
    events.forEach(event => {
        const chip = document.createElement('div');
        chip.className = `event-chip ${event.severity}`;
        chip.textContent = `${event.type} (${event.count})`;
        container.appendChild(chip);
    });
}

function populateRelatedDatasets(relatedDatasets) {
    const container = document.getElementById('related-datasets');
    container.innerHTML = '';
    
    if (!relatedDatasets || relatedDatasets.length === 0) {
        container.innerHTML = '<div class="no-related">No related datasets found</div>';
        return;
    }
    
    relatedDatasets.forEach(dataset => {
        const item = document.createElement('div');
        item.className = 'related-dataset';
        
        // Handle both old format (string IDs) and new format (objects with id and title)
        const datasetId = typeof dataset === 'string' ? dataset : dataset.id;
        const datasetTitle = typeof dataset === 'string' ? dataset : dataset.title;
        
        item.innerHTML = `
            <a href="/datasets/${datasetId}" title="${datasetId}">${datasetTitle}</a>
        `;
        container.appendChild(item);
    });
}

async function loadTimelineEmbed(datasetId) {
    try {
        const response = await fetch(`/api/enhanced/datasets/${datasetId}/timeline-embed`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const timeline = await response.json();
        populateTimelineEmbed(timeline);
        
    } catch (error) {
        console.error('Error loading timeline embed:', error);
        document.getElementById('timeline-embed').innerHTML = '<div class="timeline-error">Failed to load timeline</div>';
    }
}

function populateTimelineEmbed(timeline) {
    const container = document.getElementById('timeline-embed');
    container.innerHTML = '';
    
    // Create a simple timeline visualization
    const timelineDiv = document.createElement('div');
    timelineDiv.className = 'timeline-visualization';
    
    // Create cells for the timeline
    timeline.cells.forEach(cell => {
        const cellDiv = document.createElement('div');
        cellDiv.className = `timeline-cell ${cell.changed ? 'changed' : 'unchanged'}`;
        cellDiv.title = `${cell.field}: ${cell.value}`;
        timelineDiv.appendChild(cellDiv);
    });
    
    container.appendChild(timelineDiv);
}

async function loadDatasetContext(datasetId) {
    try {
        const response = await fetch(`/api/enhanced/datasets/${datasetId}/context`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const context = await response.json();
        populateContextSidebar(context);
        
    } catch (error) {
        console.error('Error loading dataset context:', error);
    }
}

function populateContextSidebar(context) {
    // Populate usage refs
    const usageList = document.getElementById('usage-list');
    usageList.innerHTML = '';
    
    if (!context.usage_refs || context.usage_refs.length === 0) {
        usageList.innerHTML = '<div class="no-results">No usage references found</div>';
    } else {
        context.usage_refs.forEach(ref => {
            const item = document.createElement('div');
            item.className = 'usage-item';
            item.innerHTML = `
                <div class="item-header">
                    <a href="${ref.url}" target="_blank" class="item-title">${ref.title}</a>
                    <span class="item-type">${ref.type}</span>
                </div>
                ${ref.snippet ? `<div class="item-snippet">${ref.snippet}</div>` : ''}
                ${ref.date ? `<div class="item-date">${formatDate(ref.date)}</div>` : ''}
            `;
            usageList.appendChild(item);
        });
    }
    
    // Populate policy refs
    const policyList = document.getElementById('policy-list');
    policyList.innerHTML = '';
    
    if (!context.policy_refs || context.policy_refs.length === 0) {
        policyList.innerHTML = '<div class="no-results">No policy references found</div>';
    } else {
        context.policy_refs.forEach(ref => {
            const item = document.createElement('div');
            item.className = 'policy-item';
            item.innerHTML = `
                <div class="item-header">
                    <a href="${ref.url}" target="_blank" class="item-title">${ref.title}</a>
                    <span class="item-agency">${ref.agency}</span>
                </div>
                ${ref.snippet ? `<div class="item-snippet">${ref.snippet}</div>` : ''}
                ${ref.date ? `<div class="item-date">${formatDate(ref.date)}</div>` : ''}
            `;
            policyList.appendChild(item);
        });
    }
    
    // Populate news mentions
    const newsList = document.getElementById('news-list');
    newsList.innerHTML = '';
    
    if (!context.news_mentions || context.news_mentions.length === 0) {
        newsList.innerHTML = '<div class="no-results">No news mentions found</div>';
    } else {
        context.news_mentions.forEach(mention => {
            const item = document.createElement('div');
            item.className = 'news-item';
            item.innerHTML = `
                <div class="item-header">
                    <a href="${mention.url}" target="_blank" class="item-title">${mention.title}</a>
                    <span class="item-source">${mention.source}</span>
                </div>
                ${mention.snippet ? `<div class="item-snippet">${mention.snippet}</div>` : ''}
                ${mention.date ? `<div class="item-date">${formatDate(mention.date)}</div>` : ''}
            `;
            newsList.appendChild(item);
        });
    }
}

function switchTab(tabName) {
    // Remove active class from all tabs and panels
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));
    
    // Add active class to selected tab and panel
    document.querySelector(`[onclick="switchTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`${tabName}-tab`).classList.add('active');
}

function toggleTimeline() {
    const modal = document.getElementById('timeline-modal');
    modal.style.display = 'block';
    
    // Load full timeline
    loadFullTimeline(currentDatasetId);
}

function closeTimeline() {
    const modal = document.getElementById('timeline-modal');
    modal.style.display = 'none';
}

async function loadFullTimeline(datasetId) {
    try {
        const response = await fetch(`/api/timeline/chromogram/${datasetId}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const timeline = await response.json();
        populateFullTimeline(timeline);
        
    } catch (error) {
        console.error('Error loading full timeline:', error);
        document.getElementById('full-timeline').innerHTML = '<div class="timeline-error">Failed to load full timeline</div>';
    }
}

function populateFullTimeline(timeline) {
    const container = document.getElementById('full-timeline');
    container.innerHTML = '';
    
    // Create a more detailed timeline visualization
    const timelineDiv = document.createElement('div');
    timelineDiv.className = 'full-timeline-visualization';
    
    // Add timeline bands
    timeline.bands.forEach(band => {
        const bandDiv = document.createElement('div');
        bandDiv.className = 'timeline-band';
        bandDiv.innerHTML = `<h4>${band.name}</h4>`;
        
        const cellsDiv = document.createElement('div');
        cellsDiv.className = 'timeline-cells';
        
        timeline.cells.forEach(cell => {
            if (cell.band === band.name) {
                const cellDiv = document.createElement('div');
                cellDiv.className = `timeline-cell ${cell.changed ? 'changed' : 'unchanged'}`;
                cellDiv.title = `${cell.field}: ${cell.value}`;
                cellsDiv.appendChild(cellDiv);
            }
        });
        
        bandDiv.appendChild(cellsDiv);
        timelineDiv.appendChild(bandDiv);
    });
    
    container.appendChild(timelineDiv);
}

function showError(message) {
    // Create error notification
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-notification';
    errorDiv.textContent = message;
    errorDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #fef2f2;
        color: #dc2626;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #fecaca;
        z-index: 1000;
    `;
    
    document.body.appendChild(errorDiv);
    
    // Remove after 5 seconds
    setTimeout(() => {
        errorDiv.remove();
    }, 5000);
}

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('timeline-modal');
    if (event.target === modal) {
        closeTimeline();
    }
}
