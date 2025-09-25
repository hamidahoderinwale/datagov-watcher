// Main JavaScript functionality for Concordance Dataset State Historian

// Global variables
let allDatasets = [];
let filteredDatasets = [];
let currentPage = 1;
let currentFilter = 'all';
let itemsPerPage = 20;
let totalPages = 1;
let autoRefresh = true;
let refreshInterval;
let totalDatasetCount = 0;

// Socket.IO connection
const socket = io();

// Connection status handling
socket.on('connect', function() {
    updateConnectionStatus('connected', 'Connected');
    document.getElementById('realTimeIndicator').textContent = 'Online';
    document.getElementById('realTimeIndicator').classList.remove('offline');
    document.getElementById('realTimeIndicator').classList.add('online');
});

socket.on('disconnect', function() {
    updateConnectionStatus('disconnected', 'Disconnected');
    document.getElementById('realTimeIndicator').textContent = 'Offline';
    document.getElementById('realTimeIndicator').classList.remove('online');
    document.getElementById('realTimeIndicator').classList.add('offline');
});

// Real-time updates
socket.on('stats_update', function(data) {
    showUpdateIndicator();
    updateStats(data);
});

socket.on('connected', function(data) {
    console.log('Connected to server:', data.message);
});

// Utility functions
function updateConnectionStatus(status, text) {
    const statusElement = document.getElementById('connectionStatus');
    if (statusElement) {
        statusElement.textContent = text;
        statusElement.className = `connection-status ${status}`;
    }
}

function showUpdateIndicator() {
    const indicator = document.getElementById('updateIndicator');
    if (indicator) {
        indicator.classList.add('show');
        setTimeout(() => {
            indicator.classList.remove('show');
        }, 2000); // Show for 2 seconds instead of 1
    }
}

function updateStats(data) {
    if (data.total_datasets !== undefined) {
        const element = document.getElementById('totalDatasets');
        if (element) element.textContent = data.total_datasets.toLocaleString();
    }
    if (data.total_snapshots !== undefined) {
        const element = document.getElementById('totalSnapshots');
        if (element) element.textContent = data.total_snapshots.toLocaleString();
    }
    if (data.total_diffs !== undefined) {
        const element = document.getElementById('totalDiffs');
        if (element) element.textContent = data.total_diffs.toLocaleString();
    }
    if (data.available !== undefined) {
        const element = document.getElementById('availableDatasets');
        if (element) element.textContent = data.available.toLocaleString();
    }
}

// Navigation functions
function showPage(pageId) {
    // Hide all pages
    const pages = document.querySelectorAll('.page');
    pages.forEach(page => page.classList.remove('active'));
    
    // Show selected page
    const targetPage = document.getElementById(pageId);
    if (targetPage) {
        targetPage.classList.add('active');
    }
    
    // Update navigation
    const navLinks = document.querySelectorAll('.nav-link');
    navLinks.forEach(link => link.classList.remove('active'));
    
    const activeLink = document.querySelector(`[onclick="showPage('${pageId}')"]`);
    if (activeLink) {
        activeLink.classList.add('active');
    }
    
    // Load page-specific data
    if (pageId === 'dashboard') {
        loadDashboardData();
    } else if (pageId === 'timeline') {
        loadTimelineData();
    } else if (pageId === 'data-viewer') {
        loadDataViewerData();
    } else if (pageId === 'vanished') {
        loadVanishedData();
    }
}

// Dashboard functions
async function loadDashboardData() {
    try {
        // Load stats
        const statsResponse = await fetch('/api/stats');
        const stats = await statsResponse.json();
        
        document.getElementById('totalDatasets').textContent = stats.unique_datasets || 0;
        document.getElementById('totalSnapshots').textContent = stats.total_snapshots || 0;
        document.getElementById('unavailableDatasets').textContent = stats.availability_stats?.unavailable || 0;
        document.getElementById('availableDatasets').textContent = stats.availability_stats?.available || 0;
        
        // Load timeline data
        await loadTimelineData();
        
        // Load datasets
        const datasetsResponse = await fetch('/api/datasets');
        const datasetsData = await datasetsResponse.json();
        
        // Handle new paginated response format
        if (datasetsData.datasets) {
            allDatasets = datasetsData.datasets;
            filteredDatasets = [...allDatasets];
            
            // Update total count display
            totalDatasetCount = datasetsData.pagination?.total_count || allDatasets.length;
            console.log('Total dataset count:', totalDatasetCount);
            updateTableCount(totalDatasetCount);
        } else {
            // Fallback for old format
            allDatasets = datasetsData;
            filteredDatasets = [...allDatasets];
            totalDatasetCount = allDatasets.length;
            updateTableCount(totalDatasetCount);
        }
        
        // Populate agency filter
        populateAgencyFilter();
        
        // Render table
        renderTable();
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        const tableBody = document.getElementById('datasetsTableBody');
        if (tableBody) {
            tableBody.innerHTML = '<tr><td colspan="8" class="error">Error loading datasets. Please try again.</td></tr>';
        }
    }
}

function populateAgencyFilter() {
    const agencies = [...new Set(allDatasets.map(d => d.agency).filter(Boolean))].sort();
    const agencyFilter = document.getElementById('agencyFilter');
    
    if (agencyFilter) {
        // Clear existing options except the first one
        while (agencyFilter.children.length > 1) {
            agencyFilter.removeChild(agencyFilter.lastChild);
        }
        
        agencies.forEach(agency => {
            const option = document.createElement('option');
            option.value = agency;
            option.textContent = agency;
            agencyFilter.appendChild(option);
        });
    }
}

function renderTable() {
    const tableBody = document.getElementById('datasetsTableBody');
    if (!tableBody) return;
    
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = filteredDatasets.slice(startIndex, endIndex);
    
    if (pageData.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="6" class="loading">No datasets found</td></tr>';
        return;
    }
    
    tableBody.innerHTML = pageData.map(dataset => `
        <tr onclick="openDatasetModal('${dataset.dataset_id}')">
            <td class="col-id">${formatDatasetId(dataset.dataset_id)}</td>
            <td class="col-title">${formatTitle(dataset.title)}</td>
            <td class="col-agency">${formatAgency(dataset.agency)}</td>
            <td class="col-status">${formatStatus(dataset.availability)}</td>
            <td class="col-last-checked">${formatDate(dataset.last_checked)}</td>
            <td class="col-response">${formatResponseTime(dataset.response_time_ms || dataset.dimension_computation_time_ms)}</td>
        </tr>
    `).join('');
    
    updatePagination();
    updateTableCount();
}

// Helper functions for better formatting
function formatDatasetId(id) {
    if (!id) return '<span class="na-value">N/A</span>';
    return `<span title="${id}">${id.substring(0, 8)}...</span>`;
}

function formatTitle(title) {
    if (!title) return '<span class="na-value">N/A</span>';
    const maxLength = 50;
    if (title.length > maxLength) {
        return `<span title="${title}">${title.substring(0, maxLength)}...</span>`;
    }
    return title;
}

function formatAgency(agency) {
    if (!agency) return '<span class="na-value">N/A</span>';
    const maxLength = 30;
    if (agency.length > maxLength) {
        return `<span title="${agency}">${agency.substring(0, maxLength)}...</span>`;
    }
    return agency;
}

function formatStatus(status) {
    if (!status) return '<span class="na-value">N/A</span>';
    const statusClass = status === 'available' ? 'status-available' : 
                       status === 'unavailable' ? 'status-unavailable' : 'na-value';
    const statusIcon = status === 'available' ? 'OK' : 
                      status === 'unavailable' ? 'ERROR' : 'WARNING';
    return `<span class="${statusClass}">${statusIcon} ${status}</span>`;
}

function formatNumber(value) {
    if (!value || value === 0 || value === '0') return '<span class="na-value">N/A</span>';
    return value.toLocaleString();
}

function formatDate(date) {
    if (!date) return '<span class="na-value">N/A</span>';
    try {
        return new Date(date).toLocaleDateString();
    } catch (e) {
        return '<span class="na-value">N/A</span>';
    }
}

function formatResponseTime(time) {
    if (!time || time === 0) return '<span class="na-value">N/A</span>';
    return `${time}ms`;
}

function updateTableCount(totalCount = null) {
    const countElement = document.getElementById('tableCount');
    if (countElement) {
        const total = totalCount || totalDatasetCount || allDatasets.length;
        const displayText = `Showing ${filteredDatasets.length} of ${total.toLocaleString()} datasets`;
        countElement.textContent = displayText;
        console.log('Updated table count:', displayText, 'totalDatasetCount:', totalDatasetCount);
    } else {
        console.warn('Table count element not found');
    }
}

// Additional helper functions for data viewer
function formatResourceFormat(format) {
    if (!format || format === '') return '<span class="na-value">N/A</span>';
    return format.toUpperCase();
}

function formatFileSize(size) {
    if (!size || size === 0) return '<span class="na-value">N/A</span>';
    
    const units = ['B', 'KB', 'MB', 'GB'];
    let unitIndex = 0;
    let fileSize = size;
    
    while (fileSize >= 1024 && unitIndex < units.length - 1) {
        fileSize /= 1024;
        unitIndex++;
    }
    
    return `${fileSize.toFixed(1)} ${units[unitIndex]}`;
}

function formatLastModified(modified) {
    if (!modified || modified === '') return '<span class="na-value">N/A</span>';
    try {
        const date = new Date(modified);
        return date.toLocaleDateString();
    } catch (e) {
        return '<span class="na-value">N/A</span>';
    }
}

function updatePagination() {
    totalPages = Math.ceil(filteredDatasets.length / itemsPerPage);
    
    const paginationContainer = document.getElementById('pagination');
    if (!paginationContainer) return;
    
    let paginationHTML = '';
    
    // Previous button
    paginationHTML += `<button onclick="changePage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}>Previous</button>`;
    
    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    for (let i = startPage; i <= endPage; i++) {
        const isCurrent = i === currentPage;
        paginationHTML += `<button onclick="changePage(${i})" class="${isCurrent ? 'current-page' : ''}" ${isCurrent ? 'disabled' : ''}>${i}</button>`;
    }
    
    // Next button
    paginationHTML += `<button onclick="changePage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}>Next</button>`;
    
    paginationContainer.innerHTML = paginationHTML;
}

function changePage(page) {
    if (page < 1 || page > totalPages) return;
    currentPage = page;
    renderTable();
}

// Filter functions
function applyFilters() {
    const searchTerm = document.getElementById('searchInput')?.value.toLowerCase() || '';
    const agencyFilter = document.getElementById('agencyFilter')?.value || '';
    const statusFilter = document.getElementById('statusFilter')?.value || '';
    const dataTypeFilter = document.getElementById('dataTypeFilter')?.value || '';
    const dateAddedFilter = document.getElementById('dateAddedFilter')?.value || '';
    const fileSizeFilter = document.getElementById('fileSizeFilter')?.value || '';
    const licenseFilter = document.getElementById('licenseFilter')?.value || '';
    const sortByFilter = document.getElementById('sortByFilter')?.value || 'last_checked';
    
    filteredDatasets = allDatasets.filter(dataset => {
        // Search filter
        const matchesSearch = !searchTerm || 
            (dataset.title && dataset.title.toLowerCase().includes(searchTerm)) ||
            (dataset.agency && dataset.agency.toLowerCase().includes(searchTerm)) ||
            (dataset.dataset_id && dataset.dataset_id.toLowerCase().includes(searchTerm));
        
        // Agency filter
        const matchesAgency = !agencyFilter || dataset.agency === agencyFilter;
        
        // Status filter
        const matchesStatus = !statusFilter || dataset.availability === statusFilter;
        
        // Data type filter
        const matchesDataType = !dataTypeFilter || dataset.resource_format === dataTypeFilter;
        
        // Date added filter
        const matchesDateAdded = !dateAddedFilter || checkDateAdded(dataset.created_at, dateAddedFilter);
        
        // File size filter
        const matchesFileSize = !fileSizeFilter || checkFileSize(dataset.file_size, fileSizeFilter);
        
        // License filter
        const matchesLicense = !licenseFilter || checkLicense(dataset.license, licenseFilter);
        
        return matchesSearch && matchesAgency && matchesStatus && matchesDataType && matchesDateAdded && matchesFileSize && matchesLicense;
    });
    
    // Apply sorting
    sortDatasets(sortByFilter);
    
    currentPage = 1;
    renderTable();
}

function checkDateAdded(createdAt, filter) {
    if (!createdAt) return false;
    
    const now = new Date();
    const created = new Date(createdAt);
    const diffTime = now - created;
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    
    switch (filter) {
        case 'today':
            return diffDays <= 1;
        case 'week':
            return diffDays <= 7;
        case 'month':
            return diffDays <= 30;
        case 'quarter':
            return diffDays <= 90;
        case 'year':
            return diffDays <= 365;
        default:
            return true;
    }
}

function checkFileSize(fileSize, filter) {
    if (!fileSize || fileSize === 0) return false;
    
    const sizeInMB = fileSize / (1024 * 1024);
    
    switch (filter) {
        case 'small':
            return sizeInMB < 1;
        case 'medium':
            return sizeInMB >= 1 && sizeInMB < 10;
        case 'large':
            return sizeInMB >= 10 && sizeInMB < 100;
        case 'xlarge':
            return sizeInMB >= 100;
        default:
            return true;
    }
}

function checkLicense(license, filter) {
    if (!license) return filter === 'Unknown';
    
    // Handle both old string format and new structured format
    let licenseInfo;
    if (typeof license === 'string') {
        // Legacy format - try to parse as JSON first
        try {
            licenseInfo = JSON.parse(license);
        } catch (e) {
            // Fallback to simple string matching for backward compatibility
            return checkLicenseLegacy(license, filter);
        }
    } else {
        licenseInfo = license;
    }
    
    // Use structured license information
    const category = licenseInfo.category || 'unknown';
    const name = licenseInfo.name || '';
    
    switch (filter) {
        case 'Unknown':
            return category === 'unknown';
        case 'Public Domain':
            return category === 'public_domain';
        case 'CC0':
            return category === 'cc0';
        case 'CC BY':
            return category === 'cc_by';
        case 'CC BY-SA':
            return category === 'cc_by_sa';
        case 'CC BY-NC':
            return category === 'cc_by_nc';
        case 'CC BY-NC-SA':
            return category === 'cc_by_nc_sa';
        case 'MIT':
            return category === 'mit';
        case 'Apache':
            return category === 'apache';
        case 'GPL':
            return category === 'gpl';
        case 'BSD':
            return category === 'bsd';
        case 'Proprietary':
            return category === 'proprietary';
        case 'Open':
            return licenseInfo.is_open === true;
        case 'Other':
            return category !== 'unknown' && !['public_domain', 'cc0', 'cc_by', 'cc_by_sa', 'cc_by_nc', 'cc_by_nc_sa', 'mit', 'apache', 'gpl', 'bsd', 'proprietary'].includes(category);
        default:
            return true;
    }
}

function checkLicenseLegacy(license, filter) {
    // Legacy function for backward compatibility with old string-based licenses
    if (!license) return filter === 'Unknown';
    
    const licenseLower = license.toLowerCase();
    
    switch (filter) {
        case 'Unknown':
            return !license || license === 'Unknown' || license === 'null' || license === '';
        case 'Public Domain':
            return licenseLower.includes('public domain') || licenseLower.includes('publicdomain');
        case 'CC0':
            return licenseLower.includes('cc0') || licenseLower.includes('creative commons zero');
        case 'CC BY':
            return licenseLower.includes('cc by') && !licenseLower.includes('sa') && !licenseLower.includes('nc');
        case 'CC BY-SA':
            return licenseLower.includes('cc by-sa') || licenseLower.includes('cc by sa');
        case 'CC BY-NC':
            return licenseLower.includes('cc by-nc') || licenseLower.includes('cc by nc');
        case 'MIT':
            return licenseLower.includes('mit');
        case 'Apache':
            return licenseLower.includes('apache');
        case 'GPL':
            return licenseLower.includes('gpl');
        case 'Other':
            return license && license !== 'Unknown' && license !== 'null' && license !== '' &&
                   !licenseLower.includes('public domain') && !licenseLower.includes('cc0') &&
                   !licenseLower.includes('cc by') && !licenseLower.includes('mit') &&
                   !licenseLower.includes('apache') && !licenseLower.includes('gpl');
        default:
            return true;
    }
}

function sortDatasets(sortBy) {
    filteredDatasets.sort((a, b) => {
        switch (sortBy) {
            case 'title':
                return (a.title || '').localeCompare(b.title || '');
            case 'agency':
                return (a.agency || '').localeCompare(b.agency || '');
            case 'created_at':
                return new Date(b.created_at || 0) - new Date(a.created_at || 0);
            case 'file_size':
                return (b.file_size || 0) - (a.file_size || 0);
            case 'response_time':
                return (b.response_time_ms || 0) - (a.response_time_ms || 0);
            case 'last_checked':
            default:
                return new Date(b.last_checked || 0) - new Date(a.last_checked || 0);
        }
    });
}

function clearFilters() {
    // Clear all filter inputs
    const filterInputs = [
        'searchInput', 'agencyFilter', 'statusFilter', 'dataTypeFilter', 
        'dateAddedFilter', 'fileSizeFilter', 'licenseFilter'
    ];
    
    filterInputs.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.value = '';
        }
    });
    
    // Reset sort to default
    const sortByFilter = document.getElementById('sortByFilter');
    if (sortByFilter) {
        sortByFilter.value = 'last_checked';
    }
    
    applyFilters();
}

// Timeline functions
async function loadTimelineData() {
    try {
        const response = await fetch('/api/timeline');
        const data = await response.json();
        
        // Update timeline chart placeholder
        const timelineChart = document.getElementById('timelineChart');
        if (timelineChart) {
            // Extract data from the correct structure
            const summary = data.summary || {};
            const totalDatasets = summary.total_datasets || 0;
            const totalChanges = summary.total_changes || 0;
            const lastUpdated = summary.last_updated || 'N/A';
            
            timelineChart.innerHTML = `
                <div class="text-center">
                    <h3>Timeline Data</h3>
                    <p>Datasets: ${totalDatasets.toLocaleString()}</p>
                    <p>Changes: ${totalChanges.toLocaleString()}</p>
                    <p>Last Updated: ${lastUpdated}</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading timeline data:', error);
        // Show error state
        const timelineChart = document.getElementById('timelineChart');
        if (timelineChart) {
            timelineChart.innerHTML = `
                <div class="text-center">
                    <h3>Timeline Data</h3>
                    <p>Error loading timeline data</p>
                    <p>Please try again later</p>
                </div>
            `;
        }
    }
}

// Data viewer functions
async function loadDataViewerData() {
    try {
        const response = await fetch('/api/datasets');
        const datasets = await response.json();
        
        const dataViewer = document.getElementById('dataViewer');
        if (dataViewer) {
            dataViewer.innerHTML = `
                <div class="data-viewer-header">
                    <h3 class="data-viewer-title">Dataset Data Viewer</h3>
                    <span class="last-updated">Last updated: ${new Date().toLocaleTimeString()}</span>
                </div>
                <div class="data-content">
                    <div class="table-container">
                        <div class="table-header">
                            <h3>Dataset Details</h3>
                            <div class="table-info">
                                <span>Showing ${Math.min(10, datasets.length)} of ${datasets.length} datasets</span>
                            </div>
                        </div>
                        <div class="table-wrapper">
                            <table class="dataset-table">
                                <thead>
                                    <tr>
                                        <th class="col-id">Dataset ID</th>
                                        <th class="col-title">Title</th>
                                        <th class="col-agency">Agency</th>
                                        <th class="col-status">Status</th>
                                        <th class="col-format">Format</th>
                                        <th class="col-size">Size</th>
                                        <th class="col-modified">Last Modified</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${datasets.slice(0, 10).map(dataset => `
                                        <tr>
                                            <td class="col-id">${formatDatasetId(dataset.dataset_id)}</td>
                                            <td class="col-title">${formatTitle(dataset.title)}</td>
                                            <td class="col-agency">${formatAgency(dataset.agency)}</td>
                                            <td class="col-status">${formatStatus(dataset.availability)}</td>
                                            <td class="col-format">${formatResourceFormat(dataset.resource_format)}</td>
                                            <td class="col-size">${formatFileSize(dataset.file_size)}</td>
                                            <td class="col-modified">${formatLastModified(dataset.last_modified)}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error loading data viewer data:', error);
    }
}

// Vanished datasets functions
async function loadVanishedData() {
    try {
        const response = await fetch('/api/vanished-datasets');
        const data = await response.json();
        
        const vanishedContainer = document.getElementById('vanishedContainer');
        if (vanishedContainer) {
            if (data.vanished_datasets && data.vanished_datasets.length > 0) {
                vanishedContainer.innerHTML = `
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Dataset ID</th>
                                    <th>Title</th>
                                    <th>Agency</th>
                                    <th>Last Seen</th>
                                    <th>Status</th>
                                    <th>Archive URL</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.vanished_datasets.map(dataset => `
                                    <tr>
                                        <td>${dataset.dataset_id || 'N/A'}</td>
                                        <td>${dataset.title || 'N/A'}</td>
                                        <td>${dataset.agency || 'N/A'}</td>
                                        <td>${dataset.last_seen_date || 'N/A'}</td>
                                        <td><span class="status-badge unavailable">${dataset.status || 'vanished'}</span></td>
                                        <td>${dataset.archive_url ? `<a href="${dataset.archive_url}" target="_blank">View Archive</a>` : 'N/A'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                `;
            } else {
                vanishedContainer.innerHTML = '<div class="text-center">No vanished datasets found</div>';
            }
        }
    } catch (error) {
        console.error('Error loading vanished data:', error);
    }
}

// Load monitoring statistics
async function loadMonitoringStats() {
    try {
        const response = await fetch('/api/monitoring/stats');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        
        // Update monitoring stats with better fallback handling
        const updateElement = (id, value, fallback = '0') => {
            const element = document.getElementById(id);
            if (element) {
                const displayValue = value !== undefined && value !== null ? value.toLocaleString() : fallback;
                element.textContent = displayValue;
                console.log(`Updated ${id}: ${displayValue}`);
            } else {
                console.warn(`Element not found: ${id}`);
            }
        };
        
        // Live Monitoring stats
        updateElement('monitoringTotal', data.total_checks);
        updateElement('monitoringAvailable', data.monitoring_stats?.available);
        updateElement('monitoringErrors', data.monitoring_stats?.error);
        updateElement('monitoringTimeouts', data.monitoring_stats?.timeout);
        
            // Status breakdown
            updateElement('status403', data.status_codes?.['403']);
            updateElement('status404', data.status_codes?.['404']);
            updateElement('status500', data.status_codes?.['500']);
        
        // Rate limiting stats
        updateElement('rateLimitDomains', data.rate_limiting?.total_domains);
        
        // Calculate active domains (domains that are not in backoff)
        const activeDomains = (data.rate_limiting?.total_domains || 0) - (data.rate_limiting?.domains_with_backoff || 0);
        updateElement('rateLimitActive', activeDomains);
        
        console.log('Monitoring stats updated successfully');
        
    } catch (error) {
        console.error('Error loading monitoring stats:', error);
        
            // Set fallback values with more descriptive text
            const fallbackData = {
                'monitoringTotal': '0',
                'monitoringAvailable': '0', 
                'monitoringErrors': '0',
                'monitoringTimeouts': '0',
                'status403': '0',
                'status404': '0', 
                'status500': '0',
                'rateLimitDomains': '0',
                'rateLimitActive': '0'
            };
        
        Object.entries(fallbackData).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) element.textContent = value;
        });
    }
}

// Update last updated timestamp
function updateLastUpdated() {
    const now = new Date();
    const timeString = now.toLocaleTimeString();
    const dateString = now.toLocaleDateString();
    const lastUpdatedElement = document.querySelector('.last-updated');
    if (lastUpdatedElement) {
        lastUpdatedElement.textContent = `Last updated: ${dateString} ${timeString}`;
    }
}

// Auto-refresh functionality
function startAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
    
    refreshInterval = setInterval(() => {
        if (autoRefresh) {
            loadDashboardData();
            loadMonitoringStats(); // Also refresh monitoring stats
            loadLicenseDistribution(); // Also refresh license distribution
            updateLastUpdated(); // Update timestamp
        }
    }, 30000); // Refresh every 30 seconds
}

function stopAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
}

function toggleAutoRefresh() {
    autoRefresh = !autoRefresh;
    const button = document.getElementById('toggleRefresh');
    if (button) {
        button.textContent = autoRefresh ? 'Stop Auto-refresh' : 'Start Auto-refresh';
        button.className = autoRefresh ? 'btn btn-danger btn-small' : 'btn btn-success btn-small';
    }
    
    if (autoRefresh) {
        startAutoRefresh();
    } else {
        stopAutoRefresh();
    }
}

// Load license distribution data
async function loadLicenseDistribution() {
    try {
        const response = await fetch('/api/licenses');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // Update license statistics
        document.getElementById('totalWithLicenses').textContent = data.summary.known_license_count.toLocaleString();
        document.getElementById('totalUnknownLicenses').textContent = data.summary.unknown_license_count.toLocaleString();
        document.getElementById('licenseCoverage').textContent = data.summary.known_license_percentage + '%';
        
        // Update license breakdown
        const breakdownContainer = document.getElementById('licenseBreakdown');
        if (breakdownContainer) {
            breakdownContainer.innerHTML = data.licenses.map(license => `
                <div class="license-item">
                    <span class="license-name">${license.license}</span>
                    <div style="display: flex; gap: var(--space-4); align-items: center;">
                        <span class="license-count">${license.dataset_count.toLocaleString()} datasets</span>
                        <span class="license-percentage">${license.percentage}%</span>
                    </div>
                </div>
            `).join('');
        }
        
    } catch (error) {
        console.error('Error loading license distribution:', error);
        // Set fallback values
        document.getElementById('totalWithLicenses').textContent = '0';
        document.getElementById('totalUnknownLicenses').textContent = '0';
        document.getElementById('licenseCoverage').textContent = '0%';
        document.getElementById('licenseBreakdown').innerHTML = '<div style="text-align: center; color: #ef4444; padding: 20px;">Error loading license data</div>';
    }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    // Set up event listeners
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('input', applyFilters);
    }
    
    // Filter event listeners
    const filterElements = [
        'agencyFilter', 'statusFilter', 'dataTypeFilter', 
        'dateAddedFilter', 'fileSizeFilter', 'licenseFilter', 'sortByFilter'
    ];
    
    filterElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', applyFilters);
        }
    });
    
    const toggleRefreshBtn = document.getElementById('toggleRefresh');
    if (toggleRefreshBtn) {
        toggleRefreshBtn.addEventListener('click', toggleAutoRefresh);
    }
    
    // Load initial data
    loadDashboardData();
    loadMonitoringStats();
    loadLicenseDistribution();
    updateLastUpdated(); // Set initial timestamp
    
    // Start auto-refresh
    startAutoRefresh();
    
    // Show dashboard by default
    showPage('dashboard');
});

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    stopAutoRefresh();
});

// Modal functions
function openDatasetModal(datasetId) {
    const modal = document.getElementById('datasetModal');
    const modalTitle = document.getElementById('modalTitle');
    const modalContent = document.getElementById('modalContent');
    
    if (!modal || !modalTitle || !modalContent) {
        console.error('Modal elements not found');
        return;
    }
    
    // Show modal
    modal.style.display = 'block';
    modalTitle.textContent = 'Dataset Details';
    modalContent.innerHTML = '<div class="loading">Loading dataset details...</div>';
    
    // Find dataset data
    const dataset = allDatasets.find(d => d.dataset_id === datasetId);
    if (!dataset) {
        modalContent.innerHTML = '<div class="error">Dataset not found</div>';
        return;
    }
    
        // Populate modal with dataset information
        modalContent.innerHTML = `
            <div>
                <h4>${dataset.title || 'N/A'}</h4>
                <p><strong>Dataset ID:</strong> ${dataset.dataset_id || 'N/A'}</p>
                <p><strong>Agency:</strong> ${dataset.agency || 'N/A'}</p>
                <p><strong>Status:</strong> <span class="status-badge ${dataset.availability || 'unknown'}">${(dataset.availability || 'unknown').toUpperCase()}</span></p>
                <p><strong>Last Checked:</strong> ${formatDate(dataset.last_checked) || 'N/A'}</p>
                <p><strong>Response Time:</strong> ${formatResponseTime(dataset.response_time_ms || dataset.dimension_computation_time_ms) || 'N/A'}</p>
                <p><strong>Rows:</strong> ${dataset.row_count ? dataset.row_count.toLocaleString() : 'N/A'}</p>
                <p><strong>Columns:</strong> ${dataset.column_count ? dataset.column_count.toLocaleString() : 'N/A'}</p>
                <p><strong>File Size:</strong> ${dataset.file_size ? formatFileSize(dataset.file_size) : 'N/A'}</p>
                <p><strong>Content Type:</strong> ${dataset.content_type || 'N/A'}</p>
                <p><strong>Resource Format:</strong> ${dataset.resource_format || 'N/A'}</p>
                <p><strong>Last Modified:</strong> ${formatDate(dataset.last_modified) || 'N/A'}</p>
                <p><strong>URL:</strong> ${dataset.url ? `<a href="${dataset.url}" target="_blank">${dataset.url}</a>` : 'N/A'}</p>
                <p><strong>Description:</strong> ${dataset.description || 'No description available'}</p>
                <p><strong>Schema:</strong> ${dataset.schema ? `<pre style="font-size: 12px; background: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto;">${JSON.stringify(JSON.parse(dataset.schema), null, 2)}</pre>` : 'N/A'}</p>
                
                <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #e5e7eb;">
                    <h5 style="margin-bottom: 15px; color: #374151;">Historical Timeline</h5>
                    <div id="wayback-timeline" style="max-height: 300px; overflow-y: auto; background: #f9fafb; padding: 15px; border-radius: 4px;">
                        <div class="loading">Loading timeline...</div>
                    </div>
                    <div style="margin-top: 10px;">
                        <button onclick="openWaybackPage('${dataset.dataset_id}')" style="padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px;">
                            View Full Timeline
                        </button>
                    </div>
                </div>
            </div>
        `;
        
        // Load wayback timeline data
        loadWaybackTimeline(datasetId);
}

function closeModal() {
    const modal = document.getElementById('datasetModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// Load wayback timeline for dataset modal
async function loadWaybackTimeline(datasetId) {
    try {
        const response = await fetch(`/api/wayback/timeline/${datasetId}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        const timelineContainer = document.getElementById('wayback-timeline');
        
        if (!timelineContainer) return;
        
        if (data.timeline && data.timeline.length > 0) {
            timelineContainer.innerHTML = `
                <div style="margin-bottom: 15px; font-size: 14px; color: #6b7280;">
                    <strong>${data.total_snapshots}</strong> snapshots • <strong>${data.change_events}</strong> change events
                </div>
                <div style="max-height: 200px; overflow-y: auto;">
                    ${data.timeline.slice(0, 5).map(item => `
                        <div style="margin-bottom: 10px; padding: 8px; background: white; border-radius: 4px; border-left: 3px solid ${item.status === 'available' ? '#10b981' : '#ef4444'};">
                            <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                                ${new Date(item.date).toLocaleDateString()}
                            </div>
                            <div style="font-size: 13px; color: #374151; margin-bottom: 4px;">
                                <span style="padding: 2px 6px; background: ${item.status === 'available' ? '#d1fae5' : '#fee2e2'}; color: ${item.status === 'available' ? '#065f46' : '#991b1b'}; border-radius: 3px; font-size: 11px; text-transform: uppercase;">
                                    ${item.status}
                                </span>
                            </div>
                            ${item.changes.length > 0 ? `
                                <div style="font-size: 12px; color: #6b7280;">
                                    ${item.changes.slice(0, 2).map(change => `• ${change}`).join('<br>')}
                                    ${item.changes.length > 2 ? `<br>• +${item.changes.length - 2} more changes` : ''}
                                </div>
                            ` : ''}
                        </div>
                    `).join('')}
                </div>
            `;
        } else {
            timelineContainer.innerHTML = '<div style="text-align: center; color: #6b7280; padding: 20px;">No timeline data available</div>';
        }
        
    } catch (error) {
        console.error('Error loading wayback timeline:', error);
        const timelineContainer = document.getElementById('wayback-timeline');
        if (timelineContainer) {
            timelineContainer.innerHTML = '<div style="text-align: center; color: #ef4444; padding: 20px;">Error loading timeline data</div>';
        }
    }
}

// Open wayback page for specific dataset
function openWaybackPage(datasetId) {
    // Close the modal first
    closeModal();
    
    // Navigate to wayback page with dataset pre-filled
    window.location.href = `/wayback?dataset=${datasetId}`;
}

function formatFileSize(bytes) {
    if (!bytes) return 'N/A';
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

// Close modal when clicking outside of it
window.addEventListener('click', function(event) {
    const modal = document.getElementById('datasetModal');
    if (event.target === modal) {
        closeModal();
    }
});

// Close modal with Escape key
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        closeModal();
    }
});
