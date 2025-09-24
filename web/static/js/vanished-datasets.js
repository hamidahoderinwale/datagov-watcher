// Vanished Datasets Page JavaScript

let vanishedData = [];
let filteredData = [];
let currentPage = 1;
const itemsPerPage = 20;

// Load vanished datasets data
async function loadVanishedDatasets() {
    try {
        showLoading();
        
        // Fetch vanished datasets from API
        const response = await fetch('/api/vanished');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        vanishedData = data.vanished_datasets || [];
        filteredData = [...vanishedData];
        
        updateStats();
        populateFilters();
        renderTable();
        renderPagination();
        
    } catch (error) {
        console.error('Error loading vanished datasets:', error);
        showError('Failed to load vanished datasets. Please try again.');
    }
}

// Update statistics
function updateStats() {
    const totalVanished = vanishedData.length;
    const recentVanished = vanishedData.filter(dataset => {
        const lastSeen = new Date(dataset.last_seen);
        const weekAgo = new Date();
        weekAgo.setDate(weekAgo.getDate() - 7);
        return lastSeen >= weekAgo;
    }).length;
    
    const agenciesAffected = new Set(vanishedData.map(dataset => dataset.agency)).size;
    const lastUpdated = new Date().toLocaleString();
    
    document.getElementById('total-vanished').textContent = totalVanished.toLocaleString();
    document.getElementById('recent-vanished').textContent = recentVanished.toLocaleString();
    document.getElementById('agencies-affected').textContent = agenciesAffected.toLocaleString();
    document.getElementById('last-updated').textContent = lastUpdated;
}

// Populate filter dropdowns
function populateFilters() {
    const agencyFilter = document.getElementById('agency-filter');
    const agencies = [...new Set(vanishedData.map(dataset => dataset.agency))].sort();
    
    // Clear existing options except the first one
    agencyFilter.innerHTML = '<option value="">All Agencies</option>';
    
    agencies.forEach(agency => {
        const option = document.createElement('option');
        option.value = agency;
        option.textContent = agency;
        agencyFilter.appendChild(option);
    });
}

// Render the table
function renderTable() {
    const tbody = document.getElementById('vanished-table-body');
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = filteredData.slice(startIndex, endIndex);
    
    if (pageData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">No vanished datasets found</td></tr>';
        return;
    }
    
    tbody.innerHTML = pageData.map(dataset => `
        <tr>
            <td class="col-id">${dataset.dataset_id}</td>
            <td class="col-title">${dataset.title || 'N/A'}</td>
            <td class="col-agency">${dataset.agency || 'N/A'}</td>
            <td class="col-last-seen">${formatDate(dataset.last_seen)}</td>
            <td class="col-status">
                <span class="status-badge status-${dataset.status?.toLowerCase() || 'unknown'}">
                    ${dataset.status || 'UNKNOWN'}
                </span>
            </td>
            <td class="col-actions">
                <a href="${dataset.archive_url || '#'}" class="action-btn" target="_blank">
                    View Archive
                </a>
            </td>
        </tr>
    `).join('');
}

// Render pagination
function renderPagination() {
    const pagination = document.getElementById('pagination');
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    
    if (totalPages <= 1) {
        pagination.innerHTML = '';
        return;
    }
    
    let paginationHTML = '';
    
    // Previous button
    paginationHTML += `
        <button class="pagination-btn" ${currentPage === 1 ? 'disabled' : ''} 
                onclick="changePage(${currentPage - 1})">
            Previous
        </button>
    `;
    
    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    if (startPage > 1) {
        paginationHTML += `<button class="pagination-btn" onclick="changePage(1)">1</button>`;
        if (startPage > 2) {
            paginationHTML += `<span class="pagination-btn" disabled>...</span>`;
        }
    }
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <button class="pagination-btn ${i === currentPage ? 'active' : ''}" 
                    onclick="changePage(${i})">
                ${i}
            </button>
        `;
    }
    
    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            paginationHTML += `<span class="pagination-btn" disabled>...</span>`;
        }
        paginationHTML += `<button class="pagination-btn" onclick="changePage(${totalPages})">${totalPages}</button>`;
    }
    
    // Next button
    paginationHTML += `
        <button class="pagination-btn" ${currentPage === totalPages ? 'disabled' : ''} 
                onclick="changePage(${currentPage + 1})">
            Next
        </button>
    `;
    
    pagination.innerHTML = paginationHTML;
}

// Change page
function changePage(page) {
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    if (page >= 1 && page <= totalPages) {
        currentPage = page;
        renderTable();
        renderPagination();
    }
}

// Filter data
function filterData() {
    const searchTerm = document.getElementById('search-input').value.toLowerCase();
    const agencyFilter = document.getElementById('agency-filter').value;
    const statusFilter = document.getElementById('status-filter').value;
    const timeFilter = document.getElementById('time-filter').value;
    
    filteredData = vanishedData.filter(dataset => {
        // Search filter
        if (searchTerm && !dataset.title?.toLowerCase().includes(searchTerm) && 
            !dataset.agency?.toLowerCase().includes(searchTerm) &&
            !dataset.dataset_id?.toLowerCase().includes(searchTerm)) {
            return false;
        }
        
        // Agency filter
        if (agencyFilter && dataset.agency !== agencyFilter) {
            return false;
        }
        
        // Status filter
        if (statusFilter && dataset.status !== statusFilter) {
            return false;
        }
        
        // Time filter
        if (timeFilter) {
            const lastSeen = new Date(dataset.last_seen);
            const daysAgo = new Date();
            daysAgo.setDate(daysAgo.getDate() - parseInt(timeFilter));
            if (lastSeen < daysAgo) {
                return false;
            }
        }
        
        return true;
    });
    
    currentPage = 1;
    renderTable();
    renderPagination();
}

// Show loading state
function showLoading() {
    document.getElementById('vanished-table-body').innerHTML = 
        '<tr><td colspan="6" class="loading">Loading vanished datasets...</td></tr>';
}

// Show error state
function showError(message) {
    document.getElementById('vanished-table-body').innerHTML = 
        `<tr><td colspan="6" class="loading" style="color: var(--error);">${message}</td></tr>`;
}

// Format date
function formatDate(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleDateString();
}

// Event listeners
document.addEventListener('DOMContentLoaded', function() {
    // Search input
    document.getElementById('search-input').addEventListener('input', filterData);
    
    // Filter selects
    document.getElementById('agency-filter').addEventListener('change', filterData);
    document.getElementById('status-filter').addEventListener('change', filterData);
    document.getElementById('time-filter').addEventListener('change', filterData);
});


