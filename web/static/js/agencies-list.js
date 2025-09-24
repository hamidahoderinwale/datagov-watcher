// Agencies List JavaScript
console.log('Agencies list JavaScript loaded');

let allAgencies = [];
let filteredAgencies = [];

async function loadAgenciesList() {
    try {
        // Show loading state
        const container = document.getElementById('agency-grid');
        container.innerHTML = '<div style="grid-column: 1 / -1; text-align: center; padding: 2rem; color: #6b7280;">Loading agencies...</div>';
        
        const response = await fetch('/api/agencies');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        allAgencies = data.agencies || [];
        filteredAgencies = [...allAgencies];
        
        console.log('Loaded agencies:', allAgencies.length);
        console.log('First agency:', allAgencies[0]);
        renderAgencyGrid();
        
    } catch (error) {
        console.error('Error loading agencies list:', error);
        showError('Failed to load agencies list');
    }
}

function renderAgencyGrid() {
    const container = document.getElementById('agency-grid');
    container.innerHTML = '';
    
    console.log('Rendering agencies grid with', filteredAgencies.length, 'agencies');
    
    if (filteredAgencies.length === 0) {
        container.innerHTML = `
            <div style="grid-column: 1 / -1; text-align: center; padding: 2rem; color: #6b7280;">
                No agencies found matching your criteria
            </div>
        `;
        return;
    }
    
    filteredAgencies.forEach(agency => {
        const agencyCard = createAgencyCard(agency);
        container.appendChild(agencyCard);
    });
}

function createAgencyCard(agency) {
    const card = document.createElement('div');
    card.className = 'agency-card';
    
    // Calculate availability rate percentage for the bar
    const availabilityPercent = agency.availability_rate || 0;
    
    // Calculate vanished rate
    const vanishedRate = ((agency.total_datasets - agency.available_datasets) / agency.total_datasets) * 100;
    
    // Determine vanished rate class
    let vanishedRateClass = 'low';
    if (vanishedRate > 30) vanishedRateClass = 'high';
    else if (vanishedRate > 10) vanishedRateClass = 'medium';
    
    card.innerHTML = `
        <div class="agency-header">
            <h3 class="agency-name">${agency.agency}</h3>
        </div>
        <div class="agency-stats">
            <div class="agency-stat">
                <div class="agency-stat-value">${agency.total_datasets.toLocaleString()}</div>
                <div class="agency-stat-label">Total</div>
            </div>
            <div class="agency-stat">
                <div class="agency-stat-value">${agency.available_datasets.toLocaleString()}</div>
                <div class="agency-stat-label">Available</div>
            </div>
            <div class="agency-stat">
                <div class="agency-stat-value">${(agency.total_datasets - agency.available_datasets).toLocaleString()}</div>
                <div class="agency-stat-label">Unavailable</div>
            </div>
        </div>
        <div class="volatility-bar">
            <div class="volatility-fill" style="width: ${availabilityPercent}%"></div>
        </div>
        <div class="agency-meta">
            <span>Availability: ${availabilityPercent.toFixed(1)}%</span>
            <span class="vanished-rate ${vanishedRateClass}">${vanishedRate.toFixed(1)}% unavailable</span>
        </div>
        <div class="agency-actions">
            <a href="/agencies/${encodeURIComponent(agency.agency)}" class="agency-btn primary">View Details</a>
            <button class="agency-btn" onclick="browseAgencyDatasets('${agency.agency}')">Browse Datasets</button>
        </div>
    `;
    
    return card;
}

function searchAgencies() {
    const searchTerm = document.getElementById('agency-search').value.toLowerCase();
    const sortFilter = document.getElementById('sort-filter').value;
    
    filteredAgencies = allAgencies.filter(agency => {
        return agency.agency.toLowerCase().includes(searchTerm);
    });
    
    // Sort based on selected criteria
    switch (sortFilter) {
        case 'datasets':
            filteredAgencies.sort((a, b) => b.total_datasets - a.total_datasets);
            break;
        case 'volatility':
            // Sort by availability rate (lower is more volatile)
            filteredAgencies.sort((a, b) => a.availability_rate - b.availability_rate);
            break;
        case 'vanished':
            // Sort by unavailable rate
            const getUnavailableRate = (agency) => ((agency.total_datasets - agency.available_datasets) / agency.total_datasets) * 100;
            filteredAgencies.sort((a, b) => getUnavailableRate(b) - getUnavailableRate(a));
            break;
    }
    
    renderAgencyGrid();
}

function browseAgencyDatasets(agencyName) {
    // Redirect to catalog with agency filter
    window.location.href = `/catalog?agency=${encodeURIComponent(agencyName)}`;
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

// Make functions globally available
window.loadAgenciesList = loadAgenciesList;
window.searchAgencies = searchAgencies;
window.browseAgencyDatasets = browseAgencyDatasets;

// Add event listeners when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('agency-search');
    const sortFilter = document.getElementById('sort-filter');
    
    if (searchInput) {
        searchInput.addEventListener('input', searchAgencies);
    }
    
    if (sortFilter) {
        sortFilter.addEventListener('change', searchAgencies);
    }
    
    // Load agencies list when DOM is ready
    loadAgenciesList();
});
