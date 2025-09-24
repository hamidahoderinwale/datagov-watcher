// Tag Gallery JavaScript functionality

let allTags = [];
let filteredTags = [];

// Load tag gallery data
async function loadTagGallery() {
    try {
        const response = await fetch('/api/tags');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        allTags = data.tags || [];
        filteredTags = [...allTags];
        
        renderTagGrid();
        
    } catch (error) {
        console.error('Error loading tag gallery:', error);
        showError('Failed to load tag gallery data');
    }
}

// Render tag grid
function renderTagGrid() {
    const grid = document.getElementById('tag-grid');
    if (!grid) return;
    
    if (filteredTags.length === 0) {
        grid.innerHTML = '<div class="loading">No tags found matching your criteria</div>';
        return;
    }
    
    grid.innerHTML = filteredTags.map(tag => createTagCard(tag)).join('');
}

// Create tag card HTML
function createTagCard(tag) {
    const categoryClass = tag.category || 'default';
    const volatility = tag.volatility || 0.5;
    const datasetCount = tag.dataset_count || 0;
    
    return `
        <div class="tag-card" onclick="openTagDetail('${tag.name}')">
            <div class="tag-header">
                <h3 class="tag-name">${tag.name}</h3>
                <span class="tag-category ${categoryClass}">${categoryClass}</span>
            </div>
            
            <div class="tag-stats">
                <div class="tag-stat">
                    <div class="tag-stat-value">${datasetCount.toLocaleString()}</div>
                    <div class="tag-stat-label">Datasets</div>
                </div>
                <div class="tag-stat">
                    <div class="tag-stat-value">${volatility.toFixed(2)}</div>
                    <div class="tag-stat-label">Volatility</div>
                </div>
            </div>
            
            <div class="tag-description">
                ${tag.description || `Datasets from ${tag.name}`}
            </div>
            
            <div class="tag-actions">
                <button class="tag-btn" onclick="event.stopPropagation(); viewTagDatasets('${tag.name}')">
                    View Datasets
                </button>
                <button class="tag-btn primary" onclick="event.stopPropagation(); openTagDetail('${tag.name}')">
                    Details
                </button>
            </div>
        </div>
    `;
}

// Search and filter tags
function searchTags() {
    const searchTerm = document.getElementById('tag-search')?.value.toLowerCase() || '';
    const categoryFilter = document.getElementById('category-filter')?.value || '';
    const sortFilter = document.getElementById('sort-filter')?.value || 'count';
    
    filteredTags = allTags.filter(tag => {
        const matchesSearch = !searchTerm || 
            tag.name.toLowerCase().includes(searchTerm) ||
            (tag.description && tag.description.toLowerCase().includes(searchTerm));
        
        const matchesCategory = !categoryFilter || tag.category === categoryFilter;
        
        return matchesSearch && matchesCategory;
    });
    
    // Sort tags
    sortTags(sortFilter);
    
    renderTagGrid();
}

// Sort tags
function sortTags(sortBy) {
    filteredTags.sort((a, b) => {
        switch (sortBy) {
            case 'name':
                return a.name.localeCompare(b.name);
            case 'volatility':
                return (b.volatility || 0) - (a.volatility || 0);
            case 'count':
            default:
                return (b.dataset_count || 0) - (a.dataset_count || 0);
        }
    });
}

// Clear tag filters
function clearTagFilters() {
    const searchInput = document.getElementById('tag-search');
    const categoryFilter = document.getElementById('category-filter');
    const sortFilter = document.getElementById('sort-filter');
    
    if (searchInput) searchInput.value = '';
    if (categoryFilter) categoryFilter.value = '';
    if (sortFilter) sortFilter.value = 'count';
    
    filteredTags = [...allTags];
    sortTags('count');
    renderTagGrid();
}

// Open tag detail modal
async function openTagDetail(tagName) {
    try {
        const response = await fetch(`/api/tags/${encodeURIComponent(tagName)}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        displayTagDetail(data);
        
    } catch (error) {
        console.error('Error loading tag details:', error);
        showError('Failed to load tag details');
    }
}

// Display tag detail modal
function displayTagDetail(tagData) {
    const modal = document.getElementById('tag-detail-modal');
    const title = document.getElementById('tag-detail-title');
    const metrics = document.getElementById('tag-metrics');
    const datasets = document.getElementById('tag-datasets');
    
    if (!modal || !title || !metrics || !datasets) return;
    
    // Set title
    title.textContent = tagData.name;
    
    // Set metrics
    metrics.innerHTML = `
        <div class="tag-metric">
            <div class="tag-metric-value">${(tagData.dataset_count || 0).toLocaleString()}</div>
            <div class="tag-metric-label">Total Datasets</div>
        </div>
        <div class="tag-metric">
            <div class="tag-metric-value">${(tagData.available_count || 0).toLocaleString()}</div>
            <div class="tag-metric-label">Available</div>
        </div>
        <div class="tag-metric">
            <div class="tag-metric-value">${(tagData.unavailable_count || 0).toLocaleString()}</div>
            <div class="tag-metric-label">Unavailable</div>
        </div>
        <div class="tag-metric">
            <div class="tag-metric-value">${(tagData.volatility || 0).toFixed(2)}</div>
            <div class="tag-metric-label">Volatility</div>
        </div>
    `;
    
    // Set datasets
    if (tagData.datasets && tagData.datasets.length > 0) {
        datasets.innerHTML = `
            <h3>Recent Datasets</h3>
            <div class="dataset-list">
                ${tagData.datasets.slice(0, 10).map(dataset => `
                    <div class="dataset-item">
                        <div class="dataset-info">
                            <div class="dataset-title">${dataset.title || 'Untitled Dataset'}</div>
                            <div class="dataset-agency">${dataset.agency || 'Unknown Agency'}</div>
                        </div>
                        <span class="dataset-status ${dataset.availability || 'unknown'}">${(dataset.availability || 'unknown').toUpperCase()}</span>
                    </div>
                `).join('')}
            </div>
        `;
    } else {
        datasets.innerHTML = '<div class="loading">No datasets found for this tag</div>';
    }
    
    // Show modal
    modal.style.display = 'block';
}

// Close tag detail modal
function closeTagDetail() {
    const modal = document.getElementById('tag-detail-modal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// Open info modal
function openInfoModal() {
    const modal = document.getElementById('info-modal');
    if (modal) {
        modal.style.display = 'block';
    }
}

// Close info modal
function closeInfoModal() {
    const modal = document.getElementById('info-modal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// View tag datasets (navigate to catalog with filter)
function viewTagDatasets(tagName) {
    window.location.href = `/catalog?tag=${encodeURIComponent(tagName)}`;
}

// Show error message
function showError(message) {
    const grid = document.getElementById('tag-grid');
    if (grid) {
        grid.innerHTML = `<div class="error">${message}</div>`;
    }
}

// Event listeners
document.addEventListener('DOMContentLoaded', function() {
    // Search input
    const searchInput = document.getElementById('tag-search');
    if (searchInput) {
        searchInput.addEventListener('input', searchTags);
    }
    
    // Filter selects
    const filterElements = ['category-filter', 'sort-filter'];
    filterElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', searchTags);
        }
    });
    
    // Modal close on outside click
    const tagModal = document.getElementById('tag-detail-modal');
    if (tagModal) {
        tagModal.addEventListener('click', function(event) {
            if (event.target === tagModal) {
                closeTagDetail();
            }
        });
    }
    
    const infoModal = document.getElementById('info-modal');
    if (infoModal) {
        infoModal.addEventListener('click', function(event) {
            if (event.target === infoModal) {
                closeInfoModal();
            }
        });
    }
    
    // Modal close on escape key
    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape') {
            closeTagDetail();
            closeInfoModal();
        }
    });
});