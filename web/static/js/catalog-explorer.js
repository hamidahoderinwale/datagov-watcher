// Catalog Explorer JavaScript - Dataset State Historian

class CatalogExplorer {
    constructor() {
        this.datasets = [];
        this.filteredDatasets = [];
        this.currentPage = 1;
        this.itemsPerPage = 20;
        this.filters = {
            search: '',
            agency: '',
            status: '',
            event: ''
        };
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.loadDatasets();
        this.loadAgencies();
    }
    
    setupEventListeners() {
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.performSearch();
                }
            });
        }
        
        const filterSelects = ['agencyFilter', 'statusFilter', 'eventFilter'];
        filterSelects.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', () => {
                    this.applyFilters();
                });
            }
        });
    }
    
    // View switching functionality
    setView(viewType) {
        try {
            const gridSection = document.getElementById('grid-view-section');
            const tableSection = document.getElementById('table-view-section');
            const gridBtn = document.getElementById('grid-view');
            const tableBtn = document.getElementById('table-view');
            
            console.log('Switching to view:', viewType);
            
            if (viewType === 'grid') {
                if (gridSection) gridSection.style.display = 'block';
                if (tableSection) tableSection.style.display = 'none';
                if (gridBtn) gridBtn.classList.add('active');
                if (tableBtn) tableBtn.classList.remove('active');
                console.log('Switched to grid view');
            } else if (viewType === 'table') {
                if (gridSection) gridSection.style.display = 'none';
                if (tableSection) tableSection.style.display = 'block';
                if (gridBtn) gridBtn.classList.remove('active');
                if (tableBtn) tableBtn.classList.add('active');
                console.log('Switched to table view');
            } else {
                console.warn('Unknown view type:', viewType);
            }
        } catch (error) {
            console.error('Error switching view:', error);
        }
    }
    
    async loadDatasets() {
        try {
            this.showLoading(true);
            const response = await fetch('/api/datasets');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            // Handle the data structure from the API
            this.datasets = data.datasets || data;
            this.filteredDatasets = [...this.datasets];
            this.renderTable();
            this.updateTableCount();
            
        } catch (error) {
            console.error('Error loading datasets:', error);
            this.showError('Failed to load datasets: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    async loadAgencies() {
        try {
            const response = await fetch('/api/agencies');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            const agencySelect = document.getElementById('agencyFilter');
            if (agencySelect && data.agencies) {
                agencySelect.innerHTML = '<option value="">All Agencies</option>';
                
                data.agencies.forEach(agency => {
                    const option = document.createElement('option');
                    option.value = agency.agency || agency.name;
                    option.textContent = agency.agency || agency.name;
                    agencySelect.appendChild(option);
                });
            }
        } catch (error) {
            console.error('Error loading agencies:', error);
        }
    }
    
    performSearch() {
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            this.filters.search = searchInput.value.toLowerCase().trim();
            this.applyFilters();
        }
    }
    
    applyFilters() {
        this.filters.agency = document.getElementById('agencyFilter')?.value || '';
        this.filters.status = document.getElementById('statusFilter')?.value || '';
        this.filters.event = document.getElementById('eventFilter')?.value || '';
        
        this.filteredDatasets = this.datasets.filter(dataset => {
            if (this.filters.search) {
                const searchText = `${dataset.title || ''} ${dataset.agency || ''} ${dataset.dataset_id || ''}`.toLowerCase();
                if (!searchText.includes(this.filters.search)) {
                    return false;
                }
            }
            
            if (this.filters.agency && dataset.agency !== this.filters.agency) {
                return false;
            }
            
            if (this.filters.status) {
                const status = this.getDatasetStatus(dataset);
                if (status !== this.filters.status) {
                    return false;
                }
            }
            
            if (this.filters.event) {
                const volatility = this.getVolatilityLevel(dataset);
                if (volatility !== this.filters.event) {
                    return false;
                }
            }
            
            return true;
        });
        
        this.currentPage = 1;
        this.renderTable();
        this.updateTableCount();
    }
    
    clearFilters() {
        document.getElementById('searchInput').value = '';
        document.getElementById('agencyFilter').value = '';
        document.getElementById('statusFilter').value = '';
        document.getElementById('eventFilter').value = '';
        
        this.filters = {
            search: '',
            agency: '',
            status: '',
            event: ''
        };
        
        this.filteredDatasets = [...this.datasets];
        this.currentPage = 1;
        this.renderTable();
        this.updateTableCount();
    }
    
    getDatasetStatus(dataset) {
        if (dataset.availability === 'available') {
            return 'active';
        } else if (dataset.availability === 'unavailable') {
            return 'vanished';
        } else if (dataset.availability === 'error') {
            return 'error';
        } else {
            return 'unknown';
        }
    }
    
    getVolatilityLevel(dataset) {
        const volatility = dataset.avg_volatility || 0;
        if (volatility > 0.7) return 'high';
        if (volatility > 0.3) return 'medium';
        return 'low';
    }
    
    getVolatilityStars(dataset) {
        const volatility = dataset.avg_volatility || 0;
        const stars = Math.min(5, Math.ceil(volatility * 5));
        
        let html = '<div class="volatility-stars">';
        for (let i = 0; i < 5; i++) {
            const filled = i < stars ? 'filled' : '';
            html += `<span class="volatility-star ${filled}">★</span>`;
        }
        html += '</div>';
        
        return html;
    }
    
    renderTable() {
        const tbody = document.getElementById('datasetsTableBody');
        if (!tbody) return;
        
        if (this.filteredDatasets.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center">No datasets found</td></tr>';
            return;
        }
        
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = Math.min(startIndex + this.itemsPerPage, this.filteredDatasets.length);
        const pageDatasets = this.filteredDatasets.slice(startIndex, endIndex);
        
        tbody.innerHTML = pageDatasets.map(dataset => {
            const status = this.getDatasetStatus(dataset);
            const lastSeen = dataset.last_checked || dataset.created_at ? 
                new Date(dataset.last_checked || dataset.created_at).toLocaleDateString() : 'Unknown';
            
            return `
                <tr class="dataset-row" onclick="catalogExplorer.showDatasetDetails('${dataset.dataset_id}')">
                    <td>
                        <div class="dataset-title">${dataset.title || 'Untitled Dataset'}</div>
                        <div class="dataset-id">${dataset.dataset_id}</div>
                    </td>
                    <td>
                        <div class="dataset-agency">${dataset.agency || 'Unknown Agency'}</div>
                    </td>
                    <td>
                        <span class="status-pill ${status}">${status}</span>
                    </td>
                    <td>
                        ${this.getVolatilityStars(dataset)}
                    </td>
                    <td>
                        ${lastSeen}
                    </td>
                    <td>
                        <button class="action-btn" onclick="event.stopPropagation(); catalogExplorer.showDatasetDetails('${dataset.dataset_id}')">
                            View Details
                        </button>
                    </td>
                </tr>
            `;
        }).join('');
        
        this.renderPagination();
    }
    
    renderPagination() {
        const pagination = document.getElementById('pagination');
        if (!pagination) return;
        
        const totalPages = Math.ceil(this.filteredDatasets.length / this.itemsPerPage);
        
        if (totalPages <= 1) {
            pagination.innerHTML = '';
            return;
        }
        
        let html = '';
        
        html += `<button ${this.currentPage === 1 ? 'disabled' : ''} onclick="catalogExplorer.goToPage(${this.currentPage - 1})">Previous</button>`;
        
        const startPage = Math.max(1, this.currentPage - 2);
        const endPage = Math.min(totalPages, this.currentPage + 2);
        
        if (startPage > 1) {
            html += `<button onclick="catalogExplorer.goToPage(1)">1</button>`;
            if (startPage > 2) {
                html += `<span>...</span>`;
            }
        }
        
        for (let i = startPage; i <= endPage; i++) {
            const active = i === this.currentPage ? 'active' : '';
            html += `<button class="${active}" onclick="catalogExplorer.goToPage(${i})">${i}</button>`;
        }
        
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                html += `<span>...</span>`;
            }
            html += `<button onclick="catalogExplorer.goToPage(${totalPages})">${totalPages}</button>`;
        }
        
        html += `<button ${this.currentPage === totalPages ? 'disabled' : ''} onclick="catalogExplorer.goToPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = html;
    }
    
    goToPage(page) {
        const totalPages = Math.ceil(this.filteredDatasets.length / this.itemsPerPage);
        if (page >= 1 && page <= totalPages) {
            this.currentPage = page;
            this.renderTable();
        }
    }
    
    async showDatasetDetails(datasetId) {
        try {
            this.showLoading(true);
            
            const modal = document.getElementById('datasetModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalContent = document.getElementById('modalContent');
            
            modalTitle.textContent = 'Dataset Details';
            modalContent.innerHTML = '<div class="loading">Loading dataset details...</div>';
            modal.style.display = 'block';
            
            const response = await fetch(`/api/dataset/${datasetId}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            modalContent.innerHTML = this.renderDatasetDetails(data);
            
        } catch (error) {
            console.error('Error loading dataset details:', error);
            const modalContent = document.getElementById('modalContent');
            modalContent.innerHTML = `<div class="error">Failed to load dataset details: ${error.message}</div>`;
        } finally {
            this.showLoading(false);
        }
    }
    
    renderDatasetDetails(data) {
        const dataset = data.dataset || {};
        const timeline = data.timeline || [];
        const diffs = data.diffs || [];
        
        return `
            <div class="dataset-details">
                <div class="dataset-header">
                    <h4>${dataset.title || 'Untitled Dataset'}</h4>
                    <p class="dataset-id">ID: ${dataset.dataset_id}</p>
                    <p class="dataset-agency">Agency: ${dataset.agency || 'Unknown'}</p>
                </div>
                
                <div class="dataset-stats">
                    <div class="stat-item">
                        <span class="stat-label">Status:</span>
                        <span class="status-pill ${this.getDatasetStatus(dataset)}">${this.getDatasetStatus(dataset)}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Snapshots:</span>
                        <span>${timeline.length}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Changes:</span>
                        <span>${diffs.length}</span>
                    </div>
                </div>
                
                <div class="dataset-timeline">
                    <h5>Recent Timeline</h5>
                    <div class="timeline-list">
                        ${timeline.slice(-5).map(snapshot => `
                            <div class="timeline-item">
                                <span class="timeline-date">${new Date(snapshot.date).toLocaleDateString()}</span>
                                <span class="timeline-status">${snapshot.availability}</span>
                                <span class="timeline-details">${snapshot.row_count || 0} rows, ${snapshot.column_count || 0} columns</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
                
                <div class="dataset-actions">
                    <button class="btn btn-primary" onclick="window.open('/datasets/${dataset.dataset_id}', '_blank')">
                        View Full Details
                    </button>
                    <button class="btn btn-secondary" onclick="catalogExplorer.closeModal()">
                        Close
                    </button>
                </div>
            </div>
        `;
    }
    
    closeModal() {
        const modal = document.getElementById('datasetModal');
        modal.style.display = 'none';
    }
    
    updateTableCount() {
        const tableCount = document.getElementById('tableCount');
        if (tableCount) {
            tableCount.textContent = `Showing ${this.filteredDatasets.length} of ${this.datasets.length} datasets`;
        }
    }
    
    showLoading(show) {
        const indicator = document.getElementById('loadingIndicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }
    
    showError(message) {
        const tbody = document.getElementById('datasetsTableBody');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="6" class="error text-center">${message}</td></tr>`;
        }
    }
}

// Global functions for HTML onclick handlers
function performSearch() {
    if (window.catalogExplorer) {
        catalogExplorer.performSearch();
    }
}

function applyFilters() {
    if (window.catalogExplorer) {
        catalogExplorer.applyFilters();
    }
}

function clearFilters() {
    if (window.catalogExplorer) {
        catalogExplorer.clearFilters();
    }
}

function closeModal() {
    if (window.catalogExplorer) {
        catalogExplorer.closeModal();
    }
}

function setView(viewType) {
    if (window.catalogExplorer) {
        catalogExplorer.setView(viewType);
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.catalogExplorer = new CatalogExplorer();
});

// Close modal when clicking outside
window.addEventListener('click', (event) => {
    const modal = document.getElementById('datasetModal');
    if (event.target === modal) {
        closeModal();
    }
});