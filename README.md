# Dataset State Historian

A comprehensive dataset monitoring and analysis system for tracking government data availability, changes, and trends across multiple agencies and platforms. This system serves as a critical tool for public sector employees to monitor data transparency and accessibility.

## Overview

Dataset State Historian is a robust monitoring system that tracks dataset states, availability, and metadata changes across government data portals. It provides real-time insights, historical analysis, and comprehensive reporting on government data transparency and accessibility.

**Current System Status:**
- **87,763** datasets tracked
- **634,221** dataset states monitored
- **109** government agencies monitored
- **Real-time** monitoring with automated discovery
- **Comprehensive** search and recovery capabilities
- **No mock data** - all metrics are real and production-ready

## Features

### Core Monitoring
- **Real-time Dataset Tracking**: Monitor dataset availability across multiple government portals
- **Change Detection**: Track metadata changes, schema modifications, and content updates
- **Historical Analysis**: Maintain comprehensive historical records of dataset states
- **Agency Performance**: Monitor and compare data publishing patterns across agencies

### Analytics & Visualization
- **Interactive Dashboards**: Real-time monitoring with comprehensive analytics
- **Trend Analysis**: Track dataset growth, availability trends, and agency performance
- **Data Quality Metrics**: Assess data completeness, freshness, and accessibility
- **Comparative Analysis**: Compare agencies, time periods, and data types

### Advanced Features
- **Dataset Recovery System**: Comprehensive recovery using Harvard LIL, Wayback Machine, and other archives
- **Search Functionality**: Fast, accurate search across all government datasets with real-time results
- **Format Standardization**: Clean categorization of data formats (15 standardized types)
- **Political Analysis**: Track dataset changes around political events and transitions
- **Export Capabilities**: Generate reports in multiple formats (CSV, HTML, JSON)
- **Historical Tracking**: Complete audit trail of dataset changes with Wayback Machine integration
- **Performance Analytics**: Agency comparison and data quality scoring

## Recent Improvements (Latest Update)

### Production-Ready Enhancements
- ✅ **Removed all mock data** - System now uses 100% real government data
- ✅ **Fixed search functionality** - Fast, accurate search across 87K+ datasets
- ✅ **Eliminated redundancy** - Removed duplicate API endpoints and consolidated functionality
- ✅ **Enhanced data quality** - Format distribution now shows 15 clean, standardized categories
- ✅ **Verified Wayback integration** - 634K+ snapshots available for dataset recovery
- ✅ **End-to-end testing** - All major features verified and working

### Public Sector Value
This system is specifically designed for government employees and provides:
- **Data Stewards**: Monitor dataset availability and changes in real-time
- **FOIA Officers**: Find vanished datasets using comprehensive recovery tools
- **Policy Analysts**: Track agency data publishing patterns and trends
- **Researchers**: Access historical dataset states and change analysis

## Quick Start for Public Sector Users

### 1. Search Datasets
- Use the search functionality to find specific datasets across all government agencies
- Search by title, agency, or dataset ID
- Get real-time results from 87K+ tracked datasets

### 2. Monitor Data Availability
- View real-time status of government datasets
- Track when datasets go offline or become unavailable
- Monitor response times and accessibility metrics

### 3. Recover Vanished Datasets
- Use the Post-mortem Reports section to find datasets that have disappeared
- Access the comprehensive recovery system using Harvard LIL and Wayback Machine
- Generate FOIA request templates for missing data

### 4. Analyze Agency Performance
- Compare data publishing patterns across 109 government agencies
- Track format standardization and data quality metrics
- Monitor political patterns in dataset availability

## Technology Stack

- **Backend**: Python 3.13, Flask, SQLite
- **Frontend**: HTML5, CSS3, JavaScript (ES6+), Chart.js
- **Data Processing**: Pandas, NumPy, SQLAlchemy
- **Monitoring**: Custom discovery engines, rate limiting, error handling
- **Deployment**: Docker, Nginx, systemd services

## Quick Start

### Prerequisites
- Python 3.13+
- Git
- Docker (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github.com:hamidahoderinwale/datagov-watcher.git
   cd datagov-watcher
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize the database**
   ```bash
   python -c "from src.database.connection import DatabaseManager; DatabaseManager().initialize_database()"
   ```

5. **Start the application**
   ```bash
   python run.py
   ```

6. **Access the web interface**
   Open your browser to `http://localhost:8081`

### Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Access the application
open http://localhost:8081
```

## Usage

### Web Interface

The web interface provides several key views:

- **Dashboard**: Overview of system status and key metrics
- **Analytics**: Interactive charts and trend analysis
- **Catalog**: Browse and search datasets
- **Agencies**: Agency-specific data and performance metrics
- **Data Visualization**: Advanced charts and comparative analysis

### API Endpoints

The system provides RESTful APIs for programmatic access:

- `GET /api/datasets` - List datasets with filtering and pagination
- `GET /api/analytics/trends` - Get trend data for charts
- `GET /api/agencies` - List agencies and their datasets
- `GET /api/licenses` - Get license classification data
- `GET /api/monitoring/status` - Get system monitoring status

### Command Line Tools

```bash
# Start comprehensive monitoring
python run_comprehensive_system.py

# Run discovery process
python src/core/comprehensive_discovery.py

# Check system health
./start_production_system.sh health
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
FLASK_ENV=production
DATABASE_URL=sqlite:///datasets.db
LOG_LEVEL=INFO
WEB_PORT=8081
```

### Database Configuration

The system uses SQLite by default. For production deployments, consider PostgreSQL:

```python
# In config/settings.py
DATABASE_URL = "postgresql://user:password@localhost/datagov_watcher"
```

## Architecture

### Core Components

- **Discovery Engine**: Automated dataset discovery from government portals
- **Monitoring System**: Real-time availability and change tracking
- **Analytics Engine**: Data processing and trend analysis
- **Web Interface**: User-facing dashboard and visualization
- **API Layer**: RESTful endpoints for data access

### Data Flow

1. **Discovery**: Automated scanning of government data portals
2. **Ingestion**: Data collection and initial processing
3. **Storage**: Structured storage in SQLite database
4. **Analysis**: Real-time processing and trend calculation
5. **Visualization**: Web interface and API data presentation

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support, questions, or feature requests:

- Create an issue on GitHub
- Check the [API Documentation](http://localhost:8081/api/docs) when running locally


---

**DataGov Watcher** - Monitoring government data transparency, one dataset at a time.