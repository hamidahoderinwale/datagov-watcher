# DataGov Watcher

A comprehensive dataset monitoring and analysis system for tracking government data availability, changes, and trends across multiple agencies and platforms.

## Overview

DataGov Watcher is a robust monitoring system that tracks dataset states, availability, and metadata changes across government data portals. It provides real-time insights, historical analysis, and comprehensive reporting on government data transparency and accessibility.

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
- **License Classification**: Intelligent license detection and categorization
- **Content Analysis**: Analyze dataset descriptions and metadata patterns
- **Automated Alerts**: Notify users of significant changes or issues
- **Export Capabilities**: Generate reports in multiple formats (CSV, HTML, JSON)

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
   python -c "from src.database.database_manager import DatabaseManager; DatabaseManager().initialize_database()"
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
- Contact: [Your Contact Information]

## Acknowledgments

- Built for government transparency and data accessibility
- Inspired by the need for better dataset monitoring and analysis
- Thanks to the open-source community for the tools and libraries used

## Roadmap

- [ ] Enhanced machine learning for content analysis
- [ ] Multi-language support
- [ ] Advanced reporting features
- [ ] Integration with additional data portals
- [ ] Mobile application
- [ ] API rate limiting and authentication
- [ ] Real-time notifications and alerts

---

**DataGov Watcher** - Monitoring government data transparency, one dataset at a time.