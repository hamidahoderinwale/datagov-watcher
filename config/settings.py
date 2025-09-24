# Dataset State Historian Configuration
# Database
DATABASE_PATH = "data/database/datasets.db"

# API Settings
API_HOST = "127.0.0.1"
API_PORT = 8081
DEBUG = True

# Data Sources
DATA_GOV_API_URL = "https://catalog.data.gov/api/3/action/package_search"
LIL_API_URL = "https://dataverse.harvard.edu/api"

# Processing
MAX_WORKERS = 4
BATCH_SIZE = 1000

# Logging
LOG_LEVEL = "INFO"
LOG_FILE = "data/logs/app.log"

