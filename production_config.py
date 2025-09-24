"""
Production Configuration for Dataset State Historian
"""

import os
from datetime import timedelta

class ProductionConfig:
    """Production configuration settings"""
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'your-production-secret-key-change-this')
    DEBUG = False
    TESTING = False
    
    # Database Configuration
    DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///datasets.db')
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Redis Configuration
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = REDIS_URL
    
    # Security Configuration
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-jwt-secret-key-change-this')
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=24)
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=30)
    
    # Rate Limiting
    RATELIMIT_STORAGE_URL = REDIS_URL
    RATELIMIT_DEFAULT = "1000 per hour"
    
    # CORS Configuration
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', 'http://localhost:3000,http://localhost:8089').split(',')
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'logs/app.log')
    
    # Backup Configuration
    BACKUP_DIR = os.environ.get('BACKUP_DIR', '/app/backups')
    BACKUP_RETENTION_DAYS = int(os.environ.get('BACKUP_RETENTION_DAYS', '30'))
    
    # Monitoring Configuration
    MONITORING_ENABLED = os.environ.get('MONITORING_ENABLED', 'true').lower() == 'true'
    METRICS_PORT = int(os.environ.get('METRICS_PORT', '9090'))
    
    # Email Configuration (for notifications)
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'smtp.gmail.com')
    MAIL_PORT = int(os.environ.get('MAIL_PORT', '587'))
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS', 'true').lower() == 'true'
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME', '')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD', '')
    
    # API Configuration
    API_RATE_LIMIT = os.environ.get('API_RATE_LIMIT', '1000 per hour')
    API_RATE_LIMIT_PER_USER = os.environ.get('API_RATE_LIMIT_PER_USER', '100 per hour')
    
    # Session Configuration
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # File Upload Configuration
    MAX_CONTENT_LENGTH = int(os.environ.get('MAX_CONTENT_LENGTH', '16 * 1024 * 1024'))  # 16MB
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', '/app/uploads')
    
    # Cache Configuration
    CACHE_DEFAULT_TIMEOUT = int(os.environ.get('CACHE_DEFAULT_TIMEOUT', '300'))  # 5 minutes
    CACHE_KEY_PREFIX = 'dataset_monitor:'
    
    # Database Connection Pool
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': int(os.environ.get('DB_POOL_SIZE', '20')),
        'pool_timeout': int(os.environ.get('DB_POOL_TIMEOUT', '30')),
        'pool_recycle': int(os.environ.get('DB_POOL_RECYCLE', '3600')),
        'max_overflow': int(os.environ.get('DB_MAX_OVERFLOW', '30'))
    }

class DevelopmentConfig:
    """Development configuration settings"""
    
    # Flask Configuration
    SECRET_KEY = 'dev-secret-key'
    DEBUG = True
    TESTING = False
    
    # Database Configuration
    DATABASE_URL = 'sqlite:///datasets.db'
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    
    # Redis Configuration (optional for dev)
    REDIS_URL = 'redis://localhost:6379/1'
    CACHE_TYPE = 'simple'
    
    # Security Configuration
    JWT_SECRET_KEY = 'dev-jwt-secret-key'
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=24)
    
    # Rate Limiting (more lenient for dev)
    RATELIMIT_DEFAULT = "10000 per hour"
    
    # CORS Configuration
    CORS_ORIGINS = ['http://localhost:3000', 'http://localhost:8089', 'http://127.0.0.1:8089']
    
    # Logging Configuration
    LOG_LEVEL = 'DEBUG'
    LOG_FILE = 'logs/dev.log'
    
    # Backup Configuration
    BACKUP_DIR = 'backups'
    BACKUP_RETENTION_DAYS = 7
    
    # Monitoring Configuration
    MONITORING_ENABLED = True
    METRICS_PORT = 9090
    
    # Session Configuration
    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # Cache Configuration
    CACHE_DEFAULT_TIMEOUT = 60  # 1 minute
    CACHE_KEY_PREFIX = 'dev_dataset_monitor:'

# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': DevelopmentConfig
}

def get_config():
    """Get configuration based on environment"""
    env = os.environ.get('FLASK_ENV', 'development')
    return config.get(env, DevelopmentConfig)

