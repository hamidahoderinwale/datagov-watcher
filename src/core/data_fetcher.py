"""
Data fetcher for LIL Data.gov Archive and live Data.gov catalog
"""
import requests
import json
import time
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
import logging
import os
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, db_path: str = "datasets.db", api_key: str = None):
        self.db_path = db_path
        self.api_key = api_key or os.getenv('DATA_GOV_API_KEY')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Data.gov Monitor/1.0 (Educational Research)'
        })
        
        # Data.gov CKAN API endpoints
        self.datagov_base_url = "https://catalog.data.gov/api/3/action"
        self.datagov_package_search = f"{self.datagov_base_url}/package_search"
        self.datagov_package_list = f"{self.datagov_base_url}/package_list"
        
        # LIL Archive endpoints (to be implemented)
        self.lil_base_url = "https://lil.law.harvard.edu"
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for storing dataset information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                agency TEXT,
                url TEXT,
                description TEXT,
                last_modified TEXT,
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vanished_datasets (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                agency TEXT,
                original_url TEXT,
                last_seen_date TEXT,
                suspected_cause TEXT,
                archive_link TEXT,
                status TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_type TEXT,
                datasets_found INTEGER,
                vanished_found INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def _rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Make a rate-limited HTTP request with error handling"""
        self._rate_limit()
        
        try:
            if headers is None:
                headers = {}
            
            # Add API key if available
            if self.api_key:
                headers['X-Api-Key'] = self.api_key
            
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response from {url}: {e}")
            return None
    
    def fetch_lil_manifest(self) -> List[Dict]:
        """
        Fetch the LIL Data.gov Archive manifest
        """
        logger.info("Fetching LIL manifest...")
        
        # Fetch from LIL API
        lil_data = self._fetch_lil_api()
        
        if lil_data:
            # Store in database
            self._store_datasets(lil_data, "lil")
            return lil_data
        else:
            logger.error("LIL API unavailable - no data fetched")
            return []
    
    def _fetch_lil_api(self) -> Optional[List[Dict]]:
        """
        Attempt to fetch data from LIL Data.gov Archive API
        Returns None if API is not accessible
        """
        try:
            # Try realistic LIL archive endpoints based on the Harvard LIL system
            lil_endpoints = [
                # Primary manifest endpoint
                f"{self.lil_base_url}/api/v1/datagov/manifest",
                f"{self.lil_base_url}/datagov/manifest.json",
                f"{self.lil_base_url}/api/datagov/datasets",
                # Alternative endpoints
                f"{self.lil_base_url}/archive/datagov/manifest",
                f"{self.lil_base_url}/api/archive/manifest",
                # Fallback endpoints
                f"{self.lil_base_url}/api/datagov/manifest",
                f"{self.lil_base_url}/datagov/archive/manifest.json",
                f"{self.lil_base_url}/api/archive/datasets"
            ]
            
            for endpoint in lil_endpoints:
                logger.info(f"Trying LIL endpoint: {endpoint}")
                response_data = self._make_request(endpoint)
                
                if response_data:
                    # Parse LIL response and convert to our format
                    parsed_data = self._parse_lil_response(response_data)
                    if parsed_data:
                        logger.info(f"Successfully fetched {len(parsed_data)} datasets from LIL")
                        return parsed_data
            
            logger.warning("All LIL endpoints failed - LIL API may not be publicly accessible")
            return None
            
        except Exception as e:
            logger.warning(f"Failed to fetch from LIL API: {e}")
            return None
    
    def _parse_lil_response(self, response_data: Dict) -> List[Dict]:
        """
        Parse LIL API response and convert to our dataset format
        """
        datasets = []
        
        # Handle different possible response formats
        if isinstance(response_data, list):
            data_list = response_data
        elif isinstance(response_data, dict):
            # Try common keys for dataset lists
            data_list = response_data.get('datasets', response_data.get('results', response_data.get('data', [])))
        else:
            return []
        
        for item in data_list:
            if isinstance(item, dict):
                dataset = {
                    "id": item.get('id', item.get('identifier', '')),
                    "title": item.get('title', item.get('name', '')),
                    "agency": item.get('organization', item.get('publisher', {}).get('name', '')),
                    "url": item.get('url', item.get('landingPage', '')),
                    "description": item.get('description', ''),
                    "last_modified": item.get('modified', item.get('lastModified', '')),
                    "source": "lil"
                }
                datasets.append(dataset)
        
        return datasets
    
    
    def fetch_live_datagov_catalog(self) -> List[Dict]:
        """
        Fetch current Data.gov catalog from CKAN API
        """
        logger.info("Fetching live Data.gov catalog...")
        
        # Fetch from Data.gov CKAN API
        live_data = self._fetch_datagov_api()
        
        if live_data:
            # Store in database
            self._store_datasets(live_data, "live")
            return live_data
        else:
            logger.error("Data.gov API unavailable - no data fetched")
            return []
    
    def _fetch_datagov_api(self) -> Optional[List[Dict]]:
        """
        Fetch datasets from Data.gov CKAN API
        Returns None if API is not accessible
        """
        try:
            datasets = []
            start = 0
            rows = 100  # Number of datasets per request
            max_datasets = 1000  # Limit total datasets for demo purposes
            
            while len(datasets) < max_datasets:
                params = {
                    'rows': rows,
                    'start': start,
                    'facet': 'true',
                    'facet.field': ['organization', 'tags', 'res_format'],
                    'sort': 'metadata_modified desc'
                }
                
                logger.info(f"Fetching Data.gov datasets: {start}-{start + rows}")
                response_data = self._make_request(self.datagov_package_search, params)
                
                if not response_data or not response_data.get('success'):
                    logger.error(f"Data.gov API request failed: {response_data}")
                    break
                
                results = response_data.get('result', {}).get('results', [])
                if not results:
                    break
                
                # Parse and convert to our format
                batch_datasets = self._parse_datagov_response(results)
                datasets.extend(batch_datasets)
                
                start += rows
                
                # Check if we've reached the end
                if len(results) < rows:
                    break
            
            logger.info(f"Fetched {len(datasets)} datasets from Data.gov")
            return datasets
            
        except Exception as e:
            logger.error(f"Failed to fetch from Data.gov API: {e}")
            return None
    
    def _parse_datagov_response(self, results: List[Dict]) -> List[Dict]:
        """
        Parse Data.gov CKAN API response and convert to our dataset format
        """
        datasets = []
        
        for item in results:
            # Extract organization name
            organization = item.get('organization', {})
            agency = organization.get('title', organization.get('name', '')) if organization else ''
            
            # Extract URL - try different possible fields
            url = item.get('url', '')
            if not url:
                resources = item.get('resources', [])
                if resources and len(resources) > 0:
                    url = resources[0].get('url', '')
            
            # Extract last modified date
            last_modified = item.get('metadata_modified', item.get('last_modified', ''))
            if last_modified:
                # Convert to readable format
                try:
                    dt = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                    last_modified = dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            dataset = {
                "id": item.get('id', item.get('name', '')),
                "title": item.get('title', ''),
                "agency": agency,
                "url": url,
                "description": item.get('notes', item.get('description', '')),
                "last_modified": last_modified,
                "source": "live"
            }
            datasets.append(dataset)
        
        return datasets
    
    
    def _store_datasets(self, datasets: List[Dict], source: str):
        """Store datasets in SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for dataset in datasets:
            cursor.execute('''
                INSERT OR REPLACE INTO datasets 
                (id, title, agency, url, description, last_modified, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset['id'],
                dataset['title'],
                dataset.get('agency', ''),
                dataset.get('url', ''),
                dataset.get('description', ''),
                dataset.get('last_modified', ''),
                source
            ))
        
        conn.commit()
        conn.close()
    
    def test_api_connectivity(self) -> Dict[str, bool]:
        """
        Test connectivity to both LIL and Data.gov APIs
        Returns a dictionary with API status
        """
        results = {
            'lil_api': False,
            'datagov_api': False
        }
        
        # Test LIL API
        try:
            lil_data = self._fetch_lil_api()
            results['lil_api'] = lil_data is not None and len(lil_data) > 0
        except Exception as e:
            logger.warning(f"LIL API test failed: {e}")
        
        # Test Data.gov API
        try:
            # Test with a small request
            params = {'rows': 1, 'start': 0}
            response = self._make_request(self.datagov_package_search, params)
            results['datagov_api'] = response is not None and response.get('success', False)
        except Exception as e:
            logger.warning(f"Data.gov API test failed: {e}")
        
        return results
    
    def get_api_stats(self) -> Dict:
        """
        Get statistics about API usage and data sources
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Count datasets by source
        cursor.execute("SELECT source, COUNT(*) FROM datasets GROUP BY source")
        source_counts = dict(cursor.fetchall())
        
        # Get last fetch times
        cursor.execute("""
            SELECT source, MAX(created_at) as last_fetch 
            FROM datasets 
            GROUP BY source
        """)
        last_fetch = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'source_counts': source_counts,
            'last_fetch': last_fetch,
            'api_connectivity': self.test_api_connectivity()
        }
    
    def get_lil_datasets(self) -> List[Dict]:
        """Get datasets from LIL source"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM datasets WHERE source = 'lil'")
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return datasets
    
    def get_live_datasets(self) -> List[Dict]:
        """Get datasets from live source"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM datasets WHERE source = 'live'")
        columns = [description[0] for description in cursor.description]
        datasets = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return datasets
    
    def log_monitoring_check(self, check_type: str, datasets_found: int, vanished_found: int):
        """Log monitoring check results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO monitoring_logs (check_type, datasets_found, vanished_found)
            VALUES (?, ?, ?)
        ''', (check_type, datasets_found, vanished_found))
        
        conn.commit()
        conn.close()
