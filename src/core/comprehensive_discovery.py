"""
Comprehensive Dataset Discovery System
Ensures all datasets from multiple sources are discovered and monitored regularly
"""

import asyncio
import aiohttp
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from pathlib import Path
import time
import hashlib
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from .license_classifier import license_classifier

logger = logging.getLogger(__name__)

class ComprehensiveDiscovery:
    """Comprehensive dataset discovery system that integrates multiple data sources"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.session = None
        self.discovered_datasets = set()
        self.init_database()
        
        # Data sources configuration
        self.data_sources = {
            'datagov_ckan': {
                'name': 'Data.gov CKAN API',
                'base_url': 'https://catalog.data.gov/api/3/action',
                'enabled': True,
                'rate_limit': 0.1,
                'max_datasets': None  # No limit
            },
            'datagov_inventory': {
                'name': 'Data.gov Inventory (data.json)',
                'base_url': 'https://inventory.data.gov',
                'enabled': True,
                'rate_limit': 0.2,
                'max_datasets': None
            },
            'lil_archive': {
                'name': 'Harvard LIL Data.gov Archive',
                'base_url': 'https://lil.law.harvard.edu',
                'enabled': True,
                'rate_limit': 0.5,
                'max_datasets': None
            },
            'agency_data_json': {
                'name': 'Agency data.json endpoints',
                'base_url': None,  # Dynamic
                'enabled': True,
                'rate_limit': 0.3,
                'max_datasets': None
            }
        }
        
        # Agency data.json endpoints (major agencies)
        self.agency_endpoints = [
            'https://www.data.gov/data.json',
            'https://www.census.gov/data.json',
            'https://www.epa.gov/data.json',
            'https://www.fda.gov/data.json',
            'https://www.ftc.gov/data.json',
            'https://www.sec.gov/data.json',
            'https://www.ed.gov/data.json',
            'https://www.hhs.gov/data.json',
            'https://www.dhs.gov/data.json',
            'https://www.doj.gov/data.json',
            'https://www.dol.gov/data.json',
            'https://www.energy.gov/data.json',
            'https://www.hud.gov/data.json',
            'https://www.doi.gov/data.json',
            'https://www.state.gov/data.json',
            'https://www.transportation.gov/data.json',
            'https://www.treasury.gov/data.json',
            'https://www.va.gov/data.json',
            'https://www.usda.gov/data.json',
            'https://www.commerce.gov/data.json'
        ]
    
    def init_database(self):
        """Initialize database tables for comprehensive discovery"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Discovery tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discovery_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                total_datasets_found INTEGER DEFAULT 0,
                new_datasets_found INTEGER DEFAULT 0,
                sources_checked TEXT,  -- JSON array
                status TEXT DEFAULT 'running',  -- 'running', 'completed', 'failed'
                error_message TEXT
            )
        ''')
        
        # Dataset source tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_url TEXT,
                first_discovered TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                discovery_count INTEGER DEFAULT 1,
                UNIQUE(dataset_id, source_name)
            )
        ''')
        
        # Enhanced dataset states table (if not exists)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                state_hash TEXT,
                file_path TEXT,
                metadata_path TEXT,
                schema_hash TEXT,
                content_hash TEXT,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                title TEXT,
                agency TEXT,
                url TEXT,
                status_code INTEGER,
                content_type TEXT,
                resource_format TEXT,
                schema TEXT,
                last_modified TEXT,
                availability TEXT,
                dimensions_computed BOOLEAN DEFAULT FALSE,
                dimension_computation_date TIMESTAMP,
                dimension_computation_error TEXT,
                dimension_computation_time_ms INTEGER,
                schema_columns TEXT,
                schema_dtypes TEXT,
                content_analyzed BOOLEAN DEFAULT FALSE,
                analysis_quality_score REAL,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def start_discovery_session(self) -> str:
        """Start a new discovery session"""
        session_id = f"discovery_{int(time.time())}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO discovery_sessions (session_id, start_time, status)
            VALUES (?, ?, ?)
        ''', (session_id, datetime.now(), 'running'))
        conn.commit()
        conn.close()
        
        logger.info(f"Started discovery session: {session_id}")
        return session_id
    
    async def run_comprehensive_discovery(self, session_id: str) -> Dict:
        """Run comprehensive discovery across all data sources"""
        logger.info("Starting comprehensive dataset discovery")
        
        async with aiohttp.ClientSession() as session:
            self.session = session
            results = {
                'session_id': session_id,
                'start_time': datetime.now().isoformat(),
                'sources_checked': [],
                'total_datasets': 0,
                'new_datasets': 0,
                'errors': []
            }
            
            # Discover from each source
            for source_key, source_config in self.data_sources.items():
                if not source_config['enabled']:
                    continue
                
                try:
                    logger.info(f"Discovering from {source_config['name']}")
                    source_results = await self._discover_from_source(source_key, source_config)
                    results['sources_checked'].append(source_key)
                    results['total_datasets'] += source_results['total_found']
                    results['new_datasets'] += source_results['new_found']
                    
                    # Rate limiting
                    await asyncio.sleep(source_config['rate_limit'])
                    
                except Exception as e:
                    logger.error(f"Error discovering from {source_key}: {e}")
                    results['errors'].append({
                        'source': source_key,
                        'error': str(e)
                    })
            
            # Update session status
            await self._update_session_status(session_id, results)
            
            results['end_time'] = datetime.now().isoformat()
            logger.info(f"Discovery completed: {results['total_datasets']} total, {results['new_datasets']} new")
            
            return results
    
    async def _discover_from_source(self, source_key: str, source_config: Dict) -> Dict:
        """Discover datasets from a specific source"""
        if source_key == 'datagov_ckan':
            return await self._discover_from_datagov_ckan(source_config)
        elif source_key == 'datagov_inventory':
            return await self._discover_from_datagov_inventory(source_config)
        elif source_key == 'lil_archive':
            return await self._discover_from_lil_archive(source_config)
        elif source_key == 'agency_data_json':
            return await self._discover_from_agency_endpoints(source_config)
        else:
            raise ValueError(f"Unknown source: {source_key}")
    
    async def _discover_from_datagov_ckan(self, config: Dict) -> Dict:
        """Discover datasets from Data.gov CKAN API"""
        datasets = []
        start = 0
        rows = 1000
        max_datasets = config.get('max_datasets', 50000)  # Increased limit
        
        while len(datasets) < max_datasets:
            url = f"{config['base_url']}/package_search"
            params = {
                'rows': rows,
                'start': start,
                'facet': 'true',
                'facet.field': ['organization', 'tags', 'res_format'],
                'sort': 'metadata_modified desc'
            }
            
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status != 200:
                        break
                    
                    data = await response.json()
                    if not data.get('success'):
                        break
                    
                    results = data.get('result', {}).get('results', [])
                    if not results:
                        break
                    
                    # Parse datasets
                    for result in results:
                        dataset = self._parse_ckan_dataset(result)
                        if dataset:
                            datasets.append(dataset)
                    
                    start += rows
                    
                    # Check if we've reached the end
                    if len(results) < rows:
                        break
                        
            except Exception as e:
                logger.error(f"Error fetching from CKAN API: {e}")
                break
        
        return await self._process_discovered_datasets(datasets, 'datagov_ckan')
    
    async def _discover_from_datagov_inventory(self, config: Dict) -> Dict:
        """Discover datasets from Data.gov inventory"""
        datasets = []
        
        try:
            # Get the main inventory data.json
            url = f"{config['base_url']}/data.json"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    datasets.extend(self._parse_data_json(data))
            
            # Get agency-specific data.json files
            for agency_url in self.agency_endpoints:
                try:
                    async with self.session.get(agency_url) as response:
                        if response.status == 200:
                            data = await response.json()
                            agency_datasets = self._parse_data_json(data)
                            datasets.extend(agency_datasets)
                            
                except Exception as e:
                    logger.warning(f"Error fetching from {agency_url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error discovering from inventory: {e}")
        
        return await self._process_discovered_datasets(datasets, 'datagov_inventory')
    
    async def _discover_from_lil_archive(self, config: Dict) -> Dict:
        """Discover datasets from Harvard LIL archive"""
        datasets = []
        
        try:
            # This would need to be implemented based on LIL's API
            # For now, we'll simulate the discovery
            logger.info("LIL archive discovery not yet implemented")
            
        except Exception as e:
            logger.error(f"Error discovering from LIL archive: {e}")
        
        return await self._process_discovered_datasets(datasets, 'lil_archive')
    
    async def _discover_from_agency_endpoints(self, config: Dict) -> Dict:
        """Discover datasets from agency data.json endpoints"""
        datasets = []
        
        for endpoint in self.agency_endpoints:
            try:
                async with self.session.get(endpoint) as response:
                    if response.status == 200:
                        data = await response.json()
                        agency_datasets = self._parse_data_json(data)
                        datasets.extend(agency_datasets)
                        
            except Exception as e:
                logger.warning(f"Error fetching from {endpoint}: {e}")
                continue
        
        return await self._process_discovered_datasets(datasets, 'agency_data_json')
    
    def _parse_ckan_dataset(self, result: Dict) -> Optional[Dict]:
        """Parse a CKAN API result into our dataset format"""
        try:
            # Extract and normalize license information
            license_title = result.get('license_title', '')
            license_url = result.get('license_url', '')
            normalized_license = license_classifier.normalize_license(license_title, license_url)
            
            return {
                'dataset_id': result.get('id', ''),
                'title': result.get('title', ''),
                'agency': result.get('organization', {}).get('title', ''),
                'url': result.get('url', ''),
                'description': result.get('notes', ''),
                'last_modified': result.get('metadata_modified', ''),
                'tags': [tag.get('name', '') for tag in result.get('tags', [])],
                'resources': result.get('resources', []),
                'license': normalized_license,
                'source': 'datagov_ckan'
            }
        except Exception as e:
            logger.warning(f"Error parsing CKAN dataset: {e}")
            return None
    
    def _parse_data_json(self, data: Dict) -> List[Dict]:
        """Parse data.json format into our dataset format"""
        datasets = []
        
        try:
            for item in data.get('dataset', []):
                # Extract and normalize license information
                license_text = item.get('license', '')
                license_url = None
                
                # Try to extract license URL from various fields
                if isinstance(license_text, dict):
                    license_url = license_text.get('url', '')
                    license_text = license_text.get('name', '') or license_text.get('title', '')
                
                normalized_license = license_classifier.normalize_license(license_text, license_url)
                
                dataset = {
                    'dataset_id': item.get('@type', '') + '_' + str(hash(item.get('identifier', ''))),
                    'title': item.get('title', ''),
                    'agency': item.get('publisher', {}).get('name', ''),
                    'url': item.get('landingPage', ''),
                    'description': item.get('description', ''),
                    'last_modified': item.get('modified', ''),
                    'tags': [tag.get('name', '') for tag in item.get('keyword', [])],
                    'resources': item.get('distribution', []),
                    'license': normalized_license,
                    'source': 'data_json'
                }
                datasets.append(dataset)
        except Exception as e:
            logger.warning(f"Error parsing data.json: {e}")
        
        return datasets
    
    async def _process_discovered_datasets(self, datasets: List[Dict], source: str) -> Dict:
        """Process discovered datasets and update database"""
        total_found = len(datasets)
        new_found = 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for dataset in datasets:
            dataset_id = dataset.get('dataset_id', '')
            if not dataset_id:
                continue
            
            # Check if this is a new dataset
            cursor.execute('SELECT COUNT(*) FROM dataset_sources WHERE dataset_id = ?', (dataset_id,))
            is_new = cursor.fetchone()[0] == 0
            
            if is_new:
                new_found += 1
                # Insert into dataset_states
                self._insert_dataset_state(cursor, dataset)
            
            # Update or insert source tracking
            cursor.execute('''
                INSERT OR REPLACE INTO dataset_sources 
                (dataset_id, source_name, source_url, first_discovered, last_seen, discovery_count)
                VALUES (?, ?, ?, 
                    COALESCE((SELECT first_discovered FROM dataset_sources WHERE dataset_id = ? AND source_name = ?), CURRENT_TIMESTAMP),
                    CURRENT_TIMESTAMP,
                    COALESCE((SELECT discovery_count FROM dataset_sources WHERE dataset_id = ? AND source_name = ?), 0) + 1)
            ''', (dataset_id, source, dataset.get('url', ''), dataset_id, source, dataset_id, source))
        
        conn.commit()
        conn.close()
        
        return {
            'total_found': total_found,
            'new_found': new_found
        }
    
    def _insert_dataset_state(self, cursor, dataset: Dict):
        """Insert a new dataset state into the database"""
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO dataset_states 
                (dataset_id, snapshot_date, title, agency, url, last_modified, availability, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset.get('dataset_id', ''),
                datetime.now().strftime('%Y-%m-%d'),
                dataset.get('title', ''),
                dataset.get('agency', ''),
                dataset.get('url', ''),
                dataset.get('last_modified', ''),
                'unknown',
                datetime.now()
            ))
        except Exception as e:
            logger.warning(f"Error inserting dataset state: {e}")
    
    async def _update_session_status(self, session_id: str, results: Dict):
        """Update discovery session status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE discovery_sessions 
            SET end_time = ?, total_datasets_found = ?, new_datasets_found = ?, 
                sources_checked = ?, status = 'completed'
            WHERE session_id = ?
        ''', (
            datetime.now(),
            results['total_datasets'],
            results['new_datasets'],
            json.dumps(results['sources_checked']),
            session_id
        ))
        conn.commit()
        conn.close()
    
    async def schedule_regular_discovery(self, interval_hours: int = 24):
        """Schedule regular discovery runs"""
        logger.info(f"Scheduling discovery every {interval_hours} hours")
        
        while True:
            try:
                session_id = await self.start_discovery_session()
                results = await self.run_comprehensive_discovery(session_id)
                logger.info(f"Discovery completed: {results['total_datasets']} total, {results['new_datasets']} new")
                
            except Exception as e:
                logger.error(f"Error in scheduled discovery: {e}")
            
            # Wait for next run
            await asyncio.sleep(interval_hours * 3600)
    
    def get_discovery_stats(self) -> Dict:
        """Get discovery statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total datasets
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
        total_datasets = cursor.fetchone()[0]
        
        # Recent discoveries
        cursor.execute('''
            SELECT COUNT(DISTINCT dataset_id) FROM dataset_sources 
            WHERE first_discovered > datetime('now', '-7 days')
        ''')
        recent_discoveries = cursor.fetchone()[0]
        
        # Source breakdown
        cursor.execute('''
            SELECT source_name, COUNT(DISTINCT dataset_id) as count
            FROM dataset_sources 
            GROUP BY source_name
        ''')
        source_breakdown = dict(cursor.fetchall())
        
        # Recent sessions
        cursor.execute('''
            SELECT session_id, start_time, total_datasets_found, new_datasets_found, status
            FROM discovery_sessions 
            ORDER BY start_time DESC 
            LIMIT 10
        ''')
        recent_sessions = [
            {
                'session_id': row[0],
                'start_time': row[1],
                'total_datasets': row[2],
                'new_datasets': row[3],
                'status': row[4]
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        return {
            'total_datasets': total_datasets,
            'recent_discoveries': recent_discoveries,
            'source_breakdown': source_breakdown,
            'recent_sessions': recent_sessions
        }

# CLI interface
async def main():
    """Main entry point for discovery system"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Comprehensive Dataset Discovery')
    parser.add_argument('--mode', choices=['once', 'schedule'], default='once',
                       help='Run mode: once (single discovery) or schedule (continuous)')
    parser.add_argument('--interval', type=int, default=24,
                       help='Discovery interval in hours (for schedule mode)')
    parser.add_argument('--db', default='datasets.db',
                       help='Database file path')
    
    args = parser.parse_args()
    
    discovery = ComprehensiveDiscovery(args.db)
    
    if args.mode == 'once':
        session_id = await discovery.start_discovery_session()
        results = await discovery.run_comprehensive_discovery(session_id)
        print(json.dumps(results, indent=2))
    else:
        await discovery.schedule_regular_discovery(args.interval)

if __name__ == '__main__':
    asyncio.run(main())


