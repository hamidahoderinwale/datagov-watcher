"""
Harvard LIL Data.gov Archive Integration
Handles fetching and processing data from the LIL archive
"""

import requests
import json
import sqlite3
import zipfile
import io
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
import hashlib
import time
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

class LILIntegration:
    """Integration with Harvard LIL Data.gov Archive"""
    
    def __init__(self, db_path: str = "datasets.db", data_dir: str = "dataset_states/lil_data"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # LIL API endpoints (these may not be publicly accessible)
        self.lil_base_url = "https://lil.law.harvard.edu"
        self.lil_catalog_url = f"{self.lil_base_url}/data-gov-archive"
        self.lil_api_url = f"{self.lil_base_url}/api"
        
        # Alternative LIL endpoints to try
        self.lil_endpoints = [
            f"{self.lil_base_url}/data-gov-archive/data.json",
            f"{self.lil_base_url}/api/3/action/package_list",
            f"{self.lil_base_url}/api/3/action/package_search",
            "https://dataverse.harvard.edu/api/datasets",
            "https://catalog.data.gov/api/3/action/package_list"
        ]
        
        self.init_database()
    
    def init_database(self):
        """Initialize LIL-specific database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # LIL catalog snapshots
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lil_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                catalog_url TEXT,
                warc_url TEXT,
                index_url TEXT,
                total_datasets INTEGER,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(snapshot_date)
            )
        ''')
        
        # LIL dataset manifests
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lil_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                title TEXT,
                description TEXT,
                publisher TEXT,
                license TEXT,
                modified TEXT,
                resources TEXT,  -- JSON
                metadata TEXT,  -- JSON
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def discover_available_snapshots(self) -> List[Dict]:
        """Discover available LIL snapshots"""
        snapshots = []
        
        # Try to discover snapshots from various sources
        for endpoint in self.lil_endpoints:
            try:
                logger.info(f"Trying LIL endpoint: {endpoint}")
                response = requests.get(endpoint, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract snapshot information based on response format
                    if 'result' in data and isinstance(data['result'], list):
                        # CKAN format
                        snapshot_info = {
                            'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
                            'catalog_url': endpoint,
                            'total_datasets': len(data['result']),
                            'source': 'lil_ckan'
                        }
                        snapshots.append(snapshot_info)
                        
                    elif 'data' in data and isinstance(data['data'], list):
                        # Dataverse format
                        snapshot_info = {
                            'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
                            'catalog_url': endpoint,
                            'total_datasets': len(data['data']),
                            'source': 'lil_dataverse'
                        }
                        snapshots.append(snapshot_info)
                        
                    else:
                        # Generic JSON format
                        snapshot_info = {
                            'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
                            'catalog_url': endpoint,
                            'total_datasets': 1,  # Assume single dataset
                            'source': 'lil_generic'
                        }
                        snapshots.append(snapshot_info)
                    
                    logger.info(f"Found snapshot with {snapshot_info['total_datasets']} datasets")
                    break
                    
            except Exception as e:
                logger.debug(f"Failed to access {endpoint}: {e}")
                continue
        
        # If no LIL access, create a fallback using current Data.gov
        if not snapshots:
            logger.warning("LIL endpoints not accessible, using Data.gov as fallback")
            snapshots = self._create_fallback_snapshot()
        
        return snapshots
    
    def _create_fallback_snapshot(self) -> List[Dict]:
        """Create a fallback snapshot using current Data.gov data"""
        try:
            # Use current Data.gov as a baseline
            response = requests.get("https://catalog.data.gov/api/3/action/package_search?rows=0", timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                # Extract count from CKAN API response
                if isinstance(data, dict) and 'result' in data:
                    dataset_count = data['result'].get('count', 0)
                else:
                    dataset_count = 0
                
                # Store this as a proper snapshot
                snapshot_info = {
                    'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
                    'catalog_url': 'https://catalog.data.gov/api/3/action/package_search',
                    'total_datasets': dataset_count,
                    'source': 'datagov_fallback'
                }
                
                # Store in database for persistence
                self._store_snapshot_metadata(snapshot_info, dataset_count)
                
                return [snapshot_info]
        except Exception as e:
            logger.error(f"Failed to create fallback snapshot: {e}")
        
        return []
    
    def fetch_snapshot_catalog(self, snapshot_info: Dict) -> List[Dict]:
        """Fetch catalog data for a specific snapshot"""
        try:
            response = requests.get(snapshot_info['catalog_url'], timeout=60)
            response.raise_for_status()
            
            data = response.json()
            datasets = []
            
            # Extract datasets based on format
            if 'dataset' in data:
                # inventory.data.gov format
                datasets = data['dataset']
            elif 'result' in data and isinstance(data['result'], list):
                # CKAN format
                datasets = data['result']
            elif 'data' in data and isinstance(data['data'], list):
                # Dataverse format
                datasets = data['data']
            else:
                # Try to find datasets in the response
                datasets = self._extract_datasets_from_response(data)
            
            logger.info(f"Fetched {len(datasets)} datasets from {snapshot_info['source']}")
            return datasets
            
        except Exception as e:
            logger.error(f"Error fetching snapshot catalog: {e}")
            return []
    
    def _extract_datasets_from_response(self, data: Dict) -> List[Dict]:
        """Extract datasets from various response formats"""
        datasets = []
        
        # Recursively search for dataset-like objects
        def find_datasets(obj, path=""):
            if isinstance(obj, dict):
                # Check if this looks like a dataset
                if any(key in obj for key in ['title', 'name', 'identifier', 'id']):
                    datasets.append(obj)
                else:
                    for key, value in obj.items():
                        find_datasets(value, f"{path}.{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    find_datasets(item, f"{path}[{i}]")
        
        find_datasets(data)
        return datasets
    
    def process_snapshot(self, snapshot_info: Dict) -> bool:
        """Process a complete snapshot"""
        try:
            # Fetch catalog data
            datasets = self.fetch_snapshot_catalog(snapshot_info)
            if not datasets:
                logger.warning(f"No datasets found for snapshot {snapshot_info['snapshot_date']}")
                return False
            
            # Store snapshot metadata
            self._store_snapshot_metadata(snapshot_info, len(datasets))
            
            # Process each dataset
            processed_count = 0
            for dataset in datasets:
                if self._process_dataset(dataset, snapshot_info['snapshot_date']):
                    processed_count += 1
            
            logger.info(f"Processed {processed_count}/{len(datasets)} datasets for snapshot {snapshot_info['snapshot_date']}")
            
            # Mark snapshot as processed
            self._mark_snapshot_processed(snapshot_info['snapshot_date'])
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing snapshot {snapshot_info['snapshot_date']}: {e}")
            return False
    
    def _store_snapshot_metadata(self, snapshot_info: Dict, dataset_count: int):
        """Store snapshot metadata in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO lil_snapshots
            (snapshot_date, catalog_url, total_datasets, processed)
            VALUES (?, ?, ?, ?)
        ''', (
            snapshot_info['snapshot_date'],
            snapshot_info['catalog_url'],
            dataset_count,
            False
        ))
        
        conn.commit()
        conn.close()
    
    def _process_dataset(self, dataset: Dict, snapshot_date: str) -> bool:
        """Process a single dataset from the snapshot"""
        try:
            # Extract dataset information
            dataset_id = self._extract_dataset_id(dataset)
            if not dataset_id:
                return False
            
            # Create dataset directory
            dataset_dir = self.data_dir / dataset_id / snapshot_date
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            # Extract and store manifest
            manifest = self._extract_manifest(dataset)
            manifest_path = dataset_dir / "manifest.json"
            
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            # Store in database
            self._store_dataset_manifest(dataset_id, snapshot_date, manifest)
            
            # Download and process resources
            self._process_dataset_resources(dataset, dataset_dir)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing dataset: {e}")
            return False
    
    def _extract_dataset_id(self, dataset: Dict) -> Optional[str]:
        """Extract a unique dataset ID"""
        # Try various ID fields
        id_fields = ['id', 'identifier', 'name', 'title', 'url']
        
        for field in id_fields:
            if field in dataset and dataset[field]:
                # Create a hash if the ID is too long
                dataset_id = str(dataset[field])
                if len(dataset_id) > 100:
                    dataset_id = hashlib.md5(dataset_id.encode()).hexdigest()
                return dataset_id
        
        return None
    
    def _extract_manifest(self, dataset: Dict) -> Dict:
        """Extract manifest information from dataset"""
        manifest = {
            'dataset_id': self._extract_dataset_id(dataset),
            'title': dataset.get('title', dataset.get('name', 'Unknown')),
            'description': dataset.get('description', ''),
            'publisher': dataset.get('publisher', dataset.get('organization', {}).get('title', 'Unknown')),
            'license': dataset.get('license', ''),
            'modified': dataset.get('modified', dataset.get('last_modified', '')),
            'resources': dataset.get('resources', []),
            'metadata': dataset
        }
        
        return manifest
    
    def _store_dataset_manifest(self, dataset_id: str, snapshot_date: str, manifest: Dict):
        """Store dataset manifest in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO lil_manifests
            (dataset_id, snapshot_date, title, description, publisher,
             license, modified, resources, metadata, file_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dataset_id,
            snapshot_date,
            manifest['title'],
            manifest['description'],
            manifest['publisher'],
            manifest['license'],
            manifest['modified'],
            json.dumps(manifest['resources']),
            json.dumps(manifest['metadata']),
            str(self.data_dir / dataset_id / snapshot_date / "manifest.json")
        ))
        
        conn.commit()
        conn.close()
    
    def _process_dataset_resources(self, dataset: Dict, dataset_dir: Path):
        """Process and download dataset resources"""
        resources = dataset.get('resources', [])
        
        for i, resource in enumerate(resources):
            try:
                # Download resource if it's a data file
                if self._is_data_resource(resource):
                    self._download_resource(resource, dataset_dir, f"resource_{i}")
                    
            except Exception as e:
                logger.debug(f"Error processing resource {i}: {e}")
    
    def _is_data_resource(self, resource: Dict) -> bool:
        """Check if resource is a data file"""
        format_type = resource.get('format', '').upper()
        mime_type = resource.get('mime_type', '').upper()
        
        data_formats = ['CSV', 'JSON', 'XML', 'XLS', 'XLSX', 'ZIP', 'TXT', 'TSV']
        data_mimes = ['TEXT/CSV', 'APPLICATION/JSON', 'APPLICATION/XML', 
                     'APPLICATION/VND.MS-EXCEL', 'APPLICATION/ZIP']
        
        return (format_type in data_formats or 
                mime_type in data_mimes or
                any(ext in resource.get('url', '').lower() for ext in ['.csv', '.json', '.xml', '.xls', '.xlsx']))
    
    def _download_resource(self, resource: Dict, dataset_dir: Path, filename: str):
        """Download a resource file"""
        url = resource.get('url', '')
        if not url:
            return
        
        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Determine file extension
            content_type = response.headers.get('content-type', '')
            if 'csv' in content_type.lower():
                ext = '.csv'
            elif 'json' in content_type.lower():
                ext = '.json'
            elif 'xml' in content_type.lower():
                ext = '.xml'
            else:
                ext = '.data'
            
            file_path = dataset_dir / f"{filename}{ext}"
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.debug(f"Downloaded resource: {file_path}")
            
        except Exception as e:
            logger.debug(f"Failed to download resource {url}: {e}")
    
    def _mark_snapshot_processed(self, snapshot_date: str):
        """Mark snapshot as processed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE lil_snapshots 
            SET processed = TRUE 
            WHERE snapshot_date = ?
        ''', (snapshot_date,))
        
        conn.commit()
        conn.close()
    
    def get_available_snapshots(self) -> List[Dict]:
        """Get list of available snapshots"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, catalog_url, total_datasets, processed, created_at
            FROM lil_snapshots
            ORDER BY snapshot_date DESC
        ''')
        
        snapshots = []
        for row in cursor.fetchall():
            snapshots.append({
                'snapshot_date': row[0],
                'catalog_url': row[1],
                'total_datasets': row[2],
                'processed': bool(row[3]),
                'created_at': row[4]
            })
        
        conn.close()
        return snapshots
    
    def get_snapshot_datasets(self, snapshot_date: str) -> List[Dict]:
        """Get datasets for a specific snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, title, description, publisher, license, 
                   modified, resources, metadata
            FROM lil_manifests
            WHERE snapshot_date = ?
            ORDER BY title
        ''', (snapshot_date,))
        
        datasets = []
        for row in cursor.fetchall():
            datasets.append({
                'dataset_id': row[0],
                'title': row[1],
                'description': row[2],
                'publisher': row[3],
                'license': row[4],
                'modified': row[5],
                'resources': json.loads(row[6]) if row[6] else [],
                'metadata': json.loads(row[7]) if row[7] else {}
            })
        
        conn.close()
        return datasets
    
    def get_actual_dataset_count(self, snapshot_date: str) -> int:
        """Get the actual count of datasets for a snapshot from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM lil_manifests WHERE snapshot_date = ?
        ''', (snapshot_date,))
        
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def update_snapshot_count(self, snapshot_date: str) -> bool:
        """Update the dataset count for a snapshot based on actual data"""
        try:
            actual_count = self.get_actual_dataset_count(snapshot_date)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE lil_snapshots 
                SET total_datasets = ? 
                WHERE snapshot_date = ?
            ''', (actual_count, snapshot_date))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated snapshot {snapshot_date} count to {actual_count}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update snapshot count: {e}")
            return False
    
    def compare_with_live_catalog(self, live_datasets: List[Dict]) -> Dict:
        """Compare LIL snapshots with live catalog to find vanished datasets"""
        # Get latest LIL snapshot
        snapshots = self.get_available_snapshots()
        if not snapshots:
            return {'error': 'No LIL snapshots available'}
        
        latest_snapshot = snapshots[0]
        snapshot_date = latest_snapshot['snapshot_date']
        
        # Update the count to be accurate
        self.update_snapshot_count(snapshot_date)
        
        # Get actual datasets
        lil_datasets = self.get_snapshot_datasets(snapshot_date)
        actual_lil_count = len(lil_datasets)
        
        # Create lookup sets
        live_ids = {d.get('id', '') for d in live_datasets}
        lil_ids = {d['dataset_id'] for d in lil_datasets}
        
        # Find vanished datasets
        vanished_ids = lil_ids - live_ids
        vanished_datasets = [d for d in lil_datasets if d['dataset_id'] in vanished_ids]
        
        # Find new datasets
        new_ids = live_ids - lil_ids
        new_datasets = [d for d in live_datasets if d.get('id', '') in new_ids]
        
        return {
            'comparison_date': snapshot_date,
            'lil_datasets_count': actual_lil_count,
            'live_datasets_count': len(live_datasets),
            'vanished_count': len(vanished_datasets),
            'new_count': len(new_datasets),
            'vanished_datasets': vanished_datasets,
            'new_datasets': new_datasets
        }
    
    def refresh_live_dataset_count(self) -> Dict:
        """Refresh the live dataset count from Data.gov API"""
        try:
            # Get current count from Data.gov using the correct API
            response = requests.get("https://catalog.data.gov/api/3/action/package_search?rows=0", timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                # Extract count from CKAN API response
                if isinstance(data, dict) and 'result' in data:
                    live_count = data['result'].get('count', 0)
                else:
                    live_count = 0
                
                # Update or create a new snapshot with current data
                snapshot_date = datetime.now().strftime('%Y-%m-%d')
                
                # Check if we already have today's snapshot
                existing_snapshots = self.get_available_snapshots()
                today_snapshot = next((s for s in existing_snapshots if s['snapshot_date'] == snapshot_date), None)
                
                if today_snapshot:
                    # Update existing snapshot
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE lil_snapshots 
                        SET total_datasets = ?, processed = FALSE
                        WHERE snapshot_date = ?
                    ''', (live_count, snapshot_date))
                    conn.commit()
                    conn.close()
                else:
                    # Create new snapshot
                    snapshot_info = {
                        'snapshot_date': snapshot_date,
                        'catalog_url': 'https://inventory.data.gov/data.json',
                        'total_datasets': live_count,
                        'source': 'datagov_live'
                    }
                    self._store_snapshot_metadata(snapshot_info, live_count)
                
                return {
                    'status': 'success',
                    'live_count': live_count,
                    'snapshot_date': snapshot_date,
                    'message': f'Updated live dataset count to {live_count}'
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Failed to fetch from Data.gov: {response.status_code}'
                }
                
        except Exception as e:
            logger.error(f"Failed to refresh live dataset count: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }