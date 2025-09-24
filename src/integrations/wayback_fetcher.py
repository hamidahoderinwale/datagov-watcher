"""
Wayback Fetcher Module for Vanished Datasets
Reconstructs historical states of vanished datasets from archival sources
"""

import json
import sqlite3
import requests
import hashlib
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import zipfile
import io
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin
import re

# Try to import waybackpy, fallback to requests if not available
try:
    import waybackpy
    WAYBACKPY_AVAILABLE = True
except ImportError:
    WAYBACKPY_AVAILABLE = False
    logging.warning("waybackpy not available, using requests fallback")

logger = logging.getLogger(__name__)

class WaybackFetcher:
    """Fetches and reconstructs vanished datasets from archival sources"""
    
    def __init__(self, db_path: str = "datasets.db", data_dir: str = "dataset_states"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for archived data
        (self.data_dir / "wayback_data").mkdir(exist_ok=True)
        (self.data_dir / "eota_data").mkdir(exist_ok=True)
        (self.data_dir / "vanished").mkdir(exist_ok=True)
        
        # Wayback Machine CDX API base URL
        self.wayback_cdx_url = "http://web.archive.org/cdx/search/cdx"
        self.wayback_base_url = "http://web.archive.org/web"
        
        # EOTA CDX API (if available)
        self.eota_cdx_url = "https://eotarchive.com/cdx/search/cdx"
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0  # 1 second between requests
    
    def detect_vanished_datasets(self) -> List[Dict]:
        """Detect datasets that have vanished from the live catalog"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all dataset IDs that have been seen before
            cursor.execute('''
                SELECT DISTINCT dataset_id, MAX(snapshot_date) as last_seen, 
                       MAX(source) as last_source
                FROM historian_snapshots
                GROUP BY dataset_id
            ''')
            
            all_known_datasets = {row[0]: {'last_seen': row[1], 'last_source': row[2]} 
                                for row in cursor.fetchall()}
            
            # Get current live datasets
            cursor.execute('''
                SELECT DISTINCT dataset_id FROM historian_snapshots 
                WHERE source = 'live' AND snapshot_date = (
                    SELECT MAX(snapshot_date) FROM historian_snapshots WHERE source = 'live'
                )
            ''')
            
            current_live_datasets = {row[0] for row in cursor.fetchall()}
            
            # Find vanished datasets
            vanished = []
            for dataset_id, info in all_known_datasets.items():
                if dataset_id not in current_live_datasets:
                    vanished.append({
                        'dataset_id': dataset_id,
                        'last_seen_date': info['last_seen'],
                        'last_seen_source': info['last_source']
                    })
            
            conn.close()
            logger.info(f"Detected {len(vanished)} vanished datasets")
            return vanished
            
        except Exception as e:
            logger.error(f"Error detecting vanished datasets: {e}")
            return []
    
    def search_wayback_cdx(self, url: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Search Wayback Machine CDX index for archived versions of a URL"""
        try:
            self._rate_limit()
            
            params = {
                'url': url,
                'output': 'json',
                'fl': 'timestamp,original,statuscode,mimetype,length,digest'
            }
            
            if start_date:
                params['from'] = start_date
            if end_date:
                params['to'] = end_date
            
            response = requests.get(self.wayback_cdx_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse CDX response
            cdx_data = response.json()
            if not cdx_data or len(cdx_data) < 2:  # Header + data
                return []
            
            # Skip header row
            results = []
            for row in cdx_data[1:]:
                if len(row) >= 6:
                    results.append({
                        'timestamp': row[0],
                        'original_url': row[1],
                        'status_code': int(row[2]) if row[2].isdigit() else None,
                        'mimetype': row[3],
                        'length': int(row[4]) if row[4].isdigit() else None,
                        'digest': row[5],
                        'wayback_url': f"{self.wayback_base_url}/{row[0]}/{url}"
                    })
            
            # Cache results
            self._cache_cdx_results('wayback', url, results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching Wayback CDX for {url}: {e}")
            return []
    
    def search_eota_cdx(self, url: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Search EOTA CDX index for archived versions"""
        try:
            self._rate_limit()
            
            params = {
                'url': url,
                'output': 'json',
                'fl': 'timestamp,original,statuscode,mimetype,length,digest'
            }
            
            if start_date:
                params['from'] = start_date
            if end_date:
                params['to'] = end_date
            
            response = requests.get(self.eota_cdx_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse CDX response (same format as Wayback)
            cdx_data = response.json()
            if not cdx_data or len(cdx_data) < 2:
                return []
            
            results = []
            for row in cdx_data[1:]:
                if len(row) >= 6:
                    results.append({
                        'timestamp': row[0],
                        'original_url': row[1],
                        'status_code': int(row[2]) if row[2].isdigit() else None,
                        'mimetype': row[3],
                        'length': int(row[4]) if row[4].isdigit() else None,
                        'digest': row[5],
                        'eota_url': f"https://eotarchive.com/web/{row[0]}/{url}"
                    })
            
            # Cache results
            self._cache_cdx_results('eota', url, results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching EOTA CDX for {url}: {e}")
            return []
    
    def fetch_archived_content(self, wayback_url: str, output_path: Path) -> bool:
        """Fetch archived content from Wayback Machine"""
        try:
            self._rate_limit()
            
            response = requests.get(wayback_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Create output directory
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save content
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Fetched archived content: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error fetching archived content from {wayback_url}: {e}")
            return False
    
    def extract_dataset_manifest(self, content: str, url: str) -> Optional[Dict]:
        """Extract dataset manifest from archived HTML content"""
        try:
            # Try to find JSON-LD structured data
            json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            matches = re.findall(json_ld_pattern, content, re.DOTALL | re.IGNORECASE)
            
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict) and data.get('@type') == 'Dataset':
                        return self._normalize_manifest(data, url)
                except json.JSONDecodeError:
                    continue
            
            # Try to find CKAN dataset data
            ckan_pattern = r'window\.CKAN\._initialState\s*=\s*({.*?});'
            matches = re.findall(ckan_pattern, content, re.DOTALL)
            
            for match in matches:
                try:
                    data = json.loads(match)
                    if 'dataset' in data:
                        return self._normalize_ckan_manifest(data['dataset'], url)
                except json.JSONDecodeError:
                    continue
            
            # Try to find data.gov specific patterns
            return self._extract_datagov_manifest(content, url)
            
        except Exception as e:
            logger.error(f"Error extracting manifest from {url}: {e}")
            return None
    
    def reconstruct_vanished_dataset(self, dataset_id: str, vanished_info: Dict) -> bool:
        """Reconstruct a vanished dataset from archival sources"""
        try:
            logger.info(f"Reconstructing vanished dataset: {dataset_id}")
            
            # Get last known information
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT title, agency, publisher, license, landing_page, modified, resources
                FROM historian_snapshots
                WHERE dataset_id = ? AND source = ?
                ORDER BY snapshot_date DESC
                LIMIT 1
            ''', (dataset_id, vanished_info['last_seen_source']))
            
            last_known = cursor.fetchone()
            if not last_known:
                logger.warning(f"No last known data for vanished dataset: {dataset_id}")
                return False
            
            title, agency, publisher, license, landing_page, modified, resources_json = last_known
            resources = json.loads(resources_json) if resources_json else []
            
            # Search for archived versions
            archival_sources = []
            
            # Search Wayback Machine
            if landing_page:
                wayback_results = self.search_wayback_cdx(
                    landing_page, 
                    start_date=vanished_info['last_seen_date'],
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                archival_sources.extend([(r, 'wayback') for r in wayback_results])
            
            # Search EOTA
            if landing_page:
                eota_results = self.search_eota_cdx(
                    landing_page,
                    start_date=vanished_info['last_seen_date'],
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                archival_sources.extend([(r, 'eota') for r in eota_results])
            
            if not archival_sources:
                logger.warning(f"No archival sources found for {dataset_id}")
                return False
            
            # Process each archival source
            reconstructed_snapshots = []
            for result, source in archival_sources:
                try:
                    snapshot = self._process_archived_snapshot(
                        dataset_id, result, source, vanished_info
                    )
                    if snapshot:
                        reconstructed_snapshots.append(snapshot)
                except Exception as e:
                    logger.error(f"Error processing archived snapshot for {dataset_id}: {e}")
                    continue
            
            # Store reconstructed snapshots
            for snapshot in reconstructed_snapshots:
                self._store_archived_snapshot(snapshot)
            
            # Update vanished dataset record
            self._update_vanished_dataset_record(dataset_id, vanished_info, archival_sources)
            
            conn.close()
            logger.info(f"Reconstructed {len(reconstructed_snapshots)} snapshots for {dataset_id}")
            return len(reconstructed_snapshots) > 0
            
        except Exception as e:
            logger.error(f"Error reconstructing vanished dataset {dataset_id}: {e}")
            return False
    
    def _process_archived_snapshot(self, dataset_id: str, cdx_result: Dict, 
                                 source: str, vanished_info: Dict) -> Optional[Dict]:
        """Process a single archived snapshot"""
        try:
            timestamp = cdx_result['timestamp']
            wayback_url = cdx_result.get('wayback_url') or cdx_result.get('eota_url')
            
            if not wayback_url:
                return None
            
            # Convert timestamp to date
            snapshot_date = datetime.strptime(timestamp, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            
            # Create output path
            output_dir = self.data_dir / "vanished" / dataset_id / snapshot_date
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Fetch archived content
            content_path = output_dir / "archived_content.html"
            if not self.fetch_archived_content(wayback_url, content_path):
                return None
            
            # Extract manifest
            with open(content_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            manifest = self.extract_dataset_manifest(content, wayback_url)
            if not manifest:
                # Use last known data as fallback
                manifest = {
                    'title': vanished_info.get('last_known_title', 'Unknown Dataset'),
                    'agency': vanished_info.get('last_known_agency', 'Unknown Agency'),
                    'publisher': 'Unknown Publisher',
                    'license': 'Unknown License',
                    'landing_page': wayback_url,
                    'modified': snapshot_date,
                    'resources': []
                }
            
            # Create provenance information
            provenance = {
                'source': source,
                'captured_at': timestamp,
                'original_url': cdx_result.get('original_url'),
                'wayback_url': wayback_url,
                'status_code': cdx_result.get('status_code'),
                'mimetype': cdx_result.get('mimetype'),
                'length': cdx_result.get('length'),
                'digest': cdx_result.get('digest')
            }
            
            return {
                'dataset_id': dataset_id,
                'snapshot_date': snapshot_date,
                'source': source,
                'title': manifest.get('title'),
                'agency': manifest.get('agency'),
                'publisher': manifest.get('publisher'),
                'license': manifest.get('license'),
                'landing_page': manifest.get('landing_page'),
                'modified': manifest.get('modified'),
                'resources': json.dumps(manifest.get('resources', [])),
                'schema_data': json.dumps(manifest.get('schema', {})),
                'fingerprint': json.dumps(manifest.get('fingerprint', {})),
                'metadata': json.dumps(manifest.get('metadata', {})),
                'provenance': json.dumps(provenance),
                'status': 'archived',
                'last_seen_date': vanished_info['last_seen_date'],
                'file_path': str(content_path),
                'manifest_path': str(output_dir / "manifest.json")
            }
            
        except Exception as e:
            logger.error(f"Error processing archived snapshot: {e}")
            return None
    
    def _normalize_manifest(self, data: Dict, url: str) -> Dict:
        """Normalize JSON-LD manifest data"""
        return {
            'title': data.get('name', 'Unknown Dataset'),
            'agency': data.get('publisher', {}).get('name', 'Unknown Agency'),
            'publisher': data.get('publisher', {}).get('name', 'Unknown Publisher'),
            'license': data.get('license', 'Unknown License'),
            'landing_page': url,
            'modified': data.get('dateModified', ''),
            'resources': data.get('distribution', []),
            'schema': data.get('schema', {}),
            'metadata': data
        }
    
    def _normalize_ckan_manifest(self, data: Dict, url: str) -> Dict:
        """Normalize CKAN manifest data"""
        return {
            'title': data.get('title', 'Unknown Dataset'),
            'agency': data.get('organization', {}).get('title', 'Unknown Agency'),
            'publisher': data.get('organization', {}).get('title', 'Unknown Publisher'),
            'license': data.get('license_title', 'Unknown License'),
            'landing_page': url,
            'modified': data.get('metadata_modified', ''),
            'resources': data.get('resources', []),
            'schema': {},
            'metadata': data
        }
    
    def _extract_datagov_manifest(self, content: str, url: str) -> Optional[Dict]:
        """Extract data.gov specific manifest patterns"""
        # This would contain data.gov specific extraction logic
        # For now, return a basic structure
        return {
            'title': 'Archived Dataset',
            'agency': 'Unknown Agency',
            'publisher': 'Unknown Publisher',
            'license': 'Unknown License',
            'landing_page': url,
            'modified': '',
            'resources': [],
            'schema': {},
            'metadata': {'source': 'wayback_extraction'}
        }
    
    def _store_archived_snapshot(self, snapshot_data: Dict) -> bool:
        """Store an archived snapshot in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO historian_snapshots
                (dataset_id, snapshot_date, source, title, agency, publisher, license,
                 landing_page, modified, resources, schema_data, fingerprint, metadata,
                 provenance, status, last_seen_date, file_path, manifest_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_data['dataset_id'],
                snapshot_data['snapshot_date'],
                snapshot_data['source'],
                snapshot_data['title'],
                snapshot_data['agency'],
                snapshot_data['publisher'],
                snapshot_data['license'],
                snapshot_data['landing_page'],
                snapshot_data['modified'],
                snapshot_data['resources'],
                snapshot_data['schema_data'],
                snapshot_data['fingerprint'],
                snapshot_data['metadata'],
                snapshot_data['provenance'],
                snapshot_data['status'],
                snapshot_data['last_seen_date'],
                snapshot_data['file_path'],
                snapshot_data['manifest_path']
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error storing archived snapshot: {e}")
            return False
    
    def _update_vanished_dataset_record(self, dataset_id: str, vanished_info: Dict, 
                                      archival_sources: List) -> None:
        """Update the vanished dataset record with archival source information"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare archival sources data
            sources_data = []
            for result, source in archival_sources:
                sources_data.append({
                    'source': source,
                    'timestamp': result['timestamp'],
                    'url': result.get('wayback_url') or result.get('eota_url'),
                    'status_code': result.get('status_code')
                })
            
            cursor.execute('''
                INSERT OR REPLACE INTO vanished_datasets
                (dataset_id, last_seen_date, last_seen_source, disappearance_date,
                 archival_sources, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                dataset_id,
                vanished_info['last_seen_date'],
                vanished_info['last_seen_source'],
                datetime.now().strftime('%Y-%m-%d'),
                json.dumps(sources_data),
                'vanished'
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating vanished dataset record: {e}")
    
    def _cache_cdx_results(self, source: str, url: str, results: List[Dict]) -> None:
        """Cache CDX search results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            table_name = f"{source}_cdx_cache"
            for result in results:
                cursor.execute(f'''
                    INSERT OR REPLACE INTO {table_name}
                    (url, timestamp, original_url, mimetype, status_code, digest, length, wayback_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    url,
                    result['timestamp'],
                    result['original_url'],
                    result['mimetype'],
                    result['status_code'],
                    result['digest'],
                    result['length'],
                    result.get('wayback_url') or result.get('eota_url')
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error caching CDX results: {e}")
    
    def _rate_limit(self) -> None:
        """Implement rate limiting for API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        
        self.last_request_time = time.time()
    
    def get_vanished_datasets(self) -> List[Dict]:
        """Get list of all vanished datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT dataset_id, last_seen_date, last_seen_source, disappearance_date,
                       last_known_title, last_known_agency, archival_sources, status
                FROM vanished_datasets
                ORDER BY disappearance_date DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'dataset_id': row[0],
                    'last_seen_date': row[1],
                    'last_seen_source': row[2],
                    'disappearance_date': row[3],
                    'last_known_title': row[4],
                    'last_known_agency': row[5],
                    'archival_sources': json.loads(row[6]) if row[6] else [],
                    'status': row[7]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting vanished datasets: {e}")
            return []
    
    def reconstruct_all_vanished(self) -> Dict:
        """Reconstruct all vanished datasets"""
        logger.info("Starting reconstruction of all vanished datasets")
        
        vanished_datasets = self.detect_vanished_datasets()
        results = {
            'total_vanished': len(vanished_datasets),
            'successful_reconstructions': 0,
            'failed_reconstructions': 0,
            'errors': []
        }
        
        for vanished_info in vanished_datasets:
            try:
                success = self.reconstruct_vanished_dataset(
                    vanished_info['dataset_id'], 
                    vanished_info
                )
                if success:
                    results['successful_reconstructions'] += 1
                else:
                    results['failed_reconstructions'] += 1
            except Exception as e:
                error_msg = f"Error reconstructing {vanished_info['dataset_id']}: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['failed_reconstructions'] += 1
        
        logger.info(f"Reconstruction complete: {results['successful_reconstructions']} successful, {results['failed_reconstructions']} failed")
        return results
