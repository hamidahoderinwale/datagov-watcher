"""
Enhanced Wayback Machine Integration
Provides comprehensive Wayback Machine integration for gap-filling and historical data
"""

import requests
import json
import sqlite3
import hashlib
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
import re
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

class WaybackEnhanced:
    """Enhanced Wayback Machine integration for historical data recovery"""
    
    def __init__(self, db_path: str = "datasets.db", data_dir: str = "dataset_states/wayback_data"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Wayback Machine API endpoints
        self.wayback_api = "https://web.archive.org/web"
        self.wayback_cdx_api = "https://web.archive.org/cdx/search/cdx"
        self.wayback_availability_api = "https://web.archive.org/wayback/available"
        
        # Rate limiting
        self.request_delay = 1.0  # seconds between requests
        self.last_request_time = 0
        
        self.init_database()
    
    def init_database(self):
        """Initialize Wayback-specific database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Wayback snapshots
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wayback_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                status_code INTEGER,
                content_type TEXT,
                content_length INTEGER,
                wayback_url TEXT,
                title TEXT,
                description TEXT,
                file_path TEXT,
                content_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, timestamp)
            )
        ''')
        
        # Wayback availability index
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wayback_availability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                closest_timestamp TEXT,
                closest_url TEXT,
                available BOOLEAN,
                last_checked TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url)
            )
        ''')
        
        # Wayback content analysis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wayback_content_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                content_type TEXT,
                is_data_file BOOLEAN,
                file_format TEXT,
                row_count INTEGER,
                column_count INTEGER,
                schema_info TEXT,  -- JSON
                content_summary TEXT,  -- JSON
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, timestamp)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def find_closest_snapshot(self, url: str, target_date: Optional[str] = None) -> Optional[Dict]:
        """Find the closest Wayback snapshot for a URL"""
        try:
            self._rate_limit()
            
            params = {
                'url': url,
                'output': 'json',
                'fl': 'timestamp,original,statuscode,mimetype,length'
            }
            
            if target_date:
                params['from'] = target_date
                params['to'] = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d%H%M%S')
            
            response = requests.get(self.wayback_cdx_api, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if not data or len(data) < 2:  # Header + data
                return None
            
            # Parse CDX data
            headers = data[0]
            snapshots = []
            
            for row in data[1:]:
                if len(row) >= len(headers):
                    snapshot = dict(zip(headers, row))
                    snapshots.append(snapshot)
            
            if not snapshots:
                return None
            
            # Find the closest snapshot to target date
            if target_date:
                target_ts = self._parse_timestamp(target_date)
                closest = min(snapshots, 
                             key=lambda x: abs(int(x.get('timestamp', '0')) - target_ts))
            else:
                # Get the most recent snapshot
                closest = max(snapshots, key=lambda x: int(x.get('timestamp', '0')))
            
            # Build wayback URL
            timestamp = closest['timestamp']
            wayback_url = f"{self.wayback_api}/{timestamp}/{url}"
            
            return {
                'url': url,
                'timestamp': timestamp,
                'wayback_url': wayback_url,
                'status_code': int(closest.get('statuscode', 0)),
                'content_type': closest.get('mimetype', ''),
                'content_length': int(closest.get('length', 0)) if closest.get('length') else 0
            }
            
        except Exception as e:
            logger.error(f"Error finding closest snapshot for {url}: {e}")
            return None
    
    def get_snapshot_content(self, url: str, timestamp: str) -> Optional[Dict]:
        """Get content from a specific Wayback snapshot"""
        try:
            self._rate_limit()
            
            wayback_url = f"{self.wayback_api}/{timestamp}/{url}"
            
            response = requests.get(wayback_url, timeout=30)
            response.raise_for_status()
            
            content = response.text
            content_hash = hashlib.sha256(content.encode()).hexdigest()
            
            # Analyze content
            analysis = self._analyze_content(content, response.headers.get('content-type', ''))
            
            # Store snapshot
            snapshot_data = {
                'url': url,
                'timestamp': timestamp,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(content),
                'wayback_url': wayback_url,
                'content_hash': content_hash,
                'analysis': analysis
            }
            
            self._store_snapshot(snapshot_data)
            
            return snapshot_data
            
        except Exception as e:
            logger.error(f"Error getting snapshot content for {url} at {timestamp}: {e}")
            return None
    
    def _analyze_content(self, content: str, content_type: str) -> Dict:
        """Analyze content to determine if it's a data file"""
        analysis = {
            'is_data_file': False,
            'file_format': 'unknown',
            'row_count': 0,
            'column_count': 0,
            'schema_info': {},
            'content_summary': {}
        }
        
        content_lower = content_type.lower()
        
        # Check if it's a data file based on content type
        if any(fmt in content_lower for fmt in ['csv', 'json', 'xml', 'excel', 'spreadsheet']):
            analysis['is_data_file'] = True
            
            # Determine format
            if 'csv' in content_lower:
                analysis['file_format'] = 'csv'
                csv_analysis = self._analyze_csv_content(content)
                analysis.update(csv_analysis)
                
            elif 'json' in content_lower:
                analysis['file_format'] = 'json'
                json_analysis = self._analyze_json_content(content)
                analysis.update(json_analysis)
                
            elif 'xml' in content_lower:
                analysis['file_format'] = 'xml'
                xml_analysis = self._analyze_xml_content(content)
                analysis.update(xml_analysis)
        
        # Check content for data patterns
        elif self._looks_like_data(content):
            analysis['is_data_file'] = True
            analysis['file_format'] = self._detect_format_from_content(content)
            
            # Analyze based on detected format
            if analysis['file_format'] == 'csv':
                csv_analysis = self._analyze_csv_content(content)
                analysis.update(csv_analysis)
            elif analysis['file_format'] == 'json':
                json_analysis = self._analyze_json_content(content)
                analysis.update(json_analysis)
        
        return analysis
    
    def _looks_like_data(self, content: str) -> bool:
        """Check if content looks like structured data"""
        # Check for common data patterns
        data_indicators = [
            r'\d+,\d+',  # Numbers with commas
            r'"[^"]*","[^"]*"',  # CSV-like quoted fields
            r'\{[^}]*\}',  # JSON-like objects
            r'<[^>]+>',  # XML-like tags
        ]
        
        for pattern in data_indicators:
            if re.search(pattern, content[:1000]):  # Check first 1000 chars
                return True
        
        return False
    
    def _detect_format_from_content(self, content: str) -> str:
        """Detect file format from content"""
        content_start = content.strip()[:100]
        
        if content_start.startswith('{') or content_start.startswith('['):
            return 'json'
        elif content_start.startswith('<'):
            return 'xml'
        elif ',' in content_start and ('"' in content_start or content_start.count(',') > 2):
            return 'csv'
        else:
            return 'unknown'
    
    def _analyze_csv_content(self, content: str) -> Dict:
        """Analyze CSV content"""
        try:
            lines = content.split('\n')
            if not lines:
                return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
            
            # Count non-empty lines
            non_empty_lines = [line for line in lines if line.strip()]
            row_count = len(non_empty_lines) - 1  # Subtract header
            
            # Analyze first line for columns
            if non_empty_lines:
                first_line = non_empty_lines[0]
                columns = [col.strip().strip('"') for col in first_line.split(',')]
                column_count = len(columns)
                
                schema_info = {
                    'columns': columns,
                    'has_header': True,
                    'delimiter': ','
                }
            else:
                column_count = 0
                schema_info = {}
            
            return {
                'row_count': max(0, row_count),
                'column_count': column_count,
                'schema_info': schema_info
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing CSV content: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def _analyze_json_content(self, content: str) -> Dict:
        """Analyze JSON content"""
        try:
            data = json.loads(content)
            
            if isinstance(data, list):
                row_count = len(data)
                if data and isinstance(data[0], dict):
                    column_count = len(data[0].keys())
                    schema_info = {
                        'structure': 'array',
                        'columns': list(data[0].keys()),
                        'sample_data': data[:3]
                    }
                else:
                    column_count = 0
                    schema_info = {'structure': 'array'}
                    
            elif isinstance(data, dict):
                row_count = 1
                column_count = len(data.keys())
                schema_info = {
                    'structure': 'object',
                    'columns': list(data.keys()),
                    'sample_data': data
                }
            else:
                row_count = 1
                column_count = 0
                schema_info = {'structure': 'primitive'}
            
            return {
                'row_count': row_count,
                'column_count': column_count,
                'schema_info': schema_info
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing JSON content: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def _analyze_xml_content(self, content: str) -> Dict:
        """Analyze XML content"""
        try:
            root = ET.fromstring(content)
            
            # Count elements (rough estimate of records)
            elements = list(root.iter())
            row_count = len(elements)
            
            # Get root element attributes
            column_count = len(root.attrib) if hasattr(root, 'attrib') else 0
            
            schema_info = {
                'root_tag': root.tag,
                'attributes': list(root.attrib.keys()) if hasattr(root, 'attrib') else [],
                'structure': 'xml'
            }
            
            return {
                'row_count': row_count,
                'column_count': column_count,
                'schema_info': schema_info
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing XML content: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def _parse_timestamp(self, date_str: str) -> int:
        """Parse timestamp string to integer"""
        try:
            if len(date_str) == 8:  # YYYYMMDD
                dt = datetime.strptime(date_str, '%Y%m%d')
                return int(dt.timestamp())
            elif len(date_str) == 14:  # YYYYMMDDHHMMSS
                dt = datetime.strptime(date_str, '%Y%m%d%H%M%S')
                return int(dt.timestamp())
            else:
                return 0
        except:
            return 0
    
    def _rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def _store_snapshot(self, snapshot_data: Dict):
        """Store snapshot data in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO wayback_snapshots
                (url, timestamp, status_code, content_type, content_length,
                 wayback_url, content_hash, file_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_data['url'],
                snapshot_data['timestamp'],
                snapshot_data['status_code'],
                snapshot_data['content_type'],
                snapshot_data['content_length'],
                snapshot_data['wayback_url'],
                snapshot_data['content_hash'],
                None  # file_path
            ))
            
            # Store content analysis
            analysis = snapshot_data.get('analysis', {})
            cursor.execute('''
                INSERT OR REPLACE INTO wayback_content_analysis
                (url, timestamp, content_type, is_data_file, file_format,
                 row_count, column_count, schema_info, content_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot_data['url'],
                snapshot_data['timestamp'],
                snapshot_data['content_type'],
                analysis.get('is_data_file', False),
                analysis.get('file_format', 'unknown'),
                analysis.get('row_count', 0),
                analysis.get('column_count', 0),
                json.dumps(analysis.get('schema_info', {})),
                json.dumps(analysis.get('content_summary', {}))
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error storing snapshot: {e}")
    
    def get_wayback_timeline(self, url: str) -> List[Dict]:
        """Get timeline of all Wayback snapshots for a URL"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, status_code, content_type, content_length,
                   wayback_url, content_hash
            FROM wayback_snapshots
            WHERE url = ?
            ORDER BY timestamp DESC
        ''', (url,))
        
        timeline = []
        for row in cursor.fetchall():
            timeline.append({
                'timestamp': row[0],
                'status_code': row[1],
                'content_type': row[2],
                'content_length': row[3],
                'wayback_url': row[4],
                'content_hash': row[5]
            })
        
        conn.close()
        return timeline
    
    def find_missing_datasets(self, current_datasets: List[str], historical_datasets: List[str]) -> List[Dict]:
        """Find datasets that are missing from current but present in historical"""
        missing = []
        
        current_set = set(current_datasets)
        historical_set = set(historical_datasets)
        
        missing_ids = historical_set - current_set
        
        for dataset_id in missing_ids:
            # Try to find in Wayback
            wayback_info = self.find_closest_snapshot(f"https://catalog.data.gov/dataset/{dataset_id}")
            if wayback_info:
                missing.append({
                    'dataset_id': dataset_id,
                    'last_seen': wayback_info['timestamp'],
                    'wayback_url': wayback_info['wayback_url'],
                    'status': 'found_in_wayback'
                })
            else:
                missing.append({
                    'dataset_id': dataset_id,
                    'last_seen': 'unknown',
                    'wayback_url': None,
                    'status': 'not_found'
                })
        
        return missing


