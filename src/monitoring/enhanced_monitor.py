"""
Enhanced Concordance Monitor: Full-Scale Dataset Monitoring
Analyzes ALL datasets with live diffing, provenance tracking, and advanced features
"""

import asyncio
import aiohttp
import sqlite3
import json
import hashlib
import time
import ssl
import io
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

logger = logging.getLogger(__name__)

class EnhancedConcordanceMonitor:
    def __init__(self, db_path: str = "datasets.db", max_workers: int = 10):
        self.db_path = db_path
        self.max_workers = max_workers
        self.running = False
        self.change_queue = queue.Queue()
        self.init_enhanced_tables()
        
        # Create output directories
        Path("dataset_states").mkdir(exist_ok=True)
        Path("live_diffs").mkdir(exist_ok=True)
        Path("provenance_logs").mkdir(exist_ok=True)
        Path("alerts").mkdir(exist_ok=True)
    
    def init_enhanced_tables(self):
        """Initialize enhanced database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Live monitoring table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS live_monitoring (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                last_checked TIMESTAMP,
                status TEXT,
                response_time_ms INTEGER,
                content_hash TEXT,
                change_detected BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Provenance tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS provenance_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                event_type TEXT,
                event_description TEXT,
                old_value TEXT,
                new_value TEXT,
                confidence_score REAL,
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Change alerts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS change_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                alert_type TEXT,
                severity TEXT,
                message TEXT,
                metadata TEXT,
                acknowledged BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Performance metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT,
                metric_value REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def fetch_all_datasets(self) -> List[Dict]:
        """Fetch ALL datasets from Data.gov with pagination"""
        all_datasets = []
        offset = 0
        batch_size = 1000
        
        async with aiohttp.ClientSession() as session:
            while True:
                url = f"https://catalog.data.gov/api/3/action/package_search?rows={batch_size}&start={offset}"
                
                try:
                    async with session.get(url, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            datasets = data.get('result', {}).get('results', [])
                            
                            if not datasets:
                                break
                            
                            all_datasets.extend(datasets)
                            logger.info(f"Fetched {len(datasets)} datasets (total: {len(all_datasets)})")
                            
                            offset += batch_size
                            
                            # Rate limiting
                            await asyncio.sleep(0.1)
                        else:
                            logger.error(f"API error: {response.status}")
                            break
                
                except Exception as e:
                    logger.error(f"Error fetching datasets: {e}")
                    logger.error(f"Failed at offset: {offset}")
                    break
        
        logger.info(f"Total datasets fetched: {len(all_datasets)}")
        return all_datasets
    
    async def analyze_dataset_async(self, session: aiohttp.ClientSession, dataset: Dict) -> Dict:
        """Analyze a single dataset asynchronously"""
        dataset_id = dataset.get('id', '')
        title = dataset.get('title', 'Unknown')
        
        # Get the best resource URL (prioritize CSV, JSON, XML, etc.)
        resources = dataset.get('resources', [])
        url = None
        resource_format = None
        
        # Look for data files (CSV, JSON, XML, etc.) first
        data_formats = ['CSV', 'JSON', 'XML', 'XLS', 'XLSX', 'ZIP', 'TXT', 'TSV']
        for resource in resources:
            format_type = resource.get('format', '').upper()
            if format_type in data_formats:
                url = resource.get('url', '')
                resource_format = format_type
                break
        
        # If no data file found, try any resource
        if not url and resources:
            url = resources[0].get('url', '')
            resource_format = resources[0].get('format', 'Unknown')
        
        # Fallback to dataset URL if no resources
        if not url:
            url = dataset.get('url', '')
            resource_format = 'Web Page'
        
        # Validate URL before making request
        if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            return {
                'dataset_id': dataset_id,
                'status': 'error',
                'error': 'Invalid or missing URL',
                'response_time_ms': 0,
                'content_hash': None,
                'change_detected': False,
                'timestamp': datetime.now().isoformat()
            }
        
        start_time = time.time()
        
        try:
            # Check URL availability and fetch content
            logger.debug(f"Analyzing dataset {dataset_id}: {url} (format: {resource_format})")
            
            # Create SSL context that doesn't verify certificates for problematic sites
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as analysis_session:
                async with analysis_session.get(url, timeout=30, allow_redirects=True) as response:
                    response_time = int((time.time() - start_time) * 1000)
                    status_code = response.status
                    
                    # Get content hash and analyze data
                    content_hash = None
                    file_size = 0
                    content_type = response.headers.get('content-type', 'unknown')
                    row_count = 0
                    column_count = 0
                    schema_info = {}
                    
                    if status_code == 200:
                        try:
                            content = await response.read()
                            content_hash = hashlib.sha256(content).hexdigest()
                            file_size = len(content)
                            
                            # Analyze content based on format
                            if resource_format in ['CSV', 'TXT', 'TSV']:
                                try:
                                    # Try to parse CSV content
                                    text_content = content.decode('utf-8', errors='ignore')
                                    
                                    # Get full row count, not just first 100 lines
                                    all_lines = text_content.split('\n')
                                    row_count = len([line for line in all_lines if line.strip()]) - 1  # Subtract header
                                    
                                    # Parse CSV to get column info
                                    import pandas as pd
                                    df = pd.read_csv(io.StringIO(text_content))
                                    column_count = len(df.columns)
                                    
                                    # Get column info
                                    schema_info = {
                                        'columns': list(df.columns),
                                        'dtypes': df.dtypes.to_dict(),
                                        'sample_data': df.head(3).to_dict('records')
                                    }
                                    
                                except Exception as e:
                                    logger.debug(f"Could not parse CSV for {dataset_id}: {e}")
                                    # Fallback: count lines
                                    all_lines = text_content.split('\n')
                                    row_count = len([line for line in all_lines if line.strip()]) - 1
                                    column_count = len(all_lines[0].split(',')) if all_lines else 0
                            
                            elif resource_format == 'JSON':
                                try:
                                    json_data = json.loads(content.decode('utf-8', errors='ignore'))
                                    
                                    if isinstance(json_data, list):
                                        row_count = len(json_data)
                                        if json_data:
                                            column_count = len(json_data[0].keys()) if isinstance(json_data[0], dict) else 0
                                            schema_info = {
                                                'sample_data': json_data[:3],
                                                'structure': 'array'
                                            }
                                    elif isinstance(json_data, dict):
                                        row_count = 1
                                        column_count = len(json_data.keys())
                                        schema_info = {
                                            'sample_data': json_data,
                                            'structure': 'object'
                                        }
                                    
                                except Exception as e:
                                    logger.debug(f"Could not parse JSON for {dataset_id}: {e}")
                            
                            elif resource_format == 'ZIP':
                                try:
                                    import zipfile
                                    with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                                        # Find CSV files in the ZIP
                                        csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                                        if csv_files:
                                            # Analyze the first CSV file found
                                            with zip_file.open(csv_files[0]) as csv_file:
                                                csv_content = csv_file.read().decode('utf-8', errors='ignore')
                                                all_lines = csv_content.split('\n')
                                                row_count = len([line for line in all_lines if line.strip()]) - 1
                                                
                                                # Parse CSV to get column info
                                                import pandas as pd
                                                df = pd.read_csv(io.StringIO(csv_content))
                                                column_count = len(df.columns)
                                                
                                                schema_info = {
                                                    'columns': list(df.columns),
                                                    'dtypes': df.dtypes.to_dict(),
                                                    'sample_data': df.head(3).to_dict('records'),
                                                    'zip_files': csv_files
                                                }
                                        else:
                                            # No CSV files found, just count total files
                                            row_count = len(zip_file.namelist())
                                            column_count = 0
                                            schema_info = {
                                                'zip_files': zip_file.namelist(),
                                                'structure': 'zip_archive'
                                            }
                                    
                                except Exception as e:
                                    logger.debug(f"Could not parse ZIP for {dataset_id}: {e}")
                                    row_count = 0
                                    column_count = 0
                            
                            elif resource_format in ['XLS', 'XLSX']:
                                try:
                                    import pandas as pd
                                    # Read Excel file
                                    df = pd.read_excel(io.BytesIO(content))
                                    row_count = len(df)
                                    column_count = len(df.columns)
                                    
                                    schema_info = {
                                        'columns': list(df.columns),
                                        'dtypes': df.dtypes.to_dict(),
                                        'sample_data': df.head(3).to_dict('records')
                                    }
                                    
                                except Exception as e:
                                    logger.debug(f"Could not parse Excel for {dataset_id}: {e}")
                                    row_count = 0
                                    column_count = 0
                            
                            elif resource_format == 'XML':
                                try:
                                    import xml.etree.ElementTree as ET
                                    root = ET.fromstring(content.decode('utf-8', errors='ignore'))
                                    
                                    # Count XML elements (rough estimate of records)
                                    row_count = len(list(root.iter()))
                                    column_count = len(root.attrib) if hasattr(root, 'attrib') else 0
                                    
                                    schema_info = {
                                        'root_tag': root.tag,
                                        'attributes': list(root.attrib.keys()) if hasattr(root, 'attrib') else [],
                                        'structure': 'xml'
                                    }
                                    
                                except Exception as e:
                                    logger.debug(f"Could not parse XML for {dataset_id}: {e}")
                                    row_count = 0
                                    column_count = 0
                            
                            # Skip analysis for non-data formats
                            elif resource_format in ['HTML', 'PDF', 'API', '']:
                                row_count = 0
                                column_count = 0
                                schema_info = {
                                    'structure': 'non_data_format',
                                    'format': resource_format
                                }
                            
                            # Check for changes
                            change_detected = self.check_for_changes(dataset_id, content_hash)
                            
                            # Store comprehensive snapshot
                            self.store_dataset_snapshot(dataset_id, {
                                'title': title,
                                'agency': dataset.get('organization', {}).get('title', 'Unknown'),
                                'url': url,
                                'status_code': status_code,
                                'content_hash': content_hash,
                                'file_size': file_size,
                                'content_type': content_type,
                                'resource_format': resource_format,
                                'row_count': row_count,
                                'column_count': column_count,
                                'schema': schema_info,
                                'last_modified': response.headers.get('last-modified', ''),
                                'availability': 'available'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Could not analyze content for {dataset_id}: {e}")
                            change_detected = False
                    else:
                        change_detected = False
                        # Store error snapshot
                        self.store_dataset_snapshot(dataset_id, {
                            'title': title,
                            'agency': dataset.get('organization', {}).get('title', 'Unknown'),
                            'url': url,
                            'status_code': status_code,
                            'content_hash': None,
                            'file_size': 0,
                            'content_type': content_type,
                            'resource_format': resource_format,
                            'row_count': 0,
                            'column_count': 0,
                            'schema': {},
                            'availability': 'unavailable'
                        })
                    
                    # Log provenance if change detected
                    if change_detected:
                        self.log_provenance_change(dataset_id, content_hash)
                    
                    # Create alert if significant change
                    if change_detected:
                        self.create_change_alert(dataset_id, "content_change", "medium")
                
                return {
                    'dataset_id': dataset_id,
                    'status': 'available' if status_code == 200 else 'unavailable',
                    'response_time_ms': response_time,
                    'content_hash': content_hash,
                    'change_detected': change_detected,
                    'status_code': status_code,
                    'timestamp': datetime.now().isoformat()
                }
        
        except asyncio.TimeoutError:
            return {
                'dataset_id': dataset_id,
                'status': 'timeout',
                'response_time_ms': int((time.time() - start_time) * 1000),
                'content_hash': None,
                'change_detected': False,
                'status_code': 0,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error analyzing {dataset_id}: {e}")
            return {
                'dataset_id': dataset_id,
                'status': 'error',
                'response_time_ms': int((time.time() - start_time) * 1000),
                'content_hash': None,
                'change_detected': False,
                'status_code': 0,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def store_dataset_snapshot(self, dataset_id: str, snapshot_data: Dict):
        """Store a comprehensive dataset snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create dataset_states table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                title TEXT,
                agency TEXT,
                url TEXT,
                status_code INTEGER,
                content_hash TEXT,
                file_size INTEGER,
                content_type TEXT,
                resource_format TEXT,
                row_count INTEGER,
                column_count INTEGER,
                schema TEXT,
                last_modified TEXT,
                availability TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        # Insert snapshot
        cursor.execute('''
            INSERT OR REPLACE INTO dataset_states 
            (dataset_id, snapshot_date, title, agency, url, status_code, content_hash, 
             file_size, content_type, resource_format, row_count, column_count, 
             schema, last_modified, availability)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dataset_id,
            datetime.now().strftime('%Y-%m-%d'),
            snapshot_data.get('title', ''),
            snapshot_data.get('agency', ''),
            snapshot_data.get('url', ''),
            snapshot_data.get('status_code', 0),
            snapshot_data.get('content_hash', ''),
            snapshot_data.get('file_size', 0),
            snapshot_data.get('content_type', ''),
            snapshot_data.get('resource_format', ''),
            snapshot_data.get('row_count', 0),
            snapshot_data.get('column_count', 0),
            json.dumps(snapshot_data.get('schema', {})),
            snapshot_data.get('last_modified', ''),
            snapshot_data.get('availability', 'unknown')
        ))
        
        conn.commit()
        conn.close()
    
    def check_for_changes(self, dataset_id: str, new_hash: str) -> bool:
        """Check if dataset content has changed"""
        if not new_hash:
            return False
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get last known hash
        cursor.execute('''
            SELECT content_hash FROM live_monitoring 
            WHERE dataset_id = ? 
            ORDER BY last_checked DESC LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        last_hash = result[0] if result else None
        
        conn.close()
        
        return last_hash != new_hash
    
    def log_provenance_change(self, dataset_id: str, new_hash: str):
        """Log provenance change"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO provenance_log 
            (dataset_id, event_type, event_description, new_value, confidence_score, source)
            VALUES (?, 'content_change', 'Dataset content hash changed', ?, 0.9, 'live_monitoring')
        ''', (dataset_id, new_hash))
        
        conn.commit()
        conn.close()
    
    def create_change_alert(self, dataset_id: str, alert_type: str, severity: str):
        """Create change alert"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        message = f"Dataset {dataset_id} has changed ({alert_type})"
        
        cursor.execute('''
            INSERT INTO change_alerts 
            (dataset_id, alert_type, severity, message, metadata)
            VALUES (?, ?, ?, ?, ?)
        ''', (dataset_id, alert_type, severity, message, json.dumps({
            'timestamp': datetime.now().isoformat(),
            'alert_id': f"{dataset_id}_{int(time.time())}"
        })))
        
        conn.commit()
        conn.close()
    
    async def monitor_all_datasets(self):
        """Monitor ALL datasets with live diffing"""
        logger.info("Starting enhanced monitoring of ALL datasets")
        
        # Fetch all datasets
        all_datasets = await self.fetch_all_datasets()
        logger.info(f"Monitoring {len(all_datasets)} datasets")
        
        # Process in batches
        batch_size = 50
        semaphore = asyncio.Semaphore(self.max_workers)
        
        async def process_batch(batch):
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    tasks = [self.analyze_dataset_async(session, dataset) for dataset in batch]
                    return await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process all datasets in batches
        all_results = []
        for i in range(0, len(all_datasets), batch_size):
            batch = all_datasets[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(all_datasets) + batch_size - 1)//batch_size}")
            
            batch_results = await process_batch(batch)
            all_results.extend(batch_results)
            
            # Store results
            self.store_monitoring_results(batch_results)
            
            # Rate limiting between batches
            await asyncio.sleep(1)
        
        logger.info(f"Completed monitoring of {len(all_results)} datasets")
        return all_results
    
    def store_monitoring_results(self, results: List[Dict]):
        """Store monitoring results in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception in result: {result}")
                continue
            
            # Ensure timestamp exists
            timestamp = result.get('timestamp', datetime.now().isoformat())
            
            cursor.execute('''
                INSERT INTO live_monitoring 
                (dataset_id, last_checked, status, response_time_ms, content_hash, change_detected)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                result['dataset_id'],
                timestamp,
                result['status'],
                result['response_time_ms'],
                result['content_hash'],
                result['change_detected']
            ))
        
        conn.commit()
        conn.close()
    
    def get_monitoring_stats(self) -> Dict:
        """Get comprehensive monitoring statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Basic stats
        cursor.execute('SELECT COUNT(*) FROM live_monitoring')
        total_checks = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM live_monitoring')
        unique_datasets = cursor.fetchone()[0]
        
        # Status breakdown
        cursor.execute('''
            SELECT status, COUNT(*) FROM live_monitoring 
            WHERE last_checked > datetime('now', '-1 hour')
            GROUP BY status
        ''')
        status_breakdown = dict(cursor.fetchall())
        
        # Change stats
        cursor.execute('''
            SELECT COUNT(*) FROM live_monitoring 
            WHERE change_detected = TRUE 
            AND last_checked > datetime('now', '-24 hours')
        ''')
        recent_changes = cursor.fetchone()[0]
        
        # Performance stats
        cursor.execute('''
            SELECT AVG(response_time_ms), MAX(response_time_ms), MIN(response_time_ms)
            FROM live_monitoring 
            WHERE last_checked > datetime('now', '-1 hour')
        ''')
        perf_result = cursor.fetchone()
        avg_response_time = perf_result[0] if perf_result[0] else 0
        max_response_time = perf_result[1] if perf_result[1] else 0
        min_response_time = perf_result[2] if perf_result[2] else 0
        
        # Recent alerts
        cursor.execute('''
            SELECT COUNT(*) FROM change_alerts 
            WHERE created_at > datetime('now', '-24 hours')
        ''')
        recent_alerts = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_checks': total_checks,
            'unique_datasets': unique_datasets,
            'status_breakdown': status_breakdown,
            'recent_changes': recent_changes,
            'performance': {
                'avg_response_time_ms': avg_response_time,
                'max_response_time_ms': max_response_time,
                'min_response_time_ms': min_response_time
            },
            'recent_alerts': recent_alerts,
            'last_updated': datetime.now().isoformat()
        }
    
    def get_change_timeline(self, hours: int = 24) -> List[Dict]:
        """Get timeline of changes in the last N hours"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, last_checked, status, change_detected, response_time_ms
            FROM live_monitoring 
            WHERE last_checked > datetime('now', '-{} hours')
            AND change_detected = TRUE
            ORDER BY last_checked DESC
        '''.format(hours))
        
        columns = [description[0] for description in cursor.description]
        changes = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return changes
    
    def get_provenance_log(self, dataset_id: str = None) -> List[Dict]:
        """Get provenance log for dataset(s)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if dataset_id:
            cursor.execute('''
                SELECT * FROM provenance_log 
                WHERE dataset_id = ?
                ORDER BY created_at DESC
            ''', (dataset_id,))
        else:
            cursor.execute('''
                SELECT * FROM provenance_log 
                ORDER BY created_at DESC
                LIMIT 100
            ''')
        
        columns = [description[0] for description in cursor.description]
        log_entries = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return log_entries
    
    def get_active_alerts(self) -> List[Dict]:
        """Get active (unacknowledged) alerts with dataset information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if change_alerts table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='change_alerts'")
        alerts_table_exists = cursor.fetchone() is not None
        
        if not alerts_table_exists:
            conn.close()
            return []
        
        cursor.execute('''
            SELECT ca.*, ds.title, ds.agency
            FROM change_alerts ca
            LEFT JOIN dataset_states ds ON ca.dataset_id = ds.dataset_id
            WHERE ca.acknowledged = FALSE
            ORDER BY ca.created_at DESC
        ''')
        
        columns = [description[0] for description in cursor.description]
        alerts = []
        for row in cursor.fetchall():
            alert = dict(zip(columns, row))
            # Enhance the message with dataset title
            if alert.get('title'):
                alert['enhanced_message'] = f"{alert['title']} ({alert['dataset_id'][:8]}...)"
                alert['dataset_name'] = alert['title']
                alert['agency_name'] = alert.get('agency', 'Unknown Agency')
            else:
                alert['enhanced_message'] = f"Dataset {alert['dataset_id'][:8]}..."
                alert['dataset_name'] = f"Dataset {alert['dataset_id'][:8]}..."
                alert['agency_name'] = 'Unknown Agency'
            alerts.append(alert)
        
        conn.close()
        return alerts
    
    def acknowledge_alert(self, alert_id: int):
        """Acknowledge an alert"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE change_alerts 
            SET acknowledged = TRUE 
            WHERE id = ?
        ''', (alert_id,))
        
        conn.commit()
        conn.close()
    
    async def start_continuous_monitoring(self, interval_minutes: int = 30):
        """Start continuous monitoring with specified interval"""
        self.running = True
        logger.info(f"Starting continuous monitoring (every {interval_minutes} minutes)")
        
        while self.running:
            try:
                logger.info("Starting monitoring cycle")
                await self.monitor_all_datasets()
                logger.info("Monitoring cycle completed")
                
                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in monitoring cycle: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.running = False
        logger.info("Stopping continuous monitoring")
    
    async def run_quick_check(self):
        """Run a quick check on a sample of datasets for real-time updates"""
        logger.info("Running quick check on sample datasets")
        
        # Get a sample of datasets that haven't been checked recently
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get datasets that haven't been checked in the last hour
        cursor.execute('''
            SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url
            FROM dataset_states ds
            LEFT JOIN live_monitoring lm ON ds.dataset_id = lm.dataset_id
            WHERE lm.last_checked IS NULL 
            OR lm.last_checked < datetime('now', '-1 hour')
            ORDER BY ds.created_at DESC
            LIMIT 50
        ''')
        
        sample_datasets = cursor.fetchall()
        conn.close()
        
        if not sample_datasets:
            logger.info("No datasets need checking")
            return
        
        logger.info(f"Quick checking {len(sample_datasets)} datasets")
        
        # Convert to dict format for analysis
        datasets = []
        for row in sample_datasets:
            datasets.append({
                'id': row[0],
                'title': row[1],
                'organization': {'title': row[2]},
                'url': row[3],
                'resources': [{'url': row[3], 'format': 'Unknown'}]
            })
        
        # Process sample datasets
        async with aiohttp.ClientSession() as session:
            tasks = [self.analyze_dataset_async(session, dataset) for dataset in datasets]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Store results
        self.store_monitoring_results(results)
        
        logger.info(f"Quick check completed for {len(results)} datasets")

async def main():
    """Main function for testing"""
    monitor = EnhancedConcordanceMonitor()
    
    # Run one monitoring cycle
    await monitor.monitor_all_datasets()
    
    # Print stats
    stats = monitor.get_monitoring_stats()
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
