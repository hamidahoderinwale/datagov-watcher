"""
Enhanced Concordance Monitor with Guaranteed Row/Column Computation
Ensures all datasets have accurate row and column counts computed and stored
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
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import pandas as pd

logger = logging.getLogger(__name__)

class EnhancedConcordanceMonitorWithDimensions:
    """Enhanced monitor that guarantees row/column computation for all datasets"""
    
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
        """Initialize enhanced database tables with dimension tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced dataset_states table with dimension tracking
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
                dimensions_computed BOOLEAN DEFAULT FALSE,
                dimension_computation_date TIMESTAMP,
                dimension_computation_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        # Dimension computation tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dimension_computation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                computation_date TIMESTAMP,
                success BOOLEAN,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                error_message TEXT,
                computation_time_ms INTEGER,
                resource_format TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Live monitoring table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS live_monitoring (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                last_checked TIMESTAMP,
                status TEXT,
                response_time_ms INTEGER,
                error_message TEXT,
                dimensions_checked BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # State diffs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS state_diffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                from_date TEXT,
                to_date TEXT,
                diff_type TEXT,
                diff_data TEXT,
                volatility_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Change alerts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS change_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                alert_type TEXT,
                message TEXT,
                severity TEXT,
                acknowledged BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def monitor_all_datasets(self) -> Dict:
        """Monitor all datasets with guaranteed dimension computation"""
        logger.info("Starting comprehensive dataset monitoring with dimension computation")
        
        results = {
            'start_time': datetime.now().isoformat(),
            'datasets_processed': 0,
            'datasets_updated': 0,
            'dimensions_computed': 0,
            'errors': []
        }
        
        try:
            # Get all datasets from Data.gov
            datasets = await self.fetch_all_datasets()
            logger.info(f"Found {len(datasets)} datasets to monitor")
            
            # Process datasets in batches
            batch_size = 50
            for i in range(0, len(datasets), batch_size):
                batch = datasets[i:i + batch_size]
                batch_results = await self.process_batch_with_dimensions(batch)
                
                # Update results
                results['datasets_processed'] += batch_results['processed']
                results['datasets_updated'] += batch_results['updated']
                results['dimensions_computed'] += batch_results['dimensions_computed']
                results['errors'].extend(batch_results['errors'])
                
                logger.info(f"Processed batch {i//batch_size + 1}: {batch_results['processed']} datasets")
                
                # Small delay between batches
                await asyncio.sleep(1)
            
            results['end_time'] = datetime.now().isoformat()
            results['success'] = len(results['errors']) == 0
            
            logger.info(f"Monitoring complete: {results['datasets_updated']} updated, {results['dimensions_computed']} dimensions computed")
            return results
            
        except Exception as e:
            logger.error(f"Error in comprehensive monitoring: {e}")
            results['errors'].append(f"System error: {str(e)}")
            results['end_time'] = datetime.now().isoformat()
            return results
    
    async def fetch_all_datasets(self) -> List[Dict]:
        """Fetch all datasets from Data.gov API"""
        try:
            async with aiohttp.ClientSession() as session:
                datasets = []
                page = 1
                max_pages = 100  # Limit to prevent infinite loops
                
                while page <= max_pages:
                    url = f"https://catalog.data.gov/api/3/action/package_search?rows=1000&start={(page-1)*1000}"
                    
                    async with session.get(url, timeout=30) as response:
                        if response.status != 200:
                            logger.error(f"API request failed: {response.status}")
                            break
                        
                        data = await response.json()
                        results = data.get('result', {}).get('results', [])
                        
                        if not results:
                            break
                        
                        for result in results:
                            # Extract dataset information
                            dataset = {
                                'dataset_id': result.get('id', ''),
                                'title': result.get('title', ''),
                                'agency': self.extract_agency(result),
                                'url': self.extract_download_url(result),
                                'resource_format': self.extract_resource_format(result),
                                'last_modified': result.get('metadata_modified', ''),
                                'tags': [tag.get('name', '') for tag in result.get('tags', [])],
                                'organization': result.get('organization', {}).get('title', ''),
                                'notes': result.get('notes', '')
                            }
                            
                            if dataset['dataset_id'] and dataset['url']:
                                datasets.append(dataset)
                        
                        page += 1
                        await asyncio.sleep(0.1)  # Rate limiting
                
                logger.info(f"Fetched {len(datasets)} datasets from API")
                return datasets
                
        except Exception as e:
            logger.error(f"Error fetching datasets: {e}")
            return []
    
    def extract_agency(self, result: Dict) -> str:
        """Extract agency name from dataset result"""
        organization = result.get('organization', {})
        return organization.get('title', 'Unknown Agency')
    
    def extract_download_url(self, result: Dict) -> str:
        """Extract download URL from dataset result"""
        resources = result.get('resources', [])
        if not resources:
            return ''
        
        # Prefer CSV, JSON, or Excel files
        preferred_formats = ['CSV', 'JSON', 'XLSX', 'XLS', 'ZIP']
        
        for resource in resources:
            format_type = resource.get('format', '').upper()
            if format_type in preferred_formats:
                return resource.get('url', '')
        
        # Fallback to first resource
        return resources[0].get('url', '')
    
    def extract_resource_format(self, result: Dict) -> str:
        """Extract resource format from dataset result"""
        resources = result.get('resources', [])
        if not resources:
            return 'CSV'  # Default
        
        # Get format from first resource
        format_type = resources[0].get('format', '').upper()
        return format_type if format_type else 'CSV'
    
    async def process_batch_with_dimensions(self, datasets: List[Dict]) -> Dict:
        """Process a batch of datasets with guaranteed dimension computation"""
        results = {
            'processed': 0,
            'updated': 0,
            'dimensions_computed': 0,
            'errors': []
        }
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=self.max_workers)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            semaphore = asyncio.Semaphore(self.max_workers)
            
            tasks = []
            for dataset in datasets:
                task = self.process_single_dataset_with_dimensions(session, semaphore, dataset)
                tasks.append(task)
            
            # Wait for all tasks to complete
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(batch_results):
                results['processed'] += 1
                
                if isinstance(result, Exception):
                    results['errors'].append(f"Dataset {datasets[i]['dataset_id']}: {str(result)}")
                elif result and result.get('success', False):
                    results['updated'] += 1
                    if result.get('dimensions_computed', False):
                        results['dimensions_computed'] += 1
                else:
                    error_msg = result.get('error', 'Unknown error') if result else 'No result'
                    results['errors'].append(f"Dataset {datasets[i]['dataset_id']}: {error_msg}")
        
        return results
    
    async def process_single_dataset_with_dimensions(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, dataset: Dict) -> Dict:
        """Process a single dataset with guaranteed dimension computation"""
        async with semaphore:
            try:
                dataset_id = dataset['dataset_id']
                url = dataset['url']
                resource_format = dataset['resource_format']
                
                logger.debug(f"Processing {dataset_id}: {url}")
                
                # Check if we need to update this dataset
                needs_update = self.dataset_needs_update(dataset_id, url)
                if not needs_update:
                    return {'success': True, 'skipped': True}
                
                # Make request
                start_time = time.time()
                
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60), allow_redirects=True) as response:
                        response_time = int((time.time() - start_time) * 1000)
                        status_code = response.status
                        
                        # Get content and compute dimensions
                        content = await response.read() if status_code == 200 else b''
                        content_hash = hashlib.sha256(content).hexdigest() if content else None
                        file_size = len(content)
                        
                        # Compute dimensions
                        dimensions = self.compute_dimensions(content, resource_format)
                        
                        # Store dataset state
                        success = self.store_dataset_state(
                            dataset_id=dataset_id,
                            title=dataset['title'],
                            agency=dataset['agency'],
                            url=url,
                            status_code=status_code,
                            content_hash=content_hash,
                            file_size=file_size,
                            resource_format=resource_format,
                            dimensions=dimensions,
                            response_time=response_time
                        )
                        
                        if success:
                            # Log dimension computation
                            self.log_dimension_computation(dataset_id, dimensions, resource_format, response_time)
                            
                            return {
                                'success': True,
                                'dimensions_computed': dimensions.get('success', False),
                                'row_count': dimensions.get('row_count', 0),
                                'column_count': dimensions.get('column_count', 0)
                            }
                        else:
                            return {'success': False, 'error': 'Failed to store dataset state'}
                
                except asyncio.TimeoutError:
                    return {'success': False, 'error': 'Request timeout'}
                except Exception as e:
                    return {'success': False, 'error': f'Request error: {str(e)}'}
                
            except Exception as e:
                logger.error(f"Error processing {dataset['dataset_id']}: {e}")
                return {'success': False, 'error': str(e)}
    
    def dataset_needs_update(self, dataset_id: str, url: str) -> bool:
        """Check if dataset needs updating"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if we have recent data for this dataset
            cursor.execute('''
                SELECT created_at, url, dimensions_computed
                FROM dataset_states 
                WHERE dataset_id = ? 
                ORDER BY created_at DESC 
                LIMIT 1
            ''', (dataset_id,))
            
            result = cursor.fetchone()
            if not result:
                return True  # New dataset
            
            last_update, last_url, dimensions_computed = result
            last_update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            
            # Update if URL changed or if dimensions not computed
            if last_url != url or not dimensions_computed:
                return True
            
            # Update if older than 24 hours
            if datetime.now() - last_update_time > timedelta(hours=24):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if dataset needs update: {e}")
            return True
        finally:
            conn.close()
    
    def compute_dimensions(self, content: bytes, resource_format: str) -> Dict:
        """Compute row and column dimensions for content"""
        try:
            if not content:
                return {'success': False, 'error': 'No content', 'row_count': 0, 'column_count': 0}
            
            if resource_format.upper() in ['CSV', 'TXT', 'TSV']:
                return self.analyze_csv_dimensions(content)
            elif resource_format.upper() == 'JSON':
                return self.analyze_json_dimensions(content)
            elif resource_format.upper() == 'ZIP':
                return self.analyze_zip_dimensions(content)
            elif resource_format.upper() in ['XLS', 'XLSX']:
                return self.analyze_excel_dimensions(content)
            elif resource_format.upper() == 'XML':
                return self.analyze_xml_dimensions(content)
            else:
                return self.analyze_unknown_dimensions(content)
        
        except Exception as e:
            logger.debug(f"Error computing dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_csv_dimensions(self, content: bytes) -> Dict:
        """Analyze CSV content for dimensions"""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            lines = [line for line in text_content.split('\n') if line.strip()]
            
            if not lines:
                return {'success': True, 'row_count': 0, 'column_count': 0}
            
            # Count rows (subtract header)
            row_count = max(0, len(lines) - 1)
            
            # Parse CSV to get column info
            try:
                df = pd.read_csv(io.StringIO(text_content))
                column_count = len(df.columns)
                
                schema_info = {
                    'columns': list(df.columns),
                    'dtypes': {str(k): str(v) for k, v in df.dtypes.to_dict().items()},
                    'sample_data': df.head(3).to_dict('records')
                }
            except:
                # Fallback: basic column counting
                column_count = len(lines[0].split(',')) if lines else 0
                schema_info = {'columns': lines[0].split(',') if lines else []}
            
            return {
                'success': True,
                'row_count': row_count,
                'column_count': column_count,
                'schema_info': schema_info
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing CSV dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_json_dimensions(self, content: bytes) -> Dict:
        """Analyze JSON content for dimensions"""
        try:
            json_data = json.loads(content.decode('utf-8', errors='ignore'))
            
            if isinstance(json_data, list):
                row_count = len(json_data)
                column_count = len(json_data[0].keys()) if json_data and isinstance(json_data[0], dict) else 0
                
                schema_info = {
                    'sample_data': json_data[:3],
                    'structure': 'array',
                    'total_items': len(json_data)
                }
            elif isinstance(json_data, dict):
                row_count = 1
                column_count = len(json_data.keys())
                
                schema_info = {
                    'sample_data': json_data,
                    'structure': 'object',
                    'keys': list(json_data.keys())
                }
            else:
                row_count = 1
                column_count = 0
                schema_info = {
                    'structure': 'primitive',
                    'type': type(json_data).__name__
                }
            
            return {
                'success': True,
                'row_count': row_count,
                'column_count': column_count,
                'schema_info': schema_info
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing JSON dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_zip_dimensions(self, content: bytes) -> Dict:
        """Analyze ZIP content for dimensions"""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                # Find data files in the ZIP
                csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                json_files = [f for f in zip_file.namelist() if f.lower().endswith('.json')]
                
                total_rows = 0
                total_columns = 0
                schema_info = {'zip_files': zip_file.namelist()}
                
                # Analyze CSV files
                if csv_files:
                    try:
                        with zip_file.open(csv_files[0]) as csv_file:
                            csv_content = csv_file.read().decode('utf-8', errors='ignore')
                            lines = [line for line in csv_content.split('\n') if line.strip()]
                            total_rows += max(0, len(lines) - 1)
                            
                            if lines:
                                df = pd.read_csv(io.StringIO(csv_content))
                                total_columns = max(total_columns, len(df.columns))
                                schema_info['csv_columns'] = list(df.columns)
                    except Exception as e:
                        logger.debug(f"Error analyzing CSV in ZIP: {e}")
                
                # Analyze JSON files
                if json_files:
                    try:
                        with zip_file.open(json_files[0]) as json_file:
                            json_content = json_file.read().decode('utf-8', errors='ignore')
                            json_data = json.loads(json_content)
                            
                            if isinstance(json_data, list):
                                total_rows += len(json_data)
                                if json_data and isinstance(json_data[0], dict):
                                    total_columns = max(total_columns, len(json_data[0].keys()))
                            elif isinstance(json_data, dict):
                                total_rows += 1
                                total_columns = max(total_columns, len(json_data.keys()))
                    except Exception as e:
                        logger.debug(f"Error analyzing JSON in ZIP: {e}")
                
                # If no data files found, count total files
                if not csv_files and not json_files:
                    total_rows = len(zip_file.namelist())
                    total_columns = 0
                
                return {
                    'success': True,
                    'row_count': total_rows,
                    'column_count': total_columns,
                    'schema_info': schema_info
                }
                
        except Exception as e:
            logger.debug(f"Error analyzing ZIP dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_excel_dimensions(self, content: bytes) -> Dict:
        """Analyze Excel content for dimensions"""
        try:
            df = pd.read_excel(io.BytesIO(content))
            
            return {
                'success': True,
                'row_count': len(df),
                'column_count': len(df.columns),
                'schema_info': {
                    'columns': list(df.columns),
                    'dtypes': {str(k): str(v) for k, v in df.dtypes.to_dict().items()},
                    'sample_data': df.head(3).to_dict('records')
                }
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing Excel dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_xml_dimensions(self, content: bytes) -> Dict:
        """Analyze XML content for dimensions"""
        try:
            root = ET.fromstring(content.decode('utf-8', errors='ignore'))
            
            # Count XML elements (rough estimate of records)
            row_count = len(list(root.iter()))
            column_count = len(root.attrib) if hasattr(root, 'attrib') else 0
            
            return {
                'success': True,
                'row_count': row_count,
                'column_count': column_count,
                'schema_info': {
                    'root_tag': root.tag,
                    'attributes': list(root.attrib.keys()) if hasattr(root, 'attrib') else [],
                    'structure': 'xml'
                }
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing XML dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def analyze_unknown_dimensions(self, content: bytes) -> Dict:
        """Analyze content with unknown format for dimensions"""
        try:
            # Try to detect if it's text-based
            text_content = content.decode('utf-8', errors='ignore')
            
            # Check if it looks like CSV
            lines = text_content.split('\n')
            if len(lines) > 1 and ',' in lines[0]:
                return self.analyze_csv_dimensions(content)
            
            # Check if it looks like JSON
            try:
                json_data = json.loads(text_content)
                return self.analyze_json_dimensions(content)
            except:
                pass
            
            # Default: treat as single record
            return {
                'success': True,
                'row_count': 1,
                'column_count': 0,
                'schema_info': {
                    'structure': 'unknown',
                    'content_preview': text_content[:200]
                }
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing unknown dimensions: {e}")
            return {'success': False, 'error': str(e), 'row_count': 0, 'column_count': 0}
    
    def store_dataset_state(self, dataset_id: str, title: str, agency: str, url: str, 
                          status_code: int, content_hash: str, file_size: int, 
                          resource_format: str, dimensions: Dict, response_time: int) -> bool:
        """Store dataset state with dimensions"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Determine availability
            if status_code == 200:
                availability = 'available'
            elif 400 <= status_code < 500:
                availability = 'unavailable'
            else:
                availability = 'partially_available'
            
            # Extract dimension data
            row_count = dimensions.get('row_count', 0)
            column_count = dimensions.get('column_count', 0)
            schema_info = dimensions.get('schema_info', {})
            dimensions_computed = dimensions.get('success', False)
            
            # Store in dataset_states table
            cursor.execute('''
                INSERT OR REPLACE INTO dataset_states 
                (dataset_id, snapshot_date, title, agency, url, status_code, content_hash, 
                 file_size, content_type, resource_format, row_count, column_count, schema,
                 last_modified, availability, dimensions_computed, dimension_computation_date,
                 dimension_computation_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset_id,
                datetime.now().strftime('%Y-%m-%d'),
                title,
                agency,
                url,
                status_code,
                content_hash,
                file_size,
                'application/octet-stream',  # Default content type
                resource_format,
                row_count,
                column_count,
                json.dumps(schema_info),
                datetime.now().isoformat(),
                availability,
                dimensions_computed,
                datetime.now().isoformat() if dimensions_computed else None,
                dimensions.get('error') if not dimensions_computed else None,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error storing dataset state: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def log_dimension_computation(self, dataset_id: str, dimensions: Dict, resource_format: str, response_time: int):
        """Log dimension computation results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO dimension_computation_log
                (dataset_id, computation_date, success, row_count, column_count, 
                 file_size, error_message, computation_time_ms, resource_format)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset_id,
                datetime.now().isoformat(),
                dimensions.get('success', False),
                dimensions.get('row_count', 0),
                dimensions.get('column_count', 0),
                0,  # File size not available here
                dimensions.get('error'),
                response_time,
                resource_format
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error logging dimension computation: {e}")
        finally:
            conn.close()
    
    def get_dimension_statistics(self) -> Dict:
        """Get comprehensive dimension statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get total datasets
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
            total_datasets = cursor.fetchone()[0] or 0
            
            # Get datasets with computed dimensions
            cursor.execute('''
                SELECT COUNT(DISTINCT dataset_id) 
                FROM dataset_states 
                WHERE dimensions_computed = TRUE
            ''')
            datasets_with_dimensions = cursor.fetchone()[0] or 0
            
            # Get datasets missing dimensions
            cursor.execute('''
                SELECT COUNT(DISTINCT dataset_id) 
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.dimensions_computed = FALSE
                AND ds.availability = 'available'
            ''')
            datasets_missing_dimensions = cursor.fetchone()[0] or 0
            
            # Get average dimensions
            cursor.execute('''
                SELECT AVG(row_count), AVG(column_count) 
                FROM dataset_states 
                WHERE dimensions_computed = TRUE AND row_count > 0 AND column_count > 0
            ''')
            avg_result = cursor.fetchone()
            avg_rows = avg_result[0] if avg_result[0] else 0
            avg_columns = avg_result[1] if avg_result[1] else 0
            
            # Get recent computation activity
            cursor.execute('''
                SELECT COUNT(*) 
                FROM dimension_computation_log 
                WHERE computation_date >= datetime('now', '-24 hours')
            ''')
            recent_computations = cursor.fetchone()[0] or 0
            
            return {
                'total_datasets': total_datasets,
                'datasets_with_dimensions': datasets_with_dimensions,
                'datasets_missing_dimensions': datasets_missing_dimensions,
                'completion_percentage': (datasets_with_dimensions / total_datasets * 100) if total_datasets > 0 else 0,
                'average_rows': round(avg_rows, 2),
                'average_columns': round(avg_columns, 2),
                'recent_computations_24h': recent_computations
            }
            
        except Exception as e:
            logger.error(f"Error getting dimension statistics: {e}")
            return {}
        finally:
            conn.close()

async def main():
    """Main function for testing"""
    monitor = EnhancedConcordanceMonitorWithDimensions()
    
    # Get current statistics
    stats = monitor.get_dimension_statistics()
    print("Current dimension statistics:")
    print(json.dumps(stats, indent=2))
    
    # Run monitoring
    results = await monitor.monitor_all_datasets()
    print("\nMonitoring results:")
    print(json.dumps(results, indent=2))

if __name__ == '__main__':
    asyncio.run(main())


