"""
Enhanced Row/Column Computer for Dataset State Historian
Ensures all datasets have accurate row and column counts computed and stored
"""

import asyncio
import aiohttp
import sqlite3
import json
import logging
import hashlib
import io
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
import ssl

logger = logging.getLogger(__name__)

class EnhancedRowColumnComputer:
    """Enhanced system for computing and ensuring row/column counts for all datasets"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.max_workers = 5  # Reduced to prevent database locks
        self.timeout = 60
        self.max_file_size = 100 * 1024 * 1024  # 100MB limit
        
    async def ensure_all_datasets_have_dimensions(self, force_recompute: bool = False) -> Dict:
        """Ensure all datasets have row/column counts computed"""
        logger.info("Starting comprehensive row/column computation for all datasets")
        
        results = {
            'start_time': datetime.now().isoformat(),
            'datasets_processed': 0,
            'datasets_updated': 0,
            'datasets_failed': 0,
            'datasets_skipped': 0,
            'errors': []
        }
        
        try:
            # Get all datasets that need processing
            datasets_to_process = self.get_datasets_needing_dimensions(force_recompute)
            logger.info(f"Found {len(datasets_to_process)} datasets to process")
            
            if not datasets_to_process:
                logger.info("All datasets already have dimension data")
                return results
            
            # Process datasets in batches
            batch_size = 50
            for i in range(0, len(datasets_to_process), batch_size):
                batch = datasets_to_process[i:i + batch_size]
                batch_results = await self.process_batch(batch)
                
                # Update results
                results['datasets_processed'] += batch_results['processed']
                results['datasets_updated'] += batch_results['updated']
                results['datasets_failed'] += batch_results['failed']
                results['datasets_skipped'] += batch_results['skipped']
                results['errors'].extend(batch_results['errors'])
                
                logger.info(f"Processed batch {i//batch_size + 1}: {batch_results['processed']} datasets")
                
                # Small delay between batches to avoid overwhelming servers
                await asyncio.sleep(1)
            
            results['end_time'] = datetime.now().isoformat()
            results['success'] = results['datasets_failed'] == 0
            
            logger.info(f"Row/column computation complete: {results['datasets_updated']} updated, {results['datasets_failed']} failed")
            return results
            
        except Exception as e:
            logger.error(f"Error in comprehensive dimension computation: {e}")
            results['errors'].append(f"System error: {str(e)}")
            results['end_time'] = datetime.now().isoformat()
            return results
    
    def get_datasets_needing_dimensions(self, force_recompute: bool = False) -> List[Dict]:
        """Get datasets that need row/column computation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if force_recompute:
            # Get all available datasets
            query = '''
                SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url, ds.resource_format, ds.availability
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.url IS NOT NULL 
                AND ds.url != ''
                AND ds.availability = 'available'
                ORDER BY ds.created_at DESC
            '''
        else:
            # Get datasets missing dimension data
            query = '''
                SELECT DISTINCT ds.dataset_id, ds.title, ds.agency, ds.url, ds.resource_format, ds.availability
                FROM dataset_states ds
                INNER JOIN (
                    SELECT dataset_id, MAX(created_at) as max_created
                    FROM dataset_states 
                    GROUP BY dataset_id
                ) latest ON ds.dataset_id = latest.dataset_id 
                AND ds.created_at = latest.max_created
                WHERE ds.url IS NOT NULL 
                AND ds.url != ''
                AND ds.availability = 'available'
                AND (ds.row_count IS NULL OR ds.row_count = 0 OR ds.column_count IS NULL OR ds.column_count = 0)
                ORDER BY ds.created_at DESC
            '''
        
        cursor.execute(query)
        datasets = []
        
        for row in cursor.fetchall():
            datasets.append({
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'url': row[3],
                'resource_format': row[4] or 'CSV',
                'availability': row[5]
            })
        
        conn.close()
        return datasets
    
    async def process_batch(self, datasets: List[Dict]) -> Dict:
        """Process a batch of datasets"""
        results = {
            'processed': 0,
            'updated': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        # Create SSL context for HTTPS requests
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=self.max_workers)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.max_workers)
            
            tasks = []
            for dataset in datasets:
                task = self.process_single_dataset(session, semaphore, dataset)
                tasks.append(task)
            
            # Wait for all tasks to complete
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(batch_results):
                results['processed'] += 1
                
                if isinstance(result, Exception):
                    results['failed'] += 1
                    results['errors'].append(f"Dataset {datasets[i]['dataset_id']}: {str(result)}")
                elif result is None:
                    results['skipped'] += 1
                elif result.get('success', False):
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Dataset {datasets[i]['dataset_id']}: {result.get('error', 'Unknown error')}")
        
        return results
    
    async def process_single_dataset(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, dataset: Dict) -> Optional[Dict]:
        """Process a single dataset to compute dimensions"""
        async with semaphore:
            try:
                dataset_id = dataset['dataset_id']
                url = dataset['url']
                resource_format = dataset['resource_format']
                
                logger.debug(f"Processing {dataset_id}: {url}")
                
                # Skip if URL is not accessible
                if not url or url.startswith('mailto:') or url.startswith('tel:'):
                    return {'success': False, 'error': 'Invalid URL format'}
                
                # Make request with timeout
                start_time = datetime.now()
                
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=self.timeout), allow_redirects=True) as response:
                        if response.status != 200:
                            return {'success': False, 'error': f'HTTP {response.status}'}
                        
                        # Check content length
                        content_length = response.headers.get('content-length')
                        if content_length and int(content_length) > self.max_file_size:
                            return {'success': False, 'error': f'File too large: {content_length} bytes'}
                        
                        # Read content
                        content = await response.read()
                        
                        if len(content) > self.max_file_size:
                            return {'success': False, 'error': f'File too large: {len(content)} bytes'}
                        
                        # Analyze content
                        analysis_result = self.analyze_content(content, resource_format)
                        
                        if analysis_result:
                            # Update database
                            success = await self.update_dataset_dimensions(dataset_id, analysis_result)
                            
                            if success:
                                logger.debug(f"✓ {dataset_id}: {analysis_result.get('row_count', 0)} rows, {analysis_result.get('column_count', 0)} columns")
                                return {'success': True, 'result': analysis_result}
                            else:
                                return {'success': False, 'error': 'Failed to update database'}
                        else:
                            return {'success': False, 'error': 'Could not analyze content'}
                
                except asyncio.TimeoutError:
                    return {'success': False, 'error': 'Request timeout'}
                except Exception as e:
                    return {'success': False, 'error': f'Request error: {str(e)}'}
                
            except Exception as e:
                logger.error(f"Error processing {dataset['dataset_id']}: {e}")
                return {'success': False, 'error': str(e)}
    
    def analyze_content(self, content: bytes, resource_format: str) -> Optional[Dict]:
        """Analyze content to extract row and column counts"""
        try:
            if resource_format.upper() in ['CSV', 'TXT', 'TSV']:
                return self.analyze_csv_content(content)
            elif resource_format.upper() == 'JSON':
                return self.analyze_json_content(content)
            elif resource_format.upper() == 'ZIP':
                return self.analyze_zip_content(content)
            elif resource_format.upper() in ['XLS', 'XLSX']:
                return self.analyze_excel_content(content)
            elif resource_format.upper() == 'XML':
                return self.analyze_xml_content(content)
            else:
                # Try to detect format from content
                return self.analyze_unknown_content(content)
        
        except Exception as e:
            logger.debug(f"Error analyzing content: {e}")
            return None
    
    def analyze_csv_content(self, content: bytes) -> Optional[Dict]:
        """Analyze CSV content"""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            lines = [line for line in text_content.split('\n') if line.strip()]
            
            if not lines:
                return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
            
            # Count rows (subtract header)
            row_count = max(0, len(lines) - 1)
            
            # Parse CSV to get column info
            try:
                df = pd.read_csv(io.StringIO(text_content), low_memory=False)
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
                'row_count': row_count,
                'column_count': column_count,
                'file_size': len(content),
                'content_hash': hashlib.sha256(content).hexdigest(),
                'schema_info': schema_info,
                'analyzed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing CSV: {e}")
            return None
    
    def analyze_json_content(self, content: bytes) -> Optional[Dict]:
        """Analyze JSON content"""
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
                'row_count': row_count,
                'column_count': column_count,
                'file_size': len(content),
                'content_hash': hashlib.sha256(content).hexdigest(),
                'schema_info': schema_info,
                'analyzed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing JSON: {e}")
            return None
    
    def analyze_zip_content(self, content: bytes) -> Optional[Dict]:
        """Analyze ZIP content"""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                # Find data files in the ZIP
                csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                json_files = [f for f in zip_file.namelist() if f.lower().endswith('.json')]
                xlsx_files = [f for f in zip_file.namelist() if f.lower().endswith(('.xlsx', '.xls'))]
                
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
                if not csv_files and not json_files and not xlsx_files:
                    total_rows = len(zip_file.namelist())
                    total_columns = 0
                
                return {
                    'row_count': total_rows,
                    'column_count': total_columns,
                    'file_size': len(content),
                    'content_hash': hashlib.sha256(content).hexdigest(),
                    'schema_info': schema_info,
                    'analyzed_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.debug(f"Error analyzing ZIP: {e}")
            return None
    
    def analyze_excel_content(self, content: bytes) -> Optional[Dict]:
        """Analyze Excel content"""
        try:
            df = pd.read_excel(io.BytesIO(content))
            
            return {
                'row_count': len(df),
                'column_count': len(df.columns),
                'file_size': len(content),
                'content_hash': hashlib.sha256(content).hexdigest(),
                'schema_info': {
                    'columns': list(df.columns),
                    'dtypes': {str(k): str(v) for k, v in df.dtypes.to_dict().items()},
                    'sample_data': df.head(3).to_dict('records')
                },
                'analyzed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing Excel: {e}")
            return None
    
    def analyze_xml_content(self, content: bytes) -> Optional[Dict]:
        """Analyze XML content"""
        try:
            root = ET.fromstring(content.decode('utf-8', errors='ignore'))
            
            # Count XML elements (rough estimate of records)
            row_count = len(list(root.iter()))
            column_count = len(root.attrib) if hasattr(root, 'attrib') else 0
            
            return {
                'row_count': row_count,
                'column_count': column_count,
                'file_size': len(content),
                'content_hash': hashlib.sha256(content).hexdigest(),
                'schema_info': {
                    'root_tag': root.tag,
                    'attributes': list(root.attrib.keys()) if hasattr(root, 'attrib') else [],
                    'structure': 'xml'
                },
                'analyzed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing XML: {e}")
            return None
    
    def analyze_unknown_content(self, content: bytes) -> Optional[Dict]:
        """Analyze content with unknown format"""
        try:
            # Try to detect if it's text-based
            text_content = content.decode('utf-8', errors='ignore')
            
            # Check if it looks like CSV
            lines = text_content.split('\n')
            if len(lines) > 1 and ',' in lines[0]:
                return self.analyze_csv_content(content)
            
            # Check if it looks like JSON
            try:
                json_data = json.loads(text_content)
                return self.analyze_json_content(content)
            except:
                pass
            
            # Default: treat as single record
            return {
                'row_count': 1,
                'column_count': 0,
                'file_size': len(content),
                'content_hash': hashlib.sha256(content).hexdigest(),
                'schema_info': {
                    'structure': 'unknown',
                    'content_preview': text_content[:200]
                },
                'analyzed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error analyzing unknown content: {e}")
            return None
    
    async def update_dataset_dimensions(self, dataset_id: str, analysis_result: Dict) -> bool:
        """Update dataset with computed dimensions"""
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                cursor = conn.cursor()
                
                # Update the latest snapshot for this dataset
                cursor.execute('''
                    UPDATE dataset_states 
                    SET row_count = ?, column_count = ?, file_size = ?, 
                        content_hash = ?, schema = ?
                    WHERE dataset_id = ? AND created_at = (
                        SELECT MAX(created_at) FROM dataset_states WHERE dataset_id = ?
                    )
                ''', (
                    analysis_result.get('row_count', 0),
                    analysis_result.get('column_count', 0),
                    analysis_result.get('file_size', 0),
                    analysis_result.get('content_hash', ''),
                    json.dumps(analysis_result.get('schema_info', {})),
                    dataset_id,
                    dataset_id
                ))
                
                conn.commit()
                return True
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Database locked for {dataset_id}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    if conn:
                        conn.close()
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    logger.error(f"Error updating {dataset_id}: {e}")
                    if conn:
                        conn.rollback()
                    return False
            except Exception as e:
                logger.error(f"Error updating {dataset_id}: {e}")
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    conn.close()
        
        return False
    
    def get_dimension_statistics(self) -> Dict:
        """Get statistics about dimension computation"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        try:
            # Get total datasets
            cursor.execute('SELECT COUNT(DISTINCT dataset_id) FROM dataset_states')
            total_datasets = cursor.fetchone()[0] or 0
            
            # Get datasets with dimensions
            cursor.execute('''
                SELECT COUNT(DISTINCT dataset_id) 
                FROM dataset_states 
                WHERE row_count IS NOT NULL AND row_count > 0 
                AND column_count IS NOT NULL AND column_count > 0
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
                WHERE (ds.row_count IS NULL OR ds.row_count = 0 OR ds.column_count IS NULL OR ds.column_count = 0)
                AND ds.availability = 'available'
            ''')
            datasets_missing_dimensions = cursor.fetchone()[0] or 0
            
            # Get average dimensions
            cursor.execute('''
                SELECT AVG(row_count), AVG(column_count) 
                FROM dataset_states 
                WHERE row_count > 0 AND column_count > 0
            ''')
            avg_result = cursor.fetchone()
            avg_rows = avg_result[0] if avg_result[0] else 0
            avg_columns = avg_result[1] if avg_result[1] else 0
            
            return {
                'total_datasets': total_datasets,
                'datasets_with_dimensions': datasets_with_dimensions,
                'datasets_missing_dimensions': datasets_missing_dimensions,
                'completion_percentage': (datasets_with_dimensions / total_datasets * 100) if total_datasets > 0 else 0,
                'average_rows': round(avg_rows, 2),
                'average_columns': round(avg_columns, 2)
            }
            
        except Exception as e:
            logger.error(f"Error getting dimension statistics: {e}")
            return {}
        finally:
            conn.close()

async def main():
    """Main function for testing"""
    computer = EnhancedRowColumnComputer()
    
    # Get current statistics
    stats = computer.get_dimension_statistics()
    print("Current dimension statistics:")
    print(json.dumps(stats, indent=2))
    
    # Run comprehensive computation
    results = await computer.ensure_all_datasets_have_dimensions(force_recompute=False)
    print("\nComputation results:")
    print(json.dumps(results, indent=2))

if __name__ == '__main__':
    asyncio.run(main())


