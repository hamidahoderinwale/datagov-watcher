#!/usr/bin/env python3
"""
Dataset Content Analyzer
Analyzes dataset content to populate missing row_count and column_count data
"""

import asyncio
import aiohttp
import sqlite3
import json
import hashlib
import time
import io
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class DatasetContentAnalyzer:
    def __init__(self, db_path: str = "datasets.db", max_workers: int = 10):
        self.db_path = db_path
        self.max_workers = max_workers
        self.session = None
        
    async def analyze_datasets(self, limit: int = 1000):
        """Analyze datasets to populate row and column counts"""
        print(f"Starting analysis of up to {limit} datasets...")
        
        # Get datasets that need analysis
        datasets = self.get_datasets_to_analyze(limit)
        print(f"Found {len(datasets)} datasets to analyze")
        
        if not datasets:
            print("No datasets found that need analysis")
            return
        
        # Create HTTP session with SSL context that doesn't verify certificates
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(
            limit=self.max_workers, 
            limit_per_host=5,
            ssl=ssl_context
        )
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers={'User-Agent': 'Dataset-Monitor/1.0'}
        ) as session:
            self.session = session
            
            # Process datasets in batches
            batch_size = 50
            for i in range(0, len(datasets), batch_size):
                batch = datasets[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1}/{(len(datasets)-1)//batch_size + 1}")
                
                tasks = [self.analyze_single_dataset(dataset) for dataset in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Store results
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Error analyzing {batch[j]['dataset_id']}: {result}")
                    else:
                        self.store_analysis_result(batch[j]['dataset_id'], result)
                
                # Small delay between batches
                await asyncio.sleep(1)
        
        print("Analysis complete!")
    
    def get_datasets_to_analyze(self, limit: int) -> List[Dict]:
        """Get datasets that need row/column analysis"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get datasets with URLs but missing row/column data
        cursor.execute('''
            SELECT dataset_id, title, agency, url, resource_format
            FROM dataset_states 
            WHERE url IS NOT NULL 
            AND url != ''
            AND (row_count IS NULL OR row_count = 0 OR column_count IS NULL OR column_count = 0)
            AND availability = 'available'
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))
        
        datasets = []
        for row in cursor.fetchall():
            datasets.append({
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'url': row[3],
                'resource_format': row[4] or 'CSV'
            })
        
        conn.close()
        return datasets
    
    async def analyze_single_dataset(self, dataset: Dict) -> Dict:
        """Analyze a single dataset to get row and column counts"""
        dataset_id = dataset['dataset_id']
        url = dataset['url']
        resource_format = dataset['resource_format']
        
        try:
            # Make request with timeout
            async with self.session.get(url) as response:
                if response.status != 200:
                    return {'row_count': 0, 'column_count': 0, 'error': f'HTTP {response.status}'}
                
                content = await response.read()
                content_hash = hashlib.sha256(content).hexdigest()
                file_size = len(content)
                
                row_count = 0
                column_count = 0
                schema_info = {}
                
                # Analyze content based on format
                if resource_format.upper() in ['CSV', 'TXT', 'TSV']:
                    try:
                        text_content = content.decode('utf-8', errors='ignore')
                        
                        # Count rows (subtract header)
                        all_lines = [line for line in text_content.split('\n') if line.strip()]
                        row_count = max(0, len(all_lines) - 1) if all_lines else 0
                        
                        # Parse CSV to get column info
                        if all_lines:
                            df = pd.read_csv(io.StringIO(text_content))
                            column_count = len(df.columns)
                            
                            schema_info = {
                                'columns': list(df.columns),
                                'dtypes': {str(k): str(v) for k, v in df.dtypes.to_dict().items()},
                                'sample_data': df.head(3).to_dict('records')
                            }
                        else:
                            column_count = 0
                            
                    except Exception as e:
                        logger.debug(f"Could not parse CSV for {dataset_id}: {e}")
                        # Fallback: basic line counting
                        all_lines = [line for line in text_content.split('\n') if line.strip()]
                        row_count = max(0, len(all_lines) - 1) if all_lines else 0
                        column_count = len(all_lines[0].split(',')) if all_lines else 0
                
                elif resource_format.upper() == 'JSON':
                    try:
                        json_data = json.loads(content.decode('utf-8', errors='ignore'))
                        
                        if isinstance(json_data, list):
                            row_count = len(json_data)
                            if json_data and isinstance(json_data[0], dict):
                                column_count = len(json_data[0].keys())
                                schema_info = {
                                    'sample_data': json_data[:3],
                                    'structure': 'array'
                                }
                        elif isinstance(json_data, dict):
                            row_count = 1
                            column_count = len(json_data.keys())
                            schema_info = {
                                'sample_data': [json_data],
                                'structure': 'object'
                            }
                            
                    except Exception as e:
                        logger.debug(f"Could not parse JSON for {dataset_id}: {e}")
                
                elif resource_format.upper() in ['XLSX', 'XLS']:
                    try:
                        # For Excel files, we'd need openpyxl or xlrd
                        # For now, just set basic counts
                        row_count = 0
                        column_count = 0
                        logger.debug(f"Excel format not fully supported for {dataset_id}")
                    except Exception as e:
                        logger.debug(f"Could not parse Excel for {dataset_id}: {e}")
                
                return {
                    'row_count': row_count,
                    'column_count': column_count,
                    'file_size': file_size,
                    'content_hash': content_hash,
                    'schema_info': schema_info,
                    'analyzed_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error analyzing {dataset_id}: {e}")
            return {'row_count': 0, 'column_count': 0, 'error': str(e)}
    
    def store_analysis_result(self, dataset_id: str, result: Dict):
        """Store analysis results in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Update the dataset_states table (without analyzed_at column)
            cursor.execute('''
                UPDATE dataset_states 
                SET row_count = ?, column_count = ?, file_size = ?, 
                    content_hash = ?, schema = ?
                WHERE dataset_id = ? AND created_at = (
                    SELECT MAX(created_at) FROM dataset_states WHERE dataset_id = ?
                )
            ''', (
                result.get('row_count', 0),
                result.get('column_count', 0),
                result.get('file_size', 0),
                result.get('content_hash', ''),
                json.dumps(result.get('schema_info', {})),
                dataset_id,
                dataset_id
            ))
            
            conn.commit()
            
            if result.get('row_count', 0) > 0 or result.get('column_count', 0) > 0:
                print(f"✓ {dataset_id}: {result.get('row_count', 0)} rows, {result.get('column_count', 0)} columns")
            
        except Exception as e:
            logger.error(f"Error storing results for {dataset_id}: {e}")
            conn.rollback()
        finally:
            conn.close()

async def main():
    """Main function"""
    analyzer = DatasetContentAnalyzer()
    
    # Analyze first 1000 datasets
    await analyzer.analyze_datasets(limit=1000)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
