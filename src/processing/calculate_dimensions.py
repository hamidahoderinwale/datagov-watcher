#!/usr/bin/env python3
"""
Calculate Dataset Dimensions
Analyzes actual dataset content to calculate row and column counts
"""

import asyncio
import aiohttp
import sqlite3
import json
import hashlib
import time
import io
import pandas as pd
import zipfile
import ssl
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class DatasetDimensionCalculator:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        
    async def calculate_dimensions(self, limit: int = 100):
        """Calculate dimensions for datasets with URLs"""
        print(f"Calculating dimensions for up to {limit} datasets...")
        
        # Get datasets with URLs
        datasets = self.get_datasets_with_urls(limit)
        print(f"Found {len(datasets)} datasets with URLs")
        
        if not datasets:
            print("No datasets found with URLs")
            return
        
        # Create HTTP session with SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=2, ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers={'User-Agent': 'Dataset-Monitor/1.0'}
        ) as session:
            
            # Process datasets one by one to avoid overwhelming servers
            for i, dataset in enumerate(datasets):
                print(f"Processing {i+1}/{len(datasets)}: {dataset['title'][:50]}...")
                
                try:
                    result = await self.analyze_dataset_content(session, dataset)
                    if result:
                        self.update_dataset_dimensions(dataset['dataset_id'], result)
                        print(f"✓ {dataset['dataset_id'][:8]}...: {result.get('row_count', 0)} rows, {result.get('column_count', 0)} columns")
                    else:
                        print(f"✗ {dataset['dataset_id'][:8]}...: Could not analyze")
                        
                except Exception as e:
                    print(f"✗ {dataset['dataset_id'][:8]}...: Error - {str(e)[:50]}")
                
                # Small delay between requests
                await asyncio.sleep(2)
        
        print("Dimension calculation complete!")
    
    def get_datasets_with_urls(self, limit: int) -> List[Dict]:
        """Get datasets that have URLs and need dimension calculation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, title, url, resource_format
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
                'url': row[2],
                'resource_format': row[3] or 'CSV'
            })
        
        conn.close()
        return datasets
    
    async def analyze_dataset_content(self, session: aiohttp.ClientSession, dataset: Dict) -> Optional[Dict]:
        """Analyze dataset content to get dimensions"""
        url = dataset['url']
        resource_format = dataset['resource_format']
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                
                content = await response.read()
                content_hash = hashlib.sha256(content).hexdigest()
                file_size = len(content)
                
                row_count = 0
                column_count = 0
                schema_info = {}
                
                # Analyze based on format
                if resource_format.upper() in ['CSV', 'TXT', 'TSV']:
                    result = self.analyze_csv_content(content)
                    row_count = result['row_count']
                    column_count = result['column_count']
                    schema_info = result['schema_info']
                    
                elif resource_format.upper() == 'JSON':
                    result = self.analyze_json_content(content)
                    row_count = result['row_count']
                    column_count = result['column_count']
                    schema_info = result['schema_info']
                    
                elif resource_format.upper() == 'ZIP':
                    result = self.analyze_zip_content(content)
                    row_count = result['row_count']
                    column_count = result['column_count']
                    schema_info = result['schema_info']
                    
                elif resource_format.upper() in ['XLSX', 'XLS']:
                    # For Excel files, we'd need openpyxl
                    result = self.analyze_excel_content(content)
                    row_count = result['row_count']
                    column_count = result['column_count']
                    schema_info = result['schema_info']
                
                return {
                    'row_count': row_count,
                    'column_count': column_count,
                    'file_size': file_size,
                    'content_hash': content_hash,
                    'schema_info': schema_info,
                    'analyzed_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.debug(f"Error analyzing {dataset['dataset_id']}: {e}")
            return None
    
    def analyze_csv_content(self, content: bytes) -> Dict:
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
            
            return {'row_count': row_count, 'column_count': column_count, 'schema_info': schema_info}
            
        except Exception as e:
            logger.debug(f"Error analyzing CSV: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def analyze_json_content(self, content: bytes) -> Dict:
        """Analyze JSON content"""
        try:
            json_data = json.loads(content.decode('utf-8', errors='ignore'))
            
            if isinstance(json_data, list):
                row_count = len(json_data)
                if json_data and isinstance(json_data[0], dict):
                    column_count = len(json_data[0].keys())
                    schema_info = {
                        'sample_data': json_data[:3],
                        'structure': 'array',
                        'columns': list(json_data[0].keys()) if json_data else []
                    }
                else:
                    column_count = 0
                    schema_info = {'structure': 'array'}
            elif isinstance(json_data, dict):
                row_count = 1
                column_count = len(json_data.keys())
                schema_info = {
                    'sample_data': [json_data],
                    'structure': 'object',
                    'columns': list(json_data.keys())
                }
            else:
                row_count = 0
                column_count = 0
                schema_info = {'structure': 'primitive'}
            
            return {'row_count': row_count, 'column_count': column_count, 'schema_info': schema_info}
            
        except Exception as e:
            logger.debug(f"Error analyzing JSON: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def analyze_zip_content(self, content: bytes) -> Dict:
        """Analyze ZIP content"""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                # Look for CSV files in the ZIP
                csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                
                if csv_files:
                    # Analyze the first CSV file
                    with zip_file.open(csv_files[0]) as csv_file:
                        csv_content = csv_file.read()
                        return self.analyze_csv_content(csv_content)
                else:
                    return {'row_count': 0, 'column_count': 0, 'schema_info': {'files': zip_file.namelist()}}
                    
        except Exception as e:
            logger.debug(f"Error analyzing ZIP: {e}")
            return {'row_count': 0, 'column_count': 0, 'schema_info': {}}
    
    def analyze_excel_content(self, content: bytes) -> Dict:
        """Analyze Excel content (basic implementation)"""
        # This would require openpyxl or xlrd
        # For now, return basic info
        return {'row_count': 0, 'column_count': 0, 'schema_info': {'note': 'Excel analysis not implemented'}}
    
    def update_dataset_dimensions(self, dataset_id: str, result: Dict):
        """Update dataset with calculated dimensions"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
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
            
        except Exception as e:
            logger.error(f"Error updating {dataset_id}: {e}")
            conn.rollback()
        finally:
            conn.close()

async def main():
    """Main function"""
    calculator = DatasetDimensionCalculator()
    
    # Calculate dimensions for first 50 datasets
    await calculator.calculate_dimensions(limit=50)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())


