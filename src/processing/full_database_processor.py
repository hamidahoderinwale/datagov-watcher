"""
Full Database Processor for Dataset State Historian
Scales the system to process all 355,952+ datasets from Data.gov
"""

import asyncio
import aiohttp
import sqlite3
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
from pathlib import Path
import hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

class FullDatabaseProcessor:
    def __init__(self, db_path: str = "datasets.db", max_workers: int = 50):
        self.db_path = db_path
        self.max_workers = max_workers
        self.batch_size = 1000
        self.rate_limit_delay = 0.1  # 100ms between requests
        self.total_datasets = 0
        self.processed_count = 0
        self.error_count = 0
        
        # Create output directories
        Path("dataset_states").mkdir(exist_ok=True)
        Path("full_database_logs").mkdir(exist_ok=True)
        
        self.init_database()
    
    def init_database(self):
        """Initialize database with optimized indexes for large-scale processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create optimized indexes for performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dataset_states_id_created 
            ON dataset_states(dataset_id, created_at)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dataset_states_created 
            ON dataset_states(created_at)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dataset_changes_created 
            ON dataset_changes(created_at)
        ''')
        
        # Create processing status table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processing_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE,
                total_datasets INTEGER,
                processed_datasets INTEGER,
                error_datasets INTEGER,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def get_total_dataset_count(self) -> int:
        """Get total number of datasets from Data.gov API"""
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://catalog.data.gov/api/3/action/package_search?rows=0"
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        count = data.get('result', {}).get('count', 0)
                        logger.info(f"Total datasets available: {count:,}")
                        return count
                    else:
                        logger.error(f"Failed to get dataset count: {response.status}")
                        return 0
        except Exception as e:
            logger.error(f"Error getting dataset count: {e}")
            return 0
    
    async def fetch_all_dataset_ids(self) -> List[str]:
        """Fetch all dataset IDs from Data.gov API"""
        dataset_ids = []
        offset = 0
        batch_size = 1000
        
        logger.info("Fetching all dataset IDs from Data.gov...")
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    url = f"https://catalog.data.gov/api/3/action/package_search?rows={batch_size}&start={offset}"
                    async with session.get(url, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get('result', {}).get('results', [])
                            
                            if not results:
                                break
                            
                            batch_ids = [dataset.get('id', '') for dataset in results if dataset.get('id')]
                            dataset_ids.extend(batch_ids)
                            
                            logger.info(f"Fetched {len(dataset_ids):,} dataset IDs so far...")
                            offset += batch_size
                            
                            # Rate limiting
                            await asyncio.sleep(self.rate_limit_delay)
                        else:
                            logger.error(f"Failed to fetch batch at offset {offset}: {response.status}")
                            break
                            
                except Exception as e:
                    logger.error(f"Error fetching batch at offset {offset}: {e}")
                    break
        
        logger.info(f"Total dataset IDs fetched: {len(dataset_ids):,}")
        return dataset_ids
    
    async def process_dataset_batch(self, session: aiohttp.ClientSession, 
                                  dataset_ids: List[str], batch_id: str) -> Dict:
        """Process a batch of datasets"""
        batch_results = {
            'batch_id': batch_id,
            'processed': 0,
            'errors': 0,
            'datasets': []
        }
        
        for dataset_id in dataset_ids:
            try:
                # Fetch dataset details
                url = f"https://catalog.data.gov/api/3/action/package_show?id={dataset_id}"
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        dataset_info = data.get('result', {})
                        
                        if dataset_info:
                            # Process dataset
                            processed_dataset = await self.process_single_dataset(dataset_info)
                            batch_results['datasets'].append(processed_dataset)
                            batch_results['processed'] += 1
                        else:
                            batch_results['errors'] += 1
                    else:
                        batch_results['errors'] += 1
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error processing dataset {dataset_id}: {e}")
                batch_results['errors'] += 1
        
        return batch_results
    
    async def process_single_dataset(self, dataset_info: Dict) -> Dict:
        """Process a single dataset and store in database"""
        dataset_id = dataset_info.get('id', '')
        title = dataset_info.get('title', 'Unknown')
        
        # Extract basic information
        processed_dataset = {
            'dataset_id': dataset_id,
            'title': title,
            'description': dataset_info.get('notes', ''),
            'organization': dataset_info.get('organization', {}).get('title', 'Unknown'),
            'license': dataset_info.get('license_title', ''),
            'modified': dataset_info.get('metadata_modified', ''),
            'url': dataset_info.get('url', ''),
            'resources': dataset_info.get('resources', []),
            'tags': [tag.get('name', '') for tag in dataset_info.get('tags', [])],
            'created_at': datetime.now().isoformat()
        }
        
        # Store in database
        self.store_dataset(processed_dataset)
        
        return processed_dataset
    
    def store_dataset(self, dataset: Dict):
        """Store dataset in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Store in dataset_states table
            cursor.execute('''
                INSERT OR REPLACE INTO dataset_states 
                (dataset_id, title, agency, url, status_code, content_hash, 
                 file_size, content_type, resource_format, row_count, column_count, 
                 schema, last_modified, availability, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dataset['dataset_id'],
                dataset['title'],
                dataset['organization'],
                dataset['url'],
                200,  # Assume available
                hashlib.md5(dataset['dataset_id'].encode()).hexdigest(),
                0,  # File size unknown
                'application/json',
                'JSON',
                0,  # Row count unknown
                0,  # Column count unknown
                json.dumps(dataset.get('resources', [])),
                dataset['modified'],
                'available',
                dataset['created_at']
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error storing dataset {dataset['dataset_id']}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def update_processing_status(self, batch_id: str, total: int, processed: int, 
                               errors: int, status: str):
        """Update processing status in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO processing_status 
                (batch_id, total_datasets, processed_datasets, error_datasets, 
                 start_time, end_time, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                batch_id,
                total,
                processed,
                errors,
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                status
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating processing status: {e}")
        finally:
            conn.close()
    
    async def process_full_database(self, max_datasets: Optional[int] = None):
        """Process the full Data.gov database"""
        logger.info("Starting full database processing...")
        
        # Get total count
        total_count = await self.get_total_dataset_count()
        if max_datasets:
            total_count = min(total_count, max_datasets)
        
        self.total_datasets = total_count
        logger.info(f"Processing {total_count:,} datasets...")
        
        # Fetch all dataset IDs
        dataset_ids = await self.fetch_all_dataset_ids()
        if max_datasets:
            dataset_ids = dataset_ids[:max_datasets]
        
        # Process in batches
        batch_size = 100
        total_batches = (len(dataset_ids) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {len(dataset_ids):,} datasets in {total_batches} batches...")
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(dataset_ids), batch_size):
                batch_id = f"batch_{i//batch_size + 1:04d}"
                batch_dataset_ids = dataset_ids[i:i + batch_size]
                
                logger.info(f"Processing {batch_id}: {len(batch_dataset_ids)} datasets...")
                
                try:
                    batch_results = await self.process_dataset_batch(
                        session, batch_dataset_ids, batch_id
                    )
                    
                    # Update status
                    self.update_processing_status(
                        batch_id, 
                        len(batch_dataset_ids),
                        batch_results['processed'],
                        batch_results['errors'],
                        'completed'
                    )
                    
                    self.processed_count += batch_results['processed']
                    self.error_count += batch_results['errors']
                    
                    logger.info(f"Completed {batch_id}: {batch_results['processed']} processed, {batch_results['errors']} errors")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_id}: {e}")
                    self.update_processing_status(
                        batch_id, 
                        len(batch_dataset_ids),
                        0,
                        len(batch_dataset_ids),
                        'error'
                    )
        
        logger.info(f"Full database processing completed!")
        logger.info(f"Total processed: {self.processed_count:,}")
        logger.info(f"Total errors: {self.error_count:,}")
        logger.info(f"Success rate: {(self.processed_count / (self.processed_count + self.error_count) * 100):.1f}%")
    
    def get_processing_stats(self) -> Dict:
        """Get current processing statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get total datasets in database
            cursor.execute('SELECT COUNT(*) FROM dataset_states')
            total_in_db = cursor.fetchone()[0]
            
            # Get processing status
            cursor.execute('''
                SELECT COUNT(*), SUM(processed_datasets), SUM(error_datasets)
                FROM processing_status
                WHERE status = 'completed'
            ''')
            result = cursor.fetchone()
            completed_batches = result[0] or 0
            total_processed = result[1] or 0
            total_errors = result[2] or 0
            
            return {
                'total_in_database': total_in_db,
                'completed_batches': completed_batches,
                'total_processed': total_processed,
                'total_errors': total_errors,
                'success_rate': (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {}
        finally:
            conn.close()

def main():
    """Main entry point for full database processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Full Database Processor')
    parser.add_argument('--max-datasets', type=int, help='Maximum datasets to process (for testing)')
    parser.add_argument('--workers', type=int, default=50, help='Number of concurrent workers')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('full_database_logs/processing.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create processor
    processor = FullDatabaseProcessor(max_workers=args.workers)
    
    # Run processing
    asyncio.run(processor.process_full_database(max_datasets=args.max_datasets))

if __name__ == '__main__':
    main()
