"""
Scaled Monitor for Full Database Processing
Handles monitoring and analysis of all 355,952+ datasets
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
import queue

logger = logging.getLogger(__name__)

class ScaledMonitor:
    def __init__(self, db_path: str = "datasets.db", max_workers: int = 100):
        self.db_path = db_path
        self.max_workers = max_workers
        self.batch_size = 500
        self.rate_limit_delay = 0.05  # 50ms between requests
        self.processing_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.is_running = False
        self.stats = {
            'total_datasets': 0,
            'processed': 0,
            'errors': 0,
            'start_time': None,
            'last_update': None
        }
        
        self.init_database()
    
    def init_database(self):
        """Initialize database with optimized indexes for large-scale monitoring"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create monitoring tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_batches (
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                total_datasets INTEGER,
                available_datasets INTEGER,
                unavailable_datasets INTEGER,
                error_datasets INTEGER,
                processing_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_monitoring_batches_status 
            ON monitoring_batches(status)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_monitoring_stats_timestamp 
            ON monitoring_stats(timestamp)
        ''')
        
        conn.commit()
        conn.close()
    
    async def get_dataset_sample(self, sample_size: int = 1000) -> List[str]:
        """Get a random sample of dataset IDs for testing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT dataset_id FROM dataset_states 
                ORDER BY RANDOM() 
                LIMIT ?
            ''', (sample_size,))
            
            dataset_ids = [row[0] for row in cursor.fetchall()]
            return dataset_ids
            
        except Exception as e:
            logger.error(f"Error getting dataset sample: {e}")
            return []
        finally:
            conn.close()
    
    async def monitor_dataset_batch(self, session: aiohttp.ClientSession, 
                                  dataset_ids: List[str], batch_id: str) -> Dict:
        """Monitor a batch of datasets for changes"""
        batch_results = {
            'batch_id': batch_id,
            'processed': 0,
            'errors': 0,
            'changes': 0,
            'datasets': []
        }
        
        for dataset_id in dataset_ids:
            try:
                # Check dataset availability and get current state
                dataset_info = await self.check_dataset_status(session, dataset_id)
                
                if dataset_info:
                    # Compare with stored state
                    changes = await self.detect_changes(dataset_id, dataset_info)
                    
                    if changes:
                        batch_results['changes'] += 1
                        await self.record_changes(dataset_id, changes)
                    
                    batch_results['datasets'].append(dataset_info)
                    batch_results['processed'] += 1
                else:
                    batch_results['errors'] += 1
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error monitoring dataset {dataset_id}: {e}")
                batch_results['errors'] += 1
        
        return batch_results
    
    async def check_dataset_status(self, session: aiohttp.ClientSession, dataset_id: str) -> Optional[Dict]:
        """Check the current status of a dataset"""
        try:
            url = f"https://catalog.data.gov/api/3/action/package_show?id={dataset_id}"
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    dataset_info = data.get('result', {})
                    
                    if dataset_info:
                        return {
                            'dataset_id': dataset_id,
                            'title': dataset_info.get('title', ''),
                            'modified': dataset_info.get('metadata_modified', ''),
                            'url': dataset_info.get('url', ''),
                            'status': 'available',
                            'checked_at': datetime.now().isoformat()
                        }
                    else:
                        return {
                            'dataset_id': dataset_id,
                            'status': 'not_found',
                            'checked_at': datetime.now().isoformat()
                        }
                else:
                    return {
                        'dataset_id': dataset_id,
                        'status': 'error',
                        'error_code': response.status,
                        'checked_at': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error checking dataset {dataset_id}: {e}")
            return {
                'dataset_id': dataset_id,
                'status': 'error',
                'error': str(e),
                'checked_at': datetime.now().isoformat()
            }
    
    async def detect_changes(self, dataset_id: str, current_info: Dict) -> List[Dict]:
        """Detect changes in dataset compared to stored state"""
        changes = []
        
        # Get stored state
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT title, url, last_modified, availability
                FROM dataset_states 
                WHERE dataset_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            ''', (dataset_id,))
            
            stored = cursor.fetchone()
            if not stored:
                return changes
            
            stored_title, stored_url, stored_modified, stored_availability = stored
            current_title = current_info.get('title', '')
            current_url = current_info.get('url', '')
            current_modified = current_info.get('modified', '')
            current_status = current_info.get('status', '')
            
            # Check for changes
            if stored_title != current_title:
                changes.append({
                    'field': 'title',
                    'old_value': stored_title,
                    'new_value': current_title,
                    'change_type': 'title_change'
                })
            
            if stored_url != current_url:
                changes.append({
                    'field': 'url',
                    'old_value': stored_url,
                    'new_value': current_url,
                    'change_type': 'url_change'
                })
            
            if stored_modified != current_modified:
                changes.append({
                    'field': 'modified',
                    'old_value': stored_modified,
                    'new_value': current_modified,
                    'change_type': 'metadata_change'
                })
            
            if stored_availability != current_status:
                changes.append({
                    'field': 'availability',
                    'old_value': stored_availability,
                    'new_value': current_status,
                    'change_type': 'availability_change'
                })
            
        except Exception as e:
            logger.error(f"Error detecting changes for {dataset_id}: {e}")
        finally:
            conn.close()
        
        return changes
    
    async def record_changes(self, dataset_id: str, changes: List[Dict]):
        """Record detected changes in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for change in changes:
                cursor.execute('''
                    INSERT INTO dataset_changes 
                    (dataset_id, change_date, change_type, change_description, 
                     old_value, new_value, severity, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dataset_id,
                    datetime.now().strftime('%Y-%m-%d'),
                    change['change_type'],
                    f"{change['field']} changed",
                    change['old_value'],
                    change['new_value'],
                    'medium',
                    datetime.now().isoformat()
                ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error recording changes for {dataset_id}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    async def start_monitoring(self, sample_size: Optional[int] = None):
        """Start monitoring all datasets"""
        logger.info("Starting scaled monitoring...")
        
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        
        # Get datasets to monitor
        if sample_size:
            dataset_ids = await self.get_dataset_sample(sample_size)
            logger.info(f"Monitoring sample of {len(dataset_ids)} datasets")
        else:
            # Get all dataset IDs from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT dataset_id FROM dataset_states')
            dataset_ids = [row[0] for row in cursor.fetchall()]
            conn.close()
            logger.info(f"Monitoring all {len(dataset_ids)} datasets")
        
        self.stats['total_datasets'] = len(dataset_ids)
        
        # Process in batches
        batch_size = 100
        total_batches = (len(dataset_ids) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {len(dataset_ids):,} datasets in {total_batches} batches...")
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(dataset_ids), batch_size):
                if not self.is_running:
                    break
                
                batch_id = f"monitor_batch_{i//batch_size + 1:04d}"
                batch_dataset_ids = dataset_ids[i:i + batch_size]
                
                logger.info(f"Processing {batch_id}: {len(batch_dataset_ids)} datasets...")
                
                try:
                    batch_results = await self.monitor_dataset_batch(
                        session, batch_dataset_ids, batch_id
                    )
                    
                    # Update stats
                    self.stats['processed'] += batch_results['processed']
                    self.stats['errors'] += batch_results['errors']
                    self.stats['last_update'] = datetime.now()
                    
                    # Record batch results
                    await self.record_batch_results(batch_id, batch_results)
                    
                    logger.info(f"Completed {batch_id}: {batch_results['processed']} processed, {batch_results['errors']} errors, {batch_results['changes']} changes")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_id}: {e}")
        
        self.is_running = False
        logger.info("Scaled monitoring completed!")
    
    async def record_batch_results(self, batch_id: str, results: Dict):
        """Record batch processing results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO monitoring_batches 
                (batch_id, total_datasets, processed_datasets, error_datasets, 
                 start_time, end_time, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                batch_id,
                len(results.get('datasets', [])),
                results['processed'],
                results['errors'],
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                'completed'
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error recording batch results: {e}")
        finally:
            conn.close()
    
    def get_monitoring_stats(self) -> Dict:
        """Get current monitoring statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get total datasets
            cursor.execute('SELECT COUNT(*) FROM dataset_states')
            total_datasets = cursor.fetchone()[0]
            
            # Get recent monitoring stats
            cursor.execute('''
                SELECT COUNT(*), SUM(processed_datasets), SUM(error_datasets)
                FROM monitoring_batches
                WHERE status = 'completed'
                AND created_at > datetime('now', '-1 hour')
            ''')
            result = cursor.fetchone()
            recent_batches = result[0] or 0
            recent_processed = result[1] or 0
            recent_errors = result[2] or 0
            
            # Calculate processing rate
            processing_rate = recent_processed / 3600 if recent_processed > 0 else 0  # per second
            
            return {
                'total_datasets': total_datasets,
                'recent_batches': recent_batches,
                'recent_processed': recent_processed,
                'recent_errors': recent_errors,
                'processing_rate': processing_rate,
                'is_running': self.is_running,
                'stats': self.stats
            }
            
        except Exception as e:
            logger.error(f"Error getting monitoring stats: {e}")
            return {}
        finally:
            conn.close()
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
        logger.info("Monitoring stopped")

def main():
    """Main entry point for scaled monitoring"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scaled Monitor')
    parser.add_argument('--sample-size', type=int, help='Sample size for testing')
    parser.add_argument('--workers', type=int, default=100, help='Number of concurrent workers')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create monitor
    monitor = ScaledMonitor(max_workers=args.workers)
    
    # Run monitoring
    asyncio.run(monitor.start_monitoring(sample_size=args.sample_size))

if __name__ == '__main__':
    main()
