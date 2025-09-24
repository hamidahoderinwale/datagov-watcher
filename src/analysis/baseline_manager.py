"""
Baseline Manager for Dataset Monitoring
Creates and manages historical baselines of Data.gov datasets
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class BaselineManager:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.init_baseline_tables()
    
    def init_baseline_tables(self):
        """Initialize tables for baseline tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Baseline snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS baseline_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_datasets INTEGER,
                snapshot_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Dataset history table for tracking individual dataset changes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT,
                title TEXT,
                agency TEXT,
                url TEXT,
                status TEXT,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                disappeared_at TIMESTAMP,
                reappeared_at TIMESTAMP,
                change_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # URL availability tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_availability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                status_code INTEGER,
                available BOOLEAN,
                last_checked TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_baseline(self, datasets: List[Dict]) -> int:
        """Create a new baseline snapshot from current datasets"""
        logger.info(f"Creating baseline snapshot with {len(datasets)} datasets")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store snapshot
        snapshot_data = json.dumps(datasets, default=str)
        cursor.execute('''
            INSERT INTO baseline_snapshots (total_datasets, snapshot_data)
            VALUES (?, ?)
        ''', (len(datasets), snapshot_data))
        
        snapshot_id = cursor.lastrowid
        
        # Update dataset history
        for dataset in datasets:
            self._update_dataset_history(cursor, dataset, 'active')
        
        conn.commit()
        conn.close()
        
        logger.info(f"Baseline snapshot {snapshot_id} created successfully")
        return snapshot_id
    
    def _update_dataset_history(self, cursor, dataset: Dict, status: str):
        """Update dataset history tracking"""
        dataset_id = dataset.get('id', '')
        title = dataset.get('title', '')
        agency = dataset.get('agency', '')
        url = dataset.get('url', '')
        
        # Check if dataset exists in history
        cursor.execute('''
            SELECT id, status, change_count FROM dataset_history 
            WHERE dataset_id = ? ORDER BY created_at DESC LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        
        if result:
            hist_id, current_status, change_count = result
            
            if current_status != status:
                # Status changed
                if status == 'active' and current_status == 'disappeared':
                    # Dataset reappeared
                    cursor.execute('''
                        UPDATE dataset_history 
                        SET reappeared_at = CURRENT_TIMESTAMP, 
                            status = ?, 
                            last_seen = CURRENT_TIMESTAMP,
                            change_count = change_count + 1
                        WHERE id = ?
                    ''', (status, hist_id))
                elif status == 'disappeared' and current_status == 'active':
                    # Dataset disappeared
                    cursor.execute('''
                        UPDATE dataset_history 
                        SET disappeared_at = CURRENT_TIMESTAMP, 
                            status = ?,
                            change_count = change_count + 1
                        WHERE id = ?
                    ''', (status, hist_id))
                else:
                    # Other status change
                    cursor.execute('''
                        UPDATE dataset_history 
                        SET status = ?, 
                            last_seen = CURRENT_TIMESTAMP,
                            change_count = change_count + 1
                        WHERE id = ?
                    ''', (status, hist_id))
            else:
                # Same status, update last_seen
                cursor.execute('''
                    UPDATE dataset_history 
                    SET last_seen = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (hist_id,))
        else:
            # New dataset
            cursor.execute('''
                INSERT INTO dataset_history 
                (dataset_id, title, agency, url, status, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (dataset_id, title, agency, url, status))
    
    def get_latest_baseline(self) -> Optional[Dict]:
        """Get the most recent baseline snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, snapshot_date, total_datasets, snapshot_data
            FROM baseline_snapshots 
            ORDER BY created_at DESC 
            LIMIT 1
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            snapshot_id, snapshot_date, total_datasets, snapshot_data = result
            return {
                'id': snapshot_id,
                'date': snapshot_date,
                'total_datasets': total_datasets,
                'data': json.loads(snapshot_data)
            }
        
        return None
    
    def compare_with_baseline(self, current_datasets: List[Dict]) -> Dict:
        """Compare current datasets with latest baseline"""
        baseline = self.get_latest_baseline()
        
        if not baseline:
            logger.warning("No baseline found - creating new baseline")
            self.create_baseline(current_datasets)
            return {
                'status': 'baseline_created',
                'vanished': [],
                'new': [],
                'changed': [],
                'total_current': len(current_datasets),
                'total_baseline': 0
            }
        
        baseline_data = baseline['data']
        baseline_ids = {dataset['id'] for dataset in baseline_data}
        current_ids = {dataset['id'] for dataset in current_datasets}
        
        # Find vanished datasets
        vanished_ids = baseline_ids - current_ids
        vanished = [dataset for dataset in baseline_data if dataset['id'] in vanished_ids]
        
        # Find new datasets
        new_ids = current_ids - baseline_ids
        new = [dataset for dataset in current_datasets if dataset['id'] in new_ids]
        
        # Find changed datasets (same ID but different content)
        common_ids = baseline_ids & current_ids
        changed = []
        
        for dataset_id in common_ids:
            baseline_dataset = next(d for d in baseline_data if d['id'] == dataset_id)
            current_dataset = next(d for d in current_datasets if d['id'] == dataset_id)
            
            # Compare key fields
            if (baseline_dataset.get('title') != current_dataset.get('title') or
                baseline_dataset.get('url') != current_dataset.get('url') or
                baseline_dataset.get('agency') != current_dataset.get('agency')):
                changed.append({
                    'id': dataset_id,
                    'baseline': baseline_dataset,
                    'current': current_dataset
                })
        
        # Update dataset history for vanished datasets
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for dataset in vanished:
            self._update_dataset_history(cursor, dataset, 'disappeared')
        
        for dataset in new:
            self._update_dataset_history(cursor, dataset, 'active')
        
        conn.commit()
        conn.close()
        
        return {
            'status': 'comparison_complete',
            'vanished': vanished,
            'new': new,
            'changed': changed,
            'total_current': len(current_datasets),
            'total_baseline': len(baseline_data),
            'baseline_date': baseline['date']
        }
    
    def get_vanished_datasets(self, days: int = 30) -> List[Dict]:
        """Get datasets that disappeared in the last N days"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        cursor.execute('''
            SELECT dataset_id, title, agency, url, disappeared_at, first_seen, last_seen
            FROM dataset_history 
            WHERE status = 'disappeared' 
            AND disappeared_at >= ?
            ORDER BY disappeared_at DESC
        ''', (cutoff_date,))
        
        columns = [description[0] for description in cursor.description]
        vanished = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return vanished
    
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get baseline stats
        cursor.execute('SELECT COUNT(*) FROM baseline_snapshots')
        baseline_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT MAX(created_at) FROM baseline_snapshots')
        last_baseline = cursor.fetchone()[0]
        
        # Get dataset history stats
        cursor.execute('SELECT COUNT(*) FROM dataset_history WHERE status = "active"')
        active_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dataset_history WHERE status = "disappeared"')
        disappeared_count = cursor.fetchone()[0]
        
        # Get recent vanished datasets
        recent_vanished = self.get_vanished_datasets(7)  # Last 7 days
        
        conn.close()
        
        return {
            'baseline_snapshots': baseline_count,
            'last_baseline': last_baseline,
            'active_datasets': active_count,
            'disappeared_datasets': disappeared_count,
            'recent_vanished': len(recent_vanished),
            'recent_vanished_list': recent_vanished
        }
