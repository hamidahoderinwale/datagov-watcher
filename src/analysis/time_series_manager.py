"""
Time Series Manager: Handles daily snapshots and change detection
Part of Phase 1: Time-Series Foundation
"""

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class TimeSeriesManager:
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self.init_tables()
    
    def init_tables(self):
        """Initialize time-series tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Daily snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                total_datasets INTEGER,
                available_datasets INTEGER,
                unavailable_datasets INTEGER,
                error_datasets INTEGER,
                total_rows INTEGER,
                total_columns INTEGER,
                avg_file_size REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(snapshot_date)
            )
        ''')
        
        # Dataset changes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                change_date TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_description TEXT,
                severity TEXT DEFAULT 'info',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Dataset timeline table (for tracking individual dataset changes)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_timeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                title TEXT,
                agency TEXT,
                availability TEXT,
                row_count INTEGER,
                column_count INTEGER,
                file_size INTEGER,
                content_hash TEXT,
                resource_format TEXT,
                status_code INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_daily_snapshot(self, snapshot_date: str = None) -> Dict:
        """Create a daily snapshot of the current system state"""
        if not snapshot_date:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get current dataset states
        cursor.execute('''
            SELECT ds.dataset_id, ds.title, ds.agency, ds.availability, 
                   ds.row_count, ds.column_count, ds.file_size, ds.content_hash,
                   ds.resource_format, ds.status_code, ds.created_at
            FROM dataset_states ds
            INNER JOIN (
                SELECT dataset_id, MAX(created_at) as max_created
                FROM dataset_states 
                GROUP BY dataset_id
            ) latest ON ds.dataset_id = latest.dataset_id 
            AND ds.created_at = latest.max_created
        ''')
        
        datasets = cursor.fetchall()
        
        # Calculate snapshot metrics
        total_datasets = len(datasets)
        available_datasets = len([d for d in datasets if d[3] == 'available'])
        unavailable_datasets = len([d for d in datasets if d[3] == 'unavailable'])
        error_datasets = len([d for d in datasets if d[3] == 'error'])
        
        total_rows = sum(d[4] or 0 for d in datasets)
        total_columns = sum(d[5] or 0 for d in datasets)
        avg_file_size = sum(d[6] or 0 for d in datasets) / total_datasets if total_datasets > 0 else 0
        
        # Store daily snapshot
        cursor.execute('''
            INSERT OR REPLACE INTO daily_snapshots 
            (snapshot_date, total_datasets, available_datasets, unavailable_datasets, 
             error_datasets, total_rows, total_columns, avg_file_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (snapshot_date, total_datasets, available_datasets, unavailable_datasets,
              error_datasets, total_rows, total_columns, avg_file_size))
        
        # Store individual dataset timeline entries
        for dataset in datasets:
            cursor.execute('''
                INSERT OR REPLACE INTO dataset_timeline 
                (dataset_id, snapshot_date, title, agency, availability, row_count, 
                 column_count, file_size, content_hash, resource_format, status_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (dataset[0], snapshot_date, dataset[1], dataset[2], dataset[3],
                  dataset[4], dataset[5], dataset[6], dataset[7], dataset[8], dataset[9]))
        
        conn.commit()
        conn.close()
        
        snapshot_data = {
            'snapshot_date': snapshot_date,
            'total_datasets': total_datasets,
            'available_datasets': available_datasets,
            'unavailable_datasets': unavailable_datasets,
            'error_datasets': error_datasets,
            'total_rows': total_rows,
            'total_columns': total_columns,
            'avg_file_size': avg_file_size,
            'availability_rate': (available_datasets / total_datasets * 100) if total_datasets > 0 else 0
        }
        
        logger.info(f"Created daily snapshot for {snapshot_date}: {total_datasets} datasets, {available_datasets} available")
        return snapshot_data
    
    def detect_changes(self, current_date: str = None) -> List[Dict]:
        """Detect changes between current and previous snapshots"""
        if not current_date:
            current_date = datetime.now().strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get previous snapshot date
        cursor.execute('''
            SELECT snapshot_date FROM daily_snapshots 
            WHERE snapshot_date < ? 
            ORDER BY snapshot_date DESC LIMIT 1
        ''', (current_date,))
        
        prev_result = cursor.fetchone()
        if not prev_result:
            logger.info("No previous snapshot found for change detection")
            return []
        
        prev_date = prev_result[0]
        
        # Get current and previous dataset states
        cursor.execute('''
            SELECT dataset_id, title, agency, availability, row_count, column_count, 
                   file_size, content_hash, resource_format, status_code
            FROM dataset_timeline 
            WHERE snapshot_date = ?
        ''', (current_date,))
        current_datasets = {row[0]: row[1:] for row in cursor.fetchall()}
        
        cursor.execute('''
            SELECT dataset_id, title, agency, availability, row_count, column_count, 
                   file_size, content_hash, resource_format, status_code
            FROM dataset_timeline 
            WHERE snapshot_date = ?
        ''', (prev_date,))
        prev_datasets = {row[0]: row[1:] for row in cursor.fetchall()}
        
        changes = []
        
        # Check for new datasets
        for dataset_id in current_datasets:
            if dataset_id not in prev_datasets:
                changes.append({
                    'dataset_id': dataset_id,
                    'change_date': current_date,
                    'change_type': 'dataset_added',
                    'old_value': None,
                    'new_value': current_datasets[dataset_id][0],  # title
                    'change_description': f"New dataset added: {current_datasets[dataset_id][0]}",
                    'severity': 'info'
                })
        
        # Check for removed datasets
        for dataset_id in prev_datasets:
            if dataset_id not in current_datasets:
                changes.append({
                    'dataset_id': dataset_id,
                    'change_date': current_date,
                    'change_type': 'dataset_removed',
                    'old_value': prev_datasets[dataset_id][0],  # title
                    'new_value': None,
                    'change_description': f"Dataset removed: {prev_datasets[dataset_id][0]}",
                    'severity': 'warning'
                })
        
        # Check for changes in existing datasets
        for dataset_id in current_datasets:
            if dataset_id in prev_datasets:
                current = current_datasets[dataset_id]
                previous = prev_datasets[dataset_id]
                
                # Check availability changes
                if current[2] != previous[2]:  # availability
                    changes.append({
                        'dataset_id': dataset_id,
                        'change_date': current_date,
                        'change_type': 'availability_changed',
                        'old_value': previous[2],
                        'new_value': current[2],
                        'change_description': f"Availability changed from {previous[2]} to {current[2]}",
                        'severity': 'warning' if current[2] == 'unavailable' else 'info'
                    })
                
                # Check row count changes
                if current[3] != previous[3] and current[3] is not None and previous[3] is not None:
                    row_diff = current[3] - previous[3]
                    if abs(row_diff) > 0:
                        changes.append({
                            'dataset_id': dataset_id,
                            'change_date': current_date,
                            'change_type': 'row_count_changed',
                            'old_value': str(previous[3]),
                            'new_value': str(current[3]),
                            'change_description': f"Row count changed by {row_diff:+d} ({previous[3]} → {current[3]})",
                            'severity': 'info' if abs(row_diff) < 1000 else 'warning'
                        })
                
                # Check content hash changes
                if current[6] != previous[6] and current[6] and previous[6]:
                    changes.append({
                        'dataset_id': dataset_id,
                        'change_date': current_date,
                        'change_type': 'content_changed',
                        'old_value': previous[6][:16] + '...',  # Truncated hash
                        'new_value': current[6][:16] + '...',
                        'change_description': "Dataset content has changed",
                        'severity': 'info'
                    })
        
        # Store changes in database
        for change in changes:
            cursor.execute('''
                INSERT INTO dataset_changes 
                (dataset_id, change_date, change_type, old_value, new_value, 
                 change_description, severity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (change['dataset_id'], change['change_date'], change['change_type'],
                  change['old_value'], change['new_value'], change['change_description'],
                  change['severity']))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Detected {len(changes)} changes between {prev_date} and {current_date}")
        return changes
    
    def get_timeline_data(self, days: int = 30, agency_filter: str = None) -> Dict:
        """Get timeline data for visualization with optional agency filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build agency filter condition
        agency_condition = ""
        agency_params = []
        if agency_filter:
            agency_condition = "AND dt.agency = ?"
            agency_params = [agency_filter]
        
        # Get daily snapshots with agency filtering
        cursor.execute(f'''
            SELECT ds.snapshot_date, 
                   COUNT(DISTINCT dt.dataset_id) as total_datasets,
                   COUNT(DISTINCT CASE WHEN dt.availability = 'available' THEN dt.dataset_id END) as available_datasets,
                   COUNT(DISTINCT CASE WHEN dt.availability = 'unavailable' THEN dt.dataset_id END) as unavailable_datasets,
                   COUNT(DISTINCT CASE WHEN dt.availability = 'error' THEN dt.dataset_id END) as error_datasets,
                   COALESCE(SUM(dt.row_count), 0) as total_rows,
                   COALESCE(SUM(dt.column_count), 0) as total_columns,
                   COALESCE(AVG(dt.file_size), 0) as avg_file_size
            FROM daily_snapshots ds
            LEFT JOIN dataset_timeline dt ON ds.snapshot_date = dt.snapshot_date
            WHERE ds.snapshot_date >= date('now', '-{days} days')
            {agency_condition}
            GROUP BY ds.snapshot_date
            ORDER BY ds.snapshot_date DESC
        ''', agency_params)
        
        snapshots = cursor.fetchall()
        
        # Get recent changes with agency filtering
        if agency_filter:
            cursor.execute('''
                SELECT dc.change_date, dc.change_type, COUNT(*) as count
                FROM dataset_changes dc
                INNER JOIN dataset_timeline dt ON dc.dataset_id = dt.dataset_id 
                    AND dc.change_date = dt.snapshot_date
                WHERE dc.change_date >= date('now', '-{} days')
                AND dt.agency = ?
                GROUP BY dc.change_date, dc.change_type
                ORDER BY dc.change_date DESC
            '''.format(days), [agency_filter])
        else:
            cursor.execute('''
                SELECT change_date, change_type, COUNT(*) as count
                FROM dataset_changes 
                WHERE change_date >= date('now', '-{} days')
                GROUP BY change_date, change_type
                ORDER BY change_date DESC
            '''.format(days))
        
        changes = cursor.fetchall()
        
        # Get agency distribution
        cursor.execute('''
            SELECT agency, COUNT(*) as count
            FROM dataset_timeline dt
            INNER JOIN (
                SELECT dataset_id, MAX(snapshot_date) as latest_date
                FROM dataset_timeline 
                GROUP BY dataset_id
            ) latest ON dt.dataset_id = latest.dataset_id 
            AND dt.snapshot_date = latest.latest_date
            WHERE agency IS NOT NULL AND agency != ''
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
        ''')
        
        agencies = cursor.fetchall()
        
        conn.close()
        
        return {
            'snapshots': [
                {
                    'date': row[0],
                    'total_datasets': row[1],
                    'available_datasets': row[2],
                    'unavailable_datasets': row[3],
                    'error_datasets': row[4],
                    'total_rows': row[5],
                    'total_columns': row[6],
                    'avg_file_size': row[7],
                    'availability_rate': (row[2] / row[1] * 100) if row[1] > 0 else 0
                }
                for row in snapshots
            ],
            'changes': [
                {
                    'date': row[0],
                    'change_type': row[1],
                    'count': row[2]
                }
                for row in changes
            ],
            'agencies': [
                {
                    'agency': row[0],
                    'count': row[1]
                }
                for row in agencies
            ]
        }
    
    def get_monthly_timeline_data(self, months: int = 12, agency_filter: str = None) -> Dict:
        """Get monthly aggregated timeline data for visualization with optional agency filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build agency filter condition
        agency_condition = ""
        agency_params = []
        if agency_filter:
            agency_condition = "AND dt.agency = ?"
            agency_params = [agency_filter]
        
        # Get monthly snapshots by aggregating daily data with agency filtering
        if agency_filter:
            cursor.execute(f'''
                WITH daily_agency_stats AS (
                    SELECT 
                        ds.snapshot_date,
                        strftime('%Y-%m', ds.snapshot_date) as month,
                        COUNT(DISTINCT dt.dataset_id) as total_datasets,
                        COUNT(DISTINCT CASE WHEN dt.availability = 'available' THEN dt.dataset_id END) as available_datasets,
                        COUNT(DISTINCT CASE WHEN dt.availability = 'unavailable' THEN dt.dataset_id END) as unavailable_datasets,
                        COUNT(DISTINCT CASE WHEN dt.availability = 'error' THEN dt.dataset_id END) as error_datasets,
                        COALESCE(SUM(dt.row_count), 0) as total_rows,
                        COALESCE(SUM(dt.column_count), 0) as total_columns,
                        COALESCE(AVG(dt.file_size), 0) as avg_file_size
                    FROM daily_snapshots ds
                    LEFT JOIN dataset_timeline dt ON ds.snapshot_date = dt.snapshot_date
                    WHERE ds.snapshot_date >= date('now', '-{months} months')
                    {agency_condition}
                    GROUP BY ds.snapshot_date
                )
                SELECT 
                    month,
                    AVG(total_datasets) as avg_total_datasets,
                    AVG(available_datasets) as avg_available_datasets,
                    AVG(unavailable_datasets) as avg_unavailable_datasets,
                    AVG(error_datasets) as avg_error_datasets,
                    AVG(total_rows) as avg_total_rows,
                    AVG(total_columns) as avg_total_columns,
                    AVG(avg_file_size) as avg_file_size,
                    MAX(total_datasets) as max_total_datasets,
                    MIN(total_datasets) as min_total_datasets,
                    COUNT(*) as snapshot_count
                FROM daily_agency_stats
                GROUP BY month
                ORDER BY month DESC 
                LIMIT ?
            '''.format(months=months), agency_params + [months])
        else:
            cursor.execute('''
                SELECT 
                    strftime('%Y-%m', snapshot_date) as month,
                    AVG(total_datasets) as avg_total_datasets,
                    AVG(available_datasets) as avg_available_datasets,
                    AVG(unavailable_datasets) as avg_unavailable_datasets,
                    AVG(error_datasets) as avg_error_datasets,
                    AVG(total_rows) as avg_total_rows,
                    AVG(total_columns) as avg_total_columns,
                    AVG(avg_file_size) as avg_file_size,
                    MAX(total_datasets) as max_total_datasets,
                    MIN(total_datasets) as min_total_datasets,
                    COUNT(*) as snapshot_count
                FROM daily_snapshots 
                WHERE snapshot_date >= date('now', '-{} months')
                GROUP BY strftime('%Y-%m', snapshot_date)
                ORDER BY month DESC 
                LIMIT ?
            '''.format(months), (months,))
        
        monthly_snapshots = cursor.fetchall()
        
        # Get monthly changes with agency filtering
        if agency_filter:
            cursor.execute('''
                SELECT 
                    strftime('%Y-%m', dc.change_date) as month,
                    dc.change_type, 
                    COUNT(*) as count
                FROM dataset_changes dc
                INNER JOIN dataset_timeline dt ON dc.dataset_id = dt.dataset_id 
                    AND dc.change_date = dt.snapshot_date
                WHERE dc.change_date >= date('now', '-{} months')
                AND dt.agency = ?
                GROUP BY strftime('%Y-%m', dc.change_date), dc.change_type
                ORDER BY month DESC
            '''.format(months), [agency_filter])
        else:
            cursor.execute('''
                SELECT 
                    strftime('%Y-%m', change_date) as month,
                    change_type, 
                    COUNT(*) as count
                FROM dataset_changes 
                WHERE change_date >= date('now', '-{} months')
                GROUP BY strftime('%Y-%m', change_date), change_type
                ORDER BY month DESC
            '''.format(months))
        
        monthly_changes = cursor.fetchall()
        
        # Get agency distribution for the latest month
        cursor.execute('''
            SELECT agency, COUNT(*) as count
            FROM dataset_timeline dt
            INNER JOIN (
                SELECT dataset_id, MAX(snapshot_date) as latest_date
                FROM dataset_timeline 
                GROUP BY dataset_id
            ) latest ON dt.dataset_id = latest.dataset_id 
            AND dt.snapshot_date = latest.latest_date
            WHERE agency IS NOT NULL AND agency != ''
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 10
        ''')
        
        agencies = cursor.fetchall()
        
        conn.close()
        
        return {
            'snapshots': [
                {
                    'month': row[0],
                    'avg_total_datasets': round(row[1], 1),
                    'avg_available_datasets': round(row[2], 1),
                    'avg_unavailable_datasets': round(row[3], 1),
                    'avg_error_datasets': round(row[4], 1),
                    'avg_total_rows': round(row[5], 0),
                    'avg_total_columns': round(row[6], 0),
                    'avg_file_size': round(row[7], 0),
                    'max_total_datasets': row[8],
                    'min_total_datasets': row[9],
                    'snapshot_count': row[10],
                    'avg_availability_rate': round((row[2] / row[1] * 100) if row[1] > 0 else 0, 1)
                }
                for row in monthly_snapshots
            ],
            'changes': [
                {
                    'month': row[0],
                    'change_type': row[1],
                    'count': row[2]
                }
                for row in monthly_changes
            ],
            'agencies': [
                {
                    'agency': row[0],
                    'count': row[1]
                }
                for row in agencies
            ]
        }
    
    def get_dataset_timeline(self, dataset_id: str) -> List[Dict]:
        """Get timeline for a specific dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date, title, agency, availability, row_count, column_count,
                   file_size, resource_format, status_code
            FROM dataset_timeline 
            WHERE dataset_id = ?
            ORDER BY snapshot_date ASC
        ''', (dataset_id,))
        
        timeline = []
        for row in cursor.fetchall():
            timeline.append({
                'date': row[0],
                'title': row[1],
                'agency': row[2],
                'availability': row[3],
                'row_count': row[4],
                'column_count': row[5],
                'file_size': row[6],
                'resource_format': row[7],
                'status_code': row[8]
            })
        
        conn.close()
        return timeline


