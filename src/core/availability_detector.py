"""
Historical Availability Detector
Detects NEW, CHANGED, and VANISHED datasets across snapshots
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class DatasetStatus(Enum):
    NEW = "NEW"
    CHANGED = "CHANGED"
    VANISHED = "VANISHED"
    UNCHANGED = "UNCHANGED"

@dataclass
class AvailabilityEvent:
    dataset_id: str
    snapshot_date: str
    status: DatasetStatus
    previous_date: Optional[str]
    change_summary: Optional[Dict]
    severity: str  # "low", "medium", "high"
    details: Dict

class AvailabilityDetector:
    """Detects dataset availability changes across snapshots"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables for availability tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create availability events table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS availability_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                status TEXT NOT NULL,
                previous_date TEXT,
                change_summary TEXT,
                severity TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        # Create snapshot index for fast lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_availability_dataset_date 
            ON availability_events(dataset_id, snapshot_date)
        ''')
        
        conn.commit()
        conn.close()
    
    def get_dataset_snapshots(self, dataset_id: str) -> List[Dict]:
        """Get all snapshots for a dataset ordered by date"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, created_at, availability, title, agency, url, 
                   row_count, column_count, last_modified, dimension_computation_time_ms
            FROM dataset_states 
            WHERE dataset_id = ?
            ORDER BY created_at ASC
        ''', (dataset_id,))
        
        columns = [description[0] for description in cursor.description]
        snapshots = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return snapshots
    
    def get_all_snapshot_dates(self) -> List[str]:
        """Get all unique snapshot dates in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT DATE(created_at) as snapshot_date
            FROM dataset_states
            ORDER BY snapshot_date ASC
        ''')
        
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates
    
    def get_datasets_at_date(self, snapshot_date: str) -> Set[str]:
        """Get all dataset IDs present at a specific snapshot date"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT dataset_id
            FROM dataset_states
            WHERE DATE(created_at) = ?
        ''', (snapshot_date,))
        
        dataset_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return dataset_ids
    
    def detect_availability_changes(self, from_date: str, to_date: str) -> List[AvailabilityEvent]:
        """Detect availability changes between two snapshot dates"""
        from_datasets = self.get_datasets_at_date(from_date)
        to_datasets = self.get_datasets_at_date(to_date)
        
        events = []
        
        # Find NEW datasets
        new_datasets = to_datasets - from_datasets
        for dataset_id in new_datasets:
            event = AvailabilityEvent(
                dataset_id=dataset_id,
                snapshot_date=to_date,
                status=DatasetStatus.NEW,
                previous_date=None,
                change_summary=None,
                severity="medium",
                details={"reason": "First appearance in catalog"}
            )
            events.append(event)
        
        # Find VANISHED datasets
        vanished_datasets = from_datasets - to_datasets
        for dataset_id in vanished_datasets:
            event = AvailabilityEvent(
                dataset_id=dataset_id,
                snapshot_date=to_date,
                status=DatasetStatus.VANISHED,
                previous_date=from_date,
                change_summary=None,
                severity="high",
                details={"reason": "No longer present in catalog", "last_seen": from_date}
            )
            events.append(event)
        
        # Find CHANGED datasets
        common_datasets = from_datasets & to_datasets
        for dataset_id in common_datasets:
            change_summary = self._detect_dataset_changes(dataset_id, from_date, to_date)
            if change_summary and change_summary.get('has_changes', False):
                event = AvailabilityEvent(
                    dataset_id=dataset_id,
                    snapshot_date=to_date,
                    status=DatasetStatus.CHANGED,
                    previous_date=from_date,
                    change_summary=change_summary,
                    severity=self._calculate_severity(change_summary),
                    details=change_summary
                )
                events.append(event)
        
        return events
    
    def _detect_dataset_changes(self, dataset_id: str, from_date: str, to_date: str) -> Optional[Dict]:
        """Detect changes in a specific dataset between two dates"""
        from_snapshot = self._get_dataset_snapshot(dataset_id, from_date)
        to_snapshot = self._get_dataset_snapshot(dataset_id, to_date)
        
        if not from_snapshot or not to_snapshot:
            return None
        
        changes = {
            'has_changes': False,
            'metadata_changes': [],
            'schema_changes': {},
            'content_changes': {}
        }
        
        # Check metadata changes
        metadata_fields = ['title', 'agency', 'url', 'availability']
        for field in metadata_fields:
            if from_snapshot.get(field) != to_snapshot.get(field):
                changes['metadata_changes'].append({
                    'field': field,
                    'old': from_snapshot.get(field),
                    'new': to_snapshot.get(field)
                })
                changes['has_changes'] = True
        
        # Check schema changes
        from_columns = from_snapshot.get('column_count', 0) or 0
        to_columns = to_snapshot.get('column_count', 0) or 0
        if from_columns != to_columns:
            changes['schema_changes']['column_count'] = {
                'old': from_columns,
                'new': to_columns,
                'delta': to_columns - from_columns
            }
            changes['has_changes'] = True
        
        # Check content changes
        from_rows = from_snapshot.get('row_count', 0) or 0
        to_rows = to_snapshot.get('row_count', 0) or 0
        if from_rows != to_rows:
            changes['content_changes']['row_count'] = {
                'old': from_rows,
                'new': to_rows,
                'delta': to_rows - from_rows,
                'percent_change': ((to_rows - from_rows) / from_rows * 100) if from_rows > 0 else 0
            }
            changes['has_changes'] = True
        
        # Check response time changes
        from_response = from_snapshot.get('dimension_computation_time_ms', 0) or 0
        to_response = to_snapshot.get('dimension_computation_time_ms', 0) or 0
        if abs(from_response - to_response) > 1000:  # Significant response time change
            changes['content_changes']['response_time'] = {
                'old': from_response,
                'new': to_response,
                'delta': to_response - from_response
            }
            changes['has_changes'] = True
        
        return changes
    
    def _get_dataset_snapshot(self, dataset_id: str, snapshot_date: str) -> Optional[Dict]:
        """Get a specific dataset snapshot for a given date"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT dataset_id, created_at, availability, title, agency, url,
                   row_count, column_count, last_modified, dimension_computation_time_ms
            FROM dataset_states
            WHERE dataset_id = ? AND DATE(created_at) = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (dataset_id, snapshot_date))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
        return None
    
    def _calculate_severity(self, change_summary: Dict) -> str:
        """Calculate severity based on change summary"""
        severity_score = 0
        
        # Metadata changes
        metadata_changes = change_summary.get('metadata_changes', [])
        if any(change['field'] == 'availability' for change in metadata_changes):
            severity_score += 3  # Availability change is high severity
        
        if any(change['field'] == 'url' for change in metadata_changes):
            severity_score += 2  # URL change is medium-high severity
        
        # Schema changes
        schema_changes = change_summary.get('schema_changes', {})
        if 'column_count' in schema_changes:
            delta = schema_changes['column_count'].get('delta', 0)
            if abs(delta) > 5:  # Large schema change
                severity_score += 2
            elif abs(delta) > 0:
                severity_score += 1
        
        # Content changes
        content_changes = change_summary.get('content_changes', {})
        if 'row_count' in content_changes:
            percent_change = abs(content_changes['row_count'].get('percent_change', 0))
            if percent_change > 50:  # Large content change
                severity_score += 3
            elif percent_change > 20:
                severity_score += 2
            elif percent_change > 5:
                severity_score += 1
        
        if severity_score >= 4:
            return "high"
        elif severity_score >= 2:
            return "medium"
        else:
            return "low"
    
    def store_availability_events(self, events: List[AvailabilityEvent]):
        """Store availability events in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for event in events:
            cursor.execute('''
                INSERT OR REPLACE INTO availability_events
                (dataset_id, snapshot_date, status, previous_date, change_summary, 
                 severity, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event.dataset_id,
                event.snapshot_date,
                event.status.value,
                event.previous_date,
                json.dumps(event.change_summary) if event.change_summary else None,
                event.severity,
                json.dumps(event.details)
            ))
        
        conn.commit()
        conn.close()
    
    def get_availability_events(self, dataset_id: Optional[str] = None, 
                              status: Optional[DatasetStatus] = None,
                              severity: Optional[str] = None,
                              limit: int = 100) -> List[Dict]:
        """Get availability events with optional filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT dataset_id, snapshot_date, status, previous_date, 
                   change_summary, severity, details, created_at
            FROM availability_events
            WHERE 1=1
        '''
        params = []
        
        if dataset_id:
            query += ' AND dataset_id = ?'
            params.append(dataset_id)
        
        if status:
            query += ' AND status = ?'
            params.append(status.value)
        
        if severity:
            query += ' AND severity = ?'
            params.append(severity)
        
        query += ' ORDER BY snapshot_date DESC, created_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        events = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return events
    
    def process_all_snapshots(self):
        """Process all available snapshots to detect availability changes"""
        snapshot_dates = self.get_all_snapshot_dates()
        
        if len(snapshot_dates) < 2:
            print("Need at least 2 snapshot dates to detect changes")
            return
        
        print(f"Processing {len(snapshot_dates)} snapshot dates...")
        
        all_events = []
        for i in range(1, len(snapshot_dates)):
            from_date = snapshot_dates[i-1]
            to_date = snapshot_dates[i]
            
            print(f"Comparing {from_date} to {to_date}...")
            events = self.detect_availability_changes(from_date, to_date)
            all_events.extend(events)
            
            print(f"  Found {len(events)} changes")
        
        # Store all events
        self.store_availability_events(all_events)
        print(f"Stored {len(all_events)} availability events")
        
        return all_events

def main():
    """Test the availability detector"""
    detector = AvailabilityDetector()
    
    # Process all snapshots
    events = detector.process_all_snapshots()
    
    # Show summary
    if events:
        status_counts = {}
        severity_counts = {}
        
        for event in events:
            status_counts[event.status.value] = status_counts.get(event.status.value, 0) + 1
            severity_counts[event.severity] = severity_counts.get(event.severity, 0) + 1
        
        print("\n Summary:")
        print(f"Total events: {len(events)}")
        print(f"Status breakdown: {status_counts}")
        print(f"Severity breakdown: {severity_counts}")
        
        # Show recent high-severity events
        high_severity = [e for e in events if e.severity == "high"]
        if high_severity:
            print(f"\n🚨 High severity events ({len(high_severity)}):")
            for event in high_severity[:5]:  # Show first 5
                print(f"  {event.dataset_id}: {event.status.value} on {event.snapshot_date}")

if __name__ == "__main__":
    main()
