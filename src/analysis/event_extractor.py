"""
Event Extractor
Converts raw diffs and availability changes into normalized, queryable events
Implements the event specification from the plan
"""

import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

class EventType(Enum):
    # Availability events
    NEW = "NEW"
    VANISHED = "VANISHED"
    
    # Metadata events
    LICENSE_CHANGED = "LICENSE_CHANGED"
    URL_MOVED = "URL_MOVED"
    PUBLISHER_CHANGED = "PUBLISHER_CHANGED"
    TITLE_CHANGED = "TITLE_CHANGED"
    
    # Schema events
    SCHEMA_EXPAND = "SCHEMA_EXPAND"
    SCHEMA_SHRINK = "SCHEMA_SHRINK"
    COLUMN_RENAMED = "COLUMN_RENAMED"
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    DTYPE_CHANGED = "DTYPE_CHANGED"
    
    # Content events
    CONTENT_DRIFT = "CONTENT_DRIFT"
    ROWCOUNT_SPIKE = "ROWCOUNT_SPIKE"
    ROWCOUNT_DROP = "ROWCOUNT_DROP"
    CONTENT_HASH_CHANGED = "CONTENT_HASH_CHANGED"
    
    # System events
    RESPONSE_TIME_CHANGED = "RESPONSE_TIME_CHANGED"
    AVAILABILITY_CHANGED = "AVAILABILITY_CHANGED"

class EventSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class Event:
    dataset_id: str
    snapshot_date: str
    event_type: EventType
    severity: EventSeverity
    details: Dict[str, Any]
    created_at: datetime

class EventExtractor:
    """Extracts normalized events from diffs and availability changes"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize events table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for efficient querying
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_dataset_date 
            ON events(dataset_id, snapshot_date)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_type_severity 
            ON events(event_type, severity)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_date 
            ON events(snapshot_date)
        ''')
        
        conn.commit()
        conn.close()
    
    def extract_events_from_availability(self, availability_events: List[Dict]) -> List[Event]:
        """Extract events from availability changes"""
        events = []
        
        for avail_event in availability_events:
            dataset_id = avail_event['dataset_id']
            snapshot_date = avail_event['snapshot_date']
            status = avail_event['status']
            severity = avail_event['severity']
            
            if status == 'NEW':
                event = Event(
                    dataset_id=dataset_id,
                    snapshot_date=snapshot_date,
                    event_type=EventType.NEW,
                    severity=EventSeverity.MEDIUM,
                    details={
                        "reason": "First appearance in catalog",
                        "source": "availability_detection"
                    },
                    created_at=datetime.now()
                )
                events.append(event)
            
            elif status == 'VANISHED':
                event = Event(
                    dataset_id=dataset_id,
                    snapshot_date=snapshot_date,
                    event_type=EventType.VANISHED,
                    severity=EventSeverity.HIGH,
                    details={
                        "reason": "No longer present in catalog",
                        "last_seen": avail_event.get('previous_date'),
                        "source": "availability_detection"
                    },
                    created_at=datetime.now()
                )
                events.append(event)
            
            elif status == 'CHANGED':
                # Extract events from change summary
                change_summary = json.loads(avail_event.get('change_summary', '{}'))
                change_events = self._extract_events_from_changes(
                    dataset_id, snapshot_date, change_summary
                )
                events.extend(change_events)
        
        return events
    
    def extract_events_from_diff(self, diff_data: Dict) -> List[Event]:
        """Extract events from a diff result"""
        events = []
        
        dataset_id = diff_data['dataset_id']
        snapshot_date = diff_data['to_date']
        
        # Extract metadata change events
        metadata_changes = diff_data.get('metadata_changes', [])
        for change in metadata_changes:
            event = self._create_metadata_event(dataset_id, snapshot_date, change)
            if event:
                events.append(event)
        
        # Extract schema change events
        schema_changes = diff_data.get('schema_changes', {})
        schema_events = self._extract_schema_events(dataset_id, snapshot_date, schema_changes)
        events.extend(schema_events)
        
        # Extract content change events
        content_changes = diff_data.get('content_changes', {})
        content_events = self._extract_content_events(dataset_id, snapshot_date, content_changes)
        events.extend(content_events)
        
        return events
    
    def _extract_events_from_changes(self, dataset_id: str, snapshot_date: str, 
                                   change_summary: Dict) -> List[Event]:
        """Extract events from a change summary"""
        events = []
        
        # Metadata changes
        metadata_changes = change_summary.get('metadata_changes', [])
        for change in metadata_changes:
            event = self._create_metadata_event(dataset_id, snapshot_date, change)
            if event:
                events.append(event)
        
        # Schema changes
        schema_changes = change_summary.get('schema_changes', {})
        schema_events = self._extract_schema_events(dataset_id, snapshot_date, schema_changes)
        events.extend(schema_events)
        
        # Content changes
        content_changes = change_summary.get('content_changes', {})
        content_events = self._extract_content_events(dataset_id, snapshot_date, content_changes)
        events.extend(content_events)
        
        return events
    
    def _create_metadata_event(self, dataset_id: str, snapshot_date: str, 
                             change: Dict) -> Optional[Event]:
        """Create event from metadata change"""
        field = change.get('field')
        old_value = change.get('old_value')
        new_value = change.get('new_value')
        
        event_type = None
        severity = EventSeverity.LOW
        details = {
            "field": field,
            "old_value": old_value,
            "new_value": new_value,
            "change_type": change.get('change_type', 'modified')
        }
        
        if field == 'availability':
            event_type = EventType.AVAILABILITY_CHANGED
            severity = EventSeverity.HIGH
        elif field == 'url':
            event_type = EventType.URL_MOVED
            severity = EventSeverity.MEDIUM
        elif field == 'agency':
            event_type = EventType.PUBLISHER_CHANGED
            severity = EventSeverity.MEDIUM
        elif field == 'title':
            event_type = EventType.TITLE_CHANGED
            severity = EventSeverity.LOW
        elif field == 'response_time_ms':
            event_type = EventType.RESPONSE_TIME_CHANGED
            severity = EventSeverity.LOW
        
        if event_type:
            return Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=event_type,
                severity=severity,
                details=details,
                created_at=datetime.now()
            )
        
        return None
    
    def _extract_schema_events(self, dataset_id: str, snapshot_date: str, 
                             schema_changes: Dict) -> List[Event]:
        """Extract events from schema changes"""
        events = []
        
        added_columns = schema_changes.get('added_columns', [])
        removed_columns = schema_changes.get('removed_columns', [])
        renamed_columns = schema_changes.get('renamed_columns', [])
        dtype_changes = schema_changes.get('dtype_changes', [])
        row_delta = schema_changes.get('row_delta', 0)
        
        # Column added events
        if added_columns:
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.COLUMN_ADDED,
                severity=EventSeverity.LOW,
                details={
                    "columns": added_columns,
                    "count": len(added_columns)
                },
                created_at=datetime.now()
            ))
        
        # Column removed events
        if removed_columns:
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.COLUMN_REMOVED,
                severity=EventSeverity.MEDIUM,
                details={
                    "columns": removed_columns,
                    "count": len(removed_columns)
                },
                created_at=datetime.now()
            ))
        
        # Schema expand/shrink events
        if len(added_columns) > len(removed_columns):
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.SCHEMA_EXPAND,
                severity=EventSeverity.LOW,
                details={
                    "added": len(added_columns),
                    "removed": len(removed_columns),
                    "net_change": len(added_columns) - len(removed_columns)
                },
                created_at=datetime.now()
            ))
        elif len(removed_columns) > len(added_columns):
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.SCHEMA_SHRINK,
                severity=EventSeverity.MEDIUM,
                details={
                    "added": len(added_columns),
                    "removed": len(removed_columns),
                    "net_change": len(added_columns) - len(removed_columns)
                },
                created_at=datetime.now()
            ))
        
        # Column renamed events
        if renamed_columns:
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.COLUMN_RENAMED,
                severity=EventSeverity.LOW,
                details={
                    "renames": renamed_columns,
                    "count": len(renamed_columns)
                },
                created_at=datetime.now()
            ))
        
        # Data type changed events
        if dtype_changes:
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.DTYPE_CHANGED,
                severity=EventSeverity.MEDIUM,
                details={
                    "changes": dtype_changes,
                    "count": len(dtype_changes)
                },
                created_at=datetime.now()
            ))
        
        return events
    
    def _extract_content_events(self, dataset_id: str, snapshot_date: str, 
                              content_changes: Dict) -> List[Event]:
        """Extract events from content changes"""
        events = []
        
        similarity = content_changes.get('dataset_similarity', 1.0)
        row_count_delta = content_changes.get('row_count_delta', 0)
        content_hash_changed = content_changes.get('content_hash_changed', False)
        quantile_shifts = content_changes.get('quantile_shifts', [])
        
        # Content drift event
        if similarity < 0.8:
            severity = EventSeverity.HIGH if similarity < 0.5 else EventSeverity.MEDIUM
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.CONTENT_DRIFT,
                severity=severity,
                details={
                    "similarity": similarity,
                    "threshold": 0.8,
                    "quantile_shifts": quantile_shifts
                },
                created_at=datetime.now()
            ))
        
        # Row count spike/drop events
        if abs(row_count_delta) > 1000:  # Significant change threshold
            if row_count_delta > 0:
                event_type = EventType.ROWCOUNT_SPIKE
                severity = EventSeverity.MEDIUM
            else:
                event_type = EventType.ROWCOUNT_DROP
                severity = EventSeverity.HIGH
            
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=event_type,
                severity=severity,
                details={
                    "delta": row_count_delta,
                    "threshold": 1000
                },
                created_at=datetime.now()
            ))
        
        # Content hash changed event
        if content_hash_changed:
            events.append(Event(
                dataset_id=dataset_id,
                snapshot_date=snapshot_date,
                event_type=EventType.CONTENT_HASH_CHANGED,
                severity=EventSeverity.LOW,
                details={
                    "reason": "Content structure or data changed"
                },
                created_at=datetime.now()
            ))
        
        return events
    
    def store_events(self, events: List[Event]):
        """Store events in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for event in events:
            cursor.execute('''
                INSERT INTO events
                (dataset_id, snapshot_date, event_type, severity, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                event.dataset_id,
                event.snapshot_date,
                event.event_type.value,
                event.severity.value,
                json.dumps(event.details),
                event.created_at.isoformat()
            ))
        
        conn.commit()
        conn.close()
    
    def get_events(self, dataset_id: Optional[str] = None,
                  event_type: Optional[EventType] = None,
                  severity: Optional[EventSeverity] = None,
                  date_from: Optional[str] = None,
                  date_to: Optional[str] = None,
                  limit: int = 100) -> List[Dict]:
        """Query events with filters"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT dataset_id, snapshot_date, event_type, severity, details, created_at
            FROM events
            WHERE 1=1
        '''
        params = []
        
        if dataset_id:
            query += ' AND dataset_id = ?'
            params.append(dataset_id)
        
        if event_type:
            query += ' AND event_type = ?'
            params.append(event_type.value)
        
        if severity:
            query += ' AND severity = ?'
            params.append(severity.value)
        
        if date_from:
            query += ' AND snapshot_date >= ?'
            params.append(date_from)
        
        if date_to:
            query += ' AND snapshot_date <= ?'
            params.append(date_to)
        
        query += ' ORDER BY snapshot_date DESC, created_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        events = []
        
        for row in cursor.fetchall():
            event_data = dict(zip(columns, row))
            event_data['details'] = json.loads(event_data['details'] or '{}')
            events.append(event_data)
        
        conn.close()
        return events
    
    def get_event_summary(self) -> Dict[str, Any]:
        """Get summary statistics of events"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total events
        cursor.execute('SELECT COUNT(*) FROM events')
        total_events = cursor.fetchone()[0]
        
        # Events by type
        cursor.execute('''
            SELECT event_type, COUNT(*) as count
            FROM events
            GROUP BY event_type
            ORDER BY count DESC
        ''')
        events_by_type = dict(cursor.fetchall())
        
        # Events by severity
        cursor.execute('''
            SELECT severity, COUNT(*) as count
            FROM events
            GROUP BY severity
            ORDER BY count DESC
        ''')
        events_by_severity = dict(cursor.fetchall())
        
        # Recent events (last 7 days)
        cursor.execute('''
            SELECT COUNT(*) FROM events
            WHERE snapshot_date >= date('now', '-7 days')
        ''')
        recent_events = cursor.fetchone()[0]
        
        # High severity events
        cursor.execute('''
            SELECT COUNT(*) FROM events
            WHERE severity IN ('high', 'critical')
        ''')
        high_severity_events = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total_events": total_events,
            "events_by_type": events_by_type,
            "events_by_severity": events_by_severity,
            "recent_events": recent_events,
            "high_severity_events": high_severity_events
        }

def main():
    """Test the event extractor"""
    extractor = EventExtractor()
    
    # Test with some sample availability events
    sample_availability_events = [
        {
            "dataset_id": "test_dataset_1",
            "snapshot_date": "2024-01-15",
            "status": "NEW",
            "severity": "medium",
            "change_summary": None
        },
        {
            "dataset_id": "test_dataset_2", 
            "snapshot_date": "2024-01-15",
            "status": "CHANGED",
            "severity": "medium",
            "change_summary": json.dumps({
                "metadata_changes": [
                    {"field": "url", "old_value": "https://old.gov/data", "new_value": "https://new.gov/data", "change_type": "modified"}
                ],
                "schema_changes": {
                    "added_columns": ["new_column"],
                    "removed_columns": [],
                    "row_delta": 100
                },
                "content_changes": {
                    "dataset_similarity": 0.75,
                    "row_count_delta": 100,
                    "content_hash_changed": True
                }
            })
        }
    ]
    
    # Extract events
    events = extractor.extract_events_from_availability(sample_availability_events)
    
    print(f"Extracted {len(events)} events:")
    for event in events:
        print(f"  {event.dataset_id}: {event.event_type.value} ({event.severity.value})")
        print(f"    Details: {event.details}")
    
    # Store events
    extractor.store_events(events)
    print("Events stored in database")
    
    # Get summary
    summary = extractor.get_event_summary()
    print(f"\nEvent Summary:")
    print(f"Total events: {summary['total_events']}")
    print(f"Events by type: {summary['events_by_type']}")
    print(f"Events by severity: {summary['events_by_severity']}")

if __name__ == "__main__":
    main()
