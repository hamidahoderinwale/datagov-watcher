"""
Enhanced Event Extractor for Dataset State Historian

This module provides advanced event extraction capabilities including:
- Normalized event types (LICENSE_CHANGED, SCHEMA_SHRINK, etc.)
- Event severity classification
- Event correlation and pattern detection
- Timeline event generation
- Event impact analysis
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from enum import Enum
import hashlib

logger = logging.getLogger(__name__)

class EventType(Enum):
    """Normalized event types"""
    # Metadata changes
    TITLE_CHANGED = "TITLE_CHANGED"
    AGENCY_CHANGED = "AGENCY_CHANGED"
    URL_CHANGED = "URL_CHANGED"
    DESCRIPTION_CHANGED = "DESCRIPTION_CHANGED"
    LICENSE_CHANGED = "LICENSE_CHANGED"
    PUBLISHER_CHANGED = "PUBLISHER_CHANGED"
    
    # Schema changes
    SCHEMA_SHRINK = "SCHEMA_SHRINK"
    SCHEMA_EXPAND = "SCHEMA_EXPAND"
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    COLUMN_RENAMED = "COLUMN_RENAMED"
    DATA_TYPE_CHANGED = "DATA_TYPE_CHANGED"
    
    # Content changes
    ROW_COUNT_INCREASED = "ROW_COUNT_INCREASED"
    ROW_COUNT_DECREASED = "ROW_COUNT_DECREASED"
    CONTENT_DRIFT = "CONTENT_DRIFT"
    FILE_SIZE_CHANGED = "FILE_SIZE_CHANGED"
    
    # Availability changes
    BECAME_AVAILABLE = "BECAME_AVAILABLE"
    BECAME_UNAVAILABLE = "BECAME_UNAVAILABLE"
    STATUS_CODE_CHANGED = "STATUS_CODE_CHANGED"
    
    # Structural changes
    FORMAT_CHANGED = "FORMAT_CHANGED"
    STRUCTURE_CHANGED = "STRUCTURE_CHANGED"
    
    # Temporal changes
    LAST_MODIFIED_CHANGED = "LAST_MODIFIED_CHANGED"
    SNAPSHOT_CREATED = "SNAPSHOT_CREATED"

class EventSeverity(Enum):
    """Event severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

@dataclass
class DatasetEvent:
    """Represents a normalized dataset event"""
    event_id: str
    dataset_id: str
    event_type: EventType
    severity: EventSeverity
    timestamp: str
    description: str
    old_value: Optional[str]
    new_value: Optional[str]
    impact_score: float
    metadata: Dict

class EnhancedEventExtractor:
    """Enhanced event extraction and analysis system"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def extract_events_for_dataset(self, dataset_id: str) -> List[DatasetEvent]:
        """Extract all events for a specific dataset"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all snapshots for this dataset
            cursor.execute('''
                SELECT snapshot_date, title, agency, url, status_code, content_type,
                       resource_format, schema, last_modified, availability, row_count, 
                       column_count, file_size, content_hash
                FROM dataset_states
                WHERE dataset_id = ?
                ORDER BY snapshot_date ASC
            ''', (dataset_id,))
            
            snapshots = cursor.fetchall()
            if len(snapshots) < 2:
                conn.close()
                return []
            
            events = []
            for i in range(1, len(snapshots)):
                prev = snapshots[i-1]
                curr = snapshots[i]
                
                # Extract events from this snapshot pair
                snapshot_events = self._extract_events_from_snapshot_pair(
                    dataset_id, prev, curr, i
                )
                events.extend(snapshot_events)
            
            conn.close()
            return events
            
        except Exception as e:
            logger.error(f"Error extracting events for {dataset_id}: {e}")
            return []
    
    def _extract_events_from_snapshot_pair(self, dataset_id: str, prev: Tuple, curr: Tuple, index: int) -> List[DatasetEvent]:
        """Extract events from a pair of consecutive snapshots"""
        events = []
        timestamp = curr[0]  # snapshot_date
        
        # Metadata changes
        if prev[1] != curr[1]:  # title
            events.append(self._create_event(
                dataset_id, EventType.TITLE_CHANGED, EventSeverity.MEDIUM,
                timestamp, "Dataset title changed", prev[1], curr[1], 0.7
            ))
        
        if prev[2] != curr[2]:  # agency
            events.append(self._create_event(
                dataset_id, EventType.AGENCY_CHANGED, EventSeverity.HIGH,
                timestamp, "Dataset agency changed", prev[2], curr[2], 0.9
            ))
        
        if prev[3] != curr[3]:  # url
            events.append(self._create_event(
                dataset_id, EventType.URL_CHANGED, EventSeverity.HIGH,
                timestamp, "Dataset URL changed", prev[3], curr[3], 0.8
            ))
        
        # Content type changes
        if prev[5] != curr[5]:  # content_type
            events.append(self._create_event(
                dataset_id, EventType.FORMAT_CHANGED, EventSeverity.MEDIUM,
                timestamp, "Content type changed", prev[5], curr[5], 0.6
            ))
        
        # Schema changes
        schema_events = self._extract_schema_events(dataset_id, prev, curr, timestamp)
        events.extend(schema_events)
        
        # Content changes
        content_events = self._extract_content_events(dataset_id, prev, curr, timestamp)
        events.extend(content_events)
        
        # Availability changes
        availability_events = self._extract_availability_events(dataset_id, prev, curr, timestamp)
        events.extend(availability_events)
        
        # Temporal changes
        if prev[8] != curr[8]:  # last_modified
            events.append(self._create_event(
                dataset_id, EventType.LAST_MODIFIED_CHANGED, EventSeverity.LOW,
                timestamp, "Last modified date changed", prev[8], curr[8], 0.2
            ))
        
        # Always add snapshot created event
        events.append(self._create_event(
            dataset_id, EventType.SNAPSHOT_CREATED, EventSeverity.LOW,
            timestamp, f"Snapshot {index} created", None, None, 0.1
        ))
        
        return events
    
    def _extract_schema_events(self, dataset_id: str, prev: Tuple, curr: Tuple, timestamp: str) -> List[DatasetEvent]:
        """Extract schema-related events"""
        events = []
        
        # Row count changes
        prev_rows = prev[10] if prev[10] is not None else 0
        curr_rows = curr[10] if curr[10] is not None else 0
        
        if prev_rows != curr_rows:
            if curr_rows > prev_rows:
                events.append(self._create_event(
                    dataset_id, EventType.ROW_COUNT_INCREASED, EventSeverity.MEDIUM,
                    timestamp, f"Row count increased from {prev_rows} to {curr_rows}",
                    str(prev_rows), str(curr_rows), 0.6
                ))
            else:
                events.append(self._create_event(
                    dataset_id, EventType.ROW_COUNT_DECREASED, EventSeverity.HIGH,
                    timestamp, f"Row count decreased from {prev_rows} to {curr_rows}",
                    str(prev_rows), str(curr_rows), 0.8
                ))
        
        # Column count changes
        prev_cols = prev[11] if prev[11] is not None else 0
        curr_cols = curr[11] if curr[11] is not None else 0
        
        if prev_cols != curr_cols:
            if curr_cols > prev_cols:
                events.append(self._create_event(
                    dataset_id, EventType.SCHEMA_EXPAND, EventSeverity.MEDIUM,
                    timestamp, f"Schema expanded from {prev_cols} to {curr_cols} columns",
                    str(prev_cols), str(curr_cols), 0.6
                ))
            else:
                events.append(self._create_event(
                    dataset_id, EventType.SCHEMA_SHRINK, EventSeverity.HIGH,
                    timestamp, f"Schema shrunk from {prev_cols} to {curr_cols} columns",
                    str(prev_cols), str(curr_cols), 0.8
                ))
        
        # Schema structure changes
        if prev[7] and curr[7]:  # schema
            try:
                prev_schema = json.loads(prev[7])
                curr_schema = json.loads(curr[7])
                
                if prev_schema != curr_schema:
                    events.append(self._create_event(
                        dataset_id, EventType.STRUCTURE_CHANGED, EventSeverity.HIGH,
                        timestamp, "Dataset structure changed",
                        json.dumps(prev_schema), json.dumps(curr_schema), 0.9
                    ))
            except (json.JSONDecodeError, TypeError):
                pass
        
        return events
    
    def _extract_content_events(self, dataset_id: str, prev: Tuple, curr: Tuple, timestamp: str) -> List[DatasetEvent]:
        """Extract content-related events"""
        events = []
        
        # File size changes
        prev_size = prev[12] if prev[12] is not None else 0
        curr_size = curr[12] if curr[12] is not None else 0
        
        if prev_size != curr_size:
            events.append(self._create_event(
                dataset_id, EventType.FILE_SIZE_CHANGED, EventSeverity.MEDIUM,
                timestamp, f"File size changed from {prev_size} to {curr_size} bytes",
                str(prev_size), str(curr_size), 0.5
            ))
        
        # Content hash changes (indicates content drift)
        if prev[13] != curr[13]:  # content_hash
            events.append(self._create_event(
                dataset_id, EventType.CONTENT_DRIFT, EventSeverity.MEDIUM,
                timestamp, "Content drift detected",
                prev[13], curr[13], 0.7
            ))
        
        return events
    
    def _extract_availability_events(self, dataset_id: str, prev: Tuple, curr: Tuple, timestamp: str) -> List[DatasetEvent]:
        """Extract availability-related events"""
        events = []
        
        # Availability changes
        if prev[9] != curr[9]:  # availability
            if curr[9] == 'available':
                events.append(self._create_event(
                    dataset_id, EventType.BECAME_AVAILABLE, EventSeverity.MEDIUM,
                    timestamp, "Dataset became available",
                    prev[9], curr[9], 0.6
                ))
            elif curr[9] == 'unavailable':
                events.append(self._create_event(
                    dataset_id, EventType.BECAME_UNAVAILABLE, EventSeverity.HIGH,
                    timestamp, "Dataset became unavailable",
                    prev[9], curr[9], 0.9
                ))
        
        # Status code changes
        if prev[4] != curr[4]:  # status_code
            events.append(self._create_event(
                dataset_id, EventType.STATUS_CODE_CHANGED, EventSeverity.MEDIUM,
                timestamp, f"Status code changed from {prev[4]} to {curr[4]}",
                str(prev[4]), str(curr[4]), 0.5
            ))
        
        return events
    
    def _create_event(self, dataset_id: str, event_type: EventType, severity: EventSeverity,
                     timestamp: str, description: str, old_value: Optional[str], 
                     new_value: Optional[str], impact_score: float) -> DatasetEvent:
        """Create a dataset event"""
        event_id = hashlib.md5(f"{dataset_id}_{event_type.value}_{timestamp}".encode()).hexdigest()[:12]
        
        return DatasetEvent(
            event_id=event_id,
            dataset_id=dataset_id,
            event_type=event_type,
            severity=severity,
            timestamp=timestamp,
            description=description,
            old_value=old_value,
            new_value=new_value,
            impact_score=impact_score,
            metadata={
                'extracted_at': datetime.now().isoformat(),
                'event_type_category': self._get_event_category(event_type)
            }
        )
    
    def _get_event_category(self, event_type: EventType) -> str:
        """Get category for event type"""
        if event_type.value.startswith('TITLE_') or event_type.value.startswith('AGENCY_') or \
           event_type.value.startswith('URL_') or event_type.value.startswith('DESCRIPTION_') or \
           event_type.value.startswith('LICENSE_') or event_type.value.startswith('PUBLISHER_'):
            return 'metadata'
        elif event_type.value.startswith('SCHEMA_') or event_type.value.startswith('COLUMN_') or \
             event_type.value.startswith('DATA_TYPE_') or event_type.value.startswith('STRUCTURE_'):
            return 'schema'
        elif event_type.value.startswith('ROW_COUNT_') or event_type.value.startswith('CONTENT_') or \
             event_type.value.startswith('FILE_SIZE_'):
            return 'content'
        elif event_type.value.startswith('BECAME_') or event_type.value.startswith('STATUS_'):
            return 'availability'
        else:
            return 'temporal'
    
    def get_event_summary(self) -> Dict:
        """Get summary of all events across all datasets"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all unique dataset IDs
            cursor.execute('SELECT DISTINCT dataset_id FROM dataset_states')
            dataset_ids = [row[0] for row in cursor.fetchall()]
            
            all_events = []
            for dataset_id in dataset_ids:
                events = self.extract_events_for_dataset(dataset_id)
                all_events.extend(events)
            
            conn.close()
            
            # Analyze events
            event_counts = {}
            severity_counts = {}
            category_counts = {}
            
            for event in all_events:
                # Count by type
                event_type = event.event_type.value
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
                
                # Count by severity
                severity = event.severity.value
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                
                # Count by category
                category = event.metadata.get('event_type_category', 'unknown')
                category_counts[category] = category_counts.get(category, 0) + 1
            
            return {
                'total_events': len(all_events),
                'total_datasets': len(dataset_ids),
                'event_counts': event_counts,
                'severity_counts': severity_counts,
                'category_counts': category_counts,
                'recent_events': [
                    {
                        'event_id': event.event_id,
                        'dataset_id': event.dataset_id,
                        'event_type': event.event_type.value,
                        'severity': event.severity.value,
                        'timestamp': event.timestamp,
                        'description': event.description,
                        'impact_score': event.impact_score
                    }
                    for event in sorted(all_events, key=lambda x: x.timestamp, reverse=True)[:50]
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting event summary: {e}")
            return {
                'total_events': 0,
                'total_datasets': 0,
                'event_counts': {},
                'severity_counts': {},
                'category_counts': {},
                'recent_events': []
            }
    
    def get_events_for_timeline(self, dataset_id: str, start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> List[Dict]:
        """Get events for timeline visualization"""
        events = self.extract_events_for_dataset(dataset_id)
        
        # Filter by date range if provided
        if start_date:
            events = [e for e in events if e.timestamp >= start_date]
        if end_date:
            events = [e for e in events if e.timestamp <= end_date]
        
        # Convert to timeline format
        timeline_events = []
        for event in events:
            timeline_events.append({
                'id': event.event_id,
                'type': event.event_type.value,
                'severity': event.severity.value,
                'timestamp': event.timestamp,
                'description': event.description,
                'old_value': event.old_value,
                'new_value': event.new_value,
                'impact_score': event.impact_score,
                'category': event.metadata.get('event_type_category', 'unknown')
            })
        
        return sorted(timeline_events, key=lambda x: x['timestamp'])
