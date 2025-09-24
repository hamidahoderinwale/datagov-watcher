"""
Chromogram Timeline V2
Interactive timeline visualization with field-level change tracking
Implements the Chromogram specification from the plan
"""

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib

class BandType(Enum):
    METADATA = "metadata"
    SCHEMA = "schema"
    CONTENT = "content"

@dataclass
class TimelineBand:
    name: str
    band_type: BandType
    fields: List[str]
    color_map: Dict[str, str]

@dataclass
class TimelineCell:
    field: str
    date: str
    value: Any
    changed: bool
    old_value: Optional[Any] = None
    color: Optional[str] = None

@dataclass
class TimelineEvent:
    date: str
    event_type: str
    severity: str
    description: str
    dataset_id: str

@dataclass
class ChromogramData:
    dataset_id: str
    date_range: Tuple[str, str]
    bands: List[TimelineBand]
    cells: List[TimelineCell]
    events: List[TimelineEvent]
    vanished_date: Optional[str] = None

class ChromogramTimelineV2:
    """Enhanced timeline visualization with Chromogram support"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize timeline data tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create timeline snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS timeline_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                metadata_band TEXT,
                schema_band TEXT,
                content_band TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, snapshot_date)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def generate_chromogram_data(self, dataset_id: str, 
                               date_from: Optional[str] = None,
                               date_to: Optional[str] = None) -> ChromogramData:
        """Generate Chromogram data for a dataset"""
        
        # Get dataset snapshots
        snapshots = self._get_dataset_snapshots(dataset_id, date_from, date_to)
        if not snapshots:
            return ChromogramData(
                dataset_id=dataset_id,
                date_range=("", ""),
                bands=[],
                cells=[],
                events=[]
            )
        
        # Determine date range
        dates = sorted([s['snapshot_date'] for s in snapshots])
        date_range = (dates[0], dates[-1])
        
        # Create bands
        bands = self._create_timeline_bands()
        
        # Generate cells
        cells = self._generate_timeline_cells(snapshots, bands)
        
        # Get events
        events = self._get_timeline_events(dataset_id, date_from, date_to)
        
        # Check if dataset vanished
        vanished_date = self._get_vanished_date(dataset_id)
        
        return ChromogramData(
            dataset_id=dataset_id,
            date_range=date_range,
            bands=bands,
            cells=cells,
            events=events,
            vanished_date=vanished_date
        )
    
    def _get_dataset_snapshots(self, dataset_id: str, 
                             date_from: Optional[str] = None,
                             date_to: Optional[str] = None) -> List[Dict]:
        """Get dataset snapshots for timeline"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT dataset_id, DATE(created_at) as snapshot_date, title, agency, url,
                   availability, row_count, column_count, dimension_computation_time_ms, last_modified
            FROM dataset_states
            WHERE dataset_id = ?
        '''
        params = [dataset_id]
        
        if date_from:
            query += ' AND DATE(created_at) >= ?'
            params.append(date_from)
        
        if date_to:
            query += ' AND DATE(created_at) <= ?'
            params.append(date_to)
        
        query += ' ORDER BY created_at ASC'
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        snapshots = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return snapshots
    
    def _create_timeline_bands(self) -> List[TimelineBand]:
        """Create timeline bands for visualization"""
        bands = [
            TimelineBand(
                name="Metadata",
                band_type=BandType.METADATA,
                fields=["title", "agency", "url", "availability"],
                color_map={
                    "title": self._get_field_color("title"),
                    "agency": self._get_field_color("agency"),
                    "url": self._get_field_color("url"),
                    "availability": self._get_field_color("availability")
                }
            ),
            TimelineBand(
                name="Schema",
                band_type=BandType.SCHEMA,
                fields=["column_count"],
                color_map={
                    "column_count": self._get_field_color("column_count")
                }
            ),
            TimelineBand(
                name="Content",
                band_type=BandType.CONTENT,
                fields=["row_count", "response_time"],
                color_map={
                    "row_count": self._get_field_color("row_count"),
                    "response_time": self._get_field_color("response_time")
                }
            )
        ]
        
        return bands
    
    def _get_field_color(self, field: str) -> str:
        """Get consistent color for a field based on its name"""
        # Use first 3 characters of field name to generate consistent color
        hash_input = field[:3].lower()
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest()[:6], 16)
        
        # Convert to HSL and then to RGB for better color distribution
        hue = (hash_value % 360) / 360.0
        saturation = 0.7
        lightness = 0.5
        
        # Convert HSL to RGB
        import colorsys
        r, g, b = colorsys.hls_to_rgb(hue, lightness, saturation)
        
        return f"rgb({int(r*255)}, {int(g*255)}, {int(b*255)})"
    
    def _generate_timeline_cells(self, snapshots: List[Dict], 
                               bands: List[TimelineBand]) -> List[TimelineCell]:
        """Generate timeline cells for visualization"""
        cells = []
        
        # Create a map of previous values for change detection
        previous_values = {}
        
        for i, snapshot in enumerate(snapshots):
            snapshot_date = snapshot['snapshot_date']
            
            for band in bands:
                for field in band.fields:
                    # Map field names to snapshot data
                    if field == "response_time":
                        value = snapshot.get('dimension_computation_time_ms')
                    else:
                        value = snapshot.get(field)
                    
                    # Check if this field changed
                    field_key = f"{band.name}_{field}"
                    old_value = previous_values.get(field_key)
                    changed = old_value != value
                    
                    # Create cell
                    cell = TimelineCell(
                        field=field,
                        date=snapshot_date,
                        value=value,
                        changed=changed,
                        old_value=old_value,
                        color=band.color_map.get(field)
                    )
                    cells.append(cell)
                    
                    # Update previous value
                    previous_values[field_key] = value
        
        return cells
    
    def _get_timeline_events(self, dataset_id: str, 
                           date_from: Optional[str] = None,
                           date_to: Optional[str] = None) -> List[TimelineEvent]:
        """Get events for timeline markers"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT snapshot_date, event_type, severity, details
            FROM events
            WHERE dataset_id = ?
        '''
        params = [dataset_id]
        
        if date_from:
            query += ' AND snapshot_date >= ?'
            params.append(date_from)
        
        if date_to:
            query += ' AND snapshot_date <= ?'
            params.append(date_to)
        
        query += ' ORDER BY snapshot_date ASC'
        
        cursor.execute(query, params)
        events = []
        
        for row in cursor.fetchall():
            snapshot_date, event_type, severity, details_json = row
            details = json.loads(details_json or '{}')
            
            event = TimelineEvent(
                date=snapshot_date,
                event_type=event_type,
                severity=severity,
                description=self._format_event_description(event_type, details),
                dataset_id=dataset_id
            )
            events.append(event)
        
        conn.close()
        return events
    
    def _format_event_description(self, event_type: str, details: Dict) -> str:
        """Format event description for display"""
        if event_type == "NEW":
            return "Dataset first appeared"
        elif event_type == "VANISHED":
            return "Dataset disappeared"
        elif event_type == "URL_MOVED":
            return f"URL changed to {details.get('new_value', 'unknown')}"
        elif event_type == "SCHEMA_EXPAND":
            return f"Added {details.get('added', 0)} columns"
        elif event_type == "SCHEMA_SHRINK":
            return f"Removed {details.get('removed', 0)} columns"
        elif event_type == "CONTENT_DRIFT":
            similarity = details.get('similarity', 0)
            return f"Content similarity: {similarity:.2f}"
        elif event_type == "ROWCOUNT_SPIKE":
            delta = details.get('delta', 0)
            return f"Row count increased by {delta:,}"
        elif event_type == "ROWCOUNT_DROP":
            delta = details.get('delta', 0)
            return f"Row count decreased by {abs(delta):,}"
        else:
            return event_type.replace("_", " ").title()
    
    def _get_vanished_date(self, dataset_id: str) -> Optional[str]:
        """Get the date when dataset vanished"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT snapshot_date FROM events
            WHERE dataset_id = ? AND event_type = 'VANISHED'
            ORDER BY snapshot_date DESC LIMIT 1
        ''', (dataset_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        return row[0] if row else None
    
    def get_field_diff_history(self, dataset_id: str, field: str) -> List[Dict]:
        """Get change history for a specific field"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all snapshots for this dataset
        cursor.execute('''
            SELECT DATE(created_at) as snapshot_date, {}
            FROM dataset_states
            WHERE dataset_id = ?
            ORDER BY created_at ASC
        '''.format(field), (dataset_id,))
        
        snapshots = cursor.fetchall()
        
        # Build change history
        history = []
        previous_value = None
        
        for snapshot_date, value in snapshots:
            if value != previous_value:
                history.append({
                    "date": snapshot_date,
                    "value": value,
                    "old_value": previous_value,
                    "changed": True
                })
            else:
                history.append({
                    "date": snapshot_date,
                    "value": value,
                    "old_value": None,
                    "changed": False
                })
            
            previous_value = value
        
        conn.close()
        return history
    
    def export_timeline_data(self, dataset_id: str, format: str = "json") -> str:
        """Export timeline data in specified format"""
        chromogram_data = self.generate_chromogram_data(dataset_id)
        
        if format == "json":
            return json.dumps({
                "dataset_id": chromogram_data.dataset_id,
                "date_range": chromogram_data.date_range,
                "bands": [
                    {
                        "name": band.name,
                        "type": band.band_type.value,
                        "fields": band.fields,
                        "color_map": band.color_map
                    }
                    for band in chromogram_data.bands
                ],
                "cells": [
                    {
                        "field": cell.field,
                        "date": cell.date,
                        "value": cell.value,
                        "changed": cell.changed,
                        "old_value": cell.old_value,
                        "color": cell.color
                    }
                    for cell in chromogram_data.cells
                ],
                "events": [
                    {
                        "date": event.date,
                        "event_type": event.event_type,
                        "severity": event.severity,
                        "description": event.description
                    }
                    for event in chromogram_data.events
                ],
                "vanished_date": chromogram_data.vanished_date
            }, indent=2)
        
        elif format == "csv":
            # Export cells as CSV
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow(["field", "date", "value", "changed", "old_value", "color"])
            
            # Data
            for cell in chromogram_data.cells:
                writer.writerow([
                    cell.field,
                    cell.date,
                    cell.value,
                    cell.changed,
                    cell.old_value,
                    cell.color
                ])
            
            return output.getvalue()
        
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def get_timeline_summary(self, dataset_id: str) -> Dict[str, Any]:
        """Get summary statistics for timeline"""
        chromogram_data = self.generate_chromogram_data(dataset_id)
        
        # Count changes by field
        field_changes = {}
        for cell in chromogram_data.cells:
            if cell.changed:
                field_changes[cell.field] = field_changes.get(cell.field, 0) + 1
        
        # Count events by type
        event_counts = {}
        for event in chromogram_data.events:
            event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1
        
        # Count events by severity
        severity_counts = {}
        for event in chromogram_data.events:
            severity_counts[event.severity] = severity_counts.get(event.severity, 0) + 1
        
        return {
            "dataset_id": dataset_id,
            "date_range": chromogram_data.date_range,
            "total_snapshots": len(set(cell.date for cell in chromogram_data.cells)),
            "total_changes": sum(field_changes.values()),
            "field_changes": field_changes,
            "event_counts": event_counts,
            "severity_counts": severity_counts,
            "vanished": chromogram_data.vanished_date is not None,
            "vanished_date": chromogram_data.vanished_date
        }

def main():
    """Test the Chromogram timeline"""
    timeline = ChromogramTimelineV2()
    
    # Get a sample dataset
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT dataset_id FROM dataset_states LIMIT 1')
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        print("No datasets found in database")
        return
    
    dataset_id = row[0]
    print(f"Generating timeline for dataset: {dataset_id}")
    
    # Generate chromogram data
    chromogram_data = timeline.generate_chromogram_data(dataset_id)
    
    print(f"Timeline generated:")
    print(f"  Date range: {chromogram_data.date_range[0]} to {chromogram_data.date_range[1]}")
    print(f"  Bands: {len(chromogram_data.bands)}")
    print(f"  Cells: {len(chromogram_data.cells)}")
    print(f"  Events: {len(chromogram_data.events)}")
    print(f"  Vanished: {chromogram_data.vanished_date is not None}")
    
    # Show summary
    summary = timeline.get_timeline_summary(dataset_id)
    print(f"\nSummary:")
    print(f"  Total snapshots: {summary['total_snapshots']}")
    print(f"  Total changes: {summary['total_changes']}")
    print(f"  Field changes: {summary['field_changes']}")
    print(f"  Event counts: {summary['event_counts']}")
    
    # Export sample data
    json_data = timeline.export_timeline_data(dataset_id, "json")
    print(f"\nExported {len(json_data)} characters of JSON data")

if __name__ == "__main__":
    main()
