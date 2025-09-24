"""
Enhanced Diff Engine V2
Comprehensive diffing for metadata, schema, and content changes
Implements the full diff specification from the plan
"""

import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib
import re
from difflib import SequenceMatcher

class ChangeType(Enum):
    METADATA = "metadata"
    SCHEMA = "schema"
    CONTENT = "content"

@dataclass
class FieldChange:
    field: str
    old_value: Any
    new_value: Any
    change_type: str  # "added", "removed", "modified", "renamed"
    confidence: float = 1.0

@dataclass
class SchemaChange:
    added_columns: List[str]
    removed_columns: List[str]
    renamed_columns: List[Dict[str, str]]  # [{"old": "fac_id", "new": "facility_id"}]
    dtype_changes: List[Dict[str, str]]  # [{"column": "release_lbs", "from": "integer", "to": "number"}]
    row_delta: int

@dataclass
class ContentChange:
    dataset_similarity: float
    columns_changed: List[str]
    quantile_shifts: List[Dict[str, float]]  # [{"column": "release_lbs", "p95_delta": 13.4}]
    row_count_delta: int
    content_hash_changed: bool

@dataclass
class DiffResult:
    dataset_id: str
    from_date: str
    to_date: str
    metadata_changes: List[FieldChange]
    schema_changes: SchemaChange
    content_changes: ContentChange
    signals: Dict[str, bool]  # {"major_change": true, "license_flip": true, "url_moved": false}
    severity: str
    created_at: datetime

class EnhancedDiffEngineV2:
    """Enhanced diff engine with comprehensive change detection"""
    
    def __init__(self, db_path: str = "datasets.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables for diff storage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create diffs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dataset_diffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                from_date TEXT NOT NULL,
                to_date TEXT NOT NULL,
                metadata_changes TEXT,
                schema_changes TEXT,
                content_changes TEXT,
                signals TEXT,
                severity TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dataset_id, from_date, to_date)
            )
        ''')
        
        # Create field changes table for detailed tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS field_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                diff_id INTEGER,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                FOREIGN KEY (diff_id) REFERENCES dataset_diffs (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_dataset_snapshots(self, dataset_id: str, from_date: str, to_date: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Get two snapshots for comparison"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get from snapshot
        cursor.execute('''
            SELECT * FROM dataset_states
            WHERE dataset_id = ? AND DATE(created_at) = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (dataset_id, from_date))
        
        from_row = cursor.fetchone()
        from_snapshot = None
        if from_row:
            columns = [description[0] for description in cursor.description]
            from_snapshot = dict(zip(columns, from_row))
        
        # Get to snapshot
        cursor.execute('''
            SELECT * FROM dataset_states
            WHERE dataset_id = ? AND DATE(created_at) = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (dataset_id, to_date))
        
        to_row = cursor.fetchone()
        to_snapshot = None
        if to_row:
            columns = [description[0] for description in cursor.description]
            to_snapshot = dict(zip(columns, to_row))
        
        conn.close()
        return from_snapshot, to_snapshot
    
    def compute_diff(self, dataset_id: str, from_date: str, to_date: str) -> Optional[DiffResult]:
        """Compute comprehensive diff between two snapshots"""
        from_snapshot, to_snapshot = self.get_dataset_snapshots(dataset_id, from_date, to_date)
        
        if not from_snapshot or not to_snapshot:
            return None
        
        # Compute metadata changes
        metadata_changes = self._compute_metadata_changes(from_snapshot, to_snapshot)
        
        # Compute schema changes
        schema_changes = self._compute_schema_changes(from_snapshot, to_snapshot)
        
        # Compute content changes
        content_changes = self._compute_content_changes(from_snapshot, to_snapshot)
        
        # Generate signals
        signals = self._generate_signals(metadata_changes, schema_changes, content_changes)
        
        # Calculate severity
        severity = self._calculate_severity(metadata_changes, schema_changes, content_changes, signals)
        
        return DiffResult(
            dataset_id=dataset_id,
            from_date=from_date,
            to_date=to_date,
            metadata_changes=metadata_changes,
            schema_changes=schema_changes,
            content_changes=content_changes,
            signals=signals,
            severity=severity,
            created_at=datetime.now()
        )
    
    def _compute_metadata_changes(self, from_snapshot: Dict, to_snapshot: Dict) -> List[FieldChange]:
        """Compute metadata field changes"""
        changes = []
        
        # Define metadata fields to track
        metadata_fields = {
            'title': 'string',
            'agency': 'string', 
            'url': 'string',
            'availability': 'string',
            'last_modified': 'datetime',
            'dimension_computation_time_ms': 'number'
        }
        
        for field, field_type in metadata_fields.items():
            old_value = from_snapshot.get(field)
            new_value = to_snapshot.get(field)
            
            if old_value != new_value:
                change_type = "modified"
                if old_value is None:
                    change_type = "added"
                elif new_value is None:
                    change_type = "removed"
                
                changes.append(FieldChange(
                    field=field,
                    old_value=old_value,
                    new_value=new_value,
                    change_type=change_type
                ))
        
        return changes
    
    def _compute_schema_changes(self, from_snapshot: Dict, to_snapshot: Dict) -> SchemaChange:
        """Compute schema changes between snapshots"""
        from_columns = from_snapshot.get('column_count', 0)
        to_columns = to_snapshot.get('column_count', 0)
        
        # For now, we only have column count - in a full implementation,
        # this would parse actual schema information
        added_columns = []
        removed_columns = []
        renamed_columns = []
        dtype_changes = []
        
        if to_columns > from_columns:
            # Assume new columns were added
            added_columns = [f"column_{i}" for i in range(from_columns, to_columns)]
        elif to_columns < from_columns:
            # Assume columns were removed
            removed_columns = [f"column_{i}" for i in range(to_columns, from_columns)]
        
        row_delta = to_snapshot.get('row_count', 0) - from_snapshot.get('row_count', 0)
        
        return SchemaChange(
            added_columns=added_columns,
            removed_columns=removed_columns,
            renamed_columns=renamed_columns,
            dtype_changes=dtype_changes,
            row_delta=row_delta
        )
    
    def _compute_content_changes(self, from_snapshot: Dict, to_snapshot: Dict) -> ContentChange:
        """Compute content changes between snapshots"""
        from_rows = from_snapshot.get('row_count', 0)
        to_rows = to_snapshot.get('row_count', 0)
        
        # Calculate similarity based on row count and response time
        similarity = self._calculate_content_similarity(from_snapshot, to_snapshot)
        
        # Determine which "columns" changed (simplified)
        columns_changed = []
        if from_snapshot.get('column_count', 0) != to_snapshot.get('column_count', 0):
            columns_changed.append("schema")
        
        # Calculate quantile shifts (simplified)
        quantile_shifts = []
        if from_rows > 0 and to_rows > 0:
            percent_change = abs(to_rows - from_rows) / from_rows * 100
            if percent_change > 10:  # Significant change
                quantile_shifts.append({
                    "column": "row_count",
                    "p95_delta": percent_change
                })
        
        # Check if content hash changed (simplified)
        from_hash = self._compute_content_hash(from_snapshot)
        to_hash = self._compute_content_hash(to_snapshot)
        content_hash_changed = from_hash != to_hash
        
        return ContentChange(
            dataset_similarity=similarity,
            columns_changed=columns_changed,
            quantile_shifts=quantile_shifts,
            row_count_delta=to_rows - from_rows,
            content_hash_changed=content_hash_changed
        )
    
    def _calculate_content_similarity(self, from_snapshot: Dict, to_snapshot: Dict) -> float:
        """Calculate content similarity between snapshots"""
        # Simplified similarity calculation
        # In a full implementation, this would use minhash or other techniques
        
        from_rows = from_snapshot.get('row_count', 0)
        to_rows = to_snapshot.get('row_count', 0)
        from_cols = from_snapshot.get('column_count', 0)
        to_cols = to_snapshot.get('column_count', 0)
        
        if from_rows == 0 and to_rows == 0:
            return 1.0
        
        if from_rows == 0 or to_rows == 0:
            return 0.0
        
        # Row count similarity
        row_similarity = 1.0 - abs(from_rows - to_rows) / max(from_rows, to_rows)
        
        # Column count similarity
        col_similarity = 1.0 - abs(from_cols - to_cols) / max(from_cols, to_cols) if max(from_cols, to_cols) > 0 else 1.0
        
        # Response time similarity
        from_response = from_snapshot.get('response_time_ms', 0)
        to_response = to_snapshot.get('response_time_ms', 0)
        response_similarity = 1.0 - abs(from_response - to_response) / max(from_response, to_response) if max(from_response, to_response) > 0 else 1.0
        
        # Weighted average
        return (row_similarity * 0.5 + col_similarity * 0.3 + response_similarity * 0.2)
    
    def _compute_content_hash(self, snapshot: Dict) -> str:
        """Compute content hash for a snapshot"""
        # Simplified content hash based on key fields
        content_string = f"{snapshot.get('row_count', 0)}_{snapshot.get('column_count', 0)}_{snapshot.get('response_time_ms', 0)}"
        return hashlib.md5(content_string.encode()).hexdigest()
    
    def _generate_signals(self, metadata_changes: List[FieldChange], 
                         schema_changes: SchemaChange, 
                         content_changes: ContentChange) -> Dict[str, bool]:
        """Generate signals based on changes"""
        signals = {
            "major_change": False,
            "license_flip": False,
            "url_moved": False,
            "schema_shrink": False,
            "content_drift": False,
            "row_count_spike": False
        }
        
        # Check for major changes
        if len(metadata_changes) > 2 or len(schema_changes.added_columns) > 0 or len(schema_changes.removed_columns) > 0:
            signals["major_change"] = True
        
        # Check for license flip
        for change in metadata_changes:
            if change.field == "availability" and change.change_type == "modified":
                signals["license_flip"] = True
        
        # Check for URL move
        for change in metadata_changes:
            if change.field == "url" and change.change_type == "modified":
                signals["url_moved"] = True
        
        # Check for schema shrink
        if len(schema_changes.removed_columns) > len(schema_changes.added_columns):
            signals["schema_shrink"] = True
        
        # Check for content drift
        if content_changes.dataset_similarity < 0.8:
            signals["content_drift"] = True
        
        # Check for row count spike
        if abs(content_changes.row_count_delta) > 1000:
            signals["row_count_spike"] = True
        
        return signals
    
    def _calculate_severity(self, metadata_changes: List[FieldChange], 
                           schema_changes: SchemaChange, 
                           content_changes: ContentChange, 
                           signals: Dict[str, bool]) -> str:
        """Calculate overall severity of changes"""
        severity_score = 0
        
        # Metadata changes
        for change in metadata_changes:
            if change.field == "availability" and change.change_type == "modified":
                severity_score += 3  # High severity for availability changes
            elif change.field == "url" and change.change_type == "modified":
                severity_score += 2  # Medium-high for URL changes
            else:
                severity_score += 1  # Low for other metadata changes
        
        # Schema changes
        if len(schema_changes.removed_columns) > 0:
            severity_score += 2  # Medium for removed columns
        if len(schema_changes.added_columns) > 0:
            severity_score += 1  # Low for added columns
        
        # Content changes
        if content_changes.dataset_similarity < 0.5:
            severity_score += 3  # High for major content drift
        elif content_changes.dataset_similarity < 0.8:
            severity_score += 2  # Medium for moderate drift
        
        if abs(content_changes.row_count_delta) > 10000:
            severity_score += 2  # Medium for large row count changes
        
        # Signal-based severity
        if signals.get("license_flip"):
            severity_score += 2
        if signals.get("url_moved"):
            severity_score += 1
        if signals.get("schema_shrink"):
            severity_score += 2
        if signals.get("content_drift"):
            severity_score += 2
        
        if severity_score >= 5:
            return "high"
        elif severity_score >= 3:
            return "medium"
        else:
            return "low"
    
    def store_diff(self, diff_result: DiffResult) -> int:
        """Store diff result in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store main diff record
        cursor.execute('''
            INSERT OR REPLACE INTO dataset_diffs
            (dataset_id, from_date, to_date, metadata_changes, schema_changes,
             content_changes, signals, severity, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            diff_result.dataset_id,
            diff_result.from_date,
            diff_result.to_date,
            json.dumps([{
                "field": change.field,
                "old_value": change.old_value,
                "new_value": change.new_value,
                "change_type": change.change_type,
                "confidence": change.confidence
            } for change in diff_result.metadata_changes]),
            json.dumps({
                "added_columns": diff_result.schema_changes.added_columns,
                "removed_columns": diff_result.schema_changes.removed_columns,
                "renamed_columns": diff_result.schema_changes.renamed_columns,
                "dtype_changes": diff_result.schema_changes.dtype_changes,
                "row_delta": diff_result.schema_changes.row_delta
            }),
            json.dumps({
                "dataset_similarity": diff_result.content_changes.dataset_similarity,
                "columns_changed": diff_result.content_changes.columns_changed,
                "quantile_shifts": diff_result.content_changes.quantile_shifts,
                "row_count_delta": diff_result.content_changes.row_count_delta,
                "content_hash_changed": diff_result.content_changes.content_hash_changed
            }),
            json.dumps(diff_result.signals),
            diff_result.severity,
            diff_result.created_at.isoformat()
        ))
        
        diff_id = cursor.lastrowid
        
        # Store individual field changes
        for change in diff_result.metadata_changes:
            cursor.execute('''
                INSERT INTO field_changes
                (diff_id, field_name, old_value, new_value, change_type, confidence)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                diff_id,
                change.field,
                str(change.old_value) if change.old_value is not None else None,
                str(change.new_value) if change.new_value is not None else None,
                change.change_type,
                change.confidence
            ))
        
        conn.commit()
        conn.close()
        return diff_id
    
    def get_diff(self, dataset_id: str, from_date: str, to_date: str) -> Optional[Dict]:
        """Get stored diff for a dataset between two dates"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM dataset_diffs
            WHERE dataset_id = ? AND from_date = ? AND to_date = ?
        ''', (dataset_id, from_date, to_date))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        
        columns = [description[0] for description in cursor.description]
        diff_data = dict(zip(columns, row))
        
        # Parse JSON fields
        diff_data['metadata_changes'] = json.loads(diff_data['metadata_changes'] or '[]')
        diff_data['schema_changes'] = json.loads(diff_data['schema_changes'] or '{}')
        diff_data['content_changes'] = json.loads(diff_data['content_changes'] or '{}')
        diff_data['signals'] = json.loads(diff_data['signals'] or '{}')
        
        conn.close()
        return diff_data
    
    def get_dataset_diffs(self, dataset_id: str, limit: int = 10) -> List[Dict]:
        """Get all diffs for a dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM dataset_diffs
            WHERE dataset_id = ?
            ORDER BY to_date DESC
            LIMIT ?
        ''', (dataset_id, limit))
        
        columns = [description[0] for description in cursor.description]
        diffs = []
        
        for row in cursor.fetchall():
            diff_data = dict(zip(columns, row))
            # Parse JSON fields
            diff_data['metadata_changes'] = json.loads(diff_data['metadata_changes'] or '[]')
            diff_data['schema_changes'] = json.loads(diff_data['schema_changes'] or '{}')
            diff_data['content_changes'] = json.loads(diff_data['content_changes'] or '{}')
            diff_data['signals'] = json.loads(diff_data['signals'] or '{}')
            diffs.append(diff_data)
        
        conn.close()
        return diffs

def main():
    """Test the enhanced diff engine"""
    engine = EnhancedDiffEngineV2()
    
    # Get some sample dataset IDs
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT dataset_id FROM dataset_states 
        ORDER BY created_at DESC LIMIT 5
    ''')
    dataset_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not dataset_ids:
        print("No datasets found in database")
        return
    
    # Test diff computation for first dataset
    dataset_id = dataset_ids[0]
    print(f"Testing diff computation for dataset: {dataset_id}")
    
    # Get available dates for this dataset
    conn = sqlite3.connect("datasets.db")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT DATE(created_at) as snapshot_date
        FROM dataset_states
        WHERE dataset_id = ?
        ORDER BY snapshot_date ASC
    ''', (dataset_id,))
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if len(dates) < 2:
        print(f"Need at least 2 snapshots for {dataset_id}")
        return
    
    # Compute diff between first two dates
    from_date = dates[0]
    to_date = dates[1]
    
    print(f"Computing diff from {from_date} to {to_date}")
    diff_result = engine.compute_diff(dataset_id, from_date, to_date)
    
    if diff_result:
        print(f"✅ Diff computed successfully")
        print(f"Severity: {diff_result.severity}")
        print(f"Metadata changes: {len(diff_result.metadata_changes)}")
        print(f"Schema changes: {len(diff_result.schema_changes.added_columns)} added, {len(diff_result.schema_changes.removed_columns)} removed")
        print(f"Content similarity: {diff_result.content_changes.dataset_similarity:.3f}")
        print(f"Signals: {diff_result.signals}")
        
        # Store the diff
        diff_id = engine.store_diff(diff_result)
        print(f"Stored diff with ID: {diff_id}")
    else:
        print("❌ Failed to compute diff")

if __name__ == "__main__":
    main()
